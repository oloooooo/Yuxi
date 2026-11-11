"""Milvus 知识库图谱服务：编排实体抽取、图写入、向量索引与 PPR 检索。

Cypher 与图存储字段、PPR 参数语义、graph_structure_indexed → graph_indexed
两阶段构建状态是对外契约。本服务只做编排：PG 投影归 repositories/graphs.py，
Milvus 向量细节归 milvus_graph_vector_store.py，Cypher 模板归 graph_utils.py。
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from yuxi.infrastructure.filesystem import await_io
from yuxi.infrastructure.nebula import (
    NebulaConnectionManager,
    get_shared_nebula_connection,
    ngql_list,
    ngql_literal,
    result_rows,
)
from yuxi.infrastructure.observability.logging import logger
from yuxi.modules.knowledge.graphs.extractors import GraphExtractor, GraphExtractorFactory, normalize_extraction_result
from yuxi.modules.knowledge.graphs.graph_utils import (
    build_graph_payload,
    chunk_vid,
    compute_entity_id,
    compute_triple_id,
    entity_vid,
    ngql_insert_mention,
    ngql_insert_relation,
    ngql_upsert_chunk,
    ngql_upsert_entity,
    normalize_entity_name,
    relation_rank,
)
from yuxi.modules.knowledge.graphs.milvus_graph_vector_store import MilvusGraphVectorStore
from yuxi.modules.knowledge.repositories.bases import KnowledgeBaseRepository
from yuxi.modules.knowledge.repositories.chunks import KnowledgeChunkRepository
from yuxi.modules.knowledge.repositories.document_resets import DocumentResetRepository
from yuxi.modules.knowledge.repositories.graphs import KnowledgeGraphRepository
from yuxi.modules.knowledge.repositories.projections import knowledge_projection_lock
from yuxi.modules.knowledge.storage.offload import run_milvus_query_io, run_nebula_query_io
from yuxi.shared.datetime import utc_isoformat
from yuxi.shared.errors import ResourceNotFoundError

GRAPH_CONFIG_KEY = "graph_build_config"
GRAPH_TASK_TYPE = "knowledge_graph_index"
# 数据库游标每次预取的 chunk 数量边界，只控制查询频率和单页内存，不限制 LLM 并发。
# 实际 LLM 并发只由 extractor_options.concurrency_count 决定；预取量会按其两倍动态计算后落在此范围内。
GRAPH_BUILD_FETCH_MIN_SIZE = 100
GRAPH_BUILD_FETCH_MAX_SIZE = 1000
# 构建任务 INFO 汇总日志与前端进度更新间隔；单个 chunk 的耗时明细使用 DEBUG 日志。
GRAPH_BUILD_LOG_INTERVAL_SECONDS = 5.0
# 单轮向 Milvus 认领并写入的图向量记录数；租约须覆盖最慢一轮嵌入写入，超时后记录可被重新认领。
GRAPH_VECTOR_BATCH_SIZE = 100
GRAPH_VECTOR_LEASE_SECONDS = 300
# 空闲探测的节流间隔：写入侧刚唤醒时短暂停顿，避免高频小批写入。
GRAPH_VECTOR_FLUSH_INTERVAL_SECONDS = 0.2
# 单 chunk 抽取的最大尝试次数与前两次重试的退避间隔。
GRAPH_EXTRACTION_MAX_ATTEMPTS = 3
GRAPH_EXTRACTION_RETRY_DELAYS_SECONDS = (2.0, 10.0)


class KnowledgeGraphService:
    def __init__(
        self,
        *,
        kb_id: str | None = None,
        kb_repo: KnowledgeBaseRepository | None = None,
        chunk_repo: KnowledgeChunkRepository | None = None,
        graph_repo: KnowledgeGraphRepository | None = None,
        graph_vector_store: MilvusGraphVectorStore | None = None,
        nebula_connection: NebulaConnectionManager | None = None,
    ):
        self.kb_id = kb_id
        self.kb_repo = kb_repo or KnowledgeBaseRepository()
        self.chunk_repo = chunk_repo or KnowledgeChunkRepository()
        self.graph_repo = graph_repo or KnowledgeGraphRepository()
        self._graph_vector_store = graph_vector_store
        self._graph_vector_store_lock = asyncio.Lock()
        self._connection = nebula_connection

    @property
    def connection(self) -> NebulaConnectionManager:
        if self._connection is None:
            self._connection = get_shared_nebula_connection()
        return self._connection

    @property
    def graph_vector_store(self) -> MilvusGraphVectorStore:
        if self._graph_vector_store is None:
            self._graph_vector_store = MilvusGraphVectorStore()
        return self._graph_vector_store

    async def get_graph_vector_store(self) -> MilvusGraphVectorStore:
        """首次构造在线程中落定；取消前完成缓存赋值，避免重复构造。"""
        if self._graph_vector_store is not None:
            return self._graph_vector_store
        async with self._graph_vector_store_lock:
            if self._graph_vector_store is None:
                await await_io(run_milvus_query_io(lambda: self.graph_vector_store))
        return self._graph_vector_store

    @property
    def driver(self):
        return self.connection.driver

    # ─── 配置与构建状态 ────────────────────────────────────────────

    async def get_status(self, kb_id: str, *, job_tracker: Any = None) -> dict[str, Any]:
        kb = await self._get_milvus_kb(kb_id)
        params = dict(kb.additional_params or {})
        config = params.get(GRAPH_CONFIG_KEY) or {}
        (
            total_chunks,
            pending_chunks,
            indexed_chunks,
            structured_chunks,
            extraction_counts,
            graph_counts,
            vector_counts,
        ) = await asyncio.gather(
            self.chunk_repo.count_by_kb_id(kb_id),
            self.chunk_repo.count_graph_pending_by_kb_id(kb_id),
            self.chunk_repo.count_graph_indexed_by_kb_id(kb_id),
            self.chunk_repo.count_graph_structure_indexed_by_kb_id(kb_id),
            self.chunk_repo.count_graph_extraction_statuses_by_kb_id(kb_id),
            self.graph_repo.count_by_kb_id(kb_id),
            self.graph_repo.count_vector_statuses_by_kb_id(kb_id),
        )
        entity_count, relationship_count = graph_counts

        build_job_status = None
        build_job_progress = 0
        if job_tracker is not None:
            latest_job = await job_tracker.find_job_by_payload(
                job_type=GRAPH_TASK_TYPE,
                payload_match={"kb_id": kb_id},
                statuses=None,
            )
            if latest_job:
                build_job_status, build_job_progress = self._project_build_job(latest_job)

        return {
            "kb_id": kb_id,
            "kb_type": kb.kb_type,
            "configured": bool(config),
            "locked": bool(config.get("locked")),
            "config": self._public_config(config),
            "total_chunks": total_chunks,
            "pending_chunks": pending_chunks,
            "indexed_chunks": indexed_chunks,
            "structured_chunks": structured_chunks,
            "extraction_counts": extraction_counts,
            "vector_counts": vector_counts,
            "entity_count": entity_count,
            "relationship_count": relationship_count,
            "build_job_status": build_job_status,
            "build_job_progress": build_job_progress,
        }

    async def configure(
        self,
        kb_id: str,
        extractor_type: str,
        extractor_options: dict[str, Any],
        created_by: str,
    ) -> dict:
        kb = await self._get_milvus_kb(kb_id)
        additional_params = dict(kb.additional_params or {})
        existing_config = additional_params.get(GRAPH_CONFIG_KEY) or {}
        normalized_extractor_type = (extractor_type or "").lower()
        if existing_config.get("locked"):
            existing_extractor_type = (existing_config.get("extractor_type") or "").lower()
            if normalized_extractor_type != existing_extractor_type:
                raise ValueError("图谱抽取器类型已锁定，只能修改模型、Schema 等抽取参数")

        extractor_options = extractor_options or {}
        if normalized_extractor_type == "llm" and extractor_options.get("prompt"):
            raise ValueError("LLM 图谱抽取器不支持自定义完整 Prompt，请使用 schema 配置抽取约束")
        GraphExtractorFactory.create(normalized_extractor_type, extractor_options)
        config = {
            "locked": True,
            "extractor_type": normalized_extractor_type,
            "extractor_options": extractor_options,
            "created_at": existing_config.get("created_at") or utc_isoformat(),
            "created_by": existing_config.get("created_by") or created_by,
        }
        if existing_config.get("locked"):
            config["updated_at"] = utc_isoformat()
            config["updated_by"] = created_by
        additional_params[GRAPH_CONFIG_KEY] = config
        await self.kb_repo.update(kb_id, {"additional_params": additional_params})
        return config

    async def get_failed_chunk_samples(self, kb_id: str, limit: int = 10) -> dict[str, Any]:
        await self._get_milvus_kb(kb_id)
        samples = await self.chunk_repo.list_graph_extraction_failed_samples(kb_id, limit)
        return {"kb_id": kb_id, "samples": samples}

    # ─── 图谱构建：抽取 → 结构写入 → 向量索引 ─────────────────────

    async def build_pending_chunks(
        self, kb_id: str, *, file_id: str | None = None, generation: int | None = None, context=None
    ) -> dict[str, Any]:
        """图谱写入期间固定投影和目标文件代次。"""
        if file_id is not None:
            if not file_id or not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
                raise ValueError("文件图谱任务缺少有效的文件代次")
            async with self.graph_repo.file_build_scope(kb_id, file_id, generation):
                return await self._build_pending_chunks(kb_id, file_id=file_id, context=context)
        if generation is not None:
            raise ValueError("文件代次必须与文件 ID 一起指定")
        async with knowledge_projection_lock(kb_id, shared=True):
            return await self._build_pending_chunks(kb_id, context=context)

    async def _build_pending_chunks(self, kb_id: str, *, file_id: str | None = None, context=None) -> dict[str, Any]:
        """按现有流水线构建当前可见 Chunk 的图谱。"""
        await DocumentResetRepository().require_processable(kb_id, file_id, job_id=getattr(context, "job_id", None))
        kb = await self._get_milvus_kb(kb_id)
        config = self._get_locked_config(kb.additional_params or {})
        extractor_options = self._runtime_extractor_options(config)
        extractor = GraphExtractorFactory.create(config["extractor_type"], extractor_options)
        worker_count = self._get_worker_count(config)
        scope = {"file_id": file_id} if file_id is not None else {}
        total_pending = await self.chunk_repo.count_graph_pending_by_kb_id(kb_id, **scope)
        initially_indexed = await self.chunk_repo.count_graph_indexed_by_kb_id(kb_id, **scope)
        processed = 0
        extraction_failed = 0
        write_failed = 0
        extraction_completed = 0
        write_completed = 0
        active_extractions = 0
        fetch_size = max(GRAPH_BUILD_FETCH_MIN_SIZE, min(worker_count * 2, GRAPH_BUILD_FETCH_MAX_SIZE))
        extraction_queue: asyncio.Queue[Any | None] = asyncio.Queue(maxsize=max(worker_count * 2, 1))
        write_queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=max(worker_count * 2, 1))
        reporter_stop = asyncio.Event()
        structure_done = asyncio.Event()
        vector_wakeup = asyncio.Event()
        started_at = time.monotonic()

        logger.info(f"图谱构建开始 kb_id={kb_id} pending={total_pending} extraction_concurrency={worker_count} fetch_size={fetch_size}")

        async def put_queue_item(queue: asyncio.Queue, item) -> None:
            while True:
                if context is not None:
                    await context.raise_if_cancelled()
                try:
                    await asyncio.wait_for(queue.put(item), timeout=0.5)
                    return
                except TimeoutError:
                    continue

        async def extraction_worker(worker_index: int) -> None:
            nonlocal active_extractions, extraction_completed, extraction_failed
            while True:
                chunk = await extraction_queue.get()
                try:
                    if chunk is None:
                        return
                    if context is not None:
                        await context.raise_if_cancelled()
                    active_extractions += 1
                    extraction_started_at = time.monotonic()
                    try:
                        await self._get_chunk_extraction_result(kb_id, chunk, extractor)
                        await put_queue_item(write_queue, chunk.chunk_id)
                    except Exception as exc:
                        extraction_failed += 1
                        logger.error(f"Chunk 图谱抽取失败 kb_id={kb_id} chunk_id={chunk.chunk_id} worker={worker_index}: {exc}")
                    finally:
                        active_extractions -= 1
                        extraction_completed += 1
                        logger.debug(
                            f"Chunk 图谱抽取结束 kb_id={kb_id} chunk_id={chunk.chunk_id} "
                            f"worker={worker_index} duration={time.monotonic() - extraction_started_at:.2f}s"
                        )
                finally:
                    extraction_queue.task_done()

        async def write_worker() -> None:
            nonlocal processed, write_failed, write_completed
            while True:
                chunk_id = await write_queue.get()
                try:
                    if chunk_id is None:
                        return
                    if context is not None:
                        await context.raise_if_cancelled()
                    chunk = await self.chunk_repo.get_by_chunk_id(chunk_id)
                    if chunk is None:
                        raise ValueError(f"图谱写入找不到 chunk: {chunk_id}")
                    extraction_result = await self._get_chunk_extraction_result(kb_id, chunk, extractor)
                    write_started_at = time.monotonic()
                    entities, triples = await await_io(asyncio.to_thread(self.write_chunk_graph, kb_id, chunk, extraction_result))
                    await self.graph_repo.upsert_chunk_graph(
                        kb_id=kb_id,
                        file_id=chunk.file_id,
                        chunk_id=chunk.chunk_id,
                        entities=entities,
                        triples=triples,
                    )
                    await self.chunk_repo.mark_graph_structure_indexed(
                        chunk.chunk_id,
                        ent_ids=[entity["entity_id"] for entity in entities],
                    )
                    vector_wakeup.set()
                    processed += 1
                    logger.debug(
                        f"Chunk 图谱写入结束 kb_id={kb_id} chunk_id={chunk.chunk_id} "
                        f"entities={len(entities)} triples={len(triples)} "
                        f"duration={time.monotonic() - write_started_at:.2f}s"
                    )
                except Exception as exc:
                    write_failed += 1
                    logger.error(f"Chunk 图谱写入失败 kb_id={kb_id} chunk_id={chunk_id}: {exc}")
                finally:
                    if chunk_id is not None:
                        write_completed += 1
                    write_queue.task_done()

        async def index_vector_batch(record_type: str) -> int:
            lock_token, records = await self.graph_repo.claim_vector_records(
                kb_id=kb_id,
                record_type=record_type,
                limit=GRAPH_VECTOR_BATCH_SIZE,
                lease_seconds=GRAPH_VECTOR_LEASE_SECONDS,
                **scope,
            )
            if not records:
                return 0
            record_ids = [record["id"] for record in records]
            try:
                graph_vector_store = await self.get_graph_vector_store()
                await graph_vector_store.upsert_graph_records(
                    kb_id=kb_id,
                    embedding_model_spec=kb.embedding_model_spec,
                    record_type=record_type,
                    records=records,
                )
                await self.graph_repo.mark_vector_records_indexed(
                    record_type=record_type,
                    record_ids=record_ids,
                    lock_token=lock_token,
                )
            except asyncio.CancelledError as exc:
                await self.graph_repo.mark_vector_records_failed(
                    record_type=record_type,
                    record_ids=record_ids,
                    lock_token=lock_token,
                    error=str(exc) or "cancelled",
                )
                raise
            except Exception as exc:
                await self.graph_repo.mark_vector_records_failed(
                    record_type=record_type,
                    record_ids=record_ids,
                    lock_token=lock_token,
                    error=str(exc),
                )
                logger.error(f"图谱向量索引失败 kb_id={kb_id} type={record_type} count={len(records)}: {exc}")
            return len(records)

        async def vector_worker() -> None:
            while True:
                if context is not None:
                    await context.raise_if_cancelled()
                counts = await await_io(
                    asyncio.gather(
                        index_vector_batch("entity"),
                        index_vector_batch("triple"),
                        return_exceptions=True,
                    )
                )
                for count in counts:
                    if isinstance(count, BaseException):
                        raise count
                entity_count, triple_count = counts
                if entity_count or triple_count:
                    # 本轮处理过记录：先把已达成两阶段的 chunk 推进到 graph_indexed，再继续捞取
                    await self.graph_repo.finalize_graph_indexed_chunks(kb_id, **scope)
                    continue

                # 无可认领记录：结构全部写完且向量不再有 pending/processing 才能收尾
                vector_counts = await self.graph_repo.count_vector_statuses_by_kb_id(kb_id, **scope)
                if structure_done.is_set() and not vector_counts["pending"] and not vector_counts["processing"]:
                    await self.graph_repo.finalize_graph_indexed_chunks(kb_id, **scope)
                    return
                # 等写入侧唤醒；1s 超时兜底，轮询重新认领租约过期的记录
                vector_wakeup.clear()
                try:
                    await asyncio.wait_for(vector_wakeup.wait(), timeout=1.0)
                except TimeoutError:
                    pass
                if vector_wakeup.is_set():
                    await asyncio.sleep(GRAPH_VECTOR_FLUSH_INTERVAL_SECONDS)

        async def report_progress() -> None:
            while True:
                try:
                    await asyncio.wait_for(reporter_stop.wait(), timeout=GRAPH_BUILD_LOG_INTERVAL_SECONDS)
                except TimeoutError:
                    pass

                elapsed = max(time.monotonic() - started_at, 0.001)
                extraction_rate = extraction_completed / elapsed
                vector_counts = await self.graph_repo.count_vector_statuses_by_kb_id(kb_id, **scope)
                message = (
                    f"图谱构建：抽取 {extraction_completed}/{total_pending} "
                    f"(活跃 {active_extractions}/{worker_count})，写入 {write_completed}/{total_pending}，"
                    f"向量待处理 {vector_counts['pending'] + vector_counts['processing']}，"
                    f"向量失败 {vector_counts['failed']}，抽取失败 {extraction_failed}，"
                    f"写入失败 {write_failed}，抽取吞吐 {extraction_rate:.2f} chunk/s"
                )
                logger.info(f"kb_id={kb_id} {message}")
                if context is not None:
                    # 进度权重：抽取 40 + 结构写入 20 + 向量完成 35，起点与终点各预留 5
                    extraction_progress = extraction_completed / max(total_pending, 1) * 40.0
                    structure_progress = write_completed / max(total_pending, 1) * 20.0
                    indexed_chunks = await self.chunk_repo.count_graph_indexed_by_kb_id(kb_id, **scope)
                    newly_indexed = max(indexed_chunks - initially_indexed, 0)
                    vector_progress = newly_indexed / max(total_pending, 1) * 35.0
                    await context.set_progress(5.0 + min(extraction_progress + structure_progress + vector_progress, 95.0), message)
                if reporter_stop.is_set():
                    return

        extraction_workers = [
            asyncio.create_task(extraction_worker(index + 1), name=f"graph-extractor-{index + 1}") for index in range(worker_count)
        ]
        writer_task = asyncio.create_task(write_worker(), name="graph-writer")
        vector_task = asyncio.create_task(vector_worker(), name="graph-vector-indexer")
        reporter_task = asyncio.create_task(report_progress(), name="graph-progress-reporter")

        try:
            after_id = 0
            while True:
                if context is not None:
                    await context.raise_if_cancelled()
                chunks = await self.chunk_repo.list_graph_pending_by_kb_id(
                    kb_id,
                    fetch_size,
                    after_id=after_id,
                    **scope,
                )
                if not chunks:
                    break
                for chunk in chunks:
                    after_id = chunk.id
                    if getattr(chunk, "graph_structure_indexed", False):
                        # 结构已写入、只差向量索引：跳过抽取与写入，唤醒向量轮次（重入恢复路径）
                        extraction_completed += 1
                        write_completed += 1
                        vector_wakeup.set()
                    elif chunk.extraction_result:
                        # 已有抽取结果（上次运行中断），直接进入写入队列
                        extraction_completed += 1
                        await put_queue_item(write_queue, chunk.chunk_id)
                    else:
                        await put_queue_item(extraction_queue, chunk)

            for _ in extraction_workers:
                await put_queue_item(extraction_queue, None)
            await asyncio.gather(*extraction_workers)
            await put_queue_item(write_queue, None)
            await writer_task
            # 抽取可能首次关联其他文件已有的失败共享向量，结构写完后才能完整识别目标范围。
            if file_id is not None:
                await self.graph_repo.retry_failed_file_vectors(kb_id, file_id)
            structure_done.set()
            vector_wakeup.set()
            await vector_task
        except BaseException:
            for task in [*extraction_workers, writer_task, vector_task]:
                task.cancel()
            await asyncio.gather(*extraction_workers, writer_task, vector_task, return_exceptions=True)
            raise
        finally:
            reporter_stop.set()
            await asyncio.gather(reporter_task, return_exceptions=True)

        remaining = await self.chunk_repo.count_graph_pending_by_kb_id(kb_id, **scope)
        extraction_counts = await self.chunk_repo.count_graph_extraction_statuses_by_kb_id(kb_id, **scope)
        vector_counts = await self.graph_repo.count_vector_statuses_by_kb_id(kb_id, **scope)
        logger.info(
            f"图谱构建结束 kb_id={kb_id} success={processed} extraction_failed={extraction_failed} "
            f"write_failed={write_failed} remaining={remaining} "
            f"duration={time.monotonic() - started_at:.2f}s"
        )
        incomplete = max(remaining - extraction_counts["failed"], 0)
        result = {
            "kb_id": kb_id,
            "success": max(total_pending - remaining, 0),
            "failed": extraction_failed + write_failed,
            "extraction_failed": extraction_failed,
            "write_failed": write_failed,
            "remaining": remaining,
            "vector_failed": vector_counts["failed"],
        }
        if incomplete or write_failed or vector_counts["failed"] or (file_id is not None and extraction_counts["failed"]):
            raise RuntimeError(
                f"图谱构建执行异常：chunk_incomplete={incomplete}, extraction_failed={extraction_counts['failed']}, "
                f"write_failed={write_failed}, vector_failed={vector_counts['failed']}"
            )
        return result

    async def _get_chunk_extraction_result(self, kb_id: str, chunk, extractor: GraphExtractor) -> dict[str, Any]:
        """返回 chunk 的标准化抽取结果：已有结果直接复用，失败状态先复位再重试。"""
        extractor_type = extractor.extractor_type
        if chunk.extraction_result:
            return normalize_extraction_result(chunk.extraction_result, extractor_type)

        details = getattr(chunk, "graph_extraction_details", None) or {}
        if details.get("status") == "failed":
            await self.chunk_repo.mark_graph_extraction_pending(chunk.chunk_id)

        for attempt in range(1, GRAPH_EXTRACTION_MAX_ATTEMPTS + 1):
            try:
                extraction_result = await extractor.extract(chunk.content)
                normalized_result = normalize_extraction_result(extraction_result, extractor_type)
                await self.chunk_repo.update_extraction_result(chunk.chunk_id, normalized_result, attempt)
                return normalized_result
            except Exception as exc:
                if attempt >= GRAPH_EXTRACTION_MAX_ATTEMPTS:
                    await self.chunk_repo.mark_graph_extraction_failed(chunk.chunk_id, attempt, str(exc))
                    raise
                delay = GRAPH_EXTRACTION_RETRY_DELAYS_SECONDS[attempt - 1]
                logger.warning(
                    f"Chunk 图谱抽取重试 kb_id={kb_id} chunk_id={chunk.chunk_id} "
                    f"attempt={attempt}/{GRAPH_EXTRACTION_MAX_ATTEMPTS} delay={delay:.1f}s: {exc}"
                )
                await asyncio.sleep(delay)

        raise RuntimeError(f"Chunk 图谱抽取未返回结果: {chunk.chunk_id}")

    @staticmethod
    def _get_worker_count(config: dict[str, Any]) -> int:
        if (config.get("extractor_type") or "").lower() != "llm":
            return 1
        try:
            worker_count = int((config.get("extractor_options") or {}).get("concurrency_count") or 1)
        except (TypeError, ValueError):
            return 1
        return max(1, min(worker_count, 1000))

    @staticmethod
    def _runtime_extractor_options(config: dict[str, Any]) -> dict[str, Any]:
        options = dict(config.get("extractor_options") or {})
        options.pop("prompt", None)
        return options

    def write_chunk_graph(
        self,
        kb_id: str,
        chunk,
        normalized_result: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """将单个 chunk 的抽取结果写入 NebulaGraph。"""
        graph_payload = build_graph_payload(normalized_result)
        extractor_type = graph_payload["metadata"].get("extractor_type", "unknown")
        entities = graph_payload["entities"]
        relations = graph_payload["relations"]
        entity_records = self._build_entity_records(kb_id, entities)
        entity_record_by_local_id = {entity["id"]: record for entity, record in zip(entities, entity_records, strict=True)}
        triple_records = self._build_triple_records(kb_id, relations, entity_record_by_local_id, extractor_type)
        content_preview = (chunk.content or "")[:300]

        # 单条 nGQL 无法表达多次 MERGE，按顶点/边类型分条写入
        statements = [
            ngql_upsert_chunk(
                kb_id=kb_id,
                chunk_id=chunk.chunk_id,
                file_id=chunk.file_id,
                chunk_index=chunk.chunk_index,
                content_preview=content_preview,
                start_char_pos=chunk.start_char_pos,
                end_char_pos=chunk.end_char_pos,
            )
        ]
        # 2. 实体顶点 + Chunk→Entity (MENTIONS)；取值直接来自 PG 投影记录
        for entity_record in entity_records:
            statements.append(
                ngql_upsert_entity(
                    kb_id=kb_id,
                    entity_id=entity_record["entity_id"],
                    normalized_name=entity_record["normalized_name"],
                    label=entity_record["label"],
                    name=entity_record["name"],
                    attributes=json.dumps(entity_record["attributes"], ensure_ascii=False),
                    source_chunk_id=chunk.chunk_id,
                )
            )
            statements.append(
                ngql_insert_mention(
                    kb_id=kb_id,
                    chunk_id=chunk.chunk_id,
                    file_id=chunk.file_id,
                    entity_id=entity_record["entity_id"],
                )
            )

        # 3. Entity→Entity (RELATION) 边；rank 由 chunk 与关系类型决定
        for relation, triple_record in zip(relations, triple_records, strict=True):
            source_record = entity_record_by_local_id[relation["source"]]
            target_record = entity_record_by_local_id[relation["target"]]
            statements.append(
                ngql_insert_relation(
                    kb_id=kb_id,
                    chunk_id=chunk.chunk_id,
                    file_id=chunk.file_id,
                    source_entity_id=source_record["entity_id"],
                    target_entity_id=target_record["entity_id"],
                    relation_type=triple_record["relation_type"],
                    triple_id=triple_record["triple_id"],
                    text=triple_record["text"],
                    source_name=source_record["normalized_name"],
                    target_name=target_record["normalized_name"],
                    extractor_type=triple_record["extractor_type"],
                )
            )

        for statement in statements:
            self.connection.execute(statement)
        return entity_records, triple_records

    def _build_entity_records(self, kb_id: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records = []
        for entity in entities:
            label = entity.get("label") or "Entity"
            normalized_name = normalize_entity_name(entity["text"])
            entity_id = compute_entity_id(kb_id, normalized_name, label)
            records.append(
                {
                    "entity_id": entity_id,
                    "kb_id": kb_id,
                    "normalized_name": normalized_name,
                    "label": label,
                    "name": entity["text"],
                    "attributes": entity.get("attributes") or [],
                    "content": normalized_name,
                }
            )
        return records

    def _build_triple_records(
        self,
        kb_id: str,
        relations: list[dict[str, Any]],
        entity_record_by_local_id: dict[str, dict[str, Any]],
        extractor_type: str,
    ) -> list[dict[str, Any]]:
        records = []
        for relation in relations:
            source_record = entity_record_by_local_id[relation["source"]]
            target_record = entity_record_by_local_id[relation["target"]]
            relation_type = relation.get("label") or "RELATED_TO"
            triple_id = compute_triple_id(
                kb_id,
                source_record["normalized_name"],
                source_record["label"],
                relation_type,
                target_record["normalized_name"],
                target_record["label"],
            )
            content = f"{source_record['normalized_name']} → {relation_type} → {target_record['normalized_name']}"
            records.append(
                {
                    "triple_id": triple_id,
                    "kb_id": kb_id,
                    "source_entity_id": source_record["entity_id"],
                    "target_entity_id": target_record["entity_id"],
                    "relation_type": relation_type,
                    "content": content,
                    "text": relation["text"],
                    "extractor_type": extractor_type,
                }
            )
        return records

    # ─── 图谱检索：子图查询与 PPR 排序 ─────────────────────────────

    async def query_nodes(
        self,
        kb_id: str | None = None,
        *,
        keyword: str = "",
        max_depth: int = 1,
        max_nodes: int = 50,
        exclude_chunk: bool = False,
        file_id: str | None = None,
    ) -> dict[str, Any]:
        effective_kb_id = kb_id or self.kb_id
        if not effective_kb_id:
            return {"nodes": [], "edges": []}

        if file_id is not None:
            # 同一事务持有投影锁与文件行锁，直到图读取完成。
            async with self.graph_repo.file_graph_snapshot(effective_kb_id, file_id) as snapshot:
                if snapshot is None:
                    raise ResourceNotFoundError("文件不存在")
                entity_mentions, triple_mentions, chunk_ids = snapshot
                if not entity_mentions and (exclude_chunk or not chunk_ids):
                    return {"nodes": [], "edges": []}
                result = await run_nebula_query_io(
                    self._query_file_nodes_sync,
                    effective_kb_id,
                    keyword,
                    max_nodes,
                    max_depth,
                    exclude_chunk,
                    entity_mentions,
                    triple_mentions,
                    chunk_ids,
                )
                mention_pairs = {(m["id"], m["chunk_id"]) for m in entity_mentions}
                for node in result["nodes"]:
                    properties = node["properties"]
                    if properties.get("entity_id") and (properties["entity_id"], properties.get("source_chunk_id")) not in mention_pairs:
                        node["properties"] = {
                            key: properties[key] for key in ("entity_id", "normalized_name", "label", "kb_id") if key in properties
                        }
                        node["name"] = properties.get("normalized_name", node["name"])
                        node["normalized"]["name"] = node["name"]
                return result

        limit = max_nodes
        result = await run_nebula_query_io(
            self._query_nodes_sync,
            effective_kb_id,
            keyword,
            limit,
            max_depth,
            exclude_chunk,
        )
        return await self._filter_visible_subgraph(effective_kb_id, result)

    def _rows(self, ngql: str) -> list[dict[str, Any]]:
        """执行 nGQL 并返回字典行。"""
        return result_rows(self.connection.execute(ngql))

    def _subgraph_ngql(self, kb_id: str, seeds: list[str], max_depth: int, path_filters: list[str], path_limit: int) -> str:
        """按种子顶点展开变长路径；顶点属性只在 tag 限定下可读。"""
        conditions = [f"id(seed) IN {ngql_list(seeds)}", f"all(x IN nodes(p) WHERE x.Node.kb_id == {ngql_literal(kb_id)})"]
        conditions.extend(path_filters)
        where = " AND ".join(conditions)
        return f"""
        MATCH p=(seed:Node)-[*1..{max_depth}]-(m:Node)
        WHERE {where}
        RETURN [x IN nodes(p) | id(x)] AS node_ids,
               [x IN nodes(p) | properties(x)] AS node_props,
               [r IN relationships(p) | [src(r), dst(r), type(r), rank(r), properties(r)]] AS edges
        LIMIT {max(path_limit, 1)}
        """

    def _subgraph_from_ngql(self, ngql: str, limit: int) -> dict[str, Any]:
        """把多行路径结果去重并裁成前端契约的 {nodes, edges}。"""
        raw_nodes: dict[str, dict[str, Any]] = {}
        raw_edges: list[dict[str, Any]] = []
        for row in self._rows(ngql):
            for node_id, props in zip(row.get("node_ids") or [], row.get("node_props") or [], strict=True):
                properties = props or {}
                raw_nodes.setdefault(node_id, {"id": node_id, "labels": [properties.get("kind")], "properties": properties})
            for source, target, edge_type, rank, props in row.get("edges") or []:
                properties = props or {}
                raw_edges.append(
                    {
                        "id": f"{source}|{target}|{rank}",
                        "source_id": source,
                        "target_id": target,
                        # 关系边对外暴露业务关系类型，与原 properties.type 口径一致
                        "type": properties.get("rel_type") or edge_type,
                        "properties": properties,
                    }
                )
        nodes = [self._normalize_node(node) for node in raw_nodes.values()]
        edges = [self._normalize_edge(edge) for edge in raw_edges]
        return self._finalize_subgraph_result(nodes, edges, limit)

    def _query_file_nodes_sync(
        self,
        kb_id: str,
        keyword: str,
        limit: int,
        max_depth: int,
        exclude_chunk: bool,
        entity_mentions: list[dict[str, str]],
        triple_mentions: list[dict[str, str]],
        chunk_ids: list[str],
    ) -> dict[str, Any]:
        """先按 PG 来源约束路径，再限制结果数量。"""
        entity_ids = list({m["id"] for m in entity_mentions})
        entity_vids = [entity_vid(kb_id, entity_id) for entity_id in entity_ids]
        scope = [self._in_list("n.Node.chunk_id", chunk_ids)]
        path_scope = [self._in_list("x.Node.chunk_id", chunk_ids)]
        if not exclude_chunk:
            scope.insert(0, self._in_list("n.Node.entity_id", entity_ids))
            scope = ["(" + " OR ".join(scope) + ")"]
            path_scope.insert(0, self._in_list("x.Node.entity_id", entity_ids))
            path_scope = ["(" + " OR ".join(path_scope) + ")"]
        else:
            scope = [self._in_list("n.Node.entity_id", entity_ids)]
            path_scope = [self._in_list("x.Node.entity_id", entity_ids)]

        seeds = self._seed_vids(kb_id, keyword, limit, scope)
        if max_depth <= 0:
            return self._finalize_subgraph_result(seeds[:limit], [], limit)
        if not seeds:
            return self._empty_subgraph()
        mention_filter = (
            f'(type(r) == "MENTIONS" AND {self._in_list("r.chunk_id", chunk_ids)} AND {self._in_list("dst(r)", entity_vids)})'
            f' OR (type(r) == "RELATION" AND {self._in_list("r.chunk_id", chunk_ids)}'
            f' AND {self._in_list("r.triple_id", [m["id"] for m in triple_mentions])})'
        )
        path_filters = ["all(x IN nodes(p) WHERE " + " AND ".join(path_scope) + ")", f"all(r IN relationships(p) WHERE {mention_filter})"]
        seed_vids = [seed["id"] for seed in seeds]
        ngql = self._subgraph_ngql(kb_id, seed_vids, min(max_depth, 3), path_filters, max(limit, 1) * 10)
        result = self._subgraph_from_ngql(ngql, limit)
        result["nodes"].sort(key=lambda node: seed_vids.index(node["id"]) if node["id"] in seed_vids else len(seed_vids))
        return result

    @staticmethod
    def _in_list(column: str, values: list[str]) -> str:
        """空列表不能用 `IN []`，退化为恒假条件。"""
        return f"{column} IN {ngql_list(values)}" if values else "false"

    def _seed_vids(self, kb_id: str, keyword: str, limit: int, scope: list[str]) -> list[dict[str, Any]]:
        """按关键字与作用域选种子顶点，返回 vid 与属性。"""
        conditions = [f"n.Node.kb_id == {ngql_literal(kb_id)}", *scope, *self._keyword_conditions(keyword)]
        ngql = (
            "MATCH (n:Node) WHERE " + " AND ".join(conditions) + "\n"
            "RETURN id(n) AS id, properties(n) AS props\n"
            f"LIMIT {max(int(limit), 1)}"
        )
        return [
            self._normalize_node({"id": row["id"], "labels": [(row["props"] or {}).get("kind")], "properties": row["props"] or {}})
            for row in self._rows(ngql)
        ]

    @staticmethod
    def _keyword_conditions(keyword: str) -> list[str]:
        """关键字命中名字、预览或 chunk 标识。"""
        if not keyword or keyword == "*":
            return []
        value = ngql_literal(keyword.lower())
        columns = ("name", "normalized_name", "content_preview", "chunk_id")
        return ["(" + " OR ".join(f"toLower(coalesce(n.Node.{column}, \"\")) CONTAINS {value}" for column in columns) + ")"]

    def _empty_subgraph(self) -> dict[str, Any]:
        return {"nodes": [], "edges": []}

    def _query_nodes_sync(
        self,
        kb_id: str,
        keyword: str,
        limit: int,
        max_depth: int,
        exclude_chunk: bool,
    ) -> dict[str, Any]:
        scope = ["n.Node.kind != \"Chunk\""] if exclude_chunk else []
        seeds = self._seed_vids(kb_id, keyword, limit, scope)
        if max_depth <= 0:
            return self._finalize_subgraph_result(seeds, [], limit)
        if not seeds:
            return self._empty_subgraph()
        path_filters = ["all(x IN nodes(p) WHERE x.Node.kind != \"Chunk\")"] if exclude_chunk else []
        ngql = self._subgraph_ngql(kb_id, [seed["id"] for seed in seeds], min(max_depth, 3), path_filters, max(limit, 1) * 10)
        result = self._subgraph_from_ngql(ngql, limit)
        # 种子必须排在扩展结果之前，前端按此顺序渲染
        seed_ids = [seed["id"] for seed in seeds]
        order = {vid: index for index, vid in enumerate(seed_ids)}
        result["nodes"].sort(key=lambda node: order.get(node["id"], len(order)))
        return result

    async def query_seed_subgraph(
        self,
        kb_id: str,
        *,
        entity_ids: list[str],
        max_nodes: int,
    ) -> dict[str, Any]:
        if not entity_ids:
            return {"nodes": [], "edges": []}
        seed_entity_ids = list(dict.fromkeys(entity_ids))
        seeds = [entity_vid(kb_id, entity_id) for entity_id in seed_entity_ids]
        ngql = self._subgraph_ngql(kb_id, seeds, 2, [], max(max_nodes, 1) * 4)
        result = self._subgraph_from_ngql(ngql, max_nodes)
        return await self._filter_visible_subgraph(kb_id, result)

    async def query_and_rank_chunks_by_ppr(
        self,
        kb_id: str,
        seed_weights: dict[str, float],
        *,
        max_nodes: int,
        top_k: int,
        damping: float,
    ) -> list[tuple[str, float]]:
        if not seed_weights:
            return []
        subgraph = await self.query_seed_subgraph(
            kb_id,
            entity_ids=list(seed_weights.keys()),
            max_nodes=max_nodes,
        )
        return self.rank_chunks_by_ppr(subgraph, seed_weights, top_k=top_k, damping=damping)

    @staticmethod
    def rank_chunks_by_ppr(
        subgraph: dict[str, Any],
        seed_weights: dict[str, float],
        *,
        top_k: int,
        damping: float,
    ) -> list[tuple[str, float]]:
        nodes = subgraph.get("nodes") or []
        edges = subgraph.get("edges") or []
        if not nodes:
            return []

        import networkx as nx

        node_ids = [node["id"] for node in nodes]
        index_by_id = {node_id: index for index, node_id in enumerate(node_ids)}
        edge_indices = [
            (index_by_id[edge["source_id"]], index_by_id[edge["target_id"]])
            for edge in edges
            if edge.get("source_id") in index_by_id and edge.get("target_id") in index_by_id
        ]
        if not edge_indices:
            return []

        # networkx 节点 id 直接用节点索引（与 igraph 的 reset 向量语义对齐）。
        graph = nx.Graph()
        graph.add_nodes_from(range(len(nodes)))
        graph.add_edges_from(edge_indices)
        reset = [0.0] * len(nodes)
        chunk_node_indexes: list[tuple[int, str]] = []
        for index, node in enumerate(nodes):
            properties = node.get("properties") or {}
            if node.get("type") == "Chunk" and properties.get("chunk_id"):
                chunk_node_indexes.append((index, properties["chunk_id"]))
                continue
            entity_id = properties.get("entity_id")
            if entity_id in seed_weights:
                reset[index] = seed_weights[entity_id]

        reset_total = sum(reset)
        if reset_total <= 0 or not chunk_node_indexes:
            return []
        reset = [value / reset_total for value in reset]
        # networkx 的 personalization 必须覆盖全部节点（缺节点会抛错），0 值节点贡献为 0；
        # 默认 dangling=None 时跳转分布使用 personalization，与 igraph 的 reset 语义一致。
        personalization = {index: value for index, value in enumerate(reset)}
        scores = nx.pagerank(graph, alpha=min(max(damping, 0.1), 0.99), personalization=personalization)
        ranked = sorted(
            ((chunk_id, float(scores[index])) for index, chunk_id in chunk_node_indexes),
            key=lambda item: item[1],
            reverse=True,
        )
        return ranked[:top_k]

    async def get_labels(self, kb_id: str | None = None) -> list[str]:
        effective_kb_id = kb_id or self.kb_id
        if not effective_kb_id:
            return []
        rows = self._rows(
            f"MATCH (n:Node) WHERE n.Node.kb_id == {ngql_literal(effective_kb_id)} RETURN DISTINCT n.Node.kind AS kind"
        )
        return sorted({row["kind"] for row in rows if row["kind"]})

    async def get_stats(self, kb_id: str | None = None) -> dict[str, Any]:
        effective_kb_id = kb_id or self.kb_id
        if not effective_kb_id:
            return {"total_nodes": 0, "total_edges": 0, "entity_types": []}
        return await run_nebula_query_io(self._get_stats_sync, ngql_literal(effective_kb_id))

    def _get_stats_sync(self, kb_literal: str) -> dict[str, Any]:
        node_rows = self._rows(f"MATCH (n:Node) WHERE n.Node.kb_id == {kb_literal} RETURN count(n) AS node_count")
        edge_rows = self._rows(
            f"MATCH (a:Node)-[r:MENTIONS|RELATION]->(b:Node) WHERE a.Node.kb_id == {kb_literal} RETURN count(r) AS edge_count"
        )
        label_rows = self._rows(
            f"MATCH (n:Node) WHERE n.Node.kb_id == {kb_literal} AND n.Node.kind == \"Entity\" "
            "RETURN n.Node.label AS entity_label, count(*) AS count"
        )
        entity_types = sorted(
            ({"type": row["entity_label"], "count": row["count"]} for row in label_rows if row["entity_label"]),
            key=lambda item: item["count"],
            reverse=True,
        )
        return {
            "total_nodes": node_rows[0]["node_count"] if node_rows else 0,
            "total_edges": edge_rows[0]["edge_count"] if edge_rows else 0,
            "entity_types": entity_types,
        }

    # ─── 运维：重置、向量对账与删除 ───────────────────────────────

    async def reset(self, kb_id: str, *, clear_extraction_result: bool, clear_config: bool) -> dict[str, Any]:
        """重置与所有投影写入互斥。"""
        async with knowledge_projection_lock(kb_id, shared=False):
            return await self._reset(kb_id, clear_extraction_result=clear_extraction_result, clear_config=clear_config)

    async def _reset(self, kb_id: str, *, clear_extraction_result: bool, clear_config: bool) -> dict[str, Any]:
        """删除图谱后重置知识库构建状态。"""
        await DocumentResetRepository().require_processable(kb_id)
        kb = await self._get_milvus_kb(kb_id)
        await await_io(asyncio.to_thread(self.delete_graph, kb_id))
        await self.graph_repo.delete_by_kb_id(kb_id)
        reset_chunks = await self.chunk_repo.reset_graph_state_by_kb_id(kb_id, clear_extraction_result)
        if clear_config:
            additional_params = dict(kb.additional_params or {})
            additional_params.pop(GRAPH_CONFIG_KEY, None)
            await self.kb_repo.update(kb_id, {"additional_params": additional_params})
        return {
            "message": "图谱构建状态已重置",
            "status": "success",
            "reset_chunks": reset_chunks,
            "clear_extraction_result": clear_extraction_result,
            "clear_config": clear_config,
        }

    async def reconcile_vectors(self, kb_id: str, *, all_vectors: bool) -> dict[str, Any]:
        """向量重置与构建、删除互斥。"""
        async with knowledge_projection_lock(kb_id, shared=False):
            return await self._reconcile_vectors(kb_id, all_vectors=all_vectors)

    async def _reconcile_vectors(self, kb_id: str, *, all_vectors: bool) -> dict[str, Any]:
        """执行已有图谱向量重置用例。"""
        await DocumentResetRepository().require_processable(kb_id)
        await self._get_milvus_kb(kb_id)
        reset_records = await self.graph_repo.reconcile_vector_records(kb_id, all_vectors=all_vectors)
        if all_vectors:
            graph_vector_store = await self.get_graph_vector_store()
            await await_io(asyncio.to_thread(graph_vector_store.drop_graph_collections, kb_id))
        return {
            "kb_id": kb_id,
            "mode": "all_vectors" if all_vectors else "failed",
            "reset_records": reset_records,
        }

    def delete_graph(self, kb_id: str) -> None:
        """删除知识库全部顶点与边；空图直接跳过。"""
        kb = ngql_literal(kb_id)
        if not self._rows(f"MATCH (n:Node) WHERE n.Node.kb_id == {kb} RETURN count(n) AS c")[0]["c"]:
            self.graph_vector_store.drop_graph_collections(kb_id)
            return
        self.connection.execute(
            f"LOOKUP ON Node WHERE Node.kb_id == {kb} YIELD id(vertex) AS vid | DELETE VERTEX $-.vid WITH EDGE"
        )
        self.graph_vector_store.drop_graph_collections(kb_id)

    async def clear_file_graph(self, kb_id: str, file_id: str, *, generation: int, context=None) -> dict[str, Any]:
        """撤销文件全部图谱产物，保留文件、正文、分块和普通检索索引。"""
        async with self.graph_repo.file_clear_scope(kb_id, file_id, generation):
            # 排他锁可能等待超过 attempt 租约；必须在获得写权限之后重新确认 Owner。
            if context is not None:
                await context.raise_if_cancelled()
            await DocumentResetRepository().require_processable(kb_id, file_id, job_id=getattr(context, "job_id", None))
            await self._get_milvus_kb(kb_id)
            await self.delete_file_graph(kb_id, file_id, clear_extraction_result=True)
        return {"kb_id": kb_id, "file_id": file_id, "status": "cleared"}

    async def delete_file_graph(
        self, kb_id: str, file_id: str, *, generation: int | None = None, clear_extraction_result: bool = False
    ) -> None:
        """外部清理成功后才提交关联移除，失败保留重试所需的 ID。"""
        chunk_ids = None
        if generation is not None:
            chunk_ids = await self.chunk_repo.list_ids_by_file_generation(file_id, generation)
        affected_entities, affected_triples = await await_io(asyncio.to_thread(self._file_graph_record_ids, kb_id, file_id, chunk_ids))
        # 历史孤儿已无 PG 提及；先提交其撤销，保留图库归属作为后续失败重试的范围证据。
        await self.graph_repo.delete_orphan_records(entity_ids=affected_entities, triple_ids=affected_triples)
        sources = await self.graph_repo.file_entity_sources(file_id, generation=generation, additional_entity_ids=affected_entities)
        restored_by_id = {}
        for source in sources:
            entity = restored_by_id.setdefault(
                source["entity_id"],
                {
                    "entity_id": source["entity_id"],
                    "name": source["normalized_name"],
                    "attributes": [],
                    "source_chunk_id": None,
                },
            )
            if entity["source_chunk_id"] is not None:
                continue
            if source["extraction_result"]:
                payload = build_graph_payload(source["extraction_result"])
                records = self._build_entity_records(kb_id, payload["entities"])
                for record in records:
                    if record["entity_id"] == source["entity_id"]:
                        entity.update(name=record["name"], attributes=record["attributes"], source_chunk_id=source["chunk_id"])
                        break
        restored_entities = list(restored_by_id.values())
        options = {
            "generation": generation,
            "restored_entities": restored_entities,
            "additional_entity_ids": affected_entities,
            "additional_triple_ids": affected_triples,
        }
        if clear_extraction_result:
            options["clear_extraction_result"] = True
        async with self.graph_repo.delete_file_references(file_id, **options) as scope:
            orphan_entity_ids, orphan_triple_ids, chunk_ids = scope
            graph_vector_store = await self.get_graph_vector_store()
            await await_io(
                graph_vector_store.delete_graph_records(
                    kb_id,
                    entity_ids=orphan_entity_ids,
                    triple_ids=orphan_triple_ids,
                )
            )
            # 全文件清理也覆盖历史上已经失去 PG Chunk 的图半成品。
            graph_chunks = chunk_ids if generation is not None else None
            await await_io(asyncio.to_thread(self._delete_file_graph_from_graph, kb_id, file_id, graph_chunks, restored_entities))

    def _file_graph_record_ids(self, kb_id: str, file_id: str, chunk_ids: list[str] | None) -> tuple[list[str], list[str]]:
        """读取文件可归属的实体和三元组，补足 PG 提及丢失的半成品。"""
        kb = ngql_literal(kb_id)
        file_ref = ngql_literal(file_id)
        chunk_scope = f" AND {self._in_list('c.Node.chunk_id', chunk_ids)}" if chunk_ids is not None else ""
        entity_rows = self._rows(
            f"MATCH (c:Node)-[:MENTIONS]->(e:Node) WHERE c.Node.kb_id == {kb} AND c.Node.file_id == {file_ref}{chunk_scope} "
            "RETURN DISTINCT e.Node.entity_id AS id"
        )
        relation_scope = f" AND {self._in_list('r.chunk_id', chunk_ids)}" if chunk_ids is not None else ""
        triple_rows = self._rows(
            f"MATCH (a:Node)-[r:RELATION]->(b:Node) WHERE r.kb_id == {kb} AND r.file_id == {file_ref}{relation_scope} "
            "RETURN DISTINCT r.triple_id AS id"
        )
        return (
            [row["id"] for row in entity_rows if row["id"]],
            [row["id"] for row in triple_rows if row["id"]],
        )

    def _delete_file_graph_from_graph(
        self, kb_id: str, file_id: str, chunk_ids: list[str] | None, restored_entities: list[dict] | None = None
    ) -> None:
        """删掉文件的关系边、提及边、chunk 顶点，并清理失去全部提及的实体。"""
        kb = ngql_literal(kb_id)
        file_ref = ngql_literal(file_id)
        chunk_scope = f" AND {self._in_list('r.chunk_id', chunk_ids)}" if chunk_ids is not None else ""

        for row in self._rows(
            f"MATCH (a:Node)-[r:RELATION]->(b:Node) WHERE r.kb_id == {kb} AND r.file_id == {file_ref}{chunk_scope} "
            "RETURN src(r) AS source, dst(r) AS target, rank(r) AS rank"
        ):
            self.connection.execute(
                f"DELETE EDGE RELATION {ngql_literal(row['source'])}->{ngql_literal(row['target'])}@{row['rank']}"
            )

        mention_scope = f" AND {self._in_list('r.chunk_id', chunk_ids)}" if chunk_ids is not None else ""
        mention_rows = self._rows(
            f"MATCH (c:Node)-[r:MENTIONS]->(e:Node) WHERE c.Node.kb_id == {kb} AND c.Node.file_id == {file_ref}{mention_scope} "
            "RETURN src(r) AS source, dst(r) AS target, rank(r) AS rank, dst(r) AS entity"
        )
        for row in mention_rows:
            self.connection.execute(
                f"DELETE EDGE MENTIONS {ngql_literal(row['source'])}->{ngql_literal(row['target'])}@{row['rank']}"
            )

        # 只剩孤立实体时才删顶点，仍被其他 chunk 提及的实体必须保留
        candidates = {row["entity"] for row in mention_rows}
        if candidates:
            still_mentioned = {
                row["entity"]
                for row in self._rows(
                    f"MATCH (e:Node)<-[:MENTIONS]-(c:Node) WHERE {self._in_list('id(e)', sorted(candidates))} "
                    "RETURN DISTINCT id(e) AS entity"
                )
            }
            orphans = sorted(candidates - still_mentioned)
            if orphans:
                self.connection.execute("DELETE VERTEX " + ", ".join(ngql_literal(vid) for vid in orphans) + " WITH EDGE")

        chunk_filter = self._in_list("n.Node.chunk_id", chunk_ids) if chunk_ids is not None else "true"
        chunk_vids = [
            row["vid"]
            for row in self._rows(
                f'MATCH (n:Node) WHERE n.Node.kb_id == {kb} AND n.Node.file_id == {file_ref} AND n.Node.kind == "Chunk" '
                f"AND {chunk_filter} RETURN id(n) AS vid"
            )
            if row["vid"]
        ]
        if chunk_vids:
            self.connection.execute("DELETE VERTEX " + ", ".join(ngql_literal(vid) for vid in chunk_vids) + " WITH EDGE")

        for entity in restored_entities or []:
            self.connection.execute(
                f"UPDATE VERTEX ON Node {ngql_literal(entity_vid(kb_id, entity['entity_id']))} SET "
                f"Node.name = {ngql_literal(entity['name'])}, "
                f"Node.attributes = {ngql_literal(json.dumps(entity['attributes'], ensure_ascii=False))}, "
                f"Node.source_chunk_id = {ngql_literal(entity['source_chunk_id']) if entity['source_chunk_id'] else 'NULL'}"
            )

    # ─── 内部辅助：校验、投影与 Cypher/结果处理 ────────────────────

    async def _get_milvus_kb(self, kb_id: str):
        kb = await self.kb_repo.get_by_kb_id(kb_id)
        if kb is None:
            raise ValueError(f"知识库 {kb_id} 不存在")
        if (kb.kb_type or "").lower() != "milvus":
            raise ValueError("仅 Milvus 知识库支持独立图谱构建")
        return kb

    def _get_locked_config(self, additional_params: dict[str, Any]) -> dict[str, Any]:
        config = additional_params.get(GRAPH_CONFIG_KEY) or {}
        if not config.get("locked"):
            raise ValueError("请先确认并锁定图谱抽取配置")
        if not config.get("extractor_type"):
            raise ValueError("图谱抽取配置缺少 extractor_type")
        return config

    def _public_config(self, config: dict[str, Any]) -> dict[str, Any] | None:
        if not config:
            return None
        return {
            "locked": bool(config.get("locked")),
            "extractor_type": config.get("extractor_type"),
            "extractor_options": self._runtime_extractor_options(config),
            "created_at": config.get("created_at"),
            "created_by": config.get("created_by"),
            "updated_at": config.get("updated_at"),
            "updated_by": config.get("updated_by"),
        }

    @staticmethod
    def _project_build_job(latest_job: Any) -> tuple[str | None, int]:
        """把最近一次构建任务投影为前端状态语义；未知状态按无任务处理。"""
        if latest_job.status in {"pending", "running"}:
            return latest_job.status, round(latest_job.progress)
        if latest_job.status == "success":
            return "completed", 100
        if latest_job.status in {"failed", "cancelled"}:
            return "failed", 0
        return None, 0

    async def _filter_visible_subgraph(self, kb_id: str, result: dict[str, Any]) -> dict[str, Any]:
        """图库尚未完成清理时，也不能发布 PG 已不可见的内容。"""
        chunk_ids = {n["properties"]["chunk_id"] for n in result["nodes"] if n["properties"].get("chunk_id")}
        chunk_ids.update(e["properties"]["chunk_id"] for e in result["edges"] if e["properties"].get("chunk_id"))
        chunk_ids.update(n["properties"]["source_chunk_id"] for n in result["nodes"] if n["properties"].get("source_chunk_id"))
        entity_ids = {n["properties"]["entity_id"] for n in result["nodes"] if n["properties"].get("entity_id")}
        visible_chunks, visible_entities = await self.graph_repo.visible_graph_ids(kb_id, list(chunk_ids), list(entity_ids))
        nodes = [
            n
            for n in result["nodes"]
            if (n["properties"].get("chunk_id") in visible_chunks or n["properties"].get("entity_id") in visible_entities)
        ]
        for node in nodes:
            properties = node["properties"]
            if properties.get("entity_id") and properties.get("source_chunk_id") not in visible_chunks:
                node["properties"] = {
                    key: properties[key] for key in ("entity_id", "normalized_name", "label", "kb_id") if key in properties
                }
                node["name"] = properties["normalized_name"]
                node["normalized"]["name"] = node["name"]
        node_ids = {n["id"] for n in nodes}
        edges = [
            e
            for e in result["edges"]
            if (e["source_id"] in node_ids and e["target_id"] in node_ids and e["properties"].get("chunk_id") in visible_chunks)
        ]
        return {"nodes": nodes, "edges": edges}

    @staticmethod
    def _finalize_subgraph_result(nodes: list[dict[str, Any]], edges: list[dict[str, Any]], limit: int) -> dict[str, Any]:
        limit = max(0, limit)
        final_nodes = nodes[:limit]
        node_ids = {node["id"] for node in final_nodes}
        final_edges = [edge for edge in edges if edge.get("source_id") in node_ids and edge.get("target_id") in node_ids]
        return {"nodes": final_nodes, "edges": final_edges[: limit * 2]}

    def _normalize_node(self, raw_node: Any, kb_id: str | None = None) -> dict[str, Any]:
        if not isinstance(raw_node, dict):
            return {}
        node_id = raw_node.get("id")
        labels = raw_node.get("labels") or []
        properties = raw_node.get("properties") or {}

        effective_kb_id = kb_id or self.kb_id
        kb_label = properties.get("kb_id") or effective_kb_id
        filtered_labels = [label for label in labels if label not in {"MilvusKB", kb_label}]
        entity_type = "Chunk" if "Chunk" in labels else properties.get("label", "Entity")
        name = properties.get("name") or properties.get("content_preview") or properties.get("chunk_id") or "Unknown"
        return {
            "id": node_id,
            "name": name,
            "original_id": node_id,
            "type": entity_type,
            "labels": filtered_labels,
            "properties": properties,
            "normalized": {
                "name": name,
                "type": entity_type,
                "source": "milvus",
            },
            "graph_type": "milvus",
        }

    def _normalize_edge(self, raw_edge: Any) -> dict[str, Any]:
        if not isinstance(raw_edge, dict):
            return {}
        edge_id = raw_edge.get("id")
        edge_type = raw_edge.get("type")
        source_id = raw_edge.get("source_id")
        target_id = raw_edge.get("target_id")
        properties = raw_edge.get("properties", {})

        return {
            "id": edge_id,
            "source_id": source_id,
            "target_id": target_id,
            "type": edge_type,
            "properties": properties,
            "normalized": {
                "type": edge_type,
                "direction": "directed",
            },
        }
