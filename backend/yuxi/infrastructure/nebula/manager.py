"""NebulaGraph 连接池、语句转义与 schema 引导。

nGQL 不支持绑定参数（nebula3-python 的 `execute_parameter` 在本版本会断连），
因此所有取值都经 `ngql_literal` 转义后拼进语句；这是唯一的注入边界。
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from typing import Any

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
from yuxi.infrastructure.observability.logging import logger

# 顶点主键长度上限；vid = "<kb_id>:<业务 ID>"。
VID_LENGTH = 64

TAG_SCHEMA = """
CREATE TAG IF NOT EXISTS Node(
    kind string,
    kb_id string,
    file_id string,
    chunk_id string,
    chunk_index int,
    content_preview string,
    start_char_pos int,
    end_char_pos int,
    entity_id string,
    name string,
    normalized_name string,
    label string,
    attributes string,
    source_chunk_id string
)
"""

EDGE_SCHEMAS = (
    "CREATE EDGE IF NOT EXISTS MENTIONS(chunk_id string, file_id string, kb_id string)",
    "CREATE EDGE IF NOT EXISTS RELATION(kb_id string, chunk_id string, triple_id string, rel_type string,"
    " text string, source_name string, target_name string, file_id string, extractor_type string)",
)

TAG_INDEXES = (
    "CREATE TAG INDEX IF NOT EXISTS node_kb ON Node(kb_id(64))",
    "CREATE TAG INDEX IF NOT EXISTS node_kind ON Node(kind(16))",
)

_INDEX_NAMES = ("node_kb", "node_kind")
_DDL_TIMEOUT_SECONDS = 120
_shared_nebula_connection: NebulaConnectionManager | None = None
_shared_nebula_connection_lock = threading.Lock()


def ngql_literal(value: Any) -> str:
    """把 Python 值转成 nGQL 字面量；字符串走转义，其余按数值/布尔直出。"""
    if value is None:
        raise ValueError("nGQL 字面量不接受 None")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    text = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
    return f'"{text}"'


def ngql_list(values: list[Any]) -> str:
    """把列表转成 nGQL 列表字面量。"""
    return "[" + ", ".join(ngql_literal(value) for value in values) + "]"


def value_to_python(value: Any) -> Any:
    """把 Nebula ValueWrapper 转成普通 Python 值。"""
    if value is None or value.is_null():
        return None
    if value.is_bool():
        return value.as_bool()
    if value.is_int():
        return value.as_int()
    if value.is_double():
        return value.as_double()
    if value.is_string():
        return value.as_string()
    if value.is_list():
        return [value_to_python(item) for item in value.as_list()]
    if value.is_map():
        return {key: value_to_python(item) for key, item in value.as_map().items()}
    return str(value)


def result_rows(result: Any) -> list[dict[str, Any]]:
    """把 ResultSet 转成按列名取值的字典列表。"""
    keys = list(result.keys())
    return [{key: value_to_python(value) for key, value in zip(keys, result.row_values(index), strict=True)} for index in range(result.row_size())]


class NebulaConnectionManager:
    """持有连接池并在首次连接时幂等补齐 space / tag / edge / index。"""

    def __init__(self):
        self.pool: ConnectionPool | None = None
        self.status = "closed"
        self.space = os.environ.get("NEBULA_SPACE", "yuxi")
        self._schema_ready = False
        self._schema_lock = threading.Lock()
        self._connect()

    def is_running(self) -> bool:
        """报告管理器是否处于可用或处理中状态。"""
        return self.status == "open" or self.status == "processing"

    def close(self) -> None:
        """关闭连接池并回到未连接状态。"""
        if self.pool:
            self.pool.close()
            self.pool = None
            self._schema_ready = False
            self.status = "closed"

    @contextmanager
    def session(self):
        """借出一个 session，退出时归还连接池。"""
        if self.pool is None:
            raise ConnectionError("NebulaGraph 连接池未建立")
        session = self.pool.get_session(
            os.environ.get("NEBULA_USER", "root"),
            os.environ.get("NEBULA_PASSWORD", "nebula"),
        )
        try:
            yield session
        finally:
            session.release()

    def execute(self, ngql: str) -> Any:
        """在当前 space 执行一条 nGQL，失败即抛错。"""
        with self.session() as session:
            if not session.execute(f"USE `{self.space}`").is_succeeded():
                raise ConnectionError(f"NebulaGraph space 不可用: {self.space}")
            result = session.execute(ngql)
            if not result.is_succeeded():
                raise RuntimeError(f"nGQL 执行失败: {result.error_msg()} | {ngql[:200]}")
            return result

    def _connect(self) -> None:
        """建池、探活并补齐 schema，失败向上抛出。"""
        hosts = os.environ.get("NEBULA_HOSTS", "localhost:9669")
        cfg = Config()
        cfg.max_connection_pool_size = int(os.environ.get("NEBULA_POOL_SIZE", "10"))
        pool = ConnectionPool()
        addresses = []
        for entry in hosts.split(","):
            host, _, port = entry.strip().partition(":")
            addresses.append((host, int(port or 9669)))
        if not pool.init(addresses, cfg):
            raise ConnectionError(f"NebulaGraph 连接失败: {hosts}")

        self.pool = pool
        try:
            self._ensure_schema()
        except Exception:
            self.close()
            raise
        self.status = "open"
        logger.info(f"Successfully connected to NebulaGraph space {self.space}")

    def _ensure_schema(self) -> None:
        """按 storaged 注册 → space → tag/edge → index 的顺序幂等引导。"""
        with self._schema_lock:
            if self._schema_ready:
                return
            storage_host = os.environ.get("NEBULA_STORAGE_HOST", "")
            if storage_host:
                self._add_hosts(storage_host)
            self._create_space()
            self._create_schema()
            self._await_schema_visible()
            self._schema_ready = True

    def _add_hosts(self, storage_host: str) -> None:
        """注册 storaged 并等待 ONLINE；已注册的忽略。"""
        host, _, port = storage_host.partition(":")
        session = self.pool.get_session("root", os.environ.get("NEBULA_PASSWORD", "nebula"))
        try:
            result = session.execute(f'ADD HOSTS "{host}":{port or 9779}')
            if not result.is_succeeded() and "Existed" not in result.error_msg():
                raise RuntimeError(f"ADD HOSTS 失败: {result.error_msg()}")
            deadline = time.monotonic() + _DDL_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                hosts = session.execute("SHOW HOSTS")
                statuses = [row["Status"] for row in result_rows(hosts)] if hosts.is_succeeded() else []
                if statuses and "OFFLINE" not in statuses:
                    return
                time.sleep(1.0)
            raise RuntimeError("storaged 未在超时内变为 ONLINE")
        finally:
            session.release()

    def _create_space(self) -> None:
        """建 space 并等待其可 USE。"""
        with self.session() as session:
            statement = (
                f"CREATE SPACE IF NOT EXISTS `{self.space}`"
                f"(vid_type=FIXED_STRING({VID_LENGTH}), partition_num=10, replica_factor=1)"
            )
            result = session.execute(statement)
            if not result.is_succeeded():
                raise RuntimeError(f"创建 space 失败: {result.error_msg()}")
            self._await(session, f"USE `{self.space}`", "space 未就绪")

    def _create_schema(self) -> None:
        """建 tag/edge/index 并等待 DDL 生效。"""
        with self.session() as session:
            session.execute(f"USE `{self.space}`")
            for statement in (TAG_SCHEMA, *EDGE_SCHEMAS):
                result = session.execute(statement)
                if not result.is_succeeded():
                    raise RuntimeError(f"DDL 失败: {result.error_msg()} | {statement[:80]}")
            self._await(session, "DESCRIBE TAG Node", "tag Node 未就绪")
            for statement in TAG_INDEXES:
                result = session.execute(statement)
                if not result.is_succeeded():
                    raise RuntimeError(f"索引 DDL 失败: {result.error_msg()}")
            self._rebuild_indexes(session)
            self._await_indexes(session)

    @staticmethod
    def _rebuild_indexes(session: Any) -> None:
        """轮询重建索引：索引要等 schema 同步到 meta 后才能被重建。"""
        statement = f"REBUILD TAG INDEX {', '.join(_INDEX_NAMES)}"
        deadline = time.monotonic() + _DDL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if session.execute(statement).is_succeeded():
                return
            time.sleep(1.0)
        raise RuntimeError("索引未在超时内可重建")

    def _await_schema_visible(self) -> None:
        """等新会话也能看到 schema：DDL 在会话间有同步延迟。"""
        deadline = time.monotonic() + _DDL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            with self.session() as session:
                if session.execute("DESCRIBE TAG Node").is_succeeded() and session.execute("DESCRIBE EDGE MENTIONS").is_succeeded():
                    return
            time.sleep(1.0)
        raise RuntimeError("图 schema 未在超时内对新会话可见")

    @staticmethod
    def _await(session: Any, statement: str, message: str) -> None:
        """轮询直到语句成功；DDL 是异步的。"""
        deadline = time.monotonic() + _DDL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if session.execute(statement).is_succeeded():
                return
            time.sleep(1.0)
        raise RuntimeError(message)

    @staticmethod
    def _await_indexes(session: Any) -> None:
        """等待每个索引各自出现 FINISHED 的重建结果。"""
        deadline = time.monotonic() + _DDL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            result = session.execute("SHOW TAG INDEX STATUS")
            finished: set[str] = set()
            if result.is_succeeded():
                for row in result_rows(result):
                    if row["Index Status"] == "FINISHED":
                        finished.update(row["Name"].split(","))
            if all(name in finished for name in _INDEX_NAMES):
                return
            time.sleep(1.0)
        raise RuntimeError("tag 索引未在超时内完成")


def get_shared_nebula_connection() -> NebulaConnectionManager:
    """返回进程共享连接，池缺失时在锁内重建。"""
    global _shared_nebula_connection
    if _shared_nebula_connection is None or not _shared_nebula_connection.pool:
        with _shared_nebula_connection_lock:
            if _shared_nebula_connection is None or not _shared_nebula_connection.pool:
                _shared_nebula_connection = NebulaConnectionManager()
    return _shared_nebula_connection


def close_shared_nebula_connection() -> None:
    """关闭进程共享连接。"""
    with _shared_nebula_connection_lock:
        if _shared_nebula_connection is not None:
            _shared_nebula_connection.close()
