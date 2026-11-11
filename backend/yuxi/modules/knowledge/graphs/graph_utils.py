"""图谱构建相关的纯函数工具集。

将数据变换逻辑从 KnowledgeGraphService 中抽离，
使 service 类专注于 I/O 和业务编排。
"""

from __future__ import annotations

import hashlib
from typing import Any

from yuxi.infrastructure.nebula import ngql_literal
from yuxi.shared.hashing import hashstr


def normalize_entity_name(text: str) -> str:
    """统一实体名称：去首尾空白、小写化、压缩内部连续空白。"""
    return " ".join(text.strip().lower().split())


def compute_entity_id(kb_id: str, normalized_name: str, label: str) -> str:
    """按库、归一名与标签生成稳定实体 ID。"""
    return hashstr(f"{kb_id}:{normalized_name}:{label}", length=32)


def compute_triple_id(
    kb_id: str,
    source_normalized_name: str,
    source_label: str,
    relation_type: str,
    target_normalized_name: str,
    target_label: str,
) -> str:
    """按库、两端实体与关系类型生成稳定三元组 ID。"""
    return hashstr(
        f"{kb_id}:{source_normalized_name}:{source_label}:{relation_type}:{target_normalized_name}:{target_label}",
        length=32,
    )


def graph_entity_collection_name(kb_id: str) -> str:
    """实体向量集合命名。"""
    return f"{kb_id}_entity"


def graph_triple_collection_name(kb_id: str) -> str:
    """三元组向量集合命名。"""
    return f"{kb_id}_triple"


def build_graph_payload(normalized_result: dict[str, Any]) -> dict[str, Any]:
    """将抽取器产出的标准化结果转换为图库写入所需的图结构。

    输入经过 normalize_extraction_result；这里只分配局部 ID 并转换关系引用。
    """
    entities = [{**entity, "id": f"e{index}"} for index, entity in enumerate(normalized_result["entities"], 1)]
    entity_ids = {(normalize_entity_name(entity["text"]), entity["label"]): entity["id"] for entity in entities}
    relations = [
        {
            **relation,
            "source": entity_ids[(normalize_entity_name(relation["source"]["text"]), relation["source"]["label"])],
            "target": entity_ids[(normalize_entity_name(relation["target"]["text"]), relation["target"]["label"])],
        }
        for relation in normalized_result["relations"]
    ]
    return {"entities": entities, "relations": relations, "metadata": normalized_result["metadata"]}


# ─── nGQL 模板 ──────────────────────────────────────────────────
# Nebula 3.x 不支持动态边类型，也不支持按属性 MERGE，
# 因此节点身份用 vid 承载、关系身份用 (src, dst, rank) 承载。


def chunk_vid(kb_id: str, chunk_id: str) -> str:
    """Chunk 顶点主键。"""
    return f"{kb_id}:{chunk_id}"


def entity_vid(kb_id: str, entity_id: str) -> str:
    """Entity 顶点主键。"""
    return f"{kb_id}:{entity_id}"


def relation_rank(chunk_id: str, relation_type: str) -> int:
    """同一对实体之间允许并存多条 RELATION，用 rank 区分到 chunk 粒度。"""
    digest = hashlib.blake2b(f"{chunk_id}\x00{relation_type}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFFFFFFFFFFFFFF


def ngql_upsert_chunk(
    *,
    kb_id: str,
    chunk_id: str,
    file_id: str,
    chunk_index: int,
    content_preview: str,
    start_char_pos: int | None,
    end_char_pos: int | None,
) -> str:
    """写入/更新 Chunk 顶点。"""
    assignments = [
        ("kind", "Chunk"),
        ("kb_id", kb_id),
        ("chunk_id", chunk_id),
        ("file_id", file_id),
        ("chunk_index", chunk_index),
        ("content_preview", content_preview),
    ]
    if start_char_pos is not None:
        assignments.append(("start_char_pos", start_char_pos))
    if end_char_pos is not None:
        assignments.append(("end_char_pos", end_char_pos))
    return (
        f'UPSERT VERTEX ON Node {ngql_literal(chunk_vid(kb_id, chunk_id))} SET '
        + ", ".join(f"Node.{name} = {ngql_literal(value)}" for name, value in assignments)
    )


def ngql_upsert_entity(
    *,
    kb_id: str,
    entity_id: str,
    normalized_name: str,
    label: str,
    name: str,
    attributes: str,
    source_chunk_id: str,
) -> str:
    """写入/更新 Entity 顶点；归属 chunk 随最后一次提及推进。"""
    assignments = {
        "kind": "Entity",
        "kb_id": kb_id,
        "entity_id": entity_id,
        "normalized_name": normalized_name,
        "label": label,
        "name": name,
        "attributes": attributes,
        "source_chunk_id": source_chunk_id,
    }
    return (
        f'UPSERT VERTEX ON Node {ngql_literal(entity_vid(kb_id, entity_id))} SET '
        + ", ".join(f"Node.{key} = {ngql_literal(value)}" for key, value in assignments.items())
    )


def ngql_insert_mention(*, kb_id: str, chunk_id: str, file_id: str, entity_id: str) -> str:
    """建立 Chunk → Entity 的 MENTIONS 边。"""
    source = ngql_literal(chunk_vid(kb_id, chunk_id))
    target = ngql_literal(entity_vid(kb_id, entity_id))
    values = ", ".join(ngql_literal(value) for value in (chunk_id, file_id, kb_id))
    return f"INSERT EDGE IF NOT EXISTS MENTIONS(chunk_id, file_id, kb_id) VALUES {source}->{target}:({values})"


def ngql_insert_relation(
    *,
    kb_id: str,
    chunk_id: str,
    file_id: str,
    source_entity_id: str,
    target_entity_id: str,
    relation_type: str,
    triple_id: str,
    text: str,
    source_name: str,
    target_name: str,
    extractor_type: str,
) -> str:
    """建立 Entity → Entity 的 RELATION 边；rank 由 chunk 与关系类型决定。"""
    source = ngql_literal(entity_vid(kb_id, source_entity_id))
    target = ngql_literal(entity_vid(kb_id, target_entity_id))
    rank = relation_rank(chunk_id, relation_type)
    columns = "kb_id, chunk_id, triple_id, rel_type, text, source_name, target_name, file_id, extractor_type"
    values = ", ".join(
        ngql_literal(value)
        for value in (kb_id, chunk_id, triple_id, relation_type, text, source_name, target_name, file_id, extractor_type)
    )
    return f"INSERT EDGE IF NOT EXISTS RELATION({columns}) VALUES {source}->{target}@{rank}:({values})"
