"""集成测试读取 NebulaGraph 结果的公共语句。"""

from __future__ import annotations

from yuxi.infrastructure.nebula import result_rows


def rows(connection, ngql: str) -> list[dict]:
    """执行 nGQL 并返回字典行。"""
    return result_rows(connection.execute(ngql))


def scoped(kb_id: str, conditions: str) -> str:
    """拼接限定在单个知识库内的 WHERE 片段。"""
    return f'Node.kb_id == "{kb_id}" AND {conditions}'


def entity_props(connection, kb_id: str, normalized_name: str) -> dict | None:
    """读取实体属性，不存在时返回 None。"""
    found = rows(
        connection,
        f'MATCH (n:Node) WHERE n.Node.kb_id == "{kb_id}" AND n.Node.kind == "Entity" '
        f'AND n.Node.normalized_name == "{normalized_name}" RETURN properties(n) AS props',
    )
    return found[0]["props"] if found else None


def chunk_count(connection, kb_id: str, **filters: str) -> int:
    """按 file_id/chunk_id 统计 chunk 顶点数。"""
    conditions = " AND ".join(f'Node.{name} == "{value}"' for name, value in filters.items())
    clause = scoped(kb_id, conditions) if conditions else scoped(kb_id, 'Node.kind == "Chunk"')
    return rows(
        connection, f'MATCH (n:Node) WHERE n.Node.kind == "Chunk" AND {clause} RETURN count(n) AS n'
    )[0]["n"]


def relation_file_ids(connection, kb_id: str) -> list[str]:
    """列出该知识库全部关系边归属的文件。"""
    return [
        row["file_id"]
        for row in rows(
            connection,
            f'MATCH (a:Node)-[r:RELATION]->(b:Node) WHERE r.kb_id == "{kb_id}" RETURN r.file_id AS file_id',
        )
    ]


def drop_kb(connection, kb_id: str) -> None:
    """清空测试知识库的全部顶点与边。"""
    connection.execute(f'LOOKUP ON Node WHERE Node.kb_id == "{kb_id}" YIELD id(vertex) AS vid | DELETE VERTEX $-.vid WITH EDGE')
