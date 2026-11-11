from __future__ import annotations

from uuid import uuid4

import pytest

from yuxi.infrastructure.nebula import get_shared_nebula_connection, result_rows
from yuxi.modules.knowledge.graphs.graph_utils import (
    ngql_insert_mention,
    ngql_upsert_chunk,
    ngql_upsert_entity,
)
from yuxi.modules.knowledge.graphs.service import KnowledgeGraphService


def _names(connection, kb_id: str) -> set[str]:
    rows = result_rows(
        connection.execute(
            f'MATCH (n:Node) WHERE n.Node.kb_id == "{kb_id}" AND n.Node.kind == "Entity" RETURN n.Node.name AS name'
        )
    )
    return {row["name"] for row in rows}


def _files(connection, kb_id: str) -> set[str]:
    rows = result_rows(
        connection.execute(
            f'MATCH (n:Node) WHERE n.Node.kb_id == "{kb_id}" AND n.Node.kind == "Chunk" RETURN n.Node.file_id AS file_id'
        )
    )
    return {row["file_id"] for row in rows}


@pytest.mark.integration
@pytest.mark.self_managed_resources
def test_delete_file_graph_preserves_shared_and_unrelated_entities():
    kb_id = f"pytest_delete_graph_{uuid4().hex}"
    connection = get_shared_nebula_connection()
    service = KnowledgeGraphService(nebula_connection=connection)

    try:
        for chunk_id, file_id in (("c1", "f1"), ("c2", "f2")):
            connection.execute(
                ngql_upsert_chunk(
                    kb_id=kb_id,
                    chunk_id=chunk_id,
                    file_id=file_id,
                    chunk_index=0,
                    content_preview=chunk_id,
                    start_char_pos=0,
                    end_char_pos=1,
                )
            )
        for entity_id, name, source_chunk_id in (
            ("shared", "shared", "c1"),
            ("f1_only", "f1_only", "c1"),
            ("unrelated", "unrelated", "c1"),
        ):
            connection.execute(
                ngql_upsert_entity(
                    kb_id=kb_id,
                    entity_id=entity_id,
                    normalized_name=name,
                    label="测试",
                    name=name,
                    attributes="{}",
                    source_chunk_id=source_chunk_id,
                )
            )
        for chunk_id, file_id, entity_id in (("c1", "f1", "shared"), ("c1", "f1", "f1_only"), ("c2", "f2", "shared")):
            connection.execute(ngql_insert_mention(kb_id=kb_id, chunk_id=chunk_id, file_id=file_id, entity_id=entity_id))

        service._delete_file_graph_from_graph(kb_id, "f1", ["c1"])

        assert _names(connection, kb_id) == {"shared", "unrelated"}
        assert _files(connection, kb_id) == {"f2"}

        service._delete_file_graph_from_graph(kb_id, "f2", ["c2"])

        assert _names(connection, kb_id) == {"unrelated"}
    finally:
        connection.execute(
            f'LOOKUP ON Node WHERE Node.kb_id == "{kb_id}" YIELD id(vertex) AS vid | DELETE VERTEX $-.vid WITH EDGE'
        )
