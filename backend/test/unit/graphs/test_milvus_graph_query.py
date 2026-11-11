from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from yuxi.modules.knowledge.graphs.milvus_graph_vector_store import MilvusGraphVectorStore
from yuxi.modules.knowledge.graphs.service import KnowledgeGraphService

pytestmark = pytest.mark.usefixtures("milvus_graph_projection_lock")


class _FakeConnection:
    """记录 nGQL 语句，供断言写入与删除的形状。"""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, ngql: str) -> list:
        self.statements.append(" ".join(ngql.split()))
        return []


def _service(rows_by_marker: dict[str, list[dict]] | None = None) -> tuple[KnowledgeGraphService, _FakeConnection]:
    """构造只替换 `_rows` 的服务，其余走真实实现。"""
    connection = _FakeConnection()
    service = KnowledgeGraphService(nebula_connection=SimpleNamespace(execute=connection.execute))
    markers = rows_by_marker or {}

    def rows(ngql: str) -> list[dict]:
        for marker, rows_value in markers.items():
            if marker in ngql:
                return rows_value
        return []

    service._rows = rows  # type: ignore[method-assign]
    return service, connection


def _node(vid: str, kind: str, **properties) -> dict:
    return {"id": vid, "labels": [kind], "properties": {"kind": kind, "kb_id": "kb_test", **properties}}


def test_subgraph_ngql_builds_depth_and_path_filters():
    service, _ = _service()

    query = " ".join(
        service._subgraph_ngql("kb_test", ["kb_test:e1"], 2, ['all(x IN nodes(p) WHERE x.Node.chunk_id IN ["c1"])'], 30).split()
    )

    assert "[*1..2]" in query
    assert 'all(x IN nodes(p) WHERE x.Node.kb_id == "kb_test")' in query
    assert "nodes(p)" in query and "relationships(p)" in query
    assert "LIMIT 30" in query


def test_subgraph_from_ngql_dedupes_rows_and_honors_limit():
    rows = [
        {
            "node_ids": ["kb_test:e1", "kb_test:c1"],
            "node_props": [
                {"kind": "Entity", "kb_id": "kb_test", "entity_id": "e1", "name": "苹果"},
                {"kind": "Chunk", "kb_id": "kb_test", "chunk_id": "c1", "content_preview": "苹果很好吃"},
            ],
            "edges": [["kb_test:c1", "kb_test:e1", "MENTIONS", 0, {"chunk_id": "c1", "kb_id": "kb_test"}]],
        },
        {
            "node_ids": ["kb_test:e1", "kb_test:c1"],
            "node_props": [{"kind": "Entity", "kb_id": "kb_test", "entity_id": "e1"}, {"kind": "Chunk"}],
            "edges": [["kb_test:c1", "kb_test:e1", "MENTIONS", 0, {"chunk_id": "c1"}]],
        },
    ]
    service, _ = _service({"MATCH p=": rows})

    result = service._subgraph_from_ngql("MATCH p=...", limit=2)

    assert [node["id"] for node in result["nodes"]] == ["kb_test:e1", "kb_test:c1"]
    assert result["nodes"][0]["type"] == "Entity" and result["nodes"][1]["type"] == "Chunk"
    assert result["nodes"][0]["labels"] == ["Entity"]
    assert [edge["id"] for edge in result["edges"]] == ["kb_test:c1|kb_test:e1|0"]
    assert result["edges"][0]["type"] == "MENTIONS"


def test_subgraph_from_ngql_drops_edges_of_truncated_nodes():
    rows = [
        {
            "node_ids": ["kb_test:e1", "kb_test:e2"],
            "node_props": [{"kind": "Entity"}, {"kind": "Entity"}],
            "edges": [["kb_test:e1", "kb_test:e2", "RELATION", 3, {"rel_type": "likes"}]],
        }
    ]
    service, _ = _service({"MATCH p=": rows})

    result = service._subgraph_from_ngql("MATCH p=...", limit=1)

    assert [node["id"] for node in result["nodes"]] == ["kb_test:e1"]
    assert result["edges"] == []


def _depth_of_path_query(service: KnowledgeGraphService, max_depth: int) -> str:
    """跑一次节点查询，返回生成的变长路径语句。"""
    queries: list[str] = []
    inner = service._rows
    service._rows = lambda ngql: queries.append(" ".join(ngql.split())) or inner(ngql)  # type: ignore[method-assign]
    service._query_nodes_sync("kb_test", "e1", 3, max_depth, False)
    return next(query for query in queries if "MATCH p=" in query)


@pytest.mark.parametrize("max_depth", [1, 2, 3])
def test_query_nodes_sync_uses_requested_depth(max_depth):
    service, _ = _service(_seed_rows())

    assert f"[*1..{max_depth}]" in _depth_of_path_query(service, max_depth)


def test_query_nodes_sync_caps_max_depth_at_three():
    service, _ = _service(_seed_rows())

    assert "[*1..3]" in _depth_of_path_query(service, 100)


def _seed_rows() -> dict[str, list[dict]]:
    return {
        "RETURN id(n) AS id, properties(n) AS props": [
            {"id": "kb_test:e1", "props": {"kind": "Entity", "kb_id": "kb_test", "entity_id": "e1"}}
        ]
    }


def test_seed_vids_selects_by_keyword_and_scope():
    service, _ = _service(
        {
            "RETURN id(n) AS id, properties(n) AS props": [
                {"id": "kb_test:e1", "props": {"kind": "Entity", "kb_id": "kb_test", "entity_id": "e1", "name": "苹果"}}
            ]
        }
    )

    seeds = service._seed_vids("kb_test", "苹果", 5, [])

    assert seeds[0]["id"] == "kb_test:e1"
    assert seeds[0]["name"] == "苹果"


def test_in_list_degrades_to_false_for_empty_scope():
    assert KnowledgeGraphService._in_list("n.Node.chunk_id", []) == "false"
    assert KnowledgeGraphService._in_list("n.Node.chunk_id", ["c1", "c2"]) == 'n.Node.chunk_id IN ["c1", "c2"]'


def test_delete_file_graph_keeps_entities_still_mentioned_elsewhere():
    service, connection = _service(
        {
            "-[r:RELATION]->": [{"source": "kb_test:e1", "target": "kb_test:e2", "rank": 7}],
            "<-[:MENTIONS]-": [{"entity": "kb_test:e1"}],
            "-[r:MENTIONS]->": [
                {"source": "kb_test:c1", "target": "kb_test:e1", "rank": 0, "entity": "kb_test:e1"}
            ],
            'kind == "Chunk"': [{"vid": "kb_test:c1"}],
        }
    )

    service._delete_file_graph_from_graph("kb_test", "file_1", ["c1"], restored_entities=None)

    joined = "\n".join(connection.statements)
    assert 'DELETE EDGE RELATION "kb_test:e1"->"kb_test:e2"@7' in joined
    assert 'DELETE EDGE MENTIONS "kb_test:c1"->"kb_test:e1"@0' in joined
    assert 'DELETE VERTEX "kb_test:c1" WITH EDGE' in joined
    assert "DELETE VERTEX" not in joined.replace('DELETE VERTEX "kb_test:c1" WITH EDGE', "")


def test_delete_file_graph_removes_orphan_entities_and_restores_attributes():
    service, connection = _service(
        {
            "-[r:RELATION]->": [],
            "<-[:MENTIONS]-": [],
            "-[r:MENTIONS]->": [
                {"source": "kb_test:c1", "target": "kb_test:e1", "rank": 0, "entity": "kb_test:e1"}
            ],
            'kind == "Chunk"': [{"vid": "kb_test:c1"}],
        }
    )
    restored = [{"entity_id": "e9", "name": "梨", "attributes": {"来源": "旧文件"}, "source_chunk_id": "c9"}]

    service._delete_file_graph_from_graph("kb_test", "file_1", None, restored_entities=restored)

    joined = "\n".join(connection.statements)
    assert 'DELETE VERTEX "kb_test:e1" WITH EDGE' in joined
    assert 'UPDATE VERTEX ON Node "kb_test:e9" SET Node.name = "梨"' in joined
    assert 'Node.attributes = "{\\"来源\\": \\"旧文件\\"}"' in joined


def test_delete_graph_skips_lookup_when_no_vertex():
    service, connection = _service({"RETURN count(n) AS c": [{"c": 0}]})
    service.graph_vector_store = SimpleNamespace(drop_graph_collections=MagicMock())

    service.delete_graph("kb_test")

    assert connection.statements == []
    service.graph_vector_store.drop_graph_collections.assert_called_once_with("kb_test")


def test_delete_graph_removes_all_vertices_with_edges():
    service, connection = _service({"RETURN count(n) AS c": [{"c": 3}]})
    service.graph_vector_store = SimpleNamespace(drop_graph_collections=MagicMock())

    service.delete_graph("kb_test")

    assert connection.statements == [
        'LOOKUP ON Node WHERE Node.kb_id == "kb_test" YIELD id(vertex) AS vid | DELETE VERTEX $-.vid WITH EDGE'
    ]


def test_get_stats_reads_counts_and_entity_labels():
    service, _ = _service(
        {
            "RETURN count(n) AS node_count": [{"node_count": 12}],
            "RETURN count(r) AS edge_count": [{"edge_count": 30}],
            "RETURN n.Node.label AS entity_label": [
                {"entity_label": "人物", "count": 7},
                {"entity_label": "组织", "count": 2},
            ],
        }
    )

    stats = service._get_stats_sync('"kb_test"')

    assert stats == {
        "total_nodes": 12,
        "total_edges": 30,
        "entity_types": [{"type": "人物", "count": 7}, {"type": "组织", "count": 2}],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "kwargs", "expected"),
    [
        ("query_nodes", {"keyword": "test"}, {"nodes": [], "edges": []}),
        ("get_labels", {}, []),
        ("get_stats", {}, {"total_nodes": 0, "total_edges": 0, "entity_types": []}),
    ],
)
async def test_milvus_graph_service_early_returns_empty_for_missing_kb_id(method, kwargs, expected):
    service = KnowledgeGraphService()
    assert await getattr(service, method)(kb_id=None, **kwargs) == expected


@pytest.mark.parametrize("operation", ["has_collection", "drop_collection"])
def test_drop_graph_collections_propagates_storage_failure(monkeypatch, operation):
    """图集合检查或删除失败必须向上传播。"""
    from yuxi.modules.knowledge.graphs import milvus_graph_vector_store as module

    store = MilvusGraphVectorStore.__new__(MilvusGraphVectorStore)
    store.connection_alias = "test"
    error = RuntimeError("Milvus unavailable")
    monkeypatch.setattr(module.utility, "has_collection", MagicMock(return_value=True))
    monkeypatch.setattr(module.utility, operation, MagicMock(side_effect=error))

    with pytest.raises(RuntimeError) as caught:
        store.drop_graph_collections("kb_test")

    assert caught.value is error


def test_graph_ppr_ranks_chunk_nodes_from_seed_entities():
    """查询图中的实体种子能够按 PPR 找到关联 chunk。"""
    subgraph = {
        "nodes": [
            {"id": "e1", "type": "Entity", "properties": {"entity_id": "seed"}},
            {"id": "c1", "type": "Chunk", "properties": {"chunk_id": "chunk_a"}},
            {"id": "e2", "type": "Entity", "properties": {"entity_id": "other"}},
            {"id": "c2", "type": "Chunk", "properties": {"chunk_id": "chunk_b"}},
        ],
        "edges": [
            {"source_id": "e1", "target_id": "c1"},
            {"source_id": "e1", "target_id": "e2"},
            {"source_id": "e2", "target_id": "c2"},
        ],
    }

    ranked = KnowledgeGraphService.rank_chunks_by_ppr(subgraph, {"seed": 1.0}, top_k=2, damping=0.85)

    assert [chunk_id for chunk_id, _ in ranked] == ["chunk_a", "chunk_b"]


async def test_delete_file_graph_initializes_vector_store_outside_event_loop(monkeypatch):
    """首次删除将同步客户端初始化移出事件循环，完成两种投影清理。"""
    import threading
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    import yuxi.modules.knowledge.graphs.service as module

    loop_thread = threading.get_ident()
    store = SimpleNamespace(delete_graph_records=AsyncMock())
    initialized_threads = []

    def create_store():
        """记录构造线程，拒绝事件循环内初始化。"""
        initialized_threads.append(threading.get_ident())
        assert threading.get_ident() != loop_thread, "网络客户端在事件循环内初始化"
        return store

    @asynccontextmanager
    async def references(_file_id, *, generation, restored_entities, additional_entity_ids, additional_triple_ids):
        """提供本次删除的文件图谱引用。"""
        assert generation == 2
        yield ["entity-1"], ["triple-1"], ["chunk-1"]

    monkeypatch.setattr(module, "MilvusGraphVectorStore", create_store)
    service = KnowledgeGraphService(
        graph_repo=SimpleNamespace(
            delete_file_references=references, file_entity_sources=AsyncMock(return_value=[]), delete_orphan_records=AsyncMock()
        ),
        chunk_repo=SimpleNamespace(list_ids_by_file_generation=AsyncMock(return_value=["chunk-1"])),
    )
    service._file_graph_record_ids = lambda *args: ([], [])
    deleted = []
    service._delete_file_graph_from_graph = lambda *args: deleted.append(args)
    await service.delete_file_graph("kb-1", "file-1", generation=2)
    assert initialized_threads and initialized_threads[0] != loop_thread
    store.delete_graph_records.assert_awaited_once_with("kb-1", entity_ids=["entity-1"], triple_ids=["triple-1"])
    assert deleted == [("kb-1", "file-1", ["chunk-1"], [])]
