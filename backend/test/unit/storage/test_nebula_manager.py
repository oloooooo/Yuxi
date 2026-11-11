"""nGQL 字面量转义的注入边界。"""

import pytest

from yuxi.infrastructure.nebula import ngql_list, ngql_literal
from yuxi.infrastructure.nebula import manager as nebula_manager


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("kb_test", '"kb_test"'),
        ("a\\b", '"a\\\\b"'),
        ('say "hi"', '"say \\"hi\\""'),
        ("line\nbreak", '"line\\nbreak"'),
        (7, "7"),
        (1.5, "1.5"),
        (True, "true"),
    ],
)
def test_ngql_literal_escapes_strings_and_keeps_numbers(value, expected):
    assert ngql_literal(value) == expected


def test_ngql_literal_rejects_none():
    with pytest.raises(ValueError, match="不接受 None"):
        ngql_literal(None)


def test_ngql_list_joins_literals():
    assert ngql_list(["a", "b"]) == '["a", "b"]'


def test_close_shared_nebula_connection_closes_existing_manager(monkeypatch):
    class FakeConnection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    connection = FakeConnection()
    monkeypatch.setattr(nebula_manager, "_shared_nebula_connection", connection)

    nebula_manager.close_shared_nebula_connection()

    assert connection.closed is True
