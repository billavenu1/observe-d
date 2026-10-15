"""SQL tests run against a real ClickHouse (the local compose one, or $CLICKHOUSE_HOST) in a throwaway
database with every migration applied. Skipped when no ClickHouse is reachable."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

from tools.migrate import discover, migrate


@pytest.fixture(scope="session")
def ch() -> Iterator[object]:
    clickhouse_connect = pytest.importorskip("clickhouse_connect")
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    try:
        admin = clickhouse_connect.get_client(
            host=host,
            port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "otel"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "otel"),
        )
        admin.command("SELECT 1")
    except Exception as exc:
        pytest.skip(f"no ClickHouse at {host}: {exc}")
    db = f"odtest_{uuid.uuid4().hex[:8]}"
    admin.command(f"CREATE DATABASE {db}")
    client = clickhouse_connect.get_client(
        host=host,
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "otel"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "otel"),
        database=db,
    )
    try:
        migrate(client, discover(), db)
        # refreshes also run on their schedule; tests trigger them explicitly (they are idempotent)
        yield client
    finally:
        admin.command(f"DROP DATABASE IF EXISTS {db} SYNC")


def refresh(client, *views: str) -> None:  # type: ignore[no-untyped-def]
    db = client.database
    for view in views:
        client.command(f"SYSTEM REFRESH VIEW {db}.{view}")
        client.command(f"SYSTEM WAIT VIEW {db}.{view}")
