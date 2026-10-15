"""Every dashboard panel query must run and (unless it is an "only when something is wrong" panel) return
data on a running local stack.  `uv run python -m tools.dev up` first, let it run ~5 minutes, then:

    uv run pytest -m e2e tests/e2e
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
PROM_URL = os.environ.get("PROM_URL", "http://localhost:9090")

# Panels that are empty on a healthy system by design (they show faults), or that depend on the vendor /
# deployment (PCIe throughput has no AMD equivalent; CFS throttling needs CPU limits).
MAY_BE_EMPTY = {
    "PCIe TX / RX",
    "CPU throttling (tokenizer / scheduler starvation)",
    "Firing alerts",
    "Throttling by reason (NVIDIA)",
    "OOMKilled (last termination)",
    "NCCL / RCCL errors",
    "Routing fallbacks (last hour)",
    "No endpoint available",
    "Kubernetes events for the pod (±5 min)",
    "PCIe replays (link quality)",
    "Last fatal event (Xid)",
    "Uncorrectable ECC (total)",
    "Correctable ECC rate",
    "Waste: aborted output tokens & preemptions",
    "Preemptions / s",
    "Model pod startup phases",
    "Join health (missing spans = broken propagation)",
}


def panels() -> list[tuple[str, str, dict[str, Any]]]:
    out = []
    for path in sorted((ROOT / "grafana" / "dashboards").glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        for p in doc["panels"]:
            for t in p.get("targets", []):
                out.append((path.stem, p["title"], t))
    return out


@pytest.fixture(scope="module")
def ch():  # type: ignore[no-untyped-def]
    import clickhouse_connect

    try:
        client = clickhouse_connect.get_client(host="localhost", username="otel", password="otel", database="otel")
        client.command("SELECT 1")
    except Exception as exc:
        pytest.skip(f"local stack not running: {exc}")
    return client


@pytest.fixture(scope="module")
def sample_trace(ch) -> str:  # type: ignore[no-untyped-def]
    rows = ch.query(
        "SELECT trace_id FROM llm_requests FINAL WHERE has_engine = 1 AND ts > now() - INTERVAL 30 MINUTE "
        "AND ts < now() - INTERVAL 1 MINUTE ORDER BY ts DESC LIMIT 1"
    ).result_rows
    if not rows:
        pytest.fail("no joined requests in llm_requests yet: is loadgen running?")
    return str(rows[0][0])


def render_sql(sql: str, trace: str) -> str:
    sql = re.sub(r"\$__timeFilter\((\w+)\)", r"\1 BETWEEN now() - INTERVAL 1 HOUR AND now()", sql)
    sql = sql.replace("$__interval_s", "60")
    sql = re.sub(r"\$\{\w+:singlequote\}", "", sql)  # "All"
    return sql.replace("${trace}", trace)


@pytest.mark.parametrize(("board", "title", "target"), panels(), ids=lambda v: v if isinstance(v, str) else "")
def test_panel_query(board: str, title: str, target: dict[str, Any], ch, sample_trace: str) -> None:  # type: ignore[no-untyped-def]
    if target["datasource"]["uid"] == "prometheus":
        r = httpx.get(f"{PROM_URL}/api/v1/query", params={"query": target["expr"]}, timeout=30)
        body = r.json()
        assert body.get("status") == "success", body
        rows = body["data"]["result"]
    else:
        rows = ch.query(render_sql(target["rawSql"], sample_trace)).result_rows
    if title not in MAY_BE_EMPTY:
        assert rows, f"{board} / {title}: query returned no data"
