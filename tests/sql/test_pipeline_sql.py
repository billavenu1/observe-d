"""Request join, rollups and tokenomics math against hand-computed fixtures (MVP.md §8: within 1%)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.sql.conftest import refresh

pytestmark = pytest.mark.sql

NOW = datetime.now(UTC)  # tz-aware: clickhouse_connect treats naive datetimes as local time


def tid() -> str:
    return uuid.uuid4().hex


def gateway_log(
    client,
    trace: str,
    *,
    org: str,
    app: str,
    model: str,
    pod: str,
    code: int,  # type: ignore[no-untyped-def]
    ts: datetime = NOW,
) -> None:
    attrs = {
        "log.source": "gateway_access",
        "request_id": f"req-{trace[:8]}",
        "org_id": org,
        "app_id": app,
        "user_id": "u1",
        "session_id": "s1",
        "model": model,
        "upstream_pod": pod,
        "response_code": str(code),
        "duration_ms": "812.5",
        "bytes_received": "300",
        "bytes_sent": "900",
    }
    client.insert(
        "gateway_access_null",
        [[ts, trace, "gwspan", "inference-gateway", attrs]],
        column_names=["Timestamp", "TraceId", "SpanId", "ServiceName", "LogAttributes"],
    )


def engine_span(
    client,
    trace: str,
    *,
    model: str,
    pod: str,
    tin: int,
    tcached: int,
    tout: int,  # type: ignore[no-untyped-def]
    ttft: float,
    decode: float,
    finish: str = "stop",
    ts: datetime = NOW,
) -> None:
    attrs = {
        "gen_ai.request.id": f"req-{trace[:8]}",
        "gen_ai.response.model": model,
        "gen_ai.usage.prompt_tokens": str(tin),
        "gen_ai.usage.cached_tokens": str(tcached),
        "gen_ai.usage.completion_tokens": str(tout),
        "gen_ai.latency.time_in_queue": "0.01",
        "gen_ai.latency.time_in_model_prefill": str(ttft - 0.01),
        "gen_ai.latency.time_in_model_decode": str(decode),
        "gen_ai.latency.time_to_first_token": str(ttft),
        "gen_ai.latency.e2e": str(ttft + decode),
        "gen_ai.response.finish_reasons": finish,
        "gen_ai.request.temperature": "0.7",
    }
    res = {"k8s.pod.name": pod, "k8s.node.name": "node-a", "llm.engine": "vllm", "llm.engine_version": "test"}
    client.insert(
        "otel_traces",
        [
            [
                ts,
                trace,
                "engspan",
                "gwspan",
                "llm_request",
                "SPAN_KIND_SERVER",
                "vllm",
                res,
                attrs,
                int((ttft + decode) * 1e9),
                "STATUS_CODE_UNSET",
            ]
        ],
        column_names=[
            "Timestamp",
            "TraceId",
            "SpanId",
            "ParentSpanId",
            "SpanName",
            "SpanKind",
            "ServiceName",
            "ResourceAttributes",
            "SpanAttributes",
            "Duration",
            "StatusCode",
        ],
    )


def route_span(client, trace: str, *, pod: str, predicted: bool) -> None:  # type: ignore[no-untyped-def]
    attrs = {
        "llm_d.chosen_pod": pod,
        "llm_d.candidates": "2",
        "llm_d.score.prefix": "1" if predicted else "0",
        "llm_d.prefix_hit_predicted": "true" if predicted else "false",
        "llm_d.fallback_reason": "",
    }
    client.insert(
        "otel_traces",
        [
            [
                NOW,
                trace,
                "rspan",
                "gwspan",
                "llm_d.route",
                "SPAN_KIND_INTERNAL",
                "inference-gateway",
                attrs,
                250_000,
            ]
        ],
        column_names=[
            "Timestamp",
            "TraceId",
            "SpanId",
            "ParentSpanId",
            "SpanName",
            "SpanKind",
            "ServiceName",
            "SpanAttributes",
            "Duration",
        ],
    )


def final_rows(client, where: str) -> list[dict]:  # type: ignore[no-untyped-def,type-arg]
    r = client.query(f"SELECT * FROM llm_requests FINAL WHERE {where} ORDER BY trace_id")
    return [dict(zip(r.column_names, row, strict=True)) for row in r.result_rows]


def test_join_statuses_and_late_engine_span(ch) -> None:  # type: ignore[no-untyped-def]
    complete, late, rejected = tid(), tid(), tid()
    gateway_log(ch, complete, org="acme", app="chat", model="m1", pod="p1", code=200)
    engine_span(ch, complete, model="m1", pod="p1", tin=1000, tcached=600, tout=101, ttft=0.2, decode=2.0)
    route_span(ch, complete, pod="p1", predicted=True)
    gateway_log(ch, late, org="acme", app="chat", model="m1", pod="p1", code=200)
    gateway_log(ch, rejected, org="unknown", app="unknown", model="m1", pod="", code=401)
    refresh(ch, "llm_requests_refresh")

    ids = f"trace_id IN ('{complete}', '{late}', '{rejected}')"
    rows = {r["trace_id"]: r for r in final_rows(ch, ids)}
    c = rows[complete]
    assert (c["status"], c["has_engine"], c["has_route"]) == ("ok", 1, 1)
    assert (c["tokens_in"], c["tokens_cached"], c["tokens_out"]) == (1000, 600, 101)
    assert c["ttft_us"] == 200_000 and c["tpot_us"] == 20_000  # 2.0 s / 100 inter-token gaps
    assert c["prefix_hit_predicted"] == 1 and c["prefix_hit_actual"] == 1
    assert c["org_id"] == "acme" and c["pod"] == "p1" and c["request_id"] == f"req-{complete[:8]}"
    assert rows[late]["has_engine"] == 0
    assert rows[rejected]["status"] == "rejected"

    # engine span arrives after the first join: the next refresh replaces the incomplete row
    engine_span(ch, late, model="m1", pod="p1", tin=50, tcached=0, tout=10, ttft=0.1, decode=0.5)
    refresh(ch, "llm_requests_refresh")
    refresh(ch, "llm_requests_refresh")  # idempotent
    rows = {r["trace_id"]: r for r in final_rows(ch, ids)}
    assert len(rows) == 3
    assert rows[late]["has_engine"] == 1 and rows[late]["tokens_out"] == 10


def test_rollup_slo_and_quantiles(ch) -> None:  # type: ignore[no-untyped-def]
    # qwen SLO (0005): ttft <= 500 ms, tpot <= 30 ms
    traces = []
    for ttft, decode in ((0.2, 1.0), (0.3, 1.5), (0.9, 1.0), (0.1, 5.0)):  # good, good, slow ttft, slow tpot
        t = tid()
        traces.append(t)
        gateway_log(ch, t, org="slo-org", app="a", model="qwen2.5-7b-instruct", pod="q1", code=200)
        engine_span(
            ch,
            t,
            model="qwen2.5-7b-instruct",
            pod="q1",
            tin=100,
            tcached=0,
            tout=101,
            ttft=ttft,
            decode=decode,
        )
    refresh(ch, "llm_requests_refresh", "llm_rollup_1m_refresh")
    r = ch.query(
        "SELECT sum(requests), sum(slo_good), "
        "arrayElement(quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(ttft_q), 1) "
        "FROM llm_rollup_1m FINAL WHERE org_id = 'slo-org'"
    ).result_rows[0]
    assert r[0] == 4 and r[1] == 2
    assert 100_000 <= r[2] <= 900_000


def test_tokenomics_cost_energy_allocation(ch) -> None:  # type: ignore[no-untyped-def]
    pod, model, gpu_model = "tok-pod", "llama-3.1-70b-instruct", "AMD Instinct MI300X"  # $2.20 / GPU-hour
    minutes = 10
    rows_g, rows_s = [], []
    base = NOW.replace(second=0, microsecond=0) - timedelta(minutes=minutes - 1)
    for gpu, owner in (("g0", pod), ("g1", pod), ("g7", "")):  # 2 GPUs on the pod, 1 unallocated
        for m in range(minutes):
            ts = base + timedelta(minutes=m)
            attrs = {
                "vendor": "amd",
                "node": "node-a",
                "gpu_uuid": f"uuid-{gpu}",
                "gpu_index": gpu[1:],
                "gpu_model": gpu_model,
                "pod": owner,
                "namespace": "llm" if owner else "",
            }
            rows_g.append([ts, "gpu.power_watts", attrs, 500.0])
            rows_s.append([ts, "gpu.energy_joules_total", attrs, 1_000_000.0 + 30_000.0 * m])  # 500 W x 60 s
    ch.insert("otel_metrics_gauge", rows_g, column_names=["TimeUnix", "MetricName", "Attributes", "Value"])
    ch.insert("otel_metrics_sum", rows_s, column_names=["TimeUnix", "MetricName", "Attributes", "Value"])

    for org, tout in (("org-a", 301), ("org-a", 301), ("org-a", 301), ("org-b", 301)):  # 3:1 token split
        t = tid()
        gateway_log(ch, t, org=org, app="app", model=model, pod=pod, code=200)
        engine_span(ch, t, model=model, pod=pod, tin=1000, tcached=500, tout=tout, ttft=0.3, decode=3.0)
    refresh(ch, "llm_requests_refresh", "tokenomics_hourly_refresh")

    res = {
        row[0]: row[1:]
        for row in ch.query(
            "SELECT org_id, sum(gpu_seconds), sum(energy_j), sum(cost_infra_usd), sum(tokens_out), "
            "sum(cache_savings_usd) FROM tokenomics_hourly FINAL "
            f"WHERE pod IN ('{pod}', '') GROUP BY org_id"
        ).result_rows
    }
    pod_gpu_seconds = 2 * minutes * 60  # 2 GPUs present in 10 distinct minutes
    pod_energy = 2 * 30_000.0 * (minutes - 1)  # Δ counter per GPU (first..last sample)
    pod_cost = pod_gpu_seconds / 3600 * 2.20
    a, b = res["org-a"], res["org-b"]
    assert a[0] + b[0] == pytest.approx(pod_gpu_seconds, rel=0.01)
    assert a[0] == pytest.approx(0.75 * pod_gpu_seconds, rel=0.01)
    assert a[1] + b[1] == pytest.approx(pod_energy, rel=0.01)
    assert a[2] == pytest.approx(0.75 * pod_cost, rel=0.01)
    assert b[2] == pytest.approx(0.25 * pod_cost, rel=0.01)
    # J / output token for the pod
    assert (a[1] + b[1]) / (a[3] + b[3]) == pytest.approx(pod_energy / (4 * 301), rel=0.01)
    # cache savings: 500 cached tokens x ($0.60 - $0.15) / 1M per request
    assert a[4] + b[4] == pytest.approx(4 * 500 * (0.60 - 0.15) / 1e6, rel=0.01)
    unalloc = res["(unallocated)"]
    assert unalloc[2] == pytest.approx(minutes * 60 / 3600 * 2.20, rel=0.01)


def test_gpu_fatal_events_parse_nvidia_and_amd(ch) -> None:  # type: ignore[no-untyped-def]
    from sim.fake_gpu_exporter import kernel_log_lines, new_gpus

    lines = [
        *kernel_log_lines("nvidia", "xid", new_gpus("nvidia", "n")[2]),
        *kernel_log_lines("amd", "xid", new_gpus("amd", "n")[0]),
        "NCCL WARN NET/IB : Got completion from peer with error 12",
        "INFO all good",
    ]
    res = {"k8s.node.name": "fatal-node", "k8s.pod.name": "exporter-0"}
    ch.insert(
        "otel_logs",
        [[NOW, "fake-gpu-exporter", line, res] for line in lines],
        column_names=["Timestamp", "ServiceName", "Body", "ResourceAttributes"],
    )
    rows = ch.query(
        "SELECT vendor, kind, code, pci_bus, critical FROM gpu_fatal_events "
        "WHERE node = 'fatal-node' ORDER BY vendor, kind"
    ).result_rows
    assert ("nvidia", "xid", 79, "0000:3b:00.0", 1) in rows
    assert ("amd", "ras_uncorrectable", 0, "0000:1b:00.0", 1) in rows
    assert ("amd", "gpu_reset", 0, "0000:1b:00.0", 1) in rows
    assert len(rows) == 3
    assert ch.query("SELECT count() FROM collective_errors WHERE node = 'fatal-node'").result_rows[0][0] == 1
