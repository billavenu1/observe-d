from __future__ import annotations

import argparse

from sim.telemetry_replay import Generator, rate_at


def attrs(kvs) -> dict:  # type: ignore[no-untyped-def,type-arg]
    return {kv.key: kv.value.WhichOneof("value") and getattr(kv.value, kv.value.WhichOneof("value")) for kv in kvs}


def test_record_set_per_request_is_consistent() -> None:
    traces, logs, n = Generator("t1", seed=1).build(200)
    assert n == 200
    spans = [s for rs in traces.resource_spans for ss in rs.scope_spans for s in ss.spans]
    roles = {
        s.span_id: attrs(rs.resource.attributes).get("llm_d.role", "")
        for rs in traces.resource_spans
        for ss in rs.scope_spans
        for s in ss.spans
    }
    records = [r for rl in logs.resource_logs for sl in rl.scope_logs for r in sl.log_records]
    assert len(records) == 200
    by_trace: dict[bytes, list] = {}  # type: ignore[type-arg]
    for s in spans:
        by_trace.setdefault(s.trace_id, []).append(s)
    pd = 0
    for log in records:
        group = {(s.name, roles[s.span_id]): s for s in by_trace[log.trace_id]}
        gw, route = group[("gateway.request", "")], group[("llm_d.route", "")]
        la = attrs(log.attributes)
        assert la["log.source"] == "gateway_access" and la["org_id"] == "loadtest-t1"
        assert la["api_key_id"].startswith("key_")
        assert log.span_id == gw.span_id == route.parent_span_id
        if ("llm_request", "aggregated") in group:
            assert len(group) == 3
            eng = group[("llm_request", "aggregated")]
            assert la["upstream_pod"] in {f"replay-qwen2.5-{i}" for i in range(3)}
        else:  # P/D: prefill + decode llm_request spans under the gateway span, kv.transfer under decode
            pd += 1
            assert len(group) == 5
            eng, pre, xfer = (
                group[("llm_request", "decode")],
                group[("llm_request", "prefill")],
                group[("kv.transfer", "decode")],
            )
            assert pre.parent_span_id == gw.span_id and xfer.parent_span_id == eng.span_id
            pa, xa = attrs(pre.attributes), attrs(xfer.attributes)
            assert la["upstream_pod"] in {f"replay-llama-decode-{i}" for i in range(3)}
            assert xa["llm_d.kv.target_pod"] == la["upstream_pod"]
            assert xa["llm_d.kv.source_pod"] == pre_pod(traces, pre.span_id)
            assert xa["llm_d.kv.bytes"] == pa["gen_ai.usage.prompt_tokens"] * 327_680
            assert pre.end_time_unix_nano <= eng.start_time_unix_nano <= xfer.start_time_unix_nano
            assert attrs(route.attributes)["llm_d.prefill_pod"] == xa["llm_d.kv.source_pod"]
        ea = attrs(eng.attributes)
        assert eng.parent_span_id == gw.span_id
        assert la["request_id"] == ea["gen_ai.request.id"]
        assert ea["gen_ai.usage.cached_tokens"] <= ea["gen_ai.usage.prompt_tokens"]
    assert 0 < pd < 200  # both serving modes are exercised


def pre_pod(traces, span_id: bytes) -> str:  # type: ignore[no-untyped-def]
    for rs in traces.resource_spans:
        for ss in rs.scope_spans:
            if any(s.span_id == span_id for s in ss.spans):
                return str(attrs(rs.resource.attributes)["k8s.pod.name"])
    raise AssertionError("span not found")


def test_burst_window() -> None:
    a = argparse.Namespace(rate=500, burst_rate=700, burst_at=60, burst_duration=30)
    assert rate_at(10, a) == 500 and rate_at(70, a) == 700 and rate_at(95, a) == 500


def test_trace_ids_unique_across_runs_with_same_pod_index() -> None:
    def ids(run_id: str) -> set[bytes]:
        traces, _, _ = Generator(run_id, seed=0).build(200)
        return {s.trace_id for rs in traces.resource_spans for ss in rs.scope_spans for s in ss.spans}

    assert not ids("run-a") & ids("run-b")
