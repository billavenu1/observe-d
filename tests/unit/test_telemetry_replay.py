from __future__ import annotations

import argparse

from sim.telemetry_replay import Generator, rate_at


def attrs(kvs) -> dict:  # type: ignore[no-untyped-def,type-arg]
    return {kv.key: kv.value.WhichOneof("value") and getattr(kv.value, kv.value.WhichOneof("value")) for kv in kvs}


def test_record_set_per_request_is_consistent() -> None:
    traces, logs, n = Generator("t1", seed=1).build(50)
    assert n == 50
    spans = [s for rs in traces.resource_spans for ss in rs.scope_spans for s in ss.spans]
    records = [r for rl in logs.resource_logs for sl in rl.scope_logs for r in sl.log_records]
    assert len(spans) == 150 and len(records) == 50
    by_trace: dict[bytes, dict[str, object]] = {}
    for s in spans:
        by_trace.setdefault(s.trace_id, {})[s.name] = s
    for log in records:
        group = by_trace[log.trace_id]
        gw, eng, route = group["gateway.request"], group["llm_request"], group["llm_d.route"]
        assert log.span_id == gw.span_id == eng.parent_span_id == route.parent_span_id  # type: ignore[attr-defined]
        la, ea = attrs(log.attributes), attrs(eng.attributes)  # type: ignore[attr-defined]
        assert la["log.source"] == "gateway_access" and la["org_id"] == "loadtest-t1"
        assert la["request_id"] == ea["gen_ai.request.id"]
        assert la["upstream_pod"] in {
            "replay-llama-0",
            "replay-llama-1",
            "replay-llama-2",
            "replay-qwen2.5-0",
            "replay-qwen2.5-1",
            "replay-qwen2.5-2",
        }
        assert ea["gen_ai.usage.cached_tokens"] <= ea["gen_ai.usage.prompt_tokens"]


def test_burst_window() -> None:
    a = argparse.Namespace(rate=500, burst_rate=700, burst_at=60, burst_duration=30)
    assert rate_at(10, a) == 500 and rate_at(70, a) == 700 and rate_at(95, a) == 500


def test_trace_ids_unique_across_runs_with_same_pod_index() -> None:
    def ids(run_id: str) -> set[bytes]:
        traces, _, _ = Generator(run_id, seed=0).build(200)
        return {s.trace_id for rs in traces.resource_spans for ss in rs.scope_spans for s in ss.spans}

    assert not ids("run-a") & ids("run-b")
