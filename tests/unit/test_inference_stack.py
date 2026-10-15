from __future__ import annotations

import asyncio
import random

from aiohttp.test_utils import TestServer
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from sim.inference_stack import (
    AppProfile,
    Endpoint,
    Engine,
    EngineConfig,
    Gateway,
    LoadGen,
    Scheduler,
    api_keys_from_apps,
    engine_app,
    gateway_app,
    parse_api_keys,
)
from sim.telemetry import setup


def tel():  # type: ignore[no-untyped-def]
    spans, logs = InMemorySpanExporter(), InMemoryLogRecordExporter()
    return setup(Resource.create({"service.name": "t"}), spans, logs), spans, logs


def test_scheduler_prefers_prefix_owner_then_least_loaded() -> None:
    s = Scheduler()
    s.endpoints = {
        "a": Endpoint("a", pod="pa", model="m", waiting=0, ready=True),
        "b": Endpoint("b", pod="pb", model="m", waiting=0, ready=True),
        "x": Endpoint("x", pod="px", model="other", ready=True),
    }
    first = s.pick("m", "sess-1")
    assert first.endpoint is not None and first.candidates == 2 and not first.prefix_hit_predicted
    again = s.pick("m", "sess-1")
    assert again.endpoint is first.endpoint and again.prefix_hit_predicted
    # overload the owner: queue score drops enough to move the session
    first.endpoint.waiting = first.endpoint.max_num_seqs
    first.endpoint.kv_usage = 1.0
    moved = s.pick("m", "sess-1")
    assert moved.endpoint is not first.endpoint and moved.fallback_reason == "prefix_owner_overloaded"
    assert s.pick("missing-model", "z").fallback_reason == "no_endpoint_available"


def test_engine_prefix_cache_and_abort() -> None:
    t, _, _ = tel()
    eng = Engine(EngineConfig(model="m", itl_ms=0.1, prefill_us_per_token=1), t, random.Random(1))

    async def go() -> None:
        r1 = await eng.run({"prompt_tokens": 500, "max_tokens": 20, "prefix_id": "s", "prefix_tokens": 0})
        r2 = await eng.run({"prompt_tokens": 900, "max_tokens": 20, "prefix_id": "s", "prefix_tokens": 520})
        r3 = await eng.run({"prompt_tokens": 100, "max_tokens": 50, "abort_at_token": 3})
        assert r1.tokens_cached == 0 and r2.tokens_cached == 520
        assert (r3.finish_reason in ("abort", "error") and r3.tokens_out <= 3) or r3.status == 500
        assert eng.running == 0 and eng.kv_tokens == 0

    asyncio.run(go())


def test_parse_keys() -> None:
    spec = "k1:acme:chat:model-a:100:10,k2:globex:search:model-b:100:10"
    assert parse_api_keys(api_keys_from_apps(spec)) == {"k1": ("acme", "chat"), "k2": ("globex", "search")}


def test_end_to_end_telemetry_shapes() -> None:
    """loadgen -> gateway -> engine in-process; check spans/logs carry the fields observe-d joins on."""
    eng_tel, eng_spans, eng_logs = tel()
    gw_tel, gw_spans, gw_logs = tel()
    lg_tel, lg_spans, _ = tel()

    async def go() -> None:
        eng = Engine(EngineConfig(model="m", itl_ms=0.2, prefill_us_per_token=2, pod="engine-0"), eng_tel)
        async with TestServer(engine_app(eng)) as es:
            gw = Gateway(gw_tel, {"k": ("acme", "chat")}, f"127.0.0.1:{es.port},", "pool")
            gw.discovery = f"127.0.0.1:{es.port}"
            async with TestServer(gateway_app(gw)) as gs:
                for _ in range(40):  # wait for the first endpoint poll
                    if any(e.ready for e in gw.scheduler.endpoints.values()):
                        break
                    await asyncio.sleep(0.05)
                lg = LoadGen(lg_tel, f"http://127.0.0.1:{gs.port}", 50, [AppProfile("k", "chat", "m", 300, 16)])
                await lg.run(duration_s=0.6)
                assert lg.codes.get(200, 0) > 0, lg.codes

    asyncio.run(go())
    for t in (eng_tel, gw_tel, lg_tel):
        t.tracer_provider.force_flush()
        t.logger_provider.force_flush()

    client = {s.context.trace_id: s for s in lg_spans.get_finished_spans()}
    gateway = [s for s in gw_spans.get_finished_spans() if s.name == "gateway.request"]
    route = [s for s in gw_spans.get_finished_spans() if s.name == "llm_d.route"]
    engine = [s for s in eng_spans.get_finished_spans() if s.name == "llm_request"]
    access = [r for r in gw_logs.get_finished_logs() if r.log_record.attributes["log.source"] == "gateway_access"]
    assert gateway and route and engine and access

    g = gateway[0]
    assert g.context.trace_id in client  # gateway joined the client's trace
    assert g.attributes["org.id"] == "acme"
    e = next(s for s in engine if s.context.trace_id == g.context.trace_id)
    assert e.parent is not None and e.parent.span_id == g.context.span_id  # engine parented on gateway span
    assert e.attributes["gen_ai.request.id"] == g.attributes["request.id"]
    for k in ("gen_ai.usage.prompt_tokens", "gen_ai.latency.time_to_first_token", "gen_ai.latency.e2e"):
        assert k in e.attributes
    rt = next(s for s in route if s.context.trace_id == g.context.trace_id)
    assert rt.attributes["llm_d.chosen_pod"] == "engine-0"
    log = next(r.log_record for r in access if r.log_record.trace_id == g.context.trace_id)
    assert log.attributes["org_id"] == "acme" and log.attributes["upstream_pod"] == "engine-0"
    assert log.span_id == g.context.span_id
    started = [r for r in eng_logs.get_finished_logs() if r.log_record.event_name == "engine.started"]
    assert started


def test_loadgen_caps_session_context() -> None:
    t, _, _ = tel()
    lg = LoadGen(t, "http://x", 1, [AppProfile("k", "chat", "m", 4000, 512)], seed=3)
    for _ in range(500):
        _, _, body = lg.build()
        assert body["prompt_tokens"] < 12_000 + 4000 * 8  # one turn past the cap at most
    assert all(s.context_tokens < 12_000 or s.turns_left >= 0 for s in lg.sessions["chat"])
