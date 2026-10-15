"""Prefill/decode-disaggregated mode of the inference simulator (docs/observability-story.md §5)."""

from __future__ import annotations

import asyncio
import random

from aiohttp.test_utils import TestServer
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import sim.inference_stack as stack
from sim.inference_stack import (
    AppProfile,
    Endpoint,
    Engine,
    EngineConfig,
    Gateway,
    LoadGen,
    Scheduler,
    engine_app,
    format_weights,
    gateway_app,
    hash_api_key,
    parse_apps,
    parse_prompt_shift,
    parse_weights,
)
from sim.telemetry import setup
from tools import dev


def tel(role: str = ""):  # type: ignore[no-untyped-def]
    spans, logs = InMemorySpanExporter(), InMemoryLogRecordExporter()
    attrs = {"service.name": "t", **({"llm_d.role": role} if role else {})}
    return setup(Resource.create(attrs), spans, logs), spans, logs


def test_scheduler_picks_per_role() -> None:
    s = Scheduler()
    s.endpoints = {
        "d": Endpoint("d", pod="pd", model="m", ready=True, role="decode"),
        "p1": Endpoint("p1", pod="pp1", model="m", ready=True, role="prefill"),
        "p2": Endpoint("p2", pod="pp2", model="m", ready=True, role="prefill"),
    }
    serve = s.pick("m", "sess")
    assert serve.endpoint is not None and serve.endpoint.role == "decode" and serve.candidates == 1
    pre = s.pick("m", "sess", roles=("prefill",))
    assert pre.endpoint is not None and pre.endpoint.role == "prefill" and pre.candidates == 2
    again = s.pick("m", "sess", roles=("prefill",))
    assert again.endpoint is pre.endpoint and again.prefix_hit_predicted  # prefix index is per profile


def test_skewed_prefix_weight_concentrates_new_sessions() -> None:
    """P4: with prefix affinity dominating, the shared system prompt pulls every new session to one pod."""

    def run(weights: str) -> dict[str, int]:
        s = Scheduler(parse_weights(weights))
        s.endpoints = {a: Endpoint(a, pod=a, model="m", ready=True, role="decode") for a in ("a", "b", "c")}
        picks: dict[str, int] = {}
        for i in range(90):
            d = s.pick("m", f"sess-{i}", system_prefix="chat", system_share=0.2)
            assert d.endpoint is not None
            d.endpoint.inflight += 1  # requests stay in flight: the queue score reflects load
            picks[d.endpoint.pod] = picks.get(d.endpoint.pod, 0) + 1
        return picks

    balanced, skewed = run(dev.DEFAULT_WEIGHTS), run(dev.SKEWED_WEIGHTS)
    assert max(balanced.values()) <= 45
    assert max(skewed.values()) == 90


def test_weights_and_shift_parsing() -> None:
    assert parse_weights("") == Scheduler.DEFAULT_WEIGHTS
    assert parse_weights("prefix=0.9,queue=0.05,kv=0.05") == (0.9, 0.05, 0.05)
    assert format_weights((0.9, 0.05, 0.05)) == "prefix=0.9,queue=0.05,kv=0.05"
    assert parse_prompt_shift("acme:3, globex:1.5") == {"acme": 3.0, "globex": 1.5}
    assert parse_apps("k:acme:chat:m:100:10")[0].org == "acme"


def test_prompt_shift_scales_one_org() -> None:
    t, _, _ = tel()
    apps = [AppProfile("k1", "chat", "m", 1000, 10, "acme"), AppProfile("k2", "bot", "m", 1000, 10, "globex")]

    def mean_new_tokens(shift: dict[str, float], org: str) -> float:
        lg = LoadGen(t, "http://x", 1, [a for a in apps if a.org == org], seed=5, prompt_shift=shift)
        bodies = [lg.build()[2] for _ in range(300)]
        return sum(b["prompt_tokens"] - b["prefix_tokens"] for b in bodies) / len(bodies)

    assert mean_new_tokens({"acme": 3}, "acme") > 2.5 * mean_new_tokens({}, "acme")
    assert mean_new_tokens({"acme": 3}, "globex") == mean_new_tokens({}, "globex")


def test_decode_kv_transfer_and_fallback() -> None:
    t, _, _ = tel()
    cfg = EngineConfig(model="m", role="decode", itl_ms=0.1, prefill_us_per_token=100, kv_bytes_per_token=1000)
    eng = Engine(cfg, t, random.Random(1))

    async def go() -> None:
        ok = await eng.run_decode({"prompt_tokens": 400, "max_tokens": 5}, "prefill-0")
        assert ok.kv is not None and not ok.kv.failed and ok.kv.nbytes == 400_000
        assert ok.kv.source_pod == "prefill-0" and ok.prefill_s < 0.02 and ok.tokens_cached == 0
        eng.cfg.kv_fail_rate = 1.0
        bad = await eng.run_decode({"prompt_tokens": 400, "max_tokens": 5}, "prefill-0")
        assert bad.kv is not None and bad.kv.failed
        assert bad.prefill_s >= 400 * 100e-6 * 0.9  # recomputed locally (40 ms)
        assert eng.running == 0 and eng.kv_tokens == 0

    asyncio.run(go())


def test_decode_waits_for_kv_blocks() -> None:
    t, _, _ = tel()
    cfg = EngineConfig(model="m", role="decode", itl_ms=20, kv_capacity_tokens=1000, kv_bytes_per_token=1)
    eng = Engine(cfg, t, random.Random(2))

    async def go() -> None:
        first = asyncio.create_task(eng.run_decode({"prompt_tokens": 600, "max_tokens": 10}, "p"))
        await asyncio.sleep(0.01)
        second = await eng.run_decode({"prompt_tokens": 600, "max_tokens": 10}, "p")
        await first
        assert second.queue_s > 0.02  # waited for the first request to free its blocks

    asyncio.run(go())


def test_prefix_cache_evicts_when_kv_is_busy() -> None:
    t, _, _ = tel()
    eng = Engine(EngineConfig(model="m", kv_capacity_tokens=10_000), t)
    for i in range(5):
        eng._remember_prefix(f"s{i}", 3000)
    assert eng.prefix_cache_tokens <= 10_000 and "s0" not in eng.prefix_cache and "s4" in eng.prefix_cache
    eng.kv_tokens = 9_000  # running requests take the blocks: cached prefixes go first
    eng._remember_prefix("s5", 500)
    assert eng.prefix_cache_tokens <= 1_000 and list(eng.prefix_cache) == ["s5"]


def test_end_to_end_pd_trace() -> None:
    """loadgen -> gateway -> decode pod -> prefill pod in-process: two llm_request spans + kv.transfer."""
    pre_tel, pre_spans, _ = tel("prefill")
    dec_tel, dec_spans, _ = tel("decode")
    gw_tel, gw_spans, gw_logs = tel()
    lg_tel, _, _ = tel()

    async def go() -> None:
        pre = Engine(
            EngineConfig(model="m", itl_ms=0.2, prefill_us_per_token=2, role="prefill", pod="prefill-0"), pre_tel
        )
        dec = Engine(
            EngineConfig(model="m", itl_ms=0.2, prefill_us_per_token=2, role="decode", pod="decode-0"), dec_tel
        )
        async with TestServer(engine_app(pre)) as ps, TestServer(engine_app(dec)) as ds:
            gw = Gateway(gw_tel, {"k": ("acme", "chat")}, f"127.0.0.1:{ps.port},127.0.0.1:{ds.port}", "pool")
            async with TestServer(gateway_app(gw)) as gs:
                for _ in range(40):
                    if sum(e.ready for e in gw.scheduler.endpoints.values()) == 2:
                        break
                    await asyncio.sleep(0.05)
                lg = LoadGen(lg_tel, f"http://127.0.0.1:{gs.port}", 50, [AppProfile("k", "chat", "m", 300, 16)])
                await lg.run(duration_s=0.6)
                assert lg.codes.get(200, 0) > 0, lg.codes

    asyncio.run(go())
    for t in (pre_tel, dec_tel, gw_tel, lg_tel):
        t.tracer_provider.force_flush()
        t.logger_provider.force_flush()

    finished = gw_spans.get_finished_spans()
    gateway = {s.context.trace_id: s for s in finished if s.name == "gateway.request"}
    route = {s.context.trace_id: s for s in finished if s.name == "llm_d.route"}
    prefill = {s.context.trace_id: s for s in pre_spans.get_finished_spans() if s.name == "llm_request"}
    decode = {s.context.trace_id: s for s in dec_spans.get_finished_spans() if s.name == "llm_request"}
    xfer = {s.context.trace_id: s for s in dec_spans.get_finished_spans() if s.name == "kv.transfer"}
    access = {
        r.log_record.trace_id: r.log_record
        for r in gw_logs.get_finished_logs()
        if r.log_record.attributes.get("log.source") == "gateway_access"
    }
    assert prefill and decode and xfer
    tid = next(iter(decode))
    g, d, p, k = gateway[tid], decode[tid], prefill[tid], xfer[tid]
    assert p.parent is not None and p.parent.span_id == g.context.span_id  # both engine spans under gateway
    assert d.parent is not None and d.parent.span_id == g.context.span_id
    assert k.parent is not None and k.parent.span_id == d.context.span_id
    assert p.resource.attributes["llm_d.role"] == "prefill" and d.resource.attributes["llm_d.role"] == "decode"
    assert p.attributes["gen_ai.usage.completion_tokens"] == 1
    assert p.end_time <= d.start_time  # decode starts once the prefill pod is done
    assert k.attributes["llm_d.kv.source_pod"] == "prefill-0" and k.attributes["llm_d.kv.target_pod"] == "decode-0"
    assert k.attributes["llm_d.kv.bytes"] == d.attributes["gen_ai.usage.prompt_tokens"] * 131_072
    assert route[tid].attributes["llm_d.chosen_pod"] == "decode-0"
    assert route[tid].attributes["llm_d.prefill_pod"] == "prefill-0"
    log = access[tid]
    assert log.attributes["upstream_pod"] == "decode-0"
    assert log.attributes["api_key_id"] == hash_api_key("k") != "k"
    assert "k" not in log.attributes.values()


def test_dev_scenarios_match_simulator() -> None:
    assert dev.ENGINE_SCENARIOS == stack.ENGINE_SCENARIOS
    for flags in dev.PLAYBOOKS.values():
        assert set(flags) <= {"engine", "gpu", "load", "routing"}
    assert parse_weights(dev.SKEWED_WEIGHTS)[0] > 0.5
    assert parse_weights(dev.DEFAULT_WEIGHTS) == Scheduler.DEFAULT_WEIGHTS
    assert parse_prompt_shift(dev.PROMPT_SHIFT)["acme"] > 1
