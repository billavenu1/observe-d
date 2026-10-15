"""Simulated llm-d inference stack for GPU-less development: gateway+EPP, vLLM engine, load generator.

The simulators emit the telemetry *shapes* the real components emit (or that observe-d configures them to emit):

  engine   vLLM-like: /metrics with vllm:* series; one OTLP span `llm_request` per request with vLLM's
           gen_ai.* attributes (parented on the incoming traceparent); startup + NCCL/RCCL logs.
  gateway  Envoy Inference Gateway + llm-d EPP: span `gateway.request`, child span `llm_d.route` with the
           routing decision, an OTLP access log (log.source=gateway_access, header-contract fields), and
           Gateway-API-Inference-Extension-style inference_pool_* / inference_objective_* metrics.
  loadgen  an instrumented client app: span `gen_ai.chat` (CLIENT), identity headers, multi-turn sessions,
           occasional client aborts.

Names marked "(verify)" in docs/telemetry-shapes.md are modeled on upstream docs and must be checked
against the real components (M1 spike / M8).

    python -m sim.inference_stack engine   --port 8000
    python -m sim.inference_stack gateway  --port 8080
    python -m sim.inference_stack loadgen  --target http://gateway:8080 --rate 30
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import socket
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web
from opentelemetry import propagate
from opentelemetry._logs import SeverityNumber
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from sim.telemetry import Telemetry, k8s_resource, setup

LATENCY_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0)
ITL_BUCKETS = (0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1, 0.2, 0.5)
MAX_CONTEXT_TOKENS = 12_000
ENGINE_SCENARIOS = ("normal", "slow", "errors", "kv_pressure", "nccl_error")


def _now_ns() -> int:
    return time.time_ns()


async def _health(_: web.Request) -> web.Response:
    return web.Response(text="ok")


BG_TASK = web.AppKey("bg_task", asyncio.Task)


def _metrics_response(registry: CollectorRegistry) -> web.Response:
    return web.Response(body=generate_latest(registry), content_type="text/plain")


# ============================================================================================ engine


@dataclass
class EngineConfig:
    model: str = "llama-3.1-8b-instruct"
    engine_version: str = "0.28.0-sim"
    tp: int = 1
    max_num_seqs: int = 64
    kv_capacity_tokens: int = 200_000
    itl_ms: float = 18.0
    prefill_us_per_token: float = 120.0
    scenario: str = "normal"
    vendor: str = "nvidia"
    pod: str = field(default_factory=lambda: os.environ.get("POD_NAME", socket.gethostname()))

    @classmethod
    def from_env(cls) -> EngineConfig:
        e = os.environ
        return cls(
            model=e.get("MODEL", cls.model),
            tp=int(e.get("TP", "1")),
            max_num_seqs=int(e.get("MAX_NUM_SEQS", "64")),
            kv_capacity_tokens=int(e.get("KV_CAPACITY_TOKENS", "200000")),
            itl_ms=float(e.get("ITL_MS", "18")),
            scenario=e.get("SCENARIO", "normal"),
            vendor=e.get("GPU_VENDOR", "nvidia"),
        )


@dataclass
class RequestResult:
    status: int
    finish_reason: str
    tokens_in: int
    tokens_cached: int
    tokens_out: int
    queue_s: float
    prefill_s: float
    decode_s: float
    ttft_s: float
    e2e_s: float
    preempted: bool


class Engine:
    def __init__(self, cfg: EngineConfig, tel: Telemetry, rng: random.Random | None = None) -> None:
        if cfg.scenario not in ENGINE_SCENARIOS:
            raise ValueError(f"unknown engine scenario {cfg.scenario!r}")
        self.cfg, self.tel = cfg, tel
        self.rng = rng or random.Random()
        self.running = 0
        self.waiting = 0
        self.kv_tokens = 0
        self.prefix_cache: OrderedDict[str, int] = OrderedDict()
        self.slots = asyncio.Semaphore(cfg.max_num_seqs)
        self.registry = CollectorRegistry()
        lbl = ["model_name"]
        r = self.registry
        self.m_running = Gauge("vllm:num_requests_running", "Running requests", lbl, registry=r)
        self.m_waiting = Gauge("vllm:num_requests_waiting", "Waiting requests", lbl, registry=r)
        self.m_kv = Gauge("vllm:kv_cache_usage_perc", "KV-cache usage (0-1)", lbl, registry=r)
        self.m_pc_queries = Counter("vllm:prefix_cache_queries", "Prefix cache queried tokens", lbl, registry=r)
        self.m_pc_hits = Counter("vllm:prefix_cache_hits", "Prefix cache hit tokens", lbl, registry=r)
        self.m_preempt = Counter("vllm:num_preemptions", "Preemptions", lbl, registry=r)
        self.m_prompt = Counter("vllm:prompt_tokens", "Prompt tokens", lbl, registry=r)
        self.m_gen = Counter("vllm:generation_tokens", "Generation tokens", lbl, registry=r)
        self.m_success = Counter("vllm:request_success", "Finished requests", [*lbl, "finished_reason"], registry=r)
        self.m_ttft = Histogram("vllm:time_to_first_token_seconds", "TTFT", lbl, buckets=LATENCY_BUCKETS, registry=r)
        self.m_itl = Histogram("vllm:inter_token_latency_seconds", "ITL", lbl, buckets=ITL_BUCKETS, registry=r)
        self.m_e2e = Histogram("vllm:e2e_request_latency_seconds", "E2E", lbl, buckets=LATENCY_BUCKETS, registry=r)
        self.m_queue = Histogram(
            "vllm:request_queue_time_seconds", "Queue time", lbl, buckets=LATENCY_BUCKETS, registry=r
        )
        self.m_prefill = Histogram(
            "vllm:request_prefill_time_seconds", "Prefill time", lbl, buckets=LATENCY_BUCKETS, registry=r
        )
        self.m_decode = Histogram(
            "vllm:request_decode_time_seconds", "Decode time", lbl, buckets=LATENCY_BUCKETS, registry=r
        )
        self._update_gauges()

    # -- state ---------------------------------------------------------------------------------
    @property
    def kv_capacity(self) -> int:
        cap = self.cfg.kv_capacity_tokens
        return cap // 5 if self.cfg.scenario == "kv_pressure" else cap

    def kv_usage(self) -> float:
        return min(1.0, self.kv_tokens / self.kv_capacity)

    def state(self) -> dict[str, Any]:
        return {
            "pod": self.cfg.pod,
            "model": self.cfg.model,
            "running": self.running,
            "waiting": self.waiting,
            "kv_usage": round(self.kv_usage(), 4),
            "max_num_seqs": self.cfg.max_num_seqs,
            "load": round(min(1.0, (self.running + self.waiting) / self.cfg.max_num_seqs), 4),
            "tp": self.cfg.tp,
        }

    def _update_gauges(self) -> None:
        m = self.cfg.model
        self.m_running.labels(m).set(self.running)
        self.m_waiting.labels(m).set(self.waiting)
        self.m_kv.labels(m).set(self.kv_usage())

    def _lookup_prefix(self, prefix_id: str, prefix_tokens: int, prompt_tokens: int) -> int:
        if not prefix_id or prefix_tokens <= 0:
            return 0
        hit = prefix_id in self.prefix_cache
        if hit:
            self.prefix_cache.move_to_end(prefix_id)
        return min(prefix_tokens, prompt_tokens) if hit else 0

    def _remember_prefix(self, prefix_id: str, tokens: int) -> None:
        if not prefix_id:
            return
        self.prefix_cache[prefix_id] = tokens
        self.prefix_cache.move_to_end(prefix_id)
        while len(self.prefix_cache) > 2000:
            self.prefix_cache.popitem(last=False)

    # -- request -------------------------------------------------------------------------------
    async def run(self, req: dict[str, Any]) -> RequestResult:
        cfg = self.cfg
        t0 = time.monotonic()
        prompt = int(req.get("prompt_tokens", 512))
        max_tokens = int(req.get("max_tokens", 256))
        prefix_id = str(req.get("prefix_id", ""))
        cached = 0
        self.waiting += 1
        self._update_gauges()
        async with self.slots:
            self.waiting -= 1
            t_sched = time.monotonic()
            cached = self._lookup_prefix(prefix_id, int(req.get("prefix_tokens", 0)), prompt)
            self.m_pc_queries.labels(cfg.model).inc(prompt)
            self.m_pc_hits.labels(cfg.model).inc(cached)
            reserve = prompt + max_tokens
            self.running += 1
            self.kv_tokens += reserve
            preempted = self.kv_usage() > 0.92
            self._update_gauges()
            try:
                prefill_s = (prompt - cached) * cfg.prefill_us_per_token / 1e6 * (1 + 0.5 * self.kv_usage())
                if preempted:
                    self.m_preempt.labels(cfg.model).inc()
                    prefill_s *= 1.8  # recompute after preemption
                await asyncio.sleep(prefill_s)
                t_first = time.monotonic()

                load = self.running / cfg.max_num_seqs
                itl = cfg.itl_ms / 1000 * (1 + 0.8 * load) * (4.0 if cfg.scenario == "slow" else 1.0)
                n_out = max(1, int(max_tokens * self.rng.uniform(0.3, 1.0)))
                finish = "length" if n_out == max_tokens else "stop"
                status = 200
                abort_at = req.get("abort_at_token")
                if abort_at is not None and int(abort_at) < n_out:
                    n_out, finish, status = int(abort_at), "abort", 499
                err_rate = {"errors": 0.10, "nccl_error": 0.02}.get(cfg.scenario, 0.002)
                if self.rng.random() < err_rate:
                    n_out, finish, status = max(1, n_out // 3), "error", 500
                await asyncio.sleep(itl * (n_out - 1))
            finally:
                self.running -= 1
                self.kv_tokens -= reserve
                self._update_gauges()
        t_end = time.monotonic()
        self._remember_prefix(prefix_id, prompt + n_out)

        res = RequestResult(
            status=status,
            finish_reason=finish,
            tokens_in=prompt,
            tokens_cached=cached,
            tokens_out=n_out,
            queue_s=t_sched - t0,
            prefill_s=t_first - t_sched,
            decode_s=t_end - t_first,
            ttft_s=t_first - t0,
            e2e_s=t_end - t0,
            preempted=preempted,
        )
        m = cfg.model
        self.m_prompt.labels(m).inc(prompt)
        self.m_gen.labels(m).inc(n_out)
        self.m_success.labels(m, finish).inc()
        self.m_ttft.labels(m).observe(res.ttft_s)
        if n_out > 1:
            self.m_itl.labels(m).observe(res.decode_s / (n_out - 1))
        self.m_e2e.labels(m).observe(res.e2e_s)
        self.m_queue.labels(m).observe(res.queue_s)
        self.m_prefill.labels(m).observe(res.prefill_s)
        self.m_decode.labels(m).observe(res.decode_s)
        return res

    def emit_span(self, req: dict[str, Any], headers: dict[str, str], res: RequestResult, start_ns: int) -> None:
        """One span per request, shaped like vLLM's OTLP request span."""
        ctx = propagate.extract(headers)
        span = self.tel.tracer.start_span("llm_request", context=ctx, kind=SpanKind.SERVER, start_time=start_ns)
        attrs: dict[str, Any] = {
            "gen_ai.request.id": headers.get("x-request-id", ""),
            "gen_ai.request.model": self.cfg.model,
            "gen_ai.response.model": self.cfg.model,
            "gen_ai.request.max_tokens": int(req.get("max_tokens", 0)),
            "gen_ai.request.temperature": float(req.get("temperature", 0.7)),
            "gen_ai.request.top_p": float(req.get("top_p", 1.0)),
            "gen_ai.request.n": 1,
            "gen_ai.usage.prompt_tokens": res.tokens_in,
            "gen_ai.usage.completion_tokens": res.tokens_out,
            "gen_ai.usage.cached_tokens": res.tokens_cached,
            "gen_ai.response.finish_reasons": res.finish_reason,
            "gen_ai.latency.time_in_queue": res.queue_s,
            "gen_ai.latency.time_to_first_token": res.ttft_s,
            "gen_ai.latency.time_in_model_prefill": res.prefill_s,
            "gen_ai.latency.time_in_model_decode": res.decode_s,
            "gen_ai.latency.e2e": res.e2e_s,
            "llm.preempted": res.preempted,
        }
        span.set_attributes(attrs)
        if res.status >= 500:
            span.set_status(Status(StatusCode.ERROR, res.finish_reason))
        span.end(end_time=start_ns + int(res.e2e_s * 1e9))

    def startup_log(self) -> None:
        c = self.cfg
        self.tel.log(
            f"vLLM engine started: model={c.model} tp={c.tp} max_num_seqs={c.max_num_seqs}",
            {
                "log.source": "engine",
                "llm.model": c.model,
                "llm.engine": "vllm",
                "llm.engine_version": c.engine_version,
                "llm.tp": c.tp,
                "llm.max_num_seqs": c.max_num_seqs,
                "llm.dtype": "bfloat16",
                "gpu.vendor": c.vendor,
            },
            event_name="engine.started",
        )

    async def nccl_errors(self) -> None:
        lib = "RCCL" if self.cfg.vendor == "amd" else "NCCL"
        while True:
            await asyncio.sleep(20)
            if self.cfg.scenario == "nccl_error":
                self.tel.log(
                    f"{lib} WARN NET/IB : Got completion from peer with error 12, opcode 0, len 0, vendor err 129",
                    {"log.source": "engine", "collective.library": lib, "collective.error": True},
                    severity=SeverityNumber.ERROR,
                )


def engine_app(engine: Engine) -> web.Application:
    async def completions(request: web.Request) -> web.Response:
        start_ns = _now_ns()
        body = await request.json()
        headers = {k.lower(): v for k, v in request.headers.items()}
        res = await engine.run(body)
        engine.emit_span(body, headers, res, start_ns)
        payload = {
            "id": headers.get("x-request-id", ""),
            "model": engine.cfg.model,
            "finish_reason": res.finish_reason,
            "usage": {
                "prompt_tokens": res.tokens_in,
                "completion_tokens": res.tokens_out,
                "cached_tokens": res.tokens_cached,
            },
        }
        return web.json_response(payload, status=res.status)

    async def state(_: web.Request) -> web.Response:
        return web.json_response(engine.state())

    async def metrics(_: web.Request) -> web.Response:
        return _metrics_response(engine.registry)

    async def on_start(app: web.Application) -> None:
        engine.startup_log()
        app[BG_TASK] = asyncio.create_task(engine.nccl_errors())

    app = web.Application()
    app.add_routes(
        [
            web.post("/v1/completions", completions),
            web.get("/sim/state", state),
            web.get("/metrics", metrics),
            web.get("/health", _health),
        ]
    )
    app.on_startup.append(on_start)
    return app


# =========================================================================================== gateway


def parse_api_keys(spec: str) -> dict[str, tuple[str, str]]:
    """'key:org:app,key2:org2:app2' -> {key: (org, app)}"""
    out: dict[str, tuple[str, str]] = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        key, org, app = item.split(":")
        out[key] = (org, app)
    return out


@dataclass
class Endpoint:
    address: str
    pod: str = ""
    model: str = ""
    running: int = 0
    waiting: int = 0
    kv_usage: float = 0.0
    max_num_seqs: int = 64
    inflight: int = 0
    ready: bool = False


@dataclass
class RouteDecision:
    endpoint: Endpoint | None
    candidates: int
    score_prefix: float = 0.0
    score_queue: float = 0.0
    score_kv: float = 0.0
    prefix_hit_predicted: bool = False
    fallback_reason: str = ""


class Scheduler:
    """llm-d EPP-like scoring: prefix affinity + queue depth + KV usage."""

    WEIGHTS = (0.4, 0.4, 0.2)

    def __init__(self) -> None:
        self.endpoints: dict[str, Endpoint] = {}
        self.prefix_index: OrderedDict[str, str] = OrderedDict()  # prefix_id -> endpoint address

    def pick(self, model: str, prefix_id: str) -> RouteDecision:
        cands = [e for e in self.endpoints.values() if e.ready and e.model == model]
        if not cands:
            return RouteDecision(None, 0, fallback_reason="no_endpoint_available")
        owner = self.prefix_index.get(prefix_id) if prefix_id else None
        best: tuple[float, Endpoint, float, float, float] | None = None
        for e in cands:
            sp = 1.0 if owner == e.address else 0.0
            sq = 1.0 - min(1.0, (e.waiting + e.inflight) / e.max_num_seqs)
            sk = 1.0 - e.kv_usage
            total = self.WEIGHTS[0] * sp + self.WEIGHTS[1] * sq + self.WEIGHTS[2] * sk
            if best is None or total > best[0]:
                best = (total, e, sp, sq, sk)
        assert best is not None
        _, chosen, sp, sq, sk = best
        if prefix_id:
            self.prefix_index[prefix_id] = chosen.address
            self.prefix_index.move_to_end(prefix_id)
            while len(self.prefix_index) > 20000:
                self.prefix_index.popitem(last=False)
        reason = "" if owner is None or owner == chosen.address else "prefix_owner_overloaded"
        return RouteDecision(chosen, len(cands), sp, sq, sk, prefix_hit_predicted=sp == 1.0, fallback_reason=reason)


class Gateway:
    def __init__(self, tel: Telemetry, api_keys: dict[str, tuple[str, str]], discovery: str, pool: str) -> None:
        self.tel, self.api_keys, self.discovery, self.pool = tel, api_keys, discovery, pool
        self.scheduler = Scheduler()
        self.session: ClientSession | None = None
        r = self.registry = CollectorRegistry()
        self.m_req = Counter("inference_objective_request", "Requests", ["model_name", "target_model_name"], registry=r)
        self.m_err = Counter("inference_objective_request_error", "Errors", ["model_name", "error_code"], registry=r)
        self.m_dur = Histogram(
            "inference_objective_request_duration_seconds",
            "Duration",
            ["model_name"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.m_in = Counter("inference_objective_input_tokens", "Input tokens", ["model_name"], registry=r)
        self.m_out = Counter("inference_objective_output_tokens", "Output tokens", ["model_name"], registry=r)
        self.m_pool_kv = Gauge("inference_pool_average_kv_cache_utilization", "Avg KV usage", ["name"], registry=r)
        self.m_pool_q = Gauge("inference_pool_average_queue_size", "Avg queue size", ["name"], registry=r)
        self.m_pool_ready = Gauge("inference_pool_ready_pods", "Ready pods", ["name"], registry=r)
        self.m_route = Histogram(
            "llm_d_epp_routing_decision_duration_seconds",
            "EPP decision time",
            buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05),
            registry=r,
        )
        self.m_route_pick = Counter(
            "llm_d_epp_routing_picks", "Picks per pod", ["pod", "prefix_hit_predicted"], registry=r
        )
        self.m_noep = Counter("llm_d_epp_no_endpoint_available", "No endpoint available", registry=r)

    def _resolve(self) -> list[str]:
        host, _, port = self.discovery.rpartition(":")
        if "," in self.discovery:
            return [a.strip() for a in self.discovery.split(",")]
        try:
            infos = socket.getaddrinfo(host, int(port), proto=socket.IPPROTO_TCP)
        except OSError:
            return []
        return sorted({f"{i[4][0]}:{port}" for i in infos})

    async def poll_endpoints(self) -> None:
        assert self.session is not None
        while True:
            addrs = await asyncio.get_running_loop().run_in_executor(None, self._resolve)
            eps = self.scheduler.endpoints
            for gone in set(eps) - set(addrs):
                del eps[gone]
            for addr in addrs:
                ep = eps.setdefault(addr, Endpoint(addr))
                try:
                    async with self.session.get(f"http://{addr}/sim/state", timeout=ClientTimeout(total=1)) as r:
                        s = await r.json()
                    ep.pod, ep.model, ep.running, ep.waiting = (
                        s["pod"],
                        s["model"],
                        s["running"],
                        s["waiting"],
                    )
                    ep.kv_usage, ep.max_num_seqs, ep.ready = s["kv_usage"], s["max_num_seqs"], True
                except Exception:
                    ep.ready = False
            ready = [e for e in eps.values() if e.ready]
            self.m_pool_ready.labels(self.pool).set(len(ready))
            if ready:
                self.m_pool_kv.labels(self.pool).set(sum(e.kv_usage for e in ready) / len(ready))
                self.m_pool_q.labels(self.pool).set(sum(e.waiting for e in ready) / len(ready))
            await asyncio.sleep(0.5)

    async def handle(self, request: web.Request) -> web.Response:
        assert self.session is not None
        start, start_ns = time.monotonic(), _now_ns()
        raw = await request.read()
        headers = {k.lower(): v for k, v in request.headers.items()}
        body: dict[str, Any] = json.loads(raw or b"{}")
        model = str(body.get("model", ""))
        request_id = headers.get("x-request-id") or str(uuid.uuid4())
        key = headers.get("authorization", "").removeprefix("Bearer ").strip()
        org, app = self.api_keys.get(key, ("unknown", "unknown"))

        parent = propagate.extract(headers)
        with self.tel.tracer.start_as_current_span("gateway.request", context=parent, kind=SpanKind.SERVER) as gspan:
            gspan.set_attributes(
                {
                    "http.request.method": request.method,
                    "url.path": request.path,
                    "gen_ai.request.model": model,
                    "request.id": request_id,
                    "org.id": org,
                    "app.id": app,
                }
            )
            upstream_pod, status, resp_body = "", 401, b'{"error":"invalid api key"}'
            if key in self.api_keys:
                with self.tel.tracer.start_as_current_span("llm_d.route", kind=SpanKind.INTERNAL) as rspan:
                    t = time.perf_counter()
                    d = self.scheduler.pick(model, str(body.get("prefix_id", "")))
                    self.m_route.observe(time.perf_counter() - t)
                    rspan.set_attributes(
                        {
                            "llm_d.candidates": d.candidates,
                            "llm_d.chosen_pod": d.endpoint.pod if d.endpoint else "",
                            "llm_d.score.prefix": d.score_prefix,
                            "llm_d.score.queue": round(d.score_queue, 4),
                            "llm_d.score.kv": round(d.score_kv, 4),
                            "llm_d.prefix_hit_predicted": d.prefix_hit_predicted,
                            "llm_d.fallback_reason": d.fallback_reason,
                            "gen_ai.request.model": model,
                        }
                    )
                if d.endpoint is None:
                    self.m_noep.inc()
                    status, resp_body = 503, b'{"error":"no endpoint available"}'
                else:
                    ep = d.endpoint
                    upstream_pod = ep.pod
                    self.m_route_pick.labels(ep.pod, str(d.prefix_hit_predicted).lower()).inc()
                    out_headers = {"content-type": "application/json", "x-request-id": request_id}
                    propagate.inject(out_headers)
                    ep.inflight += 1
                    try:
                        async with self.session.post(
                            f"http://{ep.address}/v1/completions",
                            data=raw,
                            headers=out_headers,
                            timeout=ClientTimeout(total=300),
                        ) as r:
                            status, resp_body = r.status, await r.read()
                    except Exception:
                        status, resp_body = 502, b'{"error":"upstream failure"}'
                    finally:
                        ep.inflight -= 1
            gspan.set_attribute("http.response.status_code", status)
            if status >= 500:
                gspan.set_status(Status(StatusCode.ERROR))
            duration = time.monotonic() - start

            self.m_req.labels(model, model).inc()
            self.m_dur.labels(model).observe(duration)
            if status >= 400:
                self.m_err.labels(model, str(status)).inc()
            if status in (200, 499):
                usage = json.loads(resp_body).get("usage", {})
                self.m_in.labels(model).inc(usage.get("prompt_tokens", 0))
                self.m_out.labels(model).inc(usage.get("completion_tokens", 0))

            self.tel.log(
                f"{request.method} {request.path} {status}",
                {
                    "log.source": "gateway_access",
                    "request_id": request_id,
                    "org_id": org,
                    "app_id": app,
                    "user_id": headers.get("x-user-id", ""),
                    "session_id": headers.get("x-session-id", ""),
                    "model": model,
                    "upstream_pod": upstream_pod,
                    "response_code": status,
                    "duration_ms": round(duration * 1000, 3),
                    "bytes_received": len(raw),
                    "bytes_sent": len(resp_body),
                    "method": request.method,
                    "path": request.path,
                },
                span=gspan,
                timestamp_ns=start_ns,
            )
        return web.Response(body=resp_body, status=status, content_type="application/json")


def gateway_app(gw: Gateway) -> web.Application:
    async def on_start(app: web.Application) -> None:
        gw.session = ClientSession()
        app[BG_TASK] = asyncio.create_task(gw.poll_endpoints())

    async def on_cleanup(_: web.Application) -> None:
        if gw.session:
            await gw.session.close()

    async def metrics(_: web.Request) -> web.Response:
        return _metrics_response(gw.registry)

    app = web.Application(client_max_size=4 * 1024 * 1024)
    app.add_routes(
        [
            web.post("/v1/completions", gw.handle),
            web.get("/metrics", metrics),
            web.get("/health", _health),
        ]
    )
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    return app


# =========================================================================================== loadgen


@dataclass
class AppProfile:
    key: str
    app: str
    model: str
    prompt_mean: int
    max_tokens: int


@dataclass
class Session:
    sid: str
    user: str
    turns_left: int
    context_tokens: int = 0


class LoadGen:
    def __init__(self, tel: Telemetry, target: str, rate: float, apps: list[AppProfile], seed: int = 0) -> None:
        self.tel, self.target, self.rate, self.apps = tel, target.rstrip("/"), rate, apps
        self.rng = random.Random(seed)
        self.sessions: dict[str, list[Session]] = {a.app: [] for a in apps}
        self.sent = 0
        self.codes: dict[int, int] = {}

    def _session(self, app: AppProfile) -> Session:
        pool = self.sessions[app.app]
        # sessions end after their turns or when the conversation outgrows a typical context window
        pool[:] = [s for s in pool if s.turns_left > 0 and s.context_tokens < MAX_CONTEXT_TOKENS]
        if len(pool) < 40 or self.rng.random() < 0.2:
            user = hashlib.sha256(f"{app.app}-user-{self.rng.randrange(200)}".encode()).hexdigest()[:16]
            pool.append(Session(uuid.uuid4().hex[:16], user, self.rng.randint(1, 8)))
        s = self.rng.choice(pool)
        s.turns_left -= 1
        return s

    def build(self) -> tuple[AppProfile, Session, dict[str, Any]]:
        app = self.rng.choice(self.apps)
        s = self._session(app)
        new_tokens = max(32, int(self.rng.lognormvariate(0, 0.6) * app.prompt_mean))
        body: dict[str, Any] = {
            "model": app.model,
            "prompt_tokens": s.context_tokens + new_tokens,
            "max_tokens": app.max_tokens,
            "prefix_id": s.sid,
            "prefix_tokens": s.context_tokens,
            "temperature": self.rng.choice([0.0, 0.2, 0.7, 1.0]),
            "top_p": 1.0,
        }
        if self.rng.random() < 0.02:
            body["abort_at_token"] = self.rng.randint(1, max(1, app.max_tokens // 2))
        s.context_tokens += new_tokens + app.max_tokens // 2
        return app, s, body

    async def one(self, session: ClientSession) -> None:
        app, s, body = self.build()
        with self.tel.tracer.start_as_current_span("gen_ai.chat", kind=SpanKind.CLIENT) as span:
            span.set_attributes(
                {
                    "gen_ai.operation.name": "chat",
                    "gen_ai.request.model": app.model,
                    "gen_ai.request.max_tokens": app.max_tokens,
                    "app.id": app.app,
                    "session.id": s.sid,
                }
            )
            headers = {
                "authorization": f"Bearer {app.key}",
                "x-user-id": s.user,
                "x-session-id": s.sid,
                "content-type": "application/json",
            }
            propagate.inject(headers)
            try:
                async with session.post(f"{self.target}/v1/completions", json=body, headers=headers) as r:
                    code = r.status
                    data = await r.json(content_type=None)
                usage = data.get("usage", {}) if isinstance(data, dict) else {}
                span.set_attributes(
                    {
                        "http.response.status_code": code,
                        "gen_ai.usage.input_tokens": usage.get("prompt_tokens", 0),
                        "gen_ai.usage.output_tokens": usage.get("completion_tokens", 0),
                    }
                )
            except Exception as exc:
                code = 0
                span.record_exception(exc)
            if code >= 500 or code == 0:
                span.set_status(Status(StatusCode.ERROR))
        self.codes[code] = self.codes.get(code, 0) + 1

    async def run(self, duration_s: float | None = None) -> None:
        stop = time.monotonic() + duration_s if duration_s else None
        tasks: set[asyncio.Task[None]] = set()
        async with ClientSession(timeout=ClientTimeout(total=300)) as session:
            last_report = time.monotonic()
            while stop is None or time.monotonic() < stop:
                await asyncio.sleep(self.rng.expovariate(self.rate))
                if len(tasks) < 2000:
                    t = asyncio.create_task(self.one(session))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
                    self.sent += 1
                if time.monotonic() - last_report > 30:
                    print(f"loadgen: sent={self.sent} inflight={len(tasks)} codes={self.codes}", flush=True)
                    last_report = time.monotonic()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)


DEFAULT_APPS = (
    "key-acme-chat:acme:chat-assistant:llama-3.1-70b-instruct:900:256,"
    "key-acme-code:acme:code-review:llama-3.1-70b-instruct:2500:384,"
    "key-globex-search:globex:search-summarize:qwen2.5-7b-instruct:1500:128,"
    "key-initech-support:initech:support-bot:qwen2.5-7b-instruct:600:200"
)


def parse_apps(spec: str) -> list[AppProfile]:
    out = []
    for item in filter(None, (s.strip() for s in spec.split(","))):
        key, _org, app, model, prompt_mean, max_tokens = item.split(":")
        out.append(AppProfile(key, app, model, int(prompt_mean), int(max_tokens)))
    return out


def api_keys_from_apps(spec: str) -> str:
    return ",".join(":".join(i.split(":")[:3]) for i in filter(None, (s.strip() for s in spec.split(","))))


# ============================================================================================== main


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["engine", "gateway", "loadgen"])
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    p.add_argument("--target", default=os.environ.get("TARGET", "http://localhost:8080"))
    p.add_argument("--rate", type=float, default=float(os.environ.get("RATE", "20")))
    p.add_argument("--duration", type=float, default=None)
    a = p.parse_args(argv)
    apps_spec = os.environ.get("APPS", DEFAULT_APPS)

    if a.mode == "engine":
        cfg = EngineConfig.from_env()
        tel = setup(
            k8s_resource(
                "vllm",
                {"llm.engine": "vllm", "llm.model": cfg.model, "llm.engine_version": cfg.engine_version},
            )
        )
        web.run_app(engine_app(Engine(cfg, tel)), port=a.port, access_log=None)
    elif a.mode == "gateway":
        tel = setup(k8s_resource("inference-gateway"))
        keys = parse_api_keys(os.environ.get("API_KEYS", api_keys_from_apps(apps_spec)))
        discovery = os.environ.get("ENGINE_DISCOVERY", "localhost:8000")
        gw = Gateway(tel, keys, discovery, os.environ.get("POOL_NAME", "llm-pool"))
        web.run_app(gateway_app(gw), port=a.port, access_log=None)
    else:
        tel = setup(k8s_resource("chat-frontend"))
        lg = LoadGen(tel, a.target, a.rate, parse_apps(apps_spec))
        try:
            asyncio.run(lg.run(a.duration))
        finally:
            tel.shutdown()


if __name__ == "__main__":
    main()
