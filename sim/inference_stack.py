"""Simulated llm-d inference stack for GPU-less development: gateway+EPP, vLLM engine, load generator.

The simulators emit the telemetry *shapes* the real components emit (or that observe-d configures them to emit):

  engine   vLLM-like: /metrics with vllm:* series; one OTLP span `llm_request` per request with vLLM's
           gen_ai.* attributes (parented on the incoming traceparent); startup + NCCL/RCCL logs.
           ROLE=aggregated|prefill|decode (resource `llm_d.role`, from the llm-d pod label `llm-d.ai/role`).
           In P/D mode the decode pod receives the request with `x-prefiller-host-port` (llm-d routing
           sidecar), asks the prefill pod to prefill it (max_tokens=1, do_remote_decode), waits for free KV
           blocks (wait-for-KV), pulls the prompt's KV (NIXL-like; span `kv.transfer`, child of the decode
           span; on failure it falls back to recomputing the prefill locally), then decodes. The prefill and
           decode `llm_request` spans are both children of the gateway span.
  gateway  Envoy Inference Gateway + llm-d EPP: span `gateway.request`, child span `llm_d.route` with the
           routing decision (decode + prefill pod in P/D), an OTLP access log (log.source=gateway_access,
           header-contract fields, hashed `api_key_id`), and Gateway-API-Inference-Extension-style
           inference_pool_* / inference_objective_* metrics. SCORER_WEIGHTS=prefix=..,queue=..,kv=..
  loadgen  an instrumented client app: span `gen_ai.chat` (CLIENT), identity headers, multi-turn sessions,
           a shared per-app system prompt, occasional client aborts. PROMPT_SHIFT=org:factor scales one
           org's prompt length (playbook P1).

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
from opentelemetry import propagate, trace
from opentelemetry._logs import SeverityNumber
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from sim.telemetry import Telemetry, k8s_resource, setup

LATENCY_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0)
ITL_BUCKETS = (0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1, 0.2, 0.5)
MAX_CONTEXT_TOKENS = 12_000
ENGINE_SCENARIOS = ("normal", "slow", "errors", "kv_pressure", "nccl_error", "kv_transfer_degraded")
ROLES = ("aggregated", "prefill", "decode")
SYSTEM_PROMPT_TOKENS = 200  # shared per-app system prompt (the EPP prefix index sees its blocks)
PREFILLER_HEADER = "x-prefiller-host-port"  # llm-d routing sidecar: where the decode pod gets its prefill


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
    role: str = "aggregated"
    kv_bytes_per_token: int = 131_072  # layers x kv_heads x head_dim x 2 (K,V) x 2 bytes
    kv_gbps: float = 40.0  # effective prefill->decode KV pull bandwidth (NVLink/xGMI on one node)
    kv_fail_rate: float = 0.002
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
            role=e.get("ROLE", "aggregated"),
            kv_bytes_per_token=int(e.get("KV_BYTES_PER_TOKEN", "131072")),
            kv_gbps=float(e.get("KV_GBPS", "40")),
            kv_fail_rate=float(e.get("KV_FAIL_RATE", "0.002")),
        )


@dataclass
class KvTransfer:
    """One prefill->decode KV pull, as the decode pod's connector sees it."""

    source_pod: str
    nbytes: int
    start_s: float  # offset from the decode request's start
    seconds: float
    failed: bool  # failed pulls fall back to recomputing the prefill on the decode pod


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
    kv: KvTransfer | None = None


class Engine:
    def __init__(self, cfg: EngineConfig, tel: Telemetry, rng: random.Random | None = None) -> None:
        if cfg.scenario not in ENGINE_SCENARIOS:
            raise ValueError(f"unknown engine scenario {cfg.scenario!r}")
        if cfg.role not in ROLES:
            raise ValueError(f"unknown engine role {cfg.role!r}")
        self.cfg, self.tel = cfg, tel
        self.rng = rng or random.Random()
        self.running = 0
        self.waiting = 0
        self.kv_tokens = 0
        self.prefix_cache: OrderedDict[str, int] = OrderedDict()
        self.prefix_cache_tokens = 0
        self.slots = asyncio.Semaphore(cfg.max_num_seqs)
        self.kv_freed = asyncio.Condition()  # decode requests waiting for KV blocks wake on release
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
        # KV connector (NIXL) metrics, decode side: modeled on vLLM's NixlConnector stats (verify)
        self.m_kv_xfer = Histogram(
            "vllm:nixl_xfer_time_seconds", "KV transfer time", lbl, buckets=LATENCY_BUCKETS, registry=r
        )
        self.m_kv_bytes = Counter("vllm:nixl_bytes_transferred", "KV bytes transferred", lbl, registry=r)
        self.m_kv_failed = Counter("vllm:nixl_num_failed_transfers", "Failed KV transfers", lbl, registry=r)
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
            "role": self.cfg.role,
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
        """Cached prefixes live in free KV blocks: a busy pod evicts them (LRU) as running requests need room."""
        if not prefix_id:
            return
        self.prefix_cache_tokens += tokens - self.prefix_cache.get(prefix_id, 0)
        self.prefix_cache[prefix_id] = tokens
        self.prefix_cache.move_to_end(prefix_id)
        budget = max(0, self.kv_capacity - self.kv_tokens)
        while self.prefix_cache and (len(self.prefix_cache) > 2000 or self.prefix_cache_tokens > budget):
            _, evicted = self.prefix_cache.popitem(last=False)
            self.prefix_cache_tokens -= evicted

    def _itl(self) -> float:
        load = self.running / self.cfg.max_num_seqs
        return self.cfg.itl_ms / 1000 * (1 + 0.8 * load) * (4.0 if self.cfg.scenario == "slow" else 1.0)

    def _outcome(self, req: dict[str, Any], max_tokens: int) -> tuple[int, str, int]:
        """(tokens out, finish reason, HTTP status) for one request."""
        n_out = max(1, int(max_tokens * self.rng.uniform(0.3, 1.0)))
        finish, status = ("length" if n_out == max_tokens else "stop"), 200
        abort_at = req.get("abort_at_token")
        if abort_at is not None and int(abort_at) < n_out:
            n_out, finish, status = int(abort_at), "abort", 499
        err_rate = {"errors": 0.10, "nccl_error": 0.02}.get(self.cfg.scenario, 0.002)
        if self.rng.random() < err_rate:
            n_out, finish, status = max(1, n_out // 3), "error", 500
        return n_out, finish, status

    def _observe(self, res: RequestResult) -> None:
        m = self.cfg.model
        self.m_prompt.labels(m).inc(res.tokens_in)
        self.m_gen.labels(m).inc(res.tokens_out)
        self.m_success.labels(m, res.finish_reason).inc()
        self.m_ttft.labels(m).observe(res.ttft_s)
        if res.tokens_out > 1:
            self.m_itl.labels(m).observe(res.decode_s / (res.tokens_out - 1))
        self.m_e2e.labels(m).observe(res.e2e_s)
        self.m_queue.labels(m).observe(res.queue_s)
        self.m_prefill.labels(m).observe(res.prefill_s)
        self.m_decode.labels(m).observe(res.decode_s)
        if res.kv is not None:
            self.m_kv_xfer.labels(m).observe(res.kv.seconds)
            self.m_kv_bytes.labels(m).inc(res.kv.nbytes)
            if res.kv.failed:
                self.m_kv_failed.labels(m).inc()

    async def _release(self, reserve: int) -> None:
        self.running -= 1
        self.kv_tokens -= reserve
        self._update_gauges()
        async with self.kv_freed:
            self.kv_freed.notify_all()

    # -- request -------------------------------------------------------------------------------
    async def run(self, req: dict[str, Any]) -> RequestResult:
        """Aggregated request, or the prefill half of a P/D request (the caller sends max_tokens=1)."""
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
                if cfg.role == "prefill":  # compute-bound: concurrent prefills share the GPU
                    prefill_s *= 1 + self.running / cfg.max_num_seqs
                if preempted:
                    self.m_preempt.labels(cfg.model).inc()
                    prefill_s *= 1.8  # recompute after preemption
                await asyncio.sleep(prefill_s)
                t_first = time.monotonic()
                itl = self._itl()
                n_out, finish, status = self._outcome(req, max_tokens)
                await asyncio.sleep(itl * (n_out - 1))
            finally:
                await self._release(reserve)
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
        self._observe(res)
        return res

    async def run_decode(self, req: dict[str, Any], source_pod: str) -> RequestResult:
        """Decode half of a P/D request: wait for KV blocks, pull the prompt's KV from the prefill pod, decode.

        queue_s is the decode pod's wait-for-KV (slot + free blocks); prefill_s is non-zero only when the KV
        pull failed and the prompt was recomputed locally; ttft_s counts from the decode pod's arrival."""
        cfg = self.cfg
        t0 = time.monotonic()
        prompt = int(req.get("prompt_tokens", 512))
        max_tokens = int(req.get("max_tokens", 256))
        reserve = prompt + max_tokens
        self.waiting += 1
        self._update_gauges()
        async with self.slots:
            # wait-for-KV: the pulled prompt needs free blocks (an empty pod admits even an oversized prompt)
            async with self.kv_freed:
                await self.kv_freed.wait_for(lambda: not self.kv_tokens or self.kv_tokens + reserve <= self.kv_capacity)
            self.waiting -= 1
            t_sched = time.monotonic()
            self.running += 1
            self.kv_tokens += reserve
            preempted = self.kv_usage() > 0.92
            self._update_gauges()
            try:
                degraded = cfg.scenario == "kv_transfer_degraded"
                nbytes = prompt * cfg.kv_bytes_per_token
                gbps = cfg.kv_gbps * (0.05 if degraded else 1.0)
                xfer_s = 0.0005 + nbytes / (gbps * 1e9) * self.rng.uniform(0.9, 1.3)
                failed = self.rng.random() < (0.08 if degraded else cfg.kv_fail_rate)
                if failed:
                    xfer_s *= self.rng.uniform(0.3, 1.0)  # gave up part-way
                await asyncio.sleep(xfer_s)
                t_kv = time.monotonic()
                kv = KvTransfer(source_pod, nbytes, t_sched - t0, xfer_s, failed)
                recompute_s = 0.0
                if failed:  # fallback: recompute the prefill on the decode pod
                    recompute_s = prompt * cfg.prefill_us_per_token / 1e6 * (1 + 0.5 * self.kv_usage())
                if preempted:
                    self.m_preempt.labels(cfg.model).inc()
                    recompute_s += prompt * cfg.prefill_us_per_token / 1e6
                await asyncio.sleep(recompute_s)
                t_ready = time.monotonic()
                itl = self._itl()
                await asyncio.sleep(itl)  # first decode step
                t_first = time.monotonic()
                n_out, finish, status = self._outcome(req, max_tokens)
                await asyncio.sleep(itl * (n_out - 1))
            finally:
                await self._release(reserve)
        t_end = time.monotonic()

        res = RequestResult(
            status=status,
            finish_reason=finish,
            tokens_in=prompt,
            tokens_cached=0,  # the prefix cache is used on the prefill pod
            tokens_out=n_out,
            queue_s=t_sched - t0,
            prefill_s=t_ready - t_kv,
            decode_s=t_end - t_first,
            ttft_s=t_first - t0,
            e2e_s=t_end - t0,
            preempted=preempted,
            kv=kv,
        )
        self._observe(res)
        return res

    def emit_span(self, req: dict[str, Any], headers: dict[str, str], res: RequestResult, start_ns: int) -> None:
        """One span per request, shaped like vLLM's OTLP request span (+ a `kv.transfer` child on decode)."""
        ctx = propagate.extract(headers)
        span = self.tel.tracer.start_span("llm_request", context=ctx, kind=SpanKind.SERVER, start_time=start_ns)
        request_id = headers.get("x-request-id", "")
        attrs: dict[str, Any] = {
            "gen_ai.request.id": request_id,
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
        if res.kv is not None:
            kv = res.kv
            kv_start = start_ns + int(kv.start_s * 1e9)
            kspan = self.tel.tracer.start_span(
                "kv.transfer",
                context=trace.set_span_in_context(span),
                kind=SpanKind.CLIENT,
                start_time=kv_start,
            )
            kspan.set_attributes(
                {
                    "gen_ai.request.id": request_id,
                    "gen_ai.request.model": self.cfg.model,
                    "llm_d.kv.connector": "nixl",
                    "llm_d.kv.source_pod": kv.source_pod,
                    "llm_d.kv.target_pod": self.cfg.pod,
                    "llm_d.kv.bytes": kv.nbytes,
                    "llm_d.kv.status": "failed" if kv.failed else "ok",
                    "llm_d.kv.fallback": "recompute" if kv.failed else "",
                }
            )
            if kv.failed:
                kspan.set_status(Status(StatusCode.ERROR, "kv transfer failed"))
            kspan.end(end_time=kv_start + int(kv.seconds * 1e9))
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
                "llm_d.role": c.role,
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


SESSION = web.AppKey("session", ClientSession)


async def _remote_prefill(
    session: ClientSession, prefiller: str, body: dict[str, Any], headers: dict[str, str]
) -> dict[str, Any] | None:
    """Ask the prefill pod to prefill the prompt and keep its KV for us (llm-d sidecar + NIXL connector).

    The incoming traceparent is forwarded unchanged, so the prefill span is a sibling of the decode span under
    the gateway span. Returns the prefill response, or None (the decode pod then prefills locally)."""
    pbody = {k: v for k, v in body.items() if k != "abort_at_token"}
    pbody.update(max_tokens=1, kv_transfer_params={"do_remote_decode": True})
    fwd = {k: v for k, v in headers.items() if k in ("traceparent", "tracestate", "x-request-id")}
    fwd["content-type"] = "application/json"
    try:
        async with session.post(
            f"http://{prefiller}/v1/completions", json=pbody, headers=fwd, timeout=ClientTimeout(total=120)
        ) as r:
            if r.status != 200:
                return None
            data: dict[str, Any] = await r.json()
            return data
    except Exception:
        return None


def engine_app(engine: Engine) -> web.Application:
    async def completions(request: web.Request) -> web.Response:
        start_ns = _now_ns()
        body = await request.json()
        headers = {k.lower(): v for k, v in request.headers.items()}
        prefiller = headers.get(PREFILLER_HEADER, "")
        prefill = None
        if engine.cfg.role == "decode" and prefiller:
            prefill = await _remote_prefill(request.app[SESSION], prefiller, body, headers)
        if prefill is not None:
            start_ns = _now_ns()  # the decode request starts once the prefill pod is done
            res = await engine.run_decode(body, str(prefill.get("kv_transfer_params", {}).get("remote_pod", "")))
            cached = int(prefill.get("usage", {}).get("cached_tokens", 0))
        else:
            res = await engine.run(body)
            cached = res.tokens_cached
        engine.emit_span(body, headers, res, start_ns)
        payload: dict[str, Any] = {
            "id": headers.get("x-request-id", ""),
            "model": engine.cfg.model,
            "finish_reason": res.finish_reason,
            "usage": {
                "prompt_tokens": res.tokens_in,
                "completion_tokens": res.tokens_out,
                "cached_tokens": cached,
            },
        }
        if (body.get("kv_transfer_params") or {}).get("do_remote_decode"):
            payload["kv_transfer_params"] = {"remote_pod": engine.cfg.pod, "remote_engine_id": engine.cfg.pod}
        return web.json_response(payload, status=res.status)

    async def state(_: web.Request) -> web.Response:
        return web.json_response(engine.state())

    async def metrics(_: web.Request) -> web.Response:
        return _metrics_response(engine.registry)

    async def on_start(app: web.Application) -> None:
        engine.startup_log()
        app[SESSION] = ClientSession()
        app[BG_TASK] = asyncio.create_task(engine.nccl_errors())

    async def on_cleanup(app: web.Application) -> None:
        await app[SESSION].close()

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
    app.on_cleanup.append(on_cleanup)
    return app


# =========================================================================================== gateway


def parse_api_keys(spec: str) -> dict[str, tuple[str, str]]:
    """'key:org:app,key2:org2:app2' -> {key: (org, app)}"""
    out: dict[str, tuple[str, str]] = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        key, org, app = item.split(":")
        out[key] = (org, app)
    return out


def hash_api_key(key: str) -> str:
    """Stable, non-reversible API key id for the access log (the raw key is never logged)."""
    return "key_" + hashlib.sha256(key.encode()).hexdigest()[:16] if key else ""


def parse_weights(spec: str) -> tuple[float, float, float]:
    """'prefix=0.4,queue=0.4,kv=0.2' -> (prefix, queue, kv); missing scorers keep their default."""
    w = dict(zip(("prefix", "queue", "kv"), Scheduler.DEFAULT_WEIGHTS, strict=True))
    for item in filter(None, (i.strip() for i in spec.split(","))):
        name, _, val = item.partition("=")
        if name not in w:
            raise ValueError(f"unknown scorer {name!r}")
        w[name] = float(val)
    return w["prefix"], w["queue"], w["kv"]


def format_weights(w: tuple[float, float, float]) -> str:
    return f"prefix={w[0]:g},queue={w[1]:g},kv={w[2]:g}"


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
    role: str = "aggregated"
    failed_polls: int = 0


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
    """llm-d EPP-like scoring: prefix affinity + queue depth + KV usage.

    Pods are picked per role: the serving pod (aggregated or decode) and, for a decode pod, a prefill pod
    (llm-d P/D scheduling profiles). The prefix index is per profile; a pod also scores for the app's shared
    system prompt (its blocks lead every prompt), weighted by the prompt share those blocks represent."""

    DEFAULT_WEIGHTS = (0.4, 0.4, 0.2)
    SERVE_ROLES = ("aggregated", "decode")

    def __init__(self, weights: tuple[float, float, float] = DEFAULT_WEIGHTS) -> None:
        self.weights = weights
        self.endpoints: dict[str, Endpoint] = {}
        self.prefix_index: OrderedDict[tuple[str, str], str] = OrderedDict()  # (profile, prefix) -> address

    def _remember(self, key: tuple[str, str], address: str) -> None:
        self.prefix_index[key] = address
        self.prefix_index.move_to_end(key)
        while len(self.prefix_index) > 20000:
            self.prefix_index.popitem(last=False)

    def pick(
        self,
        model: str,
        prefix_id: str,
        *,
        roles: tuple[str, ...] = SERVE_ROLES,
        system_prefix: str = "",
        system_share: float = 0.0,
    ) -> RouteDecision:
        cands = [e for e in self.endpoints.values() if e.ready and e.model == model and e.role in roles]
        if not cands:
            return RouteDecision(None, 0, fallback_reason="no_endpoint_available")
        profile = "prefill" if roles == ("prefill",) else "serve"
        owner = self.prefix_index.get((profile, prefix_id)) if prefix_id else None
        sys_owner = self.prefix_index.get((profile, "sys:" + system_prefix)) if system_prefix else None
        wp, wq, wk = self.weights
        best: tuple[float, Endpoint, float, float, float] | None = None
        for e in cands:
            sp = 1.0 if owner == e.address else (system_share if sys_owner == e.address else 0.0)
            sq = 1.0 - min(1.0, (e.waiting + e.inflight) / e.max_num_seqs)
            sk = 1.0 - e.kv_usage
            total = wp * sp + wq * sq + wk * sk
            if best is None or total > best[0]:
                best = (total, e, sp, sq, sk)
        assert best is not None
        _, chosen, sp, sq, sk = best
        if prefix_id:
            self._remember((profile, prefix_id), chosen.address)
        if system_prefix:
            self._remember((profile, "sys:" + system_prefix), chosen.address)
        reason = "" if owner is None or owner == chosen.address else "prefix_owner_overloaded"
        return RouteDecision(
            chosen, len(cands), sp, sq, sk, prefix_hit_predicted=owner == chosen.address, fallback_reason=reason
        )


class Gateway:
    def __init__(
        self,
        tel: Telemetry,
        api_keys: dict[str, tuple[str, str]],
        discovery: str,
        pool: str,
        weights: tuple[float, float, float] = Scheduler.DEFAULT_WEIGHTS,
    ) -> None:
        self.tel, self.api_keys, self.discovery, self.pool = tel, api_keys, discovery, pool
        self.scheduler = Scheduler(weights)
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
                    ep.role, ep.failed_polls = s.get("role", "aggregated"), 0
                except Exception:
                    # a busy engine can miss one poll; drop it from the pool only after repeated misses
                    ep.failed_polls += 1
                    ep.ready = ep.ready and ep.failed_polls < 3
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
                prompt_tokens = max(1, int(body.get("prompt_tokens", 1)))
                sys_prefix = str(body.get("system_prefix_id", ""))
                sys_share = min(1.0, int(body.get("system_prefix_tokens", 0)) / prompt_tokens)
                prefix_id = str(body.get("prefix_id", ""))
                with self.tel.tracer.start_as_current_span("llm_d.route", kind=SpanKind.INTERNAL) as rspan:
                    t = time.perf_counter()
                    d = self.scheduler.pick(model, prefix_id, system_prefix=sys_prefix, system_share=sys_share)
                    p: RouteDecision | None = None
                    if d.endpoint is not None and d.endpoint.role == "decode":
                        p = self.scheduler.pick(
                            model, prefix_id, roles=("prefill",), system_prefix=sys_prefix, system_share=sys_share
                        )
                        if p.endpoint is None:
                            d.fallback_reason = d.fallback_reason or "no_prefill_endpoint"
                    self.m_route.observe(time.perf_counter() - t)
                    # in P/D the prefix cache that matters is the prefill pod's
                    hint = p if p is not None and p.endpoint is not None else d
                    rspan.set_attributes(
                        {
                            "llm_d.candidates": d.candidates,
                            "llm_d.chosen_pod": d.endpoint.pod if d.endpoint else "",
                            "llm_d.prefill_pod": p.endpoint.pod if p and p.endpoint else "",
                            "llm_d.score.prefix": round(hint.score_prefix, 4),
                            "llm_d.score.queue": round(d.score_queue, 4),
                            "llm_d.score.kv": round(d.score_kv, 4),
                            "llm_d.prefix_hit_predicted": hint.prefix_hit_predicted,
                            "llm_d.fallback_reason": d.fallback_reason,
                            "llm_d.scorer_weights": format_weights(self.scheduler.weights),
                            "gen_ai.request.model": model,
                        }
                    )
                if d.endpoint is None:
                    self.m_noep.inc()
                    status, resp_body = 503, b'{"error":"no endpoint available"}'
                else:
                    ep = d.endpoint
                    pre = p.endpoint if p is not None else None
                    upstream_pod = ep.pod
                    predicted = str(hint.prefix_hit_predicted).lower()
                    self.m_route_pick.labels(ep.pod, predicted).inc()
                    out_headers = {"content-type": "application/json", "x-request-id": request_id}
                    if pre is not None:
                        self.m_route_pick.labels(pre.pod, predicted).inc()
                        out_headers[PREFILLER_HEADER] = pre.address
                        pre.inflight += 1
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
                        if pre is not None:
                            pre.inflight -= 1
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
                    "api_key_id": hash_api_key(key),
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
        weights = format_weights(gw.scheduler.weights)
        # config marker: playbook P4 starts from "scorer weight changed"
        gw.tel.log(
            f"EPP scorer weights {weights}",
            {"log.source": "gateway", "llm_d.scorer_weights": weights, "llm_d.pool": gw.pool},
            event_name="epp.config",
        )
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
    org: str = ""


@dataclass
class Session:
    sid: str
    user: str
    turns_left: int
    context_tokens: int = 0


class LoadGen:
    def __init__(
        self,
        tel: Telemetry,
        target: str,
        rate: float,
        apps: list[AppProfile],
        seed: int = 0,
        prompt_shift: dict[str, float] | None = None,
    ) -> None:
        self.tel, self.target, self.rate, self.apps = tel, target.rstrip("/"), rate, apps
        self.prompt_shift = prompt_shift or {}  # org -> prompt-length factor (P1: RAG context bloat)
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
        scale = self.prompt_shift.get(app.org, 1.0)
        new_tokens = max(32, int(self.rng.lognormvariate(0, 0.6) * app.prompt_mean * scale))
        body: dict[str, Any] = {
            "model": app.model,
            "prompt_tokens": SYSTEM_PROMPT_TOKENS + s.context_tokens + new_tokens,
            "max_tokens": app.max_tokens,
            "prefix_id": s.sid,
            "prefix_tokens": SYSTEM_PROMPT_TOKENS + s.context_tokens if s.context_tokens else 0,
            "system_prefix_id": app.app,
            "system_prefix_tokens": SYSTEM_PROMPT_TOKENS,
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
        key, org, app, model, prompt_mean, max_tokens = item.split(":")
        out.append(AppProfile(key, app, model, int(prompt_mean), int(max_tokens), org))
    return out


def parse_prompt_shift(spec: str) -> dict[str, float]:
    """'acme:3,globex:1.5' -> {org: factor}"""
    out: dict[str, float] = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        org, _, factor = item.partition(":")
        out[org] = float(factor)
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
                {
                    "llm.engine": "vllm",
                    "llm.model": cfg.model,
                    "llm.engine_version": cfg.engine_version,
                    "llm_d.role": cfg.role,
                },
            )
        )
        web.run_app(engine_app(Engine(cfg, tel)), port=a.port, access_log=None)
    elif a.mode == "gateway":
        tel = setup(k8s_resource("inference-gateway"))
        keys = parse_api_keys(os.environ.get("API_KEYS", api_keys_from_apps(apps_spec)))
        discovery = os.environ.get("ENGINE_DISCOVERY", "localhost:8000")
        weights = parse_weights(os.environ.get("SCORER_WEIGHTS", ""))
        gw = Gateway(tel, keys, discovery, os.environ.get("POOL_NAME", "llm-pool"), weights)
        web.run_app(gateway_app(gw), port=a.port, access_log=None)
    else:
        tel = setup(k8s_resource("chat-frontend"))
        shift = parse_prompt_shift(os.environ.get("PROMPT_SHIFT", ""))
        lg = LoadGen(tel, a.target, a.rate, parse_apps(apps_spec), prompt_shift=shift)
        try:
            asyncio.run(lg.run(a.duration))
        finally:
            tel.shutdown()


if __name__ == "__main__":
    main()
