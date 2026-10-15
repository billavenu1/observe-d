"""Capacity-test generator: emits the per-request telemetry of the inference path directly as OTLP.

Each simulated request produces the same record set the real path produces (and the simulators emit):
  gateway.request span · llm_d.route span · vLLM llm_request span · gateway access log (log.source=gateway_access)
and, for prefill/decode-disaggregated models, a second llm_request span (prefill pod, resource llm_d.role=prefill)
plus a kv.transfer span on the decode pod, with consistent trace/span IDs, so the full pipeline
(agent -> Kafka -> gateway collector -> ClickHouse -> request join -> rollups) is exercised at a target request rate
without running engines.

Every record carries org_id / resource `loadtest.run_id`, so a run can be counted exactly afterwards:
    SELECT count() FROM llm_requests FINAL WHERE org_id = 'loadtest-<run_id>'

    python -m sim.telemetry_replay --endpoint otel-agent:4317 --rate 500 --duration 900 --burst-rate 700 \\
        --burst-at 300 --burst-duration 120
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import grpc
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2, logs_service_pb2_grpc
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.resource.v1 import resource_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

# name, pods per role, itl_ms, disaggregated (prefill + decode pods) or aggregated, KV bytes per token
MODELS = (
    ("llama-3.1-70b-instruct", 3, 28.0, True, 327_680),
    ("qwen2.5-7b-instruct", 3, 12.0, False, 57_344),
)
KV_GBPS = 40.0
APPS = ("chat-assistant", "code-review", "search-summarize", "support-bot")
SPAN_SERVER, SPAN_INTERNAL = trace_pb2.Span.SPAN_KIND_SERVER, trace_pb2.Span.SPAN_KIND_INTERNAL


def kv(attrs: dict[str, Any]) -> list[common_pb2.KeyValue]:
    out = []
    for k, v in attrs.items():
        if isinstance(v, bool):
            val = common_pb2.AnyValue(bool_value=v)
        elif isinstance(v, int):
            val = common_pb2.AnyValue(int_value=v)
        elif isinstance(v, float):
            val = common_pb2.AnyValue(double_value=v)
        else:
            val = common_pb2.AnyValue(string_value=str(v))
        out.append(common_pb2.KeyValue(key=k, value=val))
    return out


def resource(service: str, pod: str, run_id: str, role: str = "") -> resource_pb2.Resource:
    attrs = {
        "service.name": service,
        "k8s.pod.name": pod,
        "k8s.namespace.name": "loadtest",
        "k8s.node.name": "loadtest-node",
        "loadtest.run_id": run_id,
        "llm.engine": "vllm",
    }
    if role:
        attrs["llm_d.role"] = role
    return resource_pb2.Resource(attributes=kv(attrs))


@dataclass
class Batch:
    gw_spans: list[trace_pb2.Span] = field(default_factory=list)
    eng_spans: dict[tuple[str, str], list[trace_pb2.Span]] = field(default_factory=dict)  # (pod, role)
    logs: list[logs_pb2.LogRecord] = field(default_factory=list)
    requests: int = 0


class Generator:
    def __init__(self, run_id: str, seed: int = 0) -> None:
        self.run_id = run_id
        self.org = f"loadtest-{run_id}"
        # IDs must be unique across runs (same-seed runs would reuse trace IDs and collapse in llm_requests)
        self.rng = random.Random(f"{run_id}-{seed}")
        # (model, serving pod, role, prefill pods, itl_ms, kv bytes/token); P/D models route to the decode pod
        self.pods: list[tuple[str, str, str, list[str], float, int]] = []
        for m, n, itl, pd, kv_bpt in MODELS:
            short = m.split("-")[0]
            if pd:
                prefill = [f"replay-{short}-prefill-{i}" for i in range(n)]
                self.pods += [(m, f"replay-{short}-decode-{i}", "decode", prefill, itl, kv_bpt) for i in range(n)]
            else:
                self.pods += [(m, f"replay-{short}-{i}", "aggregated", [], itl, kv_bpt) for i in range(n)]

    def _engine_span(
        self,
        trace_id: bytes,
        span_id: bytes,
        parent: bytes,
        start: int,
        end: int,
        attrs: dict[str, Any],
        error: bool = False,
    ) -> trace_pb2.Span:
        span = trace_pb2.Span(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent,
            name="llm_request",
            kind=SPAN_SERVER,
            start_time_unix_nano=start,
            end_time_unix_nano=end,
            attributes=kv(attrs),
        )
        if error:
            span.status.code = trace_pb2.Status.STATUS_CODE_ERROR
        return span

    def request(self, batch: Batch, now_ns: int) -> None:
        r = self.rng
        model, pod, role, prefill_pods, itl_ms, kv_bpt = r.choice(self.pods)
        trace_id, gw_id, route_id, eng_id = r.randbytes(16), r.randbytes(8), r.randbytes(8), r.randbytes(8)
        request_id = str(uuid.UUID(bytes=r.randbytes(16), version=4))
        tin = int(r.lognormvariate(6.8, 0.7))
        cached = int(tin * r.choice((0, 0, 0.5, 0.8)))
        tout = max(1, int(r.uniform(20, 400)))
        queue, prefill = r.expovariate(50), (tin - cached) * 120e-6
        decode = (tout - 1) * itl_ms / 1000 * r.uniform(0.9, 1.4)
        status = 499 if r.random() < 0.02 else (500 if r.random() < 0.003 else 200)
        finish = {499: "abort", 500: "error"}.get(status, "length" if r.random() < 0.1 else "stop")
        prefill_pod = r.choice(prefill_pods) if prefill_pods else ""
        if prefill_pod:  # P/D: prefill pod (queue + prefill), then decode pod (wait-for-KV, KV pull, decode)
            wait = r.expovariate(200)
            kv_bytes = tin * kv_bpt
            xfer = 0.0005 + kv_bytes / (KV_GBPS * 1e9) * r.uniform(0.9, 1.3)
            failed = r.random() < 0.002
            recompute = tin * 120e-6 if failed else 0.0
            first = itl_ms / 1000
            p_e2e = queue + prefill
            d_e2e = wait + xfer + recompute + first + decode
        else:
            wait = xfer = recompute = first = 0.0
            kv_bytes, failed = 0, False
            p_e2e, d_e2e = 0.0, queue + prefill + decode
        e2e = p_e2e + d_e2e
        start = now_ns - int((e2e + 0.002) * 1e9)

        batch.gw_spans.append(
            trace_pb2.Span(
                trace_id=trace_id,
                span_id=gw_id,
                name="gateway.request",
                kind=SPAN_SERVER,
                start_time_unix_nano=start,
                end_time_unix_nano=now_ns,
                attributes=kv(
                    {
                        "gen_ai.request.model": model,
                        "request.id": request_id,
                        "org.id": self.org,
                        "http.response.status_code": status,
                    }
                ),
            )
        )
        batch.gw_spans.append(
            trace_pb2.Span(
                trace_id=trace_id,
                span_id=route_id,
                parent_span_id=gw_id,
                name="llm_d.route",
                kind=SPAN_INTERNAL,
                start_time_unix_nano=start + 50_000,
                end_time_unix_nano=start + 150_000,
                attributes=kv(
                    {
                        "gen_ai.request.model": model,
                        "llm_d.chosen_pod": pod,
                        "llm_d.prefill_pod": prefill_pod,
                        "llm_d.candidates": 3,
                        "llm_d.score.prefix": 1.0 if cached else 0.0,
                        "llm_d.score.queue": 0.8,
                        "llm_d.score.kv": 0.6,
                        "llm_d.prefix_hit_predicted": cached > 0,
                        "llm_d.fallback_reason": "",
                    }
                ),
            )
        )
        common = {
            "gen_ai.request.id": request_id,
            "gen_ai.request.model": model,
            "gen_ai.response.model": model,
            "gen_ai.request.temperature": 0.7,
            "gen_ai.request.top_p": 1.0,
            "gen_ai.usage.prompt_tokens": tin,
        }
        eng_start = start + 200_000
        if prefill_pod:
            p_end = eng_start + int(p_e2e * 1e9)
            batch.eng_spans.setdefault((prefill_pod, "prefill"), []).append(
                self._engine_span(
                    trace_id,
                    r.randbytes(8),
                    gw_id,
                    eng_start,
                    p_end,
                    {
                        **common,
                        "gen_ai.request.max_tokens": 1,
                        "gen_ai.usage.cached_tokens": cached,
                        "gen_ai.usage.completion_tokens": 1,
                        "gen_ai.response.finish_reasons": "length",
                        "gen_ai.latency.time_in_queue": queue,
                        "gen_ai.latency.time_to_first_token": queue + prefill,
                        "gen_ai.latency.time_in_model_prefill": prefill,
                        "gen_ai.latency.time_in_model_decode": 0.0,
                        "gen_ai.latency.e2e": p_e2e,
                        "llm.preempted": False,
                    },
                )
            )
            eng_start = p_end
            kv_start = eng_start + int(wait * 1e9)
            kspan = trace_pb2.Span(
                trace_id=trace_id,
                span_id=r.randbytes(8),
                parent_span_id=eng_id,
                name="kv.transfer",
                kind=trace_pb2.Span.SPAN_KIND_CLIENT,
                start_time_unix_nano=kv_start,
                end_time_unix_nano=kv_start + int(xfer * 1e9),
                attributes=kv(
                    {
                        "gen_ai.request.id": request_id,
                        "gen_ai.request.model": model,
                        "llm_d.kv.connector": "nixl",
                        "llm_d.kv.source_pod": prefill_pod,
                        "llm_d.kv.target_pod": pod,
                        "llm_d.kv.bytes": kv_bytes,
                        "llm_d.kv.status": "failed" if failed else "ok",
                        "llm_d.kv.fallback": "recompute" if failed else "",
                    }
                ),
            )
            if failed:
                kspan.status.code = trace_pb2.Status.STATUS_CODE_ERROR
            batch.eng_spans.setdefault((pod, role), []).append(kspan)
            serving = {
                "gen_ai.usage.cached_tokens": 0,
                "gen_ai.latency.time_in_queue": wait,
                "gen_ai.latency.time_to_first_token": wait + xfer + recompute + first,
                "gen_ai.latency.time_in_model_prefill": recompute,
            }
        else:
            serving = {
                "gen_ai.usage.cached_tokens": cached,
                "gen_ai.latency.time_in_queue": queue,
                "gen_ai.latency.time_to_first_token": queue + prefill,
                "gen_ai.latency.time_in_model_prefill": prefill,
            }
        batch.eng_spans.setdefault((pod, role), []).append(
            self._engine_span(
                trace_id,
                eng_id,
                gw_id,
                eng_start,
                now_ns - 1_000_000,
                {
                    **common,
                    **serving,
                    "gen_ai.request.max_tokens": 512,
                    "gen_ai.usage.completion_tokens": tout,
                    "gen_ai.response.finish_reasons": finish,
                    "gen_ai.latency.time_in_model_decode": decode,
                    "gen_ai.latency.e2e": d_e2e,
                    "llm.preempted": False,
                },
                error=status == 500,
            )
        )
        batch.logs.append(
            logs_pb2.LogRecord(
                time_unix_nano=start,
                observed_time_unix_nano=now_ns,
                severity_number=logs_pb2.SEVERITY_NUMBER_INFO,
                severity_text="INFO",
                body=common_pb2.AnyValue(string_value=f"POST /v1/completions {status}"),
                trace_id=trace_id,
                span_id=gw_id,
                attributes=kv(
                    {
                        "log.source": "gateway_access",
                        "request_id": request_id,
                        "org_id": self.org,
                        "app_id": (app := r.choice(APPS)),
                        "api_key_id": "key_" + hashlib.sha256(f"{self.org}-{app}".encode()).hexdigest()[:16],
                        "user_id": f"u{r.randrange(5000)}",
                        "session_id": f"s{r.randrange(20000)}",
                        "model": model,
                        "upstream_pod": pod,
                        "response_code": status,
                        "duration_ms": round((e2e + 0.002) * 1000, 3),
                        "bytes_received": 1200,
                        "bytes_sent": 2400,
                    }
                ),
            )
        )
        batch.requests += 1

    def build(
        self, n: int
    ) -> tuple[trace_service_pb2.ExportTraceServiceRequest, logs_service_pb2.ExportLogsServiceRequest, int]:
        b = Batch()
        now = time.time_ns()
        for _ in range(n):
            self.request(b, now)
        rs = [
            trace_pb2.ResourceSpans(
                resource=resource("inference-gateway", "replay-gateway-0", self.run_id),
                scope_spans=[trace_pb2.ScopeSpans(spans=b.gw_spans)],
            )
        ]
        rs += [
            trace_pb2.ResourceSpans(
                resource=resource("vllm", pod, self.run_id, role), scope_spans=[trace_pb2.ScopeSpans(spans=spans)]
            )
            for (pod, role), spans in b.eng_spans.items()
        ]
        logs = logs_service_pb2.ExportLogsServiceRequest(
            resource_logs=[
                logs_pb2.ResourceLogs(
                    resource=resource("inference-gateway", "replay-gateway-0", self.run_id),
                    scope_logs=[logs_pb2.ScopeLogs(log_records=b.logs)],
                )
            ]
        )
        return trace_service_pb2.ExportTraceServiceRequest(resource_spans=rs), logs, b.requests


@dataclass
class Stats:
    sent: int = 0  # requests whose access log was accepted (a request "exists" once the gateway logged it)
    failed_batches: int = 0  # batches whose access logs could not be delivered (not counted in `sent`)
    span_failed_requests: int = 0  # requests whose logs landed but spans did not (=> incomplete joins)
    lag_s: float = 0.0


def rate_at(t: float, a: argparse.Namespace) -> float:
    if a.burst_rate and a.burst_at <= t < a.burst_at + a.burst_duration:
        return float(a.burst_rate)
    return float(a.rate)


async def run(a: argparse.Namespace) -> Stats:
    gen = Generator(a.run_id, a.seed)
    stats = Stats()
    tick = 0.2
    async with grpc.aio.insecure_channel(a.endpoint, options=[("grpc.max_send_message_length", 64 << 20)]) as ch:
        traces = trace_service_pb2_grpc.TraceServiceStub(ch)  # type: ignore[no-untyped-call]
        logs = logs_service_pb2_grpc.LogsServiceStub(ch)  # type: ignore[no-untyped-call]
        t0 = time.monotonic()
        owed = 0.0
        last_report = t0
        pending: set[asyncio.Task[None]] = set()

        async def export(call, req) -> bool:  # type: ignore[no-untyped-def]
            for attempt in range(6):
                try:
                    await call(req, timeout=30)
                    return True
                except grpc.aio.AioRpcError:
                    await asyncio.sleep(0.5 * 2**attempt)
            return False

        async def send(n: int) -> None:
            tr, lg, count = gen.build(n)
            # logs first: a request counts as sent once its access log is accepted; spans are tracked apart
            if not await export(logs.Export, lg):
                stats.failed_batches += 1
                return
            stats.sent += count
            if not await export(traces.Export, tr):
                stats.span_failed_requests += count

        while (elapsed := time.monotonic() - t0) < a.duration:
            owed += rate_at(elapsed, a) * tick
            n = int(owed)
            owed -= n
            if n:
                t = asyncio.create_task(send(n))
                pending.add(t)
                t.add_done_callback(pending.discard)
            next_tick = t0 + (int(elapsed / tick) + 1) * tick
            stats.lag_s = max(0.0, time.monotonic() - next_tick)
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
            if time.monotonic() - last_report >= 30:
                print(
                    json.dumps(
                        {
                            "elapsed_s": round(elapsed),
                            "sent": stats.sent,
                            "inflight": len(pending),
                            "failed_batches": stats.failed_batches,
                            "target_rps": rate_at(elapsed, a),
                        }
                    ),
                    flush=True,
                )
                last_report = time.monotonic()
        if pending:
            await asyncio.gather(*pending)
    return stats


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--endpoint", default=os.environ.get("OTLP_ENDPOINT", "localhost:4317"))
    p.add_argument("--rate", type=float, default=float(os.environ.get("RATE", "500")))
    p.add_argument("--duration", type=float, default=float(os.environ.get("DURATION", "900")))
    p.add_argument("--burst-rate", type=float, default=float(os.environ.get("BURST_RATE", "0")))
    p.add_argument("--burst-at", type=float, default=float(os.environ.get("BURST_AT", "300")))
    p.add_argument("--burst-duration", type=float, default=float(os.environ.get("BURST_DURATION", "120")))
    p.add_argument("--run-id", default=os.environ.get("RUN_ID", uuid.uuid4().hex[:8]))
    p.add_argument("--seed", type=int, default=int(os.environ.get("JOB_COMPLETION_INDEX", "0")))
    a = p.parse_args(argv)
    print(
        json.dumps(
            {
                "run_id": a.run_id,
                "org_id": f"loadtest-{a.run_id}",
                "rate": a.rate,
                "duration_s": a.duration,
                "burst_rate": a.burst_rate,
            }
        ),
        flush=True,
    )
    stats = asyncio.run(run(a))
    print(
        json.dumps(
            {
                "done": True,
                "run_id": a.run_id,
                "sent": stats.sent,
                "failed_batches": stats.failed_batches,
                "span_failed_requests": stats.span_failed_requests,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
