# Telemetry shapes (contract)

What each component must emit for observe-d to work, which ClickHouse object consumes it, and whether the name
is **verified** against the real component or **modeled** (from upstream docs; the simulators emit it; must be
checked in the M1 spike / M8 hardware validation). When a real component differs, change the extraction in
the migration (or a collector `transform`), never the dashboards.

## Request path (traceability, tokenomics)

### Gateway access log — OTLP log record (Envoy OTel access logger, configured by us)
Consumer: gateway collector routes `log.source == "gateway_access"` → `gateway_access_null` → `gateway_requests`.

| Field | Where | Status |
|---|---|---|
| TraceId / SpanId of the gateway span | log record context | contract (we configure the access logger) |
| `log.source` = `gateway_access` | attribute | contract |
| `request_id`, `org_id`, `app_id`, `user_id`, `session_id` | attributes (docs/header-contract.md) | contract |
| `model`, `upstream_pod`, `response_code`, `duration_ms`, `bytes_received`, `bytes_sent` | attributes | contract |
| Timestamp = request start | record timestamp | contract |

### EPP routing span — `llm_d.route`
Consumer: `routing_decisions`.

| Attribute | Status |
|---|---|
| `llm_d.chosen_pod`, `llm_d.candidates` | modeled |
| `llm_d.score.prefix`, `llm_d.score.queue`, `llm_d.score.kv` | modeled — real EPP scorer outputs may need a span processor or EPP flag |
| `llm_d.prefix_hit_predicted`, `llm_d.fallback_reason` | modeled |
| `gen_ai.request.model` | modeled |

### vLLM request span — `llm_request` (vLLM `--otlp-traces-endpoint`)
Consumer: `engine_requests`. Parent = incoming `traceparent` (the gateway span).

| Attribute | Status |
|---|---|
| `gen_ai.request.id`, `gen_ai.response.model`, `gen_ai.request.{max_tokens,temperature,top_p,n}` | modeled on vLLM tracing (verify) |
| `gen_ai.usage.prompt_tokens`, `gen_ai.usage.completion_tokens` | modeled on vLLM tracing (verify) |
| `gen_ai.usage.cached_tokens` | **sim-only** — real vLLM may not emit it; fallback: prefix-cache counters per pod |
| `gen_ai.latency.{time_in_queue,time_to_first_token,time_in_model_prefill,time_in_model_decode,e2e}` (seconds) | modeled on vLLM tracing (verify) |
| `gen_ai.response.finish_reasons` | modeled (verify name/format) |
| `llm.preempted` | **sim-only** |
| resource `llm.engine`, `llm.engine_version`, `k8s.pod.name`, `k8s.node.name` | set via `OTEL_RESOURCE_ATTRIBUTES` + `k8sattributes` |

### Engine startup event — OTLP log, `event.name = engine.started`
Consumer: `model_deployments`. Attributes `llm.model`, `llm.engine`, `llm.engine_version`, `llm.tp`,
`llm.max_num_seqs`, `llm.dtype`, `gpu.vendor`. Real vLLM: **not emitted** — produce it from the pod spec
(k8sobjects) or a startup-log `transform`, M4 follow-up.

## Metrics

| Source | Names | Normalized by | Status |
|---|---|---|---|
| DCGM exporter | `DCGM_FI_*` | normalize/gpu.yaml → `gpu:*` / `gpu.*` | most verified (see `verified:` flags) |
| AMD Device Metrics Exporter | `gpu_*` | normalize/gpu.yaml | **all modeled** — verify in M8 |
| vLLM `/metrics` | `vllm:*` | normalize/engine_vllm.yaml → `llm:*` / `llm.*` | modeled on v1 names (verify) |
| llm-d gateway / EPP | `inference_pool_*`, `inference_objective_*`, `llm_d_epp_*` | used raw | modeled on Gateway API Inference Extension (verify) |
| kube-state-metrics, cAdvisor, node-exporter | standard | `k8s:gpu_*` recording rules | verified (kube-prometheus-stack 92.4.0) |

## Logs
- GPU fatal events: kernel-style lines `NVRM: Xid (PCI:<bus>): <code>, ...` (NVIDIA) and
  `amdgpu <bus>: amdgpu: RAS: ... uncorrectable ...` / `... GPU reset ...` (AMD) → agent `filelog/pods`
  (journald on real nodes) → `gpu_fatal_events`. NVIDIA line format: verified (driver format). AMD format:
  **modeled** — confirm on MI300X (M8) and adjust the regexes in `0009_gpu_events.sql` + the agent filter.
- Kubernetes events: cluster collector `k8sobjects` (watch `events.k8s.io`) → `otel_logs` (ServiceName `k8s-events`) → `k8s_events`.
- NCCL / RCCL errors: engine logs containing `NCCL WARN` / `RCCL WARN` → `collective_errors` (network dashboard). Real pods:
  the agent `filelog/pods` receiver picks them from container stdout (the engine simulator sends them over OTLP).
