# observe-d — Implementation Plan (v2: ClickHouse)

> This is the **target** architecture. Build starts with the cut-down version in [MVP.md](MVP.md).

An OpenObserve-style observability platform for LLM inference. It collects **logs, metrics, traces, RUM and LLM events** from the full stack: GPU cluster → Kubernetes → network → llm-d → vLLM (SGLang later) → applications and databases. Storage is **ClickHouse**, fed by streaming ingest (Kafka → ClickHouse).

> v2 replaces v1's Python worker + Parquet + DuckDB + file catalog with ClickHouse. §9 lists what changed.

---

## 1. What we take from OpenObserve, and how it maps to ClickHouse

| OpenObserve concept | observe-d equivalent |
|---|---|
| Organizations + streams (logs / metrics / traces / RUM) | `org_id` column at the front of every table's sort key + one table family per signal; row policies per org |
| Ingest endpoints: OTLP, `_json`, `_bulk`, Prometheus remote-write | OTel Collector (OTLP gRPC/HTTP, Prometheus scrape/remote-write, filelog) + `ingest_api` for JSON/LLM/RUM events |
| Memtable → Parquet → object store | ClickHouse MergeTree parts on local NVMe (hot) → TTL move to S3/MinIO volume (cold) |
| VRL functions / pipelines (transform at ingest) | OTel `transform` (OTTL) / `redaction` / `filter` processors + ClickHouse materialized views |
| Enrichment tables | ClickHouse **dictionaries** (price catalog, GPU inventory, model registry) |
| Alerts (scheduled + real-time) | Grafana alerting on ClickHouse queries + rollup tables (later: own alert service) |
| Dashboards | Grafana + official ClickHouse datasource (has OTel-aware log/trace views) |
| RUM (browser SDK, sessions, errors, web vitals) | OTel browser SDK → `ingest_api /ingest/rum` → `rum_events` |
| Full-text log search | `Body` column with token/ngram bloom-filter skip index (or the newer text index on recent ClickHouse) |

**Build vs adopt.** **ClickStack** (ClickHouse + OTel Collector + HyperDX UI) is close to "OpenObserve on ClickHouse" for generic logs, traces and session replay. Using the same OTel ClickHouse schema (§5) means we could put HyperDX on top for free. Our differentiator is the LLM-specific layers in §3.5–§3.8, not generic log search.

---

## 2. Architecture

```
┌──────────────────────── GPU cluster (8×H100 / 8×MI300X nodes) ─────────────────────────┐
│  DCGM exporter / AMD device-metrics exporter   node-exporter   kernel log (Xid, RAS)    │
│  vLLM pods (/metrics + OTLP traces)   llm-d gateway / EPP / KV events   app pods        │
│  kube-state-metrics   kubelet/cAdvisor   CNI (Cilium/Hubble)   IB/RoCE exporters        │
│                                                                                         │
│  OTel Collector AGENT (DaemonSet): prometheus scrape, hostmetrics, kubeletstats,        │
│     filelog, otlp receiver, k8sattributes, resource detection, redaction                │
│  OTel Collector CLUSTER (1 replica): k8s_cluster, k8sobjects (events)                   │
└───────────────┬─────────────────────────────────────────────────────────────────────────┘
                │ OTLP (protobuf)                      apps / gateways / browsers
                ▼                                                │ JSON (LLM events, RUM)
        ┌───────────────┐                              ┌─────────▼────────┐
        │ Kafka (KRaft) │◄─────────────────────────────│   ingest_api     │ auth, validate,
        │ otlp_logs     │                              │   (FastAPI)      │ org stamping
        │ otlp_metrics  │                              └──────────────────┘
        │ otlp_traces   │
        │ llm_events    │  JSON topics ──► ClickHouse Kafka engine + MV  ("ClickHouse stream")
        │ rum_events    │
        └──────┬────────┘
               │ OTLP topics
               ▼
   OTel Collector GATEWAY (Deployment, HPA): kafka receiver → batch → clickhouse exporter
               │
               ▼
 ┌────────────────────────────────── ClickHouse ──────────────────────────────────┐
 │ raw:   otel_logs  otel_traces  otel_metrics_{gauge,sum,histogram,...}          │
 │        llm_requests  llm_payloads  llm_evals  rum_events  kv_cache_events      │
 │ dims:  gpu_inventory  model_deployments  dict_price_catalog                    │
 │ MVs →  llm_rollup_1m  gpu_rollup_1m  tokenomics_hourly  slo_rollup_1m          │
 │ storage policy: hot (NVMe) → cold (S3 / MinIO) by TTL; row policies per org    │
 └───────────────┬──────────────────────────────────────────────────┬─────────────┘
                 ▼                                                  ▼
          query_api (tenant-scoped SQL, saved queries)        Grafana (dashboards + alerts)
```

**Why Kafka in front of ClickHouse.** ClickHouse wants large, infrequent inserts; telemetry is many small writes. Kafka absorbs bursts, survives ClickHouse restarts and upgrades, and lets us replay. Two consumers:
- **OTLP topics** → gateway collectors → `clickhouseexporter` (batched, uses the OTel schema).
- **JSON topics** → ClickHouse **Kafka table engine** + materialized view → MergeTree. This is the native ClickHouse streaming path: no Python worker. Bad rows go to an errors table via `kafka_handle_error_mode='stream'`.

**Correlation keys.** Every row carries as many of these as apply, so all layers join:
`org_id · cluster · node · gpu_uuid · namespace · pod · container · model · engine · replica · trace_id · span_id · request_id · session_id`

- GPU ↔ pod mapping comes from DCGM exporter / AMD exporter Kubernetes mode (kubelet pod-resources API).
- Pod ↔ model comes from `model_deployments`.
- Request ↔ trace comes from `trace_id`.

---

## 3. What we collect, layer by layer

> Summary only. The full telemetry spec (fields, cadence, correlation, alerts, retention, table mapping, corrections) is in [all_telemetry.md](all_telemetry.md).

Scrape intervals: GPU 5–10 s, inference engine 5–15 s, k8s 30 s, network 15–30 s. Metric names are examples; **verify against the deployed exporter/engine version** (vLLM v1 renamed several metrics).

### 3.1 GPU hardware — 8×H100 (NVIDIA) / 8×MI300X (AMD)

**Sources:**
- NVIDIA: **DCGM exporter** (with k8s pod labels), NVIDIA Fabric Manager logs.
- AMD: **AMD Device Metrics Exporter** (amd-smi based, pod labels).
- Both: kernel log / journald (Xid, AMD RAS events), node-exporter, BMC/IPMI.

| Category | What to measure | NVIDIA (DCGM) examples | AMD MI300X examples | Why it matters |
|---|---|---|---|---|
| Real utilization | SM active, SM occupancy, tensor/matrix pipe active, DRAM (HBM) active | `PROF_SM_ACTIVE`, `PROF_SM_OCCUPANCY`, `PROF_PIPE_TENSOR_ACTIVE`, `PROF_DRAM_ACTIVE` | GFX activity, MMA activity, UMC (memory) activity | `GPU_UTIL` only says "a kernel was running". Decode is memory-bound (watch HBM BW), prefill is compute-bound (watch tensor active). |
| Memory | HBM used/free/reserved | `FB_USED`, `FB_FREE` | VRAM used/total (192 GB) | KV-cache headroom; vLLM pre-allocates ~90%, so pair with vLLM KV usage. |
| Power & energy | Power draw, cumulative energy, power cap | `POWER_USAGE`, `TOTAL_ENERGY_CONSUMPTION` | power usage, energy counter | Joules/token for tokenomics (§3.6). |
| Thermals & clocks | GPU/HBM temp, SM/mem clock, throttle reasons | `GPU_TEMP`, `MEMORY_TEMP`, `SM_CLOCK`, `CLOCK_THROTTLE_REASONS` | junction temp, memory temp, clocks | Thermal/power throttling shows up as unexplained TPOT regressions. |
| Reliability | Xid errors, ECC SBE/DBE, row remap pending/failed, retired pages | `XID_ERRORS`, `ECC_DBE_VOL_TOTAL`, `ROW_REMAP_FAILURE` | ECC correctable/uncorrectable per block, RAS events | DBE / Xid 79 ("fallen off the bus") = drain the node. Leading indicator of hardware failure. |
| Intra-node interconnect | NVLink/NVSwitch TX/RX bandwidth, CRC/replay/recovery errors; xGMI bandwidth (AMD) | `PROF_NVLINK_TX_BYTES`, NVLink error counters | xGMI link RX/TX | Tensor parallelism (TP=8) all-reduce traffic. Link errors cause hangs. |
| Host link | PCIe TX/RX bandwidth, replay counter, link gen/width | `PROF_PCIE_TX_BYTES`, `PCIE_REPLAY_COUNTER` | PCIe bandwidth | Model load time, KV offload to CPU, degraded links (Gen5 x16 → x8). |
| Partitioning | MIG profile (NVIDIA), compute/memory partition mode (AMD CPX/NPS) | MIG instance labels | partition mode | Per-slice attribution. |
| Inventory (dimension) | GPU UUID, serial, model, driver, CUDA/ROCm, VBIOS/firmware, node, slot | — | — | Joins + "which firmware is failing". Stored in `gpu_inventory`. |
| Host | CPU, NUMA, memory, NVMe throughput (weights loading), fans/PSU (BMC) | node-exporter | node-exporter | CPU-side tokenization/scheduling bottlenecks. |

### 3.2 Kubernetes

**Sources:** kube-state-metrics, kubelet/cAdvisor (`kubeletstats` receiver), `k8s_cluster` receiver, `k8sobjects` (events), API server/scheduler metrics, container logs (`filelog`), GPU device plugin / GPU Operator.

| Category | What to monitor |
|---|---|
| GPU scheduling | Allocatable vs requested `nvidia.com/gpu` / `amd.com/gpu` per node, **fragmentation** (nodes with 1–7 free GPUs that can't fit a TP=8 pod), pending pods with reason `Insufficient nvidia.com/gpu` |
| Pod lifecycle | Phase, restarts, `OOMKilled`, `CrashLoopBackOff`, readiness flaps, **time-to-ready** (image pull → model download → weight load → CUDA graph capture/warmup) |
| Resources | Container CPU/mem usage vs requests/limits, CPU throttling, ephemeral storage, PVC usage (model cache) |
| Scaling | HPA/KEDA decisions, replica counts per model, scale-up latency (cold starts are minutes for 70B+ models) |
| Control plane | API server latency/errors, scheduler latency, etcd health |
| Events | All k8s events → `otel_logs` (node NotReady, eviction, FailedScheduling, device plugin unhealthy) |
| Operators | GPU Operator / device plugin / DCGM health, LeaderWorkerSet (multi-node models) status |
| Topology labels | Node pool, zone, GPU type, IB fabric/rack — added as resource attributes by `k8sattributes` |

### 3.3 Network

| Layer | What to measure | Source |
|---|---|---|
| Intra-node GPU fabric | NVLink/NVSwitch, xGMI (see §3.1) | DCGM / AMD exporter |
| Inter-node GPU fabric (InfiniBand / RoCEv2) | Port TX/RX bytes & packets, symbol/link errors, link downed, **PFC pause frames, ECN marks / CNPs** (congestion), port state/speed | IB exporter / `ethtool` / NIC (ConnectX) counters |
| Collective comms | NCCL/RCCL all-reduce/all-gather time, bus bandwidth, timeouts/hangs | NCCL logs (`NCCL_DEBUG=INFO`), engine logs, NCCL profiler plugin later |
| **KV-cache transfer (P/D disaggregation)** | Transfer latency, bytes, throughput and failures between prefill and decode pods (NIXL / UCX) | llm-d / vLLM KV connector metrics + spans |
| Pod network | Pod-to-pod latency, TCP retransmits, drops, DNS latency, flow logs | Cilium/Hubble or eBPF (e.g. Beyla) |
| North-south | Gateway (Envoy) request rate, latency, 4xx/5xx, connection counts, **streaming duration** | Envoy metrics + access logs |
| Storage network | Model weight download throughput (S3/NFS/HF), object store latency | node-exporter, init-container logs |

### 3.4 llm-d (distributed inference layer)

llm-d adds Kubernetes-native routing and orchestration on top of vLLM:
- an Inference Gateway (Envoy + Gateway API Inference Extension) with an Endpoint Picker (EPP / inference scheduler)
- prefix-cache-aware and load-aware routing
- prefill/decode (P/D) disaggregation
- KV-cache indexing
- autoscaling

| Component | What to collect |
|---|---|
| Gateway / InferencePool | Requests per model/objective, latency, error rate, input/output tokens per model, pool-level avg KV-cache utilization and queue size |
| EPP / scheduler | **Routing decision per request**: candidate pods, scorer outputs (prefix-cache score, queue score, KV-usage score), chosen pod, decision latency, fallbacks. Logged as span attributes on the request trace. |
| Prefix-cache routing quality | Predicted prefix hit (from KV index) vs **actual** hit reported by vLLM. This is the core KPI of llm-d. |
| KV-cache indexer / events | KV block stored/evicted events per pod (`kv_cache_events`), index size, staleness |
| P/D disaggregation | Prefill-pod vs decode-pod queue/utilization, KV transfer time (§3.3), prefill:decode ratio, decode stalls waiting on KV |
| Autoscaler | Desired vs actual replicas per model variant, scaling reasons, saturation signals used |
| Topology | Which model / variant / LoRA is served by which pods (written to `model_deployments`) |

### 3.5 vLLM (inference engine) — SGLang later

**Sources:**
- `/metrics` (Prometheus), scraped by the agent collector.
- OTLP request traces (`--otlp-traces-endpoint`).
- Engine logs (stats lines, warnings, OOM, preemption).
- Startup config.

| Category | What to measure | vLLM examples |
|---|---|---|
| Scheduler load | Running, waiting (queue), swapped requests | `vllm:num_requests_running`, `vllm:num_requests_waiting` |
| KV cache | KV usage %, **prefix-cache queries/hits**, preemptions (recompute) | `vllm:kv_cache_usage_perc` (v0: `gpu_cache_usage_perc`), `vllm:prefix_cache_queries/hits`, `vllm:num_preemptions_total` |
| Latency SLIs (histograms) | **TTFT**, **inter-token latency (ITL/TPOT)**, E2E, queue time, prefill time, decode time | `vllm:time_to_first_token_seconds`, `vllm:inter_token_latency_seconds`, `vllm:e2e_request_latency_seconds`, `vllm:request_queue_time_seconds`, `vllm:request_prefill_time_seconds`, `vllm:request_decode_time_seconds` |
| Tokens | Prompt tokens, generation tokens, per-request token histograms, tokens per engine iteration (batch efficiency) | `vllm:prompt_tokens_total`, `vllm:generation_tokens_total`, `vllm:request_prompt_tokens`, `vllm:iteration_tokens_total` |
| Outcomes | Finished requests by `finished_reason` (stop / length / abort), errors | `vllm:request_success_total` |
| Speculative decoding | Drafts, draft tokens, accepted tokens → acceptance rate | `vllm:spec_decode_*` |
| LoRA | Active/waiting adapters | `vllm:lora_requests_info` |
| Engine config (dimension) | Model, revision, dtype/quantization (FP8/AWQ…), TP/PP/DP/EP size, `max_num_seqs`, `max_model_len`, `gpu_memory_utilization`, engine version, image | Parsed from startup log/args → `model_deployments` |
| Lifecycle | Weight load time, CUDA graph capture time, warmup, crashes/OOMs | Logs → `otel_logs` + derived events |

**Engine abstraction for SGLang.** Every engine metric is normalized on ingest (OTel `transform` processor / MV) into common names, and carries an `engine` label (`vllm` / `sglang`). Dashboards then work unchanged. SGLang exposes similar `sglang:*` metrics (running/queue, token usage, cache hit rate, TTFT, ITL), so it plugs in later with a mapping file only.

### 3.6 Tokenomics

Computed from `llm_requests` + GPU metrics + `dict_price_catalog`, materialized into `tokenomics_hourly`:

| Metric | Definition |
|---|---|
| Token volume | Input, cached-input (prefix hit), output, reasoning tokens. Grouped by org / app / user / model / LoRA / endpoint. |
| Throughput | Output tokens/s per replica, per GPU, per model. Requests/s. |
| **Cost per 1M tokens (owned GPUs)** | (GPU-seconds allocated to the model's pods × $/GPU-hour [amortized capex + power + DC]) ÷ tokens served |
| Cost per request / per user / per app | Showback/chargeback, by token share of the pod's GPU time |
| Price-based cost (API-style) | tokens × price list per model (input/cached/output), for comparing against external providers |
| **Energy per token** | Δ DCGM/AMD energy counter per GPU ÷ tokens generated on that GPU (J/token, Wh/1k tokens) |
| Utilization efficiency | Tokens per GPU-second vs benchmark peak; idle GPU-hours (allocated, no traffic); MFU estimate (later) |
| Cache savings | Prefix-cache hit tokens × prefill cost avoided |
| Waste | Tokens recomputed due to preemption, tokens generated for aborted/timed-out requests, failed requests' GPU time |
| **Goodput** | Requests (and tokens) that met SLO (e.g. TTFT ≤ 500 ms and TPOT ≤ 50 ms) ÷ total |
| Budgets & quotas | Per org/app token and cost budgets, burn rate, forecast-to-exhaustion |

### 3.7 AI & LLM traceability

End-to-end trace of every inference, following the **OTel GenAI semantic conventions** (`gen_ai.operation.name`, `gen_ai.request.model`, `gen_ai.response.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.response.finish_reasons`, `gen_ai.request.temperature`/`top_p`/`max_tokens`, …).

```
browser (RUM) → app / agent → [retrieval: embed → vector DB → rerank] → [tool calls]
  → llm-d gateway → EPP routing decision → vLLM prefill pod → KV transfer → vLLM decode pod
  → streamed response → guardrail / eval
```

| What | Detail |
|---|---|
| Request lineage | org, app, user, session/conversation ID, prompt template ID + version, model + revision, LoRA, engine + version, serving pod/GPU |
| Reproducibility | Sampling params (temperature, top_p, top_k, max_tokens, seed, stop), system-prompt hash, tool schema hash |
| Agent / RAG steps | Spans for retrieval (query, top-k, doc IDs, scores, latency), embeddings, rerank, tool/function calls (name, args hash, result status), sub-agent calls |
| Content (opt-in) | Prompt and completion text in `llm_payloads` with redaction (PII) at the collector, size cap, separate shorter TTL, stricter access |
| Quality & safety | Eval scores (LLM-judge, groundedness, toxicity), guardrail decisions (blocked/modified + rule), user feedback (thumbs, ratings) → `llm_evals` linked by `trace_id` / `request_id` |
| Audit | Who called which model with what config, and when. Immutable, long-retention view for compliance. |

### 3.8 RUM (real user monitoring) — for the chat/agent front-end

| What | Detail |
|---|---|
| Sessions & views | session ID, page/view, device, browser, geo (country only), app version |
| Web vitals | LCP, INP, CLS, TTFB |
| **User-perceived LLM latency** | Click → first streamed token (true TTFT incl. network), tokens rendered per second, stream stalls, user cancels (stop button) |
| Errors | JS errors, failed fetches, SSE/WebSocket disconnects |
| Correlation | Browser injects `traceparent`, so a slow UI interaction links to the backend trace and the vLLM pod/GPU |
| Later | Session replay (privacy masking on by default) |

### 3.9 Databases & external systems (RAG / ERP dependencies)

| System | What to collect | Source |
|---|---|---|
| Postgres / MySQL | Connections, active/idle, locks, deadlocks, replication lag, slow queries, cache hit ratio, top statements | `postgresql` / `mysql` receivers, `pg_stat_statements` |
| Redis (sessions, semantic cache) | Hit rate, latency, evictions, memory | `redis` receiver |
| Vector DB (pgvector / Milvus / Qdrant / Weaviate) | Query latency p50/p99, QPS, index size, segment/compaction state, recall (from evals) | Prometheus endpoints + client spans |
| ERP / retrieval APIs | Latency, error rate, timeouts, rate-limit hits, payload size | Client-side spans (HTTP instrumentation) + `httpcheck` synthetic probes |
| ClickHouse itself (self-monitoring) | Insert rate, parts count, merges, Kafka consumer lag, query latency, disk/S3 usage | ClickHouse Prometheus endpoint + `system.*` tables |

### 3.10 Logs (cross-cutting)

Container stdout/stderr, vLLM/llm-d engine logs, kernel/journald (Xid, NVRM, AMD RAS, OOM killer), k8s events, Envoy access logs, audit logs. All go to `otel_logs` with k8s resource attributes, `TraceId` when present, and severity normalized.

---

## 4. Signals → tables summary

| Signal | Producers | Kafka topic | ClickHouse table(s) |
|---|---|---|---|
| Metrics | DCGM / AMD exporter, node-exporter, kube-state-metrics, kubelet, vLLM, llm-d, Envoy, DB receivers, IB/NIC exporters | `otlp_metrics` | `otel_metrics_gauge`, `_sum`, `_histogram`, `_exponential_histogram`, `_summary` → `gpu_rollup_1m`, `engine_rollup_1m` |
| Logs | filelog, journald, k8sobjects, app OTLP logs | `otlp_logs` | `otel_logs` |
| Traces | vLLM OTLP, llm-d, apps/agents (GenAI semconv), Envoy | `otlp_traces` | `otel_traces` (+ `otel_traces_trace_id_ts` lookup) |
| LLM request events (wide events) | `ingest_api` (from gateway/app), or derived from vLLM spans | `llm_events` | `llm_requests`, `llm_payloads` → `llm_rollup_1m`, `tokenomics_hourly`, `slo_rollup_1m` |
| Evals / feedback | eval jobs, app feedback API | `llm_events` | `llm_evals` |
| KV-cache events | llm-d KV event stream | `kv_events` | `kv_cache_events` |
| RUM | browser SDK → `ingest_api` | `rum_events` | `rum_events` |
| Dimensions | inventory jobs, k8s watcher | — (direct insert) | `gpu_inventory`, `model_deployments`, `dict_price_catalog` |

---

## 5. ClickHouse design

**Schema strategy.**
- Pre-create the OTel tables with our own DDL, which is a superset of the `clickhouseexporter` schema, and run the exporter with `create_schema: false`.
- Add `OrgId LowCardinality(String) MATERIALIZED ResourceAttributes['org.id']` plus other materialized hot columns (`k8s.pod.name`, `gpu.uuid`, `gen_ai.request.model`), so filters don't scan Maps.
- Keeping exporter compatibility means Grafana's ClickHouse plugin and HyperDX work out of the box.

**Table conventions.**
- `PARTITION BY toDate(ts)`. Never by hour or org: too many parts.
- `ORDER BY (org_id, <low-card dims>, ts)`.
- `LowCardinality(String)` for dims.
- Codecs: `Delta, ZSTD` for timestamps, `Gorilla` / `DoubleDelta` for metric values, `ZSTD(3)` for text.
- Attributes as `Map(LowCardinality(String), String)`.
- Skip indexes: `bloom_filter` on `TraceId` / `request_id`; `tokenbf_v1` (or text index) on log `Body`.

**Key custom tables.**

```sql
-- Wide event per inference request (the core LLM table)
CREATE TABLE llm_requests (
  ts DateTime64(6, 'UTC'), ingested_at DateTime64(3) DEFAULT now64(3),
  org_id LowCardinality(String), app_id LowCardinality(String), user_id String,
  session_id String, request_id UUID, trace_id String, span_id String,
  cluster LowCardinality(String), node LowCardinality(String), pod LowCardinality(String),
  gpu_uuids Array(LowCardinality(String)),
  engine LowCardinality(String), engine_version LowCardinality(String),
  model LowCardinality(String), model_revision LowCardinality(String), lora LowCardinality(String),
  route_pod_prefill LowCardinality(String), route_pod_decode LowCardinality(String),
  route_reason LowCardinality(String), prefix_hit_predicted Float32,
  tokens_in UInt32, tokens_cached UInt32, tokens_out UInt32, tokens_reasoning UInt32,
  ttft_us UInt32, tpot_us UInt32, queue_us UInt32, prefill_us UInt32, decode_us UInt32,
  kv_transfer_us UInt32, e2e_us UInt32, preemptions UInt16,
  finish_reason LowCardinality(String), status LowCardinality(String), error String,
  temperature Float32, top_p Float32, max_tokens UInt32, prompt_template LowCardinality(String),
  attributes Map(LowCardinality(String), String),
  INDEX idx_trace trace_id TYPE bloom_filter GRANULARITY 4
) ENGINE = ReplacingMergeTree(ingested_at)          -- dedupe at-least-once replays
PARTITION BY toDate(ts)
ORDER BY (org_id, model, toStartOfHour(ts), request_id)
TTL toDateTime(ts) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(ts) + INTERVAL 90 DAY DELETE
SETTINGS storage_policy = 'hot_cold';
```

- `llm_payloads(ts, org_id, request_id, trace_id, prompt, completion)`: opt-in, redacted, TTL 7–30 days, separate grants.
- `llm_evals(ts, org_id, request_id, trace_id, evaluator, metric, score, label, feedback)`.
- `rum_events(ts, org_id, app_id, session_id, view, event_type, trace_id, lcp_ms, inp_ms, cls, stream_ttft_ms, tokens_rendered_per_s, error, attributes)`.
- `kv_cache_events(ts, pod, model, event, block_hashes, …)`.
- `gpu_inventory`, `model_deployments`: `ReplacingMergeTree`, latest state per key.
- `dict_price_catalog`: dictionary sourced from a ClickHouse table/CSV (model, $/1M in/cached/out, $/GPU-hour per GPU type).

**Rollups (materialized views)** keep dashboards fast and long-retention cheap:
- `llm_rollup_1m` (`AggregatingMergeTree`): by org, model, pod, minute → `countState`, `sumState(tokens…)`, `quantilesTDigestState(0.5,0.9,0.99)(ttft_us / tpot_us / e2e_us)`, errors by finish_reason.
- `slo_rollup_1m`: good vs total requests per SLO definition → goodput, error-budget burn.
- `tokenomics_hourly` (`SummingMergeTree`): by org, app, user, model → tokens, requests, cost (via `dictGet` price), GPU-seconds, energy J.
- `gpu_rollup_1m`: by node, gpu_uuid, pod → avg/max SM active, tensor active, HBM BW, power, temp, energy delta, throttle seconds, error counts.
- Retention: raw 30–90 days, rollups 13 months.

**Streaming ingest of JSON topics (ClickHouse Kafka engine).**

```sql
CREATE TABLE llm_events_queue (raw String) ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:9092', kafka_topic_list='llm_events',
         kafka_group_name='ch_llm_events', kafka_format='JSONAsString',
         kafka_num_consumers=4, kafka_handle_error_mode='stream';

CREATE MATERIALIZED VIEW llm_events_mv TO llm_requests AS
SELECT parseDateTime64BestEffort(JSONExtractString(raw,'ts')) AS ts, ... FROM llm_events_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW llm_events_errors_mv TO ingest_errors AS
SELECT now() AS ts, _topic, _partition, _offset, _raw_message, _error FROM llm_events_queue
WHERE length(_error) > 0;
```

**Multi-tenancy and safety** (replaces v1's DuckDB SQL sandbox; ClickHouse enforces it server-side):
- One ClickHouse role per org, plus `CREATE ROW POLICY ... USING org_id = '<org>'` on every table.
- A `readonly=1` profile with `max_execution_time`, `max_memory_usage`, `max_rows_to_read`, `max_result_rows`, and `QUOTA` per org.
- `query_api` maps API key → org → ClickHouse user. Users never get raw table-function access (`s3`, `url`, `file` disabled via grants).

**Storage.** A `hot_cold` storage policy: local NVMe volume plus an S3 disk on MinIO (dev) or S3/GCS (prod). TTL moves parts to cold. Optional Parquet export for a data lake: `INSERT INTO FUNCTION s3('…/{_partition_id}.parquet')`.

---

## 6. Repository layout

```
cmd/                              # entrypoints only, no __init__.py (stdlib `cmd` clash)
  ingest_api/main.py              # LLM events, RUM, evals, generic JSON → Kafka
  query_api/main.py               # tenant-scoped SQL + saved queries over ClickHouse
  simulators/gpu_metrics.py       # fake DCGM/AMD exporter for GPU-less dev & CI
  simulators/loadgen.py           # synthetic traffic with GenAI spans + llm events
pkg/
  config.py logging.py telemetry.py retry.py auth.py
  schema/                         # JSON event schemas → pydantic models
  clickhouse/client.py            # clickhouse-connect wrapper, per-org users
  normalize/                      # engine metric mappings (vllm.yaml, sglang.yaml)
schemas/                          # llm_request.json, rum_event.json, llm_eval.json, kv_event.json
clickhouse/
  config/storage.xml users.xml    # hot_cold policy, profiles, quotas
  migrations/0001_otel_tables.sql 0002_llm_tables.sql 0003_kafka_streams.sql
             0004_rollups.sql 0005_dictionaries.sql 0006_row_policies.sql
collector/
  agent.yaml cluster.yaml gateway.yaml   # OTel Collector configs (contrib distro)
deploy/
  helm/values-otel-operator.yaml  # DaemonSet agent, cluster collector, gateway + target allocator
  k8s/                            # dcgm-exporter / amd exporter, kube-state-metrics, llm-d sim
infra/docker-compose.yml          # kafka(KRaft), clickhouse, minio, otel gateway, grafana, apis, sims
dashboards/                       # Grafana JSON: gpu, k8s, network, llm-d, vllm, tokenomics, traces, rum
alerts/                           # Grafana alert rules (provisioned)
tests/ unit/ integration/
.github/workflows/ci.yml
```

---

## 7. Phased delivery (PR-sized)

| # | PR | Contents | Done when |
|---|---|---|---|
| 1 | Infra skeleton | Compose: Kafka KRaft, ClickHouse (hot_cold policy on MinIO), MinIO + bucket init, OTel gateway collector, Grafana + ClickHouse plugin. pyproject, lint, config/logging libs. | `docker compose up --wait` green in CI |
| 2 | ClickHouse schema | Migrations 0001–0006, small migration runner (applied-versions table), users/profiles/quotas | Migrations idempotent; unit test asserts tables, MVs, row policies |
| 3 | Collector pipelines | `agent.yaml` (prometheus scrape of DCGM/AMD/vLLM/llm-d/node/KSM, hostmetrics, kubeletstats, filelog, k8sattributes, redaction, kafka exporter), `cluster.yaml` (k8s_cluster, k8sobjects), `gateway.yaml` (kafka receiver → batch → clickhouse exporter) | Simulated metrics, logs and traces land in `otel_*` tables |
| 4 | Ingest API | `/ingest/llm` (+batch/NDJSON), `/ingest/rum`, `/ingest/eval`, API key → org, schema validation, `traceparent` honoured, Kafka producer (idempotent, zstd), 503 backpressure, `/health` `/ready` `/metrics` | Unit tests; events land in `llm_requests` via the Kafka engine |
| 5 | Simulators & load | Fake GPU exporter (8×H100 and 8×MI300X profiles incl. throttling/Xid scenarios), `llm-d-inference-sim` (vLLM-compatible metrics without GPUs), loadgen emitting GenAI spans + LLM events | Every dashboard panel has data in dev |
| 6 | Rollups & tokenomics | MVs for `llm_rollup_1m`, `slo_rollup_1m`, `tokenomics_hourly`, `gpu_rollup_1m`; price dictionary; GPU-second and energy attribution query | Cost per 1M tokens and J/token tests against hand-computed fixtures |
| 7 | Query API | Tenant-scoped SQL via per-org CH users, saved/parameterized queries (latency by model, tokens/s by replica, cost by app), pagination, query stats | Cross-org access test fails as expected |
| 8 | Dashboards & alerts | Provisioned Grafana dashboards per layer (§3) + alert rules (§8) | Dashboards render from simulated data |
| 9 | Integration & CI | Full flow: loadgen 10k requests → `llm_requests` count = 10k after `FINAL` → rollup and query < 10 s; Kafka/ClickHouse restart resilience test; CI lint → unit (≥80% `pkg/`) → compose smoke | CI green |
| 10 | k8s deploy | Helm values for OTel operator + target allocator, DCGM/AMD exporters with pod mapping, kind-based smoke test | Runs on a real GPU node |
| later | SGLang mapping, NCCL profiler, session replay, anomaly detection (TTFT/throughput baselines), own alert service, ClickHouse cluster (replication/sharding via Keeper) | | |

---

## 8. Starter alert catalog

| Layer | Alert |
|---|---|
| GPU | Xid error (critical: 48, 63/64, 74, 79, 94/95), ECC DBE > 0, row-remap failure, temp > threshold, sustained throttling, NVLink/xGMI errors increasing, GPU missing from inventory |
| k8s | Model pod not ready > N min, CrashLoop/OOMKilled, pending GPU pods, GPU fragmentation prevents TP=8 placement |
| Network | IB link down/flapping, symbol errors rising, PFC pause storm, KV-transfer p99 > threshold |
| llm-d | Prefix-hit actual ≪ predicted, EPP decision latency high, pool queue size growing |
| vLLM | KV usage > 90% sustained, preemptions rising, waiting queue growing, TTFT/TPOT p95 SLO burn rate (multi-window) |
| Tokenomics | Org budget burn-rate forecast < 3 days, cost/1M tokens regression, idle allocated GPUs > N hours |
| Traceability | Error/guardrail-block rate spike per model or prompt template, eval score drop |
| Platform | Kafka consumer lag, ClickHouse parts-too-many, insert errors, `ingest_errors` growing, cold-tier move failures |

---

## 9. What changed from v1 and why

| v1 component | v2 | Reason |
|---|---|---|
| Python Kafka worker + pyarrow batching | ClickHouse Kafka engine + MVs (JSON), OTel gateway (OTLP) | No custom consumer to maintain; offsets, batching and parts are handled by ClickHouse/collector |
| Parquet files + file catalog (SQLite/Postgres) | MergeTree parts + `system.parts`; S3 cold tier via storage policy | ClickHouse is the catalog; no orphan-file or atomic-rename logic |
| DuckDB query API + sqlglot sandbox | ClickHouse users, row policies, readonly profiles, quotas | Isolation enforced server-side instead of by SQL parsing |
| Custom dedupe / deterministic file IDs | `ReplacingMergeTree(ingested_at)` keyed on `request_id` | Native at-least-once dedupe (query with `FINAL` or via rollups) |
| Single `llm_requests` stream | Logs, metrics, traces, RUM, LLM events, evals, KV events, dims | Full-stack scope as requested |

Still valid from v1: KRaft not Zookeeper, API key → org mapping, honouring caller `traceparent`, opt-in redacted payload capture, late-event window, never making `cmd/` a package, Windows dev notes.

---

## 10. Gaps and open decisions

1. **ClickHouse topology.** Single node is enough for dev and probably for one GPU pod (8 GPUs ≈ a few thousand series, ~1k req/s ≈ 1 MB/s raw before ~10× compression). Multi-node needs ClickHouse Keeper + `ReplicatedMergeTree`. Decide now whether migrations use `Replicated*` engines from day one (recommended: use `{cluster}` macros even on a single node).
2. **Self-hosted vs ClickHouse Cloud.** Cloud gives ClickPipes (managed Kafka → ClickHouse) and S3 separation for free. Self-hosted needs the Kafka engine as above.
3. **Adopt ClickStack/HyperDX for generic logs/traces/session replay, or build our own UI?** Recommendation: Grafana now, keep the OTel schema so HyperDX remains an option.
4. **Prompt/completion capture default.** Recommendation: off; opt-in per org with redaction.
5. **GPU $/hour model for owned hardware.** Needs finance inputs: amortization period, power cost, DC overhead, per GPU type.
6. **Where the wide `llm_requests` event comes from.** The gateway/app via `ingest_api` (has user/app context), or derived from vLLM OTLP spans by an MV (no app changes needed). Recommendation: both, joined by `trace_id`; the gateway event wins for identity fields.
7. **SLO definitions** per model/tier (TTFT, TPOT, availability). Needed for goodput and burn-rate alerts.
8. **Hardware access for CI.** Real DCGM/AMD exporters need GPUs, so CI uses simulators. Need one real 8×H100 and one 8×MI300X node for validation in PR 10.
9. **Metric name drift.** vLLM, llm-d and the exporters rename metrics between versions. Pin versions, and keep mappings in `pkg/normalize/*.yaml` with a contract test per version.
10. **Cardinality guardrails.** Never put `request_id`, `user_id` or `trace_id` in metric labels. Enforce with a collector `filter`/`transform` and alert on series count.
