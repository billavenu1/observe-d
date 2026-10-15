# observe-d — MVP (full vertical, single GPU node)

The MVP covers **every layer of the stack on one GPU node**, NVIDIA and AMD alike:

**GPU → Kubernetes → Network → llm-d → vLLM → Tokenomics → AI/LLM traceability**

Each layer is cut down to the measurements that matter most, but no layer is skipped. This is a subset of [PLAN.md](PLAN.md) (target architecture) and [all_telemetry.md](all_telemetry.md) (full spec). It is built so that nothing has to be thrown away later.

> **Progress:** see [docs/status.md](docs/status.md) for what is built and verified per milestone, and what remains.

---

## 1. Assumptions (MVP defaults — change any that are wrong)

| # | Topic | MVP default |
|---|---|---|
| 1 | GPU node | **1 node: 8×H100 (SXM, NVLink/NVSwitch) or 8×MI300X (xGMI)**. Both vendors are supported by the same code, dashboards and alerts. **Order: simulators (M1–M7) → 1× 8×MI300X node (M8) → 1× 8×H100 node (M9).** |
| 2 | Other nodes | **1–2 CPU nodes** in the same cluster for Kafka, ClickHouse, Prometheus, collectors and Grafana. The stack never runs on the GPU node, so it doesn't steal CPU from vLLM and stays up if the GPU node fails. |
| 3 | Load | **Design limit: 5,000 req/s sustained, 7,000 req/s burst** (15 min), with the basic stack (1 Kafka broker, 1 ClickHouse node, collectors). Expected ceiling ~10k req/s before the architecture must change (§2.1). The project will be handed to a team with a larger cluster, so these are the numbers to size for. |
| 4 | Buffering | **Kafka** between collection and storage. |
| 5 | Serving | llm-d (Inference Gateway + EPP) in front of vLLM. TP up to 8. Optional P/D (prefill/decode) split *within* the node. |
| 6 | Users | Internal (platform / ML team). `org_id` / `app_id` recorded for showback; no per-tenant logins. |
| 7 | Cost | Showback: $/GPU-hour per GPU type (configurable), optional per-model price list. |
| 8 | Prompts | **Not captured.** Only metadata, tokens and timings. |
| 9 | Calling apps | Not required to change. Identity comes from gateway headers. Apps *may* send GenAI spans to the same collector endpoint. |
| 10 | Storage | Single-node ClickHouse, local NVMe, TTL-based retention. S3 cold tier later. |

---

## 2. Architecture

```
┌──────────────────────────── GPU node (8×H100 or 8×MI300X) ─────────────────────────────┐
│ DCGM exporter (NVIDIA GPU Operator)  ─or─  AMD Device Metrics Exporter (AMD GPU Operator)│
│ node-exporter · kubelet/cAdvisor · kernel log (Xid / AMD RAS)                           │
│ llm-d Inference Gateway (Envoy) + EPP  ──►  vLLM pods (CUDA or ROCm image)              │
│                                                                                         │
│ OTel Collector AGENT (DaemonSet)                                                        │
│   receivers: otlp (spans/logs from gateway, EPP, vLLM, apps), filelog, journald,        │
│              kubeletstats, prometheus (GPU exporter, vLLM, EPP — subset for ClickHouse) │
│   processors: k8sattributes, resourcedetection, GPU/engine normalization, memory_limiter│
│   exporter: kafka (otlp_proto)                                                          │
└──────────────┬──────────────────────────────────────────────────────────────────────────┘
               │                                    Prometheus (kube-prometheus-stack)
               │                                     scrapes everything raw, full fidelity;
               ▼                                     recording rules → normalized gpu:* / llm:*
┌───────────── CPU node(s) ────────────────────────────────────┐            │
│ Kafka (KRaft)  topics: otlp_traces · otlp_logs · otlp_metrics │            │
│                         (+ events_json reserved for later)    │            │
│        │                                                      │            │
│        ▼  consumer group                                      │            │
│ OTel Collector GATEWAY (≥2 replicas): kafka receiver → batch  │            │
│        → clickhouse exporter                                  │            │
│        ▼                                                      │            │
│ ClickHouse (single node)                                      │            │
│   otel_traces · otel_logs · otel_metrics_* (subset)           │            │
│   gateway_requests · engine_requests · routing_decisions      │            │
│   llm_requests (refreshable MV, 1 min) · gpu_samples          │            │
│   k8s_events · rollups · tokenomics · cost dictionaries       │            │
│                     │                                         │            │
│                     └──────────► Grafana ◄────────────────────┼────────────┘
│                                  dashboards + alerts → Alertmanager → Slack/PagerDuty
└───────────────────────────────────────────────────────────────┘
```

**Design choices and why**

| Choice | Why |
|---|---|
| **Kafka between agent and gateway collectors** | Absorbs 7k req/s bursts. Survives ClickHouse restarts and upgrades (replay from offsets). Lets gateway collectors scale as a consumer group. The ClickHouse Kafka-engine path stays reserved for future JSON event topics (RUM, feedback). |
| **Prometheus keeps all metrics; ClickHouse gets a subset** | Community dashboards and alerts for DCGM, AMD, vLLM, llm-d and Kubernetes work out of the box in PromQL. ClickHouse gets only the GPU/engine series needed for per-request joins, cost and energy. |
| **One vendor-neutral GPU vocabulary** (§3.1) | One mapping file (`normalize/gpu.yaml`) **generates both** the Prometheus recording rules and the collector OTTL transform. NVIDIA and AMD then share one set of dashboards, alerts and cost queries. |
| **No custom ingest or query service** | The OTel Collector is the only entry point. Grafana reads ClickHouse and Prometheus directly. Python is limited to simulators, generators, migrations and tests. |
| **Per-request row = gateway access log ⋈ vLLM request span, joined on `trace_id`** | No app changes. 100% of requests are kept. Built by a refreshable materialized view every minute. |
| **Observability stack on CPU nodes** | Isolation from the GPU workload, and it survives a GPU-node failure. |

**Kafka MVP config:**
- Single KRaft broker, replication factor 1. This is a known MVP trade-off; production uses 3 brokers with RF 3.
- 24 partitions per topic, so up to 24 gateway-collector consumers.
- Retention 24 h. That covers a full-day ClickHouse outage at 5k req/s.
- `compression=zstd`.
- Agent collector `sending_queue` backed by `file_storage`, so data survives Kafka blips.

### 2.1 Capacity: 5k sustained / 7k burst on the basic stack

**Per-request telemetry volume** (what drives everything):

| Record | Count per request | Approx. size (OTLP, uncompressed) |
|---|---|---|
| Gateway access log | 1 | ~1.0 KB |
| Gateway span | 1 | ~0.7 KB |
| EPP routing span | 1 | ~0.7 KB |
| vLLM request span | 1 (2 with P/D) | ~1.5 KB |
| **Total** | **~4–5 records** | **~4 KB** |

Fixed overhead (GPU/k8s/engine metrics subset, k8s events, engine logs) scales with **nodes and pods, not req/s**. It is small compared with the above.

**Load at the design limit:**

| | 5k req/s sustained | 7k req/s burst |
|---|---|---|
| Records/s into Kafka | ~20–25k | ~28–35k |
| Raw throughput | ~20 MB/s | ~28 MB/s |
| Kafka on disk (zstd ~4–5×) | ~4–5 MB/s → ~400 GB/day | ~6 MB/s |
| ClickHouse rows/s (raw + MV targets) | ~50–60k | ~70–85k |

**Can the basic stack do it?** Yes, comfortably, if the settings below are followed:

| Component | Capacity (single instance, typical) | Load at 7k burst | Headroom |
|---|---|---|---|
| Kafka broker (NVMe) | 100+ MB/s writes | ~28 MB/s raw / ~6 MB/s compressed | ≥ 3× |
| ClickHouse inserts (large batches) | several hundred k rows/s per node | ~85k rows/s | ≥ 3× |
| OTel gateway collector | ~10–20k records/s per replica (2 vCPU) | ~35k records/s | 4 replicas, HPA to 8 |
| Prometheus | 1–2 M active series | grows with GPUs/pods, not req/s (~100–200k series for 32 nodes) | ≥ 5× |

**Required settings** (these keep it simple *and* fast):
1. **Big batches into ClickHouse.** Gateway collector `batch` processor at ~20k records or 5 s per insert, with `async_insert=1`, so parts stay few and merges stay cheap.
2. **No double storage of high-volume raw data.**
   - Gateway access logs go to a `Null`-engine table whose MV writes `gateway_requests`. They are not kept in `otel_logs`.
   - Staging tables `gateway_requests`, `engine_requests` and `routing_decisions` have a **1-day TTL**. `llm_requests` is the durable copy.
3. **Incremental request join.** The refreshable MV runs every 1 min and only joins rows **inserted in the last 3 min** (by `_inserted_at`, not span start time, so long streaming requests aren't missed). That is ~1.3 M rows per side at 7k req/s, a few seconds of work. It is idempotent via `ReplacingMergeTree`.
4. **Dashboards read rollups, never raw tables**, except the per-request drill-down, which uses primary-key lookups.
5. **vLLM per-request logging off** (`--disable-log-requests` or equivalent on the pinned version). Per-request data comes from spans, not log lines.
6. **Traces kept at 100%** (no tail sampling, so no extra complexity), with **3-day TTL** on `otel_traces`.

**Storage at 5k req/s:**

| Data | Per day | Retention | On disk |
|---|---|---|---|
| `otel_traces` | ~120–200 GB | 3 d | ~0.6 TB |
| `llm_requests` | ~40–50 GB | 30 d | ~1.5 TB |
| Staging tables | ~60 GB | 1 d | ~60 GB |
| `gpu_samples`, logs, rollups | ~20 GB | 7–30 d / 13 mo | ~0.3 TB |
| **Total** | | | **~2.5 TB** → provision **4 TB NVMe** |

**Observability stack hardware (CPU nodes) for 5k / 7k:**

| Component | Size |
|---|---|
| Kafka (1 broker) | 8 vCPU, 32 GB RAM, 2 TB NVMe |
| ClickHouse (1 node) | 32 vCPU, 128 GB RAM, 4 TB NVMe |
| OTel gateway collectors | 4 × (2 vCPU, 4 GB), HPA to 8 |
| Prometheus | 8 vCPU, 32 GB RAM, 500 GB SSD |
| Grafana | 2 vCPU, 4 GB |
| **Total** | **~2–3 CPU nodes** (e.g. 2 × 32 vCPU / 128 GB) |

**Ceiling, and what to change beyond it.** The basic stack should hold to roughly **10k req/s sustained**, with more disk and gateway-collector replicas only. Past that, in this order:
1. 3 Kafka brokers, RF 3 (durability first).
2. Trace sampling for non-critical spans (`llm_requests` stays at 100%).
3. 2+ ClickHouse shards (`Distributed` table over `ReplicatedMergeTree`).

These are documented upgrades, not MVP work.

> All numbers are engineering estimates. They are **proven or corrected by the M7 capacity test** before handover.

---

## 3. What we measure, layer by layer (MVP scope)

Each layer lists: **Measure** (MVP must-haves) · **Source** · **Lands in** · **Dashboard** · **Alerts**. Everything else in all_telemetry.md for that layer is deferred.

### 3.1 GPU — 8×H100 / 8×MI300X

**Normalized metric vocabulary.** Dashboards, alerts and SQL use only these names; vendor names live in `normalize/gpu.yaml`. The vendor columns below are **examples to verify against the pinned exporter versions in M1**.

| Normalized name | Meaning | NVIDIA (DCGM) | AMD (Device Metrics Exporter / amd-smi) |
|---|---|---|---|
| `gpu.busy_pct` | Any-kernel-running time (coarse) | `DCGM_FI_DEV_GPU_UTIL` | GFX activity / busy % |
| `gpu.compute_active_pct` | Real compute engagement | `DCGM_FI_PROF_SM_ACTIVE` | GFX activity (closest equivalent) |
| `gpu.matrix_active_pct` | Tensor / matrix-core activity | `DCGM_FI_PROF_PIPE_TENSOR_ACTIVE` | MMA activity |
| `gpu.mem_bw_active_pct` | HBM bandwidth utilization (decode-bound signal) | `DCGM_FI_PROF_DRAM_ACTIVE` | UMC activity |
| `gpu.mem_used_bytes` / `gpu.mem_total_bytes` | HBM usage (80 GB H100 / 192 GB MI300X) | `DCGM_FI_DEV_FB_USED/FREE` | VRAM used / total |
| `gpu.power_watts` | Power draw | `DCGM_FI_DEV_POWER_USAGE` | socket/package power |
| `gpu.energy_joules_total` | Cumulative energy (counter) | `DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION` (mJ → J) | energy counter (unit conversion in mapping) |
| `gpu.temp_c` / `gpu.mem_temp_c` | Core / HBM temperature | `DCGM_FI_DEV_GPU_TEMP` / `_MEMORY_TEMP` | junction (hotspot) / memory temperature |
| `gpu.clock_sm_mhz` | Compute clock | `DCGM_FI_DEV_SM_CLOCK` | GFX clock |
| `gpu.throttle_active{reason}` | Throttling by reason (power, thermal, …) | `DCGM_FI_DEV_CLOCK_THROTTLE_REASONS` (bitmask → reasons) | violation/throttle status counters |
| `gpu.ecc_correctable_total` / `gpu.ecc_uncorrectable_total` | Memory errors | `DCGM_FI_DEV_ECC_SBE/DBE_VOL_TOTAL` | ECC correctable / uncorrectable (per block, summed) |
| `gpu.row_remap_pending` / `gpu.row_remap_failed` | HBM row remapping (NVIDIA) / bad pages (AMD) | `DCGM_FI_DEV_ROW_REMAP_*` | bad/retired page counts |
| `gpu.fatal_event_total{code}` | Fatal driver/hardware events | **Xid** (`DCGM_FI_DEV_XID_ERRORS` + kernel log) | **RAS / GPU reset events** (kernel log + exporter) |
| `gpu.link_tx_bytes` / `gpu.link_rx_bytes{link}` | GPU↔GPU fabric | NVLink TX/RX (`DCGM_FI_PROF_NVLINK_*`) | xGMI TX/RX per link |
| `gpu.link_errors_total{link}` | Fabric errors | NVLink CRC/replay/recovery | xGMI error counters (where exposed) |
| `gpu.pcie_tx_bytes` / `gpu.pcie_rx_bytes`, `gpu.pcie_replay_total` | Host link | `DCGM_FI_PROF_PCIE_*`, `PCIE_REPLAY_COUNTER` | PCIe bandwidth / replay |

**Common labels:** `vendor`, `gpu_uuid`, `gpu_index`, `gpu_model`, `node`, `pod`, `namespace`, `container`. **Pod mapping** comes from DCGM / AMD exporter Kubernetes mode.

**Inventory** (`gpu_inventory`, refreshed hourly):
- `gpu_uuid`, serial, model, memory, vendor
- driver version, CUDA or ROCm version, VBIOS/firmware
- PCI bus ID, NUMA node
- partition mode (MIG on NVIDIA; SPX/CPX + NPS on AMD)

| | |
|---|---|
| **Cadence** | 5 s (profiling/activity fields), 10 s (power/temp/mem), 30 s (ECC/remap) |
| **Lands in** | Prometheus (full, raw + normalized); ClickHouse `otel_metrics_*` subset → MV `gpu_samples` (one row per GPU per 5 s) → `gpu_rollup_1m` |
| **Dashboard** | GPU fleet (8 GPUs as tiles): activity, HBM, power/energy, temps, throttling, errors, fabric bandwidth; GPU → pod → model mapping |
| **Alerts** | Fatal event (Xid 48/63/64/74/79/94/95; AMD RAS uncorrectable / GPU reset) · uncorrectable ECC > 0 · row-remap failed/pending · GPU missing from inventory · temp > threshold or throttled > 5 min · link errors rising · PCIe replays rising |

### 3.2 Kubernetes

| Measure | Notes |
|---|---|
| Node ready + conditions; **allocatable vs requested GPUs** (`nvidia.com/gpu` / `amd.com/gpu`) | Vendor-neutral via a recording rule (`k8s:gpu_allocatable`, `k8s:gpu_requested`) |
| Pod phase, readiness, restarts, last termination reason (OOMKilled, Error) | kube-state-metrics |
| Pending pods + reason (Insufficient GPU) | KSM + k8s events |
| Container CPU/memory vs requests/limits, **CPU throttling** of vLLM / gateway / EPP pods | cAdvisor / kubeletstats. CPU throttling hurts tokenization and scheduling. |
| **Model pod time-to-ready**: scheduled → image pulled → weights loaded → ready | k8s events + vLLM startup log lines → `pod_startups` |
| GPU stack health: GPU Operator / device plugin / exporter pods up; healthy device count | KSM + operator metrics |
| Deployment / LeaderWorkerSet / InferencePool changes | `k8sobjects` → `k8s_events`, shown as Grafana annotations (lightweight change events) |

| | |
|---|---|
| **Source** | kube-state-metrics, kubelet/cAdvisor, `k8sobjects` (events + watched objects), GPU Operator (NVIDIA or AMD) |
| **Lands in** | Prometheus; ClickHouse `otel_logs` → MV `k8s_events`, `pod_startups` |
| **Dashboard** | Cluster / workload: GPU allocation, pod health, restarts, startup phases, event timeline |
| **Alerts** | Pod Pending > 5 min (Insufficient GPU) · CrashLoopBackOff / restarts > 3 in 15 min · OOMKilled · node NotReady · device plugin reports fewer than 8 healthy GPUs · model pod not ready > 15 min |

### 3.3 Network (single-node scope)

With one node, the fabric that matters is **inside** the node. Inter-node InfiniBand/RoCE comes with the second node.

| Measure | Why on one node | Source |
|---|---|---|
| NVLink / xGMI bandwidth and errors (from §3.1) | TP=8 all-reduce traffic. Also KV transfer if P/D runs on the same node. | GPU exporter |
| PCIe bandwidth / replays (from §3.1) | Weight loading, KV offload to CPU | GPU exporter |
| **NCCL / RCCL errors and timeouts** | TP hangs show up here first | vLLM logs (NCCL/RCCL lines) → `otel_logs` |
| **KV transfer latency / bytes** (only if P/D enabled) | Prefill → decode hand-off | vLLM/llm-d KV-connector metrics + span attributes |
| Node NIC rx/tx, drops, errors | North-south traffic at 1k+ req/s with streaming | node-exporter |
| Gateway: request rate, latency, 4xx/5xx/429, active connections, **stream duration**, request/response bytes | User-facing network | Envoy metrics + access logs |
| Pod-to-pod: gateway → EPP → vLLM latency | Routing overhead | spans (§3.7) |
| Model weight download throughput / duration | Cold start | init-container / vLLM logs |

| | |
|---|---|
| **Lands in** | Prometheus; ClickHouse `otel_logs`, `otel_traces`, `gpu_samples` |
| **Dashboard** | Network: fabric bandwidth per GPU pair/link, gateway traffic, routing hop latency, NCCL/RCCL error log panel |
| **Alerts** | NCCL/RCCL timeout or error in logs · fabric link errors rising · gateway 5xx > 1% · 429 spike · NIC drops |

### 3.4 llm-d

| Measure | Notes |
|---|---|
| Gateway / InferencePool: requests, latency, errors per model; **pool avg KV-cache utilization; pool avg queue size**; ready endpoints | llm-d / Gateway API Inference Extension metrics |
| **EPP routing decision per request**: chosen pod (prefill / decode), candidate count, scorer scores (prefix, queue, KV), decision latency, fallback reason | EPP span attributes (+ logs) → `routing_decisions` |
| **Routing effectiveness**: actual prefix-cache hit rate per pod (from vLLM) vs EPP prefix score; request share per pod; queue skew (max − min waiting across pods) | Derived. Predicted-vs-actual only if EPP exposes the score (**verify**). Otherwise distribution + actual hit rate. |
| No-endpoint-available / rejected / shed requests by objective (criticality) | EPP metrics |
| P/D (if enabled): prefill vs decode pod queue and utilization, KV transfer time | metrics + spans |
| Replicas per model, model → pod placement, TP size | `model_deployments` (from k8s objects + vLLM startup args) |

| | |
|---|---|
| **Lands in** | Prometheus; ClickHouse `otel_traces` → MV `routing_decisions` |
| **Dashboard** | llm-d routing: per-pod load and queue, prefix-hit per pod, routing distribution, decision latency, rejects |
| **Alerts** | Queue skew > threshold for 5 min · no-endpoint-available > 0 · EPP decision p99 > 50 ms · prefix-hit rate drop vs 24 h baseline |

### 3.5 vLLM (CUDA and ROCm builds; SGLang later)

Engine metrics are normalized to `llm.*` names with `engine="vllm"`. SGLang plugs in later with a mapping file only.

| Measure | vLLM examples (verify on pinned version) |
|---|---|
| Running / waiting requests | `vllm:num_requests_running`, `vllm:num_requests_waiting` |
| **KV-cache usage**, **prefix-cache queries / hits**, **preemptions** | `vllm:kv_cache_usage_perc`, `vllm:prefix_cache_queries/hits`, `vllm:num_preemptions_total` |
| **TTFT, inter-token latency, e2e, queue, prefill, decode** histograms | `vllm:time_to_first_token_seconds`, `vllm:inter_token_latency_seconds`, `vllm:e2e_request_latency_seconds`, `vllm:request_queue_time_seconds`, `…prefill_time…`, `…decode_time…` |
| Prompt / generation tokens, per-request token histograms, **tokens per iteration** (batch efficiency) | `vllm:prompt_tokens_total`, `vllm:generation_tokens_total`, `vllm:iteration_tokens_total` |
| Finished requests by `finish_reason` (stop / length / abort), errors | `vllm:request_success_total` |
| Spec-decode acceptance rate (if enabled) | `vllm:spec_decode_*` |
| Engine config: model, revision, dtype/quant, TP, `max_num_seqs`, `max_model_len`, `gpu_memory_utilization`, engine version, **CUDA vs ROCm** | startup args/log → `model_deployments` |
| Lifecycle: weight load time, CUDA/HIP graph capture, engine crashes, OOM | logs |
| **Per-request span**: request id, model, tokens in / cached / out, queue / prefill / decode / TTFT / e2e, finish reason, sampling params | vLLM OTLP tracing → `engine_requests` |

| | |
|---|---|
| **Lands in** | Prometheus (full); ClickHouse `otel_traces` → `engine_requests`; `otel_metrics_*` subset → `engine_rollup_1m`; `otel_logs` |
| **Dashboard** | Inference health per model / pod: SLO panels (TTFT / ITL / e2e p50 / p95 / p99 vs target), throughput (req/s, tokens/s), queue, KV usage, prefix hit, preemptions, finish reasons, errors |
| **Alerts** | TTFT or ITL p95 SLO burn (5 m / 1 h windows) · KV usage > 90% for 10 min · preemptions rising · waiting queue growing for 5 min · error rate > 1% · engine restart / OOM · `finish_reason=length` spike |

### 3.6 Tokenomics

All derived from `llm_requests` + `gpu_samples` + cost dictionaries. No separate write path.

| Measure | Definition |
|---|---|
| Tokens: in, cached (prefix hit), out | by model / org / app / user / pod, per minute and hour |
| Throughput | output tokens/s per pod, per GPU, per model; requests/s |
| **GPU-seconds per model** | GPUs allocated to the model's pods × time (from k8s + `model_deployments`) |
| **Infra cost per 1M tokens** | GPU-seconds × $/GPU-hour (per GPU type: H100 vs MI300X) ÷ tokens |
| Cost per org / app / user | the pod's GPU cost split by each request's token share |
| Price-list cost (optional) | tokens × $/1M (input / cached / output) per model, for comparison with external APIs |
| **Energy per token** | Δ `gpu.energy_joules_total` of the model's GPUs ÷ output tokens (J/token, Wh/1k tokens) |
| **Utilization efficiency** | tokens per GPU-second; idle allocated GPU-hours (allocated, ~0 requests) |
| **Cache savings** | cached tokens × prefill cost avoided |
| **Waste** | tokens from aborted requests (client disconnect), preemption recompute, failed requests' GPU time |
| **Goodput** | % of requests meeting TTFT + ITL SLO, and the tokens they carried |
| **Vendor comparison** | cost / 1M tokens and J/token for the same model on H100 vs MI300X (when both are measured) |

| | |
|---|---|
| **Lands in** | `tokenomics_hourly` (SummingMergeTree), `slo_rollup_1m`, dictionaries `dict_gpu_cost`, `dict_model_price` |
| **Dashboard** | Tokenomics: cost per 1M tokens per model, cost by org/app, J/token, goodput, cache savings, waste, idle GPU-hours, H100 vs MI300X |
| **Alerts** | Hourly cost per org > 3× 7-day baseline · runaway generation (output tokens per request ≫ p99 baseline) · idle allocated GPUs > 2 h · goodput < 95% |

### 3.7 AI & LLM traceability

| Measure | Notes |
|---|---|
| **One trace per request**: client (if instrumented) → gateway → EPP route → vLLM (prefill pod → [KV transfer] → decode pod) | W3C `traceparent` propagated by gateway and vLLM (**verify vLLM extracts it**) |
| **Identity** on every request: `request_id`, `trace_id`, `org_id`, `app_id`, `user_id` (pseudonymized), `session_id` | Header contract (§4) |
| **Execution context**: model, revision, engine + version, CUDA/ROCm, pod, node, `gpu_uuids`, vendor, TP size | joined from `model_deployments` + `gpu_inventory` |
| **Reproducibility**: temperature, top_p, top_k, max_tokens, seed, stop, n | vLLM span GenAI attributes |
| **Outcome**: status, finish_reason, error, aborted_by_client | gateway + vLLM |
| App / RAG / agent spans (optional) | Apps can send GenAI-semconv spans to the collector OTLP endpoint. Stored, linked, and shown in the trace view. |
| **"Why was request X slow?" drill-down** | trace → `llm_requests` breakdown → `routing_decisions` → `gpu_samples` (ASOF ±30 s, same GPUs) → `engine_rollup_1m` (queue / KV at that minute) → `k8s_events` (±5 min) |
| Logs carry `trace_id` / `request_id` | structured JSON logs; collector parses and attaches |

| | |
|---|---|
| **Lands in** | `otel_traces`, `gateway_requests`, `engine_requests`, **`llm_requests`**, `routing_decisions` |
| **Dashboard** | Request explorer (search by request_id / trace_id / org / model / slow / error) + per-request drill-down |
| **Alerts** | Unmatched-join rate > 1% (gateway rows without an engine span, or the reverse; signals broken propagation) |

---

## 4. Integration contract (agree first)

| Header | Required | Set by | Purpose |
|---|---|---|---|
| `traceparent` | yes (gateway creates if missing) | client / gateway | join gateway ↔ EPP ↔ vLLM |
| `x-request-id` | yes (gateway creates if missing) | client / gateway | lookup |
| `x-org-id`, `x-app-id` | yes | **gateway, derived from the API key** | showback |
| `x-user-id` | optional (pseudonymized) | client | per-user usage |
| `x-session-id` | optional | client | multi-turn grouping |

Deployment requirements:
- Gateway: tracing on; OTel access logger with the headers above, plus upstream pod, response code, durations and bytes.
- vLLM: `--otlp-traces-endpoint` pointing to the agent collector.
- All pods: downward-API `POD_NAME` / `NODE_NAME`.
- GPU exporters: Kubernetes pod-mapping mode on.

---

## 5. ClickHouse tables (MVP)

| Table | Built from | Notes |
|---|---|---|
| `otel_traces`, `otel_logs`, `otel_metrics_{gauge,sum,histogram}` | gateway collector (from Kafka) | Pre-created superset of the exporter schema; materialized hot columns (`org_id`, `model`, `pod`, `gpu_uuid`, `vendor`) |
| `gateway_requests` | `Null` table + MV (gateway access logs are routed here, not into `otel_logs`) | identity, pod, status, e2e, bytes. Staging, TTL 1 d. |
| `engine_requests` | MV ← `otel_traces` (vLLM request span) | tokens, stage timings, finish reason, sampling params. Staging, TTL 1 d. |
| `routing_decisions` | MV ← `otel_traces` (EPP span) | chosen pod(s), scores, decision latency. Staging, TTL 1 d (summary kept in `llm_requests`). |
| **`llm_requests`** | **Refreshable MV (1 min)**: joins rows **inserted in the last 3 min** of the three tables above on `trace_id` | `ReplacingMergeTree` keyed on `trace_id`, so overlapping windows are idempotent. Also counts unmatched rows. Durable copy, 30 d. |
| `gpu_samples` | MV ← `otel_metrics_*` (normalized GPU subset) | one row per (5 s, gpu_uuid): all §3.1 fields + pod, vendor |
| `k8s_events`, `pod_startups` | MV ← `otel_logs` | |
| `gpu_inventory`, `model_deployments` | MV ← logs/metrics labels + k8s objects | `ReplacingMergeTree`, latest state |
| `engine_rollup_1m`, `llm_rollup_1m`, `gpu_rollup_1m`, `slo_rollup_1m`, `tokenomics_hourly` | MVs | quantile states, sums, energy deltas |
| `dict_gpu_cost`, `dict_model_price`, `dict_slo` | dictionaries over small config tables | editable without redeploy |

**Retention:**

| Data | Keep |
|---|---|
| Staging request tables | 1 day |
| Traces | 3 days |
| Logs | 7 days |
| `llm_requests`, `gpu_samples` | 30 days |
| Rollups, tokenomics | 13 months |

Partition by day; columns that later phases need (`engine`, `role`, `vendor`, `org_id`) exist from day one.

---

## 6. Repository structure

```
normalize/
  gpu.yaml                 # vendor → normalized GPU names (NVIDIA + AMD), units, label maps
  engine_vllm.yaml         # vLLM → llm.* names (engine_sglang.yaml later)
  generate.py              # → Prometheus recording rules + collector OTTL transform (single source)
deploy/helm/
  gpu-operator-nvidia-values.yaml   # DCGM exporter, pod mapping, custom field list
  gpu-operator-amd-values.yaml      # AMD device plugin + metrics exporter, pod mapping
  kube-prometheus-stack-values.yaml
  kafka-values.yaml                 # KRaft, topics, retention
  clickhouse-values.yaml
  otel-agent-values.yaml  otel-gateway-values.yaml
deploy/llm-d/              # gateway access-log + tracing config, vLLM args (CUDA + ROCm variants)
collector/agent.yaml gateway.yaml   # shared with local compose
clickhouse/migrations/0001_otel.sql 0002_requests.sql 0003_gpu_k8s.sql 0004_rollups.sql 0005_tokenomics.sql
grafana/dashboards/        # 01-gpu 02-k8s 03-network 04-llm-d 05-vllm 06-tokenomics 07-request-explorer
grafana/alerts/
prometheus/rules/          # generated normalized rules + alert rules
local/docker-compose.yml   # kafka, clickhouse, prometheus, grafana, collectors, simulators
sim/
  fake_gpu_exporter.py     # --vendor nvidia|amd, 8 GPUs, scenarios: load, throttle, xid/ras, ecc, link errors
  loadgen.py               # functional traffic through gateway → llm-d sim (≈ 200–500 req/s), streaming, aborts
  telemetry_replay.py      # capacity test: emits realistic per-request OTLP records (gateway log, EPP span,
                           # vLLM span) straight into the agent collector at 5k / 7k+ req/s equivalent
  fixtures/amd_mi300x/ fixtures/nvidia_h100/   # captured real exporter output (filled in M8 / M9)
  (llm-d inference simulator image for vLLM-compatible API + metrics; multiple replicas)
tools/migrate.py
tests/
  unit/        test_normalize.py (both vendors → same names/units), test_generate.py
  sql/         fixture inserts → assert MVs, joins, rollups, cost/energy math
  e2e/         compose up → loadgen → assert every dashboard query returns data, for vendor=nvidia and vendor=amd
  load/        capacity: 5k req/s for 1 h + 7k for 15 min (telemetry_replay); Kafka lag, insert rate, parts, join completeness
docs/header-contract.md  docs/runbook.md
```

---

## 7. Milestones — thin vertical first, then deepen

| # | Milestone | Done when |
|---|---|---|
| M0 | Decisions | §1 confirmed; header contract agreed with the gateway owner; versions pinned (vLLM CUDA + ROCm, llm-d, GPU operators, exporters, collector, Kafka, ClickHouse) |
| M1 | **Walking skeleton (all 7 layers, 1–2 signals each) — simulators only** | Local compose: simulators (NVIDIA **and** AMD) + llm-d sim → agent → **Kafka** → gateway → ClickHouse + Prometheus → one panel per layer in Grafana. Verify vLLM `traceparent` extraction + span attributes on a real vLLM container. |
| M2 | GPU layer complete | `normalize/gpu.yaml` for both vendors verified against **real exporter output samples**; GPU dashboard + alerts; `gpu_samples`, `gpu_inventory` |
| M3 | k8s + network complete | §3.2 / §3.3 dashboards and alerts; `k8s_events`, `pod_startups`; NCCL/RCCL log parsing |
| M4 | llm-d + vLLM complete | §3.4 / §3.5 dashboards and alerts; `engine_requests`, `routing_decisions`, `model_deployments` |
| M5 | Traceability | `llm_requests` refreshable join with unmatched-rate metric; request explorer + slow-request drill-down |
| M6 | Tokenomics | cost/energy dictionaries, `tokenomics_hourly`, `slo_rollup_1m`; numbers match hand-computed fixtures |
| M7 | Local hardening (scaled-down) | `telemetry_replay` at **~500 req/s sustained / 700 burst** locally with the same assertions as the real test; ClickHouse-down and Kafka-restart tests; backend Helm values ready for a real cluster |
| M8 | **Real hardware #1: 8×MI300X + capacity test** | AMD GPU Operator + metrics exporter, vLLM ROCm image, llm-d on the node. Capture real exporter output into `sim/fixtures/amd_mi300x/` and fix `normalize/gpu.yaml`. All 7 dashboards populated with real AMD data. P/D on ROCm tried; fall back to aggregated serving if not ready. **Capacity test on this cluster's CPU nodes:** 5k req/s for 1 h + 7k req/s for 15 min, then a step test (8k → 10k → 12k); measured numbers replace the §2.1 estimates (`docs/capacity.md`). |
| M9 | **Real hardware #2: 8×H100** | NVIDIA GPU Operator + DCGM, vLLM CUDA image. Same steps. Confirm the **same dashboards work unchanged** across vendors. First real H100 vs MI300X tokenomics comparison (same model, same load). |

## 8. Acceptance criteria

- **All seven layers** have a dashboard with live data and at least the alerts listed in §3, for **both** `vendor=nvidia` and `vendor=amd`. Simulated in CI; real on MI300X (M8), then H100 (M9).
- **Sustained 5,000 req/s for 1 h:**
  - Kafka consumer lag < 60 s at p99
  - zero dropped telemetry (collector dropped/refused counters = 0)
  - `llm_requests` row count = requests sent (±0.1%)
  - join completeness ≥ 99%
  - ClickHouse active parts per partition stays < 300
- **7,000 req/s burst for 15 min:** no data loss; lag recovers to < 60 s within 10 min after the burst.
- **Measured ceiling** (step test) is documented, along with the first component to saturate.
- ClickHouse down for 10 min: no data loss (Kafka retains it); catches up afterwards.
- From a `request_id`, the drill-down shows trace, stage timings, routing decision, GPU state (±30 s), engine queue/KV at that minute, and k8s events, in under 5 s.
- Cost per 1M tokens, J/token and goodput match hand-computed fixtures within 1%.
- The same Grafana dashboards work unchanged for an NVIDIA and an AMD node.

## 9. Still out of the MVP (next phases)

- **Multi-node:** InfiniBand/RoCE, inter-node NCCL, ClickHouse/Kafka replication.
- **Other engines and tenancy:** SGLang, per-tenant logins and row policies.
- **Sampling and payloads:** tail sampling, prompt capture.
- **App-quality layers:** evals/guardrails, RUM, external DB/ERP dependencies, security audit log.
- **Probes and profiling:** synthetic probes, profiling.
- **Storage:** S3 cold tier.

The columns, labels and Kafka topics these need already exist, so each is an addition, not a rewrite (PLAN.md §7, all_telemetry.md).

## 10. Risks to retire early (M1–M2)

| Risk | Mitigation |
|---|---|
| vLLM doesn't propagate `traceparent`, or lacks span timing attributes on the pinned version | Test in M1. Fallback: join on `x-request-id` (gateway sets it; vLLM uses it as its request id), with timings from vLLM per-request logs. |
| Simulators drift from real exporters, so M8 finds many mismatches | Build the AMD simulator from the AMD exporter docs plus any sample output we can get early. In M8, capture real output into `sim/fixtures/` and make the simulator replay it, so CI uses real shapes from then on. |
| AMD exporter lacks a field (e.g. per-link xGMI errors, throttle reasons) | Mark as `null` for AMD in the mapping. Dashboards show "n/a" rather than 0. Document the gaps. |
| llm-d P/D or NIXL KV transfer on ROCm not ready on the pinned version | Run AMD in aggregated (non-P/D) mode for the MVP; P/D panels hidden when `role` is absent. |
| EPP doesn't expose per-request scorer outputs | Use routing distribution + actual prefix-hit per pod; open an upstream issue/PR. |
| Cardinality from vLLM histograms × pods × models | Collector drops unneeded buckets/labels for the ClickHouse subset; Prometheus series budget alert. |
| Single Kafka broker / ClickHouse node = single point of failure | Accepted for the MVP. Agent `file_storage` queue covers Kafka restarts. Replication is the first post-MVP task. |
