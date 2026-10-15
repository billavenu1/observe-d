# observe-d — Telemetry Specification (all layers)

This document lists everything we observe on an LLM inference platform running on GPU clusters (e.g. 8×H100 or 8×MI300X nodes). For each layer it covers:
- what to collect
- the signal type
- sampling cadence
- correlation keys
- alerts
- the ClickHouse table the data lands in

It is the contract for collectors, exporters, instrumentation and the ClickHouse schema in [PLAN.md](PLAN.md).

**How to read this**
- The base content comes from the original telemetry checklist. Items marked **[added]** were missing from it and added here.
- Items marked **[corrected]** fix something in the checklist that was wrong or would not work as written. Each correction is explained inline and summarized in §0.
- Storage targets are **ClickHouse tables** (PLAN v2), not Parquet files. §19 maps the checklist's Parquet tables to the ClickHouse tables.
- Metric names are examples. Exporters and engines rename metrics between versions, so pin versions and verify (PLAN §10.9).

**Signal types used below:** `M` = metric (gauge/counter/histogram), `L` = log/event, `T` = trace/span, `W` = wide event (one row per request/step with many columns), `D` = dimension/inventory (slowly changing), `P` = profile.

---

## 0. Corrections to the source checklist

| # | Checklist said | Problem | What we do instead |
|---|---|---|---|
| C1 | `gpu_util_percent (SM/compute utilization)` | NVML/DCGM `GPU_UTIL` is **not** SM utilization. It is "% of time any kernel was running". A GPU can show 100% while using 10% of its SMs. | Collect `GPU_UTIL` **and** profiling metrics: SM active, SM occupancy, tensor-pipe active, DRAM active (§A). Use SM active / tensor active for efficiency. |
| C2 | `gpu_id (local GPU index)` as the join key | The local index is not stable. `CUDA_VISIBLE_DEVICES` inside a container renumbers GPUs, and indexes repeat on every node. | Join on **`gpu_uuid`**. Keep `gpu_index` for display only. |
| C3 | vLLM "prefill (retrieval & memory cache)" | Prefill is the engine computing the KV cache for prompt tokens. Retrieval (RAG) happens in the application before the engine is called. | Retrieval → app/agent spans (§I). Prefill → engine timing (§F). |
| C4 | vLLM `batch_id`, `batch_merge_time_us`, `batch_split_time_us`, `vllm_batch_log` with full request lists | vLLM uses **continuous (iteration-level) batching**. The batch is re-formed every scheduler step (every ~10–50 ms), so there is no stable `batch_id` per request. Logging every step's request list would be millions of rows per hour per replica. | Per-request batch stats (steps taken, avg/max batch size seen, preemptions) on `llm_requests`. **Sampled** scheduler-step stats into `engine_steps` (§F.4). |
| C5 | Per-stage child spans `vllm.prefill`, `vllm.batch_wait`, `model.forward`, `model.decode` | vLLM's native OTel tracing emits one request span with **timing attributes** (queue, prefill, decode, TTFT, forward/execute time), not child spans. | Keep the attributes. Optionally have the collector or an MV synthesize child spans from them for the waterfall view (§I.3). |
| C6 | Layer D "llm-d = dispatcher with shards / shard placement" | This describes a generic dispatcher. llm-d actually provides an **Inference Gateway** (Envoy + Gateway API Inference Extension), an **Endpoint Picker (EPP)** with prefix-cache/load-aware scorers, **prefill/decode disaggregation**, a KV-cache indexer and a variant autoscaler. | Layer E is rewritten around those components. Shard placement is kept as TP/PP/EP topology in `model_deployments`. |
| C7 | Layer F "SGLang = higher-level orchestration / language program steps" | In this platform SGLang is an **inference engine** (alternative to vLLM, RadixAttention prefix cache). Its frontend language is a separate, optional use. | Layer G covers the SGLang **engine** (same normalized metrics as vLLM), plus SGLang program runs as optional. |
| C8 | Network layer = ingest_api → queue → worker → S3 | That is the **observability pipeline's** own network, not the inference network. It also misses NVLink/xGMI, InfiniBand/RoCE, NCCL and KV-cache transfer, which dominate LLM latency. | Layer D covers the inference fabric. The pipeline's own network moves to platform self-monitoring (§O). |
| C9 | Trace spans `enqueue.to.queue`, `queue.consumer.receive` inside the request trace | These are telemetry-pipeline hops, not hops of the user's inference request. Putting them in the user trace mixes two systems. | Separate pipeline traces (§O). The inference trace is browser → app → gateway → EPP → engine → response. |
| C10 | "Tag GPU samples with active request_id(s)" | A GPU runs many requests at once under continuous batching. Per-request GPU attribution from a 1 s sample is not meaningful, and request IDs as metric labels explode cardinality. | Join requests to GPU samples by `pod`/`gpu_uuid` + time window (ClickHouse `ASOF JOIN`). Attribute GPU time/energy to requests **proportionally by tokens** (§H). |
| C11 | GPU sampling every 0.5 s | DCGM profiling metrics (`DCGM_FI_PROF_*`) cost GPU overhead at high frequency and conflict with Nsight/CUPTI profilers. | Profiling metrics at 1–5 s (default 5 s). Basic gauges (power, temp, memory) at 1–10 s. Pause DCGM profiling during on-demand profiling sessions (§Q). |
| C12 | Catalog of file `min_ts/max_ts` for pruning | No longer needed. ClickHouse prunes by partition (`toDate(ts)`) and primary key. | Dropped (PLAN v2 §9). |
| C13 | "Time-based joins: round timestamps to micro/second" | Joining at µs across hosts only works if clocks agree. Clock skew of tens of ms between nodes is common without PTP. | Monitor clock offset per node (§B). Join on windows, not equality. Use `ASOF JOIN`. |

---

## 1. Common labels / correlation keys

Attach these everywhere they apply. **High-cardinality IDs (`request_id`, `trace_id`, `user_id`, `session_id`) go only on events/spans/wide rows, never on metric labels.**

| Key | Description | On metrics | On logs/spans/events |
|---|---|---|---|
| `org_id`, `team_id` | Tenant | yes (if bounded) | yes |
| `app_id` **[added]** | Calling application/product | yes | yes |
| `user_id` | End user (pseudonymized) | **no** | yes |
| `session_id` **[added]** | Conversation / browser session | no | yes |
| `request_id` | UUIDv7 per inference request | no | yes |
| `trace_id`, `span_id`, `parent_span_id` | W3C trace context | no | yes |
| `env`, `region`, `zone`, `cluster` **[added]** | Deployment location | yes | yes |
| `node` (k8s node name) | Host | yes | yes |
| `namespace`, `pod`, `container` **[added namespace/container]** | k8s workload | yes | yes |
| `gpu_uuid` **[corrected, see C2]**, `gpu_index` | Physical GPU | yes | yes |
| `gpu_model` **[added]** | H100-SXM5-80GB, MI300X, … | yes | yes |
| `model_name`, `model_version` / `revision` | Served model | yes | yes |
| `lora` **[added]** | LoRA adapter name | yes (bounded) | yes |
| `engine`, `engine_version` **[added]** | vllm / sglang + version | yes | yes |
| `role` **[added]** | `prefill` / `decode` / `both` (P/D disaggregation) | yes | yes |
| `inference_pool`, `objective` **[added]** | llm-d InferencePool / InferenceObjective (criticality) | yes | yes |
| `stage` | Pipeline stage of a span/event | — | yes |
| `stream_type`, `schema_key`, `schema_version` | Which table/schema the record belongs to | — | yes |

---

## A. GPU hardware (8×H100 / 8×MI300X)

**Goal:** per-GPU, per-process hardware behaviour that drives latency, throughput, reliability and cost.

**Sources:**
- NVIDIA: **DCGM exporter** (Kubernetes mode for pod mapping), DCGM health checks, Fabric Manager logs, NVML.
- AMD: **AMD Device Metrics Exporter** (amd-smi based, pod mapping), ROCm SMI, RAS events.

### A.1 Metrics (per GPU)

| Category | Metrics | Cadence |
|---|---|---|
| Utilization | `gpu_util_percent` (busy time, see C1), **SM active %**, **SM occupancy %**, **tensor/matrix pipe active %**, FP16/FP8 pipe active **[added]**, graphics/GFX activity (AMD) | 1–5 s |
| Memory | `memory_used/free/total_bytes`, **HBM bandwidth utilization (DRAM active %)**, memory reserved | 1–10 s |
| Power & energy | `gpu_power_watts`, **cumulative energy (J)** **[added; needed for J/token]**, power limit / enforced cap **[added]** | 1–10 s |
| Thermal & clocks | GPU temp, **HBM/memory temp** **[added]**, SM clock, memory clock, **clock throttle reasons** (power, thermal, HW slowdown, sync boost) **[added]** | 5–10 s |
| PCIe | PCIe TX/RX bytes, **PCIe replay counter**, **link gen/width (detect degraded link)** **[added]** | 5–15 s |
| Intra-node fabric **[added]** | NVLink TX/RX bytes per link, NVLink CRC/replay/recovery errors, NVSwitch errors (Fabric Manager); AMD xGMI RX/TX per link | 5–15 s |
| Reliability **[added]** | **Xid errors** (code), ECC single-bit / double-bit (volatile + aggregate), **row-remap pending/failed**, retired pages, AMD RAS per-block correctable/uncorrectable | 10–30 s + events |
| Media engines | NVDEC/NVENC/JPEG utilization (multimodal ingest only) | 10 s |
| Partitioning **[added]** | MIG instance profile / ID (NVIDIA); compute partition (SPX/CPX) and memory partition (NPS1/NPS4) mode (AMD) | on change |

### A.2 Per-process / per-container

| Metric | Notes |
|---|---|
| `process_gpu_mem_bytes`, `process_gpu_util_pct`, `pid`, `container_id`, `pod` | DCGM/AMD exporter k8s mode maps GPU → pod; process accounting via NVML |
| CUDA/HIP context count | Detects stray processes on serving GPUs |
| Kernel times (cuBLAS/cuDNN/NCCL/attention kernels) | Profiling only, on demand (§Q), not continuous |
| PyTorch CUDA caching-allocator stats **[added]** | Allocated vs reserved, fragmentation, `num_alloc_retries`, `num_ooms`. Exported by the engine or captured at OOM. |

### A.3 Counters / events

- `kernel_launch_count` (profiling only), **OOM events**, **GPU reset count**, **Xid events with code + PID**, ECC events, **GPU fell off bus**, **thermal/power throttle start/stop**, NVLink link down, Fabric Manager errors.
- Model load/unload events: weights loaded, shard loaded, time taken, bytes, source.
- **DCGM active health checks** **[added]**: `dcgmi diag` level 1 at node start; levels 2–3 on drain or burn-in. Results stored as events.

### A.4 Inventory (dimension) **[added]**

`gpu_inventory`: gpu_uuid, serial, model, memory size, driver version, CUDA/ROCm version, VBIOS/firmware, InfoROM version, node, PCI bus ID, NUMA node, NVLink/xGMI topology, MIG/partition mode, first_seen, last_seen.

### A.5 Correlation & storage

- Keys: `node`, `gpu_uuid`, `gpu_index`, `pod`, `container`, `namespace`, `model_name` (via pod → `model_deployments`), `ts`.
- Raw: `otel_metrics_gauge` / `otel_metrics_sum`.
- Pivoted wide view `gpu_samples(ts, node, pod, gpu_uuid, gpu_index, util, sm_active, tensor_active, dram_active, mem_used, mem_total, power_w, energy_j, temp_c, mem_temp_c, sm_clock, throttle_reasons, xid_last, ecc_dbe)`. Built by MV for easy joins.
- Rollups: `gpu_rollup_1m` (avg/max/min, energy delta, throttle seconds, error deltas).

### A.6 Alerts

- Xid critical codes (e.g. 48, 63/64, 74, 79, 94/95) → page + cordon.
- ECC DBE > 0, row-remap failure / pending remap → drain.
- GPU missing from inventory (fell off bus).
- Temp > threshold or sustained thermal throttle > N min.
- NVLink/xGMI error counters increasing; PCIe link degraded.
- OOM frequency > X in 15 min; recurring GPU reset.
- **Underutilization:** tokens/s below expected AND SM active < 30% → queueing/scheduling issue.

---

## B. Node / host & facility **[added layer — split from A]**

| Category | What | Cadence | Source |
|---|---|---|---|
| CPU | util %, load 1/5/15, per-NUMA util, CPU throttling, context switches | 10–15 s | node-exporter / hostmetrics |
| Memory | used, available, page cache, swap, **hugepages**, NUMA misses | 10–15 s | node-exporter |
| Disk | IO bytes/s, IOPS, queue length, latency, **NVMe used by model cache**, SMART/NVMe health | 15–30 s | node-exporter, smartctl exporter |
| NIC (host) | rx/tx bytes/s, drops, errors, packet loss | 10–15 s | node-exporter |
| **Time sync** | NTP/PTP clock offset, sync state | 30–60 s | chrony/ptp exporter. Required for cross-node joins (C13). |
| Kernel | dmesg/journald: NVRM/Xid, AMD RAS, OOM killer, MCE (machine-check), soft lockups, filesystem errors | events | filelog/journald receiver |
| BMC / IPMI | Fan speeds, PSU status/power, inlet/outlet temp, chassis power draw, SEL events | 30–60 s | IPMI / Redfish exporter |
| Facility (optional) | Rack PDU power, cooling, PUE | 1–5 min | DCIM / PDU SNMP. Gives true energy-per-token. |
| Container runtime | containerd errors, image pull time/bytes, image cache size | events | containerd logs/metrics |

**Storage:** `otel_metrics_*`, `otel_logs`. **Alerts:** MCE events, clock offset > 10 ms, PSU failure, disk full on model cache, kernel soft lockup.

---

## C. Kubernetes (control plane, scheduling, pod lifecycle)

**Goal:** see scheduling, allocation, lifecycle and k8s events that affect availability and cold-start latency.

**Sources:** kube-state-metrics, kubelet/cAdvisor (`kubeletstats`), `k8s_cluster` receiver, Kubernetes API watch (`k8sobjects`: events + objects), API server / scheduler / etcd metrics, GPU Operator.

### C.1 Metrics (10–60 s)

| Category | Metrics |
|---|---|
| Nodes | ready status, conditions (MemoryPressure, DiskPressure, PIDPressure), allocatable CPU/mem/**GPU** (`nvidia.com/gpu` / `amd.com/gpu`), cordoned/unschedulable, taints |
| Pods | phase, restart count, `OOMKilled` / last termination reason, readiness, container CPU/mem usage vs requests/limits, **CPU throttled seconds** |
| Workloads | Deployment/StatefulSet/**LeaderWorkerSet** **[added; multi-node models]** desired/available/unavailable replicas |
| Scheduling | Scheduling latency (scheduler histogram), pending pod count by reason, **GPU fragmentation** **[added]**: nodes with free GPUs < model TP size, and the largest contiguous free GPU block per node |
| Autoscaling | HPA/KEDA desired vs current, scaling events, metric values used (e.g. `num_requests_waiting`) |
| **Cold start / time-to-ready** **[added]** | Pod created → scheduled → image pulled → model downloaded → weights loaded → CUDA graphs captured → ready. Each phase timed. |
| Control plane | API server request latency/errors, etcd db size & latency, controller work-queue depth |
| GPU stack **[added]** | GPU Operator component health, device plugin healthy devices, DCGM exporter up |
| Batch queues **[added, if shared cluster]** | Kueue/Volcano queue depth, admitted vs pending GPU workloads, preemptions of inference by training |
| Storage | PVC capacity/usage (model cache), volume attach/mount latency |

### C.2 Events / logs

- Scheduler: FailedScheduling (Insufficient GPU, taints, affinity), preemption.
- Kubelet: OOM kills, evictions, probe failures, container runtime errors.
- Pod lifecycle: create, schedule, pull, start, ready, stop, restart, preempt.
- Admission controller / webhook failures, image pull errors (`ErrImagePull`, `ImagePullBackOff`).
- **Object changes** **[added]**: Deployment/ConfigMap/InferencePool spec changes → `change_events` (§N).

### C.3 Traces

Optional: operator/controller flows. **Synthesized pod startup trace [added]**: scheduling → image pull → model download → weight load → warmup → ready, built from timestamps in events and engine logs.

### C.4 Correlation & storage

- Keys: `pod`, `node`, `namespace`, `deployment`/`lws`, labels (`app`, `model_name`, `role`), `container_id`.
- `request_id`/`trace_id` appear only where pods emit them (engine/app spans).
- Raw: `otel_metrics_*`, `otel_logs` (events via `k8sobjects`).
- MV `k8s_events(ts, kind, namespace, name, reason, type, message, pod, node, count, labels)`.
- `pod_startups(ts, pod, model, phase timings…)` **[added]**.

### C.5 Alerts

- Pod Pending > 5 min with reason Insufficient GPU.
- Node NotReady; GPU node cordoned unexpectedly.
- CrashLoopBackOff / restarts > N in 15 min; OOMKilled spike.
- **Model pod time-to-ready > N min** **[added]**.
- **GPU fragmentation blocks TP=8 placement while total free GPUs ≥ 8** **[added]**.
- HPA at max replicas while queue still growing **[added]**.

---

## D. Network & interconnect

**Goal:** latency, bandwidth, errors and congestion on every path a request and its KV cache take. **[corrected, see C8]**

### D.1 Inference fabric **[added]**

| Path | Metrics | Cadence | Source |
|---|---|---|---|
| Intra-node GPU↔GPU (NVLink/NVSwitch, xGMI) | per-link bandwidth, errors (see §A) | 5–15 s | DCGM / AMD exporter |
| Inter-node GPU↔GPU (InfiniBand / RoCEv2) | port xmit/rcv data & packets, **symbol errors, link error recovery, link downed**, port state/speed, **PFC pause frames (tx/rx)**, **ECN marked packets / CNPs** (congestion), discards | 5–15 s | IB/NIC exporter, `ethtool -S`, ConnectX counters |
| Collectives (NCCL / RCCL) | all-reduce / all-gather / all-to-all time, achieved bus bandwidth, timeouts, **watchdog hangs** | per op (sampled) / events | NCCL logs (`NCCL_DEBUG=INFO`), PyTorch NCCL watchdog, NCCL profiler plugin later |
| **MoE expert parallel all-to-all** | dispatch/combine latency, bytes, imbalance | sampled | engine metrics (DeepEP etc.), when EP is used |
| **KV-cache transfer (P/D disaggregation)** | transfer latency p50/p99, bytes, throughput, failures/retries, queue wait | per request + metrics | llm-d / vLLM KV connector (NIXL/UCX) metrics + span attributes |
| KV offload tiers | GPU↔CPU↔disk/remote (LMCache etc.) transfer bytes & latency | 5–15 s | engine/connector metrics |

### D.2 Service network (from checklist)

| Metric | Notes |
|---|---|
| RTT / latency p95/p99 between client → gateway → EPP → engine pod | From spans (preferred) + mesh metrics |
| rx/tx bytes/s per interface and per pod | node-exporter, cAdvisor |
| TCP connection errors, timeouts, resets, **retransmits** | eBPF (Cilium/Hubble, Beyla) or node TCP stats |
| TLS handshake time | Envoy / mesh |
| Request and response sizes (bytes) | Envoy access logs, spans |
| **Streaming duration & SSE/HTTP2 stream resets** **[added]** | Envoy. LLM responses stream for seconds to minutes. |
| Client-facing request rate, 4xx/5xx, **429 rate-limited** **[added]** | Gateway |
| **DNS latency/failures** **[added]** | CoreDNS metrics |
| **Model weight download throughput** **[added]** | Object store / HF / NFS → node, during cold starts |

### D.3 Logs / events

Connection timeouts, dropped packets, retries, HTTP error traces, IB link flaps, NCCL errors/timeouts, KV transfer failures, Envoy access logs.

### D.4 Traces

Network hops appear as client/server spans with `net.peer.*`/`server.address`, retry counts and bytes. KV transfer appears as a span (or attributes) between prefill and decode spans.

### D.5 Storage

`otel_metrics_*`, `otel_logs`, `otel_traces`. Wide view `network_samples(ts, src, dst, node, pod, iface, rtt_ms, bytes_sent, bytes_recv, retransmits, errors)` from MV/rollup.

### D.6 Alerts

- IB link down/flapping; symbol errors rising.
- PFC pause storm; ECN/CNP spike (congestion).
- NCCL timeout/hang.
- KV-transfer p99 > threshold.
- Packet loss > threshold; high RTT gateway → engine.
- 5xx rate; 429 rate spike.

---

## E. llm-d (distributed inference: gateway, routing, P/D, KV indexing) **[corrected, see C6]**

**Goal:** observe routing quality, queueing, prefill/decode balance, KV-cache locality and autoscaling.

### E.1 Metrics

| Component | Metrics |
|---|---|
| Inference Gateway / InferencePool | requests per model/objective, request latency, error rate, input/output tokens per model, **pool avg KV-cache utilization, pool avg queue size**, ready endpoints |
| **EPP (Endpoint Picker / scheduler)** | routing decision latency, decisions per scorer outcome, fallback count, no-endpoint-available count, per-pod picks |
| Prefix-aware routing | **predicted prefix-hit ratio (from KV index) vs actual hit ratio (from engine)**. This is the core llm-d KPI. |
| Queueing | `request_queue_depth` per model/pod (from engine), time in gateway queue, `scheduling_latency_us` (enqueue → endpoint picked) |
| P/D disaggregation | prefill-pod vs decode-pod utilization/queue, **prefill:decode pod ratio**, KV transfer time (§D), decode wait-for-KV time |
| KV-cache indexer **[added]** | KV block stored/removed events per pod, index size, event lag/staleness |
| Replicas & placement | active replicas per model/variant, `replicas_per_model`, TP/PP/DP/EP topology per replica, per-pod model memory footprint |
| Model lifecycle | `model_load_time_us` (cold start), unload time, LoRA adapter load/unload time **[added]** |
| Autoscaler (variant autoscaler / HPA) | desired vs actual replicas, saturation signal values, scale decisions + reason |
| Per-replica outcomes | requests processed/failed per pod |
| **Flow control / admission** **[added]** | requests shed/rejected by criticality (objective), queue timeouts |

### E.2 Logs / events

Routing errors, no-capacity rejections, model/LoRA load/unload, KV index events, pool membership changes (pod added/removed), autoscaler decisions, P/D transfer failures.

### E.3 Traces

Span `llm_d.route` (or EPP span) as a child of the gateway span. Attributes:
- `chosen_pod_prefill`, `chosen_pod_decode`, `candidates_count`
- per-scorer scores (`score.prefix`, `score.queue`, `score.kv_usage`)
- `prefix_hit_predicted`, `objective`, `queue_time_us`, `fallback_reason`

### E.4 Storage

- `otel_traces`, `otel_metrics_*`.
- Wide table `llm_routing_decisions(ts, request_id, trace_id, org_id, model_name, objective, candidates, chosen_prefill, chosen_decode, scores Map, prefix_hit_predicted, decision_us, status, error)`. This replaces the checklist's `llm_dispatch_events`.
- `kv_cache_events`.

### E.5 Alerts

- Scheduling latency > SLA.
- **Actual prefix-hit ≪ predicted** (KV index stale or routing misconfigured).
- No-endpoint-available > 0; pool queue growing while autoscaler at max.
- Model/LoRA load failures; insufficient replicas for throughput.
- P/D imbalance: prefill queue grows while decode is idle, or vice versa.

---

## F. vLLM (inference engine)

**Goal:** instrument scheduling, KV cache, prefill, decode and token emission. **[corrected, see C3/C4/C5]**

**Sources:**
- `/metrics` (Prometheus) scraped by the OTel agent.
- Native OTLP tracing (`--otlp-traces-endpoint`, optional `--collect-detailed-traces`).
- Engine logs (periodic stats, warnings).
- Startup args/config.

### F.1 Aggregated metrics

| Category | Metrics (vLLM examples) |
|---|---|
| Scheduler / queue | running, waiting, swapped requests (`vllm:num_requests_running` / `_waiting`) |
| **KV cache** | KV usage % (`vllm:kv_cache_usage_perc`; v0: `gpu_cache_usage_perc`), **prefix-cache queries & hits** → hit rate, `tokens_served_from_cache`, **preemptions** (`vllm:num_preemptions_total`), blocks total/free |
| KV offload **[added]** | hits per tier (GPU / CPU / disk / remote) when using offloading connectors (e.g. LMCache) |
| Latency histograms | **TTFT**, **inter-token latency (ITL/TPOT)**, E2E, queue time, prefill time, decode time, inference time |
| Tokens | prompt tokens, generation tokens, per-request prompt/generation histograms, **tokens per scheduler iteration** (batch efficiency) |
| Batch | `batch_size_distribution` (running seqs per step), `iteration_tokens` histogram, **max_num_seqs / max_num_batched_tokens saturation** **[added]** |
| Throughput | tokens generated/s per replica, prompt tokens/s |
| Outcomes | requests finished by `finish_reason` (stop / length / abort), errors, **aborts due to client disconnect** **[added]** |
| **Speculative decoding** **[added]** | draft tokens, accepted tokens → acceptance rate, mean accepted length |
| **LoRA** **[added]** | running/waiting adapters, max LoRAs, adapter swap count |
| **Structured output / guided decoding** **[added]** | grammar compile time, per-token constraint overhead |
| **Multimodal** **[added]** | image/audio preprocessing time, encoder time, multimodal tokens, encoder-cache hit rate |
| **Context limits** **[added]** | requests rejected/truncated for exceeding `max_model_len`, prompt length distribution |
| API server (CPU side) **[added]** | tokenization/detokenization time, API-server event-loop lag, HTTP concurrency |
| Retries & errors | request retries, engine errors, **engine-core dead / restarts** **[added]** |
| Per-model hotness | requests and tokens per model/LoRA per replica |

### F.2 Per-request wide event (lands in `llm_requests`; replaces the checklist's `vllm_per_request`)

| Field | Notes |
|---|---|
| ts, request_id, trace_id, org_id, model_name, model_version, lora, engine, replica/pod, node, gpu_uuids, role | identity |
| tokens_in, **tokens_cached (prefix hit)**, tokens_prefill (computed = in − cached), tokens_generated, tokens_reasoning **[added]** | tokens |
| latency_queue_us, latency_prefill_us, latency_decode_us, ttft_us, tpot_us (mean ITL), **itl_p99_us** **[added]**, e2e_us, latency_forward_us / execute_us (detailed traces only), latency_postprocess_us (detokenize/stream) | stages (glossary §F.6) |
| steps, avg_batch_size_seen, max_batch_size_seen, preemptions **[corrected C4, replaces batch_id]** | batching |
| prefix_cache_hit (bool / ratio), kv_offload_tier_hit **[added]** | cache |
| spec_accepted_tokens, spec_draft_tokens **[added]** | spec decode |
| finish_reason, status, error, aborted_by_client **[added]** | outcome |
| temperature, top_p, top_k, max_tokens, seed, n, structured_output **[added]** | reproducibility |

### F.3 Logs / events

- Engine stats lines, preemption warnings, KV-cache full / swapping, OOM / CUDA errors, NCCL errors.
- Model load start/finish (bytes, duration), CUDA graph capture duration, warmup.
- Prefix cache evictions (counts only; per-key logging is too high-volume).
- **Degenerate output detection [moved]**: repetition loops, empty output, max-length hits. Computed by an async eval job or gateway post-processor (§J), not the engine.
- Startup config dump → `model_deployments`.

### F.4 Sampled scheduler steps (`engine_steps`; replaces `vllm_batch_log`) **[corrected C4]**

Every Nth iteration, or all iterations for a short window when debugging, record:

`ts, pod, step_id, num_running, num_waiting, num_prefill_seqs, num_decode_seqs, num_batched_tokens, kv_usage, step_time_us, forward_time_us`

No full request list by default. Opt-in debug mode records the request_id list for a few seconds.

### F.5 Traces

- One `llm_request` span per request from vLLM, with timing attributes (queue, TTFT, prefill, decode, e2e, forward/execute when detailed tracing is on) and GenAI semconv attributes (`gen_ai.request.model`, `gen_ai.usage.input_tokens`/`output_tokens`, …).
- Optional derived child spans for the waterfall (C5).
- Attributes: `prefix_cache_hit`, `tokens_cached`, `preemptions`, `role`, `gpu_uuids`.

### F.6 Latency glossary **[added — single definition for all dashboards]**

| Term | Definition |
|---|---|
| queue | arrival at engine → first scheduled |
| prefill | first scheduled → first token computed (includes chunked-prefill steps) |
| TTFT | arrival at engine → first token emitted (≈ queue + prefill). **Client TTFT** (§K) adds network + gateway + routing. |
| decode | first token → last token |
| ITL / TPOT | time between consecutive output tokens (TPOT = decode ÷ (tokens_out − 1)) |
| forward / execute | GPU model execution time summed over the request's steps (detailed tracing) |
| postprocess | detokenize + stream write |
| e2e | arrival → last token sent |

### F.7 Correlation & storage

- Keys: `model_name`, `model_version`, `request_id`, `trace_id`, `replica/pod`, `gpu_uuids`, `role`.
- Tables: `otel_metrics_*` → `engine_rollup_1m`; `llm_requests`; `engine_steps`; `otel_traces`; `otel_logs`; `model_deployments`.

### F.8 Alerts

- KV usage > 90% sustained; preemptions rising.
- Prefix-cache hit rate below baseline (caching regression).
- Waiting queue growing; batch wait time above threshold.
- TTFT / TPOT p95/p99 SLO burn (multi-window).
- Decode/engine error spike; engine-core restarts.
- Spec-decode acceptance rate drop.
- Context-length rejections spike.

---

## G. SGLang (later) **[corrected, see C7]**

### G.1 SGLang as engine (primary)

The same categories as §F, normalized to the same names with `engine="sglang"`. Examples:
- running/queued requests
- token usage (KV)
- **RadixAttention cache hit rate**
- TTFT, ITL, E2E
- prompt/generation tokens, throughput
- spec-decode acceptance

Mapping lives in `pkg/normalize/sglang.yaml`. Dashboards and alerts are reused unchanged.

### G.2 SGLang programs (optional, if the frontend language is used)

| Field | Notes |
|---|---|
| run_id, step_id, step_name, start/end/duration_us | per step |
| step_input/output_size_bytes, token usage per step, model calls per step | per step |
| step_failure_count, retry_count, status | per step |
| run_id → request_id / trace_id, user_id, org_id | correlation |

- Traces: run span → step spans → model-call spans (link to engine spans).
- Storage: `sglang_runs` (wide) + `otel_traces`.
- Inputs/outputs go only in `llm_payloads`, under the same opt-in rules.

---

## H. Tokenomics (usage, cost, efficiency)

**Goal:** attribute tokens, GPU time and energy to orgs, apps, users, models and features. Find waste and optimize cost.

### H.1 Per-request (from `llm_requests`; no separate write)

| Field | Notes |
|---|---|
| tokens_in, tokens_cached, tokens_prefill, tokens_generated, tokens_reasoning, tokens_out | |
| token_type breakdown: system / user / assistant-history / retrieval-context / tool **[kept; requires gateway-side tokenization, so optional]** | Shows what is driving prompt size (e.g. RAG context bloat) |
| `cost_estimate_usd` (price-list based) | `dictGet` on `dict_price_catalog` |
| **`gpu_seconds_attributed`, `energy_j_attributed`** **[added]** | Pod GPU time and energy over the window, split by token share (C10) |
| **`cost_infra_usd`** **[added]** | gpu_seconds × $/GPU-hour (amortized capex + power + DC overhead) |
| usage_type (chat, completion, embedding, rerank, batch) **[added]** | |

### H.2 Events

`billing_event` when a request completes. Labels: org_id, app_id, user_id, api_key_id, model, usage_type, tokens, cost. Derived by MV, not emitted separately.

### H.3 Aggregates (MVs)

| Aggregate | Notes |
|---|---|
| tokens/min, cost/min per org/app/model | `tokenomics_hourly` + 1-min rollup |
| top-k cost contributors (user / org / model / api_key) | |
| **cost per 1M tokens (infra)** per model/GPU type **[added]** | |
| **energy per token (J/token, Wh/1k tokens)** **[added]** | |
| **tokens per GPU-second**, idle allocated GPU-hours **[added]** | efficiency |
| **cache savings** = cached tokens × prefill cost **[added]** | |
| **waste**: preempted/recomputed tokens, tokens for aborted/timeout requests, failed requests' GPU time **[added]** | |
| **goodput**: requests/tokens meeting the SLO ÷ total **[added]** | |
| budget burn rate & forecast-to-exhaustion per org/app **[added]** | |
| **cost per successful task/session** (agent workflows) **[added]** | joins traces → many LLM calls per task |

### H.4 Alerts

- Org hourly cost > budget, or spike > 3× baseline.
- **Runaway generation**: tokens_generated per request ≫ expected, or `finish_reason=length` spike.
- **Runaway agent loop** **[added]**: one session/trace with > N LLM calls.
- Cost per 1M tokens regression after a deploy (join `change_events`).
- Idle allocated GPUs > N hours.

### H.5 Storage

`llm_requests` (source of truth), `tokenomics_hourly` / `tokenomics_daily` (replaces `token_usage` and `billing_aggregates`), `dict_price_catalog`, `dict_gpu_cost` **[added]**.

---

## I. AI & LLM traceability (end-to-end)

**Goal:** trace any request from the user's click → app → retrieval/tools → gateway → routing → GPU → response, for root-cause analysis and audit.

### I.1 IDs and propagation

- `request_id`: generated at the first ingress (app/gateway), UUIDv7. Propagated through the gateway headers, the engine (vLLM `request_id` / `X-Request-Id`), all spans, logs and the `llm_requests` row.
- `trace_id` / `span_id`: W3C `traceparent` from the browser (§K) or the app. **Never regenerated by the telemetry ingest API** (PLAN G7).
- `session_id` / `conversation_id`, `turn_index` **[added]**: multi-turn context.
- `task_id` / `agent_run_id` **[added]**: one agent task spans many LLM calls.
- `batch_id`: **dropped** (C4); replaced by per-request batch stats + sampled `engine_steps`.
- GPU linkage: by `pod` + `gpu_uuids` + time window (C10), not by tagging GPU samples.

### I.2 Mandatory attributes on all request telemetry

`request_id, trace_id, org_id, app_id, model_name, model_version, engine, replica/pod, node, ts, stage`. Add `session_id` where known.

### I.3 Recommended span tree **[corrected C5/C9, extended]**

```
rum.interaction (browser)                                  [added]
└─ http.server (app / agent)
   ├─ agent.step / chain.step ...                          [added]
   │  ├─ retrieval.embed        (model, tokens, latency)   [added]
   │  ├─ retrieval.vector_search(db, top_k, doc_ids, scores) [added]
   │  ├─ retrieval.rerank       (model, kept_k)            [added]
   │  ├─ tool.call              (tool name, status, args_hash) [added]
   │  └─ guardrail.input        (policy, decision)         [added]
   └─ gen_ai.chat  (client span, GenAI semconv)
      └─ gateway.request (Envoy / Inference Gateway)
         ├─ request.auth / rate_limit                      [kept; rate_limit added]
         ├─ llm_d.route (EPP decision, scores)
         └─ vllm.llm_request (prefill pod)
            ├─ [derived] queue → prefill
            ├─ kv.transfer (P/D)                           [added]
            └─ vllm.llm_request (decode pod) → [derived] decode → postprocess
      └─ guardrail.output / eval.async (linked)            [added]
http.response.send / stream.close
```

### I.4 Logs

Always include `request_id` and `trace_id` (JSON structured logs; the OTel log bridge injects them automatically).

### I.5 Lineage & reproducibility **[added]**

- prompt template ID + version, system-prompt hash, tool-schema hash
- model revision (commit/hash), tokenizer version, quantization, LoRA version
- sampling params, engine version + config hash (from `model_deployments`)
- retrieval index version / embedding model version (RAG)

### I.6 Correlation techniques

- Equality joins on `request_id`, `trace_id`, `pod`, `node`, `model_name`.
- **Time-window / ASOF joins** for metrics (GPU, k8s, network) around span time. Requires clock sync (§B, C13).
- No file catalog needed (C12). ClickHouse primary key + partition pruning, plus the `otel_traces_trace_id_ts` lookup table for fast trace lookup.

### I.7 Storage

- `otel_traces` (replaces `trace_spans`: one row per span, attributes as Map, hot attributes as materialized columns), `llm_requests`, `llm_routing_decisions`, `llm_payloads` (opt-in), `llm_evals`, `audit_log`.
- `trace_spans` is exposed as a view over `otel_traces` for checklist compatibility.

### I.8 Sampling strategy

- **`llm_requests` wide events: 100%, never sampled.** They are small and drive billing and SLOs. **[added rule]**
- Traces: **tail-based sampling** in the OTel gateway collector (`tail_sampling` processor). Keep 100% of errors, latency > SLO, guardrail blocks and eval failures. Keep 1–5% of normal traffic, raised automatically during anomalies.
- Trace-aware load balancing (`loadbalancing` exporter by trace_id) is required so all spans of a trace reach the same tail-sampler. **[added]**
- Payloads: opt-in per org, redacted, short TTL.

### I.9 Debugging aids

- Scrubbed payloads (PII redaction at the collector) with a 1–7 day TTL.
- **"Show trace" API** (`query_api /trace/{id}`): returns spans + `llm_requests` + `llm_routing_decisions` + `gpu_samples` (ASOF, ±30 s, same pods/GPUs) + `k8s_events` (same pods, ±5 min) + `change_events` (±1 h) + evals.

---

## J. Quality, safety & evaluation **[added layer]**

| What | Fields | Source |
|---|---|---|
| Online evals | evaluator, metric (groundedness, relevance, correctness, toxicity, PII leak, hallucination), score, label, judge model | async eval workers sampling `llm_payloads` |
| Guardrails | stage (input/output), policy, decision (allow/block/modify), category (prompt injection, jailbreak, PII, toxicity), latency | guardrail service spans/events |
| User feedback | thumbs, rating, comment flag, regenerate/copy/abandon signals | app → `/ingest/eval` |
| Heuristic output checks | empty output, repetition loops, truncation (`finish_reason=length`), refusal rate, language mismatch, JSON/schema validity for structured output | gateway post-processor / eval job |
| Drift | prompt length/topic distribution shift, output length shift, refusal-rate shift per model version | rollups |
| Offline eval runs | eval suite, model version, scores, linked to `change_events` (deploy) | CI / eval pipeline |

- **Storage:** `llm_evals`, `otel_traces` (guardrail spans), rollup `quality_rollup_1h`.
- **Alerts:** eval score drop vs baseline after deploy, guardrail block-rate spike, refusal-rate spike, structured-output invalid rate.

---

## K. RUM — real user monitoring **[added layer]**

| What | Fields |
|---|---|
| Session & view | session_id, view/page, app version, device, browser, OS, country (no IP stored) |
| Web vitals | LCP, INP, CLS, TTFB, FCP |
| **User-perceived LLM latency** | click → request sent → **first streamed token (client TTFT)** → last token. Tokens rendered/s, **stream stalls** (gap > N ms), stream errors/reconnects. |
| User actions | stop-generation clicks (→ wasted tokens), regenerate, copy, edit-and-resend, abandon |
| Errors | JS errors, failed fetches, SSE/WebSocket disconnects |
| Correlation | browser injects `traceparent`, so a RUM event links to the backend trace / `request_id` |
| Later | session replay (masking on by default) |

- **Source:** OTel browser SDK → `ingest_api /ingest/rum` → Kafka → `rum_events`.
- **Alerts:** client TTFT p95 SLO, JS error-rate spike, stream-disconnect spike.

---

## L. Databases & external dependencies (RAG / ERP) **[added layer]**

| System | Metrics / events |
|---|---|
| Postgres / MySQL | connections (active/idle/max), locks, deadlocks, replication lag, cache hit ratio, slow queries / top statements, txn rate, bloat |
| Redis (sessions, semantic cache) | hit rate, latency, evictions, memory, connected clients |
| **Semantic / response cache** | hit rate, similarity threshold, tokens saved, stale-hit complaints |
| Vector DB (pgvector / Milvus / Qdrant / Weaviate) | query latency p50/p99, QPS, index size, segments/compaction, memory, **recall@k** (from evals), embedding-model version |
| Embedding & rerank model servers | same as §F (they are models too): QPS, latency, batch size, tokens |
| ERP / CRM / retrieval APIs | client-span latency, error rate, timeouts, rate-limit (429) hits, payload size, circuit-breaker state |
| Object storage (model weights, RAG docs) | GET latency, throughput, errors |
| Synthetic checks | `httpcheck` probes on each dependency |

**Storage:** `otel_metrics_*`, `otel_traces` (client spans), `otel_logs`. **Alerts:** dependency p99 or error-rate breach, replication lag, vector-search latency SLO, cache hit-rate drop.

---

## M. Security & audit **[added layer]**

| What | Fields |
|---|---|
| AuthN/AuthZ | auth failures by API key/org/IP range, invalid/expired keys, permission denials |
| API key usage | per key: requests, tokens, cost, last used, origin. Detect leaked keys (new ASN/geo, volume spike). |
| Rate limiting & quotas | 429s by org/key/model, quota exhaustion events |
| Prompt-security signals | prompt-injection / jailbreak detections (from guardrails), data-exfiltration patterns, PII in output |
| **Audit log (immutable)** | who called which model, with what config, when, from where; admin actions (model deploy, price change, retention change, role grants); data-access to `llm_payloads` |
| Telemetry-data access | queries against payload tables by user (ClickHouse `system.query_log` filtered) |
| Supply chain | model artifact hash verification, container image digest per pod |

**Storage:** `audit_log` (MergeTree, long TTL, insert-only role), `otel_logs`. **Alerts:** auth-failure spike, key used from new geo/ASN, payload table accessed by non-approved role.

---

## N. Change events / deploy markers **[added layer]**

Most incidents follow a change. Record every change as an event so dashboards can overlay it and RCA queries can join it.

| Source | Events |
|---|---|
| Kubernetes | Deployment/LWS/InferencePool/ConfigMap spec changes, image tag/digest changes, replica changes |
| Models | model version / LoRA / quantization / engine version / engine flag changes |
| Infra | GPU driver, CUDA/ROCm, firmware, kernel upgrades; node add/remove; network config |
| Platform | price catalog changes, SLO changes, routing config (EPP scorer weights) changes |
| CI/CD | deploy start/finish, commit SHA, author, ticket |

**Storage:** `change_events(ts, source, kind, target, before, after, actor, commit, ticket)`. Shown as Grafana annotations.

---

## O. Observability platform self-monitoring **[added layer; includes checklist C8/C9 items]**

| Component | Metrics |
|---|---|
| OTel collectors (agent/cluster/gateway) | received/accepted/refused/dropped spans, metrics and logs; exporter queue size & failures; memory limiter triggers; scrape failures per target; tail-sampling decisions |
| Kafka | **consumer lag per topic/group (critical)**, throughput, under-replicated partitions, broker disk, request errors |
| ingest_api | `http_requests_total`, latency, enqueue success/errors, validation rejects by reason, 503 backpressure count |
| ClickHouse | insert rows/bytes/s, **parts per partition ("too many parts")**, merges in progress, Kafka-engine consumer errors, `ingest_errors` rows, query latency/errors, memory, disk hot/cold usage, **TTL move failures to S3**, replication queue (when clustered) |
| Object storage (MinIO/S3) | PUT/GET latency, errors, capacity |
| query_api | queries, duration, rows returned, rejected (quota/timeout) |
| Data quality | late events (event time vs ingest time), future timestamps, schema rejects, cardinality (active series count) per metric |
| Pipeline traces | `ingest_api.receive → kafka.produce → ch.kafka_engine.consume` as a separate trace (C9) |

**Alerts:** consumer lag > N min, collector dropping data, too-many-parts, insert errors, series cardinality spike, cold-tier move failures.

---

## P. Synthetic probes / canaries **[added layer]**

- **Golden prompts** per model every 1–5 min through the full path (gateway → llm-d → engine). Measure TTFT, TPOT, e2e, and **output correctness** (exact/regex/eval-based), tagged `synthetic=true`.
- Probe from each region/zone the users come from.
- Endpoint health checks for gateway, query_api, dependencies.
- Synthetic traffic is excluded from tokenomics/billing, but included in SLO dashboards as a separate series.

**Storage:** `llm_requests` (flagged), `otel_traces`, `synthetic_results`. **Alerts:** probe failure, or correctness < threshold after a model change.

---

## Q. Profiling (on demand) **[added layer]**

| What | How |
|---|---|
| Continuous CPU profiling (API server, tokenizer, Python) | eBPF profiler (e.g. OTel eBPF profiler / Pyroscope) → profiles signal |
| GPU kernel profiling | Nsight Systems / rocprof / PyTorch profiler, **triggered on demand** for a pod for N seconds. Pause DCGM profiling metrics during capture (C11). |
| Memory snapshots | PyTorch CUDA memory snapshot on OOM |
| NCCL profiling | NCCL profiler plugin during multi-node debugging |

**Storage:** profile artifacts in S3 (`profiles/{pod}/{ts}`), with an index row in `profiles_index(ts, pod, kind, duration, s3_path, trigger, trace_id)`.

---

## 17. Cross-layer questions → which tables to query

| Question | Query path |
|---|---|
| **Why is request X slow?** | `otel_traces` (trace_id) → `llm_requests` (queue/prefill/decode/TTFT breakdown, preemptions, cache hit) → `llm_routing_decisions` (was it routed to a cold/busy pod?) → `gpu_samples` ASOF on its pods/GPUs (throttling? HBM saturated?) → `engine_steps` (batch size at that time) → `k8s_events` (restarts) → `change_events` (recent deploy?) |
| **Why is model Y expensive?** | `tokenomics_hourly` by model → cost per 1M tokens + idle GPU-hours + waste (preemption, aborted) + cache-hit rate + token-type breakdown (RAG context bloat?) |
| **GPUs underutilized while tail latency is high?** | `gpu_rollup_1m` (SM active, HBM BW) vs `engine_rollup_1m` (waiting queue, batch size, KV usage) vs `llm_routing_decisions` (imbalance across pods). Usual causes: routing imbalance, `max_num_seqs` too low, KV cache too small causing preemption, CPU-bound tokenizer. |
| **Why did TTFT regress today?** | `slo_rollup_1m` + `change_events` overlay + prefix-hit actual vs predicted + prompt-length distribution shift |
| **Is the network the bottleneck for P/D?** | KV-transfer latency in `llm_requests` vs IB counters (PFC/ECN) in `network_samples` |
| **Which GPU is about to fail?** | `gpu_samples` ECC/remap/Xid trends + `gpu_inventory` firmware |
| **Which users see bad UX?** | `rum_events` client TTFT and stalls → join `llm_requests` by trace_id → server vs network share |
| **Did quality drop after the deploy?** | `llm_evals` + guardrail rates before/after `change_events` |

---

## 18. Sampling & retention (ClickHouse TTL)

| Data | Raw cadence | Hot (NVMe) | Cold (S3) | Delete raw | Rollups kept |
|---|---|---|---|---|---|
| `llm_payloads` (raw prompts/outputs) | per request, opt-in | 1–7 d | — | 1–7 d (max 30 d) | — |
| `llm_requests` | 100% | 7 d | to 90 d | 30–90 d | `llm_rollup_1m` 13 mo, `tokenomics_*` ≥ 2 y (billing) |
| `otel_traces` | tail-sampled | 7 d | to 30–90 d | 30–90 d | `slo_rollup_1m` 13 mo |
| GPU / node metrics | 1–10 s | 7 d | to 30 d | 7–30 d | `gpu_rollup_1m` 90 d, hourly rollup ≥ 2 y |
| k8s / network metrics | 10–60 s | 7 d | to 30 d | 30 d | hourly ≥ 1 y |
| `otel_logs` | all (debug sampled) | 3–7 d | to 30 d | 30 d | error-count rollups 1 y |
| `engine_steps` | sampled | 3 d | — | 7 d | — |
| `llm_evals`, `rum_events` | per event | 7 d | to 90 d | 90 d | rollups 13 mo |
| `audit_log`, `change_events` | all | 30 d | to policy | ≥ 1 y (compliance) | — |
| `gpu_inventory`, `model_deployments` | on change | always | — | never (history kept) | — |

---

## 19. Checklist Parquet tables → ClickHouse tables

| Checklist table | ClickHouse | Notes |
|---|---|---|
| `llm_requests.parquet` | `llm_requests` | ReplacingMergeTree on request_id |
| `trace_spans.parquet` | `otel_traces` (+ view `trace_spans`) | OTel exporter schema |
| `gpu_samples.parquet` | `otel_metrics_*` + MV `gpu_samples` + `gpu_rollup_1m` | keyed by `gpu_uuid` |
| `k8s_events.parquet` | `otel_logs` + MV `k8s_events` | via `k8sobjects` receiver |
| `vllm_per_request` | merged into `llm_requests` | one wide row per request |
| `vllm_batches` / `vllm_batch_log` | `engine_steps` (sampled) | see C4 |
| `llm_dispatch_events` | `llm_routing_decisions` | llm-d EPP decisions |
| `token_usage.parquet` | derived from `llm_requests` | no double write |
| `billing_aggregates` | `tokenomics_hourly` / `tokenomics_daily` | SummingMergeTree |
| `network_samples.parquet` | `otel_metrics_*` + MV `network_samples` | |
| `sglang_runs` | `sglang_runs` | later |
| — **[added]** | `llm_evals`, `rum_events`, `kv_cache_events`, `audit_log`, `change_events`, `pod_startups`, `synthetic_results`, `profiles_index`, `gpu_inventory`, `model_deployments`, `ingest_errors` | |

---

## 20. Collection tools per layer

| Layer | Tools |
|---|---|
| GPU (NVIDIA) | DCGM exporter (k8s mode), DCGM health/diag, NVML, Fabric Manager logs |
| GPU (AMD) | AMD Device Metrics Exporter, amd-smi / ROCm SMI, RAS logs, rocprof (on demand) |
| Node / host | node-exporter or OTel `hostmetrics`, journald/filelog, IPMI/Redfish exporter, chrony/PTP exporter, smartctl exporter |
| Kubernetes | kube-state-metrics, kubelet/cAdvisor (`kubeletstats`), `k8s_cluster`, `k8sobjects`, `k8sattributes` processor, GPU Operator metrics |
| Network | IB/NIC exporters (`ethtool`, ConnectX counters), Cilium/Hubble or eBPF (Beyla), Envoy/mesh metrics, CoreDNS, NCCL logs |
| llm-d | Inference Gateway / EPP Prometheus metrics, OTel spans, KV-event stream |
| vLLM / SGLang | `/metrics` scrape, native OTLP tracing, JSON logs, normalization mapping files |
| Apps / agents / RAG | OTel SDK + **GenAI semantic conventions** instrumentation (OpenAI/LangChain/LlamaIndex instrumentors), structured logs |
| Browser | OTel JS browser SDK (document load, fetch/XHR, user interaction, web vitals) |
| Databases | OTel `postgresql`, `mysql`, `redis` receivers; vector DB Prometheus endpoints |
| Queue | Kafka exporter / `kafkametrics` receiver (consumer lag) |
| Pipeline | OTel collector internal telemetry, ClickHouse Prometheus endpoint + `system.*` tables |
| Tracing backend | OTel gateway collector (tail sampling) → ClickHouse `otel_traces`. Jaeger/Tempo not needed; Grafana ClickHouse plugin renders traces. |

---

## 21. Pipeline implementation checklist

- [ ] `request_id` (UUIDv7) + `traceparent` created at first ingress and propagated through gateway → EPP → engine → logs → `llm_requests`.
- [ ] Engine pods emit `pod`, `node`, `gpu_uuids`, `role` on spans and wide events (downward API env vars + `k8sattributes`).
- [ ] DCGM/AMD exporters in Kubernetes mode so GPU metrics carry `pod`/`namespace`.
- [ ] `gpu_samples` / `network_samples` / `k8s_events` MVs for easy time-window joins.
- [ ] Tail sampling with trace-ID load balancing in gateway collectors; `llm_requests` never sampled.
- [ ] Redaction processor before any payload leaves the node.
- [ ] Engine metric normalization (vLLM now, SGLang later) with a contract test per engine version.
- [ ] `change_events` from k8s watch + CI/CD webhook.
- [ ] Clock sync monitored on every node.
- [ ] Cardinality guard: drop/hash forbidden labels (`request_id`, `user_id`, `trace_id`) on metrics at the collector.
- [ ] "Show trace" API that joins spans, request row, routing decision, GPU/k8s context, changes and evals.

---

## 22. Must-capture summary (minimum for end-to-end root cause)

1. `request_id` + `trace_id` propagated end to end (browser → app → gateway → llm-d → engine → logs).
2. `llm_requests` wide row (100%) with the queue / prefill / decode / TTFT / ITL breakdown, cached tokens, preemptions, finish_reason, pod and GPUs.
3. llm-d routing decision per request (chosen pod(s), scores, predicted vs actual prefix hit).
4. Per-request batch stats + sampled `engine_steps`, instead of per-request `batch_id` (C4).
5. GPU samples keyed by `gpu_uuid` + `pod`: SM/tensor/HBM activity, power/energy, throttling, Xid/ECC.
6. Interconnect health: NVLink/xGMI, InfiniBand/RoCE congestion, NCCL, KV-transfer latency.
7. K8s events, pod startup phases, GPU allocation/fragmentation.
8. Token usage + infra cost + energy per request, rolled up for billing, budgets and goodput.
9. Quality/guardrail/eval signals linked by trace_id.
10. Change events, to answer "what changed?"
