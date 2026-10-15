# Observability story — disaggregated LLM inference

What observe-d's UI must let an inference infra engineer answer, in what order, and with which signals.
Dashboards, alerts and simulator scenarios are checked against this document. Telemetry inventory:
[all_telemetry.md](../all_telemetry.md). Emitted shapes: [telemetry-shapes.md](telemetry-shapes.md).

observe-d is not general telemetry. It serves one job: keep a **prefill/decode-disaggregated llm-d + vLLM**
deployment within its latency SLOs at the lowest cost per good token, and get from "something is wrong" to
"this stage, this pod, this cause" in minutes.

## 1. The question and the north-star

> **Are we turning GPU-seconds into SLO-meeting tokens? If not, which stage of the P/D pipeline is losing
> them, and why?**

Raw utilization is never a headline by itself. It always sits next to the output it produced.

| North-star | Definition | Why this and not utilization |
|---|---|---|
| **Goodput** | % of requests (and of output tokens) that succeeded and met the TTFT and TPOT SLOs (`slo_targets`) | What users experience |
| **Good tokens per GPU-second**, per pool | SLO-meeting output tokens ÷ GPU-seconds allocated to the pool | Falls when GPUs are "busy" doing useless work |
| **Cost per 1M good tokens** | GPU cost ÷ good output tokens | The business version of the same number |
| **Headroom** | current load ÷ load at which the SLO breaks (from a serving load test) | Answers "can we take more traffic?" |

### Busy ≠ useful: the effective-work breakdown

The GPU time of each pool (prefill, decode, aggregated) is split into where it went:

| Bucket | Contents |
|---|---|
| **Useful** | tokens of requests that met the SLO |
| **Late** | tokens delivered, but the SLO was missed |
| **Recompute** | preemption recompute, chunked-prefill re-runs |
| **Discarded** | aborted streams (client disconnect), failed requests, rejected spec-decode drafts |
| **Stalled** | allocated but idle: waiting on KV transfer, a TP straggler, or CPU (tokenizer / scheduler) |

This breakdown replaces any bare "compute active %" headline. A GPU at 100% compute can be spending a fifth of
its time on recompute and discarded work while goodput falls.

## 2. The story spine

The engineer reads top to bottom and stops at the first level that is not OK. Every level shows:
- a **verdict** (OK / degraded, plus a one-line rule-based reason);
- a **target or baseline** behind every number;
- **links to the next level that keep the filters** (time, model, pool, pod, GPU, org, key).

| # | Level | Question | Primary signals | Misleading on its own |
|---|---|---|---|---|
| 1 | **Outcome** | Are users served within SLO? | goodput; TTFT p95/p99 and TPOT p95 per model vs `slo_targets`; 5xx / 429 / client-abort rate; multi-window SLO burn | average latency, req/s |
| 2 | **Efficiency** | Are GPUs turning into good tokens? | good tokens/GPU-s per pool; effective-work breakdown; cost and J per good token; headroom | compute %, "busy" % |
| 3 | **P/D balance** | Is the prefill:decode split right for this traffic? | **prefill pool**: queue time, prefill tok/s vs capacity, tensor/compute active. **Decode pool**: KV used, wait-for-KV time, running seqs vs `max_num_seqs`, HBM-bandwidth active, TPOT. **Balance index**: prefill queue time vs decode wait-for-KV time | per-pod CPU/GPU % |
| 4 | **KV handoff** | Is the prefill→decode transfer hurting TTFT? | KV transfer p50/p95, bytes per request, share of TTFT ("handoff tax"), failures and fallback-to-recompute, NVLink/xGMI (later RDMA) link errors | link bandwidth alone |
| 5 | **Routing** | Is llm-d placing requests well? | actual vs predicted prefix hit per pod; queue skew; pick share vs pod load; EPP decision p99; fallbacks; no-endpoint; shed by criticality; scorer/config changes | request share per pod alone |
| 6 | **Engine** | Is each vLLM instance batching well? | tokens per iteration; batch size vs `max_num_seqs` / `max_num_batched_tokens`; preemptions; prefix-cache hit; finish reasons (`length` spike); spec-decode acceptance; API-server CPU throttling | running count alone |
| 7 | **Substrate (suspects)** | Can the hardware and platform be trusted? | **GPU**: Xid/RAS, ECC, row remap, throttling by reason, TP-group straggler. **Containers/k8s**: restarts, OOMKilled, CPU throttling, pending (insufficient GPU), pod time-to-ready. **Network**: gateway latency and status codes, NIC drops, NCCL/RCCL errors | node "healthy" badges |
| 8 | **Attribution** | Who consumes it, and what does it cost? | per org / app / API key / user / session: requests, tokens in/cached/out, goodput, cost, runaway generation | totals without owners |

How the levels map to the UI:
- **Levels 1–2** are the landing page.
- **Levels 3–6** are the inference investigation.
- **Level 7** is a health gate: it reads "all clear" or names a suspect, and is never the front page.
- **Level 8** is tokenomics plus the consumer view (§4).

## 3. Playbooks

Each playbook is a scripted path through the levels: **symptom → hops → evidence → action**. Each one must be
reproducible in the simulator, and every hop must be one click with the context kept.

| # | Symptom | Hops and evidence | Action |
|---|---|---|---|
| P1 | TTFT up, GPUs "at 100%" | L1 TTFT burn → L3 prefill queue rising, prefill tensor-active high, decode fine → L8 one org's prompt length doubled (RAG context bloat) | add a prefill replica, or cap context for that key |
| P2 | TPOT up, compute low | L3 decode HBM-bandwidth 85%+, KV 94%, preemptions rising → L2 recompute share growing | lower decode `max_num_seqs` or add a decode replica (the P/D ratio is wrong) |
| P3 | "100% busy" but goodput falling | L2 recompute + discarded at 20% → L6 preemptions and aborts → L8 one key retrying on timeout | fix the client timeout; protect with criticality shedding |
| P4 | One decode pod hot | L5 queue skew; actual prefix hit ≪ predicted on that pod; change event: scorer weight raised | revert the weight, or raise the queue/KV scorer weights |
| P5 | Handoff tax | L4 KV transfer p95 jumps, fallback-to-recompute > 0 → L7 link errors on one GPU pair | drain the node; open a hardware ticket |
| P6 | Low GPU use, high tail latency | L6 batch far below `max_num_seqs` → L7 API-server CPU throttled (tokenizer starved) | raise the CPU limit |
| P7 | TP straggler | L1 TPOT up on one pod only → L7 one GPU in its TP group power/thermal throttled | fix cooling, or cordon |
| P8 | Capacity and cold start | pending pod "insufficient GPU"; decode startup 3 min; one model's pods idle overnight → L8 idle allocated GPU-hours | rebalance GPUs between models; cache images and weights |
| P9 | Cost spike from one key | L8 key cost 4× baseline, `finish_reason=length` spike, output ≫ p99 → that key's requests | rate-limit, or cap `max_tokens` for the key |
| P10 | "Why was this request slow?" | request → per-stage breakdown (gateway, route, prefill queue, prefill, KV transfer, decode wait-for-KV, decode) → engine state at that minute, GPU ±30 s, k8s ±5 min, change events | follow the named likely cause |

## 4. Consumer view (traces for a user or key)

The consumer view is a first-class page, not just a search box.

- Pick an **org / app / API key / user / session** to see its SLO attainment, cost, token mix, slowest and
  failed requests, and finish reasons.
- The **session view** lists one `session_id`'s turns in order, with prefix-cache reuse between turns. It shows
  whether conversation caching works for that client.
- Each request row opens the P10 drill-down.
- Prompts are not captured (MVP rule). The story uses metadata, tokens and timings only.

## 5. Telemetry gaps

The data layer and the simulator now model **P/D** serving next to aggregated serving
([0010_disaggregated.sql](../clickhouse/migrations/0010_disaggregated.sql)); dashboards do not use the new fields
yet. ✓ = closed in the data layer (names marked modeled / sim-only in [telemetry-shapes.md](telemetry-shapes.md)
still need checking on real llm-d + vLLM).

| Gap | Today | Needed |
|---|---|---|
| Pod **role** (prefill / decode / aggregated) | ✓ resource `llm_d.role` (pod label `llm-d.ai/role`) → `engine_requests.role`, `llm_requests.role`; Prometheus `role` label on engine series | dashboards: per-pool panels |
| Two engine spans per request | ✓ join keeps the prefill and the serving (decode) span apart: `llm_requests.prefill_pod`, `decode_pod`, `has_prefill` | verify the sidecar forwards `traceparent` on real llm-d |
| **KV transfer** time, bytes, failures, fallback | ✓ `kv.transfer` span → `kv_transfers` → `llm_requests.kv_transfer_us`, `kv_bytes`, `kv_fallback`; `vllm:nixl_*` metrics (sim) | real NIXL connector has no span: patch or log-derive; alerts and panels |
| **Decode wait-for-KV** time | ✓ decode span queue time → `llm_requests.decode_wait_us` | verify vLLM's accounting of remote-KV waiting |
| Per-request preemptions and cached tokens | `llm.preempted` and `gen_ai.usage.cached_tokens` are sim-only ([telemetry-shapes.md](telemetry-shapes.md)) | verify on real vLLM; fall back to per-pod counters |
| Spec-decode acceptance; aborts by client | not wired | `vllm:spec_decode_*`; gateway 499 / abort flag |
| **API key id** | ✓ hashed `api_key_id` on the gateway access log → `gateway_requests`, `llm_requests` ([header-contract.md](header-contract.md)) | real gateway must compute the same hash; per-key rollups |
| Effective-work breakdown | not computed | GPU-seconds split by bucket (§1) in `tokenomics_hourly` |
| Headroom | [capacity.md](capacity.md) measures the telemetry pipeline, not serving | per-model SLO-break load from a serving load test |
| Change events | k8s objects only | config / scorer / image changes as annotations on every time chart |
| Simulator | ✓ P/D mode in [inference_stack.py](../sim/inference_stack.py) (`ROLE=prefill/decode`, KV pull with failure → recompute, wait-for-KV) and [telemetry_replay.py](../sim/telemetry_replay.py); the local stack runs every model P/D | P3 retry storm, P6, P9 scenarios |

## 6. Playbook readiness checklist

✓ = exists, ◐ = partial, ✗ = missing. Alert names refer to [alerts.yaml](../prometheus/rules/alerts.yaml) and
[grafana/alerts](../grafana/alerts/). Sim scenarios refer to `python -m tools.dev scenario`.

| Playbook | Signals | Dashboard panels | Alert | Sim scenario |
|---|---|---|---|---|
| P1 prefill saturated | ✓ prompt length by org; `role`, prefill-pod `queue_us` | ◐ TTFT yes, prefill pool no | ✓ `TTFTSLOBreach` | ✓ `--playbook p1` (`--load prompt_shift`: acme prompts ×3) |
| P2 decode KV pressure | ✓ KV, preemptions per pod; `role`; `decode_wait_us` | ✓ KV-cache usage, preemptions | ✓ `ITLSLOBreach`, `KVCacheSaturated`, `EnginePreempting` | ✓ `--engine kv_pressure` (decode pool) |
| P3 busy but not useful | ◐ aborts via finish reason; recompute GPU time not computed | ◐ waste panel | ✓ `GoodputBelowTarget` | ◐ `kv_pressure`; no retry storm |
| P4 routing skew | ◐ EPP scores modeled; prefix predicted vs actual; weights on the route span + `epp.config` event (sim) | ✓ queue skew, predicted vs actual | ◐ `EngineQueueSkew`; no prefix-hit-drop alert | ✓ `--playbook p4` (`--routing scorer_skew`: prefix weight 0.9) |
| P5 handoff tax | ✓ `kv_transfer_us`, `kv_bytes`, `kv_fallback` per request; `vllm:nixl_*` (modeled) | ✗ | ✗ | ✓ `--playbook p5` (`--engine kv_transfer_degraded` + `--gpu link_errors`) |
| P6 CPU-starved engine | ✓ CPU throttling; tokens per iteration | ✓ CPU throttling panel | ✗ no CPU-throttling alert | ✗ |
| P7 TP straggler | ◐ per-GPU throttling + GPU→pod map; no straggler metric | ◐ | ✓ `GPUThermalThrottling` | ◐ `--gpu throttle` (one GPU needed) |
| P8 capacity / cold start | ✓ pending, `pod_startups`, idle GPU cost | ✓ | ◐ `GPUPodPending`; no idle-GPU alert | ◐ |
| P9 key cost spike | ✓ org/app; hashed `api_key_id` on `llm_requests` | ◐ by org/app/model | ✓ `OrgCostSpike`, `RunawayGeneration` | ✗ |
| P10 slow request | ✓ prefill queue, prefill, wait-for-KV, KV transfer, decode stages on `llm_requests` | ◐ drill-down lacks the P/D stages | ◐ `TracePropagationBroken` (pipeline health) | ✓ `--engine slow` |

## 7. Page structure

Each page is built around the question it answers. The data stays the same; only the order and framing change.

| Page | Levels | Content |
|---|---|---|
| Service health | 1–2 | goodput, SLO burn per model, good tokens/GPU-s, effective-work bar, headroom, firing alerts, "first failing level" |
| P/D pipeline | 3–4 | prefill and decode pools side by side, balance index, handoff tax, ratio recommendation |
| Routing | 5 | llm-d placement quality |
| Engine | 6 | vLLM batching and KV per pod |
| Requests & consumers | 8 + P10 | request explorer, drill-down, key/user/session view |
| Infrastructure suspects | 7 | GPU, containers/k8s and network as one health gate, with drill-ins |
| Tokenomics | 8 | cost per good token; waste using the effective-work buckets |
| Logs | evidence | always opened pre-filtered from another page |

Design rules for every page:
- Every number shows its target or baseline.
- Utilization is always paired with the output it produced.
- When a page is not OK, it opens with a one-line likely cause.
- Change events are drawn on every time chart.
- Every anomaly links onward with the filters kept.
