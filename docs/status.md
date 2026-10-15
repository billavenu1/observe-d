# Status

Where the MVP stands against [MVP.md](../MVP.md) §7 (milestones) and §8 (acceptance). "Verified" means it was run
on the local stack (Podman + kind, simulated 8×MI300X node) and checked by a test or by hand.

## Milestones

| # | Milestone | State | Evidence |
|---|---|---|---|
| M0 | Bootstrap | done | pyproject/uv, versions.yaml, header contract, CI (lint, types, unit, SQL, generated-file check) |
| M1 | Walking skeleton, all 7 layers | done | `tools.dev up` brings up the whole stack; data in every layer |
| M2 | GPU layer | done (sim) | 21 normalized metrics for NVIDIA and AMD, `gpu_samples`, `gpu_inventory`, GPU dashboard, alerts; AMD names unverified until M8 |
| M3 | Kubernetes + network | done (sim) | k8s GPU rules, `k8s_events`, pod startups, filelog for fatal/collective lines, network dashboard |
| M4 | llm-d + vLLM | done (sim) | engine normalization (`llm:*`), `engine_requests`, `routing_decisions`, `model_deployments`, dashboards |
| M5 | Traceability | done (sim) | `llm_requests` join: 100% complete on live traffic; request explorer drill-down verified |
| M6 | Tokenomics | done (sim) | cost / energy / goodput / cache-savings; SQL test matches hand-computed values within 1% |
| M7 | Local hardening | done | load + resilience tests ([capacity.md](capacity.md)); real-cluster backend manifests; e2e CI workflow (not yet run on GitHub) |
| M8 | Real MI300X node + 5k/7k capacity test | **not started** | needs the hardware; follow [deploy.md](deploy.md) |
| M9 | Real H100 node | **not started** | needs the hardware |

## Tests

| Suite | Count | What it proves |
|---|---|---|
| `pytest -m "not sql and not e2e"` | 47 | normalization (both vendors), generators, simulators (aggregated and P/D), telemetry shapes, replay records |
| `pytest -m sql` | 7 | request join (late spans, idempotent re-joins, P/D prefill + decode + KV transfer, hashed key id), SLO rollup, tokenomics math, Xid/RAS parsing |
| `pytest -m e2e` | 109 | every panel query of the 8 dashboards returns data on the live stack |
| `tools.loadtest` | see [capacity.md](capacity.md) | row count exact, join complete, no drops, drain time, part counts, outage recovery |

Fault injection verified end to end: KV pressure → `KVCacheSaturated`, `EnginePreempting`, `TTFTSLOBreach`;
ECC → `GPUUncorrectableECC`; AMD RAS/GPU reset log lines → `gpu_fatal_events` → `GPUFatalEventFromLogs`;
RCCL errors → `collective_errors`.

## Known gaps (to close in M8/M9 or later)

1. **Names not yet verified against real components:**
   - every AMD exporter metric and the AMD kernel-log format
   - vLLM span attribute names
   - llm-d EPP span and metric names
   - `gen_ai.usage.cached_tokens` and the `engine.started` event are simulator-only

   See [telemetry-shapes.md](telemetry-shapes.md). M8 replaces the simulator models with captured fixtures.
2. **Kernel logs on real nodes** need a journald receiver, or a forwarder if the collector image lacks
   `journalctl`. Locally, the fault lines come from pod stdout.
3. **Single-instance components.** Kafka (RF 1) and ClickHouse (no replica) are single points of failure,
   accepted for the MVP. Production settings are marked in `deploy/backend/`.
4. **Cost dictionary credentials** are inline in `0005_cost.sql`; use a named collection in production.
5. **CI e2e workflow** (`.github/workflows/e2e.yml`, Docker) is written but not yet run on GitHub.
6. **Out of MVP scope:** SGLang (mapping-only change), InfiniBand/RoCE (second node), RUM, evals/guardrails,
   tail sampling, per-tenant access control, S3 cold tier (MVP.md §9).
