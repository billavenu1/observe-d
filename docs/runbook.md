# Runbook

## Local stack

| Task | Command |
|---|---|
| Start everything (AMD node) | `uv run python -m tools.dev up --vendor amd` |
| Start as NVIDIA node | `uv run python -m tools.dev up --vendor nvidia --skip-monitoring` |
| Rebuild simulator image | add `--rebuild` |
| Change load | `uv run python -m tools.dev rate 50` |
| Inject engine fault (llama-70b decode pool by default) | `uv run python -m tools.dev scenario --engine slow\|errors\|kv_pressure\|nccl_error\|kv_transfer_degraded\|normal [--pool decode\|prefill\|both] [--model llama-70b\|qwen-7b]` |
| Reproduce a playbook | `uv run python -m tools.dev scenario --playbook p1\|p4\|p5\|normal` (P1 `--load prompt_shift`, P4 `--routing scorer_skew`, P5 KV degradation + link errors) |
| Inject GPU fault | `uv run python -m tools.dev scenario --gpu throttle\|xid\|ecc\|link_errors\|load` |
| Re-publish dashboards | `uv run python -m tools.dev dashboards` |
| Status / URLs | `uv run python -m tools.dev status` |
| Tear down | `uv run python -m tools.dev down` |

UIs: Grafana http://localhost:3300 (admin/admin) · Prometheus http://localhost:9090 · ClickHouse http://localhost:8123 (otel/otel).

## Where is my data? (follow the pipeline)

1. **Source** — pod `/metrics` or OTLP from the pod. `kubectl -n llm logs deploy/inference-gateway`.
2. **Agent collector** — `kubectl -n observe-d logs ds/otel-agent | grep -i error`; self-metrics on :8888
   (`otelcol_exporter_send_failed_*`, `otelcol_exporter_queue_size`).
3. **Kafka** — `podman exec od-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group observe-d-gateway`
   (LAG column = backlog).
4. **Gateway collector** — `podman logs od-otel-gateway | grep -i error`.
5. **ClickHouse raw** — `SELECT count() FROM otel_traces WHERE Timestamp > now() - INTERVAL 5 MINUTE`.
6. **Derived tables** — refreshable views: `SELECT view, status, last_success_time, exception FROM system.view_refreshes`.
   MV insert errors surface in the gateway collector log.

## Alerts → first actions

| Alert | First action |
|---|---|
| GPUFatalEvent / GPUFatalEventFromLogs / GPUUncorrectableECC / GPURowRemapFailed / GPUMissing | Cordon + drain the node; open a hardware ticket with GPU UUID + serial (`gpu_inventory`). |
| GPUThermalThrottling / GPUOverTemperature | Check fans/PSU (BMC), inlet temperature, power cap; compare with neighbours. |
| GPUPCIeReplays | Degraded host link: reseat/replace; check link gen/width. |
| GPUPodPending | `kubectl describe pod` — insufficient GPUs or fragmentation (GPU dashboard → placement). |
| KVCacheSaturated / EnginePreempting | Raise `gpu_memory_utilization`, lower `max_num_seqs`/`max_model_len`, or add replicas. |
| EngineQueueGrowing / TTFTSLOBreach | Check queue skew (llm-d dashboard); if balanced, scale out. |
| EngineQueueSkew | Routing imbalance: prefix affinity too sticky or a slow pod; check EPP scores. |
| NoEndpointAvailable | No ready engine for a model: pods crashing or scaled to zero. |
| GatewayErrorRateHigh | Request explorer → filter status=error → drill down a trace. |
| RunawayGeneration | Many requests hitting max_tokens: client prompts or a model regression. |
| TracePropagationBroken (join health > 1% missing engine spans) | `traceparent` not propagated gateway → vLLM, or vLLM tracing disabled. |
| CollectiveLibraryErrors | NCCL/RCCL errors: check fabric (network dashboard), IB/xGMI link errors, then restart the engine pod. |
| GoodputBelowTarget | Check the vLLM dashboard SLO panel per model; usually KV pressure or queueing. |
| OrgCostSpike | Tokenomics dashboard → by org/app: a new workload, runaway generation, or a retry storm. |

Prometheus alerts are in Prometheus/Alertmanager; ClickHouse-based ones (log events, joins, cost) are Grafana
alerts: Grafana → Alerting → folder observe-d.

## Kernel / journald logs on real nodes

The agent ships pod logs filtered to fatal GPU lines (NVRM Xid, amdgpu RAS/reset), NCCL/RCCL errors and OOMs.
Kernel messages on real nodes live in journald: add a `journald` receiver with the same filter expression
(needs a collector image that contains `journalctl`, or run a small journald-to-OTLP forwarder per node).

## Kafka

Single-node KRaft. If the broker logs `Unable to send a heartbeat` repeatedly, it is fenced and accepts
nothing even though the process is up. The health check (`kafka-metadata-quorum.sh describe --status`)
catches this; locally it was caused by a too-small heap under CPU pressure (fixed at 768 MB).

## Changing cost / SLO numbers

```sql
INSERT INTO otel.cost_gpu (gpu_model, usd_per_gpu_hour) VALUES ('AMD Instinct MI300X', 1.95);
INSERT INTO otel.slo_targets (model, ttft_ms, tpot_ms, max_error_rate) VALUES ('qwen2.5-7b-instruct', 400, 25, 0.01);
```
Dictionaries reload within 60 s; tokenomics recomputes the current and previous hour every 5 minutes.

## Upgrading the OTel collector

The exporter schema is pinned in `clickhouse/migrations/0001_otel.sql`. Before bumping the collector tag in
`versions.yaml`, run it once with `create_schema: true` against a scratch database, diff the DDL, and add a
migration for any new columns.
