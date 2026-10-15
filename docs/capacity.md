# Capacity & resilience

## Method

`tools/loadtest.py` runs `sim/telemetry_replay.py` as an indexed Job inside the cluster (one pod per ~400 req/s).
Each simulated request produces the real per-request record set: gateway span, EPP routing span, vLLM span and
gateway access log, with consistent IDs and run-tagged `org_id`. That is roughly 4 records / ~4 KB per request
(MVP.md §2.1). The whole path is exercised:

agent collector → Kafka → gateway collector → ClickHouse → refreshable request join

After the generator stops, the tool waits for the pipeline to drain and checks:

| Check | Pass condition |
|---|---|
| rows | `llm_requests` rows for the run = requests sent, ±0.1% |
| join | ≥ 99% of rows have engine + routing spans |
| drain | every request in ClickHouse; drain time recorded |
| drops | collector refused / failed / dropped counters unchanged (agent via Prometheus, gateway via :8888) |
| parts | max active parts in any partition < 300 |

Reports: `local/.state/loadtest-<run>.json`.

## Local results (laptop: 8 GB WSL VM, 8 vCPU shared with kind + simulators)

These runs prove the pipeline logic and tooling, not production capacity. Real capacity numbers come from M8,
on the MI300X cluster's CPU nodes.

| Run | Load | Sent | Rows | Join | Drain | Drops | Max parts | Result |
|---|---|---|---|---|---|---|---|---|
| calibration | 200 req/s × 5 min | 56,480 | 56,480 | 100% | 28 s | 0 | 7 | pass |

## M8 (to fill in on the MI300X cluster)

| Run | Load | Sent | Rows | Join | Drain | Kafka lag (max) | Drops | Max parts | First component to saturate |
|---|---|---|---|---|---|---|---|---|---|
| sustained | 5,000 req/s × 60 min | | | | | | | | |
| burst | 7,000 req/s × 15 min | | | | | | | | |
| step | 8k → 10k → 12k | | | | | | | | |
