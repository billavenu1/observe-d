# observe-d

Observability for LLM inference on GPU clusters (8×H100 / 8×MI300X): GPU → Kubernetes → network → llm-d →
vLLM → tokenomics → AI/LLM traceability. OTel Collector → Kafka → ClickHouse, with Prometheus + Grafana.

| Doc | What it is |
|---|---|
| [MVP.md](MVP.md) | What is being built now (single GPU node, both vendors, 5k/7k req/s) |
| [PLAN.md](PLAN.md) | Target architecture |
| [all_telemetry.md](all_telemetry.md) | Full telemetry spec, layer by layer |
| [docs/header-contract.md](docs/header-contract.md) | Headers the gateway must carry |
| [docs/telemetry-shapes.md](docs/telemetry-shapes.md) | What each component must emit; verified vs modeled names |
| [docs/status.md](docs/status.md) | What is built and verified, what remains |
| [docs/runbook.md](docs/runbook.md) | Operating the stack, debugging the pipeline, alert first actions |
| [docs/deploy.md](docs/deploy.md) | Deploying on a real GPU cluster (M8 / M9) |
| [docs/capacity.md](docs/capacity.md) | Load-test method and measured results |
| [versions.yaml](versions.yaml) | Every pinned version |

## What's in v1

```
kind "GPU node" (fake 8×MI300X or 8×H100)                    podman-compose backend
 ├─ fake GPU exporter (DCGM / AMD format, follows engine load)  ├─ Kafka (KRaft)  otlp_{traces,logs,metrics}
 ├─ simulated llm-d: gateway + EPP → vLLM engines → loadgen     ├─ OTel gateway collector → ClickHouse
 ├─ OTel agent (DaemonSet) + cluster collector (k8s events) ───▶└─ ClickHouse: raw OTel tables, gpu_samples,
 └─ kube-prometheus-stack (Prometheus, Grafana, KSM, alerts)       engine/routing/gateway staging, llm_requests,
                                                                    rollups, tokenomics, k8s_events
```

| Layer | Collected | Dashboard |
|---|---|---|
| GPU | 21 vendor-neutral metrics (normalize/gpu.yaml), GPU→pod mapping, inventory | 01 GPU |
| Kubernetes | GPU allocation, pod health, CPU throttling, events, startup phases | 02 Kubernetes |
| Network | NVLink/xGMI, PCIe, gateway traffic/latency, hop latency, NCCL/RCCL errors | 03 Network |
| llm-d | pool KV/queue, routing decisions, predicted vs actual prefix hits, skew | 04 llm-d |
| vLLM | TTFT/ITL/E2E/queue, KV cache, preemptions, tokens, finish reasons, SLO | 05 vLLM |
| Tokenomics | cost/1M tokens, cost by org/app, J/token, idle GPU cost, cache savings, goodput | 06 Tokenomics |
| Traceability | every request joined gateway ⋈ EPP ⋈ engine; drill-down to GPU + k8s state | 07 Request explorer |

Alerts: 21 Prometheus rules in `prometheus/rules/alerts.yaml` (GPU, k8s, gateway/llm-d, engine) and 5 Grafana
rules on ClickHouse in `grafana/alerts/` (GPU fatal events from kernel logs incl. AMD RAS, NCCL/RCCL errors,
broken trace propagation, goodput, org cost spikes).

## Quick start (Windows/macOS/Linux)

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), **Podman** (rootful machine, ≥ 8 GB) and
podman-compose. kind / kubectl / helm live in the git-ignored `.tools/bin` (kind v0.33.0, kubectl v1.37.1,
helm v4.3.0).

```bash
uv sync
```

```bash
uv run python -m tools.dev up --vendor amd
```

Then open Grafana at http://localhost:3300 (admin/admin) → folder **observe-d**. Data appears within ~2 minutes;
tokenomics within ~5 minutes. Fault injection and other commands: [docs/runbook.md](docs/runbook.md).

## Tests

```bash
uv run pytest -m "not sql and not e2e"
```

```bash
uv run pytest -m sql
```

```bash
uv run pytest -m e2e
```

`sql` needs ClickHouse on localhost:8123 (the compose one) and runs in a throwaway database; `e2e` needs the
whole stack and checks that every dashboard panel query returns data. Load / resilience tests:

```bash
uv run python -m tools.loadtest --rate 500 --duration 900 --burst-rate 700 --burst-at 300 --burst-duration 300
```

```bash
uv run python -m tools.loadtest --rate 200 --duration 600 --stop clickhouse --stop-at 120 --stop-for 180
```

Docker instead of Podman: set `CONTAINER_CLI=docker` (used by the `e2e` GitHub workflow).

## Repository map

| Path | Purpose |
|---|---|
| `normalize/` | vendor/engine → normalized metric mappings + generator (Prometheus rules, collector transforms) |
| `collector/` | OTel agent, cluster and gateway collector configs (`generated/` is generated) |
| `clickhouse/migrations/` | schema, MVs, refreshable views, cost dictionaries (`python -m tools.migrate`) |
| `prometheus/rules/` | normalized recording rules (generated), k8s GPU rules, alerts |
| `grafana/build.py` | dashboards as code → `grafana/dashboards/*.json` |
| `sim/` | fake GPU exporter, simulated llm-d stack (gateway/EPP, vLLM engine, loadgen) |
| `local/`, `deploy/helm/` | compose backend, kind config + manifests, kube-prometheus-stack values |
| `tools/` | `dev.py` (local orchestration), `migrate.py`, `loadtest.py`, `render.py` (real-cluster manifests) |
| `deploy/backend/` | Kafka (Strimzi) and ClickHouse (Altinity operator) for a real cluster |
