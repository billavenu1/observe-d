"""Grafana dashboards as code -> grafana/dashboards/*.json.

    python -m grafana.build

Datasource UIDs: `prometheus` (kube-prometheus-stack default) and `clickhouse` (additionalDataSources).
Every panel query is also exercised by tests/e2e (each must return data on a running stack).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parent / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
CH = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
CH_FORMAT = {"timeseries": 0, "table": 1, "logs": 2}

# ----------------------------------------------------------------------------------------- helpers


def prom(expr: str, legend: str = "", instant: bool = False) -> dict[str, Any]:
    return {
        "datasource": PROM,
        "expr": expr,
        "legendFormat": legend,
        "instant": instant,
        "range": not instant,
    }


def ch(sql: str, kind: str = "timeseries") -> dict[str, Any]:
    return {
        "datasource": CH,
        "editorType": "sql",
        "queryType": kind,
        "format": CH_FORMAT[kind],
        "rawSql": " ".join(sql.split()),
    }


@dataclass
class Board:
    uid: str
    title: str
    description: str
    tags: list[str] = field(default_factory=list)
    panels: list[dict[str, Any]] = field(default_factory=list)
    variables: list[dict[str, Any]] = field(default_factory=list)
    time_from: str = "now-1h"
    _x: int = 0
    _y: int = 0
    _row_h: int = 0

    def _place(self, w: int, h: int) -> dict[str, int]:
        if self._x + w > 24:
            self._x, self._y = 0, self._y + self._row_h
            self._row_h = 0
        pos = {"x": self._x, "y": self._y, "w": w, "h": h}
        self._x += w
        self._row_h = max(self._row_h, h)
        return pos

    def row(self, title: str) -> None:
        if self._x:
            self._x, self._y, self._row_h = 0, self._y + self._row_h, 0
        self.panels.append({"type": "row", "title": title, "collapsed": False, "gridPos": self._place(24, 1)})
        self._x, self._y, self._row_h = 0, self._y + 1, 0

    def add(
        self,
        kind: str,
        title: str,
        targets: list[dict[str, Any]],
        *,
        w: int = 12,
        h: int = 8,
        unit: str = "",
        desc: str = "",
        options: dict[str, Any] | None = None,
        overrides: list[dict[str, Any]] | None = None,
        thresholds: list[tuple[str, float | None]] | None = None,
        decimals: int | None = None,
        bars: bool = False,
    ) -> None:
        defaults: dict[str, Any] = {"unit": unit} if unit else {}
        if bars:  # hourly series: a bar per bucket reads better than sparse points
            defaults["custom"] = {"drawStyle": "bars", "fillOpacity": 80, "stacking": {"mode": "normal"}}
        if decimals is not None:
            defaults["decimals"] = decimals
        if thresholds:
            defaults["thresholds"] = {
                "mode": "absolute",
                "steps": [{"color": c, "value": v} for c, v in thresholds],
            }
        for i, t in enumerate(targets):
            t.setdefault("refId", chr(65 + i))
        ds = targets[0]["datasource"] if targets else PROM
        self.panels.append(
            {
                "type": kind,
                "title": title,
                "description": desc,
                "datasource": ds,
                "gridPos": self._place(w, h),
                "targets": targets,
                "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
                "options": options or {},
            }
        )

    def ts(self, title: str, *targets: dict[str, Any], **kw: Any) -> None:
        self.add("timeseries", title, list(targets), **kw)

    def stat(self, title: str, target: dict[str, Any], **kw: Any) -> None:
        kw.setdefault("w", 4)
        kw.setdefault("h", 4)
        kw.setdefault("options", {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"})
        self.add("stat", title, [target], **kw)

    def table(self, title: str, target: dict[str, Any], **kw: Any) -> None:
        kw.setdefault("w", 24)
        kw.setdefault("options", {"showHeader": True})
        self.add("table", title, [target], **kw)

    def logs(self, title: str, target: dict[str, Any], **kw: Any) -> None:
        kw.setdefault("w", 24)
        kw.setdefault("options", {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"})
        self.add("logs", title, [target], **kw)

    def ch_var(self, name: str, label: str, sql: str, include_all: bool = True) -> None:
        self.variables.append(
            {
                "name": name,
                "label": label,
                "type": "query",
                "datasource": CH,
                "query": " ".join(sql.split()),
                "definition": " ".join(sql.split()),
                "refresh": 2,
                "includeAll": include_all,
                "multi": include_all,
                "allValue": "",
                "current": {"text": "All", "value": "$__all"} if include_all else {},
            }
        )

    def text_var(self, name: str, label: str, default: str = "") -> None:
        self.variables.append(
            {
                "name": name,
                "label": label,
                "type": "textbox",
                "query": default,
                "current": {"text": default, "value": default},
            }
        )

    def json(self) -> dict[str, Any]:
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": ["observe-d", *self.tags],
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "editable": True,
            "refresh": "30s",
            "time": {"from": self.time_from, "to": "now"},
            "templating": {"list": self.variables},
            "links": [{"title": "observe-d", "type": "dashboards", "tags": ["observe-d"], "asDropdown": True}],
            "panels": self.panels,
        }


# Multi-value ClickHouse variable filter: empty selection or "All" -> no filter.
def in_var(col: str, var: str) -> str:
    return f"(length([${{{var}:singlequote}}]) = 0 OR {col} IN (${{{var}:singlequote}}))"


Q = "quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)"
SLO_TTFT = "1000 * dictGet('dict_slo', 'ttft_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))"


# ---------------------------------------------------------------------------------------- overview


def overview() -> Board:
    b = Board(
        "od-overview",
        "observe-d / 00 Overview",
        "One screen for the whole vertical: GPU → k8s → network → llm-d → vLLM → tokenomics → traces.",
        ["overview"],
    )
    b.row("Inference health")
    b.stat("Requests / s", prom("sum(rate(inference_objective_request_total[2m]))"), unit="reqps", decimals=1)
    b.stat(
        "Error rate",
        prom(
            'sum(rate(inference_objective_request_error_total{error_code=~"5.."}[5m])) '
            "/ clamp_min(sum(rate(inference_objective_request_total[5m])), 1e-9)"
        ),
        unit="percentunit",
        thresholds=[("green", None), ("orange", 0.005), ("red", 0.01)],
    )
    b.stat(
        "TTFT p95",
        prom("histogram_quantile(0.95, sum by (le) (rate(llm:ttft_seconds_bucket[5m])))"),
        unit="s",
        thresholds=[("green", None), ("orange", 0.5), ("red", 1)],
    )
    b.stat(
        "ITL p95",
        prom("histogram_quantile(0.95, sum by (le) (rate(llm:itl_seconds_bucket[5m])))"),
        unit="s",
        thresholds=[("green", None), ("orange", 0.05), ("red", 0.1)],
    )
    b.stat("Output tokens / s", prom("sum(rate(llm:generation_tokens_total[2m]))"), decimals=0)
    b.stat(
        "Goodput (SLO met)",
        ch(
            "SELECT sum(slo_good) / greatest(sum(requests), 1) FROM llm_rollup_1m FINAL WHERE $__timeFilter(minute)",
            "table",
        ),
        unit="percentunit",
        thresholds=[("red", None), ("orange", 0.95), ("green", 0.99)],
    )
    b.row("GPU & cost")
    b.stat("GPU compute active (avg)", prom("avg(gpu:compute_active_percent)"), unit="percent", decimals=1)
    b.stat("GPU power", prom("sum(gpu:power_watts)"), unit="watt", decimals=0)
    b.stat("GPUs allocated", prom("sum(k8s:gpu_requested)"), decimals=0)
    b.stat(
        "GPU fatal / ECC events",
        prom("count(gpu:last_fatal_event_code > 0) or vector(0) + sum(gpu:ecc_uncorrectable_total) or vector(0)"),
        thresholds=[("green", None), ("red", 1)],
    )
    b.stat(
        "Infra cost / 1M output tokens",
        ch(
            "SELECT sum(cost_infra_usd) / greatest(sum(tokens_out), 1) * 1e6 FROM tokenomics_hourly FINAL "
            "WHERE $__timeFilter(hour) AND org_id NOT IN ('(idle)', '(unallocated)')",
            "table",
        ),
        unit="currencyUSD",
        decimals=3,
    )
    b.stat(
        "Energy / output token",
        ch(
            "SELECT sum(energy_j) / greatest(sum(tokens_out), 1) FROM tokenomics_hourly FINAL "
            "WHERE $__timeFilter(hour) AND org_id NOT IN ('(idle)', '(unallocated)')",
            "table",
        ),
        unit="J",
        decimals=2,
    )
    b.row("Traffic")
    b.ts(
        "Requests / s by model",
        prom("sum by (model_name) (rate(inference_objective_request_total[2m]))", "{{model_name}}"),
        unit="reqps",
    )
    b.ts(
        "TTFT p95 by model",
        prom("histogram_quantile(0.95, sum by (le, model) (rate(llm:ttft_seconds_bucket[5m])))", "{{model}}"),
        unit="s",
    )
    b.ts(
        "Firing alerts",
        prom('sum by (alertname) (ALERTS{alertstate="firing", observe_d="true"})', "{{alertname}}"),
        w=24,
        h=6,
    )
    return b


# --------------------------------------------------------------------------------------------- GPU


def gpu() -> Board:
    b = Board(
        "od-gpu",
        "observe-d / 01 GPU",
        "8×H100 / 8×MI300X, vendor-neutral gpu:* metrics (normalize/gpu.yaml). 'n/a' = vendor has no equivalent.",
        ["gpu"],
    )
    b.row("Utilization (use compute/tensor/HBM activity, not 'busy')")
    for metric, title in (
        ("compute_active_percent", "Compute active (SM / GFX)"),
        ("matrix_active_percent", "Tensor / matrix active"),
        ("mem_bw_active_percent", "HBM bandwidth active"),
        ("busy_percent", "Busy (any kernel running)"),
    ):
        b.ts(title, prom(f"gpu:{metric}", "GPU {{gpu_index}} {{pod}}"), unit="percent", w=12)
    b.row("Memory, power, thermals")
    b.ts(
        "HBM used / total",
        prom("gpu:mem_used_bytes / gpu:mem_total_bytes", "GPU {{gpu_index}}"),
        unit="percentunit",
    )
    b.ts("Power", prom("gpu:power_watts", "GPU {{gpu_index}}"), unit="watt")
    b.ts(
        "Energy rate (from counter)",
        prom("rate(gpu:energy_joules_total[2m])", "GPU {{gpu_index}}"),
        unit="watt",
    )
    b.ts(
        "Core / HBM temperature",
        prom("gpu:temp_celsius", "core {{gpu_index}}"),
        prom("gpu:mem_temp_celsius", "hbm {{gpu_index}}"),
        unit="celsius",
    )
    b.ts("Compute clock", prom("gpu:sm_clock_mhz", "GPU {{gpu_index}}"), unit="hertz", options={}, desc="MHz")
    b.ts("Throttling by reason (NVIDIA)", prom("gpu:throttle_active == 1", "GPU {{gpu_index}} {{reason}}"))
    b.row("Reliability")
    b.ts("Uncorrectable ECC (total)", prom("gpu:ecc_uncorrectable_total", "GPU {{gpu_index}}"), w=8)
    b.ts("Correctable ECC rate", prom("rate(gpu:ecc_correctable_total[5m])", "GPU {{gpu_index}}"), w=8)
    b.ts("Last fatal event (Xid)", prom("gpu:last_fatal_event_code", "GPU {{gpu_index}}"), w=8)
    b.row("Interconnect")
    b.ts(
        "NVLink / xGMI TX+RX",
        prom("gpu:link_tx_bytes_per_second + gpu:link_rx_bytes_per_second", "GPU {{gpu_index}}"),
        unit="Bps",
    )
    b.ts(
        "PCIe TX / RX",
        prom("gpu:pcie_tx_bytes_per_second", "tx {{gpu_index}}"),
        prom("gpu:pcie_rx_bytes_per_second", "rx {{gpu_index}}"),
        unit="Bps",
        desc="NVIDIA only (DCGM PCIe throughput); the AMD exporter has no equivalent (shows no data).",
    )
    b.row("Placement & inventory (ClickHouse)")
    b.table(
        "GPU → pod → model (latest)",
        ch(
            """
        SELECT s.gpu_index AS gpu, s.vendor, s.gpu_model, s.node, s.pod, d.model, d.tp,
               round(s.compute_active_percent, 1) AS compute_pct, round(s.power_watts) AS watts,
               round(s.temp_celsius, 1) AS temp_c, s.throttle_reasons
        FROM (SELECT * FROM gpu_samples_v WHERE ts > now() - INTERVAL 2 MINUTE
              ORDER BY ts DESC LIMIT 1 BY gpu_uuid) AS s
        LEFT JOIN (SELECT * FROM model_deployments FINAL) AS d ON d.pod = s.pod
        ORDER BY toUInt32OrZero(gpu)""",
            "table",
        ),
        h=9,
    )
    b.table(
        "Inventory",
        ch(
            """
        SELECT gpu_uuid, vendor, gpu_model, node, gpu_index, formatReadableSize(mem_total_bytes) AS hbm,
               min(first_seen) AS first_seen, max(last_seen) AS last_seen
        FROM gpu_inventory GROUP BY gpu_uuid, vendor, gpu_model, node, gpu_index, mem_total_bytes
        ORDER BY node, toUInt32OrZero(gpu_index)""",
            "table",
        ),
        h=7,
    )
    return b


# ---------------------------------------------------------------------------------------------- k8s


def k8s() -> Board:
    b = Board("od-k8s", "observe-d / 02 Kubernetes", "GPU allocation, pod health, events.", ["k8s"])
    b.row("GPU allocation")
    b.stat("Allocatable GPUs", prom("sum(k8s:gpu_allocatable)"), w=6)
    b.stat("Requested GPUs", prom("sum(k8s:gpu_requested)"), w=6)
    b.stat(
        "Free GPUs",
        prom("sum(k8s:gpu_allocatable) - sum(k8s:gpu_requested) or sum(k8s:gpu_allocatable)"),
        w=6,
    )
    b.stat(
        "Pending pods",
        prom('sum(kube_pod_status_phase{phase="Pending"})'),
        w=6,
        thresholds=[("green", None), ("red", 1)],
    )
    b.ts("GPUs requested by pod", prom("sum by (namespace, pod) (k8s:gpu_requested_by_pod)", "{{pod}}"))
    b.ts("Pods by phase", prom("sum by (phase) (kube_pod_status_phase)", "{{phase}}"))
    b.row("Pod health (inference namespace)")
    b.ts(
        "Container restarts",
        prom('sum by (pod) (kube_pod_container_status_restarts_total{namespace="llm"})', "{{pod}}"),
    )
    b.ts(
        "OOMKilled (last termination)",
        prom('sum by (pod) (kube_pod_container_status_last_terminated_reason{reason="OOMKilled"})', "{{pod}}"),
    )
    b.ts(
        "CPU throttling (tokenizer / scheduler starvation)",
        prom(
            'sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{namespace="llm"}[5m])) / '
            'clamp_min(sum by (pod) (rate(container_cpu_cfs_periods_total{namespace="llm"}[5m])), 1e-9)',
            "{{pod}}",
        ),
        desc="Only populated for containers with a CPU limit (CFS quota).",
        unit="percentunit",
    )
    b.ts(
        "Memory working set vs limit",
        prom(
            'sum by (pod) (container_memory_working_set_bytes{namespace="llm", container!=""}) / '
            'sum by (pod) (kube_pod_container_resource_limits{namespace="llm", resource="memory"})',
            "{{pod}}",
        ),
        unit="percentunit",
    )
    b.row("Events (ClickHouse)")
    b.table(
        "Kubernetes events",
        ch(
            """
        SELECT ts, type, reason, kind, namespace, name, message, count
        FROM k8s_events WHERE $__timeFilter(ts) ORDER BY ts DESC LIMIT 200""",
            "table",
        ),
        h=10,
    )
    b.table(
        "Model pod startup phases",
        ch(
            """
        SELECT pod, namespace, scheduled, pulled, started, engine_ready,
               dateDiff('second', scheduled, engine_ready) AS time_to_ready_s
        FROM pod_startups WHERE scheduled > now() - INTERVAL 7 DAY ORDER BY scheduled DESC LIMIT 50""",
            "table",
        ),
        h=8,
    )
    return b


# ------------------------------------------------------------------------------------------ network


def network() -> Board:
    b = Board(
        "od-network",
        "observe-d / 03 Network",
        "Single-node scope: GPU fabric (NVLink/xGMI), PCIe, collectives, gateway and hop latency.",
        ["network"],
    )
    b.row("GPU fabric")
    b.ts(
        "NVLink / xGMI throughput per GPU",
        prom("gpu:link_tx_bytes_per_second + gpu:link_rx_bytes_per_second", "GPU {{gpu_index}}"),
        unit="Bps",
    )
    b.ts("PCIe replays (link quality)", prom("increase(gpu:pcie_replay_total[5m])", "GPU {{gpu_index}}"))
    b.row("Gateway (north-south)")
    b.ts(
        "Requests / s by status",
        prom("sum by (error_code) (rate(inference_objective_request_error_total[2m]))", "{{error_code}}"),
        prom("sum(rate(inference_objective_request_total[2m]))", "all"),
        unit="reqps",
    )
    b.ts(
        "Gateway latency p50 / p95 / p99",
        *(
            prom(
                f"histogram_quantile({q}, sum by (le) (rate(inference_objective_request_duration_seconds_bucket[5m])))",
                f"p{int(q * 100)}",
            )
            for q in (0.5, 0.95, 0.99)
        ),
        unit="s",
    )
    b.ts(
        "Node network",
        prom(
            'sum by (device) (rate(node_network_receive_bytes_total{device!~"lo|veth.*"}[2m]))',
            "rx {{device}}",
        ),
        prom(
            'sum by (device) (rate(node_network_transmit_bytes_total{device!~"lo|veth.*"}[2m]))',
            "tx {{device}}",
        ),
        unit="Bps",
    )
    b.ts(
        "Hop latency p95: gateway total vs engine e2e vs routing",
        ch(
            """
        SELECT minute AS time,
               arrayElement("""
            + Q
            + """(gateway_q), 3) / 1e6 AS gateway,
               arrayElement("""
            + Q
            + """(e2e_q), 3) / 1e6 AS engine_e2e
        FROM llm_rollup_1m FINAL WHERE $__timeFilter(minute) GROUP BY minute ORDER BY minute"""
        ),
        unit="s",
    )
    b.row("Collectives (NCCL / RCCL)")
    b.logs(
        "NCCL / RCCL errors",
        ch(
            """
        SELECT Timestamp AS timestamp, Body AS body, SeverityText AS level, K8sPod AS pod
        FROM otel_logs WHERE $__timeFilter(Timestamp)
          AND (Body ILIKE '%NCCL WARN%' OR Body ILIKE '%RCCL WARN%' OR Body ILIKE '%NCCL error%')
        ORDER BY Timestamp DESC LIMIT 200""",
            "logs",
        ),
        h=8,
    )
    return b


# -------------------------------------------------------------------------------------------- llm-d


def llmd() -> Board:
    b = Board(
        "od-llm-d",
        "observe-d / 04 llm-d routing",
        "Inference pool health and EPP routing effectiveness (prefix-cache affinity, load balance).",
        ["llm-d"],
    )
    b.row("Pool")
    b.ts(
        "Pool avg KV-cache utilization",
        prom("inference_pool_average_kv_cache_utilization", "{{name}}"),
        unit="percentunit",
        w=8,
    )
    b.ts("Pool avg queue size", prom("inference_pool_average_queue_size", "{{name}}"), w=8)
    b.ts("Ready pods", prom("inference_pool_ready_pods", "{{name}}"), w=8)
    b.row("Routing")
    b.ts(
        "Picks per pod",
        prom("sum by (pod) (rate(llm_d_epp_routing_picks_total[2m]))", "{{pod}}"),
        unit="reqps",
    )
    b.ts(
        "Queue skew (max − min waiting across pods, per model)",
        prom("max by (model) (llm:requests_waiting) - min by (model) (llm:requests_waiting)", "{{model}}"),
    )
    b.ts(
        "EPP decision latency p99",
        prom(
            "histogram_quantile(0.99, sum by (le) (rate(llm_d_epp_routing_decision_duration_seconds_bucket[5m])))",
            "p99",
        ),
        unit="s",
    )
    b.ts(
        "No endpoint available",
        prom("sum(rate(llm_d_epp_no_endpoint_available_total[5m]))", "rate"),
        unit="reqps",
    )
    b.row("Prefix-cache routing effectiveness (ClickHouse)")
    b.ts(
        "Predicted vs actual prefix hit rate",
        ch("""
        SELECT minute AS time,
               sum(prefix_hit_predicted) / greatest(sum(requests), 1) AS predicted,
               sum(prefix_hit_actual) / greatest(sum(requests), 1) AS actual,
               sum(prefix_hit_predicted_and_actual) / greatest(sum(prefix_hit_predicted), 1) AS prediction_precision
        FROM llm_rollup_1m FINAL WHERE $__timeFilter(minute) GROUP BY minute ORDER BY minute"""),
        unit="percentunit",
    )
    b.ts(
        "Cached-token share by pod",
        ch("""
        SELECT minute AS time, pod, sum(tokens_cached) / greatest(sum(tokens_in), 1) AS cached_share
        FROM llm_rollup_1m FINAL WHERE $__timeFilter(minute) AND pod != ''
        GROUP BY minute, pod ORDER BY minute"""),
        unit="percentunit",
    )
    b.table(
        "Routing fallbacks (last hour)",
        ch(
            """
        SELECT model, chosen_pod, fallback_reason, count() AS decisions,
               round(avg(score_queue), 3) AS avg_queue_score, round(avg(score_kv), 3) AS avg_kv_score
        FROM routing_decisions WHERE ts > now() - INTERVAL 1 HOUR
        GROUP BY model, chosen_pod, fallback_reason ORDER BY decisions DESC""",
            "table",
        ),
        h=7,
    )
    return b


# --------------------------------------------------------------------------------------------- vLLM


def vllm() -> Board:
    b = Board(
        "od-vllm",
        "observe-d / 05 vLLM engine",
        "Normalized llm:* engine metrics (normalize/engine_vllm.yaml); SGLang plugs into the same names.",
        ["vllm"],
    )
    b.row("SLIs")
    for metric, title in (
        ("ttft_seconds", "TTFT"),
        ("itl_seconds", "Inter-token latency"),
        ("e2e_seconds", "E2E (engine)"),
        ("queue_seconds", "Queue time"),
    ):
        b.ts(
            f"{title} p50 / p95 / p99",
            *(
                prom(
                    f"histogram_quantile({q}, sum by (le, model) (rate(llm:{metric}_bucket[5m])))",
                    f"p{int(q * 100)} {{{{model}}}}",
                )
                for q in (0.5, 0.95, 0.99)
            ),
            unit="s",
        )
    b.ts(
        "SLO attainment by model (ClickHouse)",
        ch("""
        SELECT minute AS time, model, sum(slo_good) / greatest(sum(requests), 1) AS slo_met
        FROM llm_rollup_1m FINAL WHERE $__timeFilter(minute) AND model != ''
        GROUP BY minute, model ORDER BY minute"""),
        unit="percentunit",
        w=24,
    )
    b.row("Scheduler & KV cache")
    b.ts(
        "Running / waiting",
        prom("llm:requests_running", "running {{pod}}"),
        prom("llm:requests_waiting", "waiting {{pod}}"),
    )
    b.ts("KV-cache usage", prom("llm:kv_cache_usage_percent", "{{pod}}"), unit="percent")
    b.ts(
        "Prefix-cache hit rate",
        prom(
            "rate(llm:prefix_cache_hits_total[5m]) / clamp_min(rate(llm:prefix_cache_queries_total[5m]), 1e-9)",
            "{{pod}}",
        ),
        unit="percentunit",
    )
    b.ts("Preemptions / s", prom("rate(llm:preemptions_total[5m])", "{{pod}}"))
    b.row("Throughput & outcomes")
    b.ts(
        "Tokens / s",
        prom("sum by (model) (rate(llm:generation_tokens_total[2m]))", "output {{model}}"),
        prom("sum by (model) (rate(llm:prompt_tokens_total[2m]))", "prompt {{model}}"),
    )
    b.ts(
        "Finished requests by reason",
        prom("sum by (finished_reason) (rate(llm:requests_finished_total[2m]))", "{{finished_reason}}"),
        unit="reqps",
    )
    b.table(
        "Engines",
        ch(
            """
        SELECT pod, node, model, engine, engine_version, tp, max_num_seqs, dtype, vendor, started_at
        FROM model_deployments FINAL ORDER BY model, pod""",
            "table",
        ),
        h=6,
    )
    return b


# --------------------------------------------------------------------------------------- tokenomics


def tokenomics() -> Board:
    b = Board(
        "od-tokenomics",
        "observe-d / 06 Tokenomics",
        "Owned-GPU cost and energy attributed by token share (tokenomics_hourly). Prices: cost_gpu, "
        "cost_model_price tables.",
        ["tokenomics"],
        time_from="now-24h",
    )
    real = "org_id NOT IN ('(idle)', '(unallocated)')"
    b.row("Headline")
    b.stat(
        "Infra cost (range)",
        ch("SELECT sum(cost_infra_usd) FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour)", "table"),
        unit="currencyUSD",
        w=4,
    )
    b.stat(
        "Cost / 1M output tokens",
        ch(
            f"SELECT sum(cost_infra_usd) / greatest(sum(tokens_out), 1) * 1e6 "
            f"FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND {real}",
            "table",
        ),
        unit="currencyUSD",
        decimals=3,
        w=4,
    )
    b.stat(
        "Energy / output token",
        ch(
            f"SELECT sum(energy_j) / greatest(sum(tokens_out), 1) FROM "
            f"tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND {real}",
            "table",
        ),
        unit="J",
        decimals=2,
        w=4,
    )
    b.stat(
        "Idle + unallocated GPU cost",
        ch(
            f"SELECT sum(cost_infra_usd) FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND NOT ({real})",
            "table",
        ),
        unit="currencyUSD",
        w=4,
    )
    b.stat(
        "Prefix-cache savings",
        ch("SELECT sum(cache_savings_usd) FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour)", "table"),
        unit="currencyUSD",
        w=4,
    )
    b.stat(
        "Goodput",
        ch(
            f"SELECT sum(slo_good) / greatest(sum(requests), 1) FROM tokenomics_hourly FINAL "
            f"WHERE $__timeFilter(hour) AND {real}",
            "table",
        ),
        unit="percentunit",
        w=4,
    )
    b.row("Attribution")
    b.ts(
        "Infra cost / hour by org",
        ch("""
        SELECT hour AS time, org_id, sum(cost_infra_usd) AS cost FROM tokenomics_hourly FINAL
        WHERE $__timeFilter(hour) GROUP BY hour, org_id ORDER BY hour"""),
        unit="currencyUSD",
        bars=True,
    )
    b.ts(
        "Cost / 1M output tokens by model",
        ch(f"""
        SELECT hour AS time, model, sum(cost_infra_usd) / greatest(sum(tokens_out), 1) * 1e6 AS usd_per_1m
        FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND {real} GROUP BY hour, model ORDER BY hour"""),
        unit="currencyUSD",
        bars=True,
    )
    b.table(
        "By org / app / model",
        ch(
            """
        SELECT org_id, app_id, model, sum(requests) AS reqs, sum(tokens_in) AS in_tokens,
               sum(tokens_cached) AS cached, sum(tokens_out) AS out_tokens,
               round(sum(gpu_seconds) / 3600, 3) AS gpu_hours, round(sum(energy_j) / 3.6e6, 4) AS kwh,
               round(sum(cost_infra_usd), 4) AS infra_usd, round(sum(cost_list_usd), 4) AS list_price_usd,
               round(sum(cost_infra_usd) / greatest(sum(tokens_out), 1) * 1e6, 3) AS usd_per_1m_out,
               round(sum(slo_good) / greatest(sum(requests), 1), 4) AS goodput
        FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour)
        GROUP BY org_id, app_id, model ORDER BY infra_usd DESC""",
            "table",
        ),
        h=9,
    )
    b.row("Efficiency & waste")
    b.ts(
        "Output tokens per GPU-second by model",
        ch(f"""
        SELECT hour AS time, model, sum(tokens_out) / greatest(sum(gpu_seconds), 1) AS tokens_per_gpu_s
        FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND {real} GROUP BY hour, model ORDER BY hour"""),
    )
    b.ts(
        "Waste: aborted output tokens & preemptions",
        ch(f"""
        SELECT hour AS time, sum(tokens_out_aborted) AS aborted_tokens, sum(preempted) AS preemptions
        FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND {real} GROUP BY hour ORDER BY hour"""),
    )
    b.table(
        "Vendor comparison (same model)",
        ch(
            f"""
        SELECT model, vendor, gpu_model, sum(tokens_out) AS out_tokens,
               round(sum(cost_infra_usd) / greatest(sum(tokens_out), 1) * 1e6, 3) AS usd_per_1m_out,
               round(sum(energy_j) / greatest(sum(tokens_out), 1), 3) AS joules_per_token,
               round(sum(tokens_out) / greatest(sum(gpu_seconds), 1), 1) AS tokens_per_gpu_s
        FROM tokenomics_hourly FINAL WHERE $__timeFilter(hour) AND {real}
        GROUP BY model, vendor, gpu_model ORDER BY model, vendor""",
            "table",
        ),
        h=6,
    )
    return b


# ------------------------------------------------------------------------------------ traceability


def explorer() -> Board:
    b = Board(
        "od-requests",
        "observe-d / 07 Request explorer",
        "Every request (llm_requests). Paste a trace_id / request_id for the drill-down.",
        ["traceability"],
    )
    b.ch_var("org", "Org", "SELECT DISTINCT org_id FROM llm_requests WHERE ts > now() - INTERVAL 1 DAY")
    b.ch_var(
        "model",
        "Model",
        "SELECT DISTINCT model FROM llm_requests WHERE ts > now() - INTERVAL 1 DAY AND model != ''",
    )
    b.ch_var("status", "Status", "SELECT DISTINCT status FROM llm_requests WHERE ts > now() - INTERVAL 1 DAY")
    b.text_var("trace", "trace_id / request_id")
    flt = f"{in_var('org_id', 'org')} AND {in_var('model', 'model')} AND {in_var('status', 'status')}"
    b.row("Requests")
    b.ts(
        "Requests by status",
        ch(f"""
        SELECT toStartOfInterval(ts, INTERVAL $__interval_s SECOND) AS time, status, count() AS requests
        FROM llm_requests FINAL WHERE $__timeFilter(ts) AND {flt} GROUP BY time, status ORDER BY time"""),
    )
    b.ts(
        "Join health (missing spans = broken propagation)",
        ch("""
        SELECT minute AS time, missing_engine_span / greatest(routed, 1) AS missing_engine_span_ratio
        FROM join_health WHERE $__timeFilter(minute) ORDER BY minute"""),
        unit="percentunit",
    )
    b.table(
        "Slowest / failed requests",
        ch(
            f"""
        SELECT ts, trace_id, request_id, org_id, app_id, model, pod, status, finish_reason,
               tokens_in, tokens_cached, tokens_out,
               round(ttft_us / 1e3, 1) AS ttft_ms, round(tpot_us / 1e3, 2) AS tpot_ms,
               round(queue_us / 1e3, 1) AS queue_ms, round(gateway_us / 1e3, 1) AS gateway_ms,
               prefix_hit_predicted, prefix_hit_actual, preempted
        FROM llm_requests FINAL WHERE $__timeFilter(ts) AND {flt}
        ORDER BY status = 'ok', ttft_us DESC LIMIT 200""",
            "table",
        ),
        h=10,
    )
    b.row("Drill-down: set the trace_id / request_id variable")
    req = (
        "(SELECT * FROM llm_requests FINAL WHERE trace_id = '${trace}' OR request_id = '${trace}' "
        "ORDER BY version DESC LIMIT 1)"
    )
    b.table(
        "Request",
        ch(
            f"""
        SELECT ts, trace_id, request_id, org_id, app_id, user_id, session_id, model, engine, pod, node,
               status, response_code, finish_reason, tokens_in, tokens_cached, tokens_out,
               gateway_us, routing_us, queue_us, prefill_us, decode_us, ttft_us, tpot_us, e2e_us,
               route_candidates, prefix_hit_predicted, prefix_hit_actual, fallback_reason, preempted,
               temperature, top_p, max_tokens FROM {req}""",
            "table",
        ),
        h=5,
    )
    b.table(
        "Spans (gateway → EPP → engine)",
        ch(
            """
        SELECT Timestamp, ServiceName, SpanName, SpanKind, round(Duration / 1e6, 2) AS duration_ms, StatusCode,
               SpanId, ParentSpanId, SpanAttributes
        FROM otel_traces
        WHERE TraceId = (SELECT trace_id FROM llm_requests WHERE trace_id = '${trace}' OR request_id = '${trace}'
                         LIMIT 1)
        ORDER BY Timestamp""",
            "table",
        ),
        h=7,
    )
    b.table(
        "GPU state of the request's pod (±30 s)",
        ch(
            f"""
        SELECT ts, gpu_index, round(compute_active_percent, 1) AS compute_pct,
               round(mem_bw_active_percent, 1) AS hbm_bw_pct, round(power_watts) AS watts,
               round(temp_celsius, 1) AS temp_c, sm_clock_mhz, throttle_reasons
        FROM gpu_samples_v AS g, {req} AS r
        WHERE g.pod = r.pod AND g.ts BETWEEN toDateTime(r.ts) - 30 AND toDateTime(r.ts) + 30
        ORDER BY ts, gpu_index""",
            "table",
        ),
        h=7,
    )
    b.table(
        "Engine state at that time (±60 s)",
        ch(
            f"""
        SELECT toStartOfInterval(TimeUnix, INTERVAL 10 SECOND) AS t, MetricName, round(avg(Value), 3) AS value
        FROM otel_metrics_gauge AS m, {req} AS r
        WHERE m.MetricName LIKE 'llm.%' AND m.Attributes['pod'] = r.pod
          AND m.TimeUnix BETWEEN r.ts - INTERVAL 60 SECOND AND r.ts + INTERVAL 60 SECOND
        GROUP BY t, MetricName ORDER BY t, MetricName""",
            "table",
        ),
        w=12,
        h=7,
    )
    b.table(
        "Kubernetes events for the pod (±5 min)",
        ch(
            f"""
        SELECT e.ts, e.reason, e.type, e.message
        FROM k8s_events AS e, {req} AS r
        WHERE e.name = r.pod AND e.ts BETWEEN r.ts - INTERVAL 5 MINUTE AND r.ts + INTERVAL 5 MINUTE
        ORDER BY e.ts""",
            "table",
        ),
        w=12,
        h=7,
    )
    return b


BOARDS = (overview, gpu, k8s, network, llmd, vllm, tokenomics, explorer)
FILES = (
    "00-overview",
    "01-gpu",
    "02-k8s",
    "03-network",
    "04-llm-d",
    "05-vllm",
    "06-tokenomics",
    "07-request-explorer",
)


def build() -> dict[str, dict[str, Any]]:
    return {name: fn().json() for name, fn in zip(FILES, BOARDS, strict=True)}


# ------------------------------------------------------------------------- Grafana alerts (ClickHouse)
# Signals Prometheus cannot see (logs, request joins, tokenomics) are alerted by Grafana on ClickHouse.
# Each query returns one numeric `value`; the rule fires when `value <op> threshold`.

ALERTS_OUT = Path(__file__).resolve().parent / "alerts"


@dataclass
class ChAlert:
    uid: str
    title: str
    sql: str
    op: str  # gt | lt
    threshold: float
    for_: str
    severity: str
    layer: str
    summary: str


CH_ALERTS = (
    ChAlert(
        "od-gpu-fatal-logs",
        "GPUFatalEventFromLogs",
        "SELECT count() AS value FROM gpu_fatal_events WHERE critical = 1 AND ts > now() - INTERVAL 5 MINUTE",
        "gt",
        0,
        "0s",
        "critical",
        "gpu",
        "Critical GPU event in kernel logs (NVIDIA Xid / AMD RAS uncorrectable or GPU reset): drain the node. "
        "Details: SELECT * FROM gpu_fatal_events ORDER BY ts DESC",
    ),
    ChAlert(
        "od-collective-errors",
        "CollectiveLibraryErrors",
        "SELECT count() AS value FROM collective_errors WHERE ts > now() - INTERVAL 5 MINUTE",
        "gt",
        0,
        "0s",
        "warning",
        "network",
        "NCCL/RCCL errors in engine logs: tensor-parallel collectives failing (network dashboard).",
    ),
    ChAlert(
        "od-trace-join",
        "TracePropagationBroken",
        "SELECT sum(missing_engine_span) / greatest(sum(routed), 1) AS value FROM join_health "
        "WHERE minute > now() - INTERVAL 15 MINUTE",
        "gt",
        0.01,
        "10m",
        "warning",
        "traceability",
        "More than 1% of routed requests have no engine span: traceparent not reaching vLLM, or tracing off.",
    ),
    ChAlert(
        "od-goodput",
        "GoodputBelowTarget",
        "SELECT if(sum(requests) < 50, 1, sum(slo_good) / sum(requests)) AS value FROM llm_rollup_1m FINAL "
        "WHERE minute > now() - INTERVAL 15 MINUTE",
        "lt",
        0.95,
        "15m",
        "warning",
        "vllm",
        "Less than 95% of requests met their TTFT/TPOT SLO in the last 15 minutes.",
    ),
    ChAlert(
        "od-cost-spike",
        "OrgCostSpike",
        "SELECT max(if(hours >= 24 AND avg_h > 0, last_h / avg_h, 0)) AS value FROM ("
        " SELECT org_id,"
        "  sumIf(cost_infra_usd, hour = toStartOfHour(now() - INTERVAL 1 HOUR)) AS last_h,"
        "  sumIf(cost_infra_usd, hour >= now() - INTERVAL 7 DAY)"
        "   / greatest(uniqExactIf(hour, hour >= now() - INTERVAL 7 DAY), 1) AS avg_h,"
        "  uniqExactIf(hour, hour >= now() - INTERVAL 7 DAY) AS hours"
        " FROM tokenomics_hourly FINAL WHERE org_id NOT IN ('(idle)', '(unallocated)') GROUP BY org_id)",
        "gt",
        3,
        "0s",
        "warning",
        "tokenomics",
        "An org's infra cost last hour is more than 3x its 7-day hourly average.",
    ),
)


def alert_rules() -> dict[str, Any]:
    rules = []
    for a in CH_ALERTS:
        rules.append(
            {
                "uid": a.uid,
                "title": a.title,
                "condition": "C",
                "data": [
                    {
                        "refId": "A",
                        "relativeTimeRange": {"from": 900, "to": 0},
                        "datasourceUid": "clickhouse",
                        "model": {**ch(a.sql, "table"), "refId": "A"},
                    },
                    {
                        "refId": "C",
                        "datasourceUid": "__expr__",
                        "model": {
                            "refId": "C",
                            "type": "threshold",
                            "expression": "A",
                            "conditions": [{"evaluator": {"type": a.op, "params": [a.threshold]}}],
                        },
                    },
                ],
                "noDataState": "OK",
                "execErrState": "Error",
                "for": a.for_,
                "labels": {"severity": a.severity, "layer": a.layer, "observe_d": "true"},
                "annotations": {"summary": a.summary},
            }
        )
    group = {"orgId": 1, "name": "observe-d-clickhouse", "folder": "observe-d", "interval": "1m", "rules": rules}
    return {"apiVersion": 1, "groups": [group]}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, doc in build().items():
        (OUT / f"{name}.json").write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8", newline="\n")
        print(f"wrote grafana/dashboards/{name}.json")
    ALERTS_OUT.mkdir(parents=True, exist_ok=True)
    path = ALERTS_OUT / "observe-d-clickhouse-alerts.json"
    path.write_text(json.dumps(alert_rules(), indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote grafana/alerts/{path.name}")


if __name__ == "__main__":
    main()
