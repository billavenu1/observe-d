"""Capacity / resilience test against the running stack (MVP.md §8, M7 locally, M8 on the MI300X cluster).

    uv run python -m tools.loadtest --rate 500 --duration 900 --burst-rate 700 --burst-at 300 --burst-duration 120
    uv run python -m tools.loadtest --rate 200 --duration 600 --stop clickhouse --stop-at 120 --stop-for 180

Runs sim/telemetry_replay as a Job inside the cluster, optionally stops a backend container mid-run, waits for
the pipeline to drain, then checks the acceptance criteria and writes local/.state/loadtest-<run>.json:

  rows        llm_requests rows for the run == requests sent (±0.1%)
  join        rows with an engine span >= 99%
  drain_s     time after the generator stops until every request is in ClickHouse
  dropped     collector refused/failed/dropped records == 0 (agent via Prometheus, gateway via :8888)
  parts       max active parts in any partition < 300
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
import uuid
from typing import Any

import httpx

from tools.dev import CLI, ROOT, STATE, apply_text, env, kubectl, run

PROM = "http://localhost:9090"
GATEWAY_METRICS = "http://localhost:8888/metrics"
CONTAINERS = {"clickhouse": "od-clickhouse", "kafka": "od-kafka"}
DROP_RE = re.compile(r"^otelcol_(receiver_refused|exporter_send_failed|processor_dropped|exporter_enqueue_failed)\w*")


def ch(sql: str) -> list[tuple[Any, ...]]:
    import clickhouse_connect

    client = clickhouse_connect.get_client(host="localhost", username="otel", password="otel", database="otel")
    return [tuple(r) for r in client.query(sql).result_rows]


def gateway_drops() -> float:
    try:
        text = httpx.get(GATEWAY_METRICS, timeout=10).text
    except httpx.HTTPError:
        return float("nan")
    total = 0.0
    for line in text.splitlines():
        if DROP_RE.match(line):
            total += float(line.rsplit(" ", 1)[1])
    return total


def agent_drops() -> float:
    q = 'sum({__name__=~"otelcol_(receiver_refused|exporter_send_failed|processor_dropped|exporter_enqueue_failed).*"})'
    try:
        res = httpx.get(f"{PROM}/api/v1/query", params={"query": q}, timeout=10).json()["data"]["result"]
    except (httpx.HTTPError, KeyError):
        return float("nan")
    return float(res[0]["value"][1]) if res else 0.0


def max_parts() -> int:
    rows = ch(
        "SELECT max(c) FROM (SELECT table, partition, count() AS c FROM system.parts "
        "WHERE active AND database = 'otel' GROUP BY table, partition)"
    )
    return int(rows[0][0] or 0)


def kafka_lag() -> int:
    out = subprocess.run(
        [
            CLI,
            "exec",
            "od-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            "observe-d-gateway",
        ],
        env=env(),
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    lag = 0
    for line in out.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 6 and parts[5].isdigit():
            lag += int(parts[5])
    return lag


def chaos(container: str, at: float, duration: float, log: list[str]) -> None:
    time.sleep(at)
    log.append(f"{time.strftime('%H:%M:%S')} stop {container}")
    run(CLI, "stop", container, check=False)
    time.sleep(duration)
    run(CLI, "start", container, check=False)
    log.append(f"{time.strftime('%H:%M:%S')} start {container}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rate", type=float, default=500)
    p.add_argument("--duration", type=float, default=900)
    p.add_argument("--burst-rate", type=float, default=0)
    p.add_argument("--burst-at", type=float, default=300)
    p.add_argument("--burst-duration", type=float, default=120)
    p.add_argument("--stop", choices=sorted(CONTAINERS), help="stop this backend container mid-run")
    p.add_argument("--stop-at", type=float, default=120)
    p.add_argument("--stop-for", type=float, default=120)
    p.add_argument("--drain-timeout", type=float, default=1800)
    p.add_argument("--parallelism", type=int, default=0, help="generator pods (default: 1 per 400 req/s)")
    a = p.parse_args(argv)

    run_id = uuid.uuid4().hex[:8]
    org = f"loadtest-{run_id}"
    STATE.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"run_id": run_id, "org_id": org, "params": vars(a), "events": []}
    drops_before = {"agent": agent_drops(), "gateway": gateway_drops()}

    pods = a.parallelism or max(1, int(max(a.rate, a.burst_rate) // 400) + 1)
    report["generator_pods"] = pods
    manifest = (ROOT / "local/kind/replay-job.yaml").read_text(encoding="utf-8")
    for key, val in {
        "__RUN_ID__": run_id,
        "__PARALLELISM__": pods,
        "__RATE__": a.rate / pods,
        "__DURATION__": a.duration,
        "__BURST_RATE__": a.burst_rate / pods,
        "__BURST_AT__": a.burst_at,
        "__BURST_DURATION__": a.burst_duration,
    }.items():
        manifest = manifest.replace(key, str(val))
    apply_text(manifest)
    job = f"job/replay-{run_id}"
    started = time.time()
    if a.stop:
        threading.Thread(
            target=chaos, args=(CONTAINERS[a.stop], a.stop_at, a.stop_for, report["events"]), daemon=True
        ).start()

    lag_samples: list[int] = []
    while True:
        st = kubectl(
            "-n",
            "observe-d",
            "get",
            job,
            "-o",
            "jsonpath={.status.succeeded},{.status.failed}",
            capture=True,
            check=False,
        ).strip()
        succeeded, _, failed = st.partition(",")
        lag_samples.append(kafka_lag())
        if int(succeeded or 0) >= pods or int(failed or 0) > 0:
            break
        time.sleep(15)
    gen_end = time.time()
    logs = kubectl(
        "-n",
        "observe-d",
        "logs",
        "-l",
        f"job-name=replay-{run_id}",
        "--tail=5",
        "--max-log-requests=64",
        capture=True,
        check=False,
    )
    done = [json.loads(line) for line in logs.splitlines() if line.startswith("{") and '"done"' in line]
    sent = sum(int(d["sent"]) for d in done) if len(done) == pods else -1
    report["generator_failed_batches"] = sum(int(d.get("failed_batches", 0)) for d in done)
    report["generator_span_failed_requests"] = sum(int(d.get("span_failed_requests", 0)) for d in done)
    report["sent"] = sent
    report["generator_seconds"] = round(gen_end - started, 1)
    report["achieved_rps"] = round(sent / max(a.duration, 1), 1) if sent > 0 else 0.0

    # drain: raw gateway rows for the run reach `sent`
    drain_s = None
    while time.time() - gen_end < a.drain_timeout:
        got = ch(f"SELECT count() FROM gateway_requests WHERE org_id = '{org}'")[0][0]
        lag_samples.append(kafka_lag())
        if sent >= 0 and got >= sent:
            drain_s = round(time.time() - gen_end, 1)
            break
        time.sleep(10)
    report["drain_s"] = drain_s
    time.sleep(150)  # two refresh cycles of llm_requests_refresh after the last arrival

    rows, joined = ch(
        f"SELECT count(), countIf(has_engine = 1 AND has_route = 1) FROM llm_requests FINAL WHERE org_id = '{org}'"
    )[0]
    report["rows"] = int(rows)
    report["join_ratio"] = round(joined / rows, 5) if rows else 0.0
    report["row_error"] = round(abs(rows - sent) / sent, 5) if sent > 0 else None
    report["max_kafka_lag_messages"] = max(lag_samples) if lag_samples else None
    report["dropped"] = {
        "agent": agent_drops() - drops_before["agent"],
        "gateway": gateway_drops() - drops_before["gateway"],
    }
    report["max_parts_per_partition"] = max_parts()
    report["checks"] = {
        "rows_match_0.1pct": report["row_error"] is not None and report["row_error"] <= 0.001,
        "join_ge_99pct": report["join_ratio"] >= 0.99,
        "drained": drain_s is not None,
        "no_drops": all(v == 0 for v in report["dropped"].values()),
        "no_backpressure": report["generator_failed_batches"] == 0 and report["generator_span_failed_requests"] == 0,
        "parts_lt_300": report["max_parts_per_partition"] < 300,
    }
    report["passed"] = all(report["checks"].values())
    out = STATE / f"loadtest-{run_id}.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nreport: {out.relative_to(ROOT)}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
