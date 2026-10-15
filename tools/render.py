"""Render observe-d manifests for a real cluster from the same sources the local stack uses.

    uv run python -m tools.render --cluster-name mi300x-a --out deploy/rendered

Writes (apply with `kubectl apply --server-side -f deploy/rendered/`):
  00-collectors.yaml        agent DaemonSet + cluster collector (no hostAliases: Kafka is a Service)
  01-collector-configs.yaml otel-agent-config / otel-cluster-config / otel-gateway-config ConfigMaps
  02-prometheus-rules.yaml  normalized recording rules + k8s GPU rules + alerts
  03-dashboards.yaml        Grafana dashboards + ClickHouse alert rules (sidecar ConfigMaps)
  helm/kube-prometheus-stack-values.yaml  values with the ClickHouse datasource host filled in

The simulated inference stack is NOT rendered: on real hardware the GPU Operator exporters and the llm-d /
vLLM deployment provide the telemetry (docs/deploy.md).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent


def configmap(
    name: str,
    namespace: str,
    files: dict[str, Path],
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace, "labels": labels or {}, "annotations": annotations or {}},
        "data": {k: p.read_text(encoding="utf-8") for k, p in files.items()},
    }


def dump_all(docs: list[dict[str, Any]]) -> str:
    return yaml.safe_dump_all(docs, sort_keys=False, width=1000)


def render_collectors(text: str, ns: str, kafka: str, cluster: str, otelcol_tag: str) -> str:
    text = re.sub(r"\n\s+hostAliases:\n\s+- \{ip: \"__KAFKA_IP__\", hostnames: \[kafka\]\}", "", text)
    text = text.replace('value: "kafka:9092"', f'value: "{kafka}"')
    text = text.replace(
        "# The backend (Kafka) runs outside kind on the `kind` podman network: reached via hostAliases.",
        f"# Rendered by tools/render.py for cluster {cluster}; Kafka: {kafka}.",
    )
    text = text.replace(
        "# The backend (Kafka) runs outside kind on the `kind` podman network: reached via hostAliases.",
        f"# Rendered by tools/render.py for cluster {cluster}; Kafka: {kafka}.",
    )
    text = text.replace("__OTELCOL_TAG__", otelcol_tag).replace("__CLUSTER_NAME__", cluster)
    text = text.replace("namespace: observe-d", f"namespace: {ns}")
    text = text.replace("name: observe-d\n", f"name: {ns}\n", 1)
    return text.replace("otel-agent.observe-d.svc", f"otel-agent.{ns}.svc")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cluster-name", required=True)
    p.add_argument("--namespace", default="observability")
    p.add_argument("--monitoring-namespace", default="monitoring")
    p.add_argument("--kafka", default="observe-d-kafka-bootstrap.observability.svc:9092")
    p.add_argument("--clickhouse-host", default="clickhouse-observe-d.observability.svc")
    p.add_argument("--out", type=Path, default=ROOT / "deploy" / "rendered")
    a = p.parse_args(argv)

    versions = yaml.safe_load((ROOT / "versions.yaml").read_text(encoding="utf-8"))
    tag = str(versions["otel_collector_contrib"]["tag"])
    out: Path = a.out
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run([sys.executable, "-m", "normalize.generate"], check=True, cwd=ROOT)
    subprocess.run([sys.executable, "-m", "grafana.build"], check=True, cwd=ROOT, capture_output=True)

    collectors = render_collectors(
        (ROOT / "local/kind/collectors.yaml").read_text(encoding="utf-8"),
        a.namespace,
        a.kafka,
        a.cluster_name,
        tag,
    )
    (out / "00-collectors.yaml").write_text(collectors, encoding="utf-8", newline="\n")

    configs = [
        configmap(
            "otel-agent-config",
            a.namespace,
            {
                "agent.yaml": ROOT / "collector/agent.yaml",
                "gpu-transform.yaml": ROOT / "collector/generated/gpu-transform.yaml",
                "engine-transform.yaml": ROOT / "collector/generated/engine-transform.yaml",
            },
        ),
        configmap("otel-cluster-config", a.namespace, {"cluster.yaml": ROOT / "collector/cluster.yaml"}),
        configmap("otel-gateway-config", a.namespace, {"gateway.yaml": ROOT / "collector/gateway.yaml"}),
    ]
    (out / "01-collector-configs.yaml").write_text(dump_all(configs), encoding="utf-8", newline="\n")

    rules = []
    for path in sorted((ROOT / "prometheus/rules").glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        doc["metadata"]["namespace"] = a.monitoring_namespace
        rules.append(doc)
    (out / "02-prometheus-rules.yaml").write_text(dump_all(rules), encoding="utf-8", newline="\n")

    boards = [
        configmap(
            f"dashboard-{p.stem}",
            a.monitoring_namespace,
            {p.name: p},
            {"grafana_dashboard": "1"},
            {"grafana_folder": "observe-d"},
        )
        for p in sorted((ROOT / "grafana/dashboards").glob("*.json"))
    ] + [
        configmap(f"alerts-{p.stem}", a.monitoring_namespace, {p.name: p}, {"grafana_alert": "1"})
        for p in sorted((ROOT / "grafana/alerts").glob("*.json"))
    ]
    (out / "03-dashboards.yaml").write_text(dump_all(boards), encoding="utf-8", newline="\n")

    values = (ROOT / "deploy/helm/kube-prometheus-stack-values.yaml").read_text(encoding="utf-8")
    (out / "helm").mkdir(exist_ok=True)
    (out / "helm" / "kube-prometheus-stack-values.yaml").write_text(
        values.replace("__CLICKHOUSE_IP__", a.clickhouse_host), encoding="utf-8", newline="\n"
    )
    for f in sorted(out.iterdir()):
        print(f"wrote {f.relative_to(ROOT) if f.is_relative_to(ROOT) else f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
