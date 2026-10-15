"""Local stack orchestration (cross-platform; Podman + kind).

    uv run python -m tools.dev up [--vendor amd|nvidia] [--rate 20] [--rebuild]
    uv run python -m tools.dev status
    uv run python -m tools.dev scenario --engine slow|errors|kv_pressure|nccl_error|kv_transfer_degraded|normal
                                       [--pool decode|prefill|both] [--model llama|qwen]
    uv run python -m tools.dev scenario --gpu load|throttle|xid|ecc|link_errors
    uv run python -m tools.dev scenario --load prompt_shift|normal        # P1: one org's prompts x3
    uv run python -m tools.dev scenario --routing scorer_skew|normal      # P4: prefix scorer weight raised
    uv run python -m tools.dev scenario --playbook p1|p4|p5|normal        # the scenario(s) behind a playbook
    uv run python -m tools.dev rate 50
    uv run python -m tools.dev down [--keep-cluster]

Backend (Kafka, ClickHouse, gateway collector) runs in podman-compose (or docker compose with
CONTAINER_CLI=docker) on the `kind` network; the "GPU
cluster" (fake GPUs, simulated llm-d stack, kube-prometheus-stack, agent/cluster collectors) runs in kind.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "local" / ".state"
CLUSTER = "observe-d"
NODE = f"{CLUSTER}-control-plane"
CLI = os.environ.get("CONTAINER_CLI", "podman")  # podman (default, local) | docker (CI / Linux)
COMPOSE = ["podman-compose"] if CLI == "podman" else ["docker", "compose"]
SIM_IMAGE = "localhost/observe-d-sim:dev"
TOPICS = ("otlp_logs", "otlp_metrics", "otlp_traces")
GPU_RESOURCE = {"amd": "amd.com/gpu", "nvidia": "nvidia.com/gpu"}
ENGINE_MODELS = ("llama-70b", "qwen-7b")
ENGINE_DEPLOYMENTS = tuple(f"vllm-{m}-{role}" for m in ENGINE_MODELS for role in ("prefill", "decode"))
ENGINE_SCENARIOS = ("normal", "slow", "errors", "kv_pressure", "nccl_error", "kv_transfer_degraded")
PROMPT_SHIFT = "acme:3"  # P1: acme's prompts triple (RAG context bloat) -> prefill pool saturates
SKEWED_WEIGHTS = "prefix=0.9,queue=0.05,kv=0.05"  # P4: prefix affinity dominates -> one decode pod runs hot
DEFAULT_WEIGHTS = "prefix=0.4,queue=0.4,kv=0.2"
# playbook -> scenario flags (docs/observability-story.md §3)
PLAYBOOKS: dict[str, dict[str, str]] = {
    "p1": {"load": "prompt_shift"},
    "p4": {"routing": "scorer_skew"},
    "p5": {"engine": "kv_transfer_degraded", "gpu": "link_errors"},
    "normal": {"engine": "normal", "gpu": "load", "load": "normal", "routing": "normal"},
}


def versions() -> dict:  # type: ignore[type-arg]
    return yaml.safe_load((ROOT / "versions.yaml").read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def env() -> dict[str, str]:
    e = dict(os.environ)
    e["PATH"] = str(ROOT / ".tools" / "bin") + os.pathsep + e.get("PATH", "")
    if CLI == "podman":
        e["KIND_EXPERIMENTAL_PROVIDER"] = "podman"
    e.update(backend_ips())
    return e


_IPS: dict[str, str] = {}


def backend_ips() -> dict[str, str]:
    """Static IPs for the backend on the `kind` network (.100-.102 of its IPv4 subnet).

    Pods reach the backend by IP (hostAliases / datasource), so the addresses must be fixed; the subnet differs
    between podman (10.89.0.0/24) and docker (172.18.0.0/16)."""
    if not _IPS:
        out = subprocess.run(
            [CLI, "network", "inspect", "kind", "--format", "{{range .Subnets}}{{.Subnet}} {{end}}"]
            if CLI == "podman"
            else [CLI, "network", "inspect", "kind", "--format", "{{range .IPAM.Config}}{{.Subnet}} {{end}}"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        v4 = next((n for n in out.split() if "." in n), "10.89.0.0/24")
        base = ".".join(v4.split("/")[0].split(".")[:3])
        _IPS.update({"KAFKA_IP": f"{base}.100", "CLICKHOUSE_IP": f"{base}.101", "GATEWAY_COLLECTOR_IP": f"{base}.102"})
    return _IPS


def run(*cmd: str, check: bool = True, capture: bool = False, stdin: str | None = None) -> str:
    print("+ " + " ".join(cmd), flush=True)
    exe = shutil.which(cmd[0], path=env()["PATH"]) or cmd[0]
    res = subprocess.run(
        [exe, *cmd[1:]],
        env=env(),
        cwd=ROOT,
        input=stdin,
        text=True,
        capture_output=capture,
        check=False,
    )
    if check and res.returncode != 0:
        if capture:
            print(res.stdout, res.stderr, file=sys.stderr)
        raise SystemExit(f"command failed ({res.returncode}): {' '.join(cmd)}")
    return res.stdout if capture else ""


def kubectl(*args: str, **kw: object) -> str:
    return run("kubectl", "--context", f"kind-{CLUSTER}", *args, **kw)  # type: ignore[arg-type]


def apply_text(text: str) -> None:
    kubectl("apply", "--server-side", "--force-conflicts", "-f", "-", stdin=text)


# ------------------------------------------------------------------------------------------- steps


def ensure_cluster() -> None:
    clusters = run("kind", "get", "clusters", capture=True, check=False).split()
    if CLUSTER not in clusters:
        run("kind", "create", "cluster", "--config", "local/kind-config.yaml")


def set_vendor(vendor: str) -> None:
    other = "nvidia" if vendor == "amd" else "amd"
    kubectl("label", "--overwrite", f"node/{NODE}", f"gpu-vendor={vendor}")
    patch = [{"op": "add", "path": f"/status/capacity/{GPU_RESOURCE[vendor].replace('/', '~1')}", "value": "8"}]
    kubectl("patch", f"node/{NODE}", "--subresource=status", "--type=json", "-p", json.dumps(patch))
    caps = json.loads(kubectl("get", f"node/{NODE}", "-o", "json", capture=True))["status"]["capacity"]
    if GPU_RESOURCE[other] in caps:
        rm = [{"op": "remove", "path": f"/status/capacity/{GPU_RESOURCE[other].replace('/', '~1')}"}]
        kubectl("patch", f"node/{NODE}", "--subresource=status", "--type=json", "-p", json.dumps(rm))


def backend_up() -> None:
    run(*COMPOSE, "-f", "local/docker-compose.yml", "up", "-d")
    for name in ("od-kafka", "od-clickhouse"):
        for _ in range(60):
            status = run(CLI, "inspect", "-f", "{{.State.Health.Status}}", name, capture=True, check=False)
            if status.strip() == "healthy":
                break
            time.sleep(3)
        else:
            raise SystemExit(f"{name} did not become healthy")
    for topic in TOPICS:
        run(
            CLI,
            "exec",
            "od-kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "6",
            "--config",
            "compression.type=zstd",
            "--config",
            "retention.ms=86400000",
        )
    run(sys.executable, "-m", "tools.migrate")


def load_sim_image(rebuild: bool) -> None:
    exists = subprocess.run([CLI, "image", "inspect", SIM_IMAGE], env=env(), capture_output=True).returncode == 0
    if rebuild or not exists:
        run(CLI, "build", "-t", SIM_IMAGE, "-f", "sim/Containerfile", ".")
    if CLI == "docker":
        run("kind", "load", "docker-image", SIM_IMAGE, "--name", CLUSTER)
        return
    STATE.mkdir(parents=True, exist_ok=True)
    tar = STATE / "sim.tar"
    if tar.exists():
        tar.unlink()
    run(CLI, "save", "-o", str(tar), SIM_IMAGE)
    run("kind", "load", "image-archive", str(tar), "--name", CLUSTER)


def monitoring_up() -> None:
    v = versions()["kube_prometheus_stack"]
    values = (ROOT / "deploy/helm/kube-prometheus-stack-values.yaml").read_text(encoding="utf-8")
    values = values.replace("__CLICKHOUSE_IP__", backend_ips()["CLICKHOUSE_IP"])
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as fh:
        fh.write(values)
    try:
        run(
            "helm",
            "repo",
            "add",
            "prometheus-community",
            "https://prometheus-community.github.io/helm-charts",
            check=False,
        )
        run(
            "helm",
            "upgrade",
            "--install",
            "kps",
            "prometheus-community/kube-prometheus-stack",
            "--kube-context",
            f"kind-{CLUSTER}",
            "-n",
            "monitoring",
            "--create-namespace",
            "--version",
            str(v["version"]),
            "-f",
            fh.name,
            "--timeout",
            "15m",
        )
    finally:
        os.unlink(fh.name)
    # Only the operator (CRDs, PrometheusRule/PodMonitor handling) must be ready; Grafana can take minutes
    # to load plugins on small machines and is not on the critical path.
    kubectl("-n", "monitoring", "rollout", "status", "deployment/kps-operator", "--timeout=10m")
    rules_up()


def rules_up() -> None:
    for rules in sorted((ROOT / "prometheus/rules").glob("*.yaml")):
        doc = yaml.safe_load(rules.read_text(encoding="utf-8"))
        doc["metadata"]["namespace"] = "monitoring"
        apply_text(yaml.safe_dump(doc, sort_keys=False))


def configmap(
    name: str,
    namespace: str,
    files: dict[str, Path],
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> None:
    cm = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels or {},
            "annotations": annotations or {},
        },
        "data": {key: path.read_text(encoding="utf-8") for key, path in files.items()},
    }
    apply_text(yaml.safe_dump(cm, sort_keys=False))


def workloads_up(vendor: str, rate: float) -> None:
    subs = {
        "__KAFKA_IP__": backend_ips()["KAFKA_IP"],
        "__OTELCOL_TAG__": str(versions()["otel_collector_contrib"]["tag"]),
        "__CLUSTER_NAME__": "local-kind",
        "__GPU_VENDOR__": vendor,
        "__GPU_RESOURCE__": GPU_RESOURCE[vendor],
        "__LOAD_RATE__": str(rate),
    }

    def render(path: str) -> str:
        text = (ROOT / path).read_text(encoding="utf-8")
        for k, val in subs.items():
            text = text.replace(k, val)
        return text

    apply_text(render("local/kind/collectors.yaml").split("\n---\n", 1)[0])  # namespace first
    configmap(
        "otel-agent-config",
        "observe-d",
        {
            "agent.yaml": ROOT / "collector/agent.yaml",
            "gpu-transform.yaml": ROOT / "collector/generated/gpu-transform.yaml",
            "engine-transform.yaml": ROOT / "collector/generated/engine-transform.yaml",
        },
    )
    configmap("otel-cluster-config", "observe-d", {"cluster.yaml": ROOT / "collector/cluster.yaml"})
    apply_text(render("local/kind/collectors.yaml"))
    # Collectors first (restart picks up config changes) and wait: workloads emit startup events once.
    for name in ("daemonset/otel-agent", "deployment/otel-cluster"):
        kubectl("-n", "observe-d", "rollout", "restart", name)
    for name in ("daemonset/otel-agent", "deployment/otel-cluster"):
        kubectl("-n", "observe-d", "rollout", "status", name, "--timeout=10m")
    apply_text(render("local/kind/inference-sim.yaml"))
    for ns, name in (
        ("observe-d", "daemonset/fake-gpu-exporter"),
        *(("llm", f"deployment/{name}") for name in ENGINE_DEPLOYMENTS),
        ("llm", "deployment/inference-gateway"),
        ("llm", "deployment/loadgen"),
    ):
        kubectl("-n", ns, "rollout", "restart", name)
    dashboards_up()


def dashboards_up() -> None:
    run(sys.executable, "-m", "grafana.build")
    for path in sorted((ROOT / "grafana/dashboards").glob("*.json")):
        configmap(
            f"dashboard-{path.stem}",
            "monitoring",
            {path.name: path},
            labels={"grafana_dashboard": "1"},
            annotations={"grafana_folder": "observe-d"},
        )
    for path in sorted((ROOT / "grafana/alerts").glob("*.json")):
        configmap(f"alerts-{path.stem}", "monitoring", {path.name: path}, labels={"grafana_alert": "1"})


def scenario(
    engine: str | None = None,
    gpu: str | None = None,
    load: str | None = None,
    routing: str | None = None,
    pool: str = "decode",
    model: str = "llama-70b",
) -> None:
    if engine:
        roles = ("prefill", "decode") if pool == "both" else (pool,)
        for role in roles:
            kubectl("-n", "llm", "set", "env", f"deployment/vllm-{model}-{role}", f"SCENARIO={engine}")
    if gpu:
        kubectl("-n", "observe-d", "set", "env", "daemonset/fake-gpu-exporter", f"SCENARIO={gpu}")
    if load:
        shift = PROMPT_SHIFT if load == "prompt_shift" else ""
        kubectl("-n", "llm", "set", "env", "deployment/loadgen", f"PROMPT_SHIFT={shift}")
    if routing:
        weights = SKEWED_WEIGHTS if routing == "scorer_skew" else DEFAULT_WEIGHTS
        kubectl("-n", "llm", "set", "env", "deployment/inference-gateway", f"SCORER_WEIGHTS={weights}")


def status() -> None:
    run(CLI, "ps", "--format", "{{.Names}}\t{{.Status}}", check=False)
    kubectl("get", "pods", "-A", "-o", "wide", check=False)
    run(sys.executable, "-m", "tools.migrate", "--status", check=False)
    print("\nGrafana:    http://localhost:3300  (admin/admin)")
    print("Prometheus: http://localhost:9090")
    print("ClickHouse: http://localhost:8123  (otel/otel)")


# -------------------------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("up")
    up.add_argument("--vendor", choices=["amd", "nvidia"], default="amd")
    up.add_argument("--rate", type=float, default=20)
    up.add_argument("--rebuild", action="store_true", help="rebuild the simulator image")
    up.add_argument("--skip-monitoring", action="store_true", help="skip helm install (already installed)")
    sub.add_parser("status")
    sub.add_parser("dashboards")
    sc = sub.add_parser("scenario")
    sc.add_argument("--engine", choices=ENGINE_SCENARIOS)
    sc.add_argument("--pool", choices=["decode", "prefill", "both"], default="decode", help="engine pool (P/D)")
    sc.add_argument("--model", choices=ENGINE_MODELS, default="llama-70b", help="engine model")
    sc.add_argument("--gpu", choices=["load", "throttle", "xid", "ecc", "link_errors"])
    sc.add_argument("--load", choices=["normal", "prompt_shift"], help="loadgen traffic shape")
    sc.add_argument("--routing", choices=["normal", "scorer_skew"], help="EPP scorer weights")
    sc.add_argument("--playbook", choices=sorted(PLAYBOOKS), help="apply the scenario(s) behind a playbook")
    rate = sub.add_parser("rate")
    rate.add_argument("rps", type=float)
    down = sub.add_parser("down")
    down.add_argument("--keep-cluster", action="store_true")
    a = p.parse_args(argv)

    if a.cmd == "up":
        ensure_cluster()
        set_vendor(a.vendor)
        backend_up()
        load_sim_image(a.rebuild)
        if not a.skip_monitoring:
            monitoring_up()
        else:
            rules_up()
        workloads_up(a.vendor, a.rate)
        status()
    elif a.cmd == "status":
        status()
    elif a.cmd == "dashboards":
        dashboards_up()
    elif a.cmd == "scenario":
        flags = {"engine": a.engine, "gpu": a.gpu, "load": a.load, "routing": a.routing}
        flags.update(PLAYBOOKS.get(a.playbook, {}) if a.playbook else {})
        scenario(**flags, pool=a.pool, model=a.model)
    elif a.cmd == "rate":
        kubectl("-n", "llm", "set", "env", "deployment/loadgen", f"RATE={a.rps}")
    elif a.cmd == "down":
        if not a.keep_cluster:
            run("kind", "delete", "cluster", "--name", CLUSTER, check=False)
        run(*COMPOSE, "-f", "local/docker-compose.yml", "down", "-v", check=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
