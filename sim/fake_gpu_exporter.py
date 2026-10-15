"""Fake GPU exporter: 8 simulated GPUs exposed as NVIDIA DCGM-exporter or AMD Device-Metrics-Exporter metrics.

Models the *vendor* exporters (native metric names, labels and units) independently of normalize/gpu.yaml,
so tests can check the mapping against it. AMD names/units are best guesses until real MI300X output is
captured in M8 (sim/fixtures/amd_mi300x/); NVIDIA names follow dcgm-exporter.

    python -m sim.fake_gpu_exporter --vendor amd --port 9400 --scenario load
    python -m sim.fake_gpu_exporter --vendor nvidia --k8s     # map GPUs to pods via the k8s API (in-cluster)
"""

from __future__ import annotations

import argparse
import os
import random
import threading
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Literal

Vendor = Literal["nvidia", "amd"]
SCENARIOS = ("idle", "load", "throttle", "xid", "ecc", "link_errors")
MIB = 1024 * 1024


@dataclass(frozen=True)
class Profile:
    model: str
    mem_total_bytes: int
    tdp_watts: float
    idle_watts: float
    max_clock_mhz: float


PROFILES: dict[Vendor, Profile] = {
    "nvidia": Profile("NVIDIA H100 80GB HBM3", 80 * 1024 * MIB, 700.0, 70.0, 1980.0),
    "amd": Profile("AMD Instinct MI300X", 192 * 1024 * MIB, 750.0, 140.0, 2100.0),
}

# NVML clock-event reason bits (subset)
THROTTLE_BITS = {"sw_power_cap": 2, "hw_slowdown": 3, "sw_thermal": 5, "hw_thermal": 6}


@dataclass
class GpuState:
    """One GPU, in normalized units. Vendor renderers convert to native units."""

    index: int
    uuid: str
    busy_percent: float = 0.0
    compute_active_percent: float = 0.0
    matrix_active_percent: float = 0.0
    mem_bw_active_percent: float = 0.0
    mem_used_bytes: int = 0
    mem_total_bytes: int = 80 * 1024 * MIB
    power_watts: float = 0.0
    energy_joules: float = 0.0
    temp_celsius: float = 35.0
    mem_temp_celsius: float = 40.0
    sm_clock_mhz: float = 1980.0
    throttle_reasons: set[str] = field(default_factory=set)
    ecc_correctable: int = 0
    ecc_uncorrectable: int = 0
    last_xid: int = 0
    row_remap_failed: int = 0
    pcie_tx_bps: float = 0.0
    pcie_rx_bps: float = 0.0
    pcie_replays: int = 0
    link_tx_bps: float = 0.0
    link_rx_bps: float = 0.0
    pod: str = ""
    namespace: str = ""
    container: str = ""


def new_gpus(vendor: Vendor, node: str, count: int = 8) -> list[GpuState]:
    prof = PROFILES[vendor]
    prefix = "GPU" if vendor == "nvidia" else "AMD"
    return [
        GpuState(
            index=i,
            uuid=f"{prefix}-{node}-{i:02d}-0000-0000-000000000000",
            mem_total_bytes=prof.mem_total_bytes,
            power_watts=prof.idle_watts,
            sm_clock_mhz=prof.max_clock_mhz,
        )
        for i in range(count)
    ]


def step(
    gpus: Sequence[GpuState],
    vendor: Vendor,
    scenario: str,
    dt: float,
    rng: random.Random,
    load: dict[int, float] | None = None,
) -> None:
    """Advance the simulation by dt seconds.

    `load` (GPU index -> 0..1) couples GPUs to the engine pod using them (k8s mode). GPUs without an entry
    are unallocated and idle. Without `load`, every GPU follows the scenario's synthetic load.
    """
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; choose from {SCENARIOS}")
    prof = PROFILES[vendor]
    loaded = scenario != "idle"
    for g in gpus:
        level = None if load is None else load.get(g.index)
        if load is not None and level is not None and loaded:
            g.compute_active_percent = min(100.0, 8 + 85 * level + rng.uniform(-3, 3))
            g.matrix_active_percent = g.compute_active_percent * rng.uniform(0.4, 0.7)
            g.mem_bw_active_percent = min(100.0, 10 + 80 * level + rng.uniform(-3, 3))
            g.busy_percent = min(100.0, g.compute_active_percent + 25) if level > 0 else 0.0
            g.mem_used_bytes = int(g.mem_total_bytes * 0.9)  # vLLM pre-allocates KV cache
            g.power_watts = prof.idle_watts + (prof.tdp_watts - prof.idle_watts) * g.compute_active_percent / 100
            g.temp_celsius = 40 + 30 * level + rng.uniform(0, 3)
            g.mem_temp_celsius = g.temp_celsius + 8
            g.pcie_tx_bps, g.pcie_rx_bps = 2e9 * level * rng.random(), 1e9 * level * rng.random()
            g.link_tx_bps = g.link_rx_bps = 150e9 * g.compute_active_percent / 100
        elif loaded and load is None:
            g.compute_active_percent = min(100.0, 55 + rng.uniform(-10, 25))
            g.matrix_active_percent = g.compute_active_percent * rng.uniform(0.4, 0.7)
            g.mem_bw_active_percent = min(100.0, 60 + rng.uniform(-10, 30))
            g.busy_percent = min(100.0, g.compute_active_percent + 30)
            g.mem_used_bytes = int(g.mem_total_bytes * 0.9)
            g.power_watts = prof.idle_watts + (prof.tdp_watts - prof.idle_watts) * g.compute_active_percent / 100
            g.temp_celsius, g.mem_temp_celsius = 62 + rng.uniform(0, 8), 70 + rng.uniform(0, 8)
            g.pcie_tx_bps, g.pcie_rx_bps = 2e9 * rng.random(), 1e9 * rng.random()
            g.link_tx_bps = g.link_rx_bps = 150e9 * g.compute_active_percent / 100
        else:
            g.compute_active_percent = g.matrix_active_percent = g.mem_bw_active_percent = 0.0
            g.busy_percent = 0.0
            g.mem_used_bytes = 0
            g.power_watts, g.temp_celsius, g.mem_temp_celsius = prof.idle_watts, 35.0, 40.0
            g.pcie_tx_bps = g.pcie_rx_bps = g.link_tx_bps = g.link_rx_bps = 0.0
        g.sm_clock_mhz = prof.max_clock_mhz
        g.throttle_reasons = set()
        g.energy_joules += g.power_watts * dt

    hot = gpus[0]
    if scenario == "throttle":
        hot.temp_celsius = 92.0
        hot.throttle_reasons = {"hw_thermal", "sw_thermal"}
        hot.sm_clock_mhz = prof.max_clock_mhz * 0.6
    elif scenario == "xid":
        hot.last_xid = 79  # "GPU has fallen off the bus"
    elif scenario == "ecc":
        hot.ecc_correctable += 3
        hot.ecc_uncorrectable = max(hot.ecc_uncorrectable, 1)
        hot.row_remap_failed = 1
    elif scenario == "link_errors":
        hot.pcie_replays += 5


def assign_gpus(gpus: Sequence[GpuState], pods: Iterable[tuple[str, str, str, int]]) -> None:
    """Assign GPUs to (pod, namespace, container, gpu_count) in order, like a device plugin would."""
    for g in gpus:
        g.pod = g.namespace = g.container = ""
    free = iter(gpus)
    for pod, namespace, container, count in pods:
        for _ in range(count):
            slot = next(free, None)
            if slot is None:
                return
            slot.pod, slot.namespace, slot.container = pod, namespace, container


# --- rendering (Prometheus text exposition) -------------------------------------------------------


def _fmt_labels(labels: dict[str, str]) -> str:
    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    return "{" + ",".join(f'{k}="{esc(v)}"' for k, v in labels.items()) + "}"


def _num(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else repr(float(v))


class _Writer:
    def __init__(self) -> None:
        self.families: dict[str, tuple[str, str, list[str]]] = {}

    def add(self, name: str, mtype: str, help_: str, labels: dict[str, str], value: float) -> None:
        fam = self.families.setdefault(name, (mtype, help_, []))
        fam[2].append(f"{name}{_fmt_labels(labels)} {_num(value)}")

    def text(self) -> str:
        lines: list[str] = []
        for name, (mtype, help_, samples) in self.families.items():
            lines += [f"# HELP {name} {help_}", f"# TYPE {name} {mtype}", *samples]
        return "\n".join(lines) + "\n"


def _k8s_labels(g: GpuState) -> dict[str, str]:
    return {"pod": g.pod, "namespace": g.namespace, "container": g.container} if g.pod else {}


def render_nvidia(gpus: Sequence[GpuState], node: str) -> str:
    w = _Writer()
    prof = PROFILES["nvidia"]
    for g in gpus:
        lb = {
            "gpu": str(g.index),
            "UUID": g.uuid,
            "device": f"nvidia{g.index}",
            "modelName": prof.model,
            "Hostname": node,
            **_k8s_labels(g),
        }
        bitmask = sum(1 << THROTTLE_BITS[r] for r in g.throttle_reasons)
        w.add("DCGM_FI_DEV_GPU_UTIL", "gauge", "GPU utilization (in %).", lb, round(g.busy_percent))
        w.add(
            "DCGM_FI_PROF_SM_ACTIVE",
            "gauge",
            "Ratio of cycles an SM has at least 1 warp.",
            lb,
            g.compute_active_percent / 100,
        )
        w.add(
            "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE",
            "gauge",
            "Ratio of cycles the tensor pipe is active.",
            lb,
            g.matrix_active_percent / 100,
        )
        w.add(
            "DCGM_FI_PROF_DRAM_ACTIVE",
            "gauge",
            "Ratio of cycles the memory interface is active.",
            lb,
            g.mem_bw_active_percent / 100,
        )
        w.add("DCGM_FI_DEV_FB_USED", "gauge", "Framebuffer memory used (in MiB).", lb, g.mem_used_bytes // MIB)
        w.add(
            "DCGM_FI_DEV_FB_TOTAL",
            "gauge",
            "Total framebuffer memory (in MiB).",
            lb,
            g.mem_total_bytes // MIB,
        )
        w.add("DCGM_FI_DEV_POWER_USAGE", "gauge", "Power draw (in W).", lb, g.power_watts)
        w.add(
            "DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION",
            "counter",
            "Total energy since boot (in mJ).",
            lb,
            round(g.energy_joules * 1000),
        )
        w.add("DCGM_FI_DEV_GPU_TEMP", "gauge", "GPU temperature (in C).", lb, g.temp_celsius)
        w.add("DCGM_FI_DEV_MEMORY_TEMP", "gauge", "Memory temperature (in C).", lb, g.mem_temp_celsius)
        w.add("DCGM_FI_DEV_SM_CLOCK", "gauge", "SM clock frequency (in MHz).", lb, g.sm_clock_mhz)
        w.add("DCGM_FI_DEV_CLOCKS_EVENT_REASONS", "gauge", "Current clock event reasons (bitmask).", lb, bitmask)
        w.add(
            "DCGM_FI_DEV_ECC_SBE_VOL_TOTAL",
            "counter",
            "Total single-bit volatile ECC errors.",
            lb,
            g.ecc_correctable,
        )
        w.add(
            "DCGM_FI_DEV_ECC_DBE_VOL_TOTAL",
            "counter",
            "Total double-bit volatile ECC errors.",
            lb,
            g.ecc_uncorrectable,
        )
        w.add("DCGM_FI_DEV_XID_ERRORS", "gauge", "Value of the last XID error encountered.", lb, g.last_xid)
        w.add(
            "DCGM_FI_DEV_ROW_REMAP_FAILURE",
            "gauge",
            "Whether remapping of rows has failed.",
            lb,
            g.row_remap_failed,
        )
        w.add("DCGM_FI_PROF_PCIE_TX_BYTES", "gauge", "PCIe tx bytes per second.", lb, g.pcie_tx_bps)
        w.add("DCGM_FI_PROF_PCIE_RX_BYTES", "gauge", "PCIe rx bytes per second.", lb, g.pcie_rx_bps)
        w.add("DCGM_FI_DEV_PCIE_REPLAY_COUNTER", "counter", "Total number of PCIe retries.", lb, g.pcie_replays)
        w.add("DCGM_FI_PROF_NVLINK_TX_BYTES", "gauge", "NVLink tx bytes per second.", lb, g.link_tx_bps)
        w.add("DCGM_FI_PROF_NVLINK_RX_BYTES", "gauge", "NVLink rx bytes per second.", lb, g.link_rx_bps)
    return w.text()


def render_amd(gpus: Sequence[GpuState], node: str) -> str:
    w = _Writer()
    prof = PROFILES["amd"]
    for g in gpus:
        lb = {
            "gpu_id": str(g.index),
            "gpu_uuid": g.uuid,
            "card_model": prof.model,
            "hostname": node,
            **_k8s_labels(g),
        }
        w.add("gpu_gfx_activity", "gauge", "Graphics engine activity (%).", lb, g.compute_active_percent)
        w.add("gpu_mma_activity", "gauge", "Matrix (MMA) engine activity (%).", lb, g.matrix_active_percent)
        w.add("gpu_umc_activity", "gauge", "Memory controller activity (%).", lb, g.mem_bw_active_percent)
        w.add("gpu_used_vram", "gauge", "Used VRAM (MiB).", lb, g.mem_used_bytes // MIB)
        w.add("gpu_total_vram", "gauge", "Total VRAM (MiB).", lb, g.mem_total_bytes // MIB)
        w.add("gpu_power_usage", "gauge", "Power usage (W).", lb, g.power_watts)
        w.add("gpu_energy_consumed", "counter", "Energy consumed (J).", lb, g.energy_joules)
        w.add("gpu_junction_temperature", "gauge", "Junction temperature (C).", lb, g.temp_celsius)
        w.add("gpu_memory_temperature", "gauge", "Memory temperature (C).", lb, g.mem_temp_celsius)
        w.add(
            "gpu_clock",
            "gauge",
            "Clock (MHz).",
            {**lb, "clock_type": "GPU_CLOCK_TYPE_SYSTEM"},
            g.sm_clock_mhz,
        )
        w.add("gpu_clock", "gauge", "Clock (MHz).", {**lb, "clock_type": "GPU_CLOCK_TYPE_MEMORY"}, 1300.0)
        w.add("gpu_ecc_correct_total", "counter", "Correctable ECC errors.", lb, g.ecc_correctable)
        w.add("gpu_ecc_uncorrect_total", "counter", "Uncorrectable ECC errors.", lb, g.ecc_uncorrectable)
        w.add("pcie_replay_count", "counter", "PCIe replays.", lb, g.pcie_replays)
        w.add("gpu_xgmi_link_tx", "gauge", "xGMI transmit (bytes/s, all links).", lb, g.link_tx_bps)
        w.add("gpu_xgmi_link_rx", "gauge", "xGMI receive (bytes/s, all links).", lb, g.link_rx_bps)
    return w.text()


def render(vendor: Vendor, gpus: Sequence[GpuState], node: str) -> str:
    return render_nvidia(gpus, node) if vendor == "nvidia" else render_amd(gpus, node)


# --- optional k8s pod mapping (in-cluster) --------------------------------------------------------

_SA = "/var/run/secrets/kubernetes.io/serviceaccount"


def fetch_gpu_pods(node: str, vendor: Vendor) -> list[tuple[str, str, str, int, str]]:
    """Pods on this node that request the vendor GPU resource, oldest first."""
    import httpx

    resource = "nvidia.com/gpu" if vendor == "nvidia" else "amd.com/gpu"
    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    with open(f"{_SA}/token", encoding="utf-8") as fh:
        token = fh.read().strip()
    resp = httpx.get(
        f"https://{host}:{port}/api/v1/pods",
        params={"fieldSelector": f"spec.nodeName={node},status.phase=Running"},
        headers={"Authorization": f"Bearer {token}"},
        verify=f"{_SA}/ca.crt",
        timeout=5.0,
    )
    resp.raise_for_status()
    items = sorted(resp.json()["items"], key=lambda p: p["metadata"]["creationTimestamp"])
    out: list[tuple[str, str, str, int, str]] = []
    for pod in items:
        ip = pod.get("status", {}).get("podIP", "")
        for c in pod["spec"]["containers"]:
            count = int((c.get("resources", {}).get("limits") or {}).get(resource, 0))
            if count:
                out.append((pod["metadata"]["name"], pod["metadata"]["namespace"], c["name"], count, ip))
    return out


def fetch_pod_load(ip: str, port: int = 8000) -> float | None:
    """Engine load (0..1) from the simulated engine's /sim/state; None if unreachable."""
    import httpx

    try:
        state = httpx.get(f"http://{ip}:{port}/sim/state", timeout=1.0).json()
        return float(state["load"])
    except Exception:
        return None


# --- server ---------------------------------------------------------------------------------------


def kernel_log_lines(vendor: Vendor, scenario: str, gpu: GpuState) -> list[str]:
    """Kernel-style lines a real node would log for a fault scenario (collected by the agent's filelog).

    NVIDIA: NVRM Xid lines. AMD: amdgpu RAS / reset lines (amdgpu has no Xid; its fatal signal is RAS).
    """
    pci = f"0000:{0x1B + gpu.index * 0x10:02x}:00.0"
    if vendor == "nvidia":
        if scenario == "xid":
            return [f"NVRM: Xid (PCI:{pci}): 79, pid='<unknown>', name=<unknown>, GPU has fallen off the bus."]
        if scenario == "ecc":
            return [f"NVRM: Xid (PCI:{pci}): 48, pid=1234, name=python3, An uncorrectable double bit error"]
        return []
    if scenario == "xid":
        return [
            f"amdgpu {pci}: amdgpu: GPU reset begin!",
            f"amdgpu {pci}: amdgpu: RAS: uncorrectable error in UMC, GPU reset required",
        ]
    if scenario == "ecc":
        return [f"amdgpu {pci}: amdgpu: RAS: 1 uncorrectable hardware error detected in UMC block"]
    return []


class Simulator:
    LOG_EVERY_S = 30.0

    def __init__(self, vendor: Vendor, node: str, scenario: str, seed: int, use_k8s: bool) -> None:
        self.vendor, self.node, self.scenario, self.use_k8s = vendor, node, scenario, use_k8s
        self.gpus = new_gpus(vendor, node)
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.last = time.monotonic()
        self.last_log = 0.0

    def scrape(self) -> str:
        with self.lock:
            now = time.monotonic()
            load: dict[int, float] | None = None
            if self.use_k8s:
                load = {}
                try:
                    pods = fetch_gpu_pods(self.node, self.vendor)
                    assign_gpus(self.gpus, [p[:4] for p in pods])
                    pod_load = {name: fetch_pod_load(ip) for name, _, _, _, ip in pods if ip}
                    for g in self.gpus:
                        if g.pod:
                            load[g.index] = pod_load.get(g.pod) or 0.0
                except Exception as exc:  # keep serving metrics if the API is unreachable
                    print(f"k8s pod mapping failed: {exc}", flush=True)
            step(self.gpus, self.vendor, self.scenario, now - self.last, self.rng, load)
            self.last = now
            if now - self.last_log >= self.LOG_EVERY_S:
                for line in kernel_log_lines(self.vendor, self.scenario, self.gpus[0]):
                    print(line, flush=True)
                self.last_log = now
            return render(self.vendor, self.gpus, self.node)


def serve(sim: Simulator, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.split("?")[0] != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            body = sim.scrape().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    return ThreadingHTTPServer(("0.0.0.0", port), Handler)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--vendor", choices=["nvidia", "amd"], required=True)
    p.add_argument("--port", type=int, default=9400)
    p.add_argument("--node", default=os.environ.get("NODE_NAME", "sim-node-0"))
    p.add_argument("--scenario", choices=SCENARIOS, default=os.environ.get("SCENARIO", "load"))
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--k8s", action="store_true", help="map GPUs to pods using the in-cluster k8s API")
    a = p.parse_args(argv)
    sim = Simulator(a.vendor, a.node, a.scenario, a.seed, a.k8s)
    server = serve(sim, a.port)
    print(
        f"fake {a.vendor} GPU exporter on :{a.port}/metrics (node={a.node}, scenario={a.scenario})",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
