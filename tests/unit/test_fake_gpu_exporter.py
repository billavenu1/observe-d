from __future__ import annotations

import random
import threading
import urllib.request

import pytest

from sim.fake_gpu_exporter import Simulator, assign_gpus, new_gpus, serve, step


def test_energy_is_monotonic_and_follows_power() -> None:
    gpus = new_gpus("nvidia", "n")
    rng = random.Random(0)
    step(gpus, "nvidia", "load", 10.0, rng)
    first = gpus[0].energy_joules
    assert first == pytest.approx(gpus[0].power_watts * 10.0)
    step(gpus, "nvidia", "idle", 5.0, rng)
    assert gpus[0].energy_joules > first


@pytest.mark.parametrize(
    ("scenario", "check"),
    [
        ("throttle", lambda g: "hw_thermal" in g.throttle_reasons and g.temp_celsius > 90),
        ("xid", lambda g: g.last_xid == 79),
        ("ecc", lambda g: g.ecc_uncorrectable >= 1 and g.row_remap_failed == 1),
        ("link_errors", lambda g: g.pcie_replays > 0),
    ],
)
def test_fault_scenarios_hit_gpu0_only(scenario: str, check) -> None:  # type: ignore[no-untyped-def]
    gpus = new_gpus("amd", "n")
    step(gpus, "amd", scenario, 1.0, random.Random(0))
    assert check(gpus[0])
    assert not any(check(g) for g in gpus[1:])


def test_unknown_scenario_rejected() -> None:
    with pytest.raises(ValueError):
        step(new_gpus("amd", "n"), "amd", "meltdown", 1.0, random.Random(0))


def test_vendor_profiles() -> None:
    assert new_gpus("amd", "n")[0].mem_total_bytes == 192 * 1024**3
    assert new_gpus("nvidia", "n")[0].mem_total_bytes == 80 * 1024**3


def test_assign_gpus_in_order_and_caps_at_8() -> None:
    gpus = new_gpus("nvidia", "n")
    assign_gpus(gpus, [("a", "ns", "c", 4), ("b", "ns", "c", 2), ("c", "ns", "c", 4)])
    assert [g.pod for g in gpus] == ["a"] * 4 + ["b"] * 2 + ["c"] * 2
    assign_gpus(gpus, [])
    assert all(g.pod == "" for g in gpus)


@pytest.mark.parametrize(("vendor", "family"), [("nvidia", "DCGM_FI_PROF_SM_ACTIVE"), ("amd", "gpu_gfx_activity")])
def test_http_endpoint(vendor: str, family: str) -> None:
    server = serve(Simulator(vendor, "n", "load", 0, use_k8s=False), 0)  # type: ignore[arg-type]
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            body = resp.read().decode()
        assert resp.status == 200
        assert f"# TYPE {family} gauge" in body
        assert body.count(f"{family}{{") == 8
    finally:
        server.shutdown()


def test_load_coupling_idles_unallocated_gpus() -> None:
    gpus = new_gpus("amd", "n")
    step(gpus, "amd", "load", 1.0, random.Random(0), load={0: 1.0, 1: 0.0})
    assert gpus[0].compute_active_percent > 80
    assert gpus[1].compute_active_percent < 15 and gpus[1].mem_used_bytes > 0  # allocated, idle
    assert gpus[2].compute_active_percent == 0 and gpus[2].mem_used_bytes == 0  # unallocated


@pytest.mark.parametrize(
    ("vendor", "scenario", "needle"),
    [("nvidia", "xid", "NVRM: Xid"), ("nvidia", "ecc", "Xid"), ("amd", "xid", "RAS"), ("amd", "ecc", "RAS")],
)
def test_kernel_log_lines_for_faults(vendor: str, scenario: str, needle: str) -> None:
    from sim.fake_gpu_exporter import kernel_log_lines

    lines = kernel_log_lines(vendor, scenario, new_gpus(vendor, "n")[0])  # type: ignore[arg-type]
    assert lines and all(needle in line or "reset" in line for line in lines)
    assert kernel_log_lines(vendor, "load", new_gpus(vendor, "n")[0]) == []  # type: ignore[arg-type]
