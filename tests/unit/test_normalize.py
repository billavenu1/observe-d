from __future__ import annotations

import random
import re
from pathlib import Path

import pytest
import yaml
from prometheus_client.parser import text_string_to_metric_families

from normalize.apply import Sample, normalize
from normalize.generate import ROOT, collector_processors, main, prometheus_rules, render_all
from normalize.schema import MappingError, load_mapping
from sim.fake_gpu_exporter import MIB, new_gpus, render, step

GPU_YAML = ROOT / "normalize" / "gpu.yaml"


@pytest.fixture(scope="module")
def mapping():  # type: ignore[no-untyped-def]
    return load_mapping(GPU_YAML)


_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)$")
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse(text: str) -> list[Sample]:
    """Parse exposition text keeping sample names exactly as exposed (as the Prometheus server does).

    prometheus_client's parser appends `_total` to counters, which real scrapes do not.
    """
    families = list(text_string_to_metric_families(text))  # validates the exposition format
    assert families
    out: list[Sample] = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _LINE.match(line)
        assert m, line
        labels = dict(_LABEL.findall(m.group(2) or ""))
        out.append(Sample.of(m.group(1), labels, float(m.group(3))))
    return out


def by_key(samples: list[Sample]) -> dict[tuple[str, str, str], float]:
    """(normalized name, gpu_index, reason) -> value"""
    return {(s.name, s.label_dict["gpu_index"], s.label_dict.get("reason", "")): s.value for s in samples}


def shared_state_render(vendor: str) -> str:
    """Render both vendors from an identical GPU state (memory size forced equal)."""
    gpus = new_gpus("nvidia", "node-a")
    step(gpus, "nvidia", "load", 10.0, random.Random(7))
    step(gpus, "nvidia", "throttle", 10.0, random.Random(8))
    return render(vendor, gpus, "node-a")  # type: ignore[arg-type]


# --- mapping file -------------------------------------------------------------------------------


def test_every_metric_declares_every_vendor(mapping) -> None:  # type: ignore[no-untyped-def]
    assert mapping.vendors == ("nvidia", "amd")
    for m in mapping.metrics:
        assert set(m.sources) == {"nvidia", "amd"}, m.name


def test_missing_vendor_entry_is_rejected(tmp_path: Path) -> None:
    doc = yaml.safe_load(GPU_YAML.read_text(encoding="utf-8"))
    del doc["metrics"]["power_watts"]["amd"]
    bad = tmp_path / "bad.yaml"
    bad.write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(MappingError, match="power_watts"):
        load_mapping(bad)


def test_bitmask_requires_reasons(tmp_path: Path) -> None:
    doc = yaml.safe_load(GPU_YAML.read_text(encoding="utf-8"))
    del doc["metrics"]["throttle_active"]["nvidia"]["reasons"]
    bad = tmp_path / "bad.yaml"
    bad.write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(MappingError, match="reasons"):
        load_mapping(bad)


# --- simulator -> mapping -> normalized values -------------------------------------------------------


@pytest.mark.parametrize("vendor", ["nvidia", "amd"])
def test_units_are_normalized(mapping, vendor: str) -> None:  # type: ignore[no-untyped-def]
    gpus = new_gpus(vendor, "node-a")  # type: ignore[arg-type]
    step(gpus, vendor, "load", 100.0, random.Random(1))  # type: ignore[arg-type]
    got = by_key(normalize(parse(render(vendor, gpus, "node-a")), mapping, vendor))  # type: ignore[arg-type]
    g = gpus[3]
    idx = "3"
    assert got[("gpu:mem_used_bytes", idx, "")] == pytest.approx(g.mem_used_bytes // MIB * MIB)
    assert got[("gpu:mem_total_bytes", idx, "")] == g.mem_total_bytes
    assert got[("gpu:compute_active_percent", idx, "")] == pytest.approx(g.compute_active_percent)
    assert got[("gpu:energy_joules_total", idx, "")] == pytest.approx(g.energy_joules, rel=1e-3)
    assert got[("gpu:power_watts", idx, "")] == pytest.approx(g.power_watts)
    assert got[("gpu:sm_clock_mhz", idx, "")] == pytest.approx(g.sm_clock_mhz)


@pytest.mark.parametrize("vendor", ["nvidia", "amd"])
def test_normalized_labels(mapping, vendor: str) -> None:  # type: ignore[no-untyped-def]
    gpus = new_gpus(vendor, "node-a")  # type: ignore[arg-type]
    gpus[0].pod, gpus[0].namespace, gpus[0].container = "vllm-0", "llm", "vllm"
    out = normalize(parse(render(vendor, gpus, "node-a")), mapping, vendor)  # type: ignore[arg-type]
    labels = next(s.label_dict for s in out if s.name == "gpu:power_watts" and s.label_dict["gpu_index"] == "0")
    assert labels["vendor"] == vendor
    assert labels["node"] == "node-a"
    assert labels["gpu_uuid"] == gpus[0].uuid
    assert (labels["pod"], labels["namespace"], labels["container"]) == ("vllm-0", "llm", "vllm")


def test_selector_picks_only_the_system_clock(mapping) -> None:  # type: ignore[no-untyped-def]
    out = normalize(parse(shared_state_render("amd")), mapping, "amd")
    clocks = [s for s in out if s.name == "gpu:sm_clock_mhz"]
    assert len(clocks) == 8  # memory-clock series excluded


def test_nvidia_and_amd_agree_on_shared_metrics(mapping) -> None:  # type: ignore[no-untyped-def]
    nv = by_key(normalize(parse(shared_state_render("nvidia")), mapping, "nvidia"))
    amd = by_key(normalize(parse(shared_state_render("amd")), mapping, "amd"))
    comparable = {
        mapping.prom_name(m)
        for m in mapping.metrics
        if all(s is not None and not s.approximate for s in m.sources.values())
    }
    assert comparable, "no comparable metrics"
    checked = 0
    for key, value in nv.items():
        if key[0] in comparable:
            assert amd[key] == pytest.approx(value, rel=1e-3), key
            checked += 1
    assert checked == 8 * len(comparable)


def test_bitmask_expands_to_reasons(mapping) -> None:  # type: ignore[no-untyped-def]
    got = by_key(normalize(parse(shared_state_render("nvidia")), mapping, "nvidia"))
    assert got[("gpu:throttle_active", "0", "hw_thermal")] == 1
    assert got[("gpu:throttle_active", "0", "sw_thermal")] == 1
    assert got[("gpu:throttle_active", "0", "sw_power_cap")] == 0
    assert got[("gpu:throttle_active", "1", "hw_thermal")] == 0


def test_vendor_null_produces_no_series(mapping) -> None:  # type: ignore[no-untyped-def]
    out = normalize(parse(shared_state_render("amd")), mapping, "amd")
    names = {s.name for s in out}
    assert "gpu:throttle_active" not in names
    assert "gpu:last_fatal_event_code" not in names


# --- generated artifacts ------------------------------------------------------------------------


def test_prometheus_rules_shape(mapping) -> None:  # type: ignore[no-untyped-def]
    rules = prometheus_rules(mapping)["spec"]["groups"][0]["rules"]
    sm = [r for r in rules if r["record"] == "gpu:compute_active_percent"]
    assert {r["labels"]["vendor"] for r in sm} == {"nvidia", "amd"}
    nv = next(r for r in sm if r["labels"]["vendor"] == "nvidia")
    assert "DCGM_FI_PROF_SM_ACTIVE * 100" in nv["expr"]
    assert '"gpu_uuid", "$1", "UUID"' in nv["expr"]
    throttle = [r for r in rules if r["record"] == "gpu:throttle_active"]
    assert len(throttle) == 8
    assert any("floor(DCGM_FI_DEV_CLOCKS_EVENT_REASONS / 64) % 2" in r["expr"] for r in throttle)
    amd_clock = next(r for r in rules if r["record"] == "gpu:sm_clock_mhz" and r["labels"]["vendor"] == "amd")
    assert 'gpu_clock{clock_type="GPU_CLOCK_TYPE_SYSTEM"}' in amd_clock["expr"]


def test_collector_processors_shape(mapping) -> None:  # type: ignore[no-untyped-def]
    procs = collector_processors(mapping)["processors"]
    transforms = procs["metrics_transform/gpu_normalize"]["transforms"]
    assert all(t["action"] == "insert" for t in transforms)
    gfx = [t for t in transforms if t["include"] == "gpu_gfx_activity"]
    assert {t["new_name"] for t in gfx} == {"gpu.busy_percent", "gpu.compute_active_percent"}
    bitmask = next(t for t in transforms if t["include"] == "DCGM_FI_DEV_CLOCKS_EVENT_REASONS")
    assert bitmask["new_name"] == "gpu.throttle_active_bitmask"
    energy = next(t for t in transforms if t["include"] == "DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION")
    assert {"action": "experimental_scale_value", "experimental_scale": 0.001} in energy["operations"]
    assert "filter/gpu_subset" in procs


def test_generation_is_deterministic() -> None:
    assert render_all() == render_all()


def test_generated_files_are_current() -> None:
    assert main(["--check"]) == 0, "run: uv run python -m normalize.generate"
