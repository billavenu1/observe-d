"""Load and validate a normalization mapping file (e.g. normalize/gpu.yaml)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_TYPES = ("gauge", "counter", "histogram")


class MappingError(ValueError):
    pass


@dataclass(frozen=True)
class Source:
    metric: str
    scale: float = 1.0
    selector: dict[str, str] = field(default_factory=dict)
    kind: Literal["value", "bitmask", "histogram"] = "value"
    reasons: dict[int, str] = field(default_factory=dict)
    approximate: bool = False
    verified: bool = False


@dataclass(frozen=True)
class Metric:
    name: str
    type: Literal["gauge", "counter", "histogram"]
    unit: str
    description: str
    sources: dict[str, Source | None]
    clickhouse: bool = True  # include in the collector subset shipped to ClickHouse


@dataclass(frozen=True)
class Mapping:
    namespace: str
    discriminator_label: str
    vendors: tuple[str, ...]
    labels: dict[str, dict[str, str]]
    metrics: tuple[Metric, ...]

    def prom_name(self, metric: Metric) -> str:
        return f"{self.namespace}:{metric.name}"

    def otel_name(self, metric: Metric) -> str:
        return f"{self.namespace}.{metric.name}"


def _parse_source(metric: str, vendor: str, raw: Any) -> Source | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or "metric" not in raw:
        raise MappingError(f"{metric}.{vendor}: expected a mapping with 'metric' or null")
    kind = raw.get("kind", "value")
    if kind not in ("value", "bitmask", "histogram"):
        raise MappingError(f"{metric}.{vendor}: unknown kind {kind!r}")
    reasons = {int(bit): str(reason) for bit, reason in (raw.get("reasons") or {}).items()}
    if kind == "bitmask" and not reasons:
        raise MappingError(f"{metric}.{vendor}: bitmask source needs 'reasons'")
    if kind != "bitmask" and reasons:
        raise MappingError(f"{metric}.{vendor}: 'reasons' only valid for kind: bitmask")
    unknown = set(raw) - {"metric", "scale", "selector", "kind", "reasons", "approximate", "verified"}
    if unknown:
        raise MappingError(f"{metric}.{vendor}: unknown keys {sorted(unknown)}")
    return Source(
        metric=str(raw["metric"]),
        scale=float(raw.get("scale", 1.0)),
        selector={str(k): str(v) for k, v in (raw.get("selector") or {}).items()},
        kind=kind,
        reasons=reasons,
        approximate=bool(raw.get("approximate", False)),
        verified=bool(raw.get("verified", False)),
    )


def load_mapping(path: Path) -> Mapping:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise MappingError(f"{path}: not a mapping")

    vendors = tuple(str(v) for v in doc["vendors"])
    labels = {str(v): {str(k): str(n) for k, n in m.items()} for v, m in doc["labels"].items()}
    if set(labels) != set(vendors):
        raise MappingError(f"labels must define exactly the vendors {vendors}")

    metrics: list[Metric] = []
    for name, spec in doc["metrics"].items():
        if not _NAME_RE.match(name):
            raise MappingError(f"metric name {name!r} must be snake_case")
        if spec.get("type") not in _TYPES:
            raise MappingError(f"{name}: type must be one of {_TYPES}")
        missing = [v for v in vendors if v not in spec]
        if missing:
            raise MappingError(f"{name}: no entry for {missing} (use null when the vendor has no equivalent)")
        sources = {v: _parse_source(name, v, spec[v]) for v in vendors}
        if all(s is None for s in sources.values()):
            raise MappingError(f"{name}: at least one vendor needs a source")
        if any(s is not None and s.kind == "histogram" for s in sources.values()) and spec["type"] != "histogram":
            raise MappingError(f"{name}: histogram sources need type: histogram")
        metrics.append(
            Metric(
                name=name,
                type=spec["type"],
                unit=str(spec.get("unit", "")),
                description=str(spec.get("description", "")),
                sources=sources,
                clickhouse=bool(spec.get("clickhouse", True)),
            )
        )

    return Mapping(
        namespace=str(doc["namespace"]),
        discriminator_label=str(doc["discriminator_label"]),
        vendors=vendors,
        labels=labels,
        metrics=tuple(metrics),
    )
