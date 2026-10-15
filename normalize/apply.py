"""Reference implementation of the normalization semantics (what the generated Prometheus rules compute).

Used by tests to check that a vendor exporter's output, run through the mapping, yields the expected
normalized values — and that NVIDIA and AMD produce the same thing for the same GPU state.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass

from normalize.schema import Mapping


@dataclass(frozen=True)
class Sample:
    name: str
    labels: tuple[tuple[str, str], ...]
    value: float

    @classmethod
    def of(cls, name: str, labels: dict[str, str], value: float) -> Sample:
        return cls(name, tuple(sorted(labels.items())), value)

    @property
    def label_dict(self) -> dict[str, str]:
        return dict(self.labels)


def normalize(samples: Iterable[Sample], mapping: Mapping, vendor: str) -> list[Sample]:
    """Return normalized samples. Only mapped labels survive (plus the discriminator and `reason`)."""
    by_name: dict[str, list[Sample]] = {}
    for s in samples:
        by_name.setdefault(s.name, []).append(s)

    label_map = mapping.labels[vendor]
    out: list[Sample] = []
    for metric in mapping.metrics:
        source = metric.sources.get(vendor)
        if source is None:
            continue
        for s in by_name.get(source.metric, []):
            src_labels = s.label_dict
            if any(src_labels.get(k) != v for k, v in source.selector.items()):
                continue
            labels = {dst: src_labels[src] for src, dst in label_map.items() if src in src_labels}
            labels[mapping.discriminator_label] = vendor
            if source.kind == "bitmask":
                for bit, reason in source.reasons.items():
                    bit_value = math.floor(s.value / 2**bit) % 2
                    out.append(Sample.of(mapping.prom_name(metric), {**labels, "reason": reason}, bit_value))
            else:
                out.append(Sample.of(mapping.prom_name(metric), labels, s.value * source.scale))
    return out
