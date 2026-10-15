-- GPU fatal events and collective-library errors from logs (MVP.md §3.1, §3.3).
-- NVIDIA: "NVRM: Xid (PCI:<bus>): <code>, ..." (also exposed as a DCGM metric).
-- AMD:    amdgpu RAS / GPU reset lines — AMD has no Xid, so for MI300X this table is the only fatal-event
--         signal. Lines arrive from the agent's filtered filelog (pods) or journald (kernel) receivers.

CREATE TABLE IF NOT EXISTS gpu_fatal_events
(
    `ts` DateTime64(3),
    `node` LowCardinality(String),
    `vendor` LowCardinality(String),
    `kind` LowCardinality(String),
    `code` UInt16,
    `pci_bus` LowCardinality(String),
    `critical` UInt8,
    `message` String,
    `source_pod` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (node, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS gpu_fatal_events_mv TO gpu_fatal_events AS
SELECT
    toDateTime64(Timestamp, 3) AS ts,
    ResourceAttributes['k8s.node.name'] AS node,
    if(Body LIKE '%NVRM: Xid%', 'nvidia', 'amd') AS vendor,
    multiIf(
        Body LIKE '%NVRM: Xid%', 'xid',
        Body ILIKE '%RAS%uncorrectable%', 'ras_uncorrectable',
        Body ILIKE '%GPU reset%', 'gpu_reset',
        Body ILIKE '%RAS%', 'ras',
        'other') AS kind,
    toUInt16OrZero(extract(Body, 'Xid \\([^)]*\\): (\\d+)')) AS code,
    if(vendor = 'nvidia', extract(Body, 'PCI:([0-9a-fA-F:.]+)\\)'), extract(Body, 'amdgpu ([0-9a-fA-F:.]+):')) AS pci_bus,
    toUInt8(
        (kind = 'xid' AND code IN (48, 63, 64, 74, 79, 92, 94, 95, 119, 120))
        OR kind IN ('ras_uncorrectable', 'gpu_reset')) AS critical,
    Body AS message,
    ResourceAttributes['k8s.pod.name'] AS source_pod
FROM otel_logs
WHERE Body LIKE '%NVRM: Xid%' OR match(Body, '(?i)amdgpu.*(RAS|GPU reset)');

-- NCCL / RCCL collective errors (TP all-reduce hangs show up here first).
CREATE TABLE IF NOT EXISTS collective_errors
(
    `ts` DateTime64(3),
    `node` LowCardinality(String),
    `pod` LowCardinality(String),
    `library` LowCardinality(String),
    `message` String
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (node, pod, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS collective_errors_mv TO collective_errors AS
SELECT
    toDateTime64(Timestamp, 3) AS ts,
    ResourceAttributes['k8s.node.name'] AS node,
    ResourceAttributes['k8s.pod.name'] AS pod,
    if(Body ILIKE '%RCCL%', 'RCCL', 'NCCL') AS library,
    Body AS message
FROM otel_logs
WHERE match(Body, '(?i)(NCCL|RCCL) (WARN|error)');
