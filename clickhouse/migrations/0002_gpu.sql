-- GPU layer (MVP.md §3.1). Source: normalized gpu.* metrics (normalize/gpu.yaml) shipped by the agent.
-- gpu_samples pivots one row per (GPU, 5 s) with every normalized field as a column, so requests can be
-- joined to "GPU state at that moment" with ASOF / window joins. NULL = vendor has no equivalent.

CREATE TABLE IF NOT EXISTS gpu_samples
(
    `ts` DateTime CODEC(Delta(4), ZSTD(1)),
    `vendor` LowCardinality(String),
    `node` LowCardinality(String),
    `gpu_uuid` LowCardinality(String),
    `gpu_index` SimpleAggregateFunction(anyLast, LowCardinality(String)),
    `gpu_model` SimpleAggregateFunction(anyLast, LowCardinality(String)),
    `pod` SimpleAggregateFunction(anyLast, LowCardinality(String)),
    `namespace` SimpleAggregateFunction(anyLast, LowCardinality(String)),
    `busy_percent` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `compute_active_percent` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `matrix_active_percent` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `mem_bw_active_percent` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `mem_used_bytes` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `mem_total_bytes` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `power_watts` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `energy_joules_total` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `temp_celsius` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `mem_temp_celsius` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `sm_clock_mhz` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `throttle_bitmask` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `ecc_correctable_total` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `ecc_uncorrectable_total` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `last_fatal_event_code` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `row_remap_failed` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `pcie_tx_bytes_per_second` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `pcie_rx_bytes_per_second` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `pcie_replay_total` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `link_tx_bytes_per_second` SimpleAggregateFunction(anyLast, Nullable(Float64)),
    `link_rx_bytes_per_second` SimpleAggregateFunction(anyLast, Nullable(Float64))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(ts)
ORDER BY (vendor, node, gpu_uuid, ts)
TTL ts + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS gpu_samples_gauge_mv TO gpu_samples AS
SELECT
    toDateTime(toStartOfInterval(TimeUnix, INTERVAL 5 SECOND)) AS ts,
    Attributes['vendor'] AS vendor,
    Attributes['node'] AS node,
    Attributes['gpu_uuid'] AS gpu_uuid,
    Attributes['gpu_index'] AS gpu_index,
    Attributes['gpu_model'] AS gpu_model,
    Attributes['pod'] AS pod,
    Attributes['namespace'] AS namespace,
    if(MetricName = 'gpu.busy_percent', Value, NULL) AS busy_percent,
    if(MetricName = 'gpu.compute_active_percent', Value, NULL) AS compute_active_percent,
    if(MetricName = 'gpu.matrix_active_percent', Value, NULL) AS matrix_active_percent,
    if(MetricName = 'gpu.mem_bw_active_percent', Value, NULL) AS mem_bw_active_percent,
    if(MetricName = 'gpu.mem_used_bytes', Value, NULL) AS mem_used_bytes,
    if(MetricName = 'gpu.mem_total_bytes', Value, NULL) AS mem_total_bytes,
    if(MetricName = 'gpu.power_watts', Value, NULL) AS power_watts,
    CAST(NULL, 'Nullable(Float64)') AS energy_joules_total,
    if(MetricName = 'gpu.temp_celsius', Value, NULL) AS temp_celsius,
    if(MetricName = 'gpu.mem_temp_celsius', Value, NULL) AS mem_temp_celsius,
    if(MetricName = 'gpu.sm_clock_mhz', Value, NULL) AS sm_clock_mhz,
    if(MetricName = 'gpu.throttle_active_bitmask', Value, NULL) AS throttle_bitmask,
    CAST(NULL, 'Nullable(Float64)') AS ecc_correctable_total,
    CAST(NULL, 'Nullable(Float64)') AS ecc_uncorrectable_total,
    if(MetricName = 'gpu.last_fatal_event_code', Value, NULL) AS last_fatal_event_code,
    if(MetricName = 'gpu.row_remap_failed', Value, NULL) AS row_remap_failed,
    if(MetricName = 'gpu.pcie_tx_bytes_per_second', Value, NULL) AS pcie_tx_bytes_per_second,
    if(MetricName = 'gpu.pcie_rx_bytes_per_second', Value, NULL) AS pcie_rx_bytes_per_second,
    CAST(NULL, 'Nullable(Float64)') AS pcie_replay_total,
    if(MetricName = 'gpu.link_tx_bytes_per_second', Value, NULL) AS link_tx_bytes_per_second,
    if(MetricName = 'gpu.link_rx_bytes_per_second', Value, NULL) AS link_rx_bytes_per_second
FROM otel_metrics_gauge
WHERE MetricName LIKE 'gpu.%';

-- Prometheus counters arrive as cumulative sums.
CREATE MATERIALIZED VIEW IF NOT EXISTS gpu_samples_sum_mv TO gpu_samples AS
SELECT
    toDateTime(toStartOfInterval(TimeUnix, INTERVAL 5 SECOND)) AS ts,
    Attributes['vendor'] AS vendor,
    Attributes['node'] AS node,
    Attributes['gpu_uuid'] AS gpu_uuid,
    Attributes['gpu_index'] AS gpu_index,
    Attributes['gpu_model'] AS gpu_model,
    Attributes['pod'] AS pod,
    Attributes['namespace'] AS namespace,
    CAST(NULL, 'Nullable(Float64)') AS busy_percent,
    CAST(NULL, 'Nullable(Float64)') AS compute_active_percent,
    CAST(NULL, 'Nullable(Float64)') AS matrix_active_percent,
    CAST(NULL, 'Nullable(Float64)') AS mem_bw_active_percent,
    CAST(NULL, 'Nullable(Float64)') AS mem_used_bytes,
    CAST(NULL, 'Nullable(Float64)') AS mem_total_bytes,
    CAST(NULL, 'Nullable(Float64)') AS power_watts,
    if(MetricName = 'gpu.energy_joules_total', Value, NULL) AS energy_joules_total,
    CAST(NULL, 'Nullable(Float64)') AS temp_celsius,
    CAST(NULL, 'Nullable(Float64)') AS mem_temp_celsius,
    CAST(NULL, 'Nullable(Float64)') AS sm_clock_mhz,
    CAST(NULL, 'Nullable(Float64)') AS throttle_bitmask,
    if(MetricName = 'gpu.ecc_correctable_total', Value, NULL) AS ecc_correctable_total,
    if(MetricName = 'gpu.ecc_uncorrectable_total', Value, NULL) AS ecc_uncorrectable_total,
    CAST(NULL, 'Nullable(Float64)') AS last_fatal_event_code,
    CAST(NULL, 'Nullable(Float64)') AS row_remap_failed,
    CAST(NULL, 'Nullable(Float64)') AS pcie_tx_bytes_per_second,
    CAST(NULL, 'Nullable(Float64)') AS pcie_rx_bytes_per_second,
    if(MetricName = 'gpu.pcie_replay_total', Value, NULL) AS pcie_replay_total,
    CAST(NULL, 'Nullable(Float64)') AS link_tx_bytes_per_second,
    CAST(NULL, 'Nullable(Float64)') AS link_rx_bytes_per_second
FROM otel_metrics_sum
WHERE MetricName LIKE 'gpu.%';

-- Latest known state per physical GPU.
CREATE TABLE IF NOT EXISTS gpu_inventory
(
    `gpu_uuid` String,
    `vendor` LowCardinality(String),
    `gpu_model` LowCardinality(String),
    `node` LowCardinality(String),
    `gpu_index` LowCardinality(String),
    `mem_total_bytes` Float64,
    `first_seen` SimpleAggregateFunction(min, DateTime),
    `last_seen` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY (gpu_uuid, vendor, gpu_model, node, gpu_index, mem_total_bytes);

CREATE MATERIALIZED VIEW IF NOT EXISTS gpu_inventory_mv TO gpu_inventory AS
SELECT
    Attributes['gpu_uuid'] AS gpu_uuid,
    Attributes['vendor'] AS vendor,
    Attributes['gpu_model'] AS gpu_model,
    Attributes['node'] AS node,
    Attributes['gpu_index'] AS gpu_index,
    Value AS mem_total_bytes,
    min(toDateTime(TimeUnix)) AS first_seen,
    max(toDateTime(TimeUnix)) AS last_seen
FROM otel_metrics_gauge
WHERE MetricName = 'gpu.mem_total_bytes'
GROUP BY gpu_uuid, vendor, gpu_model, node, gpu_index, mem_total_bytes;

-- Convenience view: one row per GPU per 5 s, merged, with throttle reasons decoded (NVML bit layout).
CREATE VIEW IF NOT EXISTS gpu_samples_v AS
SELECT
    *,
    if(isNull(throttle_bitmask), CAST([], 'Array(String)'),
       arrayFilter(x -> x != '', [
           if(bitTest(toUInt32(throttle_bitmask), 2), 'sw_power_cap', ''),
           if(bitTest(toUInt32(throttle_bitmask), 3), 'hw_slowdown', ''),
           if(bitTest(toUInt32(throttle_bitmask), 5), 'sw_thermal', ''),
           if(bitTest(toUInt32(throttle_bitmask), 6), 'hw_thermal', ''),
           if(bitTest(toUInt32(throttle_bitmask), 7), 'hw_power_brake', '')])) AS throttle_reasons
FROM
(
    SELECT
        ts, vendor, node, gpu_uuid,
        anyLast(gpu_index) AS gpu_index,
        anyLast(gpu_model) AS gpu_model,
        anyLast(pod) AS pod,
        anyLast(namespace) AS namespace,
        anyLast(busy_percent) AS busy_percent,
        anyLast(compute_active_percent) AS compute_active_percent,
        anyLast(matrix_active_percent) AS matrix_active_percent,
        anyLast(mem_bw_active_percent) AS mem_bw_active_percent,
        anyLast(mem_used_bytes) AS mem_used_bytes,
        anyLast(mem_total_bytes) AS mem_total_bytes,
        anyLast(power_watts) AS power_watts,
        anyLast(energy_joules_total) AS energy_joules_total,
        anyLast(temp_celsius) AS temp_celsius,
        anyLast(mem_temp_celsius) AS mem_temp_celsius,
        anyLast(sm_clock_mhz) AS sm_clock_mhz,
        anyLast(throttle_bitmask) AS throttle_bitmask,
        anyLast(ecc_correctable_total) AS ecc_correctable_total,
        anyLast(ecc_uncorrectable_total) AS ecc_uncorrectable_total,
        anyLast(last_fatal_event_code) AS last_fatal_event_code,
        anyLast(row_remap_failed) AS row_remap_failed,
        anyLast(pcie_tx_bytes_per_second) AS pcie_tx_bytes_per_second,
        anyLast(pcie_rx_bytes_per_second) AS pcie_rx_bytes_per_second,
        anyLast(pcie_replay_total) AS pcie_replay_total,
        anyLast(link_tx_bytes_per_second) AS link_tx_bytes_per_second,
        anyLast(link_rx_bytes_per_second) AS link_rx_bytes_per_second
    FROM gpu_samples
    GROUP BY ts, vendor, node, gpu_uuid
);
