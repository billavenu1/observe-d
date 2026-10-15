-- Tokenomics (MVP.md §3.6): owned-GPU cost and energy attributed to org / app / model by token share.
--
--   gpu_seconds   per pod-hour: minutes in which each GPU reported that pod, x 60 (from gpu_samples)
--   energy_j      per pod-hour: Δ cumulative energy counter per GPU (from gpu_samples)
--   cost_infra    gpu_seconds / 3600 x $/GPU-hour (dict_gpu_cost, by GPU model)
--   allocation    a pod-hour's cost/energy is split across its (org, app, model) rows by output-token share
--   idle          pod-hours with GPUs but no requests  -> org_id '(idle)'
--   unallocated   GPUs with no pod                     -> org_id '(unallocated)'
-- Recomputed every 5 minutes for the current and previous hour.

CREATE TABLE IF NOT EXISTS tokenomics_hourly
(
    `hour` DateTime,
    `org_id` LowCardinality(String),
    `app_id` LowCardinality(String),
    `model` LowCardinality(String),
    `pod` LowCardinality(String),
    `vendor` LowCardinality(String),
    `gpu_model` LowCardinality(String),
    `requests` UInt64,
    `slo_good` UInt64,
    `tokens_in` UInt64,
    `tokens_cached` UInt64,
    `tokens_out` UInt64,
    `tokens_out_aborted` UInt64,
    `preempted` UInt64,
    `gpu_seconds` Float64,
    `energy_j` Float64,
    `cost_infra_usd` Float64,
    `cost_list_usd` Float64,
    `cache_savings_usd` Float64,
    `computed_at` DateTime64(3)
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, org_id, app_id, model, pod)
TTL hour + INTERVAL 800 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS tokenomics_hourly_refresh
REFRESH EVERY 5 MINUTE OFFSET 40 SECOND APPEND TO tokenomics_hourly AS
WITH
    gpu_pod AS (
        SELECT hour, pod, any(vendor) AS vendor, any(gpu_model) AS gpu_model,
               sum(minutes) * 60 AS gpu_seconds, sum(energy) AS energy_j
        FROM (
            SELECT toStartOfHour(ts) AS hour, gpu_uuid, pod, any(vendor) AS vendor,
                   any(gpu_model) AS gpu_model,
                   uniqExact(toStartOfMinute(ts)) AS minutes,
                   max(energy_joules_total) - min(energy_joules_total) AS energy
            FROM gpu_samples_v
            WHERE ts >= toStartOfHour(now() - INTERVAL 1 HOUR)
            GROUP BY hour, gpu_uuid, pod
        )
        GROUP BY hour, pod
    ),
    req AS (
        SELECT toStartOfHour(ts) AS hour, org_id, app_id, model, pod,
               count() AS requests,
               countIf(status = 'ok'
                   AND ttft_us <= 1000 * dictGet('dict_slo', 'ttft_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))
                   AND tpot_us <= 1000 * dictGet('dict_slo', 'tpot_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))
               ) AS slo_good,
               sum(tokens_in) AS tokens_in, sum(tokens_cached) AS tokens_cached, sum(tokens_out) AS tokens_out,
               sumIf(tokens_out, status = 'client_abort') AS tokens_out_aborted, sum(preempted) AS preempted
        FROM llm_requests FINAL
        WHERE ts >= toStartOfHour(now() - INTERVAL 1 HOUR) AND pod != ''
        GROUP BY hour, org_id, app_id, model, pod
    ),
    pod_tot AS (
        SELECT hour, pod, sum(tokens_out) AS pod_tokens_out, sum(requests) AS pod_requests
        FROM req GROUP BY hour, pod
    )
SELECT hour, org_id, app_id, model, pod, vendor, gpu_model, requests, slo_good, tokens_in, tokens_cached, tokens_out, tokens_out_aborted, preempted, gpu_seconds, energy_j, cost_infra_usd, cost_list_usd, cache_savings_usd, computed_at
FROM
(
    SELECT
        g.hour AS hour,
        multiIf(g.pod = '', '(unallocated)', r.org_id = '', '(idle)', r.org_id) AS org_id,
        if(r.app_id = '', '', r.app_id) AS app_id,
        r.model AS model,
        g.pod AS pod,
        g.vendor AS vendor,
        g.gpu_model AS gpu_model,
        r.requests AS requests,
        r.slo_good AS slo_good,
        r.tokens_in AS tokens_in,
        r.tokens_cached AS tokens_cached,
        r.tokens_out AS tokens_out,
        r.tokens_out_aborted AS tokens_out_aborted,
        r.preempted AS preempted,
        multiIf(r.org_id = '', 1.0,
                p.pod_tokens_out > 0, r.tokens_out / p.pod_tokens_out,
                p.pod_requests > 0, r.requests / p.pod_requests, 1.0) AS alloc_share,
        g.gpu_seconds * alloc_share AS gpu_seconds,
        g.energy_j * alloc_share AS energy_j,
        g.gpu_seconds * alloc_share / 3600 * dictGet('dict_gpu_cost', 'usd_per_gpu_hour',
            tuple(if(dictHas('dict_gpu_cost', tuple(g.gpu_model)), g.gpu_model, 'default'))) AS cost_infra_usd,
        (
            (r.tokens_in - r.tokens_cached) * dictGet('dict_model_price', 'usd_per_1m_input', tuple(if(dictHas('dict_model_price', tuple(r.model)), r.model, 'default')))
            + r.tokens_cached * dictGet('dict_model_price', 'usd_per_1m_cached_input', tuple(if(dictHas('dict_model_price', tuple(r.model)), r.model, 'default')))
            + r.tokens_out * dictGet('dict_model_price', 'usd_per_1m_output', tuple(if(dictHas('dict_model_price', tuple(r.model)), r.model, 'default')))
        ) / 1e6 AS cost_list_usd,
        r.tokens_cached * (
            dictGet('dict_model_price', 'usd_per_1m_input', tuple(if(dictHas('dict_model_price', tuple(r.model)), r.model, 'default')))
            - dictGet('dict_model_price', 'usd_per_1m_cached_input', tuple(if(dictHas('dict_model_price', tuple(r.model)), r.model, 'default')))
        ) / 1e6 AS cache_savings_usd,
        now64(3) AS computed_at
    FROM gpu_pod AS g
    LEFT JOIN req AS r ON r.hour = g.hour AND r.pod = g.pod
    LEFT JOIN pod_tot AS p ON p.hour = g.hour AND p.pod = g.pod
)
SETTINGS prefer_column_name_to_alias = 1;
