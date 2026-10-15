-- Tokenomics for prefill/decode-disaggregated serving (replaces the 0007 refresh query).
--
-- llm_requests.pod is the serving pod (decode in P/D), so 0007 left every prefill pod's GPU time unattributed
-- ('(idle)'). Each P/D request now also counts on its prefill_pod:
--   serving pod   allocation basis = output tokens (unchanged)
--   prefill pod   allocation basis = uncached prompt tokens (the prefill work done there)
-- Prefill-pod rows carry zero requests / tokens, so sums over tokenomics_hourly do not double count.

DROP VIEW IF EXISTS tokenomics_hourly_refresh;

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
    req_rows AS (
        SELECT toStartOfHour(ts) AS hour, org_id, app_id, model, pod,
               1 AS requests,
               toUInt64(status = 'ok'
                   AND ttft_us <= 1000 * dictGet('dict_slo', 'ttft_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))
                   AND tpot_us <= 1000 * dictGet('dict_slo', 'tpot_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))
               ) AS slo_good,
               toUInt64(tokens_in) AS tokens_in, toUInt64(tokens_cached) AS tokens_cached,
               toUInt64(tokens_out) AS tokens_out,
               toUInt64(if(status = 'client_abort', tokens_out, 0)) AS tokens_out_aborted,
               toUInt64(preempted) AS preempted,
               toFloat64(tokens_out) AS basis
        FROM llm_requests FINAL
        WHERE ts >= toStartOfHour(now() - INTERVAL 1 HOUR) AND pod != ''
        UNION ALL
        SELECT toStartOfHour(ts) AS hour, org_id, app_id, model, prefill_pod AS pod,
               0 AS requests, toUInt64(0) AS slo_good, toUInt64(0) AS tokens_in, toUInt64(0) AS tokens_cached,
               toUInt64(0) AS tokens_out, toUInt64(0) AS tokens_out_aborted, toUInt64(0) AS preempted,
               toFloat64(greatest(tokens_in, tokens_cached) - tokens_cached) AS basis
        FROM llm_requests FINAL
        WHERE ts >= toStartOfHour(now() - INTERVAL 1 HOUR) AND prefill_pod != ''
    ),
    req AS (
        SELECT hour, org_id, app_id, model, pod,
               sum(requests) AS requests, sum(slo_good) AS slo_good,
               sum(tokens_in) AS tokens_in, sum(tokens_cached) AS tokens_cached, sum(tokens_out) AS tokens_out,
               sum(tokens_out_aborted) AS tokens_out_aborted, sum(preempted) AS preempted,
               sum(basis) AS basis, count() AS n
        FROM req_rows
        GROUP BY hour, org_id, app_id, model, pod
    ),
    pod_tot AS (
        SELECT hour, pod, sum(basis) AS pod_basis, sum(n) AS pod_n
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
                p.pod_basis > 0, r.basis / p.pod_basis,
                p.pod_n > 0, r.n / p.pod_n, 1.0) AS alloc_share,
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
