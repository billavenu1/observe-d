-- Rollups for dashboards and long retention (MVP.md §3.5–3.7, §5).
-- llm_requests is updated in place (ReplacingMergeTree), so rollups are NOT insert-triggered MVs (they would
-- double count re-joined rows). Instead a refreshable MV recomputes the most recent buckets every cycle and
-- ReplacingMergeTree keeps the latest computation per bucket. Query rollups with FINAL.

CREATE TABLE IF NOT EXISTS llm_rollup_1m
(
    `minute` DateTime,
    `org_id` LowCardinality(String),
    `app_id` LowCardinality(String),
    `model` LowCardinality(String),
    `pod` LowCardinality(String),
    `requests` UInt64,
    `ok` UInt64,
    `errors` UInt64,
    `client_aborts` UInt64,
    `rejected` UInt64,
    `slo_good` UInt64,
    `tokens_in` UInt64,
    `tokens_cached` UInt64,
    `tokens_out` UInt64,
    `tokens_out_aborted` UInt64,
    `preempted` UInt64,
    `prefix_hit_predicted` UInt64,
    `prefix_hit_actual` UInt64,
    `prefix_hit_predicted_and_actual` UInt64,
    `ttft_q` AggregateFunction(quantilesTDigest(0.5, 0.9, 0.95, 0.99), UInt32),
    `tpot_q` AggregateFunction(quantilesTDigest(0.5, 0.9, 0.95, 0.99), UInt32),
    `e2e_q` AggregateFunction(quantilesTDigest(0.5, 0.9, 0.95, 0.99), UInt32),
    `queue_q` AggregateFunction(quantilesTDigest(0.5, 0.9, 0.95, 0.99), UInt32),
    `gateway_q` AggregateFunction(quantilesTDigest(0.5, 0.9, 0.95, 0.99), UInt32),
    `computed_at` DateTime64(3)
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, org_id, app_id, model, pod)
TTL minute + INTERVAL 400 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS llm_rollup_1m_refresh
REFRESH EVERY 1 MINUTE OFFSET 20 SECOND APPEND TO llm_rollup_1m AS
SELECT
    toStartOfMinute(ts) AS minute,
    org_id, app_id, model, pod,
    count() AS requests,
    countIf(status = 'ok') AS ok,
    countIf(status = 'error') AS errors,
    countIf(status = 'client_abort') AS client_aborts,
    countIf(status IN ('rejected', 'no_capacity')) AS rejected,
    countIf(status = 'ok'
        AND ttft_us <= 1000 * dictGet('dict_slo', 'ttft_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))
        AND tpot_us <= 1000 * dictGet('dict_slo', 'tpot_ms', tuple(if(dictHas('dict_slo', tuple(model)), model, 'default')))
    ) AS slo_good,
    sum(tokens_in) AS tokens_in,
    sum(tokens_cached) AS tokens_cached,
    sum(tokens_out) AS tokens_out,
    sumIf(tokens_out, status = 'client_abort') AS tokens_out_aborted,
    sum(preempted) AS preempted,
    sum(prefix_hit_predicted) AS prefix_hit_predicted,
    sum(prefix_hit_actual) AS prefix_hit_actual,
    countIf(prefix_hit_predicted = 1 AND prefix_hit_actual = 1) AS prefix_hit_predicted_and_actual,
    quantilesTDigestStateIf(0.5, 0.9, 0.95, 0.99)(ttft_us, has_engine = 1) AS ttft_q,
    quantilesTDigestStateIf(0.5, 0.9, 0.95, 0.99)(tpot_us, has_engine = 1 AND tokens_out > 1) AS tpot_q,
    quantilesTDigestStateIf(0.5, 0.9, 0.95, 0.99)(e2e_us, has_engine = 1) AS e2e_q,
    quantilesTDigestStateIf(0.5, 0.9, 0.95, 0.99)(queue_us, has_engine = 1) AS queue_q,
    quantilesTDigestState(0.5, 0.9, 0.95, 0.99)(gateway_us) AS gateway_q,
    now64(3) AS computed_at
FROM llm_requests FINAL
WHERE ts >= toStartOfMinute(now() - INTERVAL 10 MINUTE)
GROUP BY minute, org_id, app_id, model, pod
SETTINGS prefer_column_name_to_alias = 1;
