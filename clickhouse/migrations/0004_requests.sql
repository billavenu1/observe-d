-- AI & LLM traceability (MVP.md §3.7): one durable row per request.
-- gateway access log (identity, upstream pod, status)  ⋈  vLLM span (tokens, timings)  ⋈  EPP span (routing)
-- joined on trace_id by a refreshable MV every minute over rows *inserted* in the last few minutes.
-- Re-joins of the same request are collapsed by ReplacingMergeTree; the most complete version wins.

CREATE TABLE IF NOT EXISTS gateway_requests
(
    `inserted_at` DateTime64(3) DEFAULT now64(3),
    `ts` DateTime64(6),
    `trace_id` String,
    `span_id` String,
    `request_id` String,
    `org_id` LowCardinality(String),
    `app_id` LowCardinality(String),
    `user_id` String,
    `session_id` String,
    `model` LowCardinality(String),
    `upstream_pod` LowCardinality(String),
    `response_code` UInt16,
    `duration_us` UInt32,
    `bytes_in` UInt32,
    `bytes_out` UInt32,
    INDEX idx_trace trace_id TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(inserted_at)
ORDER BY (inserted_at, trace_id)
TTL toDateTime(inserted_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS gateway_requests_mv TO gateway_requests AS
SELECT
    Timestamp AS ts,
    TraceId AS trace_id,
    SpanId AS span_id,
    LogAttributes['request_id'] AS request_id,
    if(LogAttributes['org_id'] = '', 'unknown', LogAttributes['org_id']) AS org_id,
    if(LogAttributes['app_id'] = '', 'unknown', LogAttributes['app_id']) AS app_id,
    LogAttributes['user_id'] AS user_id,
    LogAttributes['session_id'] AS session_id,
    LogAttributes['model'] AS model,
    LogAttributes['upstream_pod'] AS upstream_pod,
    toUInt16OrZero(LogAttributes['response_code']) AS response_code,
    toUInt32(toFloat64OrZero(LogAttributes['duration_ms']) * 1000) AS duration_us,
    toUInt32OrZero(LogAttributes['bytes_received']) AS bytes_in,
    toUInt32OrZero(LogAttributes['bytes_sent']) AS bytes_out
FROM gateway_access_null;

CREATE TABLE IF NOT EXISTS llm_requests
(
    `ts` DateTime64(6),
    `trace_id` String,
    `request_id` String,
    `org_id` LowCardinality(String),
    `app_id` LowCardinality(String),
    `user_id` String,
    `session_id` String,
    `model` LowCardinality(String),
    `engine` LowCardinality(String),
    `engine_version` LowCardinality(String),
    `pod` LowCardinality(String),
    `node` LowCardinality(String),
    `response_code` UInt16,
    `status` LowCardinality(String),
    `finish_reason` LowCardinality(String),
    `tokens_in` UInt32,
    `tokens_cached` UInt32,
    `tokens_out` UInt32,
    `gateway_us` UInt32,
    `routing_us` UInt32,
    `queue_us` UInt32,
    `prefill_us` UInt32,
    `decode_us` UInt32,
    `ttft_us` UInt32,
    `tpot_us` UInt32,
    `e2e_us` UInt32,
    `route_candidates` UInt16,
    `prefix_hit_predicted` UInt8,
    `prefix_hit_actual` UInt8,
    `fallback_reason` LowCardinality(String),
    `preempted` UInt8,
    `temperature` Float32,
    `top_p` Float32,
    `max_tokens` UInt32,
    `bytes_in` UInt32,
    `bytes_out` UInt32,
    `has_engine` UInt8,
    `has_route` UInt8,
    `version` UInt64,
    INDEX idx_trace trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_request request_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_org org_id TYPE set(100) GRANULARITY 4,
    INDEX idx_model model TYPE set(100) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(ts)
ORDER BY (toStartOfHour(ts), trace_id)
TTL toDateTime(ts) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS llm_requests_refresh
REFRESH EVERY 1 MINUTE APPEND TO llm_requests AS
SELECT
    g.ts AS ts,
    g.trace_id AS trace_id,
    if(g.request_id != '', g.request_id, e.request_id) AS request_id,
    g.org_id AS org_id,
    g.app_id AS app_id,
    g.user_id AS user_id,
    g.session_id AS session_id,
    if(e.model != '', e.model, g.model) AS model,
    e.engine AS engine,
    e.engine_version AS engine_version,
    if(e.pod != '', e.pod, g.upstream_pod) AS pod,
    e.node AS node,
    g.response_code AS response_code,
    multiIf(
        g.response_code IN (401, 403), 'rejected',
        g.response_code = 503 AND g.upstream_pod = '', 'no_capacity',
        g.response_code = 499 OR e.finish_reason = 'abort', 'client_abort',
        g.response_code >= 500, 'error',
        'ok') AS status,
    e.finish_reason AS finish_reason,
    e.tokens_in AS tokens_in,
    e.tokens_cached AS tokens_cached,
    e.tokens_out AS tokens_out,
    g.duration_us AS gateway_us,
    r.decision_us AS routing_us,
    e.queue_us AS queue_us,
    e.prefill_us AS prefill_us,
    e.decode_us AS decode_us,
    e.ttft_us AS ttft_us,
    e.tpot_us AS tpot_us,
    e.e2e_us AS e2e_us,
    r.candidates AS route_candidates,
    r.prefix_hit_predicted AS prefix_hit_predicted,
    toUInt8(e.tokens_cached > 0) AS prefix_hit_actual,
    r.fallback_reason AS fallback_reason,
    e.preempted AS preempted,
    e.temperature AS temperature,
    e.top_p AS top_p,
    e.max_tokens AS max_tokens,
    g.bytes_in AS bytes_in,
    g.bytes_out AS bytes_out,
    toUInt8(e.trace_id != '') AS has_engine,
    toUInt8(r.trace_id != '') AS has_route,
    toUInt64(has_engine * 2 + has_route) * 10000000000000 + toUInt64(toUnixTimestamp64Milli(now64(3))) AS version
FROM (SELECT * FROM gateway_requests WHERE inserted_at > now64(3) - INTERVAL 4 MINUTE) AS g
ANY LEFT JOIN (SELECT * FROM engine_requests WHERE inserted_at > now64(3) - INTERVAL 10 MINUTE) AS e
    ON e.trace_id = g.trace_id
ANY LEFT JOIN (SELECT * FROM routing_decisions WHERE inserted_at > now64(3) - INTERVAL 10 MINUTE) AS r
    ON r.trace_id = g.trace_id;

-- Join health (traceability dashboard + alert): requests that reached an engine but have no engine span
-- after 3 minutes => broken traceparent propagation; engine spans with no gateway log => missing access logs.
CREATE VIEW IF NOT EXISTS join_health AS
SELECT
    toStartOfMinute(ts) AS minute,
    countIf(pod != '' AND status NOT IN ('rejected', 'no_capacity')) AS routed,
    countIf(pod != '' AND status NOT IN ('rejected', 'no_capacity') AND has_engine = 0) AS missing_engine_span,
    countIf(has_route = 0 AND status NOT IN ('rejected')) AS missing_route_span
FROM llm_requests FINAL
WHERE ts < now() - INTERVAL 3 MINUTE
GROUP BY minute;
