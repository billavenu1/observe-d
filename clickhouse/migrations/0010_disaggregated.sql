-- Prefill/decode-disaggregated serving (docs/observability-story.md §5) and the hashed API key id.
--
--   engine_requests  + role            resource `llm_d.role` (pod label llm-d.ai/role); absent = aggregated
--   kv_transfers     new staging table  from the decode pod's `kv.transfer` span (NIXL-like KV pull)
--   gateway_requests + api_key_id      access-log attribute (hashed by the gateway, never the raw key)
--   llm_requests     + role, prefill_pod, decode_pod, kv_*, decode_wait_us, api_key_id
--
-- In P/D one request has two `llm_request` spans (prefill pod, decode pod) and one `kv.transfer` span, all in
-- the gateway's trace. The join picks the serving span (decode or aggregated) and the prefill span separately:
--   pod / role         the serving pod (the gateway's upstream: llm-d routes to the decode pod)
--   queue_us           prefill-pod queue in P/D (decode-pod queue is decode_wait_us)
--   prefill_us         prefill-pod prefill + any recompute on the decode pod (KV fallback, preemption)
--   ttft_us, e2e_us    prefill span + decode span (the decode span counts from the decode pod's arrival)
--   tokens_cached      from the prefill span (its prefix cache) in P/D
--
-- MVs are recreated (DROP + CREATE): rows inserted in the instant between the two statements are not
-- extracted. Run migrations at a quiet moment on a live system.

ALTER TABLE engine_requests
    ADD COLUMN IF NOT EXISTS `role` LowCardinality(String) DEFAULT 'aggregated' AFTER `pod`;

DROP VIEW IF EXISTS engine_requests_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS engine_requests_mv TO engine_requests AS
SELECT
    Timestamp AS ts,
    TraceId AS trace_id,
    SpanId AS span_id,
    ParentSpanId AS parent_span_id,
    SpanAttributes['gen_ai.request.id'] AS request_id,
    if(SpanAttributes['gen_ai.response.model'] != '', SpanAttributes['gen_ai.response.model'],
       SpanAttributes['gen_ai.request.model']) AS model,
    if(ResourceAttributes['llm.engine'] != '', ResourceAttributes['llm.engine'], 'vllm') AS engine,
    ResourceAttributes['llm.engine_version'] AS engine_version,
    ResourceAttributes['k8s.pod.name'] AS pod,
    if(ResourceAttributes['llm_d.role'] IN ('prefill', 'decode'), ResourceAttributes['llm_d.role'], 'aggregated') AS role,
    ResourceAttributes['k8s.node.name'] AS node,
    ResourceAttributes['k8s.namespace.name'] AS namespace,
    toUInt32OrZero(SpanAttributes['gen_ai.usage.prompt_tokens']) AS tokens_in,
    toUInt32OrZero(SpanAttributes['gen_ai.usage.cached_tokens']) AS tokens_cached,
    toUInt32OrZero(SpanAttributes['gen_ai.usage.completion_tokens']) AS tokens_out,
    toUInt32(toFloat64OrZero(SpanAttributes['gen_ai.latency.time_in_queue']) * 1e6) AS queue_us,
    toUInt32(toFloat64OrZero(SpanAttributes['gen_ai.latency.time_in_model_prefill']) * 1e6) AS prefill_us,
    toUInt32(toFloat64OrZero(SpanAttributes['gen_ai.latency.time_in_model_decode']) * 1e6) AS decode_us,
    toUInt32(toFloat64OrZero(SpanAttributes['gen_ai.latency.time_to_first_token']) * 1e6) AS ttft_us,
    if(tokens_out > 1, toUInt32(decode_us / (tokens_out - 1)), 0) AS tpot_us,
    toUInt32(toFloat64OrZero(SpanAttributes['gen_ai.latency.e2e']) * 1e6) AS e2e_us,
    SpanAttributes['gen_ai.response.finish_reasons'] AS finish_reason,
    StatusCode AS span_status,
    toUInt8(SpanAttributes['llm.preempted'] = 'true') AS preempted,
    toFloat32OrZero(SpanAttributes['gen_ai.request.temperature']) AS temperature,
    toFloat32OrZero(SpanAttributes['gen_ai.request.top_p']) AS top_p,
    toUInt32OrZero(SpanAttributes['gen_ai.request.max_tokens']) AS max_tokens
FROM otel_traces
WHERE SpanName = 'llm_request';

CREATE TABLE IF NOT EXISTS kv_transfers
(
    `inserted_at` DateTime64(3) DEFAULT now64(3),
    `ts` DateTime64(6),
    `trace_id` String,
    `span_id` String,
    `parent_span_id` String,
    `model` LowCardinality(String),
    `connector` LowCardinality(String),
    `source_pod` LowCardinality(String),
    `target_pod` LowCardinality(String),
    `bytes` UInt64,
    `transfer_us` UInt32,
    `status` LowCardinality(String),
    `fallback` LowCardinality(String),
    INDEX idx_trace trace_id TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(inserted_at)
ORDER BY (inserted_at, trace_id)
TTL toDateTime(inserted_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS kv_transfers_mv TO kv_transfers AS
SELECT
    Timestamp AS ts,
    TraceId AS trace_id,
    SpanId AS span_id,
    ParentSpanId AS parent_span_id,
    SpanAttributes['gen_ai.request.model'] AS model,
    SpanAttributes['llm_d.kv.connector'] AS connector,
    SpanAttributes['llm_d.kv.source_pod'] AS source_pod,
    if(SpanAttributes['llm_d.kv.target_pod'] != '', SpanAttributes['llm_d.kv.target_pod'],
       ResourceAttributes['k8s.pod.name']) AS target_pod,
    toUInt64OrZero(SpanAttributes['llm_d.kv.bytes']) AS bytes,
    toUInt32(Duration / 1000) AS transfer_us,
    if(SpanAttributes['llm_d.kv.status'] != '', SpanAttributes['llm_d.kv.status'],
       if(StatusCode = 'STATUS_CODE_ERROR', 'failed', 'ok')) AS status,
    SpanAttributes['llm_d.kv.fallback'] AS fallback
FROM otel_traces
WHERE SpanName = 'kv.transfer';

ALTER TABLE gateway_requests
    ADD COLUMN IF NOT EXISTS `api_key_id` String DEFAULT '' AFTER `app_id`;

DROP VIEW IF EXISTS gateway_requests_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS gateway_requests_mv TO gateway_requests AS
SELECT
    Timestamp AS ts,
    TraceId AS trace_id,
    SpanId AS span_id,
    LogAttributes['request_id'] AS request_id,
    if(LogAttributes['org_id'] = '', 'unknown', LogAttributes['org_id']) AS org_id,
    if(LogAttributes['app_id'] = '', 'unknown', LogAttributes['app_id']) AS app_id,
    LogAttributes['api_key_id'] AS api_key_id,
    LogAttributes['user_id'] AS user_id,
    LogAttributes['session_id'] AS session_id,
    LogAttributes['model'] AS model,
    LogAttributes['upstream_pod'] AS upstream_pod,
    toUInt16OrZero(LogAttributes['response_code']) AS response_code,
    toUInt32(toFloat64OrZero(LogAttributes['duration_ms']) * 1000) AS duration_us,
    toUInt32OrZero(LogAttributes['bytes_received']) AS bytes_in,
    toUInt32OrZero(LogAttributes['bytes_sent']) AS bytes_out
FROM gateway_access_null;

ALTER TABLE llm_requests
    ADD COLUMN IF NOT EXISTS `api_key_id` String DEFAULT '' AFTER `app_id`,
    ADD COLUMN IF NOT EXISTS `role` LowCardinality(String) DEFAULT '' AFTER `pod`,
    ADD COLUMN IF NOT EXISTS `prefill_pod` LowCardinality(String) DEFAULT '' AFTER `role`,
    ADD COLUMN IF NOT EXISTS `decode_pod` LowCardinality(String) DEFAULT '' AFTER `prefill_pod`,
    ADD COLUMN IF NOT EXISTS `decode_wait_us` UInt32 DEFAULT 0 AFTER `decode_us`,
    ADD COLUMN IF NOT EXISTS `kv_transfer_us` UInt32 DEFAULT 0 AFTER `decode_wait_us`,
    ADD COLUMN IF NOT EXISTS `kv_bytes` UInt64 DEFAULT 0 AFTER `kv_transfer_us`,
    ADD COLUMN IF NOT EXISTS `kv_fallback` UInt8 DEFAULT 0 AFTER `kv_bytes`,
    ADD COLUMN IF NOT EXISTS `has_prefill` UInt8 DEFAULT 0 AFTER `has_route`,
    ADD COLUMN IF NOT EXISTS `has_kv` UInt8 DEFAULT 0 AFTER `has_prefill`;

DROP VIEW IF EXISTS llm_requests_refresh;

-- version: completeness first (serving span 8, prefill span 4, kv span 2, route span 1), then refresh time,
-- so a later, more complete join of the same request replaces an earlier one.
CREATE MATERIALIZED VIEW IF NOT EXISTS llm_requests_refresh
REFRESH EVERY 1 MINUTE APPEND TO llm_requests AS
SELECT
    g.ts AS ts,
    g.trace_id AS trace_id,
    if(g.request_id != '', g.request_id, e.request_id) AS request_id,
    g.org_id AS org_id,
    g.app_id AS app_id,
    g.api_key_id AS api_key_id,
    g.user_id AS user_id,
    g.session_id AS session_id,
    if(e.model != '', e.model, g.model) AS model,
    e.engine AS engine,
    e.engine_version AS engine_version,
    if(e.pod != '', e.pod, g.upstream_pod) AS pod,
    multiIf(e.trace_id != '', e.role, p.trace_id != '', 'decode', '') AS role,
    p.pod AS prefill_pod,
    multiIf(e.role = 'decode', e.pod, p.trace_id != '', g.upstream_pod, '') AS decode_pod,
    e.node AS node,
    g.response_code AS response_code,
    multiIf(
        g.response_code IN (401, 403), 'rejected',
        g.response_code = 503 AND g.upstream_pod = '', 'no_capacity',
        g.response_code = 499 OR e.finish_reason = 'abort', 'client_abort',
        g.response_code >= 500, 'error',
        'ok') AS status,
    e.finish_reason AS finish_reason,
    if(e.tokens_in > 0, e.tokens_in, p.tokens_in) AS tokens_in,
    if(p.trace_id != '', p.tokens_cached, e.tokens_cached) AS tokens_cached,
    e.tokens_out AS tokens_out,
    g.duration_us AS gateway_us,
    r.decision_us AS routing_us,
    if(p.trace_id != '', p.queue_us, e.queue_us) AS queue_us,
    p.prefill_us + e.prefill_us AS prefill_us,
    e.decode_us AS decode_us,
    if(p.trace_id != '', e.queue_us, 0) AS decode_wait_us,
    k.transfer_us AS kv_transfer_us,
    k.bytes AS kv_bytes,
    toUInt8(k.fallback != '' OR k.status = 'failed') AS kv_fallback,
    p.ttft_us + e.ttft_us AS ttft_us,
    e.tpot_us AS tpot_us,
    p.e2e_us + e.e2e_us AS e2e_us,
    r.candidates AS route_candidates,
    r.prefix_hit_predicted AS prefix_hit_predicted,
    toUInt8(if(p.trace_id != '', p.tokens_cached, e.tokens_cached) > 0) AS prefix_hit_actual,
    r.fallback_reason AS fallback_reason,
    greatest(e.preempted, p.preempted) AS preempted,
    e.temperature AS temperature,
    e.top_p AS top_p,
    e.max_tokens AS max_tokens,
    g.bytes_in AS bytes_in,
    g.bytes_out AS bytes_out,
    toUInt8(e.trace_id != '') AS has_engine,
    toUInt8(r.trace_id != '') AS has_route,
    toUInt8(p.trace_id != '') AS has_prefill,
    toUInt8(k.trace_id != '') AS has_kv,
    toUInt64(has_engine * 8 + has_prefill * 4 + has_kv * 2 + has_route) * 10000000000000
        + toUInt64(toUnixTimestamp64Milli(now64(3))) AS version
FROM (SELECT * FROM gateway_requests WHERE inserted_at > now64(3) - INTERVAL 4 MINUTE) AS g
ANY LEFT JOIN (
    SELECT * FROM engine_requests WHERE inserted_at > now64(3) - INTERVAL 10 MINUTE AND role != 'prefill'
) AS e ON e.trace_id = g.trace_id
ANY LEFT JOIN (
    SELECT * FROM engine_requests WHERE inserted_at > now64(3) - INTERVAL 10 MINUTE AND role = 'prefill'
) AS p ON p.trace_id = g.trace_id
ANY LEFT JOIN (SELECT * FROM kv_transfers WHERE inserted_at > now64(3) - INTERVAL 10 MINUTE) AS k
    ON k.trace_id = g.trace_id
ANY LEFT JOIN (SELECT * FROM routing_decisions WHERE inserted_at > now64(3) - INTERVAL 10 MINUTE) AS r
    ON r.trace_id = g.trace_id;
