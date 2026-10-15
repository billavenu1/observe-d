-- llm-d + vLLM layer (MVP.md §3.4, §3.5): per-request staging rows extracted from spans.
-- Staging tables keep 1 day; llm_requests (0004) is the durable per-request table.
-- Span attribute names: vLLM OTLP request span (gen_ai.*) and the EPP routing span (llm_d.*);
-- see docs/telemetry-shapes.md for which names are verified.

CREATE TABLE IF NOT EXISTS engine_requests
(
    `inserted_at` DateTime64(3) DEFAULT now64(3),
    `ts` DateTime64(6),
    `trace_id` String,
    `span_id` String,
    `parent_span_id` String,
    `request_id` String,
    `model` LowCardinality(String),
    `engine` LowCardinality(String),
    `engine_version` LowCardinality(String),
    `pod` LowCardinality(String),
    `node` LowCardinality(String),
    `namespace` LowCardinality(String),
    `tokens_in` UInt32,
    `tokens_cached` UInt32,
    `tokens_out` UInt32,
    `queue_us` UInt32,
    `prefill_us` UInt32,
    `decode_us` UInt32,
    `ttft_us` UInt32,
    `tpot_us` UInt32,
    `e2e_us` UInt32,
    `finish_reason` LowCardinality(String),
    `span_status` LowCardinality(String),
    `preempted` UInt8,
    `temperature` Float32,
    `top_p` Float32,
    `max_tokens` UInt32,
    INDEX idx_trace trace_id TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(inserted_at)
ORDER BY (inserted_at, trace_id)
TTL toDateTime(inserted_at) + INTERVAL 1 DAY;

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

CREATE TABLE IF NOT EXISTS routing_decisions
(
    `inserted_at` DateTime64(3) DEFAULT now64(3),
    `ts` DateTime64(6),
    `trace_id` String,
    `span_id` String,
    `model` LowCardinality(String),
    `chosen_pod` LowCardinality(String),
    `candidates` UInt16,
    `score_prefix` Float32,
    `score_queue` Float32,
    `score_kv` Float32,
    `prefix_hit_predicted` UInt8,
    `fallback_reason` LowCardinality(String),
    `decision_us` UInt32,
    INDEX idx_trace trace_id TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(inserted_at)
ORDER BY (inserted_at, trace_id)
TTL toDateTime(inserted_at) + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS routing_decisions_mv TO routing_decisions AS
SELECT
    Timestamp AS ts,
    TraceId AS trace_id,
    SpanId AS span_id,
    SpanAttributes['gen_ai.request.model'] AS model,
    SpanAttributes['llm_d.chosen_pod'] AS chosen_pod,
    toUInt16OrZero(SpanAttributes['llm_d.candidates']) AS candidates,
    toFloat32OrZero(SpanAttributes['llm_d.score.prefix']) AS score_prefix,
    toFloat32OrZero(SpanAttributes['llm_d.score.queue']) AS score_queue,
    toFloat32OrZero(SpanAttributes['llm_d.score.kv']) AS score_kv,
    toUInt8(SpanAttributes['llm_d.prefix_hit_predicted'] = 'true') AS prefix_hit_predicted,
    SpanAttributes['llm_d.fallback_reason'] AS fallback_reason,
    toUInt32(Duration / 1000) AS decision_us
FROM otel_traces
WHERE SpanName = 'llm_d.route';

-- Which model / engine / config runs where (from the engine startup event).
CREATE TABLE IF NOT EXISTS model_deployments
(
    `pod` String,
    `namespace` LowCardinality(String),
    `node` LowCardinality(String),
    `model` LowCardinality(String),
    `engine` LowCardinality(String),
    `engine_version` LowCardinality(String),
    `tp` UInt16,
    `max_num_seqs` UInt32,
    `dtype` LowCardinality(String),
    `vendor` LowCardinality(String),
    `started_at` DateTime64(3)
)
ENGINE = ReplacingMergeTree(started_at)
ORDER BY pod;

CREATE MATERIALIZED VIEW IF NOT EXISTS model_deployments_mv TO model_deployments AS
SELECT
    ResourceAttributes['k8s.pod.name'] AS pod,
    ResourceAttributes['k8s.namespace.name'] AS namespace,
    ResourceAttributes['k8s.node.name'] AS node,
    LogAttributes['llm.model'] AS model,
    LogAttributes['llm.engine'] AS engine,
    LogAttributes['llm.engine_version'] AS engine_version,
    toUInt16OrZero(LogAttributes['llm.tp']) AS tp,
    toUInt32OrZero(LogAttributes['llm.max_num_seqs']) AS max_num_seqs,
    LogAttributes['llm.dtype'] AS dtype,
    LogAttributes['gpu.vendor'] AS vendor,
    Timestamp AS started_at
FROM otel_logs
WHERE EventName = 'engine.started';
