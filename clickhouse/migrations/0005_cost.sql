-- Cost & SLO configuration (MVP.md §3.6). Small editable tables exposed as dictionaries; change values with
-- INSERT (ReplacingMergeTree keeps the latest row per key) — dictionaries reload within 60 s.
-- The dictionary source uses the otel user; in production use a named collection instead of inline creds.

CREATE TABLE IF NOT EXISTS cost_gpu
(
    `gpu_model` String,
    `usd_per_gpu_hour` Float64,
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY gpu_model;

-- Placeholder $/GPU-hour (amortized capex + power + DC overhead). Replace with finance numbers.
INSERT INTO cost_gpu (gpu_model, usd_per_gpu_hour) VALUES
    ('NVIDIA H100 80GB HBM3', 2.50),
    ('AMD Instinct MI300X', 2.20),
    ('default', 2.50);

CREATE TABLE IF NOT EXISTS cost_model_price
(
    `model` String,
    `usd_per_1m_input` Float64,
    `usd_per_1m_cached_input` Float64,
    `usd_per_1m_output` Float64,
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY model;

-- Reference "API price list" per model, to compare owned-GPU cost with an external provider.
INSERT INTO cost_model_price (model, usd_per_1m_input, usd_per_1m_cached_input, usd_per_1m_output) VALUES
    ('llama-3.1-70b-instruct', 0.60, 0.15, 0.80),
    ('qwen2.5-7b-instruct', 0.10, 0.025, 0.20),
    ('default', 0.50, 0.125, 1.00);

CREATE TABLE IF NOT EXISTS slo_targets
(
    `model` String,
    `ttft_ms` Float64,
    `tpot_ms` Float64,
    `max_error_rate` Float64,
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY model;

-- Placeholder SLOs (MVP.md §1 #11): request is "good" if TTFT and TPOT are within target and it succeeded.
INSERT INTO slo_targets (model, ttft_ms, tpot_ms, max_error_rate) VALUES
    ('llama-3.1-70b-instruct', 1000, 60, 0.01),
    ('qwen2.5-7b-instruct', 500, 30, 0.01),
    ('default', 1000, 100, 0.01);

CREATE DICTIONARY IF NOT EXISTS dict_gpu_cost
(
    `gpu_model` String,
    `usd_per_gpu_hour` Float64
)
PRIMARY KEY gpu_model
SOURCE(CLICKHOUSE(QUERY 'SELECT gpu_model, argMax(usd_per_gpu_hour, updated_at) AS usd_per_gpu_hour FROM {{database}}.cost_gpu GROUP BY gpu_model' USER 'otel' PASSWORD 'otel'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(60);

CREATE DICTIONARY IF NOT EXISTS dict_model_price
(
    `model` String,
    `usd_per_1m_input` Float64,
    `usd_per_1m_cached_input` Float64,
    `usd_per_1m_output` Float64
)
PRIMARY KEY model
SOURCE(CLICKHOUSE(QUERY 'SELECT model, argMax(usd_per_1m_input, updated_at) AS usd_per_1m_input, argMax(usd_per_1m_cached_input, updated_at) AS usd_per_1m_cached_input, argMax(usd_per_1m_output, updated_at) AS usd_per_1m_output FROM {{database}}.cost_model_price GROUP BY model' USER 'otel' PASSWORD 'otel'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(60);

CREATE DICTIONARY IF NOT EXISTS dict_slo
(
    `model` String,
    `ttft_ms` Float64,
    `tpot_ms` Float64,
    `max_error_rate` Float64
)
PRIMARY KEY model
SOURCE(CLICKHOUSE(QUERY 'SELECT model, argMax(ttft_ms, updated_at) AS ttft_ms, argMax(tpot_ms, updated_at) AS tpot_ms, argMax(max_error_rate, updated_at) AS max_error_rate FROM {{database}}.slo_targets GROUP BY model' USER 'otel' PASSWORD 'otel'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(60);
