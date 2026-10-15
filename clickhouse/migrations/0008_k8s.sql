-- Kubernetes layer (MVP.md §3.2): events from the cluster collector's k8sobjects receiver (watch mode).
-- The log body is the watch event: {"type": "ADDED|MODIFIED", "object": <events.k8s.io/v1 Event>}.

CREATE TABLE IF NOT EXISTS k8s_events
(
    `ts` DateTime64(3),
    `watch_type` LowCardinality(String),
    `type` LowCardinality(String),
    `reason` LowCardinality(String),
    `kind` LowCardinality(String),
    `namespace` LowCardinality(String),
    `name` String,
    `message` String,
    `count` UInt32,
    `reporting_controller` LowCardinality(String),
    `cluster` LowCardinality(String),
    `uid` String
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
ORDER BY (namespace, name, reason, uid, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS k8s_events_mv TO k8s_events AS
SELECT
    coalesce(
        parseDateTime64BestEffortOrNull(JSONExtractString(Body, 'object', 'eventTime'), 3),
        parseDateTime64BestEffortOrNull(JSONExtractString(Body, 'object', 'deprecatedLastTimestamp'), 3),
        parseDateTime64BestEffortOrNull(JSONExtractString(Body, 'object', 'metadata', 'creationTimestamp'), 3),
        toDateTime64(Timestamp, 3)) AS ts,
    JSONExtractString(Body, 'type') AS watch_type,
    JSONExtractString(Body, 'object', 'type') AS type,
    JSONExtractString(Body, 'object', 'reason') AS reason,
    JSONExtractString(Body, 'object', 'regarding', 'kind') AS kind,
    JSONExtractString(Body, 'object', 'regarding', 'namespace') AS namespace,
    JSONExtractString(Body, 'object', 'regarding', 'name') AS name,
    JSONExtractString(Body, 'object', 'note') AS message,
    toUInt32(greatest(JSONExtractUInt(Body, 'object', 'series', 'count'),
                      JSONExtractUInt(Body, 'object', 'deprecatedCount'), 1)) AS count,
    JSONExtractString(Body, 'object', 'reportingController') AS reporting_controller,
    ResourceAttributes['k8s.cluster.name'] AS cluster,
    JSONExtractString(Body, 'object', 'metadata', 'uid') AS uid
FROM otel_logs
WHERE ServiceName = 'k8s-events' AND JSONHas(Body, 'object');

-- Model pod startup phases: scheduled -> image pulled -> container started -> engine ready (startup event).
CREATE VIEW IF NOT EXISTS pod_startups AS
SELECT
    e.namespace AS namespace,
    e.name AS pod,
    minIf(e.ts, e.reason = 'Scheduled') AS scheduled,
    minIf(e.ts, e.reason IN ('Pulled', 'Pulling')) AS pulled,
    minIf(e.ts, e.reason = 'Started') AS started,
    any(d.started_at) AS engine_ready
FROM k8s_events AS e
INNER JOIN (SELECT pod, started_at FROM model_deployments FINAL) AS d ON d.pod = e.name
WHERE e.kind = 'Pod'
GROUP BY namespace, pod;
