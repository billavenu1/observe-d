"""OTel SDK setup shared by the simulators (traces + logs over OTLP gRPC to the agent collector)."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry._logs import Logger, SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import Span, Tracer


@dataclass
class Telemetry:
    tracer: Tracer
    logger: Logger
    tracer_provider: TracerProvider
    logger_provider: LoggerProvider

    def log(
        self,
        body: str,
        attributes: dict[str, str | int | float | bool],
        *,
        span: Span | None = None,
        severity: SeverityNumber = SeverityNumber.INFO,
        event_name: str | None = None,
        timestamp_ns: int | None = None,
    ) -> None:
        ctx = trace.set_span_in_context(span) if span is not None else None
        self.logger.emit(
            timestamp=timestamp_ns if timestamp_ns is not None else time.time_ns(),
            context=ctx,
            severity_number=severity,
            severity_text=severity.name,
            body=body,
            attributes=attributes,
            event_name=event_name,
        )

    def shutdown(self) -> None:
        self.tracer_provider.shutdown()
        self.logger_provider.shutdown()


def k8s_resource(service_name: str, extra: dict[str, str] | None = None) -> Resource:
    attrs: dict[str, str] = {"service.name": service_name}
    for key, env in (
        ("k8s.pod.name", "POD_NAME"),
        ("k8s.namespace.name", "POD_NAMESPACE"),
        ("k8s.node.name", "NODE_NAME"),
    ):
        if os.environ.get(env):
            attrs[key] = os.environ[env]
    attrs.update(extra or {})
    return Resource.create(attrs)


def setup(
    resource: Resource,
    span_exporter: SpanExporter | None = None,
    log_exporter: LogRecordExporter | None = None,
) -> Telemetry:
    """Exporters default to OTLP gRPC at $OTEL_EXPORTER_OTLP_ENDPOINT (tests pass in-memory exporters)."""
    if span_exporter is None or log_exporter is None:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        span_exporter = span_exporter or OTLPSpanExporter(insecure=True)
        log_exporter = log_exporter or OTLPLogExporter(insecure=True)

    tp = TracerProvider(resource=resource)
    tp.add_span_processor(BatchSpanProcessor(span_exporter, max_queue_size=20000, max_export_batch_size=2048))
    lp = LoggerProvider(resource=resource)
    lp.add_log_record_processor(BatchLogRecordProcessor(log_exporter, max_queue_size=20000, max_export_batch_size=2048))
    return Telemetry(tp.get_tracer("observe-d.sim"), lp.get_logger("observe-d.sim"), tp, lp)
