from __future__ import annotations

import sys

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.util.types import AttributeValue

from pathway.internals import api

propagator = TraceContextTextMapPropagator()


class Telemetry:
    config: api.TelemetryConfig
    tracer: trace.Tracer

    def __init__(self, telemetry_config: api.TelemetryConfig) -> None:
        self.config = telemetry_config
        self.tracer = self._init_tracer()

    @classmethod
    def create(
        cls, license_key: str | None = None, telemetry_server: str | None = None
    ) -> Telemetry:
        config = api.TelemetryConfig.create(
            license_key=license_key, telemetry_server=telemetry_server
        )
        return cls(config)

    def _init_tracer(self) -> trace.Tracer:
        if self.config.telemetry_enabled:
            trace_provider = TracerProvider(
                resource=Resource(
                    attributes={
                        SERVICE_NAME: self.config.service_name or "",
                        SERVICE_VERSION: self.config.service_version or "",
                        "run.id": self.config.run_id,
                        "python.version": sys.version,
                        "otel.scope.name": "python",
                    }
                )
            )
            exporter = OTLPSpanExporter(endpoint=self.config.telemetry_server_endpoint)
            trace_provider.add_span_processor(BatchSpanProcessor(exporter))
            return trace_provider.get_tracer("pathway-tracer")
        else:
            return trace.NoOpTracer()


def get_current_context() -> tuple[Context, str | None]:
    carrier: dict[str, str | list[str]] = {}
    propagator.inject(carrier)
    context = propagator.extract(carrier)
    trace_parent = carrier.get("traceparent", None)
    assert trace_parent is None or isinstance(trace_parent, str)
    return context, trace_parent


def event(name: str, attributes: dict[str, AttributeValue]):
    span = trace.get_current_span()
    span.add_event(name, attributes)
