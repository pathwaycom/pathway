# Copyright © 2024 Pathway

"""
EXPERIMENTAL
This module provides functionality for monitoring, telemetry, and quality of service.

When enabled, telemetry data, including pipeline metadata and process metrics,
is gathered and dispatched to specified endpoints utilizing the OpenTelemetry protocol.
Logs are not included in the telemetry data sent to the telemetry server.

Additionally, the module permits the configuration of an extra OpenTelemetry Collector endpoint
or another endpoint compatible with the OTLP (via gRPC), supporting logs, metrics, and tracing.
Data forwarded to the monitoring server will include both logs and telemetry data.

By default, both telemetry and monitoring are turned OFF.
"""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from functools import cached_property

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import (
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    SERVICE_NAMESPACE,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from pathway.internals import api
from pathway.internals.trace import trace_user_frame

propagator = TraceContextTextMapPropagator()


# shush strange warnings from openetelemetry itself to not spoil the logs
logging.getLogger("opentelemetry").setLevel(logging.CRITICAL)


class Telemetry:
    config: api.TelemetryConfig
    tracer: trace.Tracer

    def __init__(self, telemetry_config: api.TelemetryConfig) -> None:
        self.config = telemetry_config
        self.tracer = self._init_tracer()

    @cached_property
    def _resource(self) -> Resource:
        return Resource(
            attributes={
                SERVICE_NAME: self.config.service_name or "",
                SERVICE_VERSION: self.config.service_version or "",
                SERVICE_NAMESPACE: self.config.service_namespace or "",
                SERVICE_INSTANCE_ID: self.config.service_instance_id or "",
                "run.id": self.config.run_id,
                "python.version": sys.version,
                "license.key": self.config.license_key or "",
            }
        )

    @classmethod
    @trace_user_frame
    def create(
        cls,
        run_id: str,
        license_key: str | None = None,
        monitoring_server: str | None = None,
    ) -> Telemetry:
        config = api.TelemetryConfig.create(
            run_id=run_id,
            license_key=license_key,
            monitoring_server=monitoring_server,
        )
        return cls(config)

    @contextmanager
    def with_logging_handler(self):
        logging_handler = self._logging_handler()
        root_logger = logging.getLogger()
        try:
            root_logger.addHandler(logging_handler)
            yield
        finally:
            logging_handler.flush()
            root_logger.removeHandler(logging_handler)

    def _logging_handler(self) -> logging.Handler:
        if len(self.config.logging_servers) > 0:
            logger_provider = LoggerProvider(resource=self._resource)
            for endpoint in self.config.logging_servers:
                exporter = OTLPLogExporter(endpoint=endpoint)
                logger_provider.add_log_record_processor(
                    BatchLogRecordProcessor(exporter)
                )
            return LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        else:
            return logging.NullHandler()

    def _init_tracer(self) -> trace.Tracer:
        if len(self.config.tracing_servers) > 0:
            trace_provider = TracerProvider(resource=self._resource)
            for endpoint in self.config.tracing_servers:
                exporter = OTLPSpanExporter(endpoint=endpoint)
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


XPACKS = {
    "pathway.xpacks.spatial",
    "pathway.xpacks.llm",
    "pathway.xpacks.connectors.sharepoint",
}


def get_imported_xpacks() -> list[str]:
    return sorted(XPACKS.intersection(sys.modules.keys()))
