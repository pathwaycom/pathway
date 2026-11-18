# Copyright © 2026 Pathway

import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2 as metrics_pb,
    metrics_service_pb2_grpc as metrics_grpc,
)

import pathway as pw
from pathway.internals.config import local_pathway_config


class MetricsCollector(metrics_grpc.MetricsServiceServicer):
    def __init__(self, reject_first):
        self.reject_first = reject_first
        self.rejected = []
        self.accepted = []

    def Export(self, request, context):
        if self.reject_first and not self.rejected:
            self.rejected.append(request)
            context.abort(grpc.StatusCode.UNAVAILABLE, "collector is temporarily busy")
        self.accepted.append(request)
        return metrics_pb.ExportMetricsServiceResponse()


def tls_credentials(tmp_path, monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
        )
        .sign(key, hashes.SHA256())
        .public_bytes(serialization.Encoding.PEM)
    )
    cert_path = tmp_path / "collector.pem"
    cert_path.write_bytes(cert)
    monkeypatch.setenv("SSL_CERT_FILE", str(cert_path))
    private_key = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return grpc.ssl_server_credentials([(private_key, cert)])


@pytest.mark.parametrize("tls", [False, True])
@pytest.mark.parametrize("reject_first", [False, True])
def test_rust_metrics_flush_and_retry(
    tls, reject_first, tmp_path, monkeypatch, tcp_port
):
    collector = MetricsCollector(reject_first)
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    metrics_grpc.add_MetricsServiceServicer_to_server(collector, server)
    address = f"localhost:{tcp_port}"
    if tls:
        assert server.add_secure_port(address, tls_credentials(tmp_path, monkeypatch))
    else:
        assert server.add_insecure_port(address)
    server.start()
    try:

        class Schema(pw.Schema):
            value: int

        class Source(pw.io.python.ConnectorSubject):
            def run(self):
                # Let the Rust observer record its initial process metrics.
                # The normal export interval is 60s, so this pipeline exercises
                # the final flush at shutdown, before a periodic export is due.
                time.sleep(2)
                self.next(value=42)

        with local_pathway_config() as config:
            config.license_key = os.environ.get("PATHWAY_LICENSE_KEY")
            config.monitoring_server = f"{'https' if tls else 'http'}://{address}"
            config.metrics_reader_interval_secs = None
            table = pw.io.python.read(Source(), schema=Schema)
            pw.io.null.write(table)
            pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        assert collector.accepted, "Rust metrics must arrive before pw.run returns"
        metrics = [
            metric
            for request in collector.accepted
            for resource in request.resource_metrics
            for scope in resource.scope_metrics
            if scope.scope.name == "pathway-stats"
            for metric in scope.metrics
        ]
        memory = [metric for metric in metrics if metric.name == "process.memory.usage"]
        assert memory
        assert any(
            point.as_int > 0 for metric in memory for point in metric.gauge.data_points
        )
        if reject_first:
            # A retry sends the same sample. A second collection during shutdown
            # would have different timestamps and must not mask a lost export.
            assert collector.rejected == collector.accepted[:1]
    finally:
        server.stop(0).wait()
