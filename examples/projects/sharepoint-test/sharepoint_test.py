import pathway as pw
from pathway.xpacks.connectors import sharepoint

if __name__ == "__main__":
    table = sharepoint.read(
        url=...,  # Example: "https://company.sharepoint.com/sites/MySite"
        root_path=...,  # Example: "Shared Documents/Indexer"
        tenant=...,  # Normally uuid4
        client_id=...,  # Normally uuid4
        thumbprint=...,  # Certificate's thumbprint
        cert_path=...,  # A path to the .pem-file for the certificate
        mode="static",
    )
    pw.io.jsonlines.write(table, "contents.txt")
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
