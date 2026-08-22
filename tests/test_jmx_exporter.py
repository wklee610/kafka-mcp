import unittest

import httpx

from kafka_mcp.configs.metrics import JmxExporterConfig
from kafka_mcp.metrics.jmx_exporter import (
    JmxExporterClient,
    parse_prometheus_metrics,
)


METRICS_PAYLOAD = """
# HELP kafka_server_replicamanager_underreplicatedpartitions Replication health.
# TYPE kafka_server_replicamanager_underreplicatedpartitions gauge
kafka_server_replicamanager_underreplicatedpartitions 0
# TYPE kafka_server_brokertopicmetrics_messagesin_total counter
kafka_server_brokertopicmetrics_messagesin_total{topic="orders"} 42
# TYPE ignored_non_finite gauge
ignored_non_finite NaN
"""


class PrometheusParserTests(unittest.TestCase):
    def test_valid_samples_are_parsed_and_non_finite_values_are_skipped(self):
        samples = parse_prometheus_metrics(METRICS_PAYLOAD)

        self.assertEqual(len(samples), 2)
        self.assertEqual(
            samples[0].name,
            "kafka_server_replicamanager_underreplicatedpartitions",
        )
        self.assertEqual(samples[1].labels, {"topic": "orders"})
        self.assertEqual(samples[1].value, 42)

    def test_invalid_payload_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid Prometheus"):
            parse_prometheus_metrics('broken{label="unterminated 1')


class JmxExporterClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_endpoints_are_scraped_with_the_prometheus_parser(self):
        async def handler(request):
            self.assertEqual(request.url.path, "/metrics")
            return httpx.Response(200, text=METRICS_PAYLOAD)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as http_client:
            client = JmxExporterClient(
                JmxExporterConfig(
                    endpoints={
                        "broker-1": "http://broker-1:7071/metrics?token=do-not-print"
                    }
                ),
                http_client=http_client,
            )
            results = await client.scrape_all()

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].reachable)
        self.assertEqual(len(results[0].metrics), 2)

    async def test_http_failures_are_returned_per_endpoint(self):
        transport = httpx.MockTransport(lambda request: httpx.Response(503))
        async with httpx.AsyncClient(transport=transport) as http_client:
            client = JmxExporterClient(
                JmxExporterConfig(
                    endpoints={"broker-1": "http://broker-1:7071/metrics"}
                ),
                http_client=http_client,
            )
            results = await client.scrape_all()

        self.assertFalse(results[0].reachable)
        self.assertIn("503", results[0].error)
        self.assertNotIn("do-not-print", results[0].error)
        self.assertNotIn("do-not-print", results[0].endpoint)

    async def test_unknown_broker_is_rejected(self):
        client = JmxExporterClient(
            JmxExporterConfig(endpoints={"broker-1": "http://broker-1:7071/metrics"})
        )

        with self.assertRaisesRegex(ValueError, "Unknown JMX Exporter broker_id"):
            await client.scrape_all("missing")


if __name__ == "__main__":
    unittest.main()
