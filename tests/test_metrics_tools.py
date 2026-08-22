import unittest
from unittest.mock import patch

from kafka_mcp.configs.metrics import JmxExporterConfig
from kafka_mcp.metrics.jmx_exporter import (
    MetricSample,
    ScrapeResult,
)
from kafka_mcp.tools.metrics import (
    describe_cluster_health,
    get_broker_metrics,
    get_topic_metrics,
)


def sample(name, value, **labels):
    return MetricSample(name=name, labels=labels, value=float(value))


class FakeJmxExporterClient:
    results = []

    def __init__(self, config):
        self.config = config

    async def scrape_all(self, broker_id=None):
        if broker_id is None:
            return list(self.results)
        matches = [result for result in self.results if result.broker_id == broker_id]
        if not matches:
            raise ValueError(f"Unknown JMX Exporter broker_id '{broker_id}'")
        return matches


class MetricsToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = JmxExporterConfig(
            endpoints={
                "broker-1": "http://broker-1:7071/metrics",
                "broker-2": "http://broker-2:7071/metrics",
            }
        )
        FakeJmxExporterClient.results = [
            ScrapeResult(
                broker_id="broker-1",
                endpoint="http://broker-1:7071/metrics",
                reachable=True,
                metrics=[
                    sample(
                        "kafka_server_replicamanager_underreplicatedpartitions",
                        0,
                    ),
                    sample(
                        "kafka_controller_kafkacontroller_activecontrollercount",
                        1,
                    ),
                    sample(
                        "kafka_controller_kafkacontroller_offlinepartitionscount",
                        0,
                    ),
                    sample(
                        "kafka_server_brokertopicmetrics_messagesin_total",
                        42,
                        topic="orders",
                    ),
                    sample(
                        "kafka_server_brokertopicmetrics_bytesin_total",
                        1024,
                        topic="orders",
                    ),
                ],
            ),
            ScrapeResult(
                broker_id="broker-2",
                endpoint="http://broker-2:7071/metrics",
                reachable=True,
                metrics=[
                    sample(
                        "kafka_server_replicamanager_underreplicatedpartitions",
                        0,
                    ),
                    sample(
                        "kafka_controller_kafkacontroller_activecontrollercount",
                        0,
                    ),
                ],
            ),
        ]
        self.config_patch = patch(
            "kafka_mcp.tools.metrics.get_jmx_exporter_config",
            return_value=self.config,
        )
        self.client_patch = patch(
            "kafka_mcp.tools.metrics.JmxExporterClient",
            FakeJmxExporterClient,
        )
        self.config_patch.start()
        self.client_patch.start()

    def tearDown(self):
        self.client_patch.stop()
        self.config_patch.stop()

    async def test_cluster_health_is_healthy_when_signals_are_clear(self):
        result = await describe_cluster_health()

        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["signals"]["under_replicated_partitions"], 0)
        self.assertEqual(result["signals"]["active_controller_count"], 1)

    async def test_cluster_health_is_degraded_for_replication_or_reachability(self):
        FakeJmxExporterClient.results[0] = ScrapeResult(
            broker_id="broker-1",
            endpoint="http://broker-1:7071/metrics",
            reachable=True,
            metrics=[
                sample(
                    "kafka_server_replicamanager_underreplicatedpartitions",
                    2,
                )
            ],
        )
        FakeJmxExporterClient.results[1] = ScrapeResult(
            broker_id="broker-2",
            endpoint="http://broker-2:7071/metrics",
            reachable=False,
            metrics=[],
            error="timeout",
        )

        result = await describe_cluster_health()

        self.assertEqual(result["status"], "degraded")
        self.assertTrue(
            any("under_replicated_partitions" in item for item in result["problems"])
        )
        self.assertTrue(any("Unreachable" in item for item in result["problems"]))

    async def test_broker_metrics_can_return_filtered_raw_samples(self):
        result = await get_broker_metrics(
            broker_id="broker-1",
            metric_name="messages_in",
            label_filters={"topic": "orders"},
        )

        self.assertEqual(result["raw_metric_count"], 1)
        self.assertEqual(result["raw_metrics"][0]["value"], 42)
        self.assertEqual(result["raw_metrics"][0]["broker_id"], "broker-1")

    async def test_topic_metrics_are_filtered_and_summarized(self):
        result = await get_topic_metrics("orders", include_raw=True)

        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["brokers"][0]["metrics"]["messages_in_total"], 42)
        self.assertEqual(result["raw_metric_count"], 2)

    async def test_cluster_health_is_unknown_when_core_metrics_are_missing(self):
        FakeJmxExporterClient.results = [
            ScrapeResult(
                broker_id="broker-1",
                endpoint="http://broker-1:7071/metrics",
                reachable=True,
                metrics=[sample("unrelated_metric", 1)],
            )
        ]

        result = await describe_cluster_health()

        self.assertEqual(result["status"], "unknown")
        self.assertTrue(
            any("core Kafka health metrics" in p for p in result["problems"])
        )

    async def test_metrics_tools_remain_available_without_configuration(self):
        with patch(
            "kafka_mcp.tools.metrics.get_jmx_exporter_config",
            return_value=JmxExporterConfig(),
        ):
            result = await describe_cluster_health()

        self.assertEqual(result["status"], "not_configured")
        self.assertFalse(result["configured"])


if __name__ == "__main__":
    unittest.main()
