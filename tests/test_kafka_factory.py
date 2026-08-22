import unittest
from unittest.mock import patch

from kafka_mcp.configs.config import KafkaConfig
from kafka_mcp.connections.kafka import KafkaClientFactory


class KafkaClientFactoryTests(unittest.TestCase):
    def setUp(self):
        self.config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="alice",
            sasl_password="secret",
            additional_config={
                "group.id": "unsafe-group",
                "auto.offset.reset": "none",
                "enable.auto.commit": True,
            },
        )

    @patch("kafka_mcp.connections.kafka.AdminClient")
    def test_admin_client_receives_the_common_security_config(self, admin_client):
        factory = KafkaClientFactory(self.config)

        factory.create_admin_client()

        passed_config = admin_client.call_args.args[0]
        self.assertEqual(passed_config["security.protocol"], "SASL_SSL")
        self.assertEqual(passed_config["sasl.mechanism"], "PLAIN")
        self.assertEqual(passed_config["sasl.username"], "alice")
        self.assertEqual(passed_config["sasl.password"], "secret")

    @patch("kafka_mcp.connections.kafka.Producer")
    def test_producer_is_cached_and_receives_the_common_config(self, producer):
        factory = KafkaClientFactory(self.config)

        first = factory.create_producer()
        second = factory.create_producer()

        self.assertIs(first, second)
        producer.assert_called_once()
        self.assertEqual(producer.call_args.args[0]["bootstrap.servers"], "kafka:9093")

    @patch("kafka_mcp.connections.kafka.Consumer")
    def test_consumer_safety_properties_override_additional_config(self, consumer):
        factory = KafkaClientFactory(self.config)

        factory.create_consumer("requested-group", "latest")

        passed_config = consumer.call_args.args[0]
        self.assertEqual(passed_config["group.id"], "requested-group")
        self.assertEqual(passed_config["auto.offset.reset"], "latest")
        self.assertIs(passed_config["enable.auto.commit"], False)
        self.assertEqual(passed_config["security.protocol"], "SASL_SSL")


if __name__ == "__main__":
    unittest.main()
