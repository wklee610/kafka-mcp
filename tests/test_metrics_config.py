import os
import unittest
from unittest.mock import patch

from kafka_mcp.configs.metrics import get_jmx_exporter_config


class JmxExporterConfigTests(unittest.TestCase):
    def load_config(self, **environment):
        with patch.dict(os.environ, environment, clear=True):
            return get_jmx_exporter_config()

    def test_metrics_are_optional(self):
        config = self.load_config()

        self.assertFalse(config.configured)
        self.assertEqual(config.endpoints, {})

    def test_endpoint_object_array_and_named_list_are_supported(self):
        values = (
            '{"one":"http://one:7071/metrics"}',
            '["http://one:7071/metrics"]',
            "one=http://one:7071/metrics",
            "http://one:7071/metrics",
        )

        for value in values:
            with self.subTest(value=value):
                config = self.load_config(KAFKA_JMX_EXPORTER_ENDPOINTS=value)
                self.assertTrue(config.configured)
                self.assertEqual(
                    list(config.endpoints.values()),
                    [value.split("=")[-1]]
                    if not value.startswith(("{", "["))
                    else ["http://one:7071/metrics"],
                )

    def test_https_auth_tls_and_headers_are_supported(self):
        config = self.load_config(
            KAFKA_JMX_EXPORTER_ENDPOINTS="https://broker:7071/metrics",
            KAFKA_JMX_TIMEOUT_SECONDS="2.5",
            KAFKA_JMX_VERIFY_SSL="false",
            KAFKA_JMX_CA_LOCATION="/certs/ca.pem",
            KAFKA_JMX_CLIENT_CERTIFICATE_LOCATION="/certs/client.pem",
            KAFKA_JMX_CLIENT_KEY_LOCATION="/certs/client.key",
            KAFKA_JMX_CLIENT_KEY_PASSWORD="secret",
            KAFKA_JMX_BEARER_TOKEN="token",
            KAFKA_JMX_HEADERS_JSON='{"X-Tenant":"platform"}',
        )

        self.assertEqual(config.timeout_seconds, 2.5)
        self.assertFalse(config.verify_ssl)
        self.assertEqual(config.headers, {"X-Tenant": "platform"})
        self.assertNotIn("secret", repr(config))
        self.assertNotIn("token", repr(config))

    def test_invalid_endpoint_and_auth_combinations_are_rejected(self):
        cases = (
            {"KAFKA_JMX_EXPORTER_ENDPOINTS": "ftp://broker/metrics"},
            {
                "KAFKA_JMX_EXPORTER_ENDPOINTS": "http://broker/metrics",
                "KAFKA_JMX_USERNAME": "alice",
            },
            {
                "KAFKA_JMX_EXPORTER_ENDPOINTS": "http://broker/metrics",
                "KAFKA_JMX_USERNAME": "alice",
                "KAFKA_JMX_PASSWORD": "secret",
                "KAFKA_JMX_BEARER_TOKEN": "token",
            },
            {
                "KAFKA_JMX_EXPORTER_ENDPOINTS": "http://broker/metrics",
                "KAFKA_JMX_VERIFY_SSL": "sometimes",
            },
            {
                "KAFKA_JMX_EXPORTER_ENDPOINTS": "http://broker/metrics",
                "KAFKA_JMX_TIMEOUT_SECONDS": "nan",
            },
            {
                "KAFKA_JMX_EXPORTER_ENDPOINTS": "http://broker/metrics",
                "KAFKA_JMX_HEADERS_JSON": '{"X-Tenant":"bad\\nvalue"}',
            },
        )

        for environment in cases:
            with self.subTest(environment=environment):
                with self.assertRaises(ValueError):
                    self.load_config(**environment)


if __name__ == "__main__":
    unittest.main()
