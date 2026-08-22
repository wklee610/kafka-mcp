import os
import unittest
from unittest.mock import patch

from kafka_mcp.configs.config import KafkaConfig, get_kafka_config


class KafkaConfigTests(unittest.TestCase):
    def load_config(self, **environment):
        with patch.dict(os.environ, environment, clear=True):
            return get_kafka_config()

    def test_bootstrap_servers_remains_the_only_required_setting(self):
        config = self.load_config(KAFKA_BOOTSTRAP_SERVERS="localhost:9092")

        self.assertEqual(
            config.to_client_config(),
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "kafka-mcp",
            },
        )

    def test_missing_bootstrap_servers_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "KAFKA_BOOTSTRAP_SERVERS"):
                get_kafka_config()

    def test_plain_and_scram_credentials_are_mapped_and_normalized(self):
        for mechanism in ("plain", "scram_sha_256", "SCRAM-SHA-512"):
            with self.subTest(mechanism=mechanism):
                config = self.load_config(
                    KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
                    KAFKA_SECURITY_PROTOCOL="sasl-ssl",
                    KAFKA_SASL_MECHANISM=mechanism,
                    KAFKA_SASL_USERNAME="alice",
                    KAFKA_SASL_PASSWORD="secret",
                )
                client_config = config.to_client_config()

                self.assertEqual(client_config["security.protocol"], "SASL_SSL")
                self.assertIn(
                    client_config["sasl.mechanism"],
                    {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"},
                )
                self.assertEqual(client_config["sasl.username"], "alice")
                self.assertEqual(client_config["sasl.password"], "secret")

    def test_sasl_requires_a_mechanism_and_plain_requires_credentials(self):
        with self.assertRaisesRegex(ValueError, "KAFKA_SASL_MECHANISM"):
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                security_protocol="SASL_SSL",
            )

        with self.assertRaisesRegex(ValueError, "requires KAFKA_SASL_USERNAME"):
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
            )

    def test_gssapi_properties_are_supported(self):
        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
            KAFKA_SECURITY_PROTOCOL="SASL_SSL",
            KAFKA_SASL_MECHANISM="GSSAPI",
            KAFKA_SASL_KERBEROS_SERVICE_NAME="kafka",
            KAFKA_SASL_KERBEROS_PRINCIPAL="client@example.com",
            KAFKA_SASL_KERBEROS_KEYTAB="/run/secrets/client.keytab",
            KAFKA_SASL_KERBEROS_KINIT_CMD="kinit -R",
            KAFKA_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN="60000",
        ).to_client_config()

        self.assertEqual(config["sasl.mechanism"], "GSSAPI")
        self.assertEqual(config["sasl.kerberos.service.name"], "kafka")
        self.assertEqual(config["sasl.kerberos.principal"], "client@example.com")
        self.assertEqual(config["sasl.kerberos.keytab"], "/run/secrets/client.keytab")
        self.assertEqual(config["sasl.kerberos.kinit.cmd"], "kinit -R")
        self.assertEqual(config["sasl.kerberos.min.time.before.relogin"], "60000")

    def test_oauthbearer_oidc_properties_are_supported(self):
        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
            KAFKA_SECURITY_PROTOCOL="SASL_SSL",
            KAFKA_SASL_MECHANISM="OAUTHBEARER",
            KAFKA_SASL_OAUTHBEARER_METHOD="oidc",
            KAFKA_SASL_OAUTHBEARER_CLIENT_ID="client-id",
            KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET="client-secret",
            KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL="https://idp/token",
            KAFKA_SASL_OAUTHBEARER_SCOPE="kafka",
            KAFKA_SASL_OAUTHBEARER_EXTENSIONS="logicalCluster=lkc-1",
        ).to_client_config()

        self.assertEqual(config["sasl.oauthbearer.method"], "oidc")
        self.assertEqual(config["sasl.oauthbearer.client.id"], "client-id")
        self.assertEqual(config["sasl.oauthbearer.client.secret"], "client-secret")
        self.assertEqual(
            config["sasl.oauthbearer.token.endpoint.url"], "https://idp/token"
        )

    def test_oauthbearer_default_mode_requires_config(self):
        with self.assertRaisesRegex(ValueError, "OAUTHBEARER_CONFIG"):
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                security_protocol="SASL_SSL",
                sasl_mechanism="OAUTHBEARER",
            )

        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
            KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT",
            KAFKA_SASL_MECHANISM="OAUTHBEARER",
            KAFKA_SASL_OAUTHBEARER_CONFIG="principal=developer",
            KAFKA_ENABLE_SASL_OAUTHBEARER_UNSECURE_JWT="true",
        ).to_client_config()
        self.assertIs(config["enable.sasl.oauthbearer.unsecure.jwt"], True)

    def test_oauthbearer_metadata_and_assertion_flows_are_not_blocked(self):
        metadata_config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
            KAFKA_SECURITY_PROTOCOL="SASL_SSL",
            KAFKA_SASL_MECHANISM="OAUTHBEARER",
            KAFKA_SASL_OAUTHBEARER_METHOD="OIDC",
            KAFKA_SASL_OAUTHBEARER_METADATA_AUTHENTICATION_TYPE="AZURE_IMDS",
        ).to_client_config()
        assertion_config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
            KAFKA_SECURITY_PROTOCOL="SASL_SSL",
            KAFKA_SASL_MECHANISM="OAUTHBEARER",
            KAFKA_SASL_OAUTHBEARER_METHOD="oidc",
            KAFKA_CLIENT_CONFIG_JSON=(
                '{"sasl.oauthbearer.grant.type":'
                '"urn:ietf:params:oauth:grant-type:jwt-bearer",'
                '"sasl.oauthbearer.client.id":"client-id",'
                '"sasl.oauthbearer.token.endpoint.url":"https://idp/token",'
                '"sasl.oauthbearer.assertion.private.key.file":"/key.pem"}'
            ),
        ).to_client_config()

        self.assertEqual(
            metadata_config["sasl.oauthbearer.metadata.authentication.type"],
            "azure_imds",
        )
        self.assertEqual(
            assertion_config["sasl.oauthbearer.grant.type"],
            "urn:ietf:params:oauth:grant-type:jwt-bearer",
        )

    def test_librdkafka_security_aliases_are_supported(self):
        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
            KAFKA_CLIENT_CONFIG_JSON=(
                '{"security.protocol":"SASL_SSL",'
                '"sasl.mechanisms":"OAUTHBEARER",'
                '"sasl.oauthbearer.method":"oidc",'
                '"sasl.oauthbearer.client.credentials.client.id":"client-id",'
                '"sasl.oauthbearer.client.credentials.client.secret":"secret",'
                '"sasl.oauthbearer.token.endpoint.url":"https://idp/token"}'
            ),
        ).to_client_config()

        self.assertEqual(config["sasl.mechanisms"], "OAUTHBEARER")
        self.assertEqual(
            config["sasl.oauthbearer.client.credentials.client.id"], "client-id"
        )

    def test_explicit_client_credentials_grant_still_requires_credentials(self):
        with self.assertRaisesRegex(ValueError, "CLIENT_SECRET"):
            self.load_config(
                KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
                KAFKA_SECURITY_PROTOCOL="SASL_SSL",
                KAFKA_SASL_MECHANISM="OAUTHBEARER",
                KAFKA_SASL_OAUTHBEARER_METHOD="oidc",
                KAFKA_SASL_OAUTHBEARER_CLIENT_ID="client-id",
                KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL="https://idp/token",
                KAFKA_CLIENT_CONFIG_JSON=(
                    '{"sasl.oauthbearer.grant.type":"client_credentials"}'
                ),
            )

    def test_tls_and_mtls_properties_are_supported(self):
        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka.example.com:9093",
            KAFKA_SECURITY_PROTOCOL="SSL",
            KAFKA_SSL_CA_LOCATION="/certs/ca.pem",
            KAFKA_SSL_CERTIFICATE_LOCATION="/certs/client.pem",
            KAFKA_SSL_KEY_LOCATION="/certs/client.key",
            KAFKA_SSL_KEY_PASSWORD="secret",
            KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM="https",
            KAFKA_SSL_CRL_LOCATION="/certs/ca.crl",
        ).to_client_config()

        self.assertEqual(config["ssl.ca.location"], "/certs/ca.pem")
        self.assertEqual(config["ssl.certificate.location"], "/certs/client.pem")
        self.assertEqual(config["ssl.key.location"], "/certs/client.key")
        self.assertEqual(config["ssl.key.password"], "secret")
        self.assertEqual(config["ssl.endpoint.identification.algorithm"], "https")
        self.assertEqual(config["ssl.crl.location"], "/certs/ca.crl")

    def test_mtls_certificate_and_key_must_be_paired(self):
        with self.assertRaisesRegex(ValueError, "must be configured together"):
            KafkaConfig(
                bootstrap_servers="localhost:9092",
                security_protocol="SSL",
                ssl_certificate_location="/certs/client.pem",
            )

    def test_additional_json_supports_advanced_properties_and_core_wins(self):
        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="real:9092",
            KAFKA_CLIENT_ID="real-client",
            KAFKA_CLIENT_CONFIG_JSON=(
                '{"bootstrap.servers":"ignored:9092","client.id":"ignored",'
                '"metadata.broker.list":"also-ignored:9092",'
                '"socket.keepalive.enable":true,"metadata.max.age.ms":5000}'
            ),
        ).to_client_config()

        self.assertEqual(config["bootstrap.servers"], "real:9092")
        self.assertEqual(config["client.id"], "real-client")
        self.assertNotIn("metadata.broker.list", config)
        self.assertIs(config["socket.keepalive.enable"], True)
        self.assertEqual(config["metadata.max.age.ms"], 5000)

    def test_additional_json_can_supply_security_properties(self):
        config = self.load_config(
            KAFKA_BOOTSTRAP_SERVERS="kafka:9093",
            KAFKA_CLIENT_CONFIG_JSON=(
                '{"security.protocol":"SASL_SSL","sasl.mechanism":"PLAIN",'
                '"sasl.username":"alice","sasl.password":"secret"}'
            ),
        ).to_client_config()

        self.assertEqual(config["security.protocol"], "SASL_SSL")
        self.assertEqual(config["sasl.mechanism"], "PLAIN")

    def test_invalid_additional_json_is_rejected(self):
        for value in ("[]", '{"nested":{"bad":true}}', "not-json"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.load_config(
                        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
                        KAFKA_CLIENT_CONFIG_JSON=value,
                    )

    def test_secrets_are_not_in_config_repr(self):
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="alice",
            sasl_password="do-not-print",
        )

        self.assertNotIn("do-not-print", repr(config))


if __name__ == "__main__":
    unittest.main()
