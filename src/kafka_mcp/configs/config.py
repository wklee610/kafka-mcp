import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


SECURITY_PROTOCOLS = {
    "PLAINTEXT",
    "SSL",
    "SASL_PLAINTEXT",
    "SASL_SSL",
}
SASL_MECHANISMS = {
    "PLAIN",
    "SCRAM-SHA-256",
    "SCRAM-SHA-512",
    "GSSAPI",
    "OAUTHBEARER",
}


def _optional_env(name: str) -> Optional[str]:
    value = os.environ.get(name)
    return value if value not in (None, "") else None


def _optional_bool_env(name: str) -> Optional[bool]:
    value = _optional_env(name)
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _parse_client_config_json(raw_value: Optional[str]) -> Dict[str, Any]:
    if raw_value is None:
        return {}

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"KAFKA_CLIENT_CONFIG_JSON must be valid JSON: {exc.msg}"
        ) from exc

    if not isinstance(parsed, dict):
        raise ValueError("KAFKA_CLIENT_CONFIG_JSON must contain a JSON object")

    for key, value in parsed.items():
        if not isinstance(key, str):
            raise ValueError("KAFKA_CLIENT_CONFIG_JSON keys must be strings")
        if value is None or isinstance(value, (dict, list)):
            raise ValueError(
                f"KAFKA_CLIENT_CONFIG_JSON value for '{key}' must be a scalar"
            )

    return parsed


def _normalize_security_protocol(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().upper().replace("-", "_")
    if normalized not in SECURITY_PROTOCOLS:
        choices = ", ".join(sorted(SECURITY_PROTOCOLS))
        raise ValueError(f"KAFKA_SECURITY_PROTOCOL must be one of: {choices}")
    return normalized


def _normalize_sasl_mechanism(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().upper().replace("_", "-")
    if normalized not in SASL_MECHANISMS:
        choices = ", ".join(sorted(SASL_MECHANISMS))
        raise ValueError(f"KAFKA_SASL_MECHANISM must be one of: {choices}")
    return normalized


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    client_id: str = "kafka-mcp"
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = field(default=None, repr=False)
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[str] = field(default=None, repr=False)
    ssl_endpoint_identification_algorithm: Optional[str] = None
    ssl_crl_location: Optional[str] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_principal: Optional[str] = None
    sasl_kerberos_keytab: Optional[str] = None
    sasl_kerberos_kinit_cmd: Optional[str] = None
    sasl_kerberos_min_time_before_relogin: Optional[str] = None
    sasl_oauthbearer_method: Optional[str] = None
    sasl_oauthbearer_metadata_authentication_type: Optional[str] = None
    sasl_oauthbearer_client_id: Optional[str] = None
    sasl_oauthbearer_client_secret: Optional[str] = field(default=None, repr=False)
    sasl_oauthbearer_token_endpoint_url: Optional[str] = None
    sasl_oauthbearer_scope: Optional[str] = None
    sasl_oauthbearer_extensions: Optional[str] = None
    sasl_oauthbearer_config: Optional[str] = field(default=None, repr=False)
    enable_sasl_oauthbearer_unsecure_jwt: Optional[bool] = None
    additional_config: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        self.bootstrap_servers = self.bootstrap_servers.strip()
        self.client_id = self.client_id.strip()
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")
        if not self.client_id:
            raise ValueError("KAFKA_CLIENT_ID cannot be empty")

        self.security_protocol = _normalize_security_protocol(self.security_protocol)
        self.sasl_mechanism = _normalize_sasl_mechanism(self.sasl_mechanism)
        if self.sasl_oauthbearer_method:
            self.sasl_oauthbearer_method = self.sasl_oauthbearer_method.strip().lower()
        if self.sasl_oauthbearer_metadata_authentication_type:
            self.sasl_oauthbearer_metadata_authentication_type = (
                self.sasl_oauthbearer_metadata_authentication_type.strip().lower()
            )
        self._validate_security_config()

    def _effective(
        self, field_value: Optional[str], *config_keys: str
    ) -> Optional[str]:
        value = field_value
        if value is None:
            for config_key in config_keys:
                additional_value = self.additional_config.get(config_key)
                if additional_value is not None:
                    value = str(additional_value)
                    break
        return value

    def _validate_security_config(self) -> None:
        protocol = (
            _normalize_security_protocol(
                self._effective(self.security_protocol, "security.protocol")
            )
            or "PLAINTEXT"
        )
        mechanism = _normalize_sasl_mechanism(
            self._effective(
                self.sasl_mechanism,
                "sasl.mechanism",
                "sasl.mechanisms",
            )
        )

        if protocol.startswith("SASL_") and mechanism is None:
            raise ValueError(
                "KAFKA_SASL_MECHANISM is required when KAFKA_SECURITY_PROTOCOL uses SASL"
            )
        if mechanism is not None and not protocol.startswith("SASL_"):
            raise ValueError(
                "KAFKA_SECURITY_PROTOCOL must be SASL_PLAINTEXT or SASL_SSL "
                "when a SASL mechanism is configured"
            )

        if mechanism in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
            username = self._effective(self.sasl_username, "sasl.username")
            password = self._effective(self.sasl_password, "sasl.password")
            if not username or not password:
                raise ValueError(
                    f"SASL {mechanism} requires KAFKA_SASL_USERNAME and "
                    "KAFKA_SASL_PASSWORD"
                )

        if mechanism == "OAUTHBEARER":
            method = self._effective(
                self.sasl_oauthbearer_method, "sasl.oauthbearer.method"
            )
            method = method.lower() if method else "default"
            if method not in {"default", "oidc"}:
                raise ValueError(
                    "KAFKA_SASL_OAUTHBEARER_METHOD must be default or oidc"
                )
            if method == "oidc":
                metadata_authentication_type = self._effective(
                    self.sasl_oauthbearer_metadata_authentication_type,
                    "sasl.oauthbearer.metadata.authentication.type",
                )
                metadata_authentication_type = (
                    metadata_authentication_type.lower()
                    if metadata_authentication_type
                    else None
                )
                raw_grant_type = self.additional_config.get(
                    "sasl.oauthbearer.grant.type"
                )
                grant_type = str(raw_grant_type).lower() if raw_grant_type else None
                uses_metadata = metadata_authentication_type not in {None, "", "none"}
                uses_assertion = (
                    grant_type == "urn:ietf:params:oauth:grant-type:jwt-bearer"
                )
                if not uses_metadata and not uses_assertion:
                    required = {
                        "KAFKA_SASL_OAUTHBEARER_CLIENT_ID": self._effective(
                            self.sasl_oauthbearer_client_id,
                            "sasl.oauthbearer.client.id",
                            "sasl.oauthbearer.client.credentials.client.id",
                        ),
                        "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET": self._effective(
                            self.sasl_oauthbearer_client_secret,
                            "sasl.oauthbearer.client.secret",
                            "sasl.oauthbearer.client.credentials.client.secret",
                        ),
                        "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL": self._effective(
                            self.sasl_oauthbearer_token_endpoint_url,
                            "sasl.oauthbearer.token.endpoint.url",
                        ),
                    }
                    missing = [name for name, value in required.items() if not value]
                    if missing:
                        raise ValueError(
                            "OAUTHBEARER OIDC requires: " + ", ".join(missing)
                        )
            else:
                oauth_config = self._effective(
                    self.sasl_oauthbearer_config, "sasl.oauthbearer.config"
                )
                unsecure_jwt = self.enable_sasl_oauthbearer_unsecure_jwt
                if unsecure_jwt is None:
                    unsecure_jwt = self.additional_config.get(
                        "enable.sasl.oauthbearer.unsecure.jwt"
                    )
                has_callback = callable(self.additional_config.get("oauth_cb"))
                if not has_callback and not (oauth_config and unsecure_jwt is True):
                    raise ValueError(
                        "OAUTHBEARER default mode requires an oauth_cb callback or "
                        "KAFKA_SASL_OAUTHBEARER_CONFIG together with "
                        "KAFKA_ENABLE_SASL_OAUTHBEARER_UNSECURE_JWT=true"
                    )

        certificate = self._effective(
            self.ssl_certificate_location, "ssl.certificate.location"
        )
        key = self._effective(self.ssl_key_location, "ssl.key.location")
        if bool(certificate) != bool(key):
            raise ValueError(
                "KAFKA_SSL_CERTIFICATE_LOCATION and KAFKA_SSL_KEY_LOCATION "
                "must be configured together"
            )

    def to_client_config(self) -> Dict[str, Any]:
        config = dict(self.additional_config)
        config.pop("metadata.broker.list", None)
        config.update(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": self.client_id,
            }
        )

        optional_values = {
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "ssl.ca.location": self.ssl_ca_location,
            "ssl.certificate.location": self.ssl_certificate_location,
            "ssl.key.location": self.ssl_key_location,
            "ssl.key.password": self.ssl_key_password,
            "ssl.endpoint.identification.algorithm": (
                self.ssl_endpoint_identification_algorithm
            ),
            "ssl.crl.location": self.ssl_crl_location,
            "sasl.kerberos.service.name": self.sasl_kerberos_service_name,
            "sasl.kerberos.principal": self.sasl_kerberos_principal,
            "sasl.kerberos.keytab": self.sasl_kerberos_keytab,
            "sasl.kerberos.kinit.cmd": self.sasl_kerberos_kinit_cmd,
            "sasl.kerberos.min.time.before.relogin": (
                self.sasl_kerberos_min_time_before_relogin
            ),
            "sasl.oauthbearer.method": self.sasl_oauthbearer_method,
            "sasl.oauthbearer.metadata.authentication.type": (
                self.sasl_oauthbearer_metadata_authentication_type
            ),
            "sasl.oauthbearer.client.id": self.sasl_oauthbearer_client_id,
            "sasl.oauthbearer.client.secret": self.sasl_oauthbearer_client_secret,
            "sasl.oauthbearer.token.endpoint.url": (
                self.sasl_oauthbearer_token_endpoint_url
            ),
            "sasl.oauthbearer.scope": self.sasl_oauthbearer_scope,
            "sasl.oauthbearer.extensions": self.sasl_oauthbearer_extensions,
            "sasl.oauthbearer.config": self.sasl_oauthbearer_config,
            "enable.sasl.oauthbearer.unsecure.jwt": (
                self.enable_sasl_oauthbearer_unsecure_jwt
            ),
        }
        config.update(
            {key: value for key, value in optional_values.items() if value is not None}
        )
        return config


def get_kafka_config() -> KafkaConfig:
    bootstrap_servers = _optional_env("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

    return KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        client_id=os.environ.get("KAFKA_CLIENT_ID", "kafka-mcp"),
        security_protocol=_optional_env("KAFKA_SECURITY_PROTOCOL"),
        sasl_mechanism=_optional_env("KAFKA_SASL_MECHANISM"),
        sasl_username=_optional_env("KAFKA_SASL_USERNAME"),
        sasl_password=_optional_env("KAFKA_SASL_PASSWORD"),
        ssl_ca_location=_optional_env("KAFKA_SSL_CA_LOCATION"),
        ssl_certificate_location=_optional_env("KAFKA_SSL_CERTIFICATE_LOCATION"),
        ssl_key_location=_optional_env("KAFKA_SSL_KEY_LOCATION"),
        ssl_key_password=_optional_env("KAFKA_SSL_KEY_PASSWORD"),
        ssl_endpoint_identification_algorithm=_optional_env(
            "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM"
        ),
        ssl_crl_location=_optional_env("KAFKA_SSL_CRL_LOCATION"),
        sasl_kerberos_service_name=_optional_env("KAFKA_SASL_KERBEROS_SERVICE_NAME"),
        sasl_kerberos_principal=_optional_env("KAFKA_SASL_KERBEROS_PRINCIPAL"),
        sasl_kerberos_keytab=_optional_env("KAFKA_SASL_KERBEROS_KEYTAB"),
        sasl_kerberos_kinit_cmd=_optional_env("KAFKA_SASL_KERBEROS_KINIT_CMD"),
        sasl_kerberos_min_time_before_relogin=_optional_env(
            "KAFKA_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN"
        ),
        sasl_oauthbearer_method=_optional_env("KAFKA_SASL_OAUTHBEARER_METHOD"),
        sasl_oauthbearer_metadata_authentication_type=_optional_env(
            "KAFKA_SASL_OAUTHBEARER_METADATA_AUTHENTICATION_TYPE"
        ),
        sasl_oauthbearer_client_id=_optional_env("KAFKA_SASL_OAUTHBEARER_CLIENT_ID"),
        sasl_oauthbearer_client_secret=_optional_env(
            "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET"
        ),
        sasl_oauthbearer_token_endpoint_url=_optional_env(
            "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL"
        ),
        sasl_oauthbearer_scope=_optional_env("KAFKA_SASL_OAUTHBEARER_SCOPE"),
        sasl_oauthbearer_extensions=_optional_env("KAFKA_SASL_OAUTHBEARER_EXTENSIONS"),
        sasl_oauthbearer_config=_optional_env("KAFKA_SASL_OAUTHBEARER_CONFIG"),
        enable_sasl_oauthbearer_unsecure_jwt=_optional_bool_env(
            "KAFKA_ENABLE_SASL_OAUTHBEARER_UNSECURE_JWT"
        ),
        additional_config=_parse_client_config_json(
            _optional_env("KAFKA_CLIENT_CONFIG_JSON")
        ),
    )
