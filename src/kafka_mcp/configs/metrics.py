import json
import math
import os
from dataclasses import dataclass, field
from typing import Dict, Optional
from urllib.parse import urlsplit


def _optional_env(name: str) -> Optional[str]:
    value = os.environ.get(name)
    return value if value not in (None, "") else None


def _parse_bool(name: str, value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _validate_endpoint(name: str, endpoint: str) -> str:
    endpoint = endpoint.strip()
    parsed = urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"JMX Exporter endpoint '{name}' must be an http or https URL")
    if parsed.username or parsed.password:
        raise ValueError(
            f"JMX Exporter endpoint '{name}' cannot contain credentials; "
            "use the JMX authentication environment variables"
        )
    return endpoint


def _parse_endpoints(raw_value: Optional[str]) -> Dict[str, str]:
    if raw_value is None:
        return {}

    raw_value = raw_value.strip()
    if not raw_value:
        return {}

    if raw_value.startswith(("{", "[")):
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"KAFKA_JMX_EXPORTER_ENDPOINTS must be valid JSON: {exc.msg}"
            ) from exc

        if isinstance(parsed, dict):
            endpoint_items = list(parsed.items())
        elif isinstance(parsed, list):
            endpoint_items = [
                (f"broker-{index}", endpoint)
                for index, endpoint in enumerate(parsed, start=1)
            ]
        else:
            raise ValueError(
                "KAFKA_JMX_EXPORTER_ENDPOINTS JSON must be an object or array"
            )
    else:
        entries = [entry.strip() for entry in raw_value.split(",") if entry.strip()]
        named_entries = ["=" in entry for entry in entries]
        if any(named_entries) and not all(named_entries):
            raise ValueError(
                "KAFKA_JMX_EXPORTER_ENDPOINTS entries must either all use name=url "
                "or all be plain URLs"
            )
        if all(named_entries):
            endpoint_items = [entry.split("=", 1) for entry in entries]
        else:
            endpoint_items = [
                (f"broker-{index}", endpoint)
                for index, endpoint in enumerate(entries, start=1)
            ]

    endpoints = {}
    for raw_name, raw_endpoint in endpoint_items:
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ValueError("JMX Exporter endpoint names must be non-empty strings")
        if not isinstance(raw_endpoint, str):
            raise ValueError("JMX Exporter endpoints must be strings")
        name = raw_name.strip()
        if name in endpoints:
            raise ValueError(f"Duplicate JMX Exporter endpoint name: {name}")
        endpoints[name] = _validate_endpoint(name, raw_endpoint)

    return endpoints


def _parse_headers(raw_value: Optional[str]) -> Dict[str, str]:
    if raw_value is None:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"KAFKA_JMX_HEADERS_JSON must be valid JSON: {exc.msg}"
        ) from exc
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in parsed.items()
    ):
        raise ValueError("KAFKA_JMX_HEADERS_JSON must be a JSON object of strings")
    if any(
        "\r" in key or "\n" in key or "\r" in value or "\n" in value
        for key, value in parsed.items()
    ):
        raise ValueError("KAFKA_JMX_HEADERS_JSON cannot contain newlines")
    return parsed


@dataclass
class JmxExporterConfig:
    endpoints: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 5.0
    verify_ssl: bool = True
    ca_location: Optional[str] = None
    client_certificate_location: Optional[str] = None
    client_key_location: Optional[str] = None
    client_key_password: Optional[str] = field(default=None, repr=False)
    username: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    bearer_token: Optional[str] = field(default=None, repr=False)
    headers: Dict[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("KAFKA_JMX_TIMEOUT_SECONDS must be greater than zero")
        if bool(self.username) != bool(self.password):
            raise ValueError(
                "KAFKA_JMX_USERNAME and KAFKA_JMX_PASSWORD must be configured together"
            )
        if self.bearer_token and self.username:
            raise ValueError(
                "JMX Exporter Basic and Bearer authentication cannot be combined"
            )
        if bool(self.client_certificate_location) != bool(self.client_key_location):
            raise ValueError(
                "KAFKA_JMX_CLIENT_CERTIFICATE_LOCATION and "
                "KAFKA_JMX_CLIENT_KEY_LOCATION must be configured together"
            )
        if any(key.lower() == "authorization" for key in self.headers) and (
            self.bearer_token or self.username
        ):
            raise ValueError(
                "Authorization in KAFKA_JMX_HEADERS_JSON cannot be combined with "
                "JMX authentication environment variables"
            )

    @property
    def configured(self) -> bool:
        return bool(self.endpoints)


def get_jmx_exporter_config() -> JmxExporterConfig:
    timeout_value = os.environ.get("KAFKA_JMX_TIMEOUT_SECONDS", "5")
    try:
        timeout_seconds = float(timeout_value)
    except ValueError as exc:
        raise ValueError("KAFKA_JMX_TIMEOUT_SECONDS must be a number") from exc

    return JmxExporterConfig(
        endpoints=_parse_endpoints(_optional_env("KAFKA_JMX_EXPORTER_ENDPOINTS")),
        timeout_seconds=timeout_seconds,
        verify_ssl=_parse_bool(
            "KAFKA_JMX_VERIFY_SSL",
            _optional_env("KAFKA_JMX_VERIFY_SSL"),
            True,
        ),
        ca_location=_optional_env("KAFKA_JMX_CA_LOCATION"),
        client_certificate_location=_optional_env(
            "KAFKA_JMX_CLIENT_CERTIFICATE_LOCATION"
        ),
        client_key_location=_optional_env("KAFKA_JMX_CLIENT_KEY_LOCATION"),
        client_key_password=_optional_env("KAFKA_JMX_CLIENT_KEY_PASSWORD"),
        username=_optional_env("KAFKA_JMX_USERNAME"),
        password=_optional_env("KAFKA_JMX_PASSWORD"),
        bearer_token=_optional_env("KAFKA_JMX_BEARER_TOKEN"),
        headers=_parse_headers(_optional_env("KAFKA_JMX_HEADERS_JSON")),
    )
