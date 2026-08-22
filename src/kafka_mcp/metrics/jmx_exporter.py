import asyncio
import math
import ssl
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlsplit, urlunsplit

import httpx
from prometheus_client.parser import text_string_to_metric_families

from ..configs.metrics import JmxExporterConfig


@dataclass(frozen=True)
class MetricSample:
    name: str
    labels: Dict[str, str]
    value: float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "labels": dict(self.labels),
            "value": self.value,
        }


@dataclass(frozen=True)
class ScrapeResult:
    broker_id: str
    endpoint: str
    reachable: bool
    metrics: Sequence[MetricSample]
    error: Optional[str] = None


def parse_prometheus_metrics(payload: str) -> List[MetricSample]:
    samples = []
    try:
        families = text_string_to_metric_families(payload)
        for family in families:
            for sample in family.samples:
                value = float(sample.value)
                if math.isfinite(value):
                    samples.append(
                        MetricSample(
                            name=sample.name,
                            labels=dict(sample.labels),
                            value=value,
                        )
                    )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Prometheus metrics payload: {exc}") from exc
    return samples


def _display_endpoint(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _build_ssl_verification(config: JmxExporterConfig) -> Union[bool, ssl.SSLContext]:
    has_custom_tls = bool(
        config.ca_location
        or config.client_certificate_location
        or not config.verify_ssl
    )
    if not has_custom_tls:
        return True

    context = ssl.create_default_context(cafile=config.ca_location)
    if not config.verify_ssl:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    if config.client_certificate_location and config.client_key_location:
        context.load_cert_chain(
            certfile=config.client_certificate_location,
            keyfile=config.client_key_location,
            password=config.client_key_password,
        )
    return context


class JmxExporterClient:
    def __init__(
        self,
        config: JmxExporterConfig,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        self.config = config
        self.http_client = http_client

    def _client_options(self) -> Dict[str, Any]:
        headers = {"Accept": "text/plain", **self.config.headers}
        if self.config.bearer_token:
            headers["Authorization"] = f"Bearer {self.config.bearer_token}"

        auth = None
        if self.config.username and self.config.password:
            auth = httpx.BasicAuth(self.config.username, self.config.password)

        return {
            "auth": auth,
            "follow_redirects": True,
            "headers": headers,
            "timeout": self.config.timeout_seconds,
            "verify": _build_ssl_verification(self.config),
        }

    async def _scrape(
        self, client: httpx.AsyncClient, broker_id: str, endpoint: str
    ) -> ScrapeResult:
        display_endpoint = _display_endpoint(endpoint)
        try:
            response = await client.get(endpoint)
            response.raise_for_status()
            metrics = parse_prometheus_metrics(response.text)
            return ScrapeResult(
                broker_id=broker_id,
                endpoint=display_endpoint,
                reachable=True,
                metrics=metrics,
            )
        except httpx.HTTPStatusError as exc:
            error = f"HTTP {exc.response.status_code} from JMX Exporter endpoint"
        except httpx.TimeoutException:
            error = "JMX Exporter request timed out"
        except httpx.RequestError as exc:
            error = f"JMX Exporter request failed: {type(exc).__name__}"
        except ValueError as exc:
            error = str(exc)

        return ScrapeResult(
            broker_id=broker_id,
            endpoint=display_endpoint,
            reachable=False,
            metrics=[],
            error=error,
        )

    async def scrape_all(self, broker_id: Optional[str] = None) -> List[ScrapeResult]:
        if broker_id is not None:
            if broker_id not in self.config.endpoints:
                raise ValueError(
                    f"Unknown JMX Exporter broker_id '{broker_id}'. Available: "
                    + ", ".join(sorted(self.config.endpoints))
                )
            endpoints = {broker_id: self.config.endpoints[broker_id]}
        else:
            endpoints = self.config.endpoints

        async def scrape_with(client: httpx.AsyncClient) -> List[ScrapeResult]:
            return list(
                await asyncio.gather(
                    *(
                        self._scrape(client, name, endpoint)
                        for name, endpoint in endpoints.items()
                    )
                )
            )

        if self.http_client is not None:
            return await scrape_with(self.http_client)

        try:
            async with httpx.AsyncClient(**self._client_options()) as client:
                return await scrape_with(client)
        except (OSError, ssl.SSLError, TypeError, ValueError) as exc:
            return [
                ScrapeResult(
                    broker_id=name,
                    endpoint=_display_endpoint(endpoint),
                    reachable=False,
                    metrics=[],
                    error=f"Failed to initialize JMX Exporter HTTP client: {exc}",
                )
                for name, endpoint in endpoints.items()
            ]
