from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..configs.metrics import JmxExporterConfig, get_jmx_exporter_config
from ..metrics.jmx_exporter import JmxExporterClient, MetricSample, ScrapeResult


MetricCatalog = Dict[str, Tuple[Sequence[str], str]]

BROKER_METRIC_CATALOG: MetricCatalog = {
    "under_replicated_partitions": (
        (
            "kafka_server_replicamanager_underreplicatedpartitions",
            "kafka_server_replicamanager_underreplicatedpartitions_value",
        ),
        "sum",
    ),
    "under_min_isr_partition_count": (
        (
            "kafka_server_replicamanager_underminisrpartitioncount",
            "kafka_server_replicamanager_underminisrpartitioncount_value",
        ),
        "sum",
    ),
    "offline_partitions_count": (
        (
            "kafka_controller_kafkacontroller_offlinepartitionscount",
            "kafka_controller_kafkacontroller_offlinepartitionscount_value",
        ),
        "sum",
    ),
    "offline_log_directory_count": (
        (
            "kafka_log_logmanager_offlinelogdirectorycount",
            "kafka_log_logmanager_offlinelogdirectorycount_value",
        ),
        "sum",
    ),
    "active_controller_count": (
        (
            "kafka_controller_kafkacontroller_activecontrollercount",
            "kafka_controller_kafkacontroller_activecontrollercount_value",
        ),
        "sum",
    ),
    "fenced_broker_count": (
        (
            "kafka_controller_kafkacontroller_fencedbrokercount",
            "kafka_controller_kafkacontroller_fencedbrokercount_value",
        ),
        "sum",
    ),
    "partition_count": (
        (
            "kafka_server_replicamanager_partitioncount",
            "kafka_server_replicamanager_partitioncount_value",
        ),
        "sum",
    ),
    "leader_count": (
        (
            "kafka_server_replicamanager_leadercount",
            "kafka_server_replicamanager_leadercount_value",
        ),
        "sum",
    ),
    "request_handler_idle_percent": (
        (
            "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
            "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_percent",
        ),
        "min",
    ),
    "network_processor_idle_percent": (
        (
            "kafka_network_socketserver_networkprocessoravgidlepercent",
            "kafka_network_socketserver_networkprocessoravgidlepercent_percent",
        ),
        "min",
    ),
}

TOPIC_METRIC_CATALOG: MetricCatalog = {
    "messages_in_total": (
        (
            "kafka_server_brokertopicmetrics_messagesin_total",
            "kafka_server_brokertopicmetrics_messagesinpersec_count",
        ),
        "sum",
    ),
    "bytes_in_total": (
        (
            "kafka_server_brokertopicmetrics_bytesin_total",
            "kafka_server_brokertopicmetrics_bytesinpersec_count",
        ),
        "sum",
    ),
    "bytes_out_total": (
        (
            "kafka_server_brokertopicmetrics_bytesout_total",
            "kafka_server_brokertopicmetrics_bytesoutpersec_count",
        ),
        "sum",
    ),
    "failed_produce_requests_total": (
        (
            "kafka_server_brokertopicmetrics_failedproducerequests_total",
            "kafka_server_brokertopicmetrics_failedproducerequestspersec_count",
        ),
        "sum",
    ),
    "failed_fetch_requests_total": (
        (
            "kafka_server_brokertopicmetrics_failedfetchrequests_total",
            "kafka_server_brokertopicmetrics_failedfetchrequestspersec_count",
        ),
        "sum",
    ),
    "total_produce_requests_total": (
        (
            "kafka_server_brokertopicmetrics_totalproducerequests_total",
            "kafka_server_brokertopicmetrics_totalproducerequestspersec_count",
        ),
        "sum",
    ),
    "total_fetch_requests_total": (
        (
            "kafka_server_brokertopicmetrics_totalfetchrequests_total",
            "kafka_server_brokertopicmetrics_totalfetchrequestspersec_count",
        ),
        "sum",
    ),
}

HEALTH_SIGNAL_NAMES = (
    "under_replicated_partitions",
    "under_min_isr_partition_count",
    "offline_partitions_count",
    "offline_log_directory_count",
    "fenced_broker_count",
)
CORE_HEALTH_SIGNAL_NAMES = (
    "under_replicated_partitions",
    "offline_partitions_count",
)


def _compact_metric_name(name: str) -> str:
    return "".join(character for character in name.lower() if character.isalnum())


def _matches_alias(name: str, aliases: Sequence[str]) -> bool:
    compact_name = _compact_metric_name(name)
    return any(
        compact_name == _compact_metric_name(alias)
        or compact_name.endswith(_compact_metric_name(alias))
        for alias in aliases
    )


def _json_number(value: float) -> Any:
    return int(value) if value.is_integer() else value


def summarize_metrics(
    samples: Sequence[MetricSample], catalog: MetricCatalog
) -> Dict[str, Optional[Any]]:
    summary: Dict[str, Optional[Any]] = {}
    for output_name, (aliases, aggregation) in catalog.items():
        values = [
            sample.value for sample in samples if _matches_alias(sample.name, aliases)
        ]
        if not values:
            summary[output_name] = None
        elif aggregation == "min":
            summary[output_name] = _json_number(min(values))
        else:
            summary[output_name] = _json_number(sum(values))
    return summary


def _has_label(sample: MetricSample, label_name: str, label_value: str) -> bool:
    return any(
        name.lower() == label_name.lower() and value == label_value
        for name, value in sample.labels.items()
    )


def _matches_filters(
    sample: MetricSample,
    metric_name: Optional[str],
    label_filters: Optional[Dict[str, str]],
) -> bool:
    if metric_name and _compact_metric_name(metric_name) not in _compact_metric_name(
        sample.name
    ):
        return False
    if label_filters:
        for name, value in label_filters.items():
            if not _has_label(sample, name, value):
                return False
    return True


def _serialize_raw_metrics(
    results: Sequence[ScrapeResult],
    metric_name: Optional[str],
    label_filters: Optional[Dict[str, str]],
    limit: int,
) -> Dict[str, Any]:
    matched = []
    for result in results:
        if not result.reachable:
            continue
        for sample in result.metrics:
            if _matches_filters(sample, metric_name, label_filters):
                matched.append({"broker_id": result.broker_id, **sample.as_dict()})

    return {
        "raw_metrics": matched[:limit],
        "raw_metric_count": len(matched),
        "raw_metrics_truncated": len(matched) > limit,
    }


def _load_config() -> Tuple[Optional[JmxExporterConfig], Optional[Dict[str, Any]]]:
    try:
        config = get_jmx_exporter_config()
    except ValueError as exc:
        return None, {
            "configured": False,
            "status": "configuration_error",
            "error": str(exc),
        }
    if not config.configured:
        return None, {
            "configured": False,
            "status": "not_configured",
            "error": "Set KAFKA_JMX_EXPORTER_ENDPOINTS to enable JMX metrics",
        }
    return config, None


def _broker_result(result: ScrapeResult) -> Dict[str, Any]:
    response = {
        "broker_id": result.broker_id,
        "endpoint": result.endpoint,
        "reachable": result.reachable,
        "sample_count": len(result.metrics),
    }
    if result.reachable:
        response["metrics"] = summarize_metrics(result.metrics, BROKER_METRIC_CATALOG)
    else:
        response["error"] = result.error
    return response


async def describe_cluster_health() -> Dict[str, Any]:
    """Summarizes Kafka health signals exposed by Prometheus JMX Exporter."""
    config, error = _load_config()
    if error:
        return error
    assert config is not None

    results = await JmxExporterClient(config).scrape_all()
    reachable = [result for result in results if result.reachable]
    unreachable = [result for result in results if not result.reachable]
    if not reachable:
        return {
            "configured": True,
            "status": "unavailable",
            "reachable_brokers": 0,
            "configured_brokers": len(results),
            "problems": ["No JMX Exporter endpoint could be reached"],
            "brokers": [_broker_result(result) for result in results],
        }

    broker_summaries = {
        result.broker_id: summarize_metrics(result.metrics, BROKER_METRIC_CATALOG)
        for result in reachable
    }
    signal_values: Dict[str, Any] = {}
    signal_coverage: Dict[str, int] = {}
    for signal_name in (*HEALTH_SIGNAL_NAMES, "active_controller_count"):
        values = [
            summary[signal_name]
            for summary in broker_summaries.values()
            if summary[signal_name] is not None
        ]
        signal_values[signal_name] = sum(values) if values else None
        signal_coverage[signal_name] = len(values)

    problems: List[str] = []
    if unreachable:
        problems.append(
            "Unreachable JMX Exporter endpoints: "
            + ", ".join(result.broker_id for result in unreachable)
        )
    for signal_name in HEALTH_SIGNAL_NAMES:
        value = signal_values[signal_name]
        if value is not None and value > 0:
            problems.append(f"{signal_name} is {value}")

    active_controller_count = signal_values["active_controller_count"]
    if active_controller_count is not None and active_controller_count != 1:
        problems.append(
            f"active_controller_count is {active_controller_count}, expected 1"
        )

    missing_core_signals = [
        name for name in CORE_HEALTH_SIGNAL_NAMES if signal_values[name] is None
    ]
    if problems:
        status = "degraded"
    elif missing_core_signals:
        status = "unknown"
        problems.append(
            "Exporter is reachable but core Kafka health metrics are missing: "
            + ", ".join(missing_core_signals)
        )
    else:
        status = "healthy"

    return {
        "configured": True,
        "status": status,
        "reachable_brokers": len(reachable),
        "configured_brokers": len(results),
        "signals": signal_values,
        "signal_coverage": signal_coverage,
        "problems": problems,
        "brokers": [_broker_result(result) for result in results],
    }


async def get_broker_metrics(
    broker_id: Optional[str] = None,
    metric_name: Optional[str] = None,
    label_filters: Optional[Dict[str, str]] = None,
    include_raw: bool = False,
    limit: int = 200,
) -> Dict[str, Any]:
    """Returns curated or filtered raw JMX Exporter metrics for one or all brokers."""
    if limit < 1 or limit > 1000:
        return {"error": "limit must be between 1 and 1000"}

    config, error = _load_config()
    if error:
        return error
    assert config is not None

    try:
        results = await JmxExporterClient(config).scrape_all(broker_id)
    except ValueError as exc:
        return {"configured": True, "status": "error", "error": str(exc)}

    response: Dict[str, Any] = {
        "configured": True,
        "brokers": [_broker_result(result) for result in results],
    }
    if include_raw or metric_name or label_filters:
        response.update(
            _serialize_raw_metrics(results, metric_name, label_filters, limit)
        )
    return response


async def get_topic_metrics(
    topic_name: str,
    broker_id: Optional[str] = None,
    include_raw: bool = False,
    limit: int = 200,
) -> Dict[str, Any]:
    """Returns JMX Exporter metrics labeled for a Kafka topic."""
    if not topic_name:
        return {"error": "topic_name is required"}
    if limit < 1 or limit > 1000:
        return {"error": "limit must be between 1 and 1000"}

    config, error = _load_config()
    if error:
        return error
    assert config is not None

    try:
        results = await JmxExporterClient(config).scrape_all(broker_id)
    except ValueError as exc:
        return {"configured": True, "status": "error", "error": str(exc)}

    brokers = []
    topic_sample_count = 0
    for result in results:
        broker_response: Dict[str, Any] = {
            "broker_id": result.broker_id,
            "endpoint": result.endpoint,
            "reachable": result.reachable,
        }
        if result.reachable:
            topic_samples = [
                sample
                for sample in result.metrics
                if _has_label(sample, "topic", topic_name)
            ]
            topic_sample_count += len(topic_samples)
            broker_response["sample_count"] = len(topic_samples)
            broker_response["metrics"] = summarize_metrics(
                topic_samples, TOPIC_METRIC_CATALOG
            )
        else:
            broker_response["sample_count"] = 0
            broker_response["error"] = result.error
        brokers.append(broker_response)

    response: Dict[str, Any] = {
        "configured": True,
        "topic": topic_name,
        "sample_count": topic_sample_count,
        "brokers": brokers,
    }
    if include_raw:
        response.update(
            _serialize_raw_metrics(
                results,
                metric_name=None,
                label_filters={"topic": topic_name},
                limit=limit,
            )
        )
    return response
