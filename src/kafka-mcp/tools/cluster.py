from typing import (
    Dict, 
    Any, 
    List
)
from connections.kafka import get_kafka_factory

def describe_cluster() -> Dict[str, Any]:
    """Returns information about the Kafka cluster (brokers, controller)."""
    admin_client = get_kafka_factory().create_admin_client()
    cluster_metadata = admin_client.list_topics(timeout=10)
    
    brokers = [
        {"id": b.id, "host": b.host, "port": b.port}
        for b in cluster_metadata.brokers.values()
    ]
    
    return {
        "cluster_id": cluster_metadata.cluster_id,
        "controller_id": cluster_metadata.controller_id,
        "brokers": brokers,
        "topic_count": len(cluster_metadata.topics),
    }

def describe_brokers() -> List[Dict[str, Any]]:
    """Returns a list of brokers with their details."""
    admin_client = get_kafka_factory().create_admin_client()
    cluster_metadata = admin_client.list_topics(timeout=10)
    
    return [
        {"id": b.id, "host": b.host, "port": b.port}
        for b in cluster_metadata.brokers.values()
    ]