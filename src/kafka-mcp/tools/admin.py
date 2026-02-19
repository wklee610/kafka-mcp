from typing import (
    List, 
    Dict, 
    Optional
)
from confluent_kafka.admin import (
    NewTopic, 
    NewPartitions, 
    ConfigResource, 
    ResourceType
)
from connections.kafka import get_kafka_factory

def list_topics() -> List[str]:
    """Returns a list of all topic names in the cluster."""
    admin_client = get_kafka_factory().create_admin_client()
    cluster_metadata = admin_client.list_topics(timeout=10)
    return list(cluster_metadata.topics.keys())

def describe_topic(
    topic_name: str
) -> Dict:
    """Returns detailed information about a specific topic."""
    admin_client = get_kafka_factory().create_admin_client()
    cluster_metadata = admin_client.list_topics(topic=topic_name, timeout=10)
    
    if topic_name not in cluster_metadata.topics:
        return {"error": f"Topic '{topic_name}' not found"}
        
    topic_metadata = cluster_metadata.topics[topic_name]
    
    partitions = []
    for p in topic_metadata.partitions.values():
        partitions.append({
            "id": p.id,
            "leader": p.leader,
            "replicas": p.replicas,
            "isrs": p.isrs,
        })
        
    return {
        "name": topic_name,
        "partitions": partitions,
        "partition_count": len(partitions),
    }

def create_topic(
    topic_name: str, 
    num_partitions: int = 1, 
    replication_factor: int = 1, 
    config: Optional[Dict[str, str]] = None
) -> str:
    """Creates a new topic."""
    admin_client = get_kafka_factory().create_admin_client()
    new_topics = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor, config=config)]
    
    fs = admin_client.create_topics(new_topics)
    
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            return f"Topic '{topic}' created successfully"
        except Exception as e:
            return f"Failed to create topic '{topic}': {e}"
    return "Unknown error"

def delete_topic(topic_name: str) -> str:
    """Deletes a topic."""
    admin_client = get_kafka_factory().create_admin_client()
    fs = admin_client.delete_topics([topic_name])
    
    for topic, f in fs.items():
        try:
            f.result()
            return f"Topic '{topic}' deleted successfully"
        except Exception as e:
            return f"Failed to delete topic '{topic}': {e}"
    return "Unknown error"

def create_partitions(
    topic_name: str, 
    new_total_count: int
) -> str:
    """Increases the number of partitions for a topic. Note: Partition count can only be increased, not decreased."""
    admin_client = get_kafka_factory().create_admin_client()
    new_parts = [NewPartitions(topic_name, new_total_count)]
    
    fs = admin_client.create_partitions(new_parts)
    
    for topic, f in fs.items():
        try:
            f.result()
            return f"Partitions for '{topic}' increased to {new_total_count}"
        except Exception as e:
            return f"Failed to create partitions for '{topic}': {e}"
    return "Unknown error"

def describe_configs(
    resource_type: str, 
    resource_name: str
) -> Dict:
    """
    Get dynamic configs for a resource.
    resource_type: "topic", "broker", or "group" (case insensitive)
    """
    res_type_map = {
        "topic": ResourceType.TOPIC,
        "broker": ResourceType.BROKER,
        "group": ResourceType.GROUP
    }
    
    r_type = res_type_map.get(resource_type.lower())
    if r_type is None:
        return {"error": f"Invalid resource type. Must be one of {list(res_type_map.keys())}"}
    
    admin_client = get_kafka_factory().create_admin_client()
    resource = ConfigResource(r_type, resource_name)
    
    fs = admin_client.describe_configs([resource])
    
    result = {}
    for res, f in fs.items():
        try:
            configs = f.result()
            for key, entry in configs.items():
                result[key] = {
                    "value": entry.value,
                    "source": str(entry.source),
                    "is_read_only": entry.is_read_only,
                    "is_default": entry.is_default
                }
        except Exception as e:
            return {"error": f"Failed to describe configs for {resource_name}: {e}"}
            
    return result

def alter_configs(
    resource_type: str, 
    resource_name: str, 
    configs: Dict[str, str]
) -> str:
    """
    Update dynamic configs for a resource.
    resource_type: "topic", "broker", or "group"
    configs: Dictionary of config key-value pairs
    """
    res_type_map = {
        "topic": ResourceType.TOPIC,
        "broker": ResourceType.BROKER,
        "group": ResourceType.GROUP
    }
    
    r_type = res_type_map.get(resource_type.lower())
    if r_type is None:
        return f"Invalid resource type. Must be one of {list(res_type_map.keys())}"

    admin_client = get_kafka_factory().create_admin_client()
    resource = ConfigResource(r_type, resource_name)
    
    for k, v in configs.items():
        resource.set_config(k, v)
    
    fs = admin_client.alter_configs([resource])
    
    for res, f in fs.items():
        try:
            f.result()
            return f"Configs for {resource_type} '{resource_name}' updated successfully"
        except Exception as e:
            return f"Failed to alter configs for {resource_name}: {e}"
    return "Unknown error"