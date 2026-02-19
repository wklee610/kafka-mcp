import time
from typing import (
    List, 
    Dict, 
    Optional, 
    Any
)
from confluent_kafka import (
    Consumer, 
    TopicPartition, 
    OFFSET_BEGINNING, 
    OFFSET_END
)
from connections.kafka import get_kafka_factory

def list_consumer_groups() -> List[Dict[str, Any]]:
    """Lists all consumer groups."""
    admin_client = get_kafka_factory().create_admin_client()
    try:
        groups_result = admin_client.list_consumer_groups()
        groups_future = groups_result.result() 
        return [
            {
                "group_id": g.group_id,
                "is_simple": g.is_simple_consumer_group,
                "state": str(g.state) if g.state else "UNKNOWN"
            }
            for g in groups_future.valid
        ]
    except AttributeError:
         pass
    except Exception as e:
        return [{"error": str(e)}]
    return []

def describe_consumer_group(group_id: str) -> Dict[str, Any]:
    """Describes a specific consumer group."""
    admin_client = get_kafka_factory().create_admin_client()
    groups_result = admin_client.describe_consumer_groups([group_id])
    
    try:
        group_desc = groups_result[group_id].result()
        
        members = []
        for m in group_desc.members:
            members.append({
                "member_id": m.member_id,
                "client_id": m.client_id,
                "host": m.host,
                "assignment": [{"topic": a.topic, "partition": a.partition} for a in m.assignment.topic_partitions]
            })

        return {
            "group_id": group_desc.group_id,
            "state": str(group_desc.state),
            "protocol_type": group_desc.protocol_type,
            "protocol": group_desc.protocol,
            "members": members,
            "coordinator": {"id": group_desc.coordinator.id} if group_desc.coordinator else None
        }
    except Exception as e:
        return {"error": f"Failed to describe group '{group_id}': {e}"}

def consume_messages(topic_name: str, partition: Optional[int] = None, offset_spec: str = 'latest', limit: int = 10, timeout: float = 10.0) -> List[Dict[str, Any]]:
    """
    Consumes messages from a topic.
    offset_spec: 'earliest', 'latest', or specific integer offset.
    timeout: seconds to wait for messages.
    """
    group_id = f"mcp-inspector-{int(time.time())}"
    consumer = get_kafka_factory().create_consumer(group_id, auto_offset_reset='latest') # reset generic, specific handling below
    
    try:
        partitions = []
        if partition is not None:
            partitions = [TopicPartition(topic_name, partition)]
        else:
            metadata = consumer.list_topics(topic_name)
            if topic_name not in metadata.topics:
                return [{"error": f"Topic {topic_name} not found"}]
            
            partitions = [TopicPartition(topic_name, p) for p in metadata.topics[topic_name].partitions]

        # Assign first
        consumer.assign(partitions)
        
        # Handle Offsets
        for p in partitions:
            if offset_spec == 'earliest':
                p.offset = OFFSET_BEGINNING
            elif offset_spec == 'latest':
                # To get last N messages, we need to query high watermark
                low, high = consumer.get_watermark_offsets(p)
                if high > 0:
                     p.offset = max(low, high - limit)
                else:
                    p.offset = OFFSET_BEGINNING
            elif offset_spec.isdigit():
                 p.offset = int(offset_spec)
            else:
                 p.offset = OFFSET_END

        # Seek
        for p in partitions:
            consumer.seek(p)
            
        messages = []
        start_time = time.time()
        
        while len(messages) < limit and (time.time() - start_time) < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Handle error
                continue
            
            messages.append({
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode('utf-8') if msg.key() else None,
                "value": msg.value().decode('utf-8') if msg.value() else str(msg.value()),
                "timestamp": msg.timestamp()[1],
                "headers": msg.headers()
            })
            
        return messages
    finally:
        consumer.close()