import time
import logging
from uuid import uuid4
from typing import (
    List, 
    Dict, 
    Optional, 
    Any
)
from confluent_kafka import TopicPartition
from confluent_kafka._model import ConsumerGroupTopicPartitions
from ..connections.kafka import get_kafka_factory

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
    group_id = f"mcp-inspector-{uuid4()}"
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

        for p in partitions:
            low, high = consumer.get_watermark_offsets(p, timeout=5.0)
            if offset_spec == 'earliest':
                p.offset = low
            elif offset_spec == 'latest':
                if high > 0:
                     p.offset = max(low, high - limit)
                else:
                    p.offset = low
            elif offset_spec.isdigit():
                 p.offset = int(offset_spec)
            else:
                 p.offset = high

        consumer.assign(partitions)
            
        messages = []
        start_time = time.time()
        
        while len(messages) < limit and (time.time() - start_time) < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
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

def _check_consumer_group_safe_to_modify(admin_client, group_id: str, force: bool = False) -> Optional[str]:
    """
    Checks if a consumer group is safe to modify (no active members, state is not STABLE).
    Returns an error message string if it's unsafe, or None if safe.
    If force is True, bypasses the block but still executes the API for transparency if needed.
    """
    try:
        futures = admin_client.describe_consumer_groups([group_id])
        result = futures[group_id].result()
        
        state = str(result.state).upper()
        
        if not force:
            if 'STABLE' in state:
                return f"Consumer group '{group_id}' is in STABLE state. Use force=True to override."
            if len(result.members) > 0:
                 return f"Consumer group '{group_id}' has {len(result.members)} active members. Use force=True to override."
            
        return None
    except Exception as e:
        return f"Failed to verify consumer group state: {e}"

def _get_current_offsets(admin_client, group_id: str, topic_name: str, partitions: List[int]) -> Dict[int, int]:
    """
    Retrieves the current committed offsets for a group and topic/partitions.
    Returns a dict mapping partition -> offset.
    """
    current_offsets = {}
    try:
        tps = [TopicPartition(topic_name, p) for p in partitions]
        request = [ConsumerGroupTopicPartitions(group_id, tps)]
        futures = admin_client.list_consumer_group_offsets(request)
        result = futures[group_id].result()
        
        for tp in result.topic_partitions:
            if tp.topic == topic_name:
                current_offsets[tp.partition] = tp.offset
    except Exception as e:
        logging.warning(
            "Failed to fetch current offsets for group='%s', topic='%s', partitions=%s: %s",
            group_id,
            topic_name,
            partitions,
            str(e)
        )
        
    return current_offsets

def _build_audit_and_impact(group_id: str, cluster_id: str, changes: List[Dict]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Builds the audit_log and impact_summary blocks for offset modifications.
    """
    audit_log = {
        "executed_by": "mcp",
        "executed_at": int(time.time() * 1000),
        "cluster_id": cluster_id,
        "group_id": group_id
    }
    
    total_messages_rewind = 0
    for c in changes:
        if c.get("delta") is not None:
             total_messages_rewind += abs(c["delta"])
             
    impact_summary = {
        "total_partitions": len(changes),
        "total_messages_rewind": total_messages_rewind,
        "estimated_reprocess_time_sec": None
    }
    
    return audit_log, impact_summary

def reset_consumer_group_offset(group_id: str, topic_name: str, partition: Optional[int] = None, offset_spec: str = 'latest', dry_run: bool = False, force: bool = False) -> Dict[str, Any]:
    """
    Resets the offset of a consumer group using AdminClient.
    offset_spec: 'earliest', 'latest', or a specific integer offset string.
    dry_run: If True, only calculates and returns the proposed offset changes without applying them.
    force: If True, forces the modification even if the group is active or STABLE.
    """
    admin_client = get_kafka_factory().create_admin_client()
    
    safety_error = _check_consumer_group_safe_to_modify(admin_client, group_id, force)
    if safety_error:
        return {"error": safety_error, "warning": safety_error}
    
    try:
        partitions = []
        if partition is not None:
            partitions = [TopicPartition(topic_name, partition)]
        else:
            cluster_meta = admin_client.list_topics(topic=topic_name, timeout=10)
            if topic_name not in cluster_meta.topics:
                return {"error": f"Topic '{topic_name}' not found"}
            
            topic_meta = cluster_meta.topics[topic_name]
            partitions = [TopicPartition(topic_name, p) for p in topic_meta.partitions]

        partition_ids = [p.partition for p in partitions]
        current_offsets_map = _get_current_offsets(admin_client, group_id, topic_name, partition_ids)

        consumer = get_kafka_factory().create_consumer(group_id=f"mcp-inspector-{uuid4()}", auto_offset_reset='latest')
        
        try:
            for p in partitions:
                if offset_spec == 'earliest':
                    low, _ = consumer.get_watermark_offsets(p)
                    p.offset = low
                elif offset_spec == 'latest':
                    _, high = consumer.get_watermark_offsets(p)
                    p.offset = high
                elif offset_spec.isdigit():
                    p.offset = int(offset_spec)
                else:
                    return {"error": f"Invalid offset_spec '{offset_spec}'. Must be 'earliest', 'latest', or an integer."}
        finally:
            consumer.close()
            
        changes = []
        for p in partitions:
            curr = current_offsets_map.get(p.partition, -1)
            target = p.offset
            delta = target - curr if curr >= 0 else None
            changes.append({
                "partition": p.partition,
                "current_offset": curr,
                "target_offset": target,
                "delta": delta
            })

        try:
            cluster_meta = admin_client.list_topics(timeout=10)
            cluster_id = cluster_meta.cluster_id
        except:
             cluster_id = "unknown"
             
        audit_log, impact_summary = _build_audit_and_impact(group_id, cluster_id, changes)

        if dry_run:
            return {
                "group_id": group_id,
                "topic": topic_name,
                "status": "dry_run",
                "message": f"Dry-run mode: Offsets for group '{group_id}' would be altered.",
                "changes": changes,
                "audit_log": audit_log,
                "impact_summary": impact_summary
            }

        cgtp = ConsumerGroupTopicPartitions(group_id, partitions)
        futures = admin_client.alter_consumer_group_offsets([cgtp])
        
        result = futures[group_id].result()
        return {
            "group_id": group_id,
            "topic": topic_name,
            "status": "success",
            "message": f"Successfully altered offsets for group '{group_id}' on topic '{topic_name}'",
            "changes": changes,
            "audit_log": audit_log,
            "impact_summary": impact_summary
        }
    except Exception as e:
        return {"error": f"Failed to reset offset for group '{group_id}': {e}"}

def rewind_consumer_group_offset_by_timestamp(group_id: str, topic_name: str, timestamp_ms: int, partition: Optional[int] = None, dry_run: bool = False, force: bool = False) -> Dict[str, Any]:
    """
    Rewinds consumer group offsets manually based on a specific timestamp.
    Finds offsets matching the timestamp using Consumer, then sets using AdminClient.
    dry_run: If True, only calculates and returns the proposed offset changes without applying them.
    force: If True, forces the modification even if the group is active or STABLE.
    """
    factory = get_kafka_factory()
    admin_client = factory.create_admin_client()
    
    safety_error = _check_consumer_group_safe_to_modify(admin_client, group_id, force)
    if safety_error:
        return {"error": safety_error, "warning": safety_error}
    
    try:
        if partition is not None:
             partitions_to_search = [TopicPartition(topic_name, partition, timestamp_ms)]
             partition_ids = [partition]
        else:
             cluster_meta = admin_client.list_topics(topic=topic_name, timeout=10)
             if topic_name not in cluster_meta.topics:
                 return {"error": f"Topic '{topic_name}' not found"}
             
             partitions_to_search = [TopicPartition(topic_name, p, timestamp_ms) for p in cluster_meta.topics[topic_name].partitions]
             partition_ids = list(cluster_meta.topics[topic_name].partitions.keys())

        current_offsets_map = _get_current_offsets(admin_client, group_id, topic_name, partition_ids)
        consumer = factory.create_consumer(group_id=f"mcp-inspector-{uuid4()}", auto_offset_reset='latest')
        
        try:
            resolved_partitions = consumer.offsets_for_times(partitions_to_search, timeout=10.0)
            
            partitions_to_alter = []
            for p in resolved_partitions:
                if p.offset > -1:
                    partitions_to_alter.append(TopicPartition(topic_name, p.partition, p.offset))
                else:
                    _, high = consumer.get_watermark_offsets(p)
                    partitions_to_alter.append(TopicPartition(topic_name, p.partition, high))
        finally:
            consumer.close()
            
        changes = []
        for p in partitions_to_alter:
            curr = current_offsets_map.get(p.partition, -1)
            target = p.offset
            delta = target - curr if curr >= 0 else None
            changes.append({
                "partition": p.partition,
                "current_offset": curr,
                "target_offset": target,
                "delta": delta
            })

        try:
            cluster_meta = admin_client.list_topics(timeout=10)
            cluster_id = cluster_meta.cluster_id
        except:
             cluster_id = "unknown"
             
        audit_log, impact_summary = _build_audit_and_impact(group_id, cluster_id, changes)

        if dry_run:
            return {
                 "group_id": group_id,
                 "topic": topic_name,
                 "status": "dry_run",
                 "message": f"Dry-run mode: Offsets for group '{group_id}' would be rewound to timestamp {timestamp_ms}.",
                 "changes": changes,
                 "audit_log": audit_log,
                 "impact_summary": impact_summary
            }

        cgtp = ConsumerGroupTopicPartitions(group_id, partitions_to_alter)
        futures = admin_client.alter_consumer_group_offsets([cgtp])
        
        result = futures[group_id].result()
        return {
             "group_id": group_id,
             "topic": topic_name,
             "status": "success",
             "message": f"Successfully rewound offsets for group '{group_id}' on topic '{topic_name}' to timestamp {timestamp_ms}",
             "changes": changes,
             "audit_log": audit_log,
             "impact_summary": impact_summary
        }
    except Exception as e:
        return {"error": f"Failed to rewind offset for group '{group_id}': {e}"}