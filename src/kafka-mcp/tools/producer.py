from typing import (
    Dict, 
    Any, 
    Optional
)
from connections.kafka import get_kafka_factory

def produce_message(
    topic_name: str, 
    value: str, 
    key: Optional[str] = None, 
    headers: Optional[Dict[str, str]] = None
) -> str:
    """Produces a message to a topic."""
    producer = get_kafka_factory().create_producer()
    
    try:
        # Produce is asynchronous
        producer.produce(
            topic_name, 
            key=key.encode('utf-8') if key else None, 
            value=value.encode('utf-8') if value else None, 
            headers=headers
        )
        producer.flush(timeout=10)
        return "Message sent successfully"
    except Exception as e:
        return f"Failed to produce message: {e}"