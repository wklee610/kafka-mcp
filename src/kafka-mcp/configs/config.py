import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    client_id: str = "kafka-mcp"

def get_kafka_config() -> KafkaConfig:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")
    
    return KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        client_id=os.environ.get("KAFKA_CLIENT_ID", "kafka-mcp"),
    )
