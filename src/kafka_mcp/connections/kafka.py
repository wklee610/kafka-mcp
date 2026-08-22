from typing import Optional

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from ..configs.config import KafkaConfig, get_kafka_config


class KafkaClientFactory:
    _instance = None

    def __init__(self, config: Optional[KafkaConfig] = None):
        self.config = config or get_kafka_config()
        self._producer = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def create_admin_client(self) -> AdminClient:
        return AdminClient(self.config.to_client_config())

    def create_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(self.config.to_client_config())
        return self._producer

    def create_consumer(
        self, group_id: str, auto_offset_reset: str = "earliest"
    ) -> Consumer:
        config = self.config.to_client_config()
        config.update(
            {
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        return Consumer(config)


def get_kafka_factory() -> KafkaClientFactory:
    return KafkaClientFactory.get_instance()
