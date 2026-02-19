from confluent_kafka import (
    Producer, 
    Consumer
)
from confluent_kafka.admin import AdminClient
from configs.config import get_kafka_config, KafkaConfig

class KafkaClientFactory:
    _instance = None
    
    def __init__(self):
        self.config = get_kafka_config()
        self._producer = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = KafkaClientFactory()
        return cls._instance

    def create_admin_client(self) -> AdminClient:
        conf = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'client.id': self.config.client_id,
        }
        return AdminClient(conf)

    def create_producer(self) -> Producer:
        if self._producer is None:
            conf = {
                'bootstrap.servers': self.config.bootstrap_servers,
                'client.id': self.config.client_id,
            }
            self._producer = Producer(conf)
        return self._producer

    def create_consumer(self, group_id: str, auto_offset_reset: str = 'earliest') -> Consumer:
        conf = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'client.id': self.config.client_id,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False, 
        }
        return Consumer(conf)

def get_kafka_factory() -> KafkaClientFactory:
    return KafkaClientFactory.get_instance()