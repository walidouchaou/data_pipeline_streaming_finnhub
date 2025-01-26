from confluent_kafka import Producer
import os
import logging
import time

class KafkaMessageProducer:
    def __init__(self):
        self.logger = logging.getLogger('KafkaProducer')
        self.max_retries = 5
        self.retry_delay = 2
        self.config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'client.id': os.getenv('KAFKA_CLIENT_ID'),
            'retry.backoff.ms': int(os.getenv('KAFKA_RETRY_BACKOFF_MS')),
            'message.send.max.retries': int(os.getenv('KAFKA_MESSAGE_MAX_RETRIES'))
        }
        self.producer = None
        self._initialize_producer_with_retry()
        self.topic = os.getenv('KAFKA_TOPIC')

    def _initialize_producer_with_retry(self):
        self.producer = Producer(self.config)

    def send_message(self, message):
        try:
            self.producer.produce(
                topic=self.topic,
                value=message
            )
            self.producer.flush()
            self.logger.info(f"Message envoyé à Kafka: {message}")
        except Exception as e:
            self.logger.error(f"Erreur d'envoi Kafka: {e}")

    def close(self):
        if self.producer:
            self.producer.flush() 