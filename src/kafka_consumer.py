from confluent_kafka import Consumer, KafkaError
import os
import logging
import json
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any
from pathlib import Path

@dataclass
class Trade:
    symbol: str
    price: float
    volume: float
    timestamp: int
    conditions: list
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Trade':
        return cls(
            symbol=data.get('s', ''),
            price=float(data.get('p', 0)),
            volume=float(data.get('v', 0)),
            timestamp=int(data.get('t', 0)),
            conditions=data.get('c', [])
        )
    
    def __str__(self) -> str:
        return (
            f"\nTrade Details:"
            f"\n  Symbol: {self.symbol}"
            f"\n  Price: {self.price:.2f}"
            f"\n  Volume: {self.volume:.8f}"
            f"\n  Time: {datetime.fromtimestamp(self.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}"
            f"\n  Conditions: {', '.join(self.conditions)}"
        )

class KafkaConsumerConfig:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.group_id = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'finnhub-consumer-group')
        self.auto_offset_reset = os.getenv('KAFKA_CONSUMER_AUTO_OFFSET_RESET', 'earliest')
        self.topic = os.getenv('KAFKA_TOPIC', 'finnhub.trades')
        
        self.validate()
    
    def validate(self):
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

class TradeConsumer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = KafkaConsumerConfig()
        
        # Configuration du consumer
        consumer_conf = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset
        }
        
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([self.config.topic])

    def process_message(self, message: Dict[str, Any]) -> None:
        if not isinstance(message, dict) or 'data' not in message:
            self.logger.warning("Received invalid message format")
            return
            
        for trade_data in message['data']:
            try:
                trade = Trade.from_dict(trade_data)
                self.logger.info(trade)
            except Exception as e:
                self.logger.error(f"Error processing trade data: {e}")
                
    def consume(self) -> None:
        self.logger.info(f"Starting consumer, listening on topic: {self.config.topic}")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug("Reached end of partition")
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    # Désérialiser avec le Schema Registry
                    deserialized_data = self.avro_deserializer(
                        msg.value(), 
                        SerializationContext(self.config.topic, MessageField.VALUE)
                    )
                    self.process_message(deserialized_data)
                except Exception as e:
                    self.logger.error(f"Deserialization error: {e}")
                    
        except KeyboardInterrupt:
            self.logger.info("Shutting down consumer...")
        finally:
            self.cleanup()
            
    def cleanup(self) -> None:
        try:
            self.consumer.close()
            self.logger.info("Consumer closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main() -> None:
    setup_logging()
    logger = logging.getLogger("Main")
    
    try:
        consumer = TradeConsumer()
        consumer.consume()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)

if __name__ == '__main__':
    main() 