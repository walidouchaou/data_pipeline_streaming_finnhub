from confluent_kafka import Consumer, KafkaError
from confluent_kafka.cimpl import KafkaException
import os
import logging
import json
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from pathlib import Path
from src.utils.avro_utils import AvroUtils
from confluent_kafka.admin import AdminClient
import time

@dataclass
class ConsumerMetrics:
    messages_processed: int = 0
    errors: int = 0
    last_processed: Optional[datetime] = None
    processing_time: float = 0.0
    
    def update(self, success: bool, processing_time: float) -> None:
        self.messages_processed += 1
        if not success:
            self.errors += 1
        self.last_processed = datetime.now()
        self.processing_time += processing_time

@dataclass
class Trade:
    symbol: str
    price: float
    volume: float
    timestamp: int
    conditions: Optional[List[str]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Trade':
        return cls(
            symbol=data.get('s', ''),
            price=float(data.get('p', 0)),
            volume=float(data.get('v', 0)),
            timestamp=int(data.get('t', 0)),
            conditions=data.get('c', None)
        )
    
    def __str__(self) -> str:
        conditions_str = ', '.join(self.conditions) if self.conditions else 'None'
        return (
            f"Trade("
            f"symbol='{self.symbol}', "
            f"price={self.price}, "
            f"volume={self.volume}, "
            f"timestamp={self.timestamp}, "
            f"conditions={conditions_str})"
        )

class KafkaConsumerConfig:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.group_id = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'finnhub-consumer-group')
        self.auto_offset_reset = os.getenv('KAFKA_CONSUMER_AUTO_OFFSET_RESET', 'earliest')
        self.topic = os.getenv('KAFKA_TOPIC', 'finnhub.trades')
        self.poll_timeout = float(os.getenv('KAFKA_POLL_TIMEOUT', '1.0'))
        self.commit_interval = int(os.getenv('KAFKA_COMMIT_INTERVAL', '5'))
        
        self.validate()
    
    def validate(self):
        if not self.bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")
        if self.poll_timeout <= 0:
            raise ValueError("KAFKA_POLL_TIMEOUT must be positive")

    def validate_consumer_config(self, config: dict) -> None:
        try:
            # Test de création d'un consumer temporaire pour valider la config
            temp_consumer = Consumer(config)
            temp_consumer.close()
        except KafkaException as e:
            self.logger.error(f"Configuration Kafka invalide: {e}")
            raise ValueError(f"Configuration Kafka invalide: {e}")

class TradeConsumer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = KafkaConsumerConfig()
        self.metrics = ConsumerMetrics()
        self.avro_utils = AvroUtils()
        
        # Chargement du schéma Avro
        schema_path = os.path.join(os.path.dirname(__file__), 'schema', 'trades.avsc')
        self.avro_schema = self.avro_utils.load_avro_schema(schema_path)
        
        # Configuration améliorée du consumer
        consumer_conf = {
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': False,
            'security.protocol': 'PLAINTEXT',
            'socket.timeout.ms': 10000,
            'session.timeout.ms': 60000,
            'heartbeat.interval.ms': 3000,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'fetch.min.bytes': 1,
            'fetch.message.max.bytes': 52428800,  # 50MB
            'statistics.interval.ms': 15000
        }
        
        try:
            self.validate_consumer_config(consumer_conf)
            self.ensure_topic_exists()
            self.consumer = Consumer(consumer_conf)
            self.consumer.subscribe([self.config.topic])
            self.last_commit_time = datetime.now()
            self.logger.info(f"Consumer initialisé pour le topic: {self.config.topic}")
        except Exception as e:
            self.logger.error(f"Erreur d'initialisation du consumer: {e}")
            raise

    def validate_trade(self, trade_data: Dict) -> bool:
        """Valide les données du trade."""
        required_fields = ['s', 'p', 'v', 't']
        if not all(field in trade_data for field in required_fields):
            return False
            
        try:
            float(trade_data['p'])
            float(trade_data['v'])
            int(trade_data['t'])
            return True
        except (ValueError, TypeError):
            return False

    def should_commit(self) -> bool:
        """Vérifie si on doit faire un commit basé sur l'intervalle configuré."""
        time_since_commit = (datetime.now() - self.last_commit_time).seconds
        return time_since_commit >= self.config.commit_interval

    def process_message(self, message: Dict[str, Any]) -> bool:
        """Traite un message et retourne True si le traitement est réussi."""
        start_time = datetime.now()
        
        if not isinstance(message, dict) or 'data' not in message:
            self.logger.warning("Format de message invalide")
            return False
            
        success = True
        for trade_data in message['data']:
            if not self.validate_trade(trade_data):
                self.logger.warning(f"Données de trade invalides: {trade_data}")
                success = False
                continue
                
            try:
                trade = Trade.from_dict(trade_data)
                self.logger.info(trade)
            except Exception as e:
                self.logger.error(f"Erreur de traitement: {e}")
                success = False
                
        processing_time = (datetime.now() - start_time).total_seconds()
        self.metrics.update(success, processing_time)
        return success
                
    def consume(self) -> None:
        self.logger.info(f"Démarrage du consumer sur le topic: {self.config.topic}")
        
        try:
            while True:
                msg = self.consumer.poll(self.config.poll_timeout)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug("Fin de partition atteinte")
                    else:
                        self.logger.error(f"Erreur consumer: {msg.error()}")
                    continue
                    
                try:
                    # Gestion plus robuste de la désérialisation
                    try:
                        # Essayer d'abord la désérialisation Avro
                        deserialized_data = self.avro_utils.avro_decode(
                            msg.value(),
                            self.avro_schema
                        )
                        message = json.loads(deserialized_data)
                    except Exception as avro_error:
                        # Si échec Avro, essayer JSON direct
                        self.logger.warning(f"Échec désérialisation Avro: {avro_error}, tentative JSON")
                        message = json.loads(msg.value())
                    
                    if self.process_message(message):
                        if self.should_commit():
                            self.consumer.commit()
                            self.last_commit_time = datetime.now()
                            self.log_metrics()
                            
                except Exception as e:
                    self.logger.error(f"Erreur de traitement du message: {e}")
                    
        except KeyboardInterrupt:
            self.logger.info("Arrêt du consumer...")
        finally:
            self.cleanup()
    
    def log_metrics(self) -> None:
        """Log les métriques actuelles."""
        self.logger.info(
            f"Métriques - Messages traités: {self.metrics.messages_processed}, "
            f"Erreurs: {self.metrics.errors}, "
            f"Temps moyen de traitement: {self.metrics.processing_time/max(1, self.metrics.messages_processed):.3f}s"
        )
            
    def cleanup(self) -> None:
        try:
            self.consumer.close()
            self.logger.info("Consumer fermé avec succès")
        except Exception as e:
            self.logger.error(f"Erreur lors de la fermeture du consumer: {e}")

    def validate_consumer_config(self, config: dict) -> None:
        """Valide la configuration du consumer Kafka."""
        try:
            # Test de création d'un consumer temporaire pour valider la config
            temp_consumer = Consumer(config)
            temp_consumer.close()
        except KafkaException as e:
            self.logger.error(f"Configuration Kafka invalide: {e}")
            raise ValueError(f"Configuration Kafka invalide: {e}")

    def ensure_topic_exists(self) -> None:
        """Vérifie si le topic existe et attend sa création si nécessaire."""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Créer un admin client temporaire
                admin_client = AdminClient({
                    'bootstrap.servers': self.config.bootstrap_servers
                })
                
                # Obtenir les métadonnées du topic
                topics = admin_client.list_topics(timeout=5)
                
                if self.config.topic in topics.topics:
                    self.logger.info(f"Topic {self.config.topic} trouvé")
                    return
                
                self.logger.warning(
                    f"Topic {self.config.topic} non trouvé. "
                    f"Tentative {attempt + 1}/{max_retries}"
                )
                time.sleep(retry_delay)
                
            except KafkaException as e:
                self.logger.error(f"Erreur lors de la vérification du topic: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay)

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
        logger.error(f"Erreur fatale: {e}")
        exit(1)

if __name__ == '__main__':
    main() 