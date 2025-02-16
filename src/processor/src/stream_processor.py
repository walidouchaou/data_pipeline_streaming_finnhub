from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_timestamp, col
from pyspark.sql import functions as F
import logging
from datetime import datetime
import uuid
from typing import Optional
from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class SparkConfig:
    app_name: str = "FinnhubStreamProcessor"
    kafka_packages: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    avro_packages: str = "org.apache.spark:spark-avro_2.12:3.5.0"
    cassandra_packages: str = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
    cassandra_host: str = os.getenv("CASSANDRA_HOST")
    cassandra_port: str = os.getenv("CASSANDRA_PORT")
    cassandra_user: str = os.getenv("CASSANDRA_USER")
    cassandra_password: str = os.getenv("CASSANDRA_PASSWORD")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = os.getenv("KAFKA_TOPIC")
    cassandra_keyspace: str = os.getenv("CASSANDRA_KEYSPACE")
    cassandra_table: str = os.getenv("CASSANDRA_TABLE")
    schema_path: str = os.getenv("SCHEMA_PATH")

class StreamProcessor:
    def __init__(self, config: SparkConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = self._create_spark_session()
        self.trade_schema = self._load_trade_schema()

    def _create_spark_session(self) -> SparkSession:
        packages = ",".join([
            self.config.kafka_packages,
            self.config.avro_packages,
            self.config.cassandra_packages
        ])
        
        return (SparkSession.builder
                .appName(self.config.app_name)
                .config("spark.jars.packages", packages)
                .config("spark.sql.avro.compression.codec", "snappy")
                .config("spark.cassandra.connection.host", self.config.cassandra_host)
                .config("spark.cassandra.connection.port", self.config.cassandra_port)
                .config("spark.cassandra.auth.username", self.config.cassandra_user)
                .config("spark.cassandra.auth.password", self.config.cassandra_password)
                .getOrCreate())

    def _load_trade_schema(self) -> str:
        schema_path = Path(self.config.schema_path)
        try:
            return schema_path.read_text()
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture du schéma Avro: {e}")
            raise

    def _transform_trades(self, df):
        return (df
                .withColumn("uuid", F.udf(lambda: str(uuid.uuid1()))())
                .withColumnRenamed("c", "trade_conditions")
                .withColumnRenamed("p", "price")
                .withColumnRenamed("s", "symbol")
                .withColumnRenamed("t", "trade_timestamp")
                .withColumnRenamed("v", "volume")
                .withColumn("trade_timestamp", (col("trade_timestamp") / 1000).cast("timestamp"))
                .withColumn("ingest_timestamp", current_timestamp()))

    def process(self):
        try:
            trades_df = (self.spark.readStream
                        .format("kafka")
                        .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
                        .option("subscribe", self.config.kafka_topic)
                        .option("startingOffsets", "latest")
                        .load())

            expanded_df = (trades_df
                         .withColumn("avroData", F.from_avro(col("value"), self.trade_schema))
                         .select("avroData.*")
                         .select(explode("data"))
                         .select("col.*"))

            final_df = self._transform_trades(expanded_df)

            query = (final_df.writeStream
                    .foreachBatch(self._write_to_cassandra)
                    .outputMode("append")
                    .start())

            query.awaitTermination()

        except Exception as e:
            self.logger.error(f"Erreur dans le traitement du stream: {e}")
            raise

    @staticmethod
    def _write_to_cassandra(batch_df, batch_id):
        (batch_df.write
         .format("org.apache.spark.sql.cassandra")
         .mode("append")
         .options(table=self.config.cassandra_table, keyspace=self.config.cassandra_keyspace)
         .save())

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger("Main")
    try:
        config = SparkConfig()
        processor = StreamProcessor(config)
        logger.info("Démarrage du processeur de streaming...")
        processor.process()
    except Exception as e:
        logger.error(f"Erreur fatale dans le processeur de streaming: {e}")
        raise

if __name__ == "__main__":
    main()

