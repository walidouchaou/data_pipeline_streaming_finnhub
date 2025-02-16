FROM bitnami/spark:3.5.0

USER root

# Installation des outils nécessaires
RUN apt-get update && apt-get install -y \
    kafkacat \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copie du requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Ajout des dépendances Spark-Kafka et Spark-Cassandra
ENV SPARK_EXTRA_JARS_DIR=/opt/bitnami/spark/jars

# Téléchargement des dépendances Kafka, Cassandra et Config
RUN wget https://repo1.maven.org/maven2/com/typesafe/config/1.4.2/config-1.4.2.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.0/spark-streaming-kafka-0-10_2.12-3.5.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-driver_2.12/3.5.0/spark-cassandra-connector-driver_2.12-3.5.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.17.0/java-driver-core-4.17.0.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/io/netty/netty-handler/4.1.82.Final/netty-handler-4.1.82.Final.jar -P $SPARK_EXTRA_JARS_DIR

# Ajout des dépendances manquantes pour le protocole Cassandra
RUN wget https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.1/native-protocol-1.5.1.jar -P $SPARK_EXTRA_JARS_DIR

# Ajout des dépendances pour Reactive Streams
RUN wget https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.4/reactive-streams-1.0.4.jar -P $SPARK_EXTRA_JARS_DIR
RUN wget https://repo1.maven.org/maven2/io/projectreactor/reactor-core/3.4.24/reactor-core-3.4.24.jar -P $SPARK_EXTRA_JARS_DIR

# Configuration de l'environnement
ENV PYTHONPATH=/opt/bitnami/spark/scripts

# Copie des scripts Spark
COPY src/processor/src/ /opt/bitnami/spark/scripts/ 