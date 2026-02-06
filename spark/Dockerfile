FROM apache/spark:3.4.2

WORKDIR /app
COPY streaming_job.py .

# Kafka + Spark connector jars (MATCHING Spark 3.4.2 / Scala 2.12)
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.2/spark-sql-kafka-0-10_2.12-3.4.2.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.2/spark-token-provider-kafka-0-10_2.12-3.4.2.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

CMD ["/opt/spark/bin/spark-submit", "streaming_job.py"]