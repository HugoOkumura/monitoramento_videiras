FROM apache/flink:1.18-scala_2.12

RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
    -P /opt/flink/lib/

ENV FLINK_PROPERTIES="classloader.check-leaked-classloader: false"

