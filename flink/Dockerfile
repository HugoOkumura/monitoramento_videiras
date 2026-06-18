FROM apache/flink:1.18-scala_2.12

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip install --no-cache-dir \
    apache-flink==1.18.1 \
    confluent-kafka==2.3.0

ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/

USER root
RUN chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    chmod 755 /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

# Volta o escopo do contêiner para o usuário padrão do Flink
USER flink

WORKDIR /opt/flink