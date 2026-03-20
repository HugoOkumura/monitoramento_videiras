FROM python:3.12.13-bookworm

WORKDIR /usr/src/app

RUN apt update && apt install -y default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar \
#     -P /opt/flink/lib/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "pre_processor.py"]

