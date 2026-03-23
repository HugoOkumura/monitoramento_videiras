FROM python:3.10.20

WORKDIR /usr/src/app

RUN apt update && apt install -y default-jdk

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
    -P /opt/flink/lib/
# RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/4.0.1-2.0/flink-connector-kafka-4.0.1-2.0.jar \

#     -P /opt/flink/lib/
RUN pip install --upgrade pip
RUN pip install "setuptools<60" "wheel<0.38"

# COPY requirements.txt .
COPY require2.txt .
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r require2.txt

COPY . .

CMD ["python", "pre_processor.py"]

