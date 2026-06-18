from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
import json
import os
from datetime import datetime


class ShortTermAggregator(AggregateFunction):
    def create_accumulator(self):
        # (sum_temp, min_temp, max_temp, count_temp sum_hum, min_hum, max_hum, count_hum)
        return (0.0, float("inf"), float("-inf"), 0, 0.0, float("inf"), float("-inf"), 0)

    def add(self, value, acc):
        data = json.loads(value)
        # Ignora payloads que não contenham leituras reais dos nós de sensores
        if data.get('origin_type') != 'NODE_SENSOR':
            return acc
        
        (
            temp_sum, temp_min, temp_max, temp_count,
            humidity_sum, humidity_min, humidity_max, humidity_count
        ) = acc


        if data['temperature'] is not None:
            t = float(data['temperature'])
            temp_sum += t
            temp_min = min(temp_min, t)
            temp_max = max(temp_max, t)
            temp_count += 1

        if data['air_humidity'] is not None:
            h = float(data['air_humidity'])
            humidity_sum += h
            humidity_min = min(humidity_min, h)
            humidity_max = max(humidity_max, h)
            humidity_count += 1

        return (temp_sum, temp_min, temp_max, temp_count,
                humidity_sum, humidity_min, humidity_max, humidity_count
                )

    def get_result(self, acc):
        if acc[3] == 0 and acc[7] == 0: return {}

        count_temp = acc[3]
        count_hum = acc[7]
        return {
            "timestamp_short": int(datetime.now().timestamp()),
            "registry_type": "SHORT_TERM_STATS",
            "temperature_mean": acc[0] / count_temp,
            "temperature_min": acc[1],
            "temperature_max": acc[2],
            "humidity_mean": acc[4] / count_hum,
            "humidity_min": acc[5],
            "humidity_max": acc[6]
        }

    def merge(self, acc1, acc2):
        return (acc1[0] + acc2[0], min(acc1[1], acc2[1]), max(acc1[2], acc2[2]), acc1[3] + acc2[3], 
                acc1[4] + acc2[4], min(acc1[5], acc2[5]), max(acc1[6], acc2[6]), acc1[7] + acc2[7])

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    KAFKA_TOPIC_CONSUME = os.getenv('KAFKA_TOPIC_CONSUME', 'telemetry.raw')
    KAFKA_TOPIC_PRODUCE = os.getenv('KAFKA_TOPIC_PRODUCE', 'telemetry.short_aggregation')

    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    
    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_topics(KAFKA_TOPIC_CONSUME) \
        .set_group_id("flink-short-term-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "sensor_raw")

    # Calcula os extremos exatos por período curto de processamento
    aggregated = stream \
        .window_all(TumblingProcessingTimeWindows.of(Time.minutes(10))) \
        .aggregate(ShortTermAggregator()) \
        .filter(lambda x: bool(x)) \
        .map(lambda x: json.dumps(x), output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder() \
                .set_topic(KAFKA_TOPIC_PRODUCE) \
                .set_value_serialization_schema(SimpleStringSchema()) \
                .build()
        ).build()

    aggregated.sink_to(sink)
    env.execute("Flink-Short-Term-Aggregation")

if __name__ == "__main__":
    main()