from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
import json
import os
from datetime import datetime


class DailyWeatherAccumulator(KeyedProcessFunction):
    """
    Accumulates NODE_SENSOR readings during the day and emits one daily aggregation
    immediately when the Open-Meteo API event arrives.

    Execution flow:
      telemetry.raw        -> updates the daily accumulator
      daily.climate.api    -> closes the current day, emits daily.climate.data, resets state
    """

    def __init__(self):
        self.state_descriptor = None
        self.daily_state = None

    def open(self, context):
        self.state_descriptor = ValueStateDescriptor("daily_weather_accumulator", Types.STRING())
        self.daily_state = context.get_state(self.state_descriptor)

    def _empty_accumulator(self):
        return {
            "sum_t": 0.0,
            "min_t": None,
            "max_t": None,
            "count_t": 0,

            "sum_h": 0.0,
            "min_h": None,
            "max_h": None,
            "count_h": 0,

            "latitude": None,
            "longitude": None
        }

    def _load_accumulator(self):
        state = self.daily_state.value()
        if state is None:
            return self._empty_accumulator()

        acc = json.loads(state)

        # Compatibility with older saved state that used only "count".
        if "count_t" not in acc:
            old_count = int(acc.get("count", 0))
            acc["count_t"] = old_count
            acc["count_h"] = old_count
            acc.pop("count", None)

        return acc

    def _save_accumulator(self, acc):
        self.daily_state.update(json.dumps(acc))

    def process_element(self, value, ctx):
        data = json.loads(value)
        acc = self._load_accumulator()

        if data.get("origin_type") == "NODE_SENSOR":
            temperature = data.get("temperature")
            if temperature is not None:
                t = float(temperature)
                acc["sum_t"] += t
                acc["min_t"] = t if acc["min_t"] is None else min(acc["min_t"], t)
                acc["max_t"] = t if acc["max_t"] is None else max(acc["max_t"], t)
                acc["count_t"] += 1

            air_humidity = data.get("air_humidity")
            if air_humidity is not None:
                h = float(air_humidity)
                acc["sum_h"] += h
                acc["min_h"] = h if acc["min_h"] is None else min(acc["min_h"], h)
                acc["max_h"] = h if acc["max_h"] is None else max(acc["max_h"], h)
                acc["count_h"] += 1

            acc["latitude"] = data.get("latitude", acc["latitude"])
            acc["longitude"] = data.get("longitude", acc["longitude"])

            self._save_accumulator(acc)
            return

        if data.get("sensor_id") == "OPEN_METEO_STATION" or data.get("origin_type") == "METEO_API":
            # Do not emit if no valid sensor metric was accumulated.
            if acc["count_t"] == 0 and acc["count_h"] == 0:
                self.daily_state.update(json.dumps(self._empty_accumulator()))
                return

            result = {
                "timestamp": int(datetime.now().timestamp()),
                "registry_type": "DAILY_CLIMATE_AGGREGATION",

                "temperature_mean": (acc["sum_t"] / acc["count_t"]) if acc["count_t"] > 0 else None,
                "temperature_min": acc["min_t"] if acc["count_t"] > 0 else None,
                "temperature_max": acc["max_t"] if acc["count_t"] > 0 else None,
                "temperature_count": acc["count_t"],

                "humidity_mean": (acc["sum_h"] / acc["count_h"]) if acc["count_h"] > 0 else None,
                "humidity_min": acc["min_h"] if acc["count_h"] > 0 else None,
                "humidity_max": acc["max_h"] if acc["count_h"] > 0 else None,
                "humidity_count": acc["count_h"],

                "precipitation": float(data.get("precipitation", 0.0)),
                "precipitation_hours": int(data.get("precipitation_hours", 0)),
                "eto_api": float(data.get("evapotranspiration_api", 0.0)),
                "daylight_duration": float(data.get("daylight_duration", 0.0)),
                "sunshine_duration": float(data.get("sunshine_duration", 0.0)),

                "latitude": float(acc["latitude"]) if acc["latitude"] is not None else None,
                "longitude": float(acc["longitude"]) if acc["longitude"] is not None else None
            }

            # Start a new daily accumulation cycle after the API closes the day.
            self.daily_state.update(json.dumps(self._empty_accumulator()))
            yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    KAFKA_TOPIC_CONSUME_1 = os.getenv("KAFKA_TOPIC_CONSUME_1", "telemetry.raw")
    KAFKA_TOPIC_CONSUME_2 = os.getenv("KAFKA_TOPIC_CONSUME_2", "daily.climate.api")
    KAFKA_TOPIC_PRODUCE = os.getenv("KAFKA_TOPIC_PRODUCE", "daily.climate.data")

    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_topics(KAFKA_TOPIC_CONSUME_1, KAFKA_TOPIC_CONSUME_2) \
        .set_group_id("flink-daily-agg") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Source-Daily")

    processed = stream \
        .key_by(lambda x: "vinhedo_principal") \
        .process(DailyWeatherAccumulator(), output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_TOPIC_PRODUCE)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    processed.sink_to(sink)
    env.execute("Daily_Aggregator")


if __name__ == "__main__":
    main()
