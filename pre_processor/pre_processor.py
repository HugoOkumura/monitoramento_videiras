from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, AllWindowFunction, AggregateFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Time
from pyflink.datastream.window import TumblingEventTimeWindows
import json
import logging
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os 

def startLog():
    logging.basicConfig(
        level=logging.INFO,
        format="{asctime} - {levelname} - {message}",
        datefmt="%d-%m-%Y %H:%M",
        style="{",
        filename="./log/log.log",
        encoding="utf-8",
        filemode="a"
    )

class ProcessSensorData(MapFunction):
    def map(self, data):
        sensor_data = json.loads(data)

        results = {
            'sensor_id': sensor_data['sensor_id'],
            'timestamp': sensor_data['timestamp'],
            'temperature': sensor_data['temperature'],
            'humidity': sensor_data['humidity'],
            'soil_moisture': sensor_data['soil_moisture']
        }

        results['water_stress'] = self.calculate_water_stress(sensor_data)
        results['disease_risk'] = self.calculate_disease_risk(sensor_data)

        return results

    def calculate_water_stress(self,data):
        return 0
    
    def calculate_disease_risk(self,data):
        return 0

class SaveToInfluxDB(MapFunction):

    def __init__(self, influx_config):
        self.influx_config = influx_config
        self.client = None
        self.write_api = None

    def open(self, runtime_context):
        self.client = InfluxDBClient(**self.influx_config)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def map(self, data):
        if 'sensor_id' in data:
            point = Point("sensor_data") \
                .tag("sensor_id", data['sensor_id']) \
                .field("temperature", data['temperature']) \
                .field("humidity", data['humidity']) \
                .field("soil_moisture", data['soil_moisture']) \
                .field("water_stress", data['water_stress']) \
                .field("disease_risk", data['disease_risk']) \
                .time(data['timestamp'])
        else:
            point = Point("field_aggregate") \
                .field("avg_temperature", data['avg_temperature']) \
                .field("max_temperature", data['max_temperature']) \
                .field("min_temperature", data['min_temperature']) \
                .field("avg_water_stress", data['avg_water_stress']) \
                .field("avg_disease_risk", data['avg_disease_risk']) \
                .field("active_sensors", data['active_sensors']) \
                .time(data['timestamp'])

        self.write_api.write(
            bucket=self.influx_config['bucket'],
            org=self.influx_config['org'],
            record=point
        )

        return data  # mantém o fluxo

    def close(self):
        if self.client:
            self.client.close()

class AgregateSensorData(AggregateFunction):
    def create_accumulator(self):
         # (sum_temperature, sum_humidity, sum_max_temp, sum_min_temp, count)
        return (0.0, 0.0, 0.0, 0.0, 0) 
    
    def add(self, value: dict, accumulator: tuple):
        total_temp, total_humidity, total_max_temp, total_min_temp, count = accumulator
        return (
            total_temp + value['temperature'],
            total_humidity + value['humidity'],
            total_max_temp + value['max_temp'],
            total_min_temp + value['min_temp'],
            count + 1
        )
    
    def get_result(self, accumulator: tuple) -> dict:
        total_temp, total_humidity, count = accumulator
        
        if count == 0:
            return {
                'avg_temperature': 0.0,
                'avg_humidity': 0.0,
                'total_records': 0
            }
        
        return {
            'avg_temperature': total_temp / count,
            'avg_humidity': total_humidity / count,
            'total_records': count
        }
    
    def merge(self, acc_a: tuple, acc_b: tuple) -> tuple:
        total_temp_a, total_humidity_a, total_max_temp_a, total_min_temp_a, count_a = acc_a
        total_temp_b, total_humidity_b, total_max_temp_b, total_min_temp_b, count_b = acc_b
        
        return (
            total_temp_a + total_temp_b,
            total_humidity_a + total_humidity_b,
            total_max_temp_a + total_max_temp_b, 
            total_min_temp_a + total_min_temp_b,
            count_a + count_b
        )

class VineyardDataProcessor:

    def __init__(self):
        self.KAFKA_BOOTSTRAP_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC') 
        
        self.flink_env = StreamExecutionEnvironment.get_execution_environment()
        self.flink_env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

        self.kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(self.KAFKA_BOOTSTRAP_SERVERS) \
            .set_topics(self.KAFKA_TOPIC) \
            .set_group_id(os.getenv('KAFKA_GROUP_ID')) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        
        self.stream = self.flink_env.from_source(
            self.kafka_source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source"
        )

        self.influx_config = {
            "url": os.getenv('INFLUXDB_URL'),
            "token": os.getenv('INFLUXDB_TOKEN'),
            "org": os.getenv('INFLUXDB_ORG'),
            "bucket": os.getenv('INFLUXDB_BUCKET')
        }

        self.postgres_config = {
            "host": os.getenv('POSTGRES_HOST'),
            "port": os.getenv('POSTGRES_PORT'),
            "database": os.getenv('POSTGRES_DB'),
            "user": os.getenv('POSTGRES_USER'),
            "password": os.getenv('POSTGRES_PASSWORD')
        }
    
    def aggregate_field_data(self,sensor_results):
        if not sensor_results:
            return None
        
        temps = [r['temperature'] for r in sensor_results]

        return{
            'timestamp': datetime.now().isoformat(),
            'avg_temperature': sum(temps) / len(temps),
            'max_temperature': max(temps),
            'min_temperature': min(temps),
            'avg_water_stress': sum(r['water_stress'] for r in sensor_results) / len(sensor_results),
            'avg_disease_risk': sum(r['disease_risk'] for r in sensor_results) / len(sensor_results),
            'active_sensors': len(sensor_results)
        }
    
    def save_to_postgres(self, data):
        pass

    def run(self):
        
        # processamento individual por sensor
        sensor_stream = self.stream \
            .map(ProcessSensorData()) \
            .key_by(lambda x: x['sensor_id'])
        
        # Calcular estatísticas por sensor em janelas de 1 minuto
        sensor_stats = sensor_stream \
            .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
            .reduce(lambda a, b: {
                'sensor_id': a['sensor_id'],
                'min_temp': min(a['temperature'], b['temperature']),
                'max_temp': max(a['temperature'], b['temperature']),
                'avg_temp': (a['temperature'] + b['temperature']) / 2,
                'avg_water_stress': (a['water_stress'] + b['water_stress']) / 2
            })
        
        sensor_stats.map(SaveToInfluxDB(self.influx_config))
        # self.stream.map(lambda x: self.save_to_postgres(x))

        field_aggregate = self.stream \
            .window_all(TumblingEventTimeWindows.of(Time.minutes(5))) \
            .aggregate(aggregate_function=AgregateSensorData())

        field_aggregate.map(SaveToInfluxDB(self.influx_config))

        self.flink_env.execute("Vineyard Data Processing Job")

if __name__ == "__main__":
    startLog()
    
    # while True:
    #     pass

    processor = VineyardDataProcessor()
    processor.run()