from pyflink.datastream import StreamExecutionEnvironment
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

class VineyardDataProcessor:

    def __init__(self):
        self.KAFKA_BOOTSRAP_SERVERS  = os.getenv('KAFKA_BOOTSRAP_SERVERS')
        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

        self.kafka_consumer = FlinkKafkaConsumer(
            topics=self.KAFKA_TOPIC,
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": self.KAFKA_BOOTSRAP_SERVERS,
                "group.id": "uva_vitoria_processor_group"
            }
        )

        self.flink_env = StreamExecutionEnvironment.get_execution_environment()
        self.flink_env.set_parallelism(1)
        self.stream = self.flink_env.add_source(self.kafka_consumer)

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

    def process_sensor_data(self, data):
        
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
    
    def calculate_water_stress(self, data):
        return 0
    
    def calculate_disease_risk(self, data):
        return 0
    
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
    
    def save_to_influxdb(self, data):
        with InfluxDBClient(**self.influx_config) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

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
                
            write_api.write(bucket=self.influx_config['bucket'], org=self.influx_config['org'], record=point)


    def save_to_postgres(self, data):
        pass

    def run(self):
        
        # processamento individual por sensor
        sensor_stream = self.stream \
            .map(self.process_sensor_data) \
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
        
        self.stream.map(lambda x: self.save_to_influxdb(x))
        self.stream.map(lambda x: self.save_to_postgres(x))

        field_aggregate = self.stream \
            .window_all(TumblingEventTimeWindows.of(Time.minutes(5))) \
            .reduce(lambda a, b: self.aggregate_field_data([a, b]))

        field_aggregate.map(lambda x: self.save_to_influxdb(x))

        self.flink_env.execute("Vineyard Data Processing Job")

if __name__ == "__main__":
    startLog()
    processor = VineyardDataProcessor()
    processor.run()