from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, AggregateFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
import json
import logging
from datetime import datetime
from influxdb_client_3 import Point, InfluxDBClient3
import os
import time
import dotenv
import sys

def startLog():
    logging.basicConfig(
        level=logging.INFO,
        format="{asctime} - {levelname} - {message}",
        datefmt="%d-%m-%Y %H:%M",
        style="{",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(filename="./log/log.log", mode="a", encoding="utf-8")
        ]
    )
'''
Class de map para extrair os dados recebidos pelo Kafka
'''
class ProcessSensorData(MapFunction):
    def map(self, data):
        sensor_data = json.loads(data)
        results = {
            'sensor_id': sensor_data['sensor_id'],
            'timestamp': sensor_data['timestamp'],
            'temperature': sensor_data['temperature'],
            'air_humidity': sensor_data['air_humidity'],
            'variety' : sensor_data['variety']
            # 'soil_moisture': sensor_data['soil_moisture']
        }

        results['water_stress'] = self.calculate_water_stress(sensor_data)
        results['disease_risk'] = self.calculate_disease_risk(sensor_data)

        logging.info(f"Recebido do Kafka: {results}")

        return results

    def calculate_water_stress(self,data):
        return 0
    
    def calculate_disease_risk(self,data):
        return 0


'''
Classe de agregação e processamento dos dados recebidos por sensor
'''
class SensorAggregate(AggregateFunction):

    def create_accumulator(self):
        # min_temp, max_temp, sum_temp, sum_air_humidity, sum_ws, sum_dr, count, sensor_id
        return (float('inf'), float('-inf'), 0.0, 0.0, 0.0, 0.0, 0, None)

    def add(self, value, acc):
        min_t, max_t, sum_t, sum_ah, sum_ws, sum_dr, count, sensor_id = acc

        temp = value['temperature']
        air_humidity = value['air_humidity']
        ws = value['water_stress']
        dr = value['disease_risk']

        return (
            min(min_t, temp),
            max(max_t, temp),
            sum_t + temp,
            sum_ah + air_humidity,
            sum_ws + ws,
            sum_dr + dr,
            count + 1,
            value['sensor_id']
        )

    def get_result(self, acc):
        min_t, max_t, sum_t, sum_ah, sum_ws, sum_dr, count, sensor_id = acc

        return {
            'type': 'sensor',
            'sensor_id': sensor_id,
            'min_temp': min_t,
            'max_temp': max_t,
            'avg_temp': sum_t / count if count else 0,
            'avg_air_humidity': sum_ah / count if count else 0,
            'avg_water_stress': sum_ws / count if count else 0,
            'avg_disease_risk': sum_dr / count if count else 0,
            'timestamp': int(datetime.now().timestamp())
        }

    def merge(self, a, b):

        return (
            min(a[0], b[0]),
            max(a[1], b[1]),
            a[2] + b[2],
            a[3] + b[3],
            a[4] + b[4],
            a[5] + b[5],
            a[6] + b[6],
            a[7] or b[7]
        )

'''
Agregação e processamento dos dados do campo inteiro
'''
class FieldAggregate(AggregateFunction):

    def create_accumulator(self):
        return (0.0, 0.0, 0.0, 0.0, 0)
        # sum_temp, sum_air_humidity, sum_water_stress, sum_disease_risk, count

    def add(self, value, acc):
        st, sh, sw, sd, c = acc
        return (
            st + value['temperature'],
            sh + value['air_humidity'],
            sw + value['water_stress'],
            sd + value['disease_risk'],
            c + 1
        )

    def get_result(self, acc):
        st, sh, sw, sd, c = acc
        return {
            'type': 'field',
            'avg_temperature': st / c if c else 0,
            'avg_air_humidity': sh / c if c else 0,
            'avg_water_stress': sw / c if c else 0,
            'avg_disease_risk': sd / c if c else 0,
            'active_sensors': c,
            'timestamp': int(datetime.now().timestamp())
        }

    def merge(self, a, b):
        return (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2],
            a[3] + b[3],
            a[4] + b[4]
        )


'''
Classe para fazer a gravação no InfluxDB
'''
class SaveToInfluxDB(MapFunction):

    def __init__(self, config):
        config = dict(config)
        self.host = config.pop('url')
        self.token = config.pop('token')
        self.database = config.pop('database')
        self.config = config
        self.client = None

    def open(self, runtime_context):
        logging.info("Tentando conectar com InfluxDB")
        try:    
            self.client = InfluxDBClient3(host=self.host, database=self.database, token=self.token, **self.config)
        except Exception as e:
            logging.error(f"SaveToInflux.open: Não foi possível conectar ao InfluxDB. {e}")

    def map(self, data):
        try:
            if data['type'] == 'sensor':
                point = Point("sensor_stats") \
                    .tag("sensor_id", data['sensor_id']) \
                    .field("min_temp", data['min_temp']) \
                    .field("max_temp", data['max_temp']) \
                    .field("avg_temp", data['avg_temp']) \
                    .time(data['timestamp'], write_precision="s")

            else:
                point = Point("field_aggregate") \
                    .field("avg_temperature", data['avg_temperature']) \
                    .field("avg_air_humidity", data['avg_air_humidity']) \
                    .field("avg_water_stress", data['avg_water_stress']) \
                    .field("avg_disease_risk", data['avg_disease_risk']) \
                    .field("active_sensors", data['active_sensors']) \
                    .time(data['timestamp'], write_precision="s")

            self.client.write(point)
            logging.info("Salvo no InfluxDB")
        except Exception as e:
            logging.error(f"SaveToInfluxDB: Erro ao gravar no Influxdb. {e}")
        return data


'''
Class mestre que faz a conexão com o Kafka e executa os jobs do Flink
'''
class VineyardDataProcessor:

    def __init__(self, influx_token):
        self.env = StreamExecutionEnvironment.get_execution_environment()

        self.env.set_restart_strategy(
            RestartStrategies.fixed_delay_restart(5, 1000)
        )
        self.env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(os.getenv('KAFKA_BOOTSTRAP_SERVERS')) \
            .set_topics(os.getenv('KAFKA_TOPIC')) \
            .set_group_id(os.getenv('KAFKA_GROUP_ID')) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
            # .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \

        self.stream = self.env.from_source(
            kafka_source,
            WatermarkStrategy.no_watermarks(),
            "Kafka Source"
        )

        self.influx_config = {
            "url": os.getenv('INFLUXDB_URL'),
            "token": influx_token,
            "database": os.getenv('INFLUXDB_DB'),
            "verify_ssl": False
        }

    def run(self):

        parsed = self.stream.map(ProcessSensorData())

        parsed = parsed.assign_timestamps_and_watermarks(
            WatermarkStrategy
                .for_bounded_out_of_orderness(Duration.of_seconds(15))
                .with_idleness(Duration.of_seconds(15))
                # .with_timestamp_assigner(SensorTimestampAssigner())
        )

        # -------- SENSOR --------
        sensor_stats = parsed \
            .key_by(lambda x: x['sensor_id']) \
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
            .aggregate(SensorAggregate())

        sensor_stats.map(SaveToInfluxDB(self.influx_config))

        # -------- FIELD --------
        field_stats = parsed \
            .window_all(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
            .aggregate(FieldAggregate())

        field_stats.map(SaveToInfluxDB(self.influx_config))

        self.env.execute("Vineyard Processing")

# ---------------- ENTRY ----------------
if __name__ == "__main__":
    startLog()
    dotenv.load_dotenv("./.env")
    
    influxdb_token = None

    logging.info("Esperando inserir token do InfluxDB3 no arquivo .env...")
    while influxdb_token is None:
        influxdb_token = os.getenv("INFLUXDB_TOKEN")
        if influxdb_token is None:
            logging.warning("Insira uma token no arquivo .env...")
        time.sleep(10)
        
    logging.info("Token carregado! Iniciando o pré-processador.")
    processor = VineyardDataProcessor(influxdb_token)
    processor.run()
