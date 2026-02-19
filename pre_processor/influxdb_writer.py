from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

class InfluxDBWriter:

    def __init__(self, url, token, org, bucket):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        logging.info(f"Conectado ao InfluxDB: {url}")

    def write_reading(self, data):
        try:
            timestamp = data['timestamp']
            point = Point("vineyard_sensors") \
            .tag("sensor_id", data['sensor_id']) \
            .tag("variedade_uva", data.get('enriched_context', {}).get('variedade_uva', 'unknown')) \
            .time(timestamp)

            for reading_type, value in data['raw_readings'].items():
                point = point.field(f"raw_{reading_type}", value)

            # if data.get('aggregated_stats'):
            #     for reading_type, stats in data['aggregated_stats'].items():
            #         for stat_name, stat_value in stats.items():
            #             if isinstance(stat_value, (int, float)):
            #                 point = point.field(f"{reading_type}_{stat_name}", stat_value)


        except Exception as e:            
            logging.error(f"Erro ao escrever leitura no InfluxDB: {e}")
            raise