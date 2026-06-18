import paho.mqtt.client as mqtt
import json
import threading
import time
import queue
import logging
import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd
import requests

# Bibliotecas Oficiais da Open-Meteo
import openmeteo_requests
import requests_cache
from retry_requests import retry

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from confluent_kafka import Producer

from models.models import MQTT_DATA, VALUE_RANGES

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

class DataIngestor:

    def __init__(self):
        # MQTT Config
        self._BROKER = os.getenv('MQTT_BROKER', 'mosquitto')
        self._PORT   = int(os.getenv('MQTT_PORT', 1883))
        self._MQTT_TOPIC  = os.getenv('MQTT_TOPIC', 'uva/mesa/leitura')        
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        self.LATITUDE = float(os.getenv('LATITUDE', '-23.48'))
        self.LONGITUDE = float(os.getenv('LONGITUDE', '-51.79'))

        # Inicialização do Cliente Oficial Open-Meteo com Cache e Retry nativos
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)

        # Kafka Config
        self.KAFKA_BOOTSTRAP_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.KAFKA_TOPIC_SENSOR = os.getenv('KAFKA_TOPIC_SENSOR', 'telemetry.raw')
        self.KAFKA_TOPIC_API    = os.getenv('KAFKA_TOPIC_API', 'daily.climate.api')
        self.kafka_producer = Producer({
            "bootstrap.servers" : self.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "vineyard_ingestor"
        })

        # Fila interna concorrente (Thread-safe)
        self.producer_queue = queue.Queue(maxsize=200)

    def run(self):
        # Threads assíncronas para balancear os múltiplos protocolos e tempos
        threading.Thread(target=self._mqtt_listener, daemon=True).start()
        threading.Thread(target=self._open_meteo_poller, daemon=True).start()
        threading.Thread(target=self._kafka_producer_loop, daemon=True).start()

        logging.info("Ingestor Multiprotocolo iniciado e rodando com sucesso.")
        while True:
            time.sleep(1)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.client.subscribe(self._MQTT_TOPIC)
        logging.info(f"MQTT conectado. Escutando o tópico: {self._MQTT_TOPIC}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            
            validate(instance=payload, schema=MQTT_DATA)
            
            payload = self.null_outliers(payload)

            evento = {
                "origin_type": "NODE_SENSOR",
                "sensor_id": payload['sensor_id'],
                "timestamp": int(time.time()),
                "temperature": payload['temperature'],
                "air_humidity": payload['air_humidity'],
                "latitude": self.LATITUDE,
                "longitude": self.LONGITUDE,
            }

            self.producer_queue.put(evento)
        except ValidationError as val_err:
            logging.error(f"Payload MQTT inválido para o Schema: {val_err.message}")
        except Exception as e:
            logging.error(f"Erro ao processar mensagem MQTT: {e}")

    def _mqtt_listener(self):
        while True:
            try:
                self.client.connect(self._BROKER, self._PORT)
                self.client.loop_forever()
            except Exception as e:
                logging.error(f"Conexão com o Broker MQTT falhou: {e}. Tentando novamente em 5s...")
                time.sleep(5)

    def _open_meteo_poller(self):
        """
        Consome os dados da API Open-Meteo usando o SDK oficial de forma planejada.
        Executa diariamente às 06:00 da manhã 
        Coleta os dados do dia anterior na Open-Meteo.
        """
        url = "https://archive-api.open-meteo.com/v1/archive"
        
        while True:
            logging.info(f"Iniciando chamada agendada ao SDK Open-Meteo (Lat: {self.LATITUDE}, Lon: {self.LONGITUDE})...")
            today = date.today()
            last_day = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            params = {
                "latitude": self.LATITUDE,
                "longitude": self.LONGITUDE,
                "start_date": last_day,
                "end_date": last_day,
                "daily": ["precipitation_sum", "et0_fao_evapotranspiration", "precipitation_hours", "daylight_duration", "sunshine_duration"],
                "timezone": "America/Sao_Paulo",
            }

            try:
                responses = self.openmeteo.weather_api(url, params=params)
                response = responses[0]

                daily = response.Daily()
                daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()[0]
                fao_evapotranspiration = daily.Variables(1).ValuesAsNumpy()[0]
                daily_precipitation_hours = daily.Variables(2).ValuesAsNumpy()[0]
                daylight_duration = daily.Variables(3).ValuesAsNumpy()[0]
                sunshine_duration = daily.Variables(4).ValuesAsNumpy()[0]

                climate_event = {
                    "origin_type": "METEO_API",
                    "sensor_id": "OPEN_METEO_STATION",
                    "timestamp": int(time.time()),
                    "precipitation": float(daily_precipitation_sum),
                    "evapotranspiration_api": float(fao_evapotranspiration),
                    "precipitation_hours": float(daily_precipitation_hours),
                    "daylight_duration": float(daylight_duration),
                    "sunshine_duration": float(sunshine_duration)
                }
                self.producer_queue.put(climate_event)
                logging.info("Dados meteorológicos da Open-Meteo ingeridos com sucesso.")

                now = datetime.now()
                next_run = now.replace(hour=6,minute=0,second=0,microsecond=0)
                if now >= next_run:
                    next_run += timedelta(days=1)
                
                wait_time = (next_run-now).total_seconds()
                logging.info(f"Próxima chamada agendada para {next_run.strftime('%d/%m/%Y %H:%M:%S')}")

                time.sleep(wait_time)

            except Exception as e:
                logging.error(f"Falha ao coletar dados do SDK Open-Meteo: {e}")

    def _kafka_producer_loop(self):
        while True:
            try:
                data = self.producer_queue.get(block=True)
                if data:
                    message = json.dumps(data)
                    if data["sensor_id"] == 'OPEN_METEO_STATION':
                        self.kafka_producer.produce(
                            self.KAFKA_TOPIC_API,
                            key=str(data['sensor_id']).encode('utf-8'),
                            value=message.encode('utf-8'),
                            callback=self._producer_send_report
                        )
                    else:
                        self.kafka_producer.produce(
                            self.KAFKA_TOPIC_SENSOR,
                            key=str(data['sensor_id']).encode('utf-8'),
                            value=message.encode('utf-8'),
                            callback=self._producer_send_report
                        )

            except Exception as e:
                logging.error(f"Erro no loop de envio para o Kafka: {e}")
                time.sleep(3)
            finally:
                self.producer_queue.task_done()
                self.kafka_producer.flush()

    def _producer_send_report(self, err, msg):
        if err is not None:
            logging.error(f"Falha na entrega da mensagem no Kafka: {err}")
        else:
            logging.info(f"Mensagem enviada com sucesso para o tópico {msg.topic()} [Partição: {msg.partition()}]")

    def null_outliers(self, data: dict):
        # Varre o payload limpando dados espúrios conforme sua regra de negócio
        for key in list(data.keys()):
            if key == "sensor_id" or key == "timestamp":
                continue
            if key in VALUE_RANGES:
                if float(data[key]) < VALUE_RANGES[key][0] or float(data[key]) > VALUE_RANGES[key][1]:
                    logging.warning(f"Outlier detectado na métrica '{key}': {data[key]}. Setando para None.")
                    data[key] = None
        return data

if __name__ == "__main__":
    os.makedirs("./log", exist_ok=True)
    if not os.path.exists("./log/log.log"):
        open("./log/log.log", "w").close()
    startLog()
    
    try:
        ingest = DataIngestor()
        ingest.run()
    except Exception as e:
        logging.critical(f"Falha catastrófica no Ingestor: {e}")



    