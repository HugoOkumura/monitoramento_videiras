import os
import json
import threading
import time
import logging
import queue
import numpy as np
from typing import Dict, Any
from datetime import datetime
from confluent_kafka import Consumer, KafkaException

from kafka_consumer import KafkaConsumer
from agregador import Agregador
from influxdb_writer import InfluxDBWriter

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


class PreProcessor():
    def __init__(self):
        #Kafka config
        kafka_config = {
            'boostrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'kafka_topic': os.getenv('KAFKA_TOPIC'),
            'kafka_group_id': os.getenv('KAFKA_GROUP_ID'),
        }
        self.kafka_consumer = KafkaConsumer(kafka_config, self.kafka_message_callback)

        #InfluxDB config
        self.influxdb = InfluxDBWriter(
            url=os.getenv('INFLUXDB_URL'),
            token=os.getenv('INFLUXDB_TOKEN'),
            org=os.getenv('INFLUXDB_ORG'),
            bucket=os.getenv('INFLUXDB_BUCKET')
        )

        #agregador por período de tempo
        self.agregador = Agregador()
        self.lock = threading.Lock()

        #Thread de processamento
        self.janela_tempo = os.getenv('AGREGAR_MINUTOS')
        self.lote_timer = threading.Timer(60*self.janela_tempo, self._processamento_lote)
        self.running = False

    def run(self):
        self.running = True

        self.lote_timer.start()
        
        try:
            self.kafka_consumer.run()
        except Exception as e:
            logging.info(f"PreProcessor/ru: {e}")

    
    def kafka_message_callback(self, message: Dict[str: Any]):
        try:
            logging.info(f"Mensagem recebida")
            self.agregador.add_leitura(message)
        except Exception as e:
            logging.error(f"PreProcessor/kafka_message_callback:{e}")

    def _processamento_lote(self):
        '''
        1.  Calcula temperatura média, mínima e máxima dentro do período de tempo,
            o cálculo é feito por cada sensor individual
        2.  Adiciona a média geral dos valores lidos no vinhedo
        3.  Adiciona os dados do lote com dados vindo de satélites (to-do)
        '''
        try:
            with self.lock:
                lote: Dict[str, Any] = self.agregador.get_lote()

                for key in lote.keys():
                    leituras = lote[key]

                    temperatura_media = np.mean([x['temperatura'] for x in leituras])
                    umidade_media = np.mean([x['umidade'] for x in leituras])

                    riscos = self.verifica_risco_doenca(temperatura_media,umidade_media)
                    
                    pre_process_data = {
                        "sensor_id" : key,
                        "temperatura_media": temperatura_media,
                        "temperatura_min": np.min([x['temperatura'] for x in leituras]),
                        "temperatura_max": np.max([x['temperatura'] for x in leituras]),
                        "umidade_media": umidade_media,
                        "riscos": riscos,   
                        #...
                        "pre_process_timestamp": datetime.now().isoformat()
                    }   

            #obtém lote por períodos de tempo (ex: de cada 5/10/15/etc minutos) - definir talvez por uma variavel de ambiente
        except Exception as e:
            logging.error(f"PreProcessor/__processamento_lote: {e}")
            time.sleep(5)

        def verifica_risco_doenca(temperatura_media, umidade_media):
            riscos = []
            #míldio
            #to-do: adicionar tempo de chuva, presença de chuva, etc
            if temperatura_media in range(18,22) and umidade_media > 75:
                riscos.append({"míldio": "alto"})
            else:
                riscos.append({"míldio": "baixo"})

            #oídio
            if temperatura_media in range(20,30) and umidade_media < 50:
                riscos.append({"oídio": "alto"})
            else:
                riscos.append({"oídio": "baixo"})

            return riscos

if __name__ == "__main__":
    os.makedirs("./log", exist_ok=True)
    open("./log/log.log","a+").close()
    startLog()
    try:
        pass
        # ingest = DataIngestor()
        # ingest.run()
    except Exception as e:
        logging.error(e)