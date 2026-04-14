import paho.mqtt.client as mqtt
import json
import threading
import time
import queue
import logging
import os
import sys
from datetime import datetime
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
        # MQTT consumer config
        self._BROKER = os.getenv('MQTT_BROKER')
        self._PORT   = os.getenv('MQTT_PORT')
        self._MQTT_TOPIC  = os.getenv('MQTT_TOPIC')        
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        # Kafka producer config
        self.KAFKA_BOOTSTRAP_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
        self.kafka_producer = Producer({
            "bootstrap.servers" : self.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "uva_vitoria_producer"
        })

        self.producer_queue = queue.Queue(maxsize=50)

    '''
    Inicialização de classe.
    Cria threads responsáveis para o consumidor do MQTT Broker e do produtor
    do Kafka
    '''
    def run(self):
        try:
            threading.Thread(target=self._mqtt_consumer, daemon=True).start()
            threading.Thread(target=self._kafka_producer, daemon=True).start()

            while True:
                time.sleep(1)

        except Exception as e:
            logging.error(f"run: {e}")

    '''
    Callback de conexão do MQTT.
    '''
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.client.subscribe(self._MQTT_TOPIC)
        logging.info(f"Subscribed to topic {self._MQTT_TOPIC}")

    '''
    -   Callback de consumo de mensage do MQTT.
        Ao receber uma mensagem no tópico especificado nas variáveis de ambiente
    valida o formato dos dados, filtra os dados fora de um certo range, adiciona
    metadados e armazena numa fila para o produtor do kafka.
    '''
    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())

            # Validação do formato dos dados
            validate(data, schema=MQTT_DATA)

            # data outliers verification
            data = self.null_outliers(data)

            # Adiciona metadados 
            enriched_data = {
                **data,
                "timestamp": datetime.now().timestamp(),
                "variety": "vitória",
                "ingestion_metadata": {
                    "protocol": client.protocol,
                    "qos": msg.qos
                }
            }

            if self.producer_queue.full():
                logging.warning("Buffer interno cheio")
                time.sleep(3)
                return
            
            self.producer_queue.put(enriched_data, block=True, timeout=5)
            self.client.ack(msg.mid, qos=1)
            logging.info("Ingestor recebeu uma mensagem com sucesso.")
 
        except ValidationError as e:
            logging.error(f"_on_message: invalid data {e}")
        except BaseException as e:
            logging.error(f"_on_message: {e}:\n{msg.payload.decode()}")


    def _producer_send_report(self, err, msg):
        if err is not None:
            logging.error(f"_producer_send_report: Erro ao enviar mensagem para o Kafka: {err}")
        else:
            logging.info(f"Mensagem enviada para o Kafka")


    '''
    - Thread que irá permanecer em loop para a conexão com o MQTT broker.
    '''
    def _mqtt_consumer(self):
        connected = False
        while not connected:
            try:
                self.client.connect(self._BROKER, int(self._PORT))

                connected = True
            except Exception as e:
                logging.error(f"MQTT_CONSUMER: {e}")
                time.sleep(5)

        logging.info(f"Connected to Broker!")
        self.client.loop_forever()

    '''
    Thread que irá enviar os dados consumidos pelo MQTT para o kafka 
    '''
    def _kafka_producer(self):
        while(True):
            try:
                data = self.producer_queue.get(block=True)
                if data:
                    message = json.dumps(data)
                    self.kafka_producer.produce(
                        self.KAFKA_TOPIC,
                        key=str(data['sensor_id']),
                        value=message.encode('utf-8'),
                        callback= self._producer_send_report
                    )

                    logging.info(f"Kafka_producer: Enviou uma leitura para o Kafka com sucesso")

            except Exception as e:
                logging.error(f"Kafka_producer: {e}")
                time.sleep(3)
            finally:
                self.producer_queue.task_done()
                self.kafka_producer.flush()

    '''
    - Método que verifica os outliers e anula-os quando encontrado
    '''
    def null_outliers(data:dict):
        for key in list(data.keys()):
            if key == "sensor_id":
                continue
            if data[key] not in range(VALUE_RANGES[key][0], VALUE_RANGES[key][1]):
                data[key] = None

        return data

if __name__ == "__main__":
    os.makedirs("./log", exist_ok=True)
    open("./log/log.log","a+").close()
    startLog()
    try:
        ingest = DataIngestor()
        ingest.run()
    except Exception as e:
        logging.error(f"main: {e}")