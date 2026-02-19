import os
import logging
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError, Message
from jsonschema import validate, ValidationError

from models.models import INGESTED_DATA

class KafkaConsumer():

    def __init__(self, config, message_handler):
        self.topic = config['kafka_topic']
        self.running = False
        self.message_handler = message_handler
        self.kafka_consumer = None

        self._consumer_config = {
            'bootstrap.servers': config['boostrap_servers'],
            'group.id': config['kafka_group_id'],
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'heartbeat.interval.ms': 15000,
            'session.timeou.ms': 45000
        }

    def run(self):
        self.kafka_consumer = Consumer(self._consumer_config)
        self.kafka_consumer.subscribe(self.topic)
        self.running = True
        try:
            while self.running:
                message = self.kafka_consumer.poll(timeout=3)

                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Kafka_consumer/run: {message.error()}")

                decoded = self._decode(message)
                self.message_handler(decoded)
                self.kafka_consumer.commit(asynchronous=False)

        except Exception as e:
            logging.error(f"Kafka_consumer/run: {e}")


    def stop(self):
        self.running = False
        if self.kafka_consumer:
            self.kafka_consumer.close()
            logging.info("Consumidor Kafka fechado")


    def _decode(self, message: Message):
        try:
            value = message.value()
            if value:
                data = json.loads(value.decode('utf-8'))

                validate(data, format=INGESTED_DATA)

                seconds = message.timestamp()[1] / 1000.0
                timestamp = datetime.fromtimestamp(seconds)

                data['_kafka_metadata'] = {
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'timestamp': timestamp
                }
                return data

        except json.JSONDecodeError as e:
            logging.error(f"Kafka_consumer/decode: Erro ao decodificar JSON: {e}")
        except ValidationError as e:
            logging.error(f"Kafka_consumer/decode: Formato de mensagem não está no formato padrão: {e} \n{data}")
        except Exception as e:
            logging.error(f"Kafka_consumer/decode: {e}")