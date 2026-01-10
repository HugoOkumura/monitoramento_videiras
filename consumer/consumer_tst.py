import paho.mqtt.client as mqtt
from datetime import datetime
import json
import time

BROKER_IP = "192.168.237.72"
BROKER_PORT = 1883
MQTT_TOPIC = "readings"


def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao broker: {rc}")
    client.subscribe(MQTT_TOPIC)
    print(f"Inscrito no tópico: {MQTT_TOPIC}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"{data} - {timestamp}")

    except Exception as e:
        print(f"Erro: {e}")
        print(f"Payload: {msg.payload.decode()}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        print(f"Conectando ao broker em {BROKER_IP}:{BROKER_PORT}")
        client.connect(BROKER_IP, BROKER_PORT,60)

        client.loop_forever()

    except KeyboardInterrupt:
        print(f"\nPrograma encerrado pelo usuário")
        client.disconnect()
    except Exception as e:
        print(f"\nErro de conexão: {e}")


if __name__ == "__main__":
    main() 