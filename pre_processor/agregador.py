import logging
import threading
import time
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any

class Agregador():

    def __init__(self):
        self.buffer = defaultdict(list)
        self.lock = threading.Lock()

        logging.info(f"Agregador inicializado configurado para juntar em janelas de {self.AGREGAR_MINUTO} minutos.")

    def add_leitura(self, leitura : Dict[str, Any]):
        with self.lock:
            self.buffer[leitura["sensor_id"]].append(leitura)

    def get_lote(self):
        return self.buffer

    def clear_lote(self):
        self.buffer = self.buffer.clear()