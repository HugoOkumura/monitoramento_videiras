from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
import json
import os
import math
from datetime import datetime

def calculate_eto(data):
    lat_rad = (data['latitude'] * math.pi) / 180.0
    t_mean, t_min, t_max = data['temperature_mean'], data['temperature_min'], data['temperature_max']
    h_mean = data['humidity_mean']
    
    #dia do ano
    j = datetime.now().timetuple().tm_yday

    #inverso do quadrado da distância Terra-Sol
    dr = 1 + 0.033 * math.cos((2 * math.pi / 365) * j)

    #inclinação da curva da pressão de vapor d'agua
    delta = 0.409 * math.sin(((2 * math.pi / 365) * j) - 1.39)

    #ângulo solar do por-do-sol
    omega_s = math.acos(-math.tan(lat_rad) * math.tan(delta))
    
    # Radiação Extraterrestre Diária
    ra = ((24 * 60) / math.pi) * 0.0820 * dr * (omega_s * math.sin(lat_rad) * math.sin(delta) + math.sin(omega_s) * math.cos(lat_rad) * math.cos(delta))
    
    # pressão de saturação do vapor d'água
    es = (0.6108 * math.exp((17.27 * t_max) / (t_max + 237.3)) + 0.6108 * math.exp((17.27 * t_min) / (t_min + 237.3))) / 2.0
    
    # pressão parcial de vapor d'água
    ea = es * (h_mean / 100.0)
    
    #declinação solar
    delta_slope = (4098 * (0.6108 * math.exp((17.27 * t_mean) / (t_mean + 237.3)))) / ((t_mean + 237.3) ** 2)
    
    # pressão atmosférica
    pa = 101.3 * ((293-0.0065*560)/293)**5.26

    # constante psicométrica
    gama = 0.0000665 * pa
    
    # Radiação Global Estimada (Rs) e Líquida (Rn) sob dados faltosos
    rs = 0.16 * math.sqrt(t_max - t_min) * ra
    rns = (1 - 0.23) * rs
    rso = (0.75 + ((2**-5) * 560.0)) * ra # Considerado altitude média de 560m

    #Constante de Stefan-Boltzmann
    sigma = 0.000000004903

    #radiação líquida de ondas longas
    rnl = ((4.903e-9 * ((t_max + 273.16)**4) + 4.903e-9 * ((t_min + 273.16)**4)) / 2.0) * (0.34 - 0.14 * math.sqrt(ea)) * max(0.05, min(1.0, 1.35 * (rs / rso) - 0.35 if rso > 0 else 1.0))
    rn = rns - rnl
    
    # Equação de Penman-Monteith Final para dados faltosos (ETo diária)
    num_pm = 0.408 * delta_slope * rn + gama * (900 / (t_mean + 273)) * 2.0 * (es - ea)
    den_pm = delta_slope + gama * (1 + 0.34 * 2.0)
    eto = num_pm/den_pm
    eto_daily_pm = 0.0 if eto < 0 else eto

    data['eto_daily_pm'] =  eto_daily_pm

    # Hargreaves e Samani (1985)
    eto = 0.0023 * (t_mean + 17.8) * (t_max-t_min)**0.5 * ra 
    eto_daily_hm = 0.0 if eto < 0 else eto
    data['eto_daily_hm'] =  eto_daily_hm
    data["timestamp_eto"] = int(datetime.now().timestamp())
    data['registry_type'] = "EVAPOTRANSPIRATION_CALCULUS"
    
    return data

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    KAFKA_TOPIC_CONSUME = os.getenv('KAFKA_TOPIC_CONSUME', 'daily.climate.data')
    KAFKA_TOPIC_PRODUCE = os.getenv('KAFKA_TOPIC_PRODUCE', 'daily.climate.eto')

    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_topics(KAFKA_TOPIC_CONSUME) \
        .set_group_id("flink-eto-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
     
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Source-Clima")
    
    processed = stream.map(lambda x: json.loads(x))\
                      .map(lambda x: calculate_eto(x))\
                      .map(lambda x: json.dumps(x), output_type=Types.STRING())
    
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_record_serializer(KafkaRecordSerializationSchema.builder() \
        .set_topic(KAFKA_TOPIC_PRODUCE).set_value_serialization_schema(SimpleStringSchema()).build()) \
        .build()
    
    processed.sink_to(sink)
    env.execute("ET0_Calculus")

if __name__ == "__main__":
    main()