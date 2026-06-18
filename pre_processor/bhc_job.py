from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
import json
import os
import math
from datetime import datetime

class StatefulBHCCalculator(KeyedProcessFunction):
    def __init__(self):
        self.cads = [75, 100, 150]
        self.methods = ["pm", "hm", "api"]
        self.state_descriptor = None
        self.arm_state = None

    def open(self, context):
        self.state_descriptor = ValueStateDescriptor("multicad_arm_state", Types.STRING())
        self.arm_state = context.get_state(self.state_descriptor)

    def create_initial_state(self):
        state = {}
        for method in self.methods:
            for cad in self.cads:
                state[f"arm_cad{cad}_{method}"] = cad
                state[f"neg_acum_cad{cad}_{method}"] = 0
        return state

    def calculate_bhc(self, arm: dict, data: dict, eto_method: str, precipitation: float, eto: float):
        method = eto_method.lower()
        balance = precipitation - eto

        for cad in self.cads:
            arm_key = f"arm_cad{cad}_{method}"
            neg_key = f"neg_acum_cad{cad}_{method}"

            previous_arm = arm[arm_key]

            if balance < 0:
                arm[neg_key] = arm[neg_key] + balance
                arm[arm_key] = cad * math.exp(arm[neg_key] / cad)

            else:

                if previous_arm > 0: 
                    arm[neg_key] = cad * math.log(previous_arm / cad)
                else:
                    arm[neg_key] = 0

                arm[arm_key] = min(cad, previous_arm + balance)

                if arm[arm_key] >= cad:
                    arm[neg_key] = 0

            current_arm = arm[arm_key]

            # ALT = alteração do armazenamento: ARM atual - ARM anterior
            alt = current_arm - previous_arm

            # ETr = evapotranspiração real
            if balance < 0:
                etr = precipitation + abs(alt)
            else:
                etr = eto

            # DEF = deficiência hídrica: ETo - ETr
            deficit = max(0, eto - etr)

            # EXC = excedente hídrico. Só ocorre quando há saldo positivo
            # maior do que a água que ainda cabia no solo.
            exc = max(0, balance - alt) if balance > 0 else 0

            data[arm_key] = current_arm
            data[f"etr_cad{cad}_{method}"] = etr
            data[f"alt_cad{cad}_{method}"] = alt
            data[f"exc_cad{cad}_{method}"] = exc
            data[f"def_cad{cad}_{method}"] = deficit
            data[neg_key] = arm[neg_key]

    def process_element(self, value, ctx):
        data = json.loads(value)

        state = self.arm_state.value()
        if state is None:
            previous_arms = self.create_initial_state()
        else:
            previous_arms = json.loads(state)

        precipitation = float(data["precipitation"])
        eto_pm = float(data["eto_daily_pm"])
        eto_hm = float(data["eto_daily_hm"])
        eto_api = float(data["eto_api"])

        self.calculate_bhc(previous_arms, data, "PM", precipitation, eto_pm)
        self.calculate_bhc(previous_arms, data, "HM", precipitation, eto_hm)
        self.calculate_bhc(previous_arms, data, "API", precipitation, eto_api)

        self.arm_state.update(json.dumps(previous_arms))

        data["timestamp_bhc"] = int(datetime.now().timestamp())
        data["registry_type"] = "BHC_CALCULUS"

        yield json.dumps(data)
        
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    KAFKA_TOPIC_CONSUME = os.getenv('KAFKA_TOPIC_CONSUME', 'daily.climate.eto')
    KAFKA_TOPIC_PRODUCE = os.getenv('KAFKA_TOPIC_PRODUCE', 'daily.climate.bhc')

    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_topics(KAFKA_TOPIC_CONSUME) \
        .set_group_id("flink-bhc-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Source-ETo")
    
    processed = stream.key_by(lambda x: "vinhedo_principal")\
                      .process(
                          StatefulBHCCalculator(),
                          output_type=Types.STRING()
                        )
    
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(kafka_server) \
        .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(KAFKA_TOPIC_PRODUCE).set_value_serialization_schema(SimpleStringSchema()).build()) \
        .build()
    processed.sink_to(sink)
    env.execute("BHC_CALCULUS")

if __name__ == "__main__":
    main()