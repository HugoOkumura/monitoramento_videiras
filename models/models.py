
MQTT_DATA = {
    "type" : "object",
    "properties": {
        "sensor_id": {"type": "number"},
        "temperatura": {"type": "number"},
        "umidade": {"type": "number"}
    },
    "required": ["sensor_id", "temperatura", "umidade"],
    "additionalProperties": False
}

INGESTED_DATA = {
    "type" : "object",
    "properties": {
        "sensor_id": {"type": "number"},
        "temperatura": {"type": "number"},
        "umidade": {"type": "number"},
        "variedade": {"type":"string"},
        "ingestion_timestamp": {
            "type":"string",
            "format":"date-time"            
        },
    },
    "required": ["sensor_id","temperatura","umidade","variedade","ingestion_timestamp"]
}