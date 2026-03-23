
MQTT_DATA = {
    "type" : "object",
    "properties": {
        "sensor_id": {"type": "number"},
        "temperature": {"type": "number"},
        "humidity": {"type": "number"}
    },
    "required": ["sensor_id", "temperature", "humidity"],
    "additionalProperties": False
}

INGESTED_DATA = {
    "type" : "object",
    "properties": {
        "sensor_id": {"type": "number"},
        "temperature": {"type": "number"},
        "humidity": {"type": "number"},
        "variety": {"type":"string"},
        "ingestion_timestamp": {
            "type":"string",
            "format":"date-time"            
        },
    },
    "required": ["sensor_id","temperature","humidity","variety","ingestion_timestamp"]
}