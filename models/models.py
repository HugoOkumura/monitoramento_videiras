
MQTT_DATA = {
    "type" : "object",
    "properties": {
        "sensor_id": {"type": ["number", "null"]},
        "temperature": {"type": ["number", "null"]},
        "air_humidity": {"type": ["number", "null"]}
    },
    "required": ["sensor_id", "temperature", "air_humidity"],
    "additionalProperties": False
}

INGESTED_DATA = {
    "type" : "object",
    "properties": {
        "sensor_id": {"type": ["number", "null"]},
        "temperature": {"type": ["number", "null"]},
        "air_humidity": {"type": ["number", "null"]},
        "variety": {"type":"string"},
        "ingestion_timestamp": {
            "type":"string",
            "format":"date-time"            
        },
    },
    "required": ["sensor_id","temperature","air_humidity","variety","ingestion_timestamp"]
}

VALUE_RANGES = {
    "temperature": [-10,50],
    "air_humidity": [0,100],
}