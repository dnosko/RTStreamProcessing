# Daša Nosková
# VUT FIT 2024
from time import sleep

import requests

connect_url = "http://connect:8083/connectors"
# connector configuration
connector_config = {
    "name": "QuestDB",
    "config": {
        "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
        "topics": "new_locations",
        "host": "questdb:9009",
        "name": "QuestDB",
        "value.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "timestamp.field.name": "timestamp",
        "include.key": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "table": "locations_table"
    }
}

# Send request to create the connector
while True:
    try:
        response = requests.post(connect_url, json=connector_config)
        if response.status_code == 201:
            print("Connector created successfully!")
        else:
            print("Failed to create connector:", response.text)
        exit(0)
    except requests.exceptions.ConnectionError:
        print("Connection error, will retry in 30 seconds...")
        sleep(30)
