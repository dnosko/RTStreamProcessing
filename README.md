# Master Thesis - Brno University Of Technology 2023/2024 - Cloud Computing System for Real-time Data Processing

## Start up
``docker compose build`` \
``docker compose up`` \

### Then questDB connector creation is needed:
``python create_kafka_questdb_connector.py`` \

### 2. Generator
Then start generating data and sending them to websocket server. \
Generator randomly chooses id of device from array of 1 to X and random x and y points from intervals 0-X, 0-Y and assigns a current timestamp. \
List of ids and intervals of X and Y are set in constructor. If not set default values are used. \
``python generator/main.py``

## Important links: 
QuestDB: http://localhost:9000/ \
API: http://127.0.0.1:8088/  \
Kafka UI: http://localhost:8080/


