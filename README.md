# Master Thesis - Brno University Of Technology 2023/2024 - Cloud Computing System for Real-time Data Processing


## Technologies used:
Apache Flink \
Apache Kafka \
Apache Sedona \
Redis \
QuestDB \
Postgres with postgis \
Nginx \
FastAPI \
Docker

## Set up
``docker compose build`` \
``docker compose up`` 

### Then questDB connector creation is needed:
``python create_kafka_questdb_connector.py`` 

## Stop
``docker compose down`` \
Don't forget to remove volumes and images too. 

### 2. Generator
Then start generating data and sending them to websocket server. \
Generator randomly chooses id of device from array of 1 to X and random x and y points from specified intervals by limit_x or limit_y and assigns a current timestamp. \
List of ids and intervals of X and Y are set through optional arguments. If not set default values are used. \
``python generator/main.py [--num_dev] [--limit_x] [--limit_y] [--ws] [--limit]``
##### Default values if not specified: 
--num_dev=1000, Number of devices \
--limit_x='0,100.0', Longitude interval.\
--limit_y='0,100.0', Latitude interval. \
--ws='ws://localhost:8088/ws', Websocket connection uri. \ 
--limit=None, Limit number of generated records.

## Important links: 
QuestDB: http://localhost:9000/ \
API: http://127.0.0.1:8088/  \
Kafka UI: http://localhost:8080/

