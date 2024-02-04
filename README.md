# Master Thesis at Brno University Of Technology 2023/2024 - Cloud Computing System for Real-time Data Processing

## Start up
### 1. Apache Kafka
First start Apache Kafka and QuestDB through docker. \
I used docker-compose from this project https://github.com/questdb/kafka-questdb-connector/tree/main/kafka-questdb-connector-samples/confluent-docker-images 
and fixed it to align with my usecase, it can be found here as docker-compose.yaml. 
The steps are listed below: \
``docker compose build`` \
``docker compose up`` \
Then go to http://localhost:8080/ui/clusters/kafka/connectors and click create connector. \
The name should be QuestDB and config is this:
``{
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
``
More info can be found on QuestDB documentation site: https://questdb.io/docs/third-party-tools/kafka/questdb-kafka/ 

The kafka should be accesible from localhost:9092
##### Set up topics
Either: \
``docker exec -it kafka1 /bin/bash`` \
``/bin/kafka-topics --create --topic new_locations --bootstrap-server localhost:9092`` 
or through kafka UI http://localhost:8080/ui/clusters/kafka/all-topics 
## TODO expose ak nie iba localhost

#### Stopping the kafka service - TODO mozno este ulozenie dat po stopnuti?????
#### TODO FIX
``docker-compose -f zk-single-kafka-single.yml stop`` \
or to remove also containers \
``docker-compose -f zk-single-kafka-single.yml down``
### 2. Producer
Start producer which serves as a websocket server. \
``python producer/main.py``
### 2. Generator
Then start generating data and sending them to websocket server. \
Generator randomly chooses id of device from array of 1 to X and random x and y points from intervals 0-X, 0-Y and assigns a current timestamp. \
List of ids and intervals of X and Y are set in constructor. If not set default values are used. \
``python generator/main.py``