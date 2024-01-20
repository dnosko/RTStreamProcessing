# Master Thesis at Brno University Of Technology 2023/2024 - Cloud Computing System for Real-time Data Processing

## Start up
### 1. Apache Kafka
First start Apache Kafka service through docker. \
I used docker-compose from this project https://github.com/conduktor/kafka-stack-docker-compose and tutorial can be found on this site https://www.conduktor.io/kafka/how-to-start-kafka-using-docker/, but the steps are listed also below: \
``git clone https://github.com/conduktor/kafka-stack-docker-compose.git`` \
``cd kafka-stack-docker-compose `` \
The kafka should be running at localhost:9092
##### Set up topics
``docker exec -it kafka1 /bin/bash`` \
``bin/kafka-topics.sh --create --topic new_locations --bootstrap-server localhost:9092``

## TODO expose ak nie iba localhost

#### Stopping the kafka service - TODO mozno este ulozenie dat po stopnuti?????
``docker-compose -f zk-single-kafka-single.yml stop`` \
or to remove also containers \
``docker-compose -f zk-single-kafka-single.yml down``
### 2. Consumer
Start consumer which serves as a websocket server. \
``python consumer/main.py``
### 2. Generator
Then start generating data and sending them to websocket server. \
Generator randomly chooses id of device from array of 1 to X and random x and y points from intervals 0-X, 0-Y and assigns a current timestamp. \
List of ids and intervals of X and Y are set in constructor. If not set default values are used. \
``python generator/main.py``