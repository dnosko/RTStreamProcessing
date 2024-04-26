#!/bin/bash
# Wait for Kafka to be ready
kafka_ready() {
    kafka-topics --bootstrap-server kafka1:19092 --list >/dev/null 2>&1
}

echo "Waiting for Kafka to become ready..."
until kafka_ready; do
    sleep 5
    echo "Retrying..."
done

echo "Kafka is ready. Configuring topics..."

CLEANUP_POLICY="compact,delete"
# Create topics
kafka-topics --bootstrap-server kafka1:19092 --create --topic new_locations --partitions 32
kafka-topics --bootstrap-server kafka1:19092 --create --topic collisions --partitions 32
# Alter topics
kafka-topics --bootstrap-server kafka1:19092 --alter --topic new_locations --partitions 32
kafka-topics --bootstrap-server kafka1:19092 --alter --topic collisions --partitions 32

sleep 10 # wait until altering is finished
kafka-configs --bootstrap-server kafka1:19092 --entity-type topics --entity-name collisions --alter --add-config cleanup.policy=[compact,delete]

tail -f /dev/null