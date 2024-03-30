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
# Alter topic
kafka-configs --bootstrap-server kafka1:19092 --entity-type topics --entity-name collisions --alter --add-config cleanup.policy=[compact,delete]
kafka-topics --bootstrap-server kafka1:19092 --alter --topic collisions --partitions 2
kafka-topics --bootstrap-server kafka1:19092 --alter --topic new_locations --partitions 2


tail -f /dev/null