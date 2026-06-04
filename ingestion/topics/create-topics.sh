#!/bin/bash

KAFKA_TOPICS=(
    "ventes"
    "stocks"
    "retours"
    "canaux_vente"
)

for topic in "${KAFKA_TOPICS[@]}"; do
    kafka-topics --create --topic "$topic" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    echo "Topic '$topic' created."

    if [ $? -eq 0 ]; then
        echo "Topic '$topic' created successfully."
    else
        echo "Failed to create topic '$topic'."
    fi
done



