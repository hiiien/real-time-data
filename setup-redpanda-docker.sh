#!/bin/bash
# File: scripts/setup-redpanda-docker.sh

echo "Starting Redpanda in Docker..."
docker-compose up -d
echo "Redpanda is running on localhost:9092"
