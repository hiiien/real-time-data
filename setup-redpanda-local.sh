#!/bin/bash
# File: scripts/setup-redpanda-local.sh

echo "Installing Redpanda locally..."
curl -fsSL https://packages.vectorized.io/install.sh | bash
sudo apt update
sudo apt install -y redpanda
sudo systemctl start redpanda

echo "Creating default topic (test-topic)..."
rpk topic create test-topic

echo "Redpanda is running on localhost:9092"
