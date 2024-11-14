package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

type ProducerManager struct {
	Producer sarama.AsyncProducer
	Config   Config
	Signals  chan os.Signal
	Metrics  *StreamMetrics //use the stream metrics struct so we track metrics for each producer
}

func NewProducerManager(config Config) (*ProducerManager, error) {
	producer, err := setupProducer([]string{config.Broker}, config)
	if err != nil {
		fmt.Printf("Error creating a new Async Producer: %s", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	manager := &ProducerManager{
		Producer: producer,
		Config:   config,
		Signals:  signals,
		Metrics:  &StreamMetrics{},
	}

	go manager.handleSuccesses()
	go manager.handleErrors()

	return manager, nil

}

func (prodManager *ProducerManager) handleSuccesses() {
	for success := range prodManager.Producer.Successes() {
		prodManager.Metrics.MessagesSucceeded.Add(1)
		log.Printf("Message delivered to partition %d at offset %d\n", success.Partition, success.Offset)
	}
}

// handleErrors listens for failed message deliveries
func (prodManager *ProducerManager) handleErrors() {
	for err := range prodManager.Producer.Errors() {
		prodManager.Metrics.MessagesFailed.Add(1)
		log.Printf("Failed to deliver message: %v\n", err)
	}
}

func setupProducer(brokers []string, config Config) (sarama.AsyncProducer, error) {
	if config.NumPartitions <= 0 {
		return nil, fmt.Errorf("invalid number of partitions: %d", config.NumPartitions)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	return producer, nil
}
