package main

import ( //external packages

	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

type Config struct { //struct for my redpanda
	Broker string `json:"broker"`
	Topic  string `json:"topic"`
}

// async kafka producer
// config data for producer
// OS signal handler for graceful shutdown
// number used to track # of messages enqueued in the kafka producer
type ProducerManager struct {
	Producer sarama.AsyncProducer
	Config   Config
	Signals  chan os.Signal
	Enqueued int
}

// loads my config file
func loadConfig(filepath string) Config {
	file, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Println("Some error ocurred reading config file: %s", err)
	}
	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		fmt.Println("Failed to parse json config file: %s", err)
	}

	return config
}

// intialization ProducerManager struct
// loads config and sets up producer
// graceful shutdown on os interrupts (CTRL + c)
func NewProducerManager(config Config) (*ProducerManager, error) {
	producer, err := setupProducer([]string{config.Broker})
	if err != nil {
		fmt.Println("Error creating a new Async Producer: %s", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	manager := &ProducerManager{
		Producer: producer,
		Config:   config,
		Signals:  signals,
	}

	go manager.handleSuccesses()
	go manager.handleErrors()

	return manager, nil

}

// Sets up producer to have a newConfig that
// Retries 5 times on a failure
// Allows for a seperate Success channel for monitoring
// Allows for a seperate Error channel for monitoring
// Returns the newAsyncProducer made with the configs
func setupProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return sarama.NewAsyncProducer(brokers, config)
}

// listens for any successful message deliveries
func (pm *ProducerManager) handleSuccesses() {
	for success := range pm.Producer.Successes() {
		log.Printf("Message delivered to partition %d at offset %d\n", success.Partition, success.Offset)
	}
}

// handleErrors listens for failed message deliveries
func (pm *ProducerManager) handleErrors() {
	for err := range pm.Producer.Errors() {
		log.Printf("Failed to deliver message: %v\n", err)
	}
}

// reads in input from the terminal to send to kafka
// tracks interupt signals
func (pm *ProducerManager) ProduceMessages(topic, value string) error {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}
	select {
	case pm.Producer.Input() <- message: // Sends the message to Kafka
		pm.Enqueued++ // Increment the counter for successfully queued messages
		log.Printf("Queued message: %s (Topic: %s)", value, topic)
		return nil
	case <-pm.Signals: // Handles interrupt signals (Ctrl+C)
		return fmt.Errorf("Interrupt signal recieved, stopping production")
	}
}
