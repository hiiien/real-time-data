package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type ConsumerManager struct {
	Consumer sarama.Consumer
	Config   Config
}

func NewConsumerManager(config Config) (*ConsumerManager, error) {
	consumer, err := setupConsumer([]string{config.Broker})
	if err != nil {
		return nil, err
	}

	manager := &ConsumerManager{
		Consumer: consumer,
		Config:   config,
	}

	return manager, nil
}

func setupConsumer(brokers []string) (sarama.Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create new consumer: %w", err)
	}

	return consumer, nil
}

func (consumerManager *ConsumerManager) ConsumeMessages() {
	partitions, err := consumerManager.Consumer.Partitions("stock-trade")
	if err != nil {
		log.Fatal("Failed to get list of partitions: %w", err)
	}

	for _, partition := range partitions {
		partitionComsumer, err := consumerManager.Consumer.ConsumePartition("stock-trades", partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to close partition consumer: %v", err)
		}
		defer func() {
			if err := partitionComsumer.Close(); err != nil {
				log.Fatalf("Failed to close parition consumer: %v", err)
			}
		}()

		go func(partitionConsumer sarama.PartitionConsumer) {
			for {
				select {
				case message := <-partitionConsumer.Messages():
					log.Printf("Received message from partition %d: %s\n", message.Partition, string(message.Value))
					//add message processing
				case err := <-partitionComsumer.Errors():
					log.Printf("Received error: %v\n", err)
				}
			}
		}(partitionComsumer)
	}
	select {}
}
