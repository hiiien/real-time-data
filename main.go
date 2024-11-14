package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin" // keep this import
)

// performance metrics
// total number of messages attempted to send
// how many successes
// how many failures
type StreamMetrics struct {
	MessagesSent      atomic.Int64
	MessagesSucceeded atomic.Int64
	MessagesFailed    atomic.Int64
}

// handles concurent streams management
// controlls the streams
// stores the active streams in a sync.Map

// async kafka producer
// config data for producer
// OS signal handler for graceful shutdown
// number used to track # of messages enqueued in the kafka producer

var (
	activeStreams   = make(map[int64]*ProducerManager)
	streamsMutex    sync.RWMutex
	streamManager   *StreamManager
	activeConsumers = make(map[int64]*ConsumerManager)
	consumersMutex  sync.RWMutex
)

// constructor for a new manager
// defines a stream limit to not overload the symstem

func main() {
	config, err := loadConfig("./config.json")
	if err != nil {
		log.Println("Error importing config: ", err)
	}

	broker := config.Broker
	if broker == "" {
		broker = "localhost:9092"
	}

	topicName := config.Topic
	if broker == "" {
		topicName = "stock-trades"
	}

	numPartitions := int32(100)
	replicationFactor := int16(1)

	if err := createTopic(broker, topicName, numPartitions, replicationFactor); err != nil {
		log.Fatalf("Error setting up topic: %v", err)
	}
	router := gin.Default()
	initializeRoutes(router)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go handleShutdown(srv)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func createTopic(broker string, topicName string, numPartitions int32, replicationFactor int16) error {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	if err != nil {
		return fmt.Errorf("failed to create cluster admin: %v", err)
	}
	defer admin.Close()

	topicDetail := sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     map[string]*string{},
	}

	err = admin.CreateTopic(topicName, &topicDetail, false)
	if err != nil {
		if err == sarama.ErrTopicAlreadyExists {
			log.Printf("Topic %s already exists", topicName)
		} else {
			return fmt.Errorf("failed to create topic: %v", err)
		}
	} else {
		log.Printf("Topic %s created sucessfully", topicName)
	}
	return nil
}
