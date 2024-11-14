package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin" // keep this import
	"github.com/google/uuid"
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

type StockTrade struct {
	Timestamp time.Time `json:"timestamp"`
	Company   string    `json:"company"`
	Price     float64   `json:"price"`
	Volume    int       `json:"volume"`
	Action    string    `json:"action"`
	OrderID   string    `json:"order_id"`
}

var (
	activeStreams = make(map[int64]*ProducerManager)
	streamsMutex  sync.RWMutex
)

func main() {
	router := gin.Default()
	router.POST("/stream/start", startNewStream)
	router.POST("/stream/:stream_id/send", prepareAndSendData)
	router.GET("/stream/:stream_id/results")

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go handleShutdown(srv)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// parse the string into int64 format
// check if a matching stream is open
// populate stockTrade struct
// put it into a JSON byte format
// ensure the trade is valid (correct format)
// Make message for sarama to send to kafka
// send the message
func prepareAndSendData(ctx *gin.Context) {

	streamIDStr := ctx.Param("stream_id")
	streamID, err := parseStreamId(streamIDStr)
	if err != nil {
		log.Printf("Failed to parse stream ID '%s': %v", streamIDStr, err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid stream ID format",
			"details": err.Error(),
		})
		return
	}

	streamsMutex.RLock()
	prodManager, exists := activeStreams[streamID]
	streamsMutex.RUnlock()

	if !exists {
		ctx.JSON(http.StatusNotFound, gin.H{
			"error": "StreamID not found",
		})
		return
	}

	var trade StockTrade
	if err := ctx.BindJSON(&trade); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if trade.Timestamp.IsZero() {
		trade.Timestamp = time.Now()
	}

	if err := validateTrade(trade); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	jsonTrade, err := json.Marshal(trade)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to marshal trade to JSON: " + err.Error(),
		})
		return
	}
	message := &sarama.ProducerMessage{
		Topic: "stock-trades",
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", streamID)),
		Value: sarama.ByteEncoder(jsonTrade),
	}
	select {
	case prodManager.Producer.Input() <- message: // Sends the message to Kafka
		prodManager.Enqueued++ // Increment the counter for successfully queued messages
		ctx.JSON(http.StatusOK, gin.H{
			"stream_id": streamID,
			"trade":     trade,
		})
	case <-prodManager.Signals: // Handles interrupt signals (Ctrl+C)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Producer shutting down",
		})
	}
}

func validateTrade(trade StockTrade) error {
	if trade.Company == "" {
		return fmt.Errorf("company name is required")
	}
	if trade.Price <= 0 {
		return fmt.Errorf("trade price must be greater than 0")
	}
	if trade.Volume <= 0 {
		return fmt.Errorf("volume must be greater than 0")
	}
	if trade.Action != "buy" && trade.Action != "sell" {
		return fmt.Errorf("action must be either 'buy' or 'sell'")
	}
	if trade.OrderID == "" {
		return fmt.Errorf("orderID is required'")
	}
	return nil
}

func parseStreamId(stringStreamID string) (int64, error) {
	var streamID int64
	_, err := fmt.Sscanf(stringStreamID, "%d", &streamID)
	return streamID, err
}

func startNewStream(ctx *gin.Context) {
	config, err := loadConfig("./config.json")
	if err != nil {
		log.Println("Error importing config: ", err)
		ctx.JSON(500, gin.H{"error": "Failed to load config"})
		return
	}
	prodManager, err := NewProducerManager(config)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to create a new producer manager: %v", err),
		})
		return
	}

	streamID := generateUniqueStreamID() //getting a new id to use

	//needs lock to allow only one producer to be able to access the active streams map
	streamsMutex.Lock()
	activeStreams[streamID] = prodManager
	streamsMutex.Unlock()

	ctx.JSON(http.StatusOK, gin.H{
		"stream_id": streamID,
		"status":    "stream started",
	})
}

func handleShutdown(srv *http.Server) {
	quit := make(chan os.Signal, 1) //creating a channel for operating system signals (Ctrl + c)
	signal.Notify(quit, os.Interrupt)
	<-quit //pauses execution beyond until os.Interrupt signal given

	streamsMutex.Lock()                        //locks stream so only one will be affected at a time
	for _, prodManger := range activeStreams { //loops through active streams and closes them
		if err := prodManger.Producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err) //reports errors
		}
	}
	streamsMutex.Unlock()
	log.Printf("Shutting down server...")
	if err := srv.Close(); err != nil { //shuts server down and logs any errors
		log.Fatal("Server forced to shutdown:", err)
	}
}

// gets current timestamp
// gets a unigue code using uuid
// combines them to get globally unique id
func generateUniqueStreamID() int64 {
	currentTimestamp := time.Now().UnixNano() / int64(time.Microsecond)
	uniqueID := uuid.New().ID()
	ID := currentTimestamp + int64(uniqueID)
	fmt.Println(ID)
	return ID
}

// loads my config file
func loadConfig(filepath string) (Config, error) {
	file, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("Some error ocurred reading config file: %s", err)
	}
	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		fmt.Printf("Failed to parse json config file: %s", err)
	}

	return config, nil
}

// intialization ProducerManager struct
// loads config and sets up producer
// graceful shutdown on os interrupts (CTRL + c)
func NewProducerManager(config Config) (*ProducerManager, error) {
	producer, err := setupProducer([]string{config.Broker})
	if err != nil {
		fmt.Printf("Error creating a new Async Producer: %s", err)
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
func (prodManager *ProducerManager) handleSuccesses() {
	for success := range prodManager.Producer.Successes() {
		log.Printf("Message delivered to partition %d at offset %d\n", success.Partition, success.Offset)
	}
}

// handleErrors listens for failed message deliveries
func (prodManager *ProducerManager) handleErrors() {
	for err := range prodManager.Producer.Errors() {
		log.Printf("Failed to deliver message: %v\n", err)
	}
}

// reads in input from the terminal to send to kafka
// tracks interupt signals
func (prodManager *ProducerManager) ProduceMessages(topic, value string) error {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}
	select {
	case prodManager.Producer.Input() <- message: // Sends the message to Kafka
		prodManager.Enqueued++ // Increment the counter for successfully queued messages
		log.Printf("Queued message: %s (Topic: %s)", value, topic)
		return nil
	case <-prodManager.Signals: // Handles interrupt signals (Ctrl+C)
		return fmt.Errorf("interrupt signal recieved, stopping production")
	}
}
