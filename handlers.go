package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

func initializeRoutes(router *gin.Engine) {
	router.POST("/stream/start", startNewStream)
	router.POST("/stream/:stream_id/send", prepareAndSendData)
	router.GET("/stream/:stream_id/results", getStreamResults)
	router.GET("/stream/:stream_id/metrics", getStreamMetrics)
}

// Implement `startNewStream`, `prepareAndSendData`, `getStreamMetrics` here

func getStreamResults(ctx *gin.Context) {
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

	consumersMutex.RLock()
	consumerManager, exists := activeConsumers[streamID]
	consumersMutex.RUnlock()

	if !exists {
		ctx.JSON(http.StatusNotFound, gin.H{
			"error": "StreamID not found",
		})
		return
	}

	go consumerManager.ConsumeMessages()

	ctx.JSON(http.StatusOK, gin.H{
		"stream_id": streamID,
		"messages":  "Messages consumed successfully",
	})
}

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
		Topic:     "stock-trades",
		Partition: hashStreamID(streamID, prodManager.Config.NumPartitions), //num partitions 1500
		Key:       sarama.StringEncoder(fmt.Sprintf("%d", streamID)),
		Value:     sarama.ByteEncoder(jsonTrade),
	}
	select {
	case prodManager.Producer.Input() <- message: // Sends the message to Kafka
		prodManager.Metrics.MessagesSent.Add(1) // Increment the counter for successfully queued messages
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

	consumerManager, err := NewConsumerManager(config)
	if err != nil {
		ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to create a new consumer manager: %v", err),
		})
		return
	}

	streamID := generateUniqueStreamID() //getting a new id to use

	//needs lock to allow only one producer to be able to access the active streams map
	streamsMutex.Lock()
	activeStreams[streamID] = prodManager
	streamsMutex.Unlock()

	consumersMutex.Lock()
	activeConsumers[streamID] = consumerManager
	consumersMutex.Unlock()

	ctx.JSON(http.StatusOK, gin.H{
		"stream_id": streamID,
		"status":    "stream started",
	})
}

func getStreamMetrics(ctx *gin.Context) {
	streamID, err := parseStreamId(ctx.Param("stream_id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid stream ID",
		})
		return
	}
	prodManger, exists := streamManager.GetStream(streamID)
	if !exists {
		ctx.JSON(http.StatusNotFound, gin.H{
			"error": "Stream not found",
		})
		return
	}

	metrics := prodManger.Metrics
	ctx.JSON(http.StatusOK, gin.H{
		"stream_id":          streamID,
		"messahes_sent":      metrics.MessagesSent.Load(),
		"messages_succeeded": metrics.MessagesSucceeded.Load(),
		"messages_failed":    metrics.MessagesFailed.Load(),
	})
}
