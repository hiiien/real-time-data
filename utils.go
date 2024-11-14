package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
)

func hashStreamID(streamID int64, numPartitions int32) int32 {
	startingHash := fnv.New32a()
	binary.Write(startingHash, binary.LittleEndian, streamID)
	hash := startingHash.Sum32()
	return int32(hash) % numPartitions
}

func generateUniqueStreamID() int64 {
	currentTimestamp := time.Now().UnixNano() / int64(time.Microsecond)
	uniqueID := uuid.New().ID()
	ID := currentTimestamp + int64(uniqueID)
	fmt.Println(ID)
	return ID
}

func parseStreamId(stringStreamID string) (int64, error) {
	var streamID int64
	_, err := fmt.Sscanf(stringStreamID, "%d", &streamID)
	return streamID, err
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
	consumersMutex.Lock()
	for _, consumer := range activeConsumers {
		consumer.Consumer.Close()
	}
	consumersMutex.Unlock()
	log.Printf("Shutting down server...")
	if err := srv.Close(); err != nil { //shuts server down and logs any errors
		log.Fatal("Server forced to shutdown:", err)
	}
}
