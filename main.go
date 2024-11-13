package main

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
)

func main() {
	// r := gin.Default()
	// r.GET("/ping", func(c *gin.Context) {
	// 	c.JSON(http.StatusOK, gin.H{
	// 		"message": "pong",
	// 	})
	// })
	// r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")

	router := gin.Default()
	router.POST("/stream/start", startNewStream)
	router.POST("/stream/:stream_id/send")
	router.GET("/stream/:stream_id/results")

	router.Run()
}

func startNewStream(ctx *gin.Context) {
	config, err := producer.loadConfig("./config.json")
	if err != nil {
		log.Println("Error importing config: ", err)
		ctx.JSON(500, gin.H{"error": "Failed to load config"})
		return

	}
	fmt.Println(config)
	fmt.Println("Hello World")
	ctx.JSON(200, gin.H{"status": "Stream started!"})
	return
}
