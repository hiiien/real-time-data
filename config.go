package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct { //struct for my redpanda
	Broker        string `json:"broker"`
	Topic         string `json:"topic"`
	NumPartitions int32  `json:"num_partitions"`
	MaxConcurrent int    `json:"max_concurrent"`
}

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
