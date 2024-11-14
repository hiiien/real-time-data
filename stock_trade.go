package main

import (
	"fmt"
	"time"
)

type StockTrade struct {
	Timestamp time.Time `json:"timestamp"`
	Company   string    `json:"company"`
	Price     float64   `json:"price"`
	Volume    int       `json:"volume"`
	Action    string    `json:"action"`
	OrderID   string    `json:"order_id"`
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
