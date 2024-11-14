package main

import (
	"sync"
	"sync/atomic"
)

type StreamManager struct {
	streams     sync.Map
	maxStreams  int
	activeCount atomic.Int32
}

func NewStreamManager(maxStreams int) *StreamManager {
	return &StreamManager{
		maxStreams: maxStreams,
	}
}

func (streamManager *StreamManager) GetStream(streamID int64) (*ProducerManager, bool) {
	if value, ok := streamManager.streams.Load(streamID); ok {
		return value.(*ProducerManager), true
	}
	return nil, false
}
