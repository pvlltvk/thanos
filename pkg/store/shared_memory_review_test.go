// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sync"
	"testing"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestSharedStoreReviewTerminal(t *testing.T) {
	response := storepb.NewSeriesResponse(&storepb.Series{})
	set := NewResponseDeduplicator(&sharedMemorySeriesStream{responses: []*storepb.SeriesResponse{response}})
	if !set.Next() || set.At() != response {
		t.Fatal("response was not returned")
	}
	for range 2 {
		if set.Next() {
			t.Fatal("unexpected response after final response")
		}
		if set.At() != nil {
			t.Fatal("terminal advancement retained the final response")
		}
	}
}

func TestSharedStoreReviewRingReuse(t *testing.T) {
	var mu sync.Mutex
	rb := newRingBuffer(1, &mu)
	mu.Lock()
	defer mu.Unlock()
	for range 4 {
		response := storepb.NewSeriesResponse(&storepb.Series{})
		if !rb.append(response) {
			t.Fatal("append after draining failed")
		}
		slot := rb.ringHead
		if rb.pop() != response {
			t.Fatal("pop returned the wrong response")
		}
		if rb.bufferedResponses[slot] != nil {
			t.Fatal("pop retained the response in a reused slot")
		}
	}
	if rb.ringHead != 0 || !rb.isEmpty() {
		t.Fatal("ring did not drain after reusing both physical slots")
	}
}
