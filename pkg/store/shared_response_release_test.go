// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"errors"
	"sync"
	"testing"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type sharedMemorySeriesStream struct {
	responses []*storepb.SeriesResponse
	i         int
}

func (s *sharedMemorySeriesStream) Next() bool {
	if s.i >= len(s.responses) {
		return false
	}
	s.i++
	return true
}

func (s *sharedMemorySeriesStream) At() *storepb.SeriesResponse {
	return s.responses[s.i-1]
}

func TestSharedStoreResponseRelease(t *testing.T) {
	t.Run("eager response set", func(t *testing.T) {
		first := storepb.NewSeriesResponse(&storepb.Series{})
		second := storepb.NewSeriesResponse(&storepb.Series{})
		responses := []*storepb.SeriesResponse{first, second}
		set := &eagerRespSet{bufferedResponses: responses, wg: &sync.WaitGroup{}}

		if !set.Next() || set.At() != first || set.At() != first {
			t.Fatal("first response was not stable across repeated At calls")
		}
		if !set.Next() || set.At() != second {
			t.Fatal("second response was not returned")
		}
		if responses[0] != nil {
			t.Fatal("advancing eager response set retained the previous response")
		}
		if set.Next() {
			t.Fatal("unexpected response after final response")
		}
		if responses[1] != nil {
			t.Fatal("terminal advancement retained the final response")
		}
	})

	t.Run("ring buffer releases popped slots across wraparound", func(t *testing.T) {
		var mu sync.Mutex
		rb := newRingBuffer(2, &mu)
		responses := []*storepb.SeriesResponse{
			storepb.NewSeriesResponse(&storepb.Series{}),
			storepb.NewSeriesResponse(&storepb.Series{}),
			storepb.NewSeriesResponse(&storepb.Series{}),
		}

		mu.Lock()
		defer mu.Unlock()
		for _, response := range responses[:2] {
			if !rb.append(response) {
				t.Fatal("append failed")
			}
		}
		if got := rb.pop(); got != responses[0] {
			t.Fatal("pop returned the wrong first response")
		}
		if rb.bufferedResponses[0] != nil {
			t.Fatal("pop retained the response in its ring slot")
		}
		if !rb.append(responses[2]) {
			t.Fatal("append after pop failed")
		}
		if got := rb.pop(); got != responses[1] {
			t.Fatal("pop returned the wrong second response")
		}
		if rb.bufferedResponses[1] != nil {
			t.Fatal("pop retained the second response in its ring slot")
		}
		if got := rb.pop(); got != responses[2] {
			t.Fatal("pop returned the wrong wrapped response")
		}
		if rb.bufferedResponses[2] != nil {
			t.Fatal("pop retained the wrapped response in its ring slot")
		}
	})

	t.Run("deduplicator preserves warning order while releasing consumed outputs", func(t *testing.T) {
		series := func(name string) *storepb.SeriesResponse {
			return storepb.NewSeriesResponse(&storepb.Series{
				Labels: labelpb.ZLabelsFromPromLabels(labelsFromStrings("name", name)),
			})
		}
		responses := []*storepb.SeriesResponse{
			storepb.NewWarnSeriesResponse(errors.New("first warning")),
			storepb.NewWarnSeriesResponse(errors.New("second warning")),
			series("a"),
			series("b"),
		}
		d := NewResponseDeduplicator(&sharedMemorySeriesStream{responses: responses})
		for i, warning := range []string{"first warning", "second warning"} {
			if !d.Next() {
				t.Fatalf("warning %d was not returned in input order", i)
			}
			current := d.At()
			if current.GetWarning() != warning || d.At() != current {
				t.Fatalf("warning %d was not returned in input order", i)
			}
			for j := 0; j < i; j++ {
				if d.bufferedResp[j] != nil {
					t.Errorf("advancing to warning %d retained consumed output at slot %d", i, j)
				}
			}
		}
		if !d.Next() || d.At().GetSeries() == nil || d.At().GetSeries().Labels[0].Name != "name" {
			t.Fatal("expected the first series after warnings")
		}
		storage := d.bufferedResp[:cap(d.bufferedResp)]
		if !d.Next() || d.At().GetSeries() == nil {
			t.Fatal("expected the next series")
		}
		for i := 1; i < len(storage); i++ {
			if storage[i] != nil {
				t.Errorf("deduplicator retained consumed output at slot %d", i)
			}
		}
	})

	t.Run("deduplicator releases group storage and keeps emitted chunks", func(t *testing.T) {
		chunk := func(data string) *storepb.Chunk { return &storepb.Chunk{Data: []byte(data)} }
		first := storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.ZLabelsFromPromLabels(labelsFromStrings("name", "same")),
			Chunks: []storepb.AggrChunk{
				{Raw: chunk("raw")}, {Count: chunk("count")}, {Sum: chunk("sum")},
				{Min: chunk("min")}, {Max: chunk("max")}, {Counter: chunk("counter")},
			},
		})
		duplicate := storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.ZLabelsFromPromLabels(labelsFromStrings("name", "same")),
			Chunks: []storepb.AggrChunk{{Raw: chunk("other")}},
		})
		next := storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.ZLabelsFromPromLabels(labelsFromStrings("name", "next")),
		})
		inputs := []*storepb.SeriesResponse{first, duplicate, next}
		d := NewResponseDeduplicator(&sharedMemorySeriesStream{responses: inputs})

		if !d.Next() {
			t.Fatal("expected first deduplicated group")
		}
		out := d.At()
		if d.At() != out {
			t.Fatal("repeated At changed the current deduplicated response")
		}
		if out.GetSeries() == nil || len(out.GetSeries().Chunks) != 7 {
			t.Fatalf("unexpected merged output: %v", out)
		}
		assertPayloads := func() {
			gotPayloads := map[string]bool{}
			for _, aggregate := range out.GetSeries().Chunks {
				for _, field := range []*storepb.Chunk{aggregate.Raw, aggregate.Count, aggregate.Sum, aggregate.Min, aggregate.Max, aggregate.Counter} {
					if field != nil {
						gotPayloads[string(field.Data)] = true
					}
				}
			}
			for _, payload := range []string{"raw", "count", "sum", "min", "max", "counter", "other"} {
				if !gotPayloads[payload] {
					t.Errorf("emitted output lost aggregate payload %q", payload)
				}
			}
		}
		assertPayloads()
		if len(d.chunkDedupMap) != 0 {
			t.Error("deduplicator retained chunk references after emitting a group")
		}
		groupStorage := d.bufferedSameSeries[:cap(d.bufferedSameSeries)]
		for i, response := range groupStorage {
			if response != nil {
				t.Errorf("deduplicator retained input response at group slot %d", i)
			}
		}
		if d.prev != next {
			t.Fatal("deduplicator did not retain the lookahead response")
		}
		if !d.Next() || d.At() != next {
			t.Fatal("deduplicator did not return the lookahead response")
		}
		if d.Next() {
			t.Fatal("unexpected response after terminal group")
		}
		if len(d.chunkDedupMap) != 0 {
			t.Error("deduplicator retained chunk references at EOF")
		}
		assertPayloads()
		terminalStorage := d.bufferedSameSeries[:cap(d.bufferedSameSeries)]
		for i, response := range terminalStorage {
			if response != nil {
				t.Errorf("deduplicator retained terminal group response at slot %d", i)
			}
		}
	})
}
