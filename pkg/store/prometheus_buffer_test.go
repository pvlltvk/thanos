// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusSeriesBuffer(t *testing.T) {
	t.Run("sorts and hands off exact chunks only at flush", func(t *testing.T) {
		upstream := &seriesBufferTestServer{}
		buffer := newTestSeriesBuffer(upstream, 0, false)
		chunks := []prompb.Chunk{{MinTimeMs: 10, MaxTimeMs: 20, Type: prompb.Chunk_XOR, Data: []byte{1, 2, 3}}}
		testutil.Ok(t, buffer.add(bufferLabels("z"), chunks))
		testutil.Ok(t, buffer.add(bufferLabels("a"), []prompb.Chunk{{MinTimeMs: 30, MaxTimeMs: 40, Type: prompb.Chunk_HISTOGRAM, Data: []byte{4, 5}}}))
		if len(upstream.responses) != 0 {
			t.Fatalf("Add sent %d responses before Flush", len(upstream.responses))
		}
		testutil.Ok(t, buffer.Flush())
		if len(upstream.responses) != 2 {
			t.Fatalf("got %d responses, want 2", len(upstream.responses))
		}
		first, second := upstream.responses[0].GetSeries(), upstream.responses[1].GetSeries()
		testutil.Equals(t, "a", first.Labels[0].Value)
		testutil.Equals(t, "z", second.Labels[0].Value)
		assertBufferChunk(t, first.Chunks[0], 30, 40, storepb.Chunk_HISTOGRAM, []byte{4, 5})
		assertBufferChunk(t, second.Chunks[0], 10, 20, storepb.Chunk_XOR, []byte{1, 2, 3})
	})

	t.Run("batch sizes flatten to the same sequence and preserve fragments", func(t *testing.T) {
		for _, batchSize := range []int{0, 1, 64} {
			t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
				upstream := &seriesBufferTestServer{}
				buffer := newTestSeriesBuffer(upstream, batchSize, false)
				inputs := []struct {
					series string
					data   byte
				}{{"b", 1}, {"a", 2}, {"a", 3}, {"c", 4}}
				for _, input := range inputs {
					chunk := prompb.Chunk{MinTimeMs: int64(input.data), MaxTimeMs: int64(input.data), Type: prompb.Chunk_XOR, Data: []byte{input.data}}
					testutil.Ok(t, buffer.add(bufferLabels(input.series), []prompb.Chunk{chunk}))
				}
				if len(upstream.responses) != 0 {
					t.Fatal("buffer emitted before Flush")
				}
				testutil.Ok(t, buffer.Flush())
				got := flattenBufferResponses(t, upstream.responses)
				if len(got) != 4 {
					t.Fatalf("got %d series, want 4", len(got))
				}
				want := []struct {
					series string
					data   byte
				}{{"a", 2}, {"a", 3}, {"b", 1}, {"c", 4}}
				for i, expected := range want {
					testutil.Equals(t, expected.series, got[i].Labels[0].Value)
					if len(got[i].Chunks) != 1 || len(got[i].Chunks[0].Raw.Data) != 1 || got[i].Chunks[0].Raw.Data[0] != expected.data {
						t.Fatalf("series %d payload=%+v, want data byte %d", i, got[i].Chunks, expected.data)
					}
				}
			})
		}
	})

	t.Run("series limit is checked before sample limit and counter increments once", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		upstream := &seriesBufferTestServer{}
		buffer := newLimitedTestSeriesBuffer(t, upstream, SeriesSelectLimits{SeriesPerRequest: 1, SamplesPerRequest: 1}, reg)
		chunk := prompb.Chunk{Data: []byte{1}}
		testutil.Ok(t, buffer.add(bufferLabels("a"), nil))
		if err := buffer.add(bufferLabels("b"), []prompb.Chunk{chunk}); err == nil {
			t.Fatal("expected Add to reject series over limit")
		}
		if err := buffer.add(bufferLabels("c"), []prompb.Chunk{chunk}); err == nil {
			t.Fatal("expected later Add to retain request limit failure")
		}
		_ = buffer.Flush()
		testutil.Equals(t, 0, len(upstream.responses))
		testutil.Equals(t, 1.0, selectDropCount(t, reg, "series"))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
	})

	t.Run("repeated flush does not reserve or send twice", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		upstream := &seriesBufferTestServer{}
		buffer := newLimitedTestSeriesBuffer(t, upstream, SeriesSelectLimits{SeriesPerRequest: 1, SamplesPerRequest: 120}, reg)
		testutil.Ok(t, buffer.add(bufferLabels("a"), []prompb.Chunk{{Data: []byte{1}}}))
		testutil.Ok(t, buffer.Flush())
		testutil.Ok(t, buffer.Flush())
		testutil.Equals(t, 1, len(upstream.responses))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
	})

	t.Run("sample budget charges 120 per chunk", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		upstream := &seriesBufferTestServer{}
		buffer := newLimitedTestSeriesBuffer(t, upstream, SeriesSelectLimits{SeriesPerRequest: 10, SamplesPerRequest: 239}, reg)
		chunks := []prompb.Chunk{{Data: []byte{1}}, {Data: []byte{2}}}
		if err := buffer.add(bufferLabels("a"), chunks); err == nil {
			t.Fatal("expected Add to reject 240-sample reservation against 239 limit")
		}
		if err := buffer.add(bufferLabels("b"), chunks); err == nil {
			t.Fatal("expected later Add to retain request limit failure")
		}
		_ = buffer.Flush()
		testutil.Equals(t, 0, len(upstream.responses))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
		testutil.Equals(t, 1.0, selectDropCount(t, reg, "chunks"))
	})

	t.Run("reset drops pending records and handed off results remain owned", func(t *testing.T) {
		upstream := &seriesBufferTestServer{}
		reg := prometheus.NewRegistry()
		buffer := newLimitedTestSeriesBuffer(t, upstream, SeriesSelectLimits{SeriesPerRequest: 1, SamplesPerRequest: 120}, reg)
		testutil.Ok(t, buffer.add(bufferLabels("discarded"), nil))
		testutil.Ok(t, buffer.reset())
		testutil.Ok(t, buffer.add(bufferLabels("kept"), []prompb.Chunk{{MinTimeMs: 5, MaxTimeMs: 5, Data: []byte{8, 9}}}))
		testutil.Ok(t, buffer.Flush())
		if len(upstream.responses) != 1 {
			t.Fatalf("got %d responses after reset, want 1", len(upstream.responses))
		}
		retained := upstream.responses[0].GetSeries()
		labelsBefore := append([]labelpb.ZLabel(nil), retained.Labels...)
		dataBefore := append([]byte(nil), retained.Chunks[0].Raw.Data...)
		testutil.Ok(t, buffer.reset())
		testutil.Ok(t, buffer.Close())
		testutil.Equals(t, labelsBefore, retained.Labels)
		testutil.Equals(t, dataBefore, retained.Chunks[0].Raw.Data)
	})

	t.Run("metadata mode omits chunks", func(t *testing.T) {
		upstream := &seriesBufferTestServer{}
		buffer := newTestSeriesBuffer(upstream, 0, true)
		testutil.Ok(t, buffer.add(bufferLabels("metadata"), nil))
		testutil.Ok(t, buffer.Flush())
		got := flattenBufferResponses(t, upstream.responses)
		if len(got) != 1 {
			t.Fatalf("got %d series, want 1", len(got))
		}
		testutil.Equals(t, 0, len(got[0].Chunks))
	})

	t.Run("downstream send and flush errors propagate", func(t *testing.T) {
		sendErr := errors.New("send failed")
		sendServer := &seriesBufferTestServer{sendErr: sendErr}
		sendBuffer := newTestSeriesBuffer(sendServer, 0, false)
		testutil.Ok(t, sendBuffer.add(bufferLabels("a"), nil))
		testutil.Assert(t, errors.Is(sendBuffer.Flush(), sendErr), "expected send error")

		flushErr := errors.New("flush failed")
		flushServer := &seriesBufferTestServer{flushErr: flushErr}
		flushBuffer := newTestSeriesBuffer(flushServer, 0, false)
		testutil.Ok(t, flushBuffer.add(bufferLabels("a"), nil))
		testutil.Assert(t, errors.Is(flushBuffer.Flush(), flushErr), "expected downstream flush error")
	})

	t.Run("concurrent requests keep independent state", func(t *testing.T) {
		const requests = 4
		servers := make([]*seriesBufferTestServer, requests)
		buffers := make([]*prometheusSeriesBuffer, requests)
		for i := range buffers {
			servers[i] = &seriesBufferTestServer{}
			buffers[i] = newTestSeriesBuffer(servers[i], 0, false)
		}
		var wg sync.WaitGroup
		errs := make(chan error, requests)
		for i := range buffers {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				if err := buffers[index].add(bufferLabels(string(rune('a'+index))), []prompb.Chunk{{Data: []byte{byte(index + 1)}}}); err != nil {
					errs <- err
					return
				}
				if err := buffers[index].Flush(); err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		for i, server := range servers {
			series := flattenBufferResponses(t, server.responses)
			if len(series) != 1 || series[0].Labels[0].Value != string(rune('a'+i)) || series[0].Chunks[0].Raw.Data[0] != byte(i+1) {
				t.Fatalf("request %d got %#v", i, series)
			}
		}
	})
}

func newTestSeriesBuffer(srv storepb.Store_SeriesServer, batchSize int, metadata bool) *prometheusSeriesBuffer {
	store := &PrometheusStore{}
	return store.newSeriesBuffer(srv, batchSize, metadata)
}

func newLimitedTestSeriesBuffer(t *testing.T, srv storepb.Store_SeriesServer, limits SeriesSelectLimits, reg *prometheus.Registry) *prometheusSeriesBuffer {
	t.Helper()
	base, err := url.Parse("http://prometheus.test")
	testutil.Ok(t, err)
	store, err := NewPrometheusStore(nil, reg, promclient.NewClient(nil, nil, ""), base, component.Sidecar,
		func() labels.Labels { return labels.EmptyLabels() }, func() (int64, int64) { return 0, -1 }, nil,
		WithPrometheusStoreLimits(limits))
	testutil.Ok(t, err)
	return store.newSeriesBuffer(srv, 0, false)
}

func bufferLabels(value string) []labelpb.ZLabel {
	return labelpb.ZLabelsFromPromLabels(labels.FromStrings("series", value))
}

func flattenBufferResponses(t *testing.T, responses []*storepb.SeriesResponse) []*storepb.Series {
	t.Helper()
	var got []*storepb.Series
	for _, response := range responses {
		if series := response.GetSeries(); series != nil {
			got = append(got, series)
			continue
		}
		if batch := response.GetBatch(); batch != nil {
			got = append(got, batch.Series...)
			continue
		}
		t.Fatalf("unexpected response %T", response.GetResult())
	}
	return got
}

func assertBufferChunk(t *testing.T, got storepb.AggrChunk, minTime, maxTime int64, encoding storepb.Chunk_Encoding, data []byte) {
	t.Helper()
	testutil.Equals(t, minTime, got.MinTime)
	testutil.Equals(t, maxTime, got.MaxTime)
	testutil.Equals(t, encoding, got.Raw.Type)
	testutil.Equals(t, data, got.Raw.Data)
	testutil.Equals(t, xxhash.Sum64(data), got.Raw.Hash)
}

type seriesBufferTestServer struct {
	storepb.Store_SeriesServer
	responses []*storepb.SeriesResponse
	sendErr   error
	flushErr  error
}

func (s *seriesBufferTestServer) Send(response *storepb.SeriesResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.responses = append(s.responses, response)
	return nil
}

func (s *seriesBufferTestServer) Context() context.Context { return context.Background() }

func (s *seriesBufferTestServer) Flush() error { return s.flushErr }
