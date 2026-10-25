// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func BenchmarkPrometheusSeriesSort(b *testing.B) {
	cases := []struct {
		mode                     string
		series, chunks, requests int
	}{
		{"streamed", 10, 1, 1}, {"streamed", 1000, 1, 1}, {"streamed", 100000, 1, 1},
		{"metadata", 10, 1, 1}, {"metadata", 1000, 1, 1}, {"metadata", 100000, 1, 1},
		{"sampled", 10, 1, 1}, {"sampled", 1000, 1, 1},
		{"streamed-histogram", 1000, 1, 1}, {"streamed", 1, 1000, 1},
		{"streamed", 1000, 1, 4}, {"streamed", 1500, 1, 1},
	}
	for _, tc := range cases {
		for _, threshold := range []int64{0, 256 * 1024} {
			name := "ram"
			if threshold > 0 {
				name = "spill_256KiB"
			}
			b.Run(fmt.Sprintf("%s/%s/series_%d/chunks_%d/requests_%d", name, tc.mode, tc.series, tc.chunks, tc.requests), func(b *testing.B) {
				fixture, contentType := memorySeriesFixture(b, tc.mode, tc.series, tc.chunks)
				dir := b.TempDir()
				p, req := newSortMemoryStore(b, tc.mode, contentType, threshold, dir, func() io.ReadCloser { return io.NopCloser(bytes.NewReader(fixture)) })
				warm := &sortMemorySink{memorySeriesSink: memorySeriesSink{ctx: context.Background()}}
				request := *req
				if err := p.Series(&request, warm); err != nil {
					b.Fatal(err)
				}
				if warm.count != tc.series || warm.digest == 0 {
					b.Fatalf("warmup count=%d digest=%d, want %d nonzero", warm.count, warm.digest, tc.series)
				}
				want := sortMemoryResult{series: warm.count, chunks: warm.chunks, digest: warm.digest}
				b.SetBytes(int64(len(fixture) * tc.requests))
				b.ReportMetric(float64(len(fixture)), "fixture_bytes/op")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					results := runSortMemoryRequests(p, req, tc.requests, 0)
					for _, result := range results {
						if result.err != nil {
							b.Fatal(result.err)
						}
						if result.series != want.series || result.chunks != want.chunks || result.digest != want.digest {
							b.Fatalf("result=%+v, want=%+v", result, want)
						}
					}
				}
			})
		}
	}
}

func newSortMemoryStore(t testing.TB, mode, contentType string, threshold int64, dir string, body func() io.ReadCloser) (*PrometheusStore, *storepb.SeriesRequest) {
	t.Helper()
	base, err := url.Parse("http://prometheus.test")
	if err != nil {
		t.Fatal(err)
	}
	client := promclient.NewClient(memoryResponseClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}}, Body: body()}, nil
	}), nil, "")
	var options []PrometheusStoreOption
	if threshold > 0 {
		options = append(options, WithPrometheusStoreSorting(units.Base2Bytes(threshold), dir, 0))
	}
	p, err := NewPrometheusStore(nil, prometheus.NewRegistry(), client, base, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("cluster", "memory-test") }, func() (int64, int64) { return 0, -1 }, func() string { return "3.0.0" }, options...)
	if err != nil {
		t.Fatal(err)
	}
	req := &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "benchmark"}}, MinTime: 0, MaxTime: 1, SkipChunks: mode == "metadata"}
	return p, req
}

type sortMemorySink struct {
	memorySeriesSink
	start   time.Time
	first   time.Duration
	ready   chan struct{}
	release <-chan struct{}
	delay   time.Duration
}

func (s *sortMemorySink) Send(response *storepb.SeriesResponse) error {
	if s.count == 0 {
		s.first = time.Since(s.start)
		if s.ready != nil {
			close(s.ready)
			<-s.release
		}
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.memorySeriesSink.Send(response)
}

type sortMemoryResult struct {
	series, chunks int
	digest         uint64
	first          time.Duration
	err            error
}

func runSortMemoryRequests(p *PrometheusStore, req *storepb.SeriesRequest, requests int, delay time.Duration) []sortMemoryResult {
	results := make([]sortMemoryResult, requests)
	start := time.Now()
	run := func(i int) {
		request := *req
		sink := &sortMemorySink{memorySeriesSink: memorySeriesSink{ctx: context.Background()}, start: start, delay: delay}
		err := p.Series(&request, sink)
		results[i] = sortMemoryResult{series: sink.count, chunks: sink.chunks, digest: sink.digest, first: sink.first, err: err}
	}
	if requests == 1 {
		run(0)
		return results
	}
	var wg sync.WaitGroup
	for i := range requests {
		wg.Go(func() { run(i) })
	}
	wg.Wait()
	return results
}

func TestPrometheusStore_Series_SortMemoryProfile(t *testing.T) {
	if os.Getenv("THANOS_SORT_MEMORY_PROFILE") != "1" {
		t.Skip("isolated sort-memory measurement requires THANOS_SORT_MEMORY_PROFILE=1")
	}
	mode := os.Getenv("THANOS_SORT_MEMORY_MODE")
	if mode == "" {
		mode = "streamed"
	}
	switch mode {
	case "streamed", "streamed-histogram", "metadata", "sampled":
	default:
		t.Fatalf("unsupported mode %q", mode)
	}
	count := sortMemoryEnvInt(t, "THANOS_SORT_MEMORY_SERIES", 1000)
	if count != 10 && count != 1000 && count != 100000 {
		t.Fatal("THANOS_SORT_MEMORY_SERIES must be 10, 1000 or 100000")
	}
	chunks := sortMemoryEnvInt(t, "THANOS_SORT_MEMORY_CHUNKS", 1)
	requests := sortMemoryEnvInt(t, "THANOS_SORT_MEMORY_CONCURRENCY", 1)
	if requests != 1 && requests != 4 {
		t.Fatal("THANOS_SORT_MEMORY_CONCURRENCY must be 1 or 4")
	}
	threshold := sortMemoryEnvInt(t, "THANOS_SORT_MEMORY_THRESHOLD", 0)
	delay := time.Duration(sortMemoryEnvInt(t, "THANOS_SORT_MEMORY_SLOW_US", 0)) * time.Microsecond
	dir := os.Getenv("THANOS_SORT_MEMORY_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	fixture, contentType := memorySeriesFixture(t, mode, count, chunks)
	workload := fmt.Sprintf("%s-%d-chunks%d-threshold%d-requests%d-slow%dus", mode, count, chunks, threshold, requests, delay.Microseconds())
	latency := os.Getenv("THANOS_SORT_MEMORY_LATENCY") == "1"
	inputGate, outputGate := make(chan struct{}), make(chan struct{})
	var releaseInput, releaseOutput sync.Once
	t.Cleanup(func() { releaseInput.Do(func() { close(inputGate) }); releaseOutput.Do(func() { close(outputGate) }) })
	bodies := make(chan *memoryProbeBody, requests)
	measuring := false
	p, req := newSortMemoryStore(t, mode, contentType, int64(threshold), dir, func() io.ReadCloser {
		if !measuring || latency {
			return io.NopCloser(bytes.NewReader(fixture))
		}
		body := &memoryProbeBody{Reader: bytes.NewReader(fixture), eof: make(chan struct{}), continueRead: inputGate}
		bodies <- body
		return body
	})
	warm := runSortMemoryRequests(p, req, requests, 0)
	for _, result := range warm {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.series != count || result.digest == 0 {
			t.Fatalf("invalid warmup: %+v", result)
		}
	}
	want := warm[0]
	if latency {
		start := time.Now()
		results := runSortMemoryRequests(p, req, requests, delay)
		completion := time.Since(start)
		total, first := checkSortMemoryResults(t, results, want)
		t.Logf("MEMORY_LATENCY {\"workload\":%q,\"fixture_bytes\":%d,\"series\":%d,\"chunks\":%d,\"requests\":%d,\"threshold_bytes\":%d,\"time_to_first_ns\":%d,\"completion_ns\":%d,\"digest\":%d}", workload, len(fixture), total.series, total.chunks, requests, threshold, first.Nanoseconds(), completion.Nanoseconds(), total.digest)
		return
	}
	measuring = true
	ready := make([]chan struct{}, requests)
	sinks := make([]*sortMemorySink, requests)
	for i := range requests {
		ready[i] = make(chan struct{})
		sinks[i] = &sortMemorySink{memorySeriesSink: memorySeriesSink{ctx: context.Background()}, ready: ready[i], release: outputGate, delay: delay}
	}
	runtime.GC()
	logMemoryProfile(t, workload, "baseline", len(fixture), 0, 0)
	if mode == "sampled" {
		t.Log("EOF checkpoint precedes eager sampled-response decoding; blocked first Send is the post-sort checkpoint")
	}
	start := time.Now()
	done := make(chan sortMemoryResult, requests)
	for _, sink := range sinks {
		sink.start = start
		go func() {
			request := *req
			err := p.Series(&request, sink)
			done <- sortMemoryResult{series: sink.count, chunks: sink.chunks, digest: sink.digest, first: sink.first, err: err}
		}()
	}
	for range requests {
		select {
		case body := <-bodies:
			waitSortMemoryCheckpoint(t, body.eof, done, "input EOF")
		case result := <-done:
			t.Fatalf("handler ended before input EOF: %+v", result)
		case <-time.After(60 * time.Second):
			t.Fatal("timed out waiting for response body")
		}
	}
	runtime.GC()
	phase := "eof_preflush"
	if mode == "sampled" {
		phase = "input_eof_before_decode"
	}
	logMemoryProfile(t, workload, phase, len(fixture), time.Since(start), 0)
	releaseInput.Do(func() { close(inputGate) })
	for _, signal := range ready {
		waitSortMemoryCheckpoint(t, signal, done, "first Send")
	}
	var first time.Duration
	for _, sink := range sinks {
		if first == 0 || sink.first < first {
			first = sink.first
		}
	}
	runtime.GC()
	logMemoryProfile(t, workload, "first_send_blocked", len(fixture), time.Since(start), first)
	releaseOutput.Do(func() { close(outputGate) })
	results := make([]sortMemoryResult, 0, requests)
	for range requests {
		select {
		case result := <-done:
			results = append(results, result)
		case <-time.After(60 * time.Second):
			t.Fatal("timed out completing memory trial")
		}
	}
	completion := time.Since(start)
	total, _ := checkSortMemoryResults(t, results, want)
	runtime.GC()
	logMemoryProfile(t, workload, "completion", len(fixture), completion, first)
	t.Logf("MEMORY_RESULT {\"workload\":%q,\"fixture_bytes\":%d,\"series\":%d,\"chunks\":%d,\"requests\":%d,\"threshold_bytes\":%d,\"digest\":%d}", workload, len(fixture), total.series, total.chunks, requests, threshold, total.digest)
	runtime.KeepAlive(fixture)
}

func sortMemoryEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		t.Fatalf("%s must be a nonnegative integer, got %q", name, value)
	}
	if (name == "THANOS_SORT_MEMORY_CHUNKS" || name == "THANOS_SORT_MEMORY_CONCURRENCY") && n == 0 {
		t.Fatalf("%s must be positive", name)
	}
	return n
}

func checkSortMemoryResults(t *testing.T, results []sortMemoryResult, want sortMemoryResult) (sortMemoryResult, time.Duration) {
	t.Helper()
	var total sortMemoryResult
	var first time.Duration
	for _, result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.series != want.series || result.chunks != want.chunks || result.digest != want.digest {
			t.Fatalf("result=%+v want=%+v", result, want)
		}
		total.series += result.series
		total.chunks += result.chunks
		total.digest += result.digest
		if first == 0 || result.first < first {
			first = result.first
		}
	}
	return total, first
}

func waitSortMemoryCheckpoint(t *testing.T, signal <-chan struct{}, done <-chan sortMemoryResult, name string) {
	t.Helper()
	select {
	case <-signal:
	case result := <-done:
		t.Fatalf("handler ended before %s: %+v", name, result)
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
