// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type memoryResponseClient func(*http.Request) (*http.Response, error)

func (f memoryResponseClient) Do(r *http.Request) (*http.Response, error) { return f(r) }

type memorySeriesSink struct {
	storepb.Store_SeriesServer
	ctx      context.Context
	count    int
	chunks   int
	digest   uint64
	previous string
}

type memoryProbeBody struct {
	*bytes.Reader
	eof, continueRead chan struct{}
	signaled          bool
}

func (b *memoryProbeBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF && !b.signaled {
		b.signaled = true
		close(b.eof)
		<-b.continueRead
	}
	return n, err
}
func (b *memoryProbeBody) Close() error { return nil }

type blockedMemorySink struct {
	memorySeriesSink
	first, release chan struct{}
}

func (s *blockedMemorySink) Send(r *storepb.SeriesResponse) error {
	if s.count == 0 {
		close(s.first)
		<-s.release
	}
	return s.memorySeriesSink.Send(r)
}

func (s *memorySeriesSink) Send(r *storepb.SeriesResponse) error {
	series := r.GetSeries()
	if series != nil {
		return s.consume(series)
	}
	if batch := r.GetBatch(); batch != nil {
		for _, series := range batch.Series {
			if err := s.consume(series); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unexpected response type %T", r.GetResult())
}
func (s *memorySeriesSink) consume(series *storepb.Series) error {
	key := string(labelpb.ZLabelsToPromLabels(series.Labels).Bytes(nil))
	if s.count > 0 && key < s.previous {
		return fmt.Errorf("labels are not sorted: %q before %q", s.previous, key)
	}
	s.previous = key
	s.count++
	s.chunks += len(series.Chunks)
	h := fnv.New64a()
	for _, l := range series.Labels {
		_, _ = h.Write([]byte(l.Name))
		_, _ = h.Write([]byte(l.Value))
	}
	for _, c := range series.Chunks {
		if c.Raw != nil {
			_, _ = h.Write(c.Raw.Data)
		}
	}
	s.digest += h.Sum64()
	return nil
}
func (s *memorySeriesSink) Context() context.Context { return s.ctx }

func BenchmarkPrometheusStoreSeriesMemory(b *testing.B) {
	for _, mode := range []string{"streamed", "sampled", "metadata"} {
		counts := []int{10, 1000, 100000}
		if mode == "sampled" {
			counts = []int{10, 1000}
		}
		for _, count := range counts {
			b.Run(fmt.Sprintf("%s/series_%d", mode, count), func(b *testing.B) {
				body, contentType := memorySeriesFixture(b, mode, count, 1)
				benchmarkPrometheusStoreSeries(b, mode, count, body, contentType)
			})
		}
	}
	b.Run("streamed-histogram/series_1000", func(b *testing.B) {
		body, contentType := memorySeriesFixture(b, "streamed-histogram", 1000, 1)
		benchmarkPrometheusStoreSeries(b, "streamed", 1000, body, contentType)
	})
	b.Run("streamed/concurrent_4_series_100", func(b *testing.B) {
		body, contentType := memorySeriesFixture(b, "streamed", 100, 1)
		benchmarkPrometheusStoreSeries(b, "concurrent4", 100, body, contentType)
	})
	b.Run("streamed/series_1_chunks_1000", func(b *testing.B) {
		body, contentType := memorySeriesFixture(b, "streamed", 1, 1000)
		benchmarkPrometheusStoreSeries(b, "streamed", 1, body, contentType)
	})
}

func benchmarkPrometheusStoreSeries(b *testing.B, mode string, want int, fixture []byte, contentType string) {
	b.Helper()
	base, _ := url.Parse("http://prometheus.test")
	client := promclient.NewClient(memoryResponseClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(bytes.NewReader(fixture))}, nil
	}), nil, "")
	p, err := NewPrometheusStore(nil, prometheus.NewRegistry(), client, base, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("cluster", "memory-test") }, func() (int64, int64) { return 0, -1 }, func() string { return "3.0.0" })
	if err != nil {
		b.Fatal(err)
	}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "job", "benchmark")
	req := &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: matcher.Name, Value: matcher.Value}}, MinTime: 0, MaxTime: 1}
	if mode == "metadata" {
		req.SkipChunks = true
	}
	b.SetBytes(int64(len(fixture)))
	if mode == "concurrent4" {
		b.SetBytes(4 * int64(len(fixture)))
	}
	b.ReportMetric(float64(len(fixture)), "fixture_bytes/op")
	b.ReportAllocs()
	warm := &memorySeriesSink{ctx: context.Background()}
	if err := p.Series(req, warm); err != nil {
		b.Fatal(err)
	}
	if warm.count != want || warm.digest == 0 {
		b.Fatalf("warmup got count=%d digest=%d, want %d and nonzero digest", warm.count, warm.digest, want)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if mode == "concurrent4" {
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					request := *req
					sink := &memorySeriesSink{ctx: context.Background()}
					if err := p.Series(&request, sink); err != nil {
						errs <- err
						return
					}
					if sink.count != want || sink.digest != warm.digest || sink.chunks != warm.chunks {
						errs <- fmt.Errorf("concurrent result count=%d digest=%d chunks=%d; want %d %d %d", sink.count, sink.digest, sink.chunks, want, warm.digest, warm.chunks)
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				b.Fatal(err)
			}
			continue
		}
		sink := &memorySeriesSink{ctx: context.Background()}
		if err := p.Series(req, sink); err != nil {
			b.Fatal(err)
		}
		if sink.count != want || sink.digest != warm.digest || sink.chunks != warm.chunks {
			b.Fatalf("got count=%d chunks=%d digest=%d; want count=%d chunks=%d digest=%d", sink.count, sink.chunks, sink.digest, want, warm.chunks, warm.digest)
		}
		if sink.digest == 0 {
			b.Fatal("sink consumed no response data")
		}
	}
}

func memorySeriesFixture(b testing.TB, mode string, seriesCount, chunksPerSeries int) ([]byte, string) {
	b.Helper()
	switch mode {
	case "metadata":
		var out strings.Builder
		out.WriteString(`{"status":"success","data":[`)
		for i := 0; i < seriesCount; i++ {
			if i > 0 {
				out.WriteByte(',')
			}
			fmt.Fprintf(&out, `{"__name__":"metric","job":"benchmark","series":"%09d"}`, i)
		}
		out.WriteString(`]}`)
		return []byte(out.String()), "application/json"
	case "sampled":
		resp := &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: make([]*prompb.TimeSeries, seriesCount)}}}
		for i := range resp.Results[0].Timeseries {
			resp.Results[0].Timeseries[i] = &prompb.TimeSeries{Labels: []labelpb.ZLabel{{Name: "job", Value: "benchmark"}, {Name: "series", Value: fmt.Sprintf("%09d", i)}}, Samples: []prompb.Sample{{Timestamp: 1, Value: float64(i)}}}
		}
		encoded, err := proto.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
		return snappy.Encode(nil, encoded), "application/x-protobuf; proto=prometheus.ReadResponse"
	default:
		var out bytes.Buffer
		writer := remote.NewChunkedWriter(&out, testHTTPFlusher{})
		series := make([]*prompb.ChunkedSeries, 0, 1)
		var chunkBytes []byte
		chunkType := prompb.Chunk_XOR
		if mode == "streamed-histogram" {
			h := &histogram.Histogram{Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}
			chunk := chunkenc.NewHistogramChunk()
			app, err := chunk.Appender()
			if err != nil {
				b.Fatal(err)
			}
			_, _, _, err = app.AppendHistogram(nil, 1, h, false)
			if err != nil {
				b.Fatal(err)
			}
			chunkBytes, chunkType = chunk.Bytes(), prompb.Chunk_HISTOGRAM
		} else {
			chunk := chunkenc.NewXORChunk()
			app, err := chunk.Appender()
			if err != nil {
				b.Fatal(err)
			}
			app.Append(1, 1)
			chunkBytes = chunk.Bytes()
		}
		for i := 0; i < seriesCount; i++ {
			chunks := make([]prompb.Chunk, chunksPerSeries)
			for j := range chunks {
				chunks[j] = prompb.Chunk{MinTimeMs: int64(j), MaxTimeMs: int64(j + 1), Type: chunkType, Data: chunkBytes}
			}
			series = append(series, &prompb.ChunkedSeries{Labels: []labelpb.ZLabel{{Name: "job", Value: "benchmark"}, {Name: "series", Value: fmt.Sprintf("%09d", i)}}, Chunks: chunks})
			if i+1 < seriesCount {
				frame, err := proto.Marshal(&prompb.ChunkedReadResponse{ChunkedSeries: series})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := writer.Write(frame); err != nil {
					b.Fatal(err)
				}
				series = series[:0]
			}
		}
		frame, err := proto.Marshal(&prompb.ChunkedReadResponse{ChunkedSeries: series})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := writer.Write(frame); err != nil {
			b.Fatal(err)
		}
		return out.Bytes(), streamedProtoContentType
	}
}

func TestPrometheusStore_Series_MemoryProfile(t *testing.T) {
	if testing.Short() || os.Getenv("THANOS_MEMORY_PROFILE") != "1" {
		t.Skip("isolated memory measurement requires THANOS_MEMORY_PROFILE=1")
	}
	latencyMode := os.Getenv("THANOS_MEMORY_LATENCY") == "1"
	seriesCount := 1000
	if latencyMode {
		seriesCount = 10
	}
	if configured := os.Getenv("THANOS_MEMORY_SERIES"); configured != "" {
		value, err := strconv.Atoi(configured)
		if err != nil || (value != 10 && value != 1000 && value != 100000) {
			t.Fatalf("THANOS_MEMORY_SERIES must be 10, 1000, or 100000, got %q", configured)
		}
		seriesCount = value
	}
	fixture, contentType := memorySeriesFixture(t, "streamed", seriesCount, 1)
	if latencyMode {
		runMemoryLatencyTrial(t, fixture, contentType, seriesCount)
		return
	}
	body := &memoryProbeBody{Reader: bytes.NewReader(fixture), eof: make(chan struct{}), continueRead: make(chan struct{})}
	base, _ := url.Parse("http://prometheus.test")
	client := promclient.NewClient(memoryResponseClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}}, Body: body}, nil
	}), nil, "")
	p, err := NewPrometheusStore(nil, prometheus.NewRegistry(), client, base, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("cluster", "memory-test") }, func() (int64, int64) { return 0, -1 }, func() string { return "3.0.0" })
	if err != nil {
		t.Fatal(err)
	}
	req := &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "benchmark"}}, MinTime: 0, MaxTime: 1}
	start := time.Now()
	sink := &blockedMemorySink{memorySeriesSink: memorySeriesSink{ctx: context.Background()}, first: make(chan struct{}), release: make(chan struct{})}
	runtime.GC()
	workload := fmt.Sprintf("streamed-%d", seriesCount)
	logMemoryProfile(t, workload, "baseline", len(fixture), 0, 0)
	done := make(chan error, 1)
	go func() { done <- p.Series(req, sink) }()
	select {
	case <-body.eof:
	case err := <-done:
		t.Fatalf("Series ended before EOF checkpoint: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for EOF")
	}
	runtime.GC()
	logMemoryProfile(t, workload, "eof_preflush", len(fixture), time.Since(start), 0)
	close(body.continueRead)
	select {
	case <-sink.first:
	case err := <-done:
		t.Fatalf("Series ended before first response: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for first response")
	}
	first := time.Since(start)
	runtime.GC()
	logMemoryProfile(t, workload, "first_send_blocked", len(fixture), first, first)
	close(sink.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	completion := time.Since(start)
	runtime.GC()
	logMemoryProfile(t, workload, "completion", len(fixture), completion, first)
	if sink.count != seriesCount {
		t.Fatalf("got %d responses, want %d", sink.count, seriesCount)
	}
}

type memoryLatencySink struct {
	memorySeriesSink
	start time.Time
	first time.Duration
}

func (s *memoryLatencySink) Send(r *storepb.SeriesResponse) error {
	if s.first == 0 {
		s.first = time.Since(s.start)
	}
	return s.memorySeriesSink.Send(r)
}

func runMemoryLatencyTrial(t *testing.T, fixture []byte, contentType string, want int) {
	t.Helper()
	base, _ := url.Parse("http://prometheus.test")
	client := promclient.NewClient(memoryResponseClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(bytes.NewReader(fixture))}, nil
	}), nil, "")
	p, err := NewPrometheusStore(nil, prometheus.NewRegistry(), client, base, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("cluster", "memory-test") }, func() (int64, int64) { return 0, -1 }, func() string { return "3.0.0" })
	if err != nil {
		t.Fatal(err)
	}
	req := &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "benchmark"}}, MinTime: 0, MaxTime: 1}
	warm := &memorySeriesSink{ctx: context.Background()}
	if err := p.Series(req, warm); err != nil {
		t.Fatal(err)
	}
	if warm.count != want || warm.digest == 0 {
		t.Fatalf("warmup got count=%d digest=%d, want %d and nonzero digest", warm.count, warm.digest, want)
	}
	sink := &memoryLatencySink{memorySeriesSink: memorySeriesSink{ctx: context.Background()}}
	sink.start = time.Now()
	if err := p.Series(req, sink); err != nil {
		t.Fatal(err)
	}
	completion := time.Since(sink.start)
	if sink.count != want || sink.digest != warm.digest || sink.chunks != warm.chunks {
		t.Fatalf("got count=%d chunks=%d digest=%d; want count=%d chunks=%d digest=%d", sink.count, sink.chunks, sink.digest, want, warm.chunks, warm.digest)
	}
	t.Logf("MEMORY_LATENCY {\"workload\":\"streamed-%d\",\"fixture_bytes\":%d,\"series\":%d,\"time_to_first_ns\":%d,\"completion_ns\":%d,\"digest\":%d}", want, len(fixture), sink.count, sink.first.Nanoseconds(), completion.Nanoseconds(), sink.digest)
}

func logMemoryProfile(t *testing.T, workload, phase string, fixtureBytes int, elapsed, first time.Duration) {
	t.Helper()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var rss uint64
	if b, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(b))
		if len(fields) > 1 {
			var pages uint64
			_, _ = fmt.Sscan(fields[1], &pages)
			rss = pages * uint64(os.Getpagesize())
		}
	}
	t.Logf("MEMORY_PROFILE {\"workload\":%q,\"phase\":%q,\"fixture_bytes\":%d,\"heap_alloc\":%d,\"rss_bytes\":%d,\"elapsed_ns\":%d,\"time_to_first_ns\":%d,\"forced_gc\":true}", workload, phase, fixtureBytes, m.HeapAlloc, rss, elapsed.Nanoseconds(), first.Nanoseconds())
}
