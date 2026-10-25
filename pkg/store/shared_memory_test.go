// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func BenchmarkSharedStoreLabels(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		for _, transform := range []bool{false, true} {
			name := "empty_external"
			var external labels.Labels
			var remove map[string]struct{}
			if transform {
				name = "external_and_removal"
				external = labels.FromStrings("cluster", "external", "region", "eu", "replica", "external")
				remove = map[string]struct{}{"replica": {}}
			}
			finalExternal := rmLabels(external.Copy(), remove)
			inputs := make([]labels.Labels, count)
			for i := range inputs {
				inputs[i] = sharedMemoryLabels(i)
			}
			for _, pipeline := range []string{"tsdb_legacy", "gateway_legacy", "reused"} {
				b.Run(fmt.Sprintf("%s/%s/series_%d", pipeline, name, count), func(b *testing.B) {
					var builder labels.Builder
					want := sharedMemoryLabelDigest(inputs, finalExternal, remove, "tsdb_legacy", &builder)
					testutil.Equals(b, want, sharedMemoryLabelDigest(inputs, finalExternal, remove, pipeline, &builder))
					b.ReportAllocs()
					b.ResetTimer()
					var digest uint64
					for i := 0; i < b.N; i++ {
						var requestBuilder labels.Builder
						digest = sharedMemoryLabelDigest(inputs, finalExternal, remove, pipeline, &requestBuilder)
					}
					b.StopTimer()
					b.ReportMetric(float64(count), "series/op")
					testutil.Equals(b, want, digest)
				})
			}
		}
	}
}

func sharedMemoryLabelDigest(inputs []labels.Labels, external labels.Labels, remove map[string]struct{}, pipeline string, builder *labels.Builder) uint64 {
	var digest uint64
	for _, input := range inputs {
		var result labels.Labels
		switch pipeline {
		case "tsdb_legacy":
			result = labelpb.ExtendSortedLabels(rmLabels(input, remove), external)
		case "gateway_legacy":
			result = labelpb.ExtendSortedLabels(input, external)
			if remove != nil {
				result = rmLabels(result, remove)
			}
		case "reused":
			result = transformPrometheusLabels(builder, input, external, remove)
		}
		result.Range(func(l labels.Label) {
			digest += xxhash.Sum64String(l.Name) ^ xxhash.Sum64String(l.Value)
		})
	}
	return digest
}

func BenchmarkSharedStoreTSDB(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		for _, skipChunks := range []bool{false, true} {
			b.Run(fmt.Sprintf("series_%d/skip_chunks_%t", count, skipChunks), func(b *testing.B) {
				chunk := chunkenc.NewXORChunk()
				appender, err := chunk.Appender()
				testutil.Ok(b, err)
				appender.Append(1, 1)
				meta := chunks.Meta{MinTime: 1, MaxTime: 1, Chunk: chunk}
				iterator := func(chunks.Iterator) chunks.Iterator { return storage.NewListChunkSeriesIterator(meta) }
				db := &sharedMemoryDB{series: make([]storage.ChunkSeries, count)}
				for i := range db.series {
					db.series[i] = &storage.ChunkSeriesEntry{Lset: sharedMemoryLabels(i), ChunkIteratorFn: iterator}
				}
				store := NewTSDBStore(nil, db, component.Receive, labels.FromStrings("cluster", "external", "region", "eu", "replica", "external"))
				req := &storepb.SeriesRequest{
					MinTime: 0, MaxTime: 2, SkipChunks: skipChunks, ResponseBatchSize: 64,
					Matchers:             []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "benchmark"}},
					WithoutReplicaLabels: []string{"replica"},
				}
				warm := &sharedMemorySink{ctx: context.Background(), requireExternal: true}
				request := *req
				testutil.Ok(b, store.Series(&request, warm))
				testutil.Equals(b, count, warm.count)
				wantDigest, wantChunks := warm.digest, warm.chunks
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					request := *req
					sink := &sharedMemorySink{ctx: context.Background(), requireExternal: true}
					testutil.Ok(b, store.Series(&request, sink))
					testutil.Equals(b, count, sink.count)
					testutil.Equals(b, wantChunks, sink.chunks)
					testutil.Equals(b, wantDigest, sink.digest)
				}
				b.StopTimer()
				b.ReportMetric(float64(count), "series/op")
			})
		}
	}
}

type sharedMemoryDB struct {
	series []storage.ChunkSeries
}

func (*sharedMemoryDB) StartTime() (int64, error) { return 0, nil }
func (d *sharedMemoryDB) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return &sharedMemoryQuerier{series: d.series}, nil
}

type sharedMemoryQuerier struct {
	storage.ChunkQuerier
	series []storage.ChunkSeries
}

func (*sharedMemoryQuerier) Close() error { return nil }
func (q *sharedMemoryQuerier) Select(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.ChunkSeriesSet {
	return &sharedMemorySet{series: q.series, index: -1}
}

type sharedMemorySet struct {
	series []storage.ChunkSeries
	index  int
}

func (s *sharedMemorySet) Next() bool                      { s.index++; return s.index < len(s.series) }
func (s *sharedMemorySet) At() storage.ChunkSeries         { return s.series[s.index] }
func (*sharedMemorySet) Err() error                        { return nil }
func (*sharedMemorySet) Warnings() annotations.Annotations { return nil }

func sharedMemoryLabels(i int) labels.Labels {
	return labels.FromStrings("cluster", "local", "job", "benchmark", "replica", "local", "series", fmt.Sprintf("%09d", i))
}

type sharedMemorySink struct {
	storepb.Store_SeriesServer
	ctx             context.Context
	requireExternal bool
	previous, key   []byte
	count, chunks   int
	digest          uint64
}

func (s *sharedMemorySink) Context() context.Context { return s.ctx }

func (s *sharedMemorySink) Send(response *storepb.SeriesResponse) error {
	if series := response.GetSeries(); series != nil {
		return s.consume(series)
	}
	if batch := response.GetBatch(); batch != nil {
		for _, series := range batch.Series {
			if err := s.consume(series); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unexpected response %T", response.GetResult())
}

func (s *sharedMemorySink) consume(series *storepb.Series) error {
	lset := labelpb.ZLabelsToPromLabels(series.Labels)
	key := lset.Bytes(s.key[:0])
	if s.count > 0 && bytes.Compare(key, s.previous) < 0 {
		return fmt.Errorf("labels out of order: %q before %q", s.previous, key)
	}
	if s.requireExternal && (lset.Get("cluster") != "external" || lset.Has("replica")) {
		return fmt.Errorf("unexpected transformed labels %s", lset.String())
	}
	s.previous, s.key = key, s.previous
	s.count++
	s.chunks += len(series.Chunks)
	var digest xxhash.Digest
	digest.Reset()
	for _, label := range series.Labels {
		_, _ = digest.WriteString(label.Name)
		_, _ = digest.WriteString(label.Value)
	}
	for _, chunk := range series.Chunks {
		for _, field := range []*storepb.Chunk{chunk.Raw, chunk.Count, chunk.Sum, chunk.Min, chunk.Max, chunk.Counter} {
			if field != nil {
				_, _ = digest.Write(field.Data)
			}
		}
	}
	s.digest += digest.Sum64()
	return nil
}

func TestSharedStoreMemoryProfile(t *testing.T) {
	if os.Getenv("SHARED_STORE_MEMORY_PROFILE") != "1" {
		t.Skip("isolated memory measurement requires SHARED_STORE_MEMORY_PROFILE=1")
	}
	count := sharedMemoryEnvInt(t, "SHARED_STORE_MEMORY_SERIES", 10000)
	if count != 10 && count != 1000 && count != 10000 {
		t.Fatal("SHARED_STORE_MEMORY_SERIES must be 10, 1000 or 10000")
	}
	payloadBytes := sharedMemoryEnvInt(t, "SHARED_STORE_MEMORY_PAYLOAD_BYTES", 4096)
	if payloadBytes < 2 {
		t.Fatal("SHARED_STORE_MEMORY_PAYLOAD_BYTES must be at least 2")
	}
	client := &sharedMemoryClient{count: count, payloadBytes: payloadBytes}
	counter := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "shared_memory_empty_responses_total", Help: "Empty streams in the memory probe."})
	sink := &sharedMemorySink{ctx: context.Background()}
	runtime.GC()
	sharedMemoryLog(t, "baseline", count, payloadBytes, sink)
	set := newEagerRespSet(0, "memory-probe", nil, func() {}, client, nil, false, counter, nil, nil)
	defer set.Close()
	set.(*eagerRespSet).wg.Wait()
	runtime.GC()
	sharedMemoryLog(t, "full_buffer", count, payloadBytes, sink)
	sharedMemoryConsume(t, set, sink, count/2)
	runtime.GC()
	sharedMemoryLog(t, "half_drained", count, payloadBytes, sink)
	sharedMemoryConsume(t, set, sink, count-count/2)
	testutil.Assert(t, !set.Next(), "unexpected extra response")
	runtime.GC()
	sharedMemoryLog(t, "terminal", count, payloadBytes, sink)
	testutil.Equals(t, count, sink.count)
	testutil.Equals(t, count, sink.chunks)
	testutil.Assert(t, sink.digest != 0, "empty payload digest")
	runtime.KeepAlive(set)
}

func sharedMemoryConsume(t *testing.T, set respSet, sink *sharedMemorySink, count int) {
	t.Helper()
	for range count {
		testutil.Assert(t, set.Next(), "iterator ended early")
		testutil.Ok(t, sink.Send(set.At()))
	}
}

type sharedMemoryClient struct {
	storepb.Store_SeriesClient
	count, payloadBytes, next int
}

func (c *sharedMemoryClient) Recv() (*storepb.SeriesResponse, error) {
	if c.next == c.count {
		return nil, io.EOF
	}
	data := make([]byte, c.payloadBytes)
	for i := range data {
		data[i] = byte(c.next)
	}
	data[0], data[1] = 0, 1
	response := storepb.NewSeriesResponse(&storepb.Series{
		Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("job", "benchmark", "series", fmt.Sprintf("%09d", c.next))),
		Chunks: []storepb.AggrChunk{{MinTime: 1, MaxTime: 1, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: data}}},
	})
	c.next++
	return response, nil
}

func (c *sharedMemoryClient) CloseSend() error { return nil }

func sharedMemoryEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		t.Fatalf("%s must be a nonnegative integer, got %q", name, value)
	}
	return n
}

func sharedMemoryLog(t *testing.T, phase string, count, payloadBytes int, sink *sharedMemorySink) {
	t.Helper()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	var rss uint64
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 1 {
			pages, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				rss = pages * uint64(os.Getpagesize())
			}
		}
	}
	t.Logf("SHARED_STORE_MEMORY {\"phase\":%q,\"series\":%d,\"payload_bytes\":%d,\"heap_alloc\":%d,\"heap_inuse\":%d,\"rss_bytes\":%d,\"consumed\":%d,\"digest\":%d,\"forced_gc\":true}", phase, count, payloadBytes, stats.HeapAlloc, stats.HeapInuse, rss, sink.count, sink.digest)
}
