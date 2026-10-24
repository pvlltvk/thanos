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
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusStore_Series_MemoryCompact(t *testing.T) {
	t.Run("streamed complete payloads and batch equivalence", func(t *testing.T) {
		floatA := compactFloatChunk(t, 10, 1.25)
		fragmentOne := compactFloatChunk(t, 100, 2.5)
		fragmentTwo := compactFloatChunk(t, 200, 3.5)
		histogramData := compactHistogramChunk(t, 300)
		frames := [][]byte{
			compactReadFrame(t, compactChunkedSeries("a", prompb.Chunk{MinTimeMs: 10, MaxTimeMs: 10, Type: prompb.Chunk_XOR, Data: floatA})),
			compactReadFrame(t, compactChunkedSeries("fragment", prompb.Chunk{MinTimeMs: 100, MaxTimeMs: 100, Type: prompb.Chunk_XOR, Data: fragmentOne})),
			compactReadFrame(t, compactChunkedSeries("fragment", prompb.Chunk{MinTimeMs: 200, MaxTimeMs: 200, Type: prompb.Chunk_XOR, Data: fragmentTwo})),
			compactReadFrame(t, compactChunkedSeries("histogram", prompb.Chunk{MinTimeMs: 300, MaxTimeMs: 300, Type: prompb.Chunk_HISTOGRAM, Data: histogramData})),
		}
		want := []compactExpectedSeries{
			{series: "a", min: 10, max: 10, encoding: storepb.Chunk_XOR, data: floatA},
			{series: "fragment", min: 100, max: 100, encoding: storepb.Chunk_XOR, data: fragmentOne},
			{series: "fragment", min: 200, max: 200, encoding: storepb.Chunk_XOR, data: fragmentTwo},
			{series: "histogram", min: 300, max: 300, encoding: storepb.Chunk_HISTOGRAM, data: histogramData},
		}
		for _, batchSize := range []int64{0, 1, 64} {
			store := compactStore(t, labels.FromStrings("cluster", "sidecar", "replica", "external-replica"), func() *http.Response {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{streamedProtoContentType}}, Body: io.NopCloser(bytes.NewReader(bytes.Join(frames, nil)))}
			})
			srv := newStoreSeriesServer(context.Background())
			req := compactRequest(false)
			req.ResponseBatchSize = batchSize
			req.WithoutReplicaLabels = []string{"replica"}
			testutil.Ok(t, store.Series(req, srv))
			assertCompactOutput(t, srv.SeriesSet, want)
		}
	})

	t.Run("streamed shard exclusion", func(t *testing.T) {
		var pool sync.Pool
		pool.New = func() any { b := make([]byte, 0, 128); return &b }
		shardInfo := &storepb.ShardInfo{ShardIndex: 0, TotalShards: 2, By: true, Labels: []string{"series"}}
		matcher := shardInfo.Matcher(&pool)
		defer matcher.Close()
		var included, excluded string
		for i := 0; i < 100 && (included == "" || excluded == ""); i++ {
			value := fmt.Sprintf("series-%d", i)
			matched := matcher.MatchesLabels(labels.FromStrings("job", "api", "series", value))
			if matched {
				included = value
			} else {
				excluded = value
			}
		}
		if included == "" || excluded == "" {
			t.Fatal("could not construct both sides of a shard")
		}
		frames := [][]byte{
			compactReadFrame(t, compactChunkedSeries(included, prompb.Chunk{MinTimeMs: 1, MaxTimeMs: 1, Type: prompb.Chunk_XOR, Data: compactFloatChunk(t, 1, 1)})),
			compactReadFrame(t, compactChunkedSeries(excluded, prompb.Chunk{MinTimeMs: 2, MaxTimeMs: 2, Type: prompb.Chunk_XOR, Data: compactFloatChunk(t, 2, 2)})),
		}
		store := compactStore(t, labels.EmptyLabels(), func() *http.Response {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{streamedProtoContentType}}, Body: io.NopCloser(bytes.NewReader(bytes.Join(frames, nil)))}
		})
		req := compactRequest(false)
		req.ShardInfo = shardInfo
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(req, srv))
		if len(srv.SeriesSet) != 1 {
			t.Fatalf("got %d series, want one shard member", len(srv.SeriesSet))
		}
		assertLabelValue(t, srv.SeriesSet[0].Labels, "series", included)
		for _, label := range srv.SeriesSet[0].Labels {
			if label.Name == "series" && label.Value == excluded {
				t.Fatalf("excluded shard series %q was returned", excluded)
			}
		}
	})

	t.Run("sampled float-only encoding and current no-shard behavior", func(t *testing.T) {
		read := &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{
			{Labels: []labelpb.ZLabel{{Name: "instance", Value: "a"}, {Name: "job", Value: "api"}}, Samples: []prompb.Sample{{Timestamp: 15, Value: 1.5}}, Histograms: []prompb.Histogram{{Timestamp: 16}}},
			{Labels: []labelpb.ZLabel{{Name: "instance", Value: "b"}, {Name: "job", Value: "api"}}, Histograms: []prompb.Histogram{{Timestamp: 17}}},
			{Labels: []labelpb.ZLabel{{Name: "instance", Value: "c"}, {Name: "job", Value: "api"}}, Samples: []prompb.Sample{{Timestamp: 25, Value: 2.5}}},
		}}}}
		encoded, err := proto.Marshal(read)
		testutil.Ok(t, err)
		body := snappy.Encode(nil, encoded)
		store := compactStore(t, labels.FromStrings("cluster", "sidecar"), func() *http.Response {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/x-protobuf; proto=prometheus.ReadResponse"}}, Body: io.NopCloser(bytes.NewReader(body))}
		})
		req := compactRequest(false)
		req.ShardInfo = &storepb.ShardInfo{ShardIndex: 0, TotalShards: 2, By: true, Labels: []string{"instance"}}
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(req, srv))
		if len(srv.SeriesSet) != 2 {
			t.Fatalf("got %d sampled series, want two floats and skip histogram-only", len(srv.SeriesSet))
		}
		assertSampledSeries(t, srv.SeriesSet[0], "a", 15, 1.5)
		assertSampledSeries(t, srv.SeriesSet[1], "c", 25, 2.5)
	})

	t.Run("metadata current label semantics", func(t *testing.T) {
		body := []byte(`{"status":"success","data":[{"job":"api","cluster":"local","replica":"local-replica"}]}`)
		store := compactStore(t, labels.FromStrings("cluster", "external", "external_only", "yes", "replica", "external-replica"), func() *http.Response {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}
		})
		req := compactRequest(true)
		req.WithoutReplicaLabels = []string{"replica"}
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(req, srv))
		if len(srv.SeriesSet) != 1 {
			t.Fatalf("got %d metadata series, want 1", len(srv.SeriesSet))
		}
		assertLabelValue(t, srv.SeriesSet[0].Labels, "cluster", "external")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "replica", "local-replica")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "external_only", "yes")
		testutil.Equals(t, 0, len(srv.SeriesSet[0].Chunks))
	})
}

type compactExpectedSeries struct {
	series   string
	min, max int64
	encoding storepb.Chunk_Encoding
	data     []byte
}

func assertCompactOutput(t *testing.T, got []storepb.Series, want []compactExpectedSeries) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d series, want %d", len(got), len(want))
	}
	counts := make(map[string]int, len(want))
	for _, expected := range want {
		counts[compactExpectedKey(expected)]++
	}
	var previous []labelpb.ZLabel
	for _, series := range got {
		if previous != nil && labels.Compare(labelpb.ZLabelsToPromLabels(previous), labelpb.ZLabelsToPromLabels(series.Labels)) > 0 {
			t.Fatalf("series labels out of order: %v then %v", previous, series.Labels)
		}
		previous = series.Labels
		assertLabelValue(t, series.Labels, "cluster", "sidecar")
		assertNoLabel(t, series.Labels, "replica")
		if len(series.Chunks) != 1 {
			t.Fatalf("series %v has %d chunks, want one", series.Labels, len(series.Chunks))
		}
		name := ""
		for _, label := range series.Labels {
			if label.Name == "series" {
				name = label.Value
			}
		}
		chunk := series.Chunks[0]
		gotExpected := compactExpectedSeries{series: name, min: chunk.MinTime, max: chunk.MaxTime, encoding: chunk.Raw.Type, data: chunk.Raw.Data}
		key := compactExpectedKey(gotExpected)
		if counts[key] == 0 {
			t.Fatalf("unexpected payload for series %q: min=%d max=%d encoding=%v bytes=%x", name, chunk.MinTime, chunk.MaxTime, chunk.Raw.Type, chunk.Raw.Data)
		}
		counts[key]--
		testutil.Equals(t, xxhash.Sum64(chunk.Raw.Data), chunk.Raw.Hash)
	}
	for key, count := range counts {
		if count != 0 {
			t.Fatalf("missing expected payload %q x %d", key, count)
		}
	}
}

func compactExpectedKey(series compactExpectedSeries) string {
	return fmt.Sprintf("%s\x00%d\x00%d\x00%d\x00%x", series.series, series.min, series.max, series.encoding, series.data)
}

func compactFloatChunk(t *testing.T, timestamp int64, value float64) []byte {
	t.Helper()
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	testutil.Ok(t, err)
	appender.Append(timestamp, value)
	return append([]byte(nil), chunk.Bytes()...)
}

func compactHistogramChunk(t *testing.T, timestamp int64) []byte {
	t.Helper()
	h := &histogram.Histogram{Schema: 0, Count: 1, Sum: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{1}}
	testutil.Ok(t, h.Validate())
	chunk := chunkenc.NewHistogramChunk()
	appender, err := chunk.Appender()
	testutil.Ok(t, err)
	_, _, _, err = appender.AppendHistogram(nil, timestamp, h, false)
	testutil.Ok(t, err)
	return append([]byte(nil), chunk.Bytes()...)
}

func compactChunkedSeries(series string, chunks ...prompb.Chunk) *prompb.ChunkedSeries {
	return &prompb.ChunkedSeries{Labels: []labelpb.ZLabel{{Name: "job", Value: "api"}, {Name: "series", Value: series}}, Chunks: chunks}
}

func compactReadFrame(t *testing.T, series *prompb.ChunkedSeries) []byte {
	t.Helper()
	return chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{series}})
}

func compactStore(t *testing.T, ext labels.Labels, response func() *http.Response) *PrometheusStore {
	t.Helper()
	base, err := url.Parse("http://prometheus.test")
	testutil.Ok(t, err)
	client := memoryResponseClient(func(*http.Request) (*http.Response, error) { return response(), nil })
	store, err := NewPrometheusStore(nil, prometheus.NewRegistry(), promclient.NewClient(client, nil, ""), base, component.Sidecar,
		func() labels.Labels { return ext }, func() (int64, int64) { return 0, -1 }, nil)
	testutil.Ok(t, err)
	return store
}

func compactRequest(metadata bool) *storepb.SeriesRequest {
	return &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}, MinTime: 0, MaxTime: 1000, SkipChunks: metadata}
}

func assertSampledSeries(t *testing.T, got storepb.Series, instance string, timestamp int64, value float64) {
	t.Helper()
	assertLabelValue(t, got.Labels, "instance", instance)
	assertLabelValue(t, got.Labels, "cluster", "sidecar")
	if len(got.Chunks) != 1 {
		t.Fatalf("got %d chunks, want one", len(got.Chunks))
	}
	chunk := got.Chunks[0]
	if chunk.MinTime != timestamp || chunk.MaxTime != timestamp || chunk.Raw.Type != storepb.Chunk_XOR {
		t.Fatalf("unexpected sampled chunk: %+v", chunk)
	}
	expected := compactFloatChunk(t, timestamp, value)
	testutil.Equals(t, expected, chunk.Raw.Data)
	testutil.Equals(t, xxhash.Sum64(expected), chunk.Raw.Hash)
}
