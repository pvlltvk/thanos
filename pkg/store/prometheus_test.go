// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestPrometheusStore_Series_e2e(t *testing.T) {
	t.Parallel()

	testPrometheusStoreSeriesE2e(t, "")
}

func TestPrometheusStore_Series_EarlyLimits(t *testing.T) {
	t.Parallel()

	for _, batchSize := range []int64{0, 1, 64} {
		t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
			cases := []struct {
				name         string
				limits       SeriesSelectLimits
				wantErr      bool
				framesRead   int
				wantSeries   int
				failedReason string
			}{
				{name: "unlimited", framesRead: 3, wantSeries: 4},
				{name: "series below limit", limits: SeriesSelectLimits{SeriesPerRequest: 5}, framesRead: 3, wantSeries: 4},
				{name: "exact series", limits: SeriesSelectLimits{SeriesPerRequest: 4}, framesRead: 3, wantSeries: 4},
				{name: "series one over", limits: SeriesSelectLimits{SeriesPerRequest: 3}, wantErr: true, framesRead: 3, failedReason: "series"},
				{name: "exact samples", limits: SeriesSelectLimits{SamplesPerRequest: 4 * MaxSamplesPerChunk}, framesRead: 3, wantSeries: 4},
				{name: "samples below limit", limits: SeriesSelectLimits{SamplesPerRequest: 5 * MaxSamplesPerChunk}, framesRead: 3, wantSeries: 4},
				{name: "samples one over", limits: SeriesSelectLimits{SamplesPerRequest: 3 * MaxSamplesPerChunk}, wantErr: true, framesRead: 3, failedReason: "chunks"},
				{name: "samples reject within frame", limits: SeriesSelectLimits{SamplesPerRequest: MaxSamplesPerChunk}, wantErr: true, framesRead: 1, failedReason: "chunks"},
				{name: "series rejects within frame", limits: SeriesSelectLimits{SeriesPerRequest: 1}, wantErr: true, framesRead: 1, failedReason: "series"},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					reg := prometheus.NewRegistry()
					body := newTrackedFramesBody(earlyLimitFrames(t))
					store := newEarlyLimitsStore(t, reg, tc.limits, body)
					srv := newStoreSeriesServer(context.Background())
					err := store.Series(&storepb.SeriesRequest{
						ResponseBatchSize: batchSize,
						Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}},
					}, srv)

					if tc.wantErr {
						testutil.NotOk(t, err)
						testutil.Equals(t, 0, len(srv.SeriesSet))
					} else {
						testutil.Ok(t, err)
						testutil.Equals(t, tc.wantSeries, len(srv.SeriesSet))
					}
					testutil.Equals(t, tc.framesRead, body.framesConsumed())
					testutil.Assert(t, body.isClosed(), "stream response body was not closed")
					wantSeriesDrops, wantChunkDrops := 0.0, 0.0
					switch tc.failedReason {
					case "series":
						wantSeriesDrops = 1
					case "chunks":
						wantChunkDrops = 1
					}
					testutil.Equals(t, wantSeriesDrops, selectDropCount(t, reg, "series"))
					testutil.Equals(t, wantChunkDrops, selectDropCount(t, reg, "chunks"))
				})
			}
		})
	}
	t.Run("request budgets reset and counter increments once", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		firstBody := newTrackedFramesBody(earlyLimitFrames(t))
		secondBody := newTrackedFramesBody(earlyLimitFramesThreeSeries(t))
		store := newEarlyLimitsStore(t, reg, SeriesSelectLimits{SeriesPerRequest: 3}, firstBody, secondBody)
		request := &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}

		first := newStoreSeriesServer(context.Background())
		err := store.Series(request, first)
		testutil.NotOk(t, err)
		testutil.Equals(t, 0, len(first.SeriesSet))

		second := newStoreSeriesServer(context.Background())
		err = store.Series(request, second)
		testutil.Ok(t, err)
		testutil.Equals(t, 3, len(second.SeriesSet))
		testutil.Equals(t, 3, firstBody.framesConsumed())
		testutil.Equals(t, 2, secondBody.framesConsumed())
		testutil.Assert(t, firstBody.isClosed(), "first response body was not closed")
		testutil.Assert(t, secondBody.isClosed(), "second response body was not closed")
		testutil.Equals(t, float64(1), selectDropCount(t, reg, "series"))
	})
}

func TestPrometheusStore_Series_EarlyLimitsOrdering(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		externalLabels labels.Labels
		without        []string
		series         []*prompb.ChunkedSeries
		wantLabels     [][]labelpb.ZLabel
		wantValues     []float64
	}{
		{
			name:           "collision-free insertion",
			externalLabels: labels.FromStrings("region", "eu"),
			series: []*prompb.ChunkedSeries{
				prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "a", Value: "1"}}, 1),
				prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}}, 2),
			},
			wantLabels: [][]labelpb.ZLabel{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}, {Name: "region", Value: "eu"}},
				{{Name: "a", Value: "1"}, {Name: "region", Value: "eu"}},
			},
			wantValues: []float64{2, 1},
		},
		{
			name:           "external label overrides input",
			externalLabels: labels.FromStrings("region", "eu"),
			series: []*prompb.ChunkedSeries{
				prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "region", Value: "a"}, {Name: "z", Value: "2"}}, 2),
				prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "region", Value: "b"}, {Name: "z", Value: "1"}}, 1),
			},
			wantLabels: [][]labelpb.ZLabel{
				{{Name: "region", Value: "eu"}, {Name: "z", Value: "1"}},
				{{Name: "region", Value: "eu"}, {Name: "z", Value: "2"}},
			},
			wantValues: []float64{1, 2},
		},
		{
			name:    "replica label removal",
			without: []string{"a"},
			series: []*prompb.ChunkedSeries{
				prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "a", Value: "0"}, {Name: "b", Value: "2"}}, 2),
				prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}}, 1),
			},
			wantLabels: [][]labelpb.ZLabel{
				{{Name: "b", Value: "1"}},
				{{Name: "b", Value: "2"}},
			},
			wantValues: []float64{1, 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var baseline []storepb.Series
			for _, batchSize := range []int64{0, 1, 64} {
				t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
					reg := prometheus.NewRegistry()
					body := newTrackedFramesBody([][]byte{chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: tc.series})})
					store := newEarlyLimitsStoreWithExternalLabels(t, reg, SeriesSelectLimits{}, tc.externalLabels, body)
					srv := newStoreSeriesServer(context.Background())
					err := store.Series(&storepb.SeriesRequest{
						ResponseBatchSize:    batchSize,
						WithoutReplicaLabels: tc.without,
						Matchers:             []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}},
					}, srv)
					testutil.Ok(t, err)
					testutil.Equals(t, tc.wantLabels, seriesLabels(srv.SeriesSet))
					testutil.Equals(t, tc.wantValues, seriesValues(t, srv.SeriesSet))
					testutil.Equals(t, uint64(1), histogramCount(t, reg, "thanos_store_server_series_requested"))
					testutil.Equals(t, float64(len(tc.series)), histogramSum(t, reg, "thanos_store_server_series_requested"))
					testutil.Equals(t, float64(len(tc.series)), histogramSum(t, reg, "thanos_store_server_chunks_requested"))
					testutil.Assert(t, body.isClosed(), "stream response body was not closed")
					if baseline == nil {
						baseline = srv.SeriesSet
					} else {
						testutil.Equals(t, baseline, srv.SeriesSet)
					}
				})
			}
		})
	}
}

func TestPrometheusStore_Series_EarlyLimitsAlternatePaths(t *testing.T) {
	t.Parallel()

	t.Run("metadata success sorts and estimates zero chunks", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		payload := newTrackedPayloadBody([]byte(`{"status":"success","data":[{"series":"z","job":"api"},{"series":"a","job":"api"}]}`))
		store := newEarlyLimitsStoreWithClient(t, reg, SeriesSelectLimits{SeriesPerRequest: 2}, labels.EmptyLabels(), roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: payload}, nil
		}))
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{SkipChunks: true, Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.Ok(t, err)
		testutil.Equals(t, [][]labelpb.ZLabel{{{Name: "job", Value: "api"}, {Name: "series", Value: "a"}}, {{Name: "job", Value: "api"}, {Name: "series", Value: "z"}}}, seriesLabels(srv.SeriesSet))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
		testutil.Equals(t, len(`{"status":"success","data":[{"series":"z","job":"api"},{"series":"a","job":"api"}]}`), payload.readCount())
		testutil.Assert(t, payload.isClosed(), "metadata response body was not closed")
	})

	t.Run("metadata rejection occurs before sorter output", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		data := []byte(`{"status":"success","data":[{"series":"a","job":"api"},{"series":"b","job":"api"}]}`)
		payload := newTrackedPayloadBody(data)
		store := newEarlyLimitsStoreWithClient(t, reg, SeriesSelectLimits{SeriesPerRequest: 1}, labels.EmptyLabels(), roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: payload}, nil
		}))
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{SkipChunks: true, Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.NotOk(t, err)
		testutil.Equals(t, 0, len(srv.SeriesSet))
		testutil.Equals(t, len(data), payload.readCount())
		testutil.Assert(t, payload.isClosed(), "metadata response body was not closed")
		testutil.Equals(t, float64(1), selectDropCount(t, reg, "series"))
	})

	t.Run("sampled success sorts and uses chunk estimate", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		payload := sampledResponseBody(t, &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{
			prometheusSampledSeries("z", 2), prometheusSampledSeries("a", 1),
		}}}})
		store := newSampledLimitsStore(t, reg, SeriesSelectLimits{SamplesPerRequest: 2 * MaxSamplesPerChunk}, payload)
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.Ok(t, err)
		testutil.Equals(t, [][]labelpb.ZLabel{{{Name: "series", Value: "a"}}, {{Name: "series", Value: "z"}}}, seriesLabels(srv.SeriesSet))
		testutil.Equals(t, []float64{1, 2}, seriesValues(t, srv.SeriesSet))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
		testutil.Assert(t, payload.isClosed(), "sampled response body was not closed")
	})

	t.Run("sampled one over rejects before sorter output", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		payload := sampledResponseBody(t, &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{
			prometheusSampledSeries("a", 1), prometheusSampledSeries("b", 2),
		}}}})
		store := newSampledLimitsStore(t, reg, SeriesSelectLimits{SamplesPerRequest: MaxSamplesPerChunk}, payload)
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.NotOk(t, err)
		testutil.Equals(t, 0, len(srv.SeriesSet))
		testutil.Equals(t, float64(1), selectDropCount(t, reg, "chunks"))
		testutil.Assert(t, payload.isClosed(), "sampled response body was not closed")
	})

	t.Run("sampled empty series does not charge sample budget", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		payload := sampledResponseBody(t, &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{{
			Labels: []labelpb.ZLabel{{Name: "series", Value: "empty"}},
		}, prometheusSampledSeries("real", 1)}}}})
		store := newSampledLimitsStore(t, reg, SeriesSelectLimits{SeriesPerRequest: 1, SamplesPerRequest: MaxSamplesPerChunk}, payload)
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(srv.SeriesSet))
		testutil.Equals(t, [][]labelpb.ZLabel{{{Name: "series", Value: "real"}}}, seriesLabels(srv.SeriesSet))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
		testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
	})
}

func TestPrometheusStore_Series_EarlyLimitsCleanup(t *testing.T) {
	t.Parallel()

	t.Run("cancellation while reading closes body", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body := &cancelAfterFrameBody{body: newTrackedFramesBody(earlyLimitFramesThreeSeries(t)[:1]), cancel: cancel}
		store := newEarlyLimitsStore(t, reg, SeriesSelectLimits{}, body)
		srv := newStoreSeriesServer(ctx)
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), context.Canceled.Error()), "want cancellation error, got %v", err)
		testutil.Assert(t, body.body.isClosed(), "stream response body was not closed")
	})

	t.Run("malformed stream closes body", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		body := newTrackedFramesBody([][]byte{chunkedRawFrame(t, []byte{0xff})})
		store := newEarlyLimitsStore(t, reg, SeriesSelectLimits{}, body)
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), "next proto"), "want malformed frame error, got %v", err)
		testutil.Assert(t, body.isClosed(), "stream response body was not closed")
	})

	t.Run("upstream request failure propagates", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		wantErr := fmt.Errorf("upstream request failed")
		store := newEarlyLimitsStoreWithClient(t, reg, SeriesSelectLimits{}, labels.EmptyLabels(), roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, wantErr
		}))
		srv := newStoreSeriesServer(context.Background())
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), wantErr.Error()), "want upstream error, got %v", err)
	})

	t.Run("downstream send failure propagates and closes body", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		body := newTrackedFramesBody(earlyLimitFramesThreeSeries(t))
		store := newEarlyLimitsStore(t, reg, SeriesSelectLimits{}, body)
		wantErr := fmt.Errorf("downstream send failed")
		srv := &failingStoreSeriesServer{ctx: context.Background(), err: wantErr}
		err := store.Series(&storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}}, srv)
		testutil.Equals(t, wantErr, err)
		testutil.Equals(t, 2, body.framesConsumed())
		testutil.Assert(t, body.isClosed(), "stream response body was not closed")
	})
}

func TestPrometheusStore_Series_EarlyLimitsSharding(t *testing.T) {
	t.Parallel()

	shard := &storepb.ShardInfo{ShardIndex: 0, TotalShards: 2, By: true, Labels: []string{"series"}}
	buffers := &sync.Pool{New: func() any { return new([]byte) }}
	matcher := shard.Matcher(buffers)
	defer matcher.Close()
	var included, excluded string
	for i := 0; i < 100 && (included == "" || excluded == ""); i++ {
		value := fmt.Sprintf("series%d", i)
		if matcher.MatchesLabels(labels.FromStrings("series", value)) {
			included = value
		} else {
			excluded = value
		}
	}
	testutil.Assert(t, included != "" && excluded != "", "could not find labels on both shards")

	for _, batchSize := range []int64{0, 1, 64} {
		t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
			reg := prometheus.NewRegistry()
			body := newTrackedFramesBody([][]byte{chunkedReadFrame(t, &prompb.ChunkedReadResponse{
				ChunkedSeries: []*prompb.ChunkedSeries{
					prometheusChunkedSeries(t, excluded),
					prometheusChunkedSeries(t, included),
					prometheusChunkedSeries(t, excluded),
				},
			})})
			store := newEarlyLimitsStore(t, reg, SeriesSelectLimits{SeriesPerRequest: 1, SamplesPerRequest: MaxSamplesPerChunk}, body)
			srv := newStoreSeriesServer(context.Background())
			testutil.Ok(t, store.Series(&storepb.SeriesRequest{
				ResponseBatchSize: batchSize,
				Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}},
				ShardInfo:         shard,
			}, srv))
			testutil.Equals(t, 1, len(srv.SeriesSet))
			testutil.Equals(t, []labelpb.ZLabel{{Name: "series", Value: included}}, srv.SeriesSet[0].Labels)
			testutil.Equals(t, 1, body.framesConsumed())
			testutil.Assert(t, body.isClosed(), "stream response body was not closed")
			testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
			testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
		})
	}
}

func TestPrometheusStore_Series_EarlyLimitsLargeHistogram(t *testing.T) {
	t.Parallel()

	h := &histogram.Histogram{
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Length: 300000}},
		PositiveBuckets: make([]int64, 300000),
	}
	for i := range h.PositiveBuckets {
		if i%2 == 0 {
			h.PositiveBuckets[i] = 1 << 40
			h.Count += 1 << 40
		} else {
			h.PositiveBuckets[i] = -(1 << 40)
		}
	}
	testutil.Ok(t, h.Validate())
	chunk := chunkenc.NewHistogramChunk()
	appender, err := chunk.Appender()
	testutil.Ok(t, err)
	_, _, _, err = appender.AppendHistogram(nil, 1, h, false)
	testutil.Ok(t, err)
	testutil.Assert(t, len(chunk.Bytes()) > 1<<20, "histogram chunk must exceed 1 MiB")
	testutil.Equals(t, 1, chunk.NumSamples())

	for _, batchSize := range []int64{0, 1, 64} {
		for _, tc := range []struct {
			name       string
			chunkCount int
			limit      uint64
			wantErr    bool
		}{
			{name: "single chunk over", chunkCount: 1, limit: MaxSamplesPerChunk - 1, wantErr: true},
			{name: "single chunk exact", chunkCount: 1, limit: MaxSamplesPerChunk},
			{name: "two chunks over", chunkCount: 2, limit: 2*MaxSamplesPerChunk - 1, wantErr: true},
			{name: "two chunks exact", chunkCount: 2, limit: 2 * MaxSamplesPerChunk},
		} {
			t.Run(fmt.Sprintf("batch_size_%d/%s", batchSize, tc.name), func(t *testing.T) {
				series := &prompb.ChunkedSeries{Labels: []labelpb.ZLabel{{Name: "series", Value: "histogram"}}}
				for range tc.chunkCount {
					series.Chunks = append(series.Chunks, prompb.Chunk{MinTimeMs: 1, MaxTimeMs: 1, Type: prompb.Chunk_HISTOGRAM, Data: chunk.Bytes()})
				}
				body := newTrackedFramesBody([][]byte{chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{series}})})
				reg := prometheus.NewRegistry()
				store := newEarlyLimitsStore(t, reg, SeriesSelectLimits{SamplesPerRequest: tc.limit}, body)
				srv := newStoreSeriesServer(context.Background())
				err := store.Series(&storepb.SeriesRequest{
					ResponseBatchSize: batchSize,
					Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}},
				}, srv)
				if tc.wantErr {
					testutil.NotOk(t, err)
					testutil.Equals(t, 0, len(srv.SeriesSet))
					testutil.Equals(t, 1.0, selectDropCount(t, reg, "chunks"))
				} else {
					testutil.Ok(t, err)
					testutil.Equals(t, 1, len(srv.SeriesSet))
					testutil.Equals(t, tc.chunkCount, len(srv.SeriesSet[0].Chunks))
					for _, actual := range srv.SeriesSet[0].Chunks {
						testutil.Equals(t, storepb.Chunk_HISTOGRAM, actual.Raw.Type)
						testutil.Equals(t, chunk.Bytes(), actual.Raw.Data)
					}
					testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
				}
				testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
				testutil.Assert(t, body.isClosed(), "stream response body was not closed")
			})
		}
	}
}

func chunkedRawFrame(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := remote.NewChunkedWriter(&buf, testHTTPFlusher{})
	_, err := writer.Write(payload)
	testutil.Ok(t, err)
	return buf.Bytes()
}

type cancelAfterFrameBody struct {
	body   *trackedFramesBody
	cancel context.CancelFunc
	reads  int
}

func (b *cancelAfterFrameBody) Read(p []byte) (int, error) {
	b.reads++
	if b.reads > 1 {
		b.cancel()
		return 0, context.Canceled
	}
	return b.body.Read(p)
}

func (b *cancelAfterFrameBody) Close() error { return b.body.Close() }

type trackedPayloadBody struct {
	mu       sync.Mutex
	data     []byte
	position int
	closed   bool
}

func newTrackedPayloadBody(data []byte) *trackedPayloadBody {
	return &trackedPayloadBody{data: data}
}

func (b *trackedPayloadBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.position == len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.position:])
	b.position += n
	return n, nil
}

func (b *trackedPayloadBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *trackedPayloadBody) readCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.position
}

func (b *trackedPayloadBody) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

type failingStoreSeriesServer struct {
	storepb.Store_SeriesServer
	ctx context.Context
	err error
}

func (s *failingStoreSeriesServer) Context() context.Context { return s.ctx }

func (s *failingStoreSeriesServer) Send(*storepb.SeriesResponse) error { return s.err }

func newSampledLimitsStore(t *testing.T, reg *prometheus.Registry, limits SeriesSelectLimits, body *trackedPayloadBody) storepb.StoreServer {
	t.Helper()
	return newEarlyLimitsStoreWithClient(t, reg, limits, labels.EmptyLabels(), roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/x-protobuf"}}, Body: body}, nil
	}))
}

func sampledResponseBody(t *testing.T, response *prompb.ReadResponse) *trackedPayloadBody {
	t.Helper()
	b, err := proto.Marshal(response)
	testutil.Ok(t, err)
	return newTrackedPayloadBody(snappy.Encode(nil, b))
}

func prometheusSampledSeries(labelValue string, value float64) *prompb.TimeSeries {
	return &prompb.TimeSeries{
		Labels:  []labelpb.ZLabel{{Name: "series", Value: labelValue}},
		Samples: []prompb.Sample{{Timestamp: 1, Value: value}},
	}
}

func seriesLabels(series []storepb.Series) [][]labelpb.ZLabel {
	labels := make([][]labelpb.ZLabel, len(series))
	for i := range series {
		labels[i] = series[i].Labels
	}
	return labels
}

func seriesValues(t *testing.T, series []storepb.Series) []float64 {
	t.Helper()
	values := make([]float64, len(series))
	for i := range series {
		testutil.Equals(t, 1, len(series[i].Chunks))
		chunk, err := chunkenc.FromData(chunkenc.EncXOR, series[i].Chunks[0].Raw.Data)
		testutil.Ok(t, err)
		samples := expandChunk(chunk.Iterator(nil))
		testutil.Equals(t, 1, len(samples))
		values[i] = samples[0].v
	}
	return values
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	testutil.Ok(t, err)
	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) > 0 {
			return family.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func earlyLimitFrames(t *testing.T) [][]byte {
	t.Helper()
	return [][]byte{
		chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{
			prometheusChunkedSeries(t, "same"),
			prometheusChunkedSeries(t, "same"),
		}}),
		chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{
			prometheusChunkedSeries(t, "same"),
		}}),
		chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{
			prometheusChunkedSeries(t, "fourth"),
		}}),
	}
}

func earlyLimitFramesThreeSeries(t *testing.T) [][]byte {
	t.Helper()
	return [][]byte{
		chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{
			prometheusChunkedSeries(t, "same"),
			prometheusChunkedSeries(t, "same"),
		}}),
		chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{
			prometheusChunkedSeries(t, "same"),
		}}),
	}
}

func newEarlyLimitsStore(t *testing.T, reg *prometheus.Registry, limits SeriesSelectLimits, bodies ...io.ReadCloser) storepb.StoreServer {
	return newEarlyLimitsStoreWithExternalLabels(t, reg, limits, labels.EmptyLabels(), bodies...)
}

func newEarlyLimitsStoreWithExternalLabels(t *testing.T, reg *prometheus.Registry, limits SeriesSelectLimits, extLabels labels.Labels, bodies ...io.ReadCloser) storepb.StoreServer {
	t.Helper()
	index := 0
	client := roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := bodies[index]
		index++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{streamedProtoContentType}},
			Body:       body,
		}, nil
	})
	return newEarlyLimitsStoreWithClient(t, reg, limits, extLabels, client)
}

func newEarlyLimitsStoreWithClient(t *testing.T, reg *prometheus.Registry, limits SeriesSelectLimits, extLabels labels.Labels, client promclient.HTTPClient) storepb.StoreServer {
	t.Helper()
	baseURL, err := url.Parse("http://prometheus.test")
	testutil.Ok(t, err)
	promStore, err := NewPrometheusStore(nil, reg, promclient.NewClient(client, nil, ""), baseURL, component.Sidecar,
		func() labels.Labels { return extLabels },
		func() (int64, int64) { return 0, -1 }, nil,
		WithPrometheusStoreLimits(limits),
	)
	testutil.Ok(t, err)
	return NewInstrumentedStoreServer(reg, promStore)
}

const streamedProtoContentType = "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"

func selectDropCount(t *testing.T, reg *prometheus.Registry, reason string) float64 {
	t.Helper()
	families, err := reg.Gather()
	testutil.Ok(t, err)
	for _, family := range families {
		if family.GetName() != "thanos_store_selects_dropped_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "reason" && label.GetValue() == reason {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func prometheusChunkedSeries(t *testing.T, labelValue string) *prompb.ChunkedSeries {
	t.Helper()
	return prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "series", Value: labelValue}}, 1)
}

func prometheusChunkedSeriesWithLabels(t *testing.T, seriesLabels []labelpb.ZLabel, sampleValue float64) *prompb.ChunkedSeries {
	t.Helper()
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	testutil.Ok(t, err)
	appender.Append(1, sampleValue)
	return &prompb.ChunkedSeries{
		Labels: seriesLabels,
		Chunks: []prompb.Chunk{{
			MinTimeMs: 1,
			MaxTimeMs: 1,
			Type:      prompb.Chunk_XOR,
			Data:      chunk.Bytes(),
		}},
	}
}

func chunkedReadFrame(t *testing.T, response *prompb.ChunkedReadResponse) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := remote.NewChunkedWriter(&buf, testHTTPFlusher{})
	b, err := proto.Marshal(response)
	testutil.Ok(t, err)
	_, err = w.Write(b)
	testutil.Ok(t, err)
	return buf.Bytes()
}

type testHTTPFlusher struct{}

func (testHTTPFlusher) Flush() {}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

type trackedFramesBody struct {
	mu       sync.Mutex
	data     []byte
	ends     []int
	position int
	consumed int
	closed   bool
}

func newTrackedFramesBody(frames [][]byte) *trackedFramesBody {
	body := &trackedFramesBody{}
	for _, frame := range frames {
		body.data = append(body.data, frame...)
		body.ends = append(body.ends, len(body.data))
	}
	return body
}

func (b *trackedFramesBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.position == len(b.data) {
		return 0, io.EOF
	}
	frameEnd := b.ends[b.consumed]
	n := copy(p, b.data[b.position:frameEnd])
	b.position += n
	if b.position == frameEnd {
		b.consumed++
	}
	return n, nil
}

func (b *trackedFramesBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *trackedFramesBody) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *trackedFramesBody) framesConsumed() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consumed
}

// Regression test for https://github.com/thanos-io/thanos/issues/478.
func TestPrometheusStore_Series_promOnPath_e2e(t *testing.T) {
	t.Parallel()

	testPrometheusStoreSeriesE2e(t, "/prometheus/sub/path")
}

func testPrometheusStoreSeriesE2e(t *testing.T, prefix string) {
	p, err := e2eutil.NewPrometheusOnPath(prefix)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	// region is an external label; by adding it as an internal label too we also trigger
	// the resorting code paths
	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "local"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "local"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "local"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start(ctx, log.NewLogfmtLogger(os.Stderr)))

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	limitMinT := int64(0)
	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return limitMinT, -1 },
		nil,
	) // MaxTime does not matter.
	testutil.Ok(t, err)

	// Query all three samples except for the first one. Since we round up queried data
	// to seconds, we can test whether the extra sample gets stripped properly.
	{
		srv := newStoreSeriesServer(ctx)
		testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
			MinTime: baseT + 101,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
		}, srv))

		testutil.Equals(t, 1, len(srv.SeriesSet))

		testutil.Equals(t, []labelpb.ZLabel{
			{Name: "a", Value: "b"},
			{Name: "region", Value: "eu-west"},
		}, srv.SeriesSet[0].Labels)
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, 1, len(srv.SeriesSet[0].Chunks))

		c := srv.SeriesSet[0].Chunks[0]
		testutil.Equals(t, storepb.Chunk_XOR, c.Raw.Type)

		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		testutil.Ok(t, err)

		samples := expandChunk(chk.Iterator(nil))
		testutil.Equals(t, []sample{{baseT + 200, 2}, {baseT + 300, 3}}, samples)

	}
	// Query all samples, but limit mint time to exclude the first one.
	{
		limitMinT = baseT + 101
		srv := newStoreSeriesServer(ctx)
		testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
		}, srv))
		// Revert for next cases.
		limitMinT = 0

		testutil.Equals(t, 1, len(srv.SeriesSet))

		testutil.Equals(t, []labelpb.ZLabel{
			{Name: "a", Value: "b"},
			{Name: "region", Value: "eu-west"},
		}, srv.SeriesSet[0].Labels)

		testutil.Equals(t, 1, len(srv.SeriesSet[0].Chunks))

		c := srv.SeriesSet[0].Chunks[0]
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, storepb.Chunk_XOR, c.Raw.Type)

		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		testutil.Ok(t, err)

		samples := expandChunk(chk.Iterator(nil))
		testutil.Equals(t, []sample{{baseT + 200, 2}, {baseT + 300, 3}}, samples)
	}
	// Querying by external labels only.
	{
		srv := newStoreSeriesServer(ctx)

		err = proxy.Series(&storepb.SeriesRequest{
			MinTime: baseT + 101,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
			},
		}, srv)
		testutil.NotOk(t, err)
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, "rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)", err.Error())
	}
}

type sample struct {
	t int64
	v float64
}

func expandChunk(cit chunkenc.Iterator) (res []sample) {
	for cit.Next() != chunkenc.ValNone {
		t, v := cit.At()
		res = append(res, sample{t, v})
	}
	return res
}

func TestPrometheusStore_SeriesLabels_e2e(t *testing.T) {
	t.Parallel()

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b", "b", "d"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "c", "b", "d", "job", "test"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "d", "b", "d", "job", "test"), baseT+300, 3)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("b", "d", "job", "test"), baseT+400, 4)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx := t.Context()

	testutil.Ok(t, p.Start(ctx, log.NewNopLogger()))

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	promStore, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return math.MinInt64/1000 + 62135596801, math.MaxInt64/1000 - 62135596801 },
		nil,
	)
	testutil.Ok(t, err)

	for _, tcase := range []struct {
		req         *storepb.SeriesRequest
		expected    []storepb.Series
		expectedErr error
	}{
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers:   []storepb.LabelMatcher{},
				MinTime:    baseT - 10000000000,
				MaxTime:    baseT + 10000000000,
			},
			expectedErr: errors.New("rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)"),
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "non_existing", Value: "something"},
				},
				MinTime: baseT - 10000000000,
				MaxTime: baseT + 10000000000,
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "b"}, {Name: "b", Value: "d"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "foo"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "b"},
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: baseT + 400,
				MaxTime: baseT + 400,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: func() int64 { minTime, _ := promStore.timestamps(); return minTime }(),
				MaxTime: func() int64 { _, maxTime := promStore.timestamps(); return maxTime }(),
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			srv := newStoreSeriesServer(ctx)
			err = promStore.Series(tcase.req, srv)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, []string(nil), srv.Warnings)
			testutil.Equals(t, tcase.expected, srv.SeriesSet)
		})
	}
}

func TestPrometheusStore_Series_MatchExternalLabel(t *testing.T) {
	t.Parallel()

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx := t.Context()

	testutil.Ok(t, p.Start(ctx, log.NewNopLogger()))

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return 0, math.MaxInt64 },
		nil)
	testutil.Ok(t, err)
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv))
	testutil.Equals(t, 1, len(srv.SeriesSet))

	testutil.Equals(t, []labelpb.ZLabel{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, srv.SeriesSet[0].Labels)

	srv = newStoreSeriesServer(ctx)
	// However, it should not match wrong external label.
	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west2"}, // Non existing label value.
		},
	}, srv))

	// No series.
	testutil.Equals(t, 0, len(srv.SeriesSet))
}

func TestPrometheusStore_Series_ChunkHashCalculation_Integration(t *testing.T) {
	t.Parallel()

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx := t.Context()

	testutil.Ok(t, p.Start(ctx, log.NewNopLogger()))

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return 0, math.MaxInt64 },
		nil)
	testutil.Ok(t, err)
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv))
	testutil.Equals(t, 1, len(srv.SeriesSet))

	for _, chunk := range srv.SeriesSet[0].Chunks {
		got := chunk.Raw.Hash
		want := xxhash.Sum64(chunk.Raw.Data)
		testutil.Equals(t, want, got)
	}
}
