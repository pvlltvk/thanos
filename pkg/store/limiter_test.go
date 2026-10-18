// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestLimiter(t *testing.T) {
	t.Parallel()

	c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	l := NewLimiter(10, c)

	testutil.Ok(t, l.Reserve(5))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.Ok(t, l.Reserve(5))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l.Reserve(1))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l.Reserve(2))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))
}

func TestLimitedServerFlush(t *testing.T) {
	t.Parallel()

	t.Run("forwards", func(t *testing.T) {
		upstream := &flushableTestSeriesServer{}
		limited := &limitedPrometheusServer{limitedServer: newLimitedServer(upstream, NewLimiter(0, nil), NewLimiter(0, nil))}
		testutil.Ok(t, limited.Flush())
		testutil.Equals(t, 1, upstream.flushes)
	})
	t.Run("propagates error", func(t *testing.T) {
		wantErr := errors.New("flush failed")
		upstream := &flushableTestSeriesServer{flushErr: wantErr}
		limited := &limitedPrometheusServer{limitedServer: newLimitedServer(upstream, NewLimiter(0, nil), NewLimiter(0, nil))}
		testutil.Equals(t, wantErr, limited.Flush())
	})
	t.Run("non-flushable server is a no-op", func(t *testing.T) {
		limited := &limitedPrometheusServer{limitedServer: newLimitedServer(newStoreSeriesServer(context.Background()), NewLimiter(0, nil), NewLimiter(0, nil))}
		testutil.Ok(t, limited.Flush())
	})
}

func TestNewLimitedStoreServerWrapsUnlimitedPrometheusStore(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	baseURL, err := url.Parse("http://prometheus.test")
	testutil.Ok(t, err)
	promStore, err := NewPrometheusStore(nil, reg, promclient.NewClient(nil, nil, ""), baseURL, component.Sidecar,
		func() labels.Labels { return labels.EmptyLabels() }, func() (int64, int64) { return 0, -1 }, nil)
	testutil.Ok(t, err)
	NewLimitedStoreServer(NewInstrumentedStoreServer(reg, promStore), reg, SeriesSelectLimits{SeriesPerRequest: 1})
}

type flushableTestSeriesServer struct {
	storepb.Store_SeriesServer
	flushes  int
	flushErr error
}

func (s *flushableTestSeriesServer) Flush() error {
	s.flushes++
	return s.flushErr
}

func TestLimitedStoreServer_TSDBSortingAndBatching(t *testing.T) {
	db, err := e2eutil.NewTSDB()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, db.Close()) }()

	appender := db.Appender(t.Context())
	for _, lset := range []labels.Labels{labels.FromStrings("a", "1"), labels.FromStrings("a", "1", "b", "1")} {
		_, err := appender.Append(0, lset, 1, 1)
		testutil.Ok(t, err)
	}
	testutil.Ok(t, appender.Commit())

	tsdbStore := NewTSDBStore(nil, db, component.Rule, labels.FromStrings("z", "1"))
	store := NewLimitedStoreServer(tsdbStore, prometheus.NewRegistry(), SeriesSelectLimits{})
	for _, batchSize := range []int64{0, 64} {
		t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
			var responses []*storepb.SeriesResponse
			srv := &mockedSeriesServer{ctx: t.Context(), send: func(response *storepb.SeriesResponse) error {
				responses = append(responses, response)
				return nil
			}}
			testutil.Ok(t, store.Series(&storepb.SeriesRequest{
				MinTime: 1, MaxTime: 1,
				ResponseBatchSize: batchSize,
				Matchers:          []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"}},
			}, srv))
			var series []*storepb.Series
			batches := 0
			for _, response := range responses {
				if s := response.GetSeries(); s != nil {
					series = append(series, s)
				}
				if batch := response.GetBatch(); batch != nil {
					series = append(series, batch.Series...)
					batches++
				}
			}
			testutil.Equals(t, 2, len(series))
			if labels.Compare(labelpb.ZLabelsToPromLabels(series[0].Labels), labelpb.ZLabelsToPromLabels(series[1].Labels)) >= 0 {
				t.Errorf("series are not sorted by final labels: %v then %v", series[0].Labels, series[1].Labels)
			}
			if batchSize > 0 && batches != 1 {
				t.Errorf("expected one batch response, got %d", batches)
			}
			if batchSize == 0 && batches != 0 {
				t.Errorf("expected individual series responses, got %d batches", batches)
			}
		})
	}
}

func TestRateLimitedServer(t *testing.T) {
	t.Parallel()

	numSamples := 60
	series := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labels.FromStrings("series", "1"), makeSamples(numSamples)),
		storeSeriesResponse(t, labels.FromStrings("series", "2"), makeSamples(numSamples)),
		storeSeriesResponse(t, labels.FromStrings("series", "3"), makeSamples(numSamples)),
	}
	batchedSeries := []*storepb.SeriesResponse{
		storepb.NewBatchResponse([]*storepb.Series{
			series[0].GetSeries(),
			nil,
			series[1].GetSeries(),
			series[2].GetSeries(),
		}),
	}
	nonSeriesResponses := []*storepb.SeriesResponse{
		storepb.NewWarnSeriesResponse(errors.New("warning")),
	}
	tests := []struct {
		name   string
		limits SeriesSelectLimits
		series []*storepb.SeriesResponse
		err    string
	}{
		{
			name: "no limits",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: 0,
			},
			series: series,
		},
		{
			name: "series below limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  3,
				SamplesPerRequest: 0,
			},
			series: series,
		},
		{
			name: "series over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  2,
				SamplesPerRequest: 0,
			},
			series: series,
			err:    "failed to send series: limit 2 violated (got 3)",
		},
		{
			name: "batched series below limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  3,
				SamplesPerRequest: uint64(3 * MaxSamplesPerChunk),
			},
			series: batchedSeries,
		},
		{
			name: "non-series responses bypass limits",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  1,
				SamplesPerRequest: 1,
			},
			series: nonSeriesResponses,
		},
		{
			name: "batched series over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  2,
				SamplesPerRequest: 0,
			},
			series: batchedSeries,
			err:    "failed to send series: limit 2 violated (got 3)",
		},
		{
			name: "chunks below limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: uint64(3 * numSamples * MaxSamplesPerChunk),
			},
			series: series,
		},
		{
			name: "chunks over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: 50,
			},
			series: series,
			err:    "failed to send samples: limit 50 violated (got 120)",
		},
		{
			name: "batched chunks over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: uint64(2 * MaxSamplesPerChunk),
			},
			series: batchedSeries,
			err:    "failed to send samples: limit 240 violated (got 360)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			store := NewLimitedStoreServer(newStoreServerStub(test.series), prometheus.NewRegistry(), test.limits)
			client := storepb.ServerAsClient(store, atomic.Bool{})
			seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{})
			testutil.Ok(t, err)
			for {
				_, err = seriesClient.Recv()
				if err == io.EOF {
					err = nil
					break
				}
				if err != nil {
					break
				}
			}
			if test.err == "" {
				testutil.Ok(t, err)
			} else {
				testutil.NotOk(t, err)
				testutil.Assert(t, test.err == err.Error(), "want %s, got %s", test.err, err.Error())
			}
		})
	}
}

func makeSamples(numSamples int) []sample {
	samples := make([]sample, numSamples)
	for i := range samples {
		samples[i] = sample{t: int64(i), v: float64(i)}
	}
	return samples
}

type testStoreServer struct {
	storepb.StoreServer
	responses []*storepb.SeriesResponse
}

func newStoreServerStub(responses []*storepb.SeriesResponse) *testStoreServer {
	return &testStoreServer{responses: responses}
}

func (m *testStoreServer) Series(_ *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	for _, r := range m.responses {
		if err := server.Send(r); err != nil {
			return err
		}
	}
	return nil
}
