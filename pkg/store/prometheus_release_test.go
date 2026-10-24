// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusStore_Series_MemoryRelease(t *testing.T) {
	t.Run("sampled outputs remain independently owned", func(t *testing.T) {
		fixture := sampledReleaseFixture(t)
		store := releasePrometheusStore(t, "application/x-protobuf; proto=prometheus.ReadResponse", func() io.ReadCloser { return io.NopCloser(bytes.NewReader(fixture)) })
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(releaseSeriesRequest(false), srv))
		if len(srv.SeriesSet) != 2 {
			t.Fatalf("got %d series, want 2", len(srv.SeriesSet))
		}
		secondLabels := append([]labelpb.ZLabel(nil), srv.SeriesSet[1].Labels...)
		secondChunk := append([]byte(nil), srv.SeriesSet[1].Chunks[0].Raw.Data...)
		srv.SeriesSet[0].Labels[0].Value = "mutated"
		srv.SeriesSet[0].Chunks[0].Raw.Data[0] ^= 0xff
		testutil.Equals(t, secondLabels, srv.SeriesSet[1].Labels)
		testutil.Equals(t, secondChunk, srv.SeriesSet[1].Chunks[0].Raw.Data)
	})

	t.Run("metadata outputs remain independently owned", func(t *testing.T) {
		fixture := []byte(`{"status":"success","data":[{"__name__":"metric","instance":"a","job":"api"},{"__name__":"metric","instance":"b","job":"api"}]}`)
		store := releasePrometheusStore(t, "application/json", func() io.ReadCloser { return io.NopCloser(bytes.NewReader(fixture)) })
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(releaseSeriesRequest(true), srv))
		if len(srv.SeriesSet) != 2 {
			t.Fatalf("got %d series, want 2", len(srv.SeriesSet))
		}
		secondLabels := append([]labelpb.ZLabel(nil), srv.SeriesSet[1].Labels...)
		srv.SeriesSet[0].Labels[0].Value = "mutated"
		testutil.Equals(t, secondLabels, srv.SeriesSet[1].Labels)
	})
}

func sampledReleaseFixture(t *testing.T) []byte {
	t.Helper()
	response := &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{
		{Labels: []labelpb.ZLabel{{Name: "instance", Value: "a"}, {Name: "job", Value: "api"}}, Samples: []prompb.Sample{{Timestamp: 1, Value: 1}}},
		{Labels: []labelpb.ZLabel{{Name: "instance", Value: "b"}, {Name: "job", Value: "api"}}, Samples: []prompb.Sample{{Timestamp: 1, Value: 2}}},
	}}}}
	encoded, err := proto.Marshal(response)
	testutil.Ok(t, err)
	return snappy.Encode(nil, encoded)
}

func releasePrometheusStore(t *testing.T, contentType string, body func() io.ReadCloser) *PrometheusStore {
	t.Helper()
	base, err := url.Parse("http://prometheus.test")
	testutil.Ok(t, err)
	client := memoryResponseClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}}, Body: body()}, nil
	})
	store, err := NewPrometheusStore(nil, prometheus.NewRegistry(), promclient.NewClient(client, nil, ""), base, component.Sidecar,
		func() labels.Labels { return labels.EmptyLabels() }, func() (int64, int64) { return 0, -1 }, nil)
	testutil.Ok(t, err)
	return store
}

func releaseSeriesRequest(metadata bool) *storepb.SeriesRequest {
	return &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}, MinTime: 0, MaxTime: 1, SkipChunks: metadata}
}
