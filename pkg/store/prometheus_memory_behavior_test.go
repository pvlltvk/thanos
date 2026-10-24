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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusStore_Series_MemoryLabels(t *testing.T) {
	t.Run("streamed overwrite removal and frame ownership", func(t *testing.T) {
		exts := labels.FromStrings("cluster", "external", "external_only", "value", "replica", "external-replica")
		first := prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{
			{Name: "cluster", Value: "local"}, {Name: "external_only", Value: "local"}, {Name: "instance", Value: "a"},
			{Name: "job", Value: "api"}, {Name: "replica", Value: "local-replica"},
		}, 1)
		second := prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "instance", Value: "b"}, {Name: "job", Value: "api"}}, 2)
		body := io.NopCloser(bytes.NewReader(bytes.Join([][]byte{
			chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{first}}),
			chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{second}}),
		}, nil)))
		store := memoryBehaviorStore(t, exts, body)
		srv := newStoreSeriesServer(context.Background())
		req := memoryLabelsRequest(false)
		req.WithoutReplicaLabels = []string{"replica"}
		if err := store.Series(req, srv); err != nil {
			t.Fatal(err)
		}
		if len(srv.SeriesSet) != 2 {
			t.Fatalf("got %d series, want 2", len(srv.SeriesSet))
		}
		assertLabelValue(t, srv.SeriesSet[0].Labels, "cluster", "external")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "external_only", "value")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "instance", "a")
		assertNoLabel(t, srv.SeriesSet[0].Labels, "replica")
		assertLabelValue(t, srv.SeriesSet[1].Labels, "cluster", "external")
		assertLabelValue(t, srv.SeriesSet[1].Labels, "instance", "b")
		assertNoLabel(t, srv.SeriesSet[1].Labels, "replica")
		before := append([]labelpb.ZLabel(nil), srv.SeriesSet[1].Labels...)
		srv.SeriesSet[0].Labels[0].Value = "mutated"
		testutil.Equals(t, before, srv.SeriesSet[1].Labels)
	})

	t.Run("empty external labels preserve frame labels", func(t *testing.T) {
		series := prometheusChunkedSeriesWithLabels(t, []labelpb.ZLabel{{Name: "instance", Value: "a"}, {Name: "job", Value: "api"}}, 1)
		store := memoryBehaviorStore(t, labels.EmptyLabels(), io.NopCloser(bytes.NewReader(chunkedReadFrame(t, &prompb.ChunkedReadResponse{ChunkedSeries: []*prompb.ChunkedSeries{series}}))))
		srv := newStoreSeriesServer(context.Background())
		if err := store.Series(memoryLabelsRequest(false), srv); err != nil {
			t.Fatal(err)
		}
		if len(srv.SeriesSet) != 1 {
			t.Fatalf("got %d series, want 1", len(srv.SeriesSet))
		}
		assertLabelValue(t, srv.SeriesSet[0].Labels, "instance", "a")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "job", "api")
	})

	t.Run("metadata local replica removal remains external-only", func(t *testing.T) {
		exts := labels.FromStrings("cluster", "external", "external_only", "value", "replica", "external-replica")
		client := memoryBehaviorRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString(`{"status":"success","data":[{"job":"api","cluster":"local","replica":"local-replica"}]}`))}, nil
		})
		store := memoryBehaviorStoreWithClient(t, exts, client)
		srv := newStoreSeriesServer(context.Background())
		req := memoryLabelsRequest(true)
		req.WithoutReplicaLabels = []string{"replica"}
		if err := store.Series(req, srv); err != nil {
			t.Fatal(err)
		}
		if len(srv.SeriesSet) != 1 {
			t.Fatalf("got %d series, want 1", len(srv.SeriesSet))
		}
		assertLabelValue(t, srv.SeriesSet[0].Labels, "cluster", "external")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "replica", "local-replica")
		assertLabelValue(t, srv.SeriesSet[0].Labels, "external_only", "value")
	})

	t.Run("streamed allocation regression", func(t *testing.T) {
		fixture, contentType := memorySeriesFixture(t, "streamed", 10, 1)
		client := memoryBehaviorRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(bytes.NewReader(fixture))}, nil
		})
		store := memoryBehaviorStoreWithClient(t, labels.FromStrings("cluster", "memory-test"), client)
		req := memoryLabelsRequest(false)
		warm := &memorySeriesSink{ctx: context.Background()}
		if err := store.Series(req, warm); err != nil {
			t.Fatal(err)
		}
		if warm.count != 10 || warm.digest == 0 {
			t.Fatalf("warmup count=%d digest=%d", warm.count, warm.digest)
		}
		allocs := testing.AllocsPerRun(10, func() {
			sink := &memorySeriesSink{ctx: context.Background()}
			if err := store.Series(req, sink); err != nil {
				panic(err)
			}
			if sink.count != warm.count || sink.chunks != warm.chunks || sink.digest != warm.digest {
				panic("handler output differs from warmup")
			}
		})
		if allocs > 290 {
			t.Fatalf("streamed handler allocated %.0f objects/op, want <= 290 (step-1 fixture baseline: 312)", allocs)
		}
	})
}

func memoryBehaviorStore(t *testing.T, ext labels.Labels, body io.ReadCloser) storepb.StoreServer {
	t.Helper()
	return memoryBehaviorStoreWithClient(t, ext, memoryBehaviorRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{streamedProtoContentType}}, Body: body}, nil
	}))
}

func memoryBehaviorStoreWithClient(t *testing.T, ext labels.Labels, client promclient.HTTPClient) storepb.StoreServer {
	t.Helper()
	base, err := url.Parse("http://prometheus.test")
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewPrometheusStore(nil, prometheus.NewRegistry(), promclient.NewClient(client, nil, ""), base, component.Sidecar,
		func() labels.Labels { return ext }, func() (int64, int64) { return 0, -1 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func memoryLabelsRequest(metadata bool) *storepb.SeriesRequest {
	return &storepb.SeriesRequest{Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"}}, MinTime: 0, MaxTime: 1, SkipChunks: metadata}
}

func assertLabelValue(t *testing.T, got []labelpb.ZLabel, name, value string) {
	t.Helper()
	for _, label := range got {
		if label.Name == name {
			if label.Value != value {
				t.Fatalf("label %q=%q, want %q", name, label.Value, value)
			}
			return
		}
	}
	t.Fatalf("label %q missing from %v", name, got)
}

func assertNoLabel(t *testing.T, got []labelpb.ZLabel, name string) {
	t.Helper()
	for _, label := range got {
		if label.Name == name {
			t.Fatalf("unexpected label %q in %v", name, got)
		}
	}
}

type memoryBehaviorRoundTripper func(*http.Request) (*http.Response, error)

func (f memoryBehaviorRoundTripper) Do(req *http.Request) (*http.Response, error) { return f(req) }
