// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage/remote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

type resortE2EExternal struct {
	name    string
	labels  labels.Labels
	without [][]string
}

// Own label names are B, Job, Zone, __name__, _x, a, instance, job, le, zone and zy.
var resortE2EExternals = []resortE2EExternal{
	{name: "none", labels: labels.EmptyLabels(), without: [][]string{nil, {"replica"}}},
	{name: "before all own names", labels: labels.FromStrings("A_first", "1"), without: [][]string{nil, {"A_first"}}},
	{name: "between upper case own names", labels: labels.FromStrings("Region", "eu"), without: [][]string{nil}},
	{name: "between lower case own names", labels: labels.FromStrings("k_mid", "m"), without: [][]string{nil, {"unknown"}}},
	{name: "after all own names", labels: labels.FromStrings("zzzz", "z"), without: [][]string{nil, {"zzzz"}}},
	{
		name:    "typical",
		labels:  labels.FromStrings("cluster", "c1", "prometheus_replica", "r0", "region", "eu"),
		without: [][]string{nil, {"prometheus_replica", "replica"}, {"region", "prometheus_replica"}},
	},
	{
		name:    "spread",
		labels:  labels.FromStrings("A_first", "1", "Region", "eu", "k_mid", "m", "replica", "r0", "zzzz", "z"),
		without: [][]string{nil, {"replica"}, {"zzzz", "k_mid"}, {"A_first", "Region", "k_mid", "replica", "zzzz"}},
	},
}

var resortE2EMatchers = [][]storepb.LabelMatcher{
	{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}},
	{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}},
	{{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "a"}},
	{{Type: storepb.LabelMatcher_EQ, Name: "zone", Value: ""}, {Type: storepb.LabelMatcher_RE, Name: "__name__", Value: "metric_0.*"}},
	{{Type: storepb.LabelMatcher_NEQ, Name: "zone", Value: ""}, {Type: storepb.LabelMatcher_NRE, Name: "__name__", Value: "big"}},
	{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: "big|up|Up"}},
	{{Type: storepb.LabelMatcher_RE, Name: "B", Value: ".*"}, {Type: storepb.LabelMatcher_NEQ, Name: "Job", Value: "A"}, {Type: storepb.LabelMatcher_RE, Name: "__name__", Value: "metric_1.|_hidden"}},
}

func resortE2ELabelSets() (small, big, blockOnly []labels.Labels) {
	names := []string{"up", "Up", "_hidden", "http_requests_total"}
	for i := range 30 {
		names = append(names, fmt.Sprintf("metric_%02d", i))
	}
	variants := [][]string{
		{},
		{"job", "a"},
		{"job", "a", "zone", "x"},
		{"job", "a", "zone", "x", "zy", "1"},
		{"job", "a", "zone", "y"},
		{"job", "a", "zy", "1"},
		{"job", "b"},
		{"zone", "x"},
		{"B", "1"},
		{"B", "1", "job", "a"},
		{"Job", "A"},
		{"Job", "A", "job", "a"},
		{"Zone", "X", "zone", "x"},
		{"_x", "1"},
		{"_x", "1", "a", "1"},
		{"a", "1"},
		{"a", "1", "instance", "i", "job", "a"},
		{"a", "1", "instance", "i", "job", "a", "le", "0.5"},
		{"instance", "i"},
		{"instance", "i", "le", "+Inf"},
	}
	for _, name := range names {
		for _, variant := range variants {
			small = append(small, labels.FromStrings(append([]string{"__name__", name}, variant...)...))
		}
		blockOnly = append(blockOnly, labels.FromStrings("__name__", name, "job", "a", "le", "block"))
	}
	big = []labels.Labels{
		labels.FromStrings("__name__", "big", "job", "a"),
		labels.FromStrings("__name__", "big", "job", "a", "zone", "x"),
		labels.FromStrings("__name__", "big", "job", "a", "zone", "x", "zy", "1"),
	}
	return small, big, blockOnly
}

type resortE2EStats struct {
	requests, series, frames, multiFrameSeries, multiChunkFrames int
	framesWithExternalLabels, rawOrderViolations                 int
	rawOrderExample                                              string
	maxHeld                                                      int
}

type resortHeldLogger struct {
	mu   sync.Mutex
	held int
}

func (l *resortHeldLogger) Log(keyvals ...any) error {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if keyvals[i] == "max_held_series" {
			l.mu.Lock()
			l.held = max(l.held, keyvals[i+1].(int))
			l.mu.Unlock()
		}
	}
	return nil
}

// resortE2ERawOrder reads the response as Prometheus sends it and fails if the series,
// without the labels Prometheus merged in from ext, are not sorted.
func resortE2ERawOrder(t *testing.T, p *PrometheusStore, ext labels.Labels, r *storepb.SeriesRequest, stats *resortE2EStats) {
	t.Helper()
	q := &prompb.Query{StartTimestampMs: r.MinTime, EndTimestampMs: r.MaxTime}
	for _, m := range r.Matchers {
		q.Matchers = append(q.Matchers, &prompb.LabelMatcher{Type: prompb.LabelMatcher_Type(m.Type), Name: m.Name, Value: m.Value})
	}
	resp, err := p.startPromRemoteRead(context.Background(), q)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, resp.Body.Close()) }()
	testutil.Assert(t, strings.HasPrefix(resp.Header.Get("Content-Type"), streamedProtoContentType), "unexpected content type %s", resp.Header.Get("Content-Type"))

	stream := remote.NewChunkedReader(resp.Body, config.DefaultChunkedReadLimit, nil)
	var prevRaw, prevOwn labels.Labels
	for {
		res := &prompb.ChunkedReadResponse{}
		err := stream.NextProto(res)
		if err == io.EOF {
			return
		}
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(res.ChunkedSeries))
		series := res.ChunkedSeries[0]
		labelpb.ReAllocZLabelsStrings(&series.Labels)
		raw := labelpb.ZLabelsToPromLabels(series.Labels)
		ownBuilder := labels.NewBuilder(raw)
		merged := false
		ext.Range(func(l labels.Label) {
			if raw.Get(l.Name) == l.Value {
				ownBuilder.Del(l.Name)
				merged = true
			}
		})
		own := ownBuilder.Labels()

		stats.frames++
		if merged {
			stats.framesWithExternalLabels++
		}
		if len(series.Chunks) > 1 {
			stats.multiChunkFrames++
		}
		switch {
		case stats.frames == 1 || !labels.Equal(prevOwn, own):
			stats.series++
		case stats.frames > 1:
			stats.multiFrameSeries++
		}
		if labels.Compare(prevOwn, own) > 0 {
			t.Fatalf("Prometheus returned series %s after %s (ext %s, matchers %v)", own, prevOwn, ext, r.Matchers)
		}
		if labels.Compare(prevRaw, raw) > 0 {
			stats.rawOrderViolations++
			if stats.rawOrderExample == "" || len(prevRaw.String())+len(raw.String()) < len(stats.rawOrderExample) {
				stats.rawOrderExample = fmt.Sprintf("%s before %s", prevRaw, raw)
			}
		}
		prevRaw, prevOwn = raw, own
	}
}

func resortE2ESeries(t *testing.T, p *PrometheusStore, r *storepb.SeriesRequest) ([]storepb.Series, error) {
	t.Helper()
	srv := newStoreSeriesServer(context.Background())
	request := *r
	err := p.Series(&request, srv)
	testutil.Equals(t, []string(nil), srv.Warnings)
	return srv.SeriesSet, err
}

func resortE2EAssertEqual(t *testing.T, want, got []storepb.Series, context string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: got %d series, want %d", context, len(got), len(want))
	}
	for i := range want {
		wantLabels, gotLabels := labelpb.ZLabelsToPromLabels(want[i].Labels), labelpb.ZLabelsToPromLabels(got[i].Labels)
		if !labels.Equal(wantLabels, gotLabels) {
			t.Fatalf("%s: series %d is %s, want %s", context, i, gotLabels, wantLabels)
		}
		if len(want[i].Chunks) != len(got[i].Chunks) {
			t.Fatalf("%s: series %d %s has %d chunks, want %d", context, i, gotLabels, len(got[i].Chunks), len(want[i].Chunks))
		}
		for j := range want[i].Chunks {
			w, g := want[i].Chunks[j], got[i].Chunks[j]
			if w.MinTime != g.MinTime || w.MaxTime != g.MaxTime || w.Raw.Type != g.Raw.Type || w.Raw.Hash != g.Raw.Hash || !bytes.Equal(w.Raw.Data, g.Raw.Data) {
				t.Fatalf("%s: series %d %s chunk %d differs", context, i, gotLabels, j)
			}
		}
	}
}

func TestPrometheusStore_Series_StreamingSort_e2e(t *testing.T) {
	t.Parallel()

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	ctx := t.Context()
	now := time.Now()
	baseT := timestamp.FromTime(now.Add(-10*time.Minute)) / 1000 * 1000
	blockMinT, blockMaxT := timestamp.FromTime(now.Add(-6*time.Hour)), timestamp.FromTime(now.Add(-5*time.Hour))
	small, big, blockOnly := resortE2ELabelSets()

	// 300 samples make three chunks per series; the big series exceed the 1MB frame limit.
	rng := rand.New(rand.NewSource(3))
	app := p.Appender()
	for _, lset := range small {
		for i := range 300 {
			_, err := app.Append(0, lset, baseT+int64(i), float64(i))
			testutil.Ok(t, err)
		}
	}
	testutil.Ok(t, app.Commit())
	for _, lset := range big {
		app = p.Appender()
		for i := range 250000 {
			_, err := app.Append(0, lset, baseT+int64(i), rng.Float64())
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())
	}
	// Half of the series also live in a block, so that Prometheus merges two sorted sources.
	blockSeries := slices.Clone(blockOnly)
	for i := 0; i < len(small); i += 2 {
		blockSeries = append(blockSeries, small[i])
	}
	_, err = e2eutil.CreateBlock(ctx, p.Dir(), blockSeries, 150, blockMinT, blockMaxT, labels.EmptyLabels(), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, p.Start(ctx, log.NewNopLogger()))

	newStore := func(t *testing.T, ext labels.Labels, logger log.Logger, options ...PrometheusStoreOption) *PrometheusStore {
		t.Helper()
		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)
		store, err := NewPrometheusStore(logger, nil, promclient.NewDefaultClient(), u, component.Sidecar,
			func() labels.Labels { return ext }, func() (int64, int64) { return 0, math.MaxInt64 }, nil, options...)
		testutil.Ok(t, err)
		return store
	}
	request := func(matchers []storepb.LabelMatcher, without []string) *storepb.SeriesRequest {
		return &storepb.SeriesRequest{MinTime: blockMinT, MaxTime: timestamp.FromTime(now.Add(time.Hour)), Matchers: matchers, WithoutReplicaLabels: without, ResponseBatchSize: 64}
	}

	compare := func(t *testing.T, external resortE2EExternal) resortE2EStats {
		t.Helper()
		var stats resortE2EStats
		held := &resortHeldLogger{}
		buffered := newStore(t, external.labels, nil)
		streamed := newStore(t, external.labels, held, WithPrometheusStoreStreamingSort(true))
		for _, matchers := range resortE2EMatchers {
			resortE2ERawOrder(t, buffered, external.labels, request(matchers, nil), &stats)
			for _, without := range external.without {
				name := fmt.Sprintf("matchers %v without %v", matchers, without)
				want, err := resortE2ESeries(t, buffered, request(matchers, without))
				testutil.Ok(t, err)
				testutil.Assert(t, len(want) > 0, "%s: no series", name)
				got, err := resortE2ESeries(t, streamed, request(matchers, without))
				testutil.Ok(t, err)
				resortE2EAssertEqual(t, want, got, name)
				again, err := resortE2ESeries(t, buffered, request(matchers, without))
				testutil.Ok(t, err)
				resortE2EAssertEqual(t, want, again, name+" (buffered twice)")
				stats.requests++
			}
		}
		stats.maxHeld = held.held
		testutil.Equals(t, map[string]float64{}, resortUnstreamable(t, buffered))
		testutil.Equals(t, map[string]float64{}, resortUnstreamable(t, streamed))
		testutil.Assert(t, stats.multiFrameSeries > 0, "no series spans several frames")
		testutil.Assert(t, stats.multiChunkFrames > 0, "no frame carries several chunks")
		t.Logf("%+v", stats)
		return stats
	}

	for _, external := range resortE2EExternals {
		t.Run("external labels known to the sidecar only/"+external.name, func(t *testing.T) {
			stats := compare(t, external)
			testutil.Equals(t, 0, stats.framesWithExternalLabels)
			testutil.Equals(t, 0, stats.rawOrderViolations)
		})
	}

	t.Run("violations are rejected", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			ext      labels.Labels
			matchers []storepb.LabelMatcher
			without  []string
			want     string
			reason   string
		}{
			{
				name:     "external label overrides an own label",
				ext:      labels.FromStrings("job", "override"),
				matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}},
				want:     `series {B="1", __name__="up", job="a"} has its own value for external label "job"`,
				reason:   "external_label",
			},
			{
				name:     "own label is removed as replica label",
				ext:      labels.FromStrings("region", "eu"),
				matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}},
				without:  []string{"zone"},
				want:     `series {Zone="X", __name__="up", zone="x"} has removed replica label "zone" as its own label`,
				reason:   "replica_label",
			},
			{
				// zone="x" cannot be told apart from a label merged in by Prometheus, so
				// up{job="a",zone="x"} counts as up{job="a"} and seems to arrive too late.
				name:     "external label equals an own label of some series",
				ext:      labels.FromStrings("zone", "x"),
				matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}, {Type: storepb.LabelMatcher_EQ, Name: "job", Value: "a"}, {Type: storepb.LabelMatcher_EQ, Name: "B", Value: ""}, {Type: storepb.LabelMatcher_EQ, Name: "Job", Value: ""}, {Type: storepb.LabelMatcher_EQ, Name: "a", Value: ""}},
				want:     `response is not sorted by the series' own labels, {__name__="up", job="a"} follows {__name__="up", job="a", le="block"}`,
				reason:   "unsorted",
			},
			{
				// up{job="a"} and up{job="a",zy="1"} are adjacent and end up with the same labels.
				name:     "external label equals an own label without disturbing the order",
				ext:      labels.FromStrings("zy", "1"),
				matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}, {Type: storepb.LabelMatcher_EQ, Name: "zone", Value: ""}, {Type: storepb.LabelMatcher_EQ, Name: "le", Value: ""}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				unstreamable := map[string]float64{}
				if tc.reason != "" {
					unstreamable[tc.reason] = 1
				}
				buffered := newStore(t, tc.ext, nil)
				want, err := resortE2ESeries(t, buffered, request(tc.matchers, tc.without))
				testutil.Ok(t, err)
				testutil.Assert(t, len(want) > 0, "no series")
				testutil.Equals(t, unstreamable, resortUnstreamable(t, buffered))
				streamed := newStore(t, tc.ext, nil, WithPrometheusStoreStreamingSort(true))
				got, err := resortE2ESeries(t, streamed, request(tc.matchers, tc.without))
				testutil.Equals(t, unstreamable, resortUnstreamable(t, streamed))
				if tc.want == "" {
					testutil.Ok(t, err)
					resortE2EAssertEqual(t, want, got, tc.name)
					return
				}
				testutil.NotOk(t, err)
				testutil.Equals(t, codes.FailedPrecondition, status.Code(err))
				testutil.Equals(t, "rpc error: code = FailedPrecondition desc = streaming series sort cannot be used for this response: "+tc.want, err.Error())
			})
		}
	})

	// A real sidecar takes its external labels from the Prometheus configuration, and
	// Prometheus merges those into every series it returns.
	for _, external := range resortE2EExternals[1:] {
		t.Run("external labels configured in Prometheus/"+external.name, func(t *testing.T) {
			var cfg strings.Builder
			cfg.WriteString("global:\n  external_labels:\n")
			external.labels.Range(func(l labels.Label) {
				fmt.Fprintf(&cfg, "    %s: %q\n", l.Name, l.Value)
			})
			p.SetConfig(cfg.String())
			testutil.Ok(t, p.Restart(ctx, log.NewNopLogger()))

			stats := compare(t, external)
			testutil.Equals(t, stats.frames, stats.framesWithExternalLabels)
			if stats.rawOrderViolations > 0 {
				t.Logf("frames are not sorted by the labels they carry, for example %s", stats.rawOrderExample)
			}
		})
	}
}
