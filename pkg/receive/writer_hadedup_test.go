// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/receive/hadedup"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

const testHADedupReplicaLabel = "prometheus_replica"

func testHADedupConfig() hadedup.Config {
	return hadedup.Config{
		ReplicaLabel:           testHADedupReplicaLabel,
		FailoverIntervals:      1.5,
		MinFailoverTimeout:     10 * time.Second,
		MaxFailoverTimeout:     5 * time.Minute,
		DefaultFailoverTimeout: time.Minute,
		MaxReplicas:            2,
	}
}

func newHADedupMultiTSDB(t testing.TB, reg prometheus.Registerer, extLabels labels.Labels, options ...MultiTSDBOption) *MultiTSDB {
	m := NewMultiTSDB(openTestRoot(t, t.TempDir()), log.NewNopLogger(), reg, &tsdb.Options{
		MinBlockDuration:      (2 * time.Hour).Milliseconds(),
		MaxBlockDuration:      (2 * time.Hour).Milliseconds(),
		RetentionDuration:     (6 * time.Hour).Milliseconds(),
		NoLockfile:            true,
		MaxExemplars:          1000,
		EnableExemplarStorage: true,
	},
		extLabels,
		"tenant_id",
		nil,
		false,
		false,
		metadata.NoneFunc,
		options...,
	)
	t.Cleanup(m.Close)

	require.NoError(t, m.Flush())
	require.NoError(t, m.Open())
	return m
}

type storedSeries struct {
	floats     []int64
	histograms []int64
	exemplars  []int64
}

// readTenantSeries returns timestamps of all samples, histograms and exemplars of the default tenant by series labels.
func readTenantSeries(t *testing.T, m *MultiTSDB) map[string]storedSeries {
	t.Helper()

	app, err := m.TenantAppendable(tenancy.DefaultTenant)
	require.NoError(t, err)
	rs := app.(*ReadyStorage)

	q, err := rs.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()

	eq, err := rs.ExemplarQuerier(context.Background())
	require.NoError(t, err)

	result := map[string]storedSeries{}
	ss := q.Select(context.Background(), false, nil, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	for ss.Next() {
		var s storedSeries
		it := ss.At().Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				s.floats = append(s.floats, it.AtT())
			default:
				s.histograms = append(s.histograms, it.AtT())
			}
		}
		require.NoError(t, it.Err())

		exs, err := eq.Select(math.MinInt64, math.MaxInt64, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, ss.At().Labels().Get(labels.MetricName))})
		require.NoError(t, err)
		for _, ex := range exs {
			if !labels.Equal(ex.SeriesLabels, ss.At().Labels()) {
				continue
			}
			for _, e := range ex.Exemplars {
				s.exemplars = append(s.exemplars, e.Ts)
			}
		}
		result[ss.At().Labels().String()] = s
	}
	require.NoError(t, ss.Err())
	return result
}

func withReplicaLabel(lbls []labelpb.ZLabel, replica string) []labelpb.ZLabel {
	res := append(slices.Clone(lbls), labelpb.ZLabel{Name: testHADedupReplicaLabel, Value: replica})
	slices.SortFunc(res, func(a, b labelpb.ZLabel) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		}
		return 0
	})
	return res
}

// haReplicaScrape returns the series written by a replica for a single scrape at ts.
func haReplicaScrape(replica string, ts int64) []prompb.TimeSeries {
	return []prompb.TimeSeries{
		{
			Labels:    withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "a"}}, replica),
			Samples:   []prompb.Sample{{Value: 1, Timestamp: ts}},
			Exemplars: []prompb.Exemplar{{Labels: []labelpb.ZLabel{{Name: "trace_id", Value: replica}}, Value: 1, Timestamp: ts}},
		},
		{
			Labels:  withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "b"}}, replica),
			Samples: []prompb.Sample{{Value: 1, Timestamp: ts}},
		},
		{
			Labels:     withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "latency"}}, replica),
			Histograms: []prompb.Histogram{prompb.HistogramToHistogramProto(ts, tsdbutil.GenerateTestHistogram(1))},
		},
	}
}

type haDedupWrite struct {
	replica string
	ts      int64
	series  []prompb.TimeSeries
}

func writeHADedupRequests(t *testing.T, m *MultiTSDB, capnp bool, writes []haDedupWrite) {
	t.Helper()

	if !capnp {
		w := NewWriter(log.NewNopLogger(), m, &WriterOptions{})
		for _, wr := range writes {
			before := make([][]labelpb.ZLabel, 0, len(wr.series))
			for _, s := range wr.series {
				before = append(before, labelpb.DeepCopy(s.Labels))
			}
			require.NoError(t, w.Write(context.Background(), tenancy.DefaultTenant, wr.series))
			for i, s := range wr.series {
				require.Equal(t, before[i], s.Labels, "request labels must not be modified")
			}
		}
		return
	}

	w := NewCapNProtoWriter(log.NewNopLogger(), m, &CapNProtoWriterOptions{})
	for _, wr := range writes {
		capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, wr.series)
		require.NoError(t, err)
		syms, err := capnpReq.Symbols()
		require.NoError(t, err)
		data, err := capnpReq.Data()
		require.NoError(t, err)
		req, err := writecapnp.NewRequest(data.At(0), syms, tenancy.DefaultTenant)
		require.NoError(t, err)
		require.NoError(t, w.Write(context.Background(), req))
		require.NoError(t, req.Close())
	}
}

func TestWriterHADedup(t *testing.T) {
	t.Parallel()

	const (
		interval = int64(15000)
		offset   = int64(5000)
	)
	base := time.Now().Add(-time.Hour).UnixMilli()
	base -= base % interval

	var (
		writes   []haDedupWrite
		expected []int64
	)
	// Both replicas scrape every 15s with a 5s offset, prometheus-0 stops after 10 scrapes.
	for i := range int64(15) {
		ts0 := base + i*interval
		ts1 := ts0 + offset
		if i < 10 {
			writes = append(writes, haDedupWrite{replica: "prometheus-0", ts: ts0, series: haReplicaScrape("prometheus-0", ts0)})
			expected = append(expected, ts0)
		}
		writes = append(writes, haDedupWrite{replica: "prometheus-1", ts: ts1, series: haReplicaScrape("prometheus-1", ts1)})
		// prometheus-1 takes over once its samples are more than 1.5 intervals newer than the last one of prometheus-0.
		if ts1 > base+9*interval+interval*3/2 {
			expected = append(expected, ts1)
		}
	}
	// Series without the replica label and series of replicas over the limit are written as they are.
	passthrough := []prompb.TimeSeries{
		{
			Labels:  []labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "c"}},
			Samples: []prompb.Sample{{Value: 1, Timestamp: base}},
		},
		{
			Labels:  withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "a"}}, "prometheus-2"),
			Samples: []prompb.Sample{{Value: 1, Timestamp: base}},
		},
	}
	writes = append(writes, haDedupWrite{series: passthrough})

	expectedSeries := map[string]storedSeries{
		`{__name__="up", instance="a"}`:                                    {floats: expected, exemplars: expected},
		`{__name__="up", instance="b"}`:                                    {floats: expected},
		`{__name__="latency"}`:                                             {histograms: expected},
		`{__name__="up", instance="c"}`:                                    {floats: []int64{base}},
		`{__name__="up", instance="a", prometheus_replica="prometheus-2"}`: {floats: []int64{base}},
	}

	results := map[string]map[string]storedSeries{}
	for _, capnp := range []bool{false, true} {
		name := "proto_writer"
		if capnp {
			name = "capnproto_writer"
		}
		t.Run(name, func(t *testing.T) {
			m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
			writeHADedupRequests(t, m, capnp, writes)

			got := readTenantSeries(t, m)
			require.Equal(t, expectedSeries, got)
			results[name] = got

			tracker := m.TenantHADedupTracker(tenancy.DefaultTenant)
			require.NotNil(t, tracker)
		})
	}
	require.Equal(t, results["proto_writer"], results["capnproto_writer"])
}

func TestWriterHADedupDisabled(t *testing.T) {
	t.Parallel()

	ts := time.Now().Add(-time.Hour).UnixMilli()
	writes := []haDedupWrite{
		{series: haReplicaScrape("prometheus-0", ts)},
		{series: haReplicaScrape("prometheus-1", ts+5000)},
	}

	for _, capnp := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
			m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"))
			writeHADedupRequests(t, m, capnp, writes)

			require.Nil(t, m.TenantHADedupTracker(tenancy.DefaultTenant))
			got := readTenantSeries(t, m)
			require.Len(t, got, 6)
			require.Contains(t, got, `{__name__="up", instance="a", prometheus_replica="prometheus-0"}`)
			require.Contains(t, got, `{__name__="up", instance="a", prometheus_replica="prometheus-1"}`)
		})
	}
}

func TestWriterHADedupStaleHandover(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Hour).UnixMilli()
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "up"}}
	sample := func(replica string, ts int64, v float64) haDedupWrite {
		return haDedupWrite{series: []prompb.TimeSeries{{
			Labels:  withReplicaLabel(lbls, replica),
			Samples: []prompb.Sample{{Value: v, Timestamp: ts}},
		}}}
	}
	stale := math.Float64frombits(value.StaleNaN)

	for _, capnp := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
			m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
			writeHADedupRequests(t, m, capnp, []haDedupWrite{
				sample("prometheus-0", base, 1),
				sample("prometheus-1", base+5000, 1),
				sample("prometheus-0", base+15000, 1),
				sample("prometheus-1", base+20000, 1),
				// The target vanished on prometheus-0 only: its stale marker is dropped.
				sample("prometheus-0", base+30000, stale),
				sample("prometheus-1", base+35000, 1),
				// Then it vanished on prometheus-1 too.
				sample("prometheus-1", base+50000, stale),
			})

			got := readTenantSeries(t, m)
			require.Equal(t, map[string]storedSeries{
				`{__name__="up"}`: {floats: []int64{base, base + 15000, base + 35000, base + 50000}},
			}, got)
		})
	}
}

func TestHADedupWriterPrepare(t *testing.T) {
	tracker := hadedup.NewTracker(testHADedupConfig(), prometheus.NewRegistry())
	lbls := []labelpb.ZLabel{
		{Name: "__name__", Value: "up"},
		{Name: "instance", Value: "a"},
		{Name: testHADedupReplicaLabel, Value: "prometheus-0"},
		{Name: "zone", Value: "z"},
	}
	stripped := []labelpb.ZLabel{
		{Name: "__name__", Value: "up"},
		{Name: "instance", Value: "a"},
		{Name: "zone", Value: "z"},
	}
	noReplica := []labelpb.ZLabel{{Name: "__name__", Value: "up"}}

	t.Run("copy", func(t *testing.T) {
		w := haDedupWriter{tracker: tracker}
		orig := slices.Clone(lbls)

		got, s := w.prepare(lbls, false)
		require.Equal(t, stripped, got)
		require.NotNil(t, s.tracker)
		require.Equal(t, orig, lbls)

		got, s = w.prepare(noReplica, false)
		require.Equal(t, noReplica, got)
		require.Nil(t, s.tracker)

		require.Zero(t, testing.AllocsPerRun(100, func() {
			got, _ = w.prepare(lbls, false)
		}))
		require.Equal(t, stripped, got)
	})

	t.Run("in place", func(t *testing.T) {
		w := haDedupWriter{tracker: tracker}
		buf := slices.Clone(lbls)

		got, s := w.prepare(buf, true)
		require.Equal(t, stripped, got)
		require.NotNil(t, s.tracker)
		require.Equal(t, &buf[0], &got[0])

		var gotInPlace []labelpb.ZLabel
		require.Zero(t, testing.AllocsPerRun(100, func() {
			buf = append(buf[:0], lbls...)
			gotInPlace, _ = w.prepare(buf, true)
		}))
		require.Equal(t, stripped, gotInPlace)
	})

	t.Run("disabled", func(t *testing.T) {
		var w haDedupWriter
		got, s := w.prepare(lbls, true)
		require.Equal(t, lbls, got)
		require.True(t, s.accept(0, 0, false))
		require.True(t, s.acceptExemplars(1))
	})
}

func TestHADedupSeries(t *testing.T) {
	t.Parallel()

	tracker := hadedup.NewTracker(testHADedupConfig(), prometheus.NewRegistry())
	r0, _ := tracker.Replica("prometheus-0")
	r1, _ := tracker.Replica("prometheus-1")

	// New series: the first sample is appended without state and elects its replica.
	s0 := haDedupSeries{tracker: tracker, replica: r0}
	require.True(t, s0.accept(0, 0, false))
	s0.appended(7)
	require.True(t, tracker.IsOwner(7, r0))

	// A failed append returns a zero reference, later samples must still be checked against the tracker.
	s1 := haDedupSeries{tracker: tracker, replica: r1}
	require.False(t, s1.accept(7, 5000, false))
	s1.ref = 7
	require.False(t, s1.accept(0, 6000, false))
	require.False(t, s1.acceptExemplars(7))
	require.True(t, s0.acceptExemplars(7))
}

func BenchmarkWriterHADedup(b *testing.B) {
	for _, dedup := range []bool{false, true} {
		b.Run(fmt.Sprintf("dedup=%v", dedup), func(b *testing.B) {
			var opts []MultiTSDBOption
			if dedup {
				opts = append(opts, WithHADedup(testHADedupConfig()))
			}
			m := newHADedupMultiTSDB(b, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), opts...)
			w := NewWriter(log.NewNopLogger(), m, &WriterOptions{})

			const numSeries = 1000
			replicas := make([][]prompb.TimeSeries, 2)
			for r := range replicas {
				replicas[r] = make([]prompb.TimeSeries, numSeries)
				for i := range numSeries {
					replicas[r][i] = prompb.TimeSeries{
						Labels: withReplicaLabel([]labelpb.ZLabel{
							{Name: "__name__", Value: "test"},
							{Name: "instance", Value: fmt.Sprintf("host-%d", i)},
							{Name: "job", Value: "node"},
						}, fmt.Sprintf("prometheus-%d", r)),
						Samples: []prompb.Sample{{Value: 1}},
					}
				}
			}

			ts := time.Now().Add(-time.Hour).UnixMilli()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ts += 15000
				for r := range replicas {
					for i := range replicas[r] {
						replicas[r][i].Samples[0].Timestamp = ts + int64(r)*5000
					}
					if err := w.Write(context.Background(), tenancy.DefaultTenant, replicas[r]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
