// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
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

	for _, wr := range writes {
		require.NoError(t, writeHADedupRequest(t, m, capnp, 0, wr.series))
	}
}

func writeHADedupRequest(t *testing.T, s TenantStorage, capnp bool, tooFarInFuture time.Duration, series []prompb.TimeSeries) error {
	t.Helper()

	if !capnp {
		before := make([][]labelpb.ZLabel, 0, len(series))
		for _, s := range series {
			before = append(before, labelpb.DeepCopy(s.Labels))
		}
		err := NewWriter(log.NewNopLogger(), s, &WriterOptions{TooFarInFutureTimeWindow: int64(tooFarInFuture)}).Write(context.Background(), tenancy.DefaultTenant, series)
		for i, s := range series {
			require.Equal(t, before[i], s.Labels, "request labels must not be modified")
		}
		return err
	}

	capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, series)
	require.NoError(t, err)
	syms, err := capnpReq.Symbols()
	require.NoError(t, err)
	data, err := capnpReq.Data()
	require.NoError(t, err)
	req, err := writecapnp.NewRequest(data.At(0), syms, tenancy.DefaultTenant)
	require.NoError(t, err)
	defer func() { require.NoError(t, req.Close()) }()
	return NewCapNProtoWriter(log.NewNopLogger(), s, &CapNProtoWriterOptions{TooFarInFutureTimeWindow: int64(tooFarInFuture)}).Write(context.Background(), req)
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

			tracker, err := m.TenantHADedupTracker(tenancy.DefaultTenant)
			require.NoError(t, err)
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

			tracker, err := m.TenantHADedupTracker(tenancy.DefaultTenant)
			require.NoError(t, err)
			require.Nil(t, tracker)
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

// failingAppendStorage fails float and histogram appends at failTs like the TSDB rejecting a sample.
type failingAppendStorage struct {
	*MultiTSDB
	failTs int64
}

func (s failingAppendStorage) TenantAppendable(tenantID string) (Appendable, error) {
	a, err := s.MultiTSDB.TenantAppendable(tenantID)
	return failingAppendable{Appendable: a, failTs: s.failTs}, err
}

type failingAppendable struct {
	Appendable
	failTs int64
}

func (a failingAppendable) Appender(ctx context.Context) (storage.Appender, error) {
	app, err := a.Appendable.Appender(ctx)
	if err != nil {
		return nil, err
	}
	return failingAppender{Appender: app, getRef: app.(storage.GetRef), failTs: a.failTs}, nil
}

type failingAppender struct {
	storage.Appender
	getRef storage.GetRef
	failTs int64
}

func (a failingAppender) GetRef(lset labels.Labels, hash uint64) (storage.SeriesRef, labels.Labels) {
	return a.getRef.GetRef(lset, hash)
}

func (a failingAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if t == a.failTs {
		return 0, storage.ErrOutOfOrderSample
	}
	return a.Appender.Append(ref, l, t, v)
}

func (a failingAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if t == a.failTs {
		return 0, storage.ErrOutOfOrderSample
	}
	return a.Appender.AppendHistogram(ref, l, t, h, fh)
}

func TestWriterHADedupRejectedSamples(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Hour).UnixMilli()
	year := (365 * 24 * time.Hour).Milliseconds()
	sample := func(replica string, ts int64, hist bool) []prompb.TimeSeries {
		series := prompb.TimeSeries{Labels: withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}}, replica)}
		if hist {
			series.Histograms = []prompb.Histogram{prompb.HistogramToHistogramProto(ts, tsdbutil.GenerateTestHistogram(1))}
		} else {
			series.Samples = []prompb.Sample{{Value: 1, Timestamp: ts}}
		}
		return []prompb.TimeSeries{series}
	}

	for _, tcase := range []struct {
		name     string
		hist     bool
		failTs   int64
		writes   []haDedupWrite
		expected []int64
	}{
		{
			name: "too far in the future",
			writes: []haDedupWrite{
				{replica: "prometheus-0", ts: base},
				{replica: "prometheus-1", ts: base + year},
				{replica: "prometheus-0", ts: base + 15000},
			},
			expected: []int64{base, base + 15000},
		},
		{
			name:   "rejected by the TSDB after a failover",
			failTs: base + 40000,
			writes: []haDedupWrite{
				{replica: "prometheus-0", ts: base},
				{replica: "prometheus-0", ts: base + 15000},
				{replica: "prometheus-1", ts: base + 40000},
				{replica: "prometheus-0", ts: base + 30000},
				{replica: "prometheus-1", ts: base + 45000},
			},
			expected: []int64{base, base + 15000, base + 30000},
		},
		{
			name: "histogram too far in the future",
			hist: true,
			writes: []haDedupWrite{
				{replica: "prometheus-0", ts: base},
				{replica: "prometheus-1", ts: base + year},
				{replica: "prometheus-0", ts: base + 15000},
			},
			expected: []int64{base, base + 15000},
		},
		{
			name:   "histogram rejected by the TSDB after a failover",
			hist:   true,
			failTs: base + 40000,
			writes: []haDedupWrite{
				{replica: "prometheus-0", ts: base},
				{replica: "prometheus-0", ts: base + 15000},
				{replica: "prometheus-1", ts: base + 40000},
				{replica: "prometheus-0", ts: base + 30000},
				{replica: "prometheus-1", ts: base + 45000},
			},
			expected: []int64{base, base + 15000, base + 30000},
		},
		{
			name:   "rejected first sample of a series",
			failTs: base,
			writes: []haDedupWrite{
				{replica: "prometheus-0", ts: base},
				{replica: "prometheus-1", ts: base + 5000},
				{replica: "prometheus-0", ts: base + 15000},
			},
			expected: []int64{base + 5000},
		},
	} {
		for _, capnp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/capnp=%v", tcase.name, capnp), func(t *testing.T) {
				t.Parallel()

				m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
				s := failingAppendStorage{MultiTSDB: m, failTs: tcase.failTs}
				for _, wr := range tcase.writes {
					err := writeHADedupRequest(t, s, capnp, time.Minute, sample(wr.replica, wr.ts, tcase.hist))
					if wr.ts == base+year || wr.ts == tcase.failTs {
						require.Error(t, err)
						continue
					}
					require.NoError(t, err)
				}

				expected := storedSeries{floats: tcase.expected}
				if tcase.hist {
					expected = storedSeries{histograms: tcase.expected}
				}
				require.Equal(t, map[string]storedSeries{`{__name__="up"}`: expected}, readTenantSeries(t, m))
			})
		}
	}
}

// All writers of a dedup-enabled tenant must set the replica label. A series without it is written as it is,
// even if another replica writes the same series with the label.
func TestWriterHADedupMissingReplicaLabel(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Hour).UnixMilli()
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "up"}}

	for _, capnp := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			m := newHADedupMultiTSDB(t, reg, labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
			writeHADedupRequests(t, m, capnp, []haDedupWrite{
				{series: []prompb.TimeSeries{{Labels: withReplicaLabel(lbls, "prometheus-0"), Samples: []prompb.Sample{{Value: 1, Timestamp: base}}}}},
				{series: []prompb.TimeSeries{{Labels: lbls, Samples: []prompb.Sample{{Value: 1, Timestamp: base + 5000}}}}},
			})

			require.Equal(t, map[string]storedSeries{
				`{__name__="up"}`: {floats: []int64{base, base + 5000}},
			}, readTenantSeries(t, m))
			require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP thanos_receive_ha_dedup_passthrough_total Total number of series written without HA deduplication, by reason.
# TYPE thanos_receive_ha_dedup_passthrough_total counter
thanos_receive_ha_dedup_passthrough_total{reason="no_label",tenant=%q} 1
thanos_receive_ha_dedup_passthrough_total{reason="replica_table_full",tenant=%q} 0
`, tenancy.DefaultTenant, tenancy.DefaultTenant)), "thanos_receive_ha_dedup_passthrough_total"))
		})
	}
}

func TestMultiTSDBHADedupTrackerCreatedWithTenant(t *testing.T) {
	t.Parallel()

	m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))

	_, err := m.TenantHADedupTracker("unknown")
	require.ErrorIs(t, err, tsdb.ErrNotReady)

	app, err := m.TenantAppendable("foo")
	require.NoError(t, err)
	require.NotNil(t, app.(*ReadyStorage).Get())

	tenant := m.testGetTenant("foo")
	require.NotNil(t, tenant.haDedupTracker(), "a ready tenant must have its tracker")

	tracker, err := m.TenantHADedupTracker("foo")
	require.NoError(t, err)
	require.Same(t, tenant.haDedupTracker(), tracker)
}

type notReadyHADedupStorage struct {
	TenantStorage
}

func (notReadyHADedupStorage) TenantHADedupTracker(string) (*hadedup.Tracker, error) {
	return nil, tsdb.ErrNotReady
}

func TestWriterHADedupTrackerNotReady(t *testing.T) {
	t.Parallel()

	m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"))
	s := notReadyHADedupStorage{TenantStorage: m}
	series := haReplicaScrape("prometheus-0", time.Now().Add(-time.Hour).UnixMilli())

	require.ErrorIs(t, NewWriter(log.NewNopLogger(), s, &WriterOptions{}).Write(context.Background(), tenancy.DefaultTenant, series), tsdb.ErrNotReady)

	capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, series)
	require.NoError(t, err)
	syms, err := capnpReq.Symbols()
	require.NoError(t, err)
	data, err := capnpReq.Data()
	require.NoError(t, err)
	req, err := writecapnp.NewRequest(data.At(0), syms, tenancy.DefaultTenant)
	require.NoError(t, err)
	require.ErrorIs(t, NewCapNProtoWriter(log.NewNopLogger(), s, &CapNProtoWriterOptions{}).Write(context.Background(), req), tsdb.ErrNotReady)
	require.NoError(t, req.Close())

	require.Empty(t, readTenantSeries(t, m))
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
	require.False(t, s1.accept(0, 6000, false))
	require.False(t, s1.acceptExemplars(7))
	require.True(t, s0.acceptExemplars(7))

	// A failed append of an accepted sample reverts its ownership change.
	require.True(t, s1.accept(0, 5000+(365*24*time.Hour).Milliseconds(), false))
	require.True(t, tracker.IsOwner(7, r1))
	s1.appended(0)
	require.True(t, tracker.IsOwner(7, r0))

	// The reference is known from the first sample even if its append fails.
	tracker.Init(8, r0, 15000)
	require.False(t, tracker.Accept(8, r1, 20000, false))
	s2 := haDedupSeries{tracker: tracker, replica: r0}
	require.True(t, s2.accept(8, 0, false))
	s2.appended(0)
	require.False(t, s2.accept(0, 30000, true), "stale marker must be handed over to the live replica")
	require.True(t, tracker.IsOwner(8, r1))
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
