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
	"github.com/pkg/errors"
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
	"go.uber.org/atomic"

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
		// Writer reallocates exemplar label strings in place, and tests write the same series from parallel subtests.
		series = slices.Clone(series)
		before := make([][]labelpb.ZLabel, 0, len(series))
		for i, s := range series {
			series[i].Exemplars = slices.Clone(s.Exemplars)
			for j := range series[i].Exemplars {
				series[i].Exemplars[j].Labels = labelpb.DeepCopy(s.Exemplars[j].Labels)
			}
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

func TestWriterHADedupStaleHandoverAfterTakeover(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Hour).UnixMilli()
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "up"}}
	sample := func(replica string, ts int64, v float64) haDedupWrite {
		return haDedupWrite{series: []prompb.TimeSeries{{
			Labels:  withReplicaLabel(lbls, replica),
			Samples: []prompb.Sample{{Value: v, Timestamp: base + ts}},
		}}}
	}
	stale := math.Float64frombits(value.StaleNaN)

	for _, tcase := range []struct {
		name     string
		writes   []haDedupWrite
		expected []int64
	}{
		{
			name: "live old owner",
			writes: []haDedupWrite{
				sample("prometheus-1", 0, 1), sample("prometheus-1", 10000, 1), sample("prometheus-1", 20000, 1),
				sample("prometheus-0", 25000, 1), sample("prometheus-0", 30000, stale),
				sample("prometheus-1", 30000, 1), sample("prometheus-1", 40000, 1),
			},
			expected: []int64{0, 10000, 20000, 25000, 30000, 40000},
		},
		{
			name: "fresher third replica",
			writes: []haDedupWrite{
				sample("prometheus-1", 0, 1), sample("prometheus-1", 10000, 1), sample("prometheus-1", 20000, 1),
				sample("prometheus-2", 25000, 1), sample("prometheus-0", 26000, 1), sample("prometheus-0", 36000, stale),
				sample("prometheus-2", 35000, 1), sample("prometheus-2", 45000, 1),
			},
			expected: []int64{0, 10000, 20000, 26000, 35000, 45000},
		},
		{
			name: "stale old owner",
			writes: []haDedupWrite{
				sample("prometheus-1", 0, 1), sample("prometheus-1", 10000, 1), sample("prometheus-1", 20000, stale),
				sample("prometheus-0", 25000, 1), sample("prometheus-0", 35000, stale),
			},
			expected: []int64{0, 10000, 20000, 25000, 35000},
		},
		{
			name: "failover sample stale",
			writes: []haDedupWrite{
				sample("prometheus-2", 0, 1), sample("prometheus-2", 10000, 1), sample("prometheus-1", 30000, stale),
				sample("prometheus-0", 35000, 1), sample("prometheus-0", 40000, stale),
			},
			expected: []int64{0, 10000, 30000, 35000, 40000},
		},
		{
			name: "old owner resumes after stale",
			writes: []haDedupWrite{
				sample("prometheus-1", 0, 1), sample("prometheus-1", 10000, 1), sample("prometheus-1", 20000, stale),
				sample("prometheus-1", 30000, 1), sample("prometheus-0", 35000, 1), sample("prometheus-0", 40000, stale),
				sample("prometheus-1", 45000, 1),
			},
			expected: []int64{0, 10000, 20000, 30000, 35000, 45000},
		},
		{
			name: "first sample stale",
			writes: []haDedupWrite{
				sample("prometheus-1", 0, stale), sample("prometheus-0", 5000, 1), sample("prometheus-0", 10000, stale),
			},
			expected: []int64{0, 5000, 10000},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			t.Parallel()
			for _, capnp := range []bool{false, true} {
				t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
					t.Parallel()
					cfg := testHADedupConfig()
					cfg.MaxReplicas = 3
					m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(cfg))
					writeHADedupRequests(t, m, capnp, tcase.writes)
					expected := make([]int64, len(tcase.expected))
					for i, ts := range tcase.expected {
						expected[i] = base + ts
					}
					require.Equal(t, map[string]storedSeries{
						`{__name__="up"}`: {floats: expected},
					}, readTenantSeries(t, m))
				})
			}
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
	exemplar := func(ts int64) []prompb.Exemplar {
		return []prompb.Exemplar{{Labels: []labelpb.ZLabel{{Name: "trace_id", Value: "a"}}, Value: 1, Timestamp: ts}}
	}

	for _, tcase := range []struct {
		name              string
		hist              bool
		failTs            int64
		writes            []haDedupWrite
		expected          []int64
		expectedExemplars []int64
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
				{replica: "prometheus-1", ts: base},
				{replica: "prometheus-0", ts: base + 5000},
				{replica: "prometheus-1", ts: base + 15000},
			},
			expected: []int64{base + 5000},
		},
		{
			// Accepted limit: the rejected sample's series is forgotten, so exemplars of the rejected replica that
			// follow in the request are appended. Those of the rejected series itself are not, as the failed append
			// leaves no reference to append them to.
			name:   "exemplars of a rejected failover sample",
			failTs: base + 40000,
			writes: []haDedupWrite{
				{replica: "prometheus-0", ts: base},
				{replica: "prometheus-0", ts: base + 15000},
				{replica: "prometheus-1", ts: base + 40000, series: []prompb.TimeSeries{
					{
						Labels:    withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}}, "prometheus-1"),
						Samples:   []prompb.Sample{{Value: 1, Timestamp: base + 40000}},
						Exemplars: exemplar(base + 40000),
					},
					{
						Labels:    withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}}, "prometheus-1"),
						Exemplars: exemplar(base + 41000),
					},
				}},
				{replica: "prometheus-0", ts: base + 30000},
			},
			expected:          []int64{base, base + 15000, base + 30000},
			expectedExemplars: []int64{base + 41000},
		},
	} {
		for _, capnp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/capnp=%v", tcase.name, capnp), func(t *testing.T) {
				t.Parallel()

				m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
				s := failingAppendStorage{MultiTSDB: m, failTs: tcase.failTs}
				for _, wr := range tcase.writes {
					series := wr.series
					if series == nil {
						series = sample(wr.replica, wr.ts, tcase.hist)
					}
					err := writeHADedupRequest(t, s, capnp, time.Minute, series)
					if wr.ts == base+year || wr.ts == tcase.failTs {
						require.Error(t, err)
						continue
					}
					require.NoError(t, err)
				}

				expected := storedSeries{floats: tcase.expected, exemplars: tcase.expectedExemplars}
				if tcase.hist {
					expected = storedSeries{histograms: tcase.expected, exemplars: tcase.expectedExemplars}
				}
				require.Equal(t, map[string]storedSeries{`{__name__="up"}`: expected}, readTenantSeries(t, m))
			})
		}
	}
}

// faultyAppendStorage fails the commit of the next request after rolling it back, like the TSDB failing to log it,
// and calls beforeGetRef in reference lookups and beforeAppendHistogram in histogram appends before delegating to the
// TSDB.
type faultyAppendStorage struct {
	*MultiTSDB
	failCommit            bool
	beforeGetRef          func(lset labels.Labels)
	beforeAppendHistogram func()
}

func (s *faultyAppendStorage) TenantAppendable(tenantID string) (Appendable, error) {
	a, err := s.MultiTSDB.TenantAppendable(tenantID)
	return faultyAppendable{Appendable: a, s: s}, err
}

type faultyAppendable struct {
	Appendable
	s *faultyAppendStorage
}

func (a faultyAppendable) Appender(ctx context.Context) (storage.Appender, error) {
	app, err := a.Appendable.Appender(ctx)
	if err != nil {
		return nil, err
	}
	failCommit := a.s.failCommit
	a.s.failCommit = false
	return faultyAppender{Appender: app, getRef: app.(storage.GetRef), failCommit: failCommit, beforeGetRef: a.s.beforeGetRef, beforeAppendHistogram: a.s.beforeAppendHistogram}, nil
}

type faultyAppender struct {
	storage.Appender
	getRef                storage.GetRef
	failCommit            bool
	beforeGetRef          func(lset labels.Labels)
	beforeAppendHistogram func()
}

func (a faultyAppender) GetRef(lset labels.Labels, hash uint64) (storage.SeriesRef, labels.Labels) {
	if a.beforeGetRef != nil {
		a.beforeGetRef(lset)
	}
	return a.getRef.GetRef(lset, hash)
}

func (a faultyAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if a.beforeAppendHistogram != nil {
		a.beforeAppendHistogram()
	}
	return a.Appender.AppendHistogram(ref, l, t, h, fh)
}

func (a faultyAppender) Commit() error {
	if a.failCommit {
		if err := a.Rollback(); err != nil {
			return err
		}
		return errors.New("write WAL")
	}
	return a.Appender.Commit()
}

func TestWriterHADedupFailedWrites(t *testing.T) {
	t.Parallel()

	stale := math.Float64frombits(value.StaleNaN)
	floats := func(replica string, samples ...prompb.Sample) []prompb.TimeSeries {
		return []prompb.TimeSeries{{Labels: withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}}, replica), Samples: samples}}
	}
	otherFloats := func(replica string, samples ...prompb.Sample) []prompb.TimeSeries {
		return []prompb.TimeSeries{{Labels: withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "other"}}, replica), Samples: samples}}
	}
	histograms := func(replica string, ts int64) []prompb.TimeSeries {
		return []prompb.TimeSeries{{
			Labels:     withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "latency"}}, replica),
			Histograms: []prompb.Histogram{prompb.HistogramToHistogramProto(ts, tsdbutil.GenerateTestHistogram(1))},
		}}
	}
	// A histogram with a count not matching its buckets passes the checks before the tracker and is rejected only
	// by the TSDB.
	malformedHistogram := func(replica string, ts int64) []prompb.TimeSeries {
		return []prompb.TimeSeries{{
			Labels:     withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}}, replica),
			Histograms: []prompb.Histogram{prompb.HistogramToHistogramProto(ts, &histogram.Histogram{Count: 1, Sum: 1})},
		}}
	}

	type failedWrite struct {
		series     []prompb.TimeSeries
		failCommit bool
		// concurrent is written while the histogram of series is being appended.
		concurrent []prompb.TimeSeries
		err        bool
	}
	base := time.Now().Add(-time.Minute).UnixMilli()
	for _, tcase := range []struct {
		name          string
		writes        []failedWrite
		expected      []int64
		expectedOther []int64
	}{
		{
			name: "owner writes while a rejected failover sample is appended",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{
					series:     malformedHistogram("prometheus-1", base+90000),
					concurrent: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000}),
				},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 30000})},
			},
			// The concurrent sample at 15s is dropped, as the rejected sample owned the series while it was written.
			expected: []int64{base, base + 30000},
		},
		{
			name: "commit failure after a stale handover is retried",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base - 60000})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base - 45000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base - 35000})},
				{
					series:     floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base - 30000}, prompb.Sample{Value: stale, Timestamp: base - 15000}),
					failCommit: true,
					err:        true,
				},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base - 30000}, prompb.Sample{Value: stale, Timestamp: base - 15000})},
			},
			expected: []int64{base - 60000, base - 45000, base - 30000, base - 15000},
		},
		{
			name: "commit failure of a new series is retried",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base}), failCommit: true, err: true},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 5000})},
			},
			expected: []int64{base},
		},
		{
			name: "commit failure of a new series does not keep its owner",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base}), failCommit: true, err: true},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 5000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 20000})},
			},
			expected: []int64{base + 5000, base + 20000},
		},
		{
			name: "rejected failover histogram",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: malformedHistogram("prometheus-1", base+90000)},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 30000})},
			},
			expected: []int64{base, base + 30000},
		},
		{
			name: "rejected histogram of a new series",
			writes: []failedWrite{
				{series: malformedHistogram("prometheus-1", base)},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 5000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 15000})},
			},
			expected: []int64{base + 5000},
		},
		{
			name: "commit failure after a timeout failover is retried",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000}), failCommit: true, err: true},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 30000})},
			},
			expected: []int64{base, base + 15000, base + 40000},
		},
		{
			name: "commit failure after a timeout failover keeps the old owner",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000}), failCommit: true, err: true},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 30000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000})},
			},
			expected: []int64{base, base + 15000, base + 30000},
		},
		{
			name: "commit failure after a failover and a handover of the same series is retried",
			writes: []failedWrite{
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{
					series: slices.Concat(
						floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 40000}),
						floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 45000}),
						floats("prometheus-0", prompb.Sample{Value: stale, Timestamp: base + 50000}),
					),
					failCommit: true,
					err:        true,
				},
				{series: slices.Concat(
					floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 40000}),
					floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 45000}),
					floats("prometheus-0", prompb.Sample{Value: stale, Timestamp: base + 50000}),
				)},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 60000})},
			},
			expected: []int64{base, base + 15000, base + 40000, base + 60000},
		},
		{
			// The sample of prometheus-1 is not dropped by the state of the failed owner sample at 40s.
			name: "commit failure forgets the state of series whose owner did not change",
			writes: []failedWrite{
				{series: slices.Concat(floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base}), otherFloats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base}))},
				{series: slices.Concat(floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000}), otherFloats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000}))},
				{
					series:     slices.Concat(floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000}), otherFloats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 40000})),
					failCommit: true,
					err:        true,
				},
				{series: otherFloats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 25000})},
				{series: slices.Concat(floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 30000}), otherFloats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 40000}))},
			},
			expected:      []int64{base, base + 15000, base + 30000},
			expectedOther: []int64{base, base + 15000, base + 25000, base + 40000},
		},
		{
			name: "commit failure of the owner does not suppress the other replica",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 45000}), failCommit: true, err: true},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 50000})},
			},
			expected: []int64{base, base + 15000, base + 50000},
		},
		{
			name: "rejected histogram of the owner",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{series: malformedHistogram("prometheus-0", base+45000)},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 50000})},
			},
			expected: []int64{base, base + 15000, base + 50000},
		},
		{
			name: "rejected histogram of the owner from the future",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{series: malformedHistogram("prometheus-0", base+90000)},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 50000})},
			},
			expected: []int64{base, base + 15000, base + 50000},
		},
		{
			// Accepted limit: forgetting the failed request's failover also drops the state established by the
			// concurrent request, like a restart. The re-elected old owner's lagging sample is rejected as out of order.
			name: "commit failure while a successful request writes the same series",
			writes: []failedWrite{
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base})},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 15000})},
				{
					series:     slices.Concat(floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000}), histograms("prometheus-1", base+40000)),
					concurrent: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 50000}),
					failCommit: true,
					err:        true,
				},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 45000}), err: true},
				{series: floats("prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 60000})},
				{series: floats("prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 55000})},
			},
			expected: []int64{base, base + 15000, base + 50000, base + 60000},
		},
	} {
		for _, capnp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/capnp=%v", tcase.name, capnp), func(t *testing.T) {
				t.Parallel()

				reg := prometheus.NewRegistry()
				m := newHADedupMultiTSDB(t, reg, labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
				s := &faultyAppendStorage{MultiTSDB: m}
				for _, wr := range tcase.writes {
					s.failCommit = wr.failCommit
					s.beforeAppendHistogram = nil
					if wr.concurrent != nil {
						s.beforeAppendHistogram = func() {
							require.NoError(t, writeHADedupRequest(t, m, capnp, time.Minute, wr.concurrent))
						}
					}
					err := writeHADedupRequest(t, s, capnp, time.Minute, wr.series)
					if wr.err {
						require.Error(t, err)
						continue
					}
					require.NoError(t, err)
				}

				expected := map[string]storedSeries{`{__name__="up"}`: {floats: tcase.expected}}
				if tcase.expectedOther != nil {
					expected[`{__name__="other"}`] = storedSeries{floats: tcase.expectedOther}
				}
				require.Equal(t, expected, readTenantSeries(t, m))
				require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP thanos_receive_ha_dedup_tracked_series Number of series tracked by HA deduplication.
# TYPE thanos_receive_ha_dedup_tracked_series gauge
thanos_receive_ha_dedup_tracked_series{tenant=%q} %d
`, tenancy.DefaultTenant, len(expected))), "thanos_receive_ha_dedup_tracked_series"))
			})
		}
	}
}

// capnpRequestFailingAt returns a request of the given series whose decoding fails once the samples read exceed
// readLimit bytes. It emulates a corrupt message without depending on its memory layout.
func capnpRequestFailingAt(t *testing.T, series []prompb.TimeSeries, readLimit uint64) *writecapnp.Request {
	t.Helper()

	capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, series)
	require.NoError(t, err)
	syms, err := capnpReq.Symbols()
	require.NoError(t, err)
	data, err := capnpReq.Data()
	require.NoError(t, err)
	req, err := writecapnp.NewRequest(data.At(0), syms, tenancy.DefaultTenant)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, req.Close()) })
	capnpReq.Message().ResetReadLimit(readLimit)
	return req
}

func TestCapNProtoWriterHADedupRequestError(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Minute).UnixMilli()
	floats := func(name, replica string, samples ...prompb.Sample) prompb.TimeSeries {
		return prompb.TimeSeries{Labels: withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: name}}, replica), Samples: samples}
	}

	reg := prometheus.NewRegistry()
	m := newHADedupMultiTSDB(t, reg, labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
	for _, ts := range []int64{base, base + 15000} {
		require.NoError(t, writeHADedupRequest(t, m, true, time.Minute, []prompb.TimeSeries{
			floats("up", "prometheus-0", prompb.Sample{Value: 1, Timestamp: ts}),
			floats("kept", "prometheus-0", prompb.Sample{Value: 1, Timestamp: ts}),
		}))
	}

	// The failover of the first series and the owner sample of the second one are decided before the samples of the
	// third one exceed the read limit.
	req := capnpRequestFailingAt(t, []prompb.TimeSeries{
		floats("up", "prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 40000}),
		floats("kept", "prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 40000}),
		floats("other", "prometheus-1", make([]prompb.Sample, 1024)...),
	}, 4096)
	err := NewCapNProtoWriter(log.NewNopLogger(), m, &CapNProtoWriterOptions{TooFarInFutureTimeWindow: int64(time.Minute)}).Write(context.Background(), req)
	require.ErrorContains(t, err, "request.At")
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP thanos_receive_ha_dedup_failovers_total Total number of series taken over by another HA replica after the owning replica stopped writing them.
# TYPE thanos_receive_ha_dedup_failovers_total counter
thanos_receive_ha_dedup_failovers_total{tenant=%[1]q} 1
# HELP thanos_receive_ha_dedup_forgotten_total Total number of series states removed because the samples they were based on were not stored.
# TYPE thanos_receive_ha_dedup_forgotten_total counter
thanos_receive_ha_dedup_forgotten_total{tenant=%[1]q} 2
# HELP thanos_receive_ha_dedup_samples_total Total number of samples from HA replicas processed by deduplication, by outcome.
# TYPE thanos_receive_ha_dedup_samples_total counter
thanos_receive_ha_dedup_samples_total{outcome="accepted",tenant=%[1]q} 6
thanos_receive_ha_dedup_samples_total{outcome="dropped",tenant=%[1]q} 0
`, tenancy.DefaultTenant)), "thanos_receive_ha_dedup_failovers_total", "thanos_receive_ha_dedup_forgotten_total", "thanos_receive_ha_dedup_samples_total"))

	require.NoError(t, writeHADedupRequest(t, m, true, time.Minute, []prompb.TimeSeries{
		floats("up", "prometheus-0", prompb.Sample{Value: 1, Timestamp: base + 30000}),
		floats("kept", "prometheus-1", prompb.Sample{Value: 1, Timestamp: base + 20000}),
	}))
	require.Equal(t, map[string]storedSeries{
		`{__name__="up"}`:   {floats: []int64{base, base + 15000, base + 30000}},
		`{__name__="kept"}`: {floats: []int64{base, base + 15000, base + 20000}},
	}, readTenantSeries(t, m))
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
thanos_receive_ha_dedup_passthrough_total{reason="no_label",tenant=%[1]q} 1
thanos_receive_ha_dedup_passthrough_total{reason="replica_table_full",tenant=%[1]q} 0
thanos_receive_ha_dedup_passthrough_total{reason="replica_value_too_long",tenant=%[1]q} 0
`, tenancy.DefaultTenant)), "thanos_receive_ha_dedup_passthrough_total"))
		})
	}
}

func TestWriterHADedupExemplarsOfUnknownReplica(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Minute).UnixMilli()
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "up"}}

	for _, capnp := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
			t.Parallel()

			m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
			writeHADedupRequests(t, m, capnp, []haDedupWrite{
				{series: []prompb.TimeSeries{{Labels: withReplicaLabel(lbls, "prometheus-0"), Samples: []prompb.Sample{{Value: 1, Timestamp: base}}}}},
				// prometheus-1 is not interned by a request without samples, so it doesn't own the series of the first
				// interned replica.
				{series: []prompb.TimeSeries{{
					Labels:    withReplicaLabel(lbls, "prometheus-1"),
					Exemplars: []prompb.Exemplar{{Labels: []labelpb.ZLabel{{Name: "trace_id", Value: "a"}}, Value: 1, Timestamp: base + 1000}},
				}}},
			})

			require.Equal(t, map[string]storedSeries{
				`{__name__="up"}`: {floats: []int64{base}},
			}, readTenantSeries(t, m))
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
	tracker := hadedup.NewTracker(log.NewNopLogger(), testHADedupConfig(), prometheus.NewRegistry())
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
		require.True(t, s.dedup)
		require.Equal(t, orig, lbls)

		got, s = w.prepare(noReplica, false)
		require.Equal(t, noReplica, got)
		require.False(t, s.dedup)

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
		require.True(t, s.dedup)
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
		require.True(t, w.accept(&s, 0, 0, false))
		require.True(t, w.acceptExemplars(&s, 1))
	})
}

func TestHADedupSeries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	tracker := hadedup.NewTracker(log.NewNopLogger(), testHADedupConfig(), reg)
	r0, _ := tracker.Replica("prometheus-0")
	r1, _ := tracker.Replica("prometheus-1")
	w := haDedupWriter{tracker: tracker, horizon: tracker.Horizon()}

	// New series: the first sample is appended without state and elects its replica.
	s0 := haDedupSeries{dedup: true, replica: r0, interned: true}
	require.True(t, w.accept(&s0, 0, 0, false))
	w.appended(&s0, 7)
	require.True(t, tracker.IsOwner(7, r0))
	require.Equal(t, []storage.SeriesRef{7}, w.changed)

	// A series is listed once per request.
	require.True(t, w.accept(&s0, 7, 1000, false))
	w.appended(&s0, 7)
	require.Equal(t, []storage.SeriesRef{7}, w.changed)

	// A failed append returns a zero reference, later samples must still be checked against the tracker.
	s1 := haDedupSeries{dedup: true, replica: r1, interned: true}
	require.False(t, w.accept(&s1, 7, 5000, false))
	require.False(t, w.accept(&s1, 0, 6000, false))
	require.False(t, w.acceptExemplars(&s1, 7))
	require.True(t, w.acceptExemplars(&s0, 7))
	// The zero replica index of a series whose replica is not interned is r0's.
	unknown := haDedupSeries{dedup: true, replicaValue: "unknown"}
	require.False(t, w.acceptExemplars(&unknown, 7))
	require.True(t, w.acceptExemplars(&unknown, 100))

	// A failed append of an accepted sample that changed the owner forgets the series.
	require.True(t, w.accept(&s1, 0, 5000+(365*24*time.Hour).Milliseconds(), false))
	require.True(t, tracker.IsOwner(7, r1))
	require.False(t, tracker.IsOwner(7, r0))
	w.appended(&s1, 0)
	require.False(t, tracker.Tracked(7))
	require.Equal(t, []storage.SeriesRef{7}, w.changed)

	// So does a failed append of an accepted sample of the owner, whose state it advanced.
	require.True(t, tracker.Init(10, r0, 0, w.horizon, false))
	s5 := haDedupSeries{dedup: true, replica: r0, interned: true}
	require.True(t, w.accept(&s5, 10, 15000, false))
	w.appended(&s5, 0)
	require.False(t, tracker.Tracked(10))
	require.Equal(t, []storage.SeriesRef{7}, w.changed)

	// The reference is known from the first sample even if its append fails.
	require.True(t, tracker.Init(8, r0, 15000, w.horizon, false))
	s2 := haDedupSeries{dedup: true, replica: r0, interned: true}
	require.True(t, w.accept(&s2, 8, 20000, false))
	w.appended(&s2, 0)
	require.True(t, w.accept(&s2, 0, 25000, false))
	w.appended(&s2, 8)
	require.True(t, tracker.IsOwner(8, r0))
	require.False(t, tracker.IsOwner(8, r1))
	accept, _ := tracker.Accept(8, r1, 30000, w.horizon, false)
	require.False(t, accept)
	// A handover drops the sample, but must be forgotten if the request fails.
	require.False(t, w.accept(&s2, 0, 35000, true), "stale marker must be handed over to the live replica")
	require.True(t, tracker.IsOwner(8, r1))
	require.Equal(t, []storage.SeriesRef{7, 8}, w.changed)

	w.forget()
	require.Empty(t, w.changed)
	require.False(t, tracker.Tracked(7))
	require.False(t, tracker.Tracked(8))

	// A failover and a handover of the same series in one request list it once.
	require.True(t, tracker.Init(9, r0, 0, w.horizon, false))
	s3 := haDedupSeries{dedup: true, replica: r1, interned: true}
	require.True(t, w.accept(&s3, 9, 90000, false))
	w.appended(&s3, 9)
	s4 := haDedupSeries{dedup: true, replica: r0, interned: true}
	require.False(t, w.accept(&s4, 9, 85000, false))
	require.False(t, w.accept(&s3, 9, 100000, true))
	require.True(t, tracker.IsOwner(9, r0))
	require.Equal(t, []storage.SeriesRef{9}, w.changed)

	w.forget()
	require.False(t, tracker.Tracked(9))
	// Forgetting series 7 a second time is not counted.
	require.Equal(t, hadedup.Counts{Accepted: 7, Dropped: 5, Elections: 2, Failovers: 2, Handovers: 2, Forgotten: 5}, w.counts)
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
# HELP thanos_receive_ha_dedup_tracked_series Number of series tracked by HA deduplication.
# TYPE thanos_receive_ha_dedup_tracked_series gauge
thanos_receive_ha_dedup_tracked_series 0
`), "thanos_receive_ha_dedup_tracked_series"))
}

func TestHADedupWriterReplicaCache(t *testing.T) {
	t.Parallel()

	series := func(replica string) []labelpb.ZLabel {
		return withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "zone", Value: "z-" + replica}}, replica)
	}
	// The replica table holds two replicas, so prometheus-2 is passed through.
	request := []string{"prometheus-0", "prometheus-0", "prometheus-1", "prometheus-2", "prometheus-2", "prometheus-0", "prometheus-2", "prometheus-1"}

	for _, inPlace := range []bool{false, true} {
		t.Run(fmt.Sprintf("inPlace=%v", inPlace), func(t *testing.T) {
			t.Parallel()

			tracker := hadedup.NewTracker(log.NewNopLogger(), testHADedupConfig(), prometheus.NewRegistry())
			for _, replica := range []string{"prometheus-0", "prometheus-1"} {
				_, ok := tracker.Replica(replica)
				require.True(t, ok)
			}
			w := haDedupWriter{tracker: tracker}
			// Like a Cap'n Proto request, every series is decoded into the same label buffer, which the in-place
			// removal of the replica label then modifies.
			var buf []labelpb.ZLabel
			for i, replica := range request {
				lbls := series(replica)
				if inPlace {
					buf = append(buf[:0], lbls...)
					lbls = buf
				}

				got, s := w.prepare(lbls, inPlace)
				if replica == "prometheus-2" {
					require.False(t, s.dedup, "series %d", i)
					require.Equal(t, series(replica), got, "series %d", i)
					continue
				}
				require.True(t, s.dedup, "series %d", i)
				expected, ok := tracker.Replica(replica)
				require.True(t, ok)
				require.Equal(t, expected, s.replica, "series %d", i)
				require.Equal(t, []labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "zone", Value: "z-" + replica}}, got, "series %d", i)
			}
			require.Equal(t, hadedup.Counts{ReplicaTableFull: 3}, w.counts)
		})
	}

	t.Run("interned by the request", func(t *testing.T) {
		t.Parallel()

		tracker := hadedup.NewTracker(log.NewNopLogger(), testHADedupConfig(), prometheus.NewRegistry())
		w := haDedupWriter{tracker: tracker, horizon: tracker.Horizon()}

		_, s := w.prepare(series("prometheus-0"), false)
		require.True(t, s.dedup)
		require.False(t, s.interned)
		require.True(t, w.intern(&s))
		r, ok, _ := tracker.LookupReplica("prometheus-0")
		require.True(t, ok)
		require.Equal(t, r, s.replica)

		// The cache knows the interned value.
		_, s = w.prepare(series("prometheus-0"), false)
		require.True(t, s.interned)
		require.Equal(t, r, s.replica)

		// A value that finds the table full when its series is interned aborts the request, as the series' labels
		// were already used without the replica label. Its retry writes the series without deduplication.
		_, s = w.prepare(series("prometheus-1"), false)
		require.True(t, s.dedup)
		_, ok = tracker.Replica("prometheus-2")
		require.True(t, ok)
		require.False(t, w.intern(&s))
		require.True(t, w.tableFilled)
		require.False(t, w.accept(&s, 1, 0, false))
		_, s = w.prepare(series("prometheus-1"), false)
		require.False(t, s.dedup, "the full table must be cached")
		require.Equal(t, hadedup.Counts{ReplicaTableFull: 1}, w.counts)
	})
}

func TestWriterHADedupMetrics(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Hour).UnixMilli()
	floats := func(instance, replica string, ts int64) prompb.TimeSeries {
		lbls := []labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: instance}}
		if replica != "" {
			lbls = withReplicaLabel(lbls, replica)
		}
		return prompb.TimeSeries{Labels: lbls, Samples: []prompb.Sample{{Value: 1, Timestamp: ts}}}
	}

	for _, capnp := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			m := newHADedupMultiTSDB(t, reg, labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
			s := &faultyAppendStorage{MultiTSDB: m}
			require.NoError(t, writeHADedupRequest(t, s, capnp, time.Minute, []prompb.TimeSeries{
				floats("a", "prometheus-0", base),
				floats("b", "prometheus-0", base),
				floats("c", "", base),
			}))
			// The replica table holds two replicas, so the series of prometheus-2 are passed through.
			require.NoError(t, writeHADedupRequest(t, s, capnp, time.Minute, []prompb.TimeSeries{
				floats("a", "prometheus-1", base+5000),
				floats("a", "prometheus-2", base+5000),
				floats("b", "prometheus-1", base+5000),
				floats("b", "prometheus-2", base+5000),
			}))
			// The decisions of a request whose commit fails are counted too.
			s.failCommit = true
			require.Error(t, writeHADedupRequest(t, s, capnp, time.Minute, []prompb.TimeSeries{floats("a", "prometheus-1", base+90000)}))

			require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP thanos_receive_ha_dedup_elections_total Total number of series whose owning HA replica was elected by their first tracked sample.
# TYPE thanos_receive_ha_dedup_elections_total counter
thanos_receive_ha_dedup_elections_total{tenant=%[1]q} 2
# HELP thanos_receive_ha_dedup_failovers_total Total number of series taken over by another HA replica after the owning replica stopped writing them.
# TYPE thanos_receive_ha_dedup_failovers_total counter
thanos_receive_ha_dedup_failovers_total{tenant=%[1]q} 1
# HELP thanos_receive_ha_dedup_forgotten_total Total number of series states removed because the samples they were based on were not stored.
# TYPE thanos_receive_ha_dedup_forgotten_total counter
thanos_receive_ha_dedup_forgotten_total{tenant=%[1]q} 1
# HELP thanos_receive_ha_dedup_handovers_total Total number of series handed over to another HA replica after the owning replica wrote a stale marker.
# TYPE thanos_receive_ha_dedup_handovers_total counter
thanos_receive_ha_dedup_handovers_total{tenant=%[1]q} 0
# HELP thanos_receive_ha_dedup_passthrough_total Total number of series written without HA deduplication, by reason.
# TYPE thanos_receive_ha_dedup_passthrough_total counter
thanos_receive_ha_dedup_passthrough_total{reason="no_label",tenant=%[1]q} 1
thanos_receive_ha_dedup_passthrough_total{reason="replica_table_full",tenant=%[1]q} 2
thanos_receive_ha_dedup_passthrough_total{reason="replica_value_too_long",tenant=%[1]q} 0
# HELP thanos_receive_ha_dedup_samples_total Total number of samples from HA replicas processed by deduplication, by outcome.
# TYPE thanos_receive_ha_dedup_samples_total counter
thanos_receive_ha_dedup_samples_total{outcome="accepted",tenant=%[1]q} 3
thanos_receive_ha_dedup_samples_total{outcome="dropped",tenant=%[1]q} 2
`, tenancy.DefaultTenant)), "thanos_receive_ha_dedup_elections_total", "thanos_receive_ha_dedup_failovers_total", "thanos_receive_ha_dedup_forgotten_total",
				"thanos_receive_ha_dedup_handovers_total", "thanos_receive_ha_dedup_passthrough_total", "thanos_receive_ha_dedup_samples_total"))
		})
	}
}

func TestWriterHADedupFutureSamples(t *testing.T) {
	t.Parallel()

	hour := time.Hour.Milliseconds()
	type write struct {
		replica string
		// ts is relative to the time the subtest starts, as the tracker compares timestamps with the current time.
		ts int64
	}

	for _, tcase := range []struct {
		name           string
		tooFarInFuture time.Duration
		writes         []write
		expected       []int64
	}{
		{
			// A sample from the future of another replica must not suppress a live owner.
			name: "without window",
			writes: []write{
				{"prometheus-0", -15000},
				{"prometheus-0", 0},
				{"made-up", hour},
				{"prometheus-0", 15000},
			},
			expected: []int64{-15000, 0, 15000},
		},
		{
			name:           "within window",
			tooFarInFuture: 5 * time.Minute,
			writes: []write{
				{"prometheus-0", -15000},
				{"prometheus-0", 0},
				{"made-up", (4 * time.Minute).Milliseconds()},
				{"prometheus-0", 15000},
			},
			expected: []int64{-15000, 0, 15000},
		},
		{
			// The owner stopped 45s ago and the clock of the other replica is an hour ahead.
			name: "dead owner and replica with a clock ahead",
			writes: []write{
				{"prometheus-0", -60000},
				{"prometheus-0", -45000},
				{"prometheus-1", hour},
				{"prometheus-1", hour + 15000},
			},
			expected: []int64{-60000, -45000, hour, hour + 15000},
		},
	} {
		for _, capnp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/capnp=%v", tcase.name, capnp), func(t *testing.T) {
				t.Parallel()

				now := time.Now().UnixMilli()
				m := newHADedupMultiTSDB(t, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
				for _, wr := range tcase.writes {
					require.NoError(t, writeHADedupRequest(t, m, capnp, tcase.tooFarInFuture, []prompb.TimeSeries{{
						Labels:  withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}}, wr.replica),
						Samples: []prompb.Sample{{Value: 1, Timestamp: now + wr.ts}},
					}}))
				}
				expected := make([]int64, 0, len(tcase.expected))
				for _, ts := range tcase.expected {
					expected = append(expected, now+ts)
				}
				require.Equal(t, map[string]storedSeries{`{__name__="up"}`: {floats: expected}}, readTenantSeries(t, m))
			})
		}
	}
}

func TestWriterHADedupReplicaTable(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Hour).UnixMilli()
	series := func(instance, replica string, samples ...int64) prompb.TimeSeries {
		s := prompb.TimeSeries{Labels: withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: instance}}, replica)}
		for _, ts := range samples {
			s.Samples = append(s.Samples, prompb.Sample{Value: 1, Timestamp: ts})
		}
		return s
	}
	longValue := strings.Repeat("r", hadedup.MaxReplicaValueLength+1)
	maxValue := strings.Repeat("r", hadedup.MaxReplicaValueLength)

	for _, capnp := range []bool{false, true} {
		t.Run(fmt.Sprintf("capnp=%v", capnp), func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			m := newHADedupMultiTSDB(t, reg, labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
			s := failingAppendStorage{MultiTSDB: m, failTs: base - 1000}
			// Series without valid samples must not take the slots of the two replicas.
			err := writeHADedupRequest(t, s, capnp, 5*time.Minute, []prompb.TimeSeries{
				series("a", "no-samples-0"),
				series("a", "no-samples-1"),
				series("a", "too-far-in-future", base+(365*24*time.Hour).Milliseconds()),
				series("a", "rejected", base-1000),
				series("a", longValue, base),
			})
			require.Error(t, err)
			require.NoError(t, writeHADedupRequest(t, s, capnp, 5*time.Minute, []prompb.TimeSeries{
				series("a", "prometheus-0", base),
				series("b", maxValue, base),
			}))
			require.NoError(t, writeHADedupRequest(t, s, capnp, 5*time.Minute, []prompb.TimeSeries{
				series("a", maxValue, base+5000),
				series("b", "prometheus-0", base),
			}))

			require.Equal(t, map[string]storedSeries{
				`{__name__="up", instance="a"}`:                                         {floats: []int64{base}},
				`{__name__="up", instance="b"}`:                                         {floats: []int64{base}},
				`{__name__="up", instance="a", prometheus_replica="` + longValue + `"}`: {floats: []int64{base}},
			}, readTenantSeries(t, m))
			require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP thanos_receive_ha_dedup_passthrough_total Total number of series written without HA deduplication, by reason.
# TYPE thanos_receive_ha_dedup_passthrough_total counter
thanos_receive_ha_dedup_passthrough_total{reason="no_label",tenant=%[1]q} 0
thanos_receive_ha_dedup_passthrough_total{reason="replica_table_full",tenant=%[1]q} 0
thanos_receive_ha_dedup_passthrough_total{reason="replica_value_too_long",tenant=%[1]q} 1
# HELP thanos_receive_ha_dedup_replicas Number of distinct HA replica label values tracked.
# TYPE thanos_receive_ha_dedup_replicas gauge
thanos_receive_ha_dedup_replicas{tenant=%[1]q} 2
`, tenancy.DefaultTenant)), "thanos_receive_ha_dedup_passthrough_total", "thanos_receive_ha_dedup_replicas"))
		})
	}
}

func TestWriterHADedupReplicaTableFillsDuringRequest(t *testing.T) {
	t.Parallel()

	base := time.Now().Add(-time.Minute).UnixMilli()
	floats := func(name, replica string, ts int64) prompb.TimeSeries {
		return prompb.TimeSeries{Labels: withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: name}}, replica), Samples: []prompb.Sample{{Value: 1, Timestamp: ts}}}
	}

	for _, tcase := range []struct {
		name string
		// metric is the name of the series of prometheus-1, whose replica value finds the table full when it is interned.
		metric   string
		expected map[string]storedSeries
	}{
		{
			name:   "existing series",
			metric: "up",
			expected: map[string]storedSeries{
				`{__name__="first"}`: {floats: []int64{base, base + 15000}},
				`{__name__="up"}`:    {floats: []int64{base}},
				`{__name__="up", prometheus_replica="prometheus-1"}`: {floats: []int64{base + 20000}},
			},
		},
		{
			name:   "new series",
			metric: "other",
			expected: map[string]storedSeries{
				`{__name__="first"}`: {floats: []int64{base, base + 15000}},
				`{__name__="up"}`:    {floats: []int64{base}},
				`{__name__="other", prometheus_replica="prometheus-1"}`: {floats: []int64{base + 20000}},
			},
		},
	} {
		for _, capnp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/capnp=%v", tcase.name, capnp), func(t *testing.T) {
				t.Parallel()

				reg := prometheus.NewRegistry()
				m := newHADedupMultiTSDB(t, reg, labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
				require.NoError(t, writeHADedupRequest(t, m, capnp, time.Minute, []prompb.TimeSeries{floats("first", "prometheus-0", base), floats("up", "prometheus-0", base)}))

				tracker, err := m.TenantHADedupTracker(tenancy.DefaultTenant)
				require.NoError(t, err)
				// Another request takes the last slot of the replica table after the series of prometheus-1 was prepared.
				s := &faultyAppendStorage{MultiTSDB: m, beforeGetRef: func(lset labels.Labels) {
					if lset.Get(labels.MetricName) == tcase.metric && !lset.Has(testHADedupReplicaLabel) {
						_, ok := tracker.Replica("prometheus-2")
						require.True(t, ok)
					}
				}}
				series := []prompb.TimeSeries{floats("first", "prometheus-0", base+15000), floats(tcase.metric, "prometheus-1", base+20000)}
				err = writeHADedupRequest(t, s, capnp, time.Minute, series)
				require.Error(t, err)
				require.True(t, isNotReady(errors.Cause(err)), "the request must be retried")
				require.Equal(t, map[string]storedSeries{
					`{__name__="first"}`: {floats: []int64{base}},
					`{__name__="up"}`:    {floats: []int64{base}},
				}, readTenantSeries(t, m))

				// The retry finds the table full and writes the series of prometheus-1 without deduplication.
				require.NoError(t, writeHADedupRequest(t, m, capnp, time.Minute, series))
				require.Equal(t, tcase.expected, readTenantSeries(t, m))
				require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP thanos_receive_ha_dedup_forgotten_total Total number of series states removed because the samples they were based on were not stored.
# TYPE thanos_receive_ha_dedup_forgotten_total counter
thanos_receive_ha_dedup_forgotten_total{tenant=%[1]q} 1
# HELP thanos_receive_ha_dedup_passthrough_total Total number of series written without HA deduplication, by reason.
# TYPE thanos_receive_ha_dedup_passthrough_total counter
thanos_receive_ha_dedup_passthrough_total{reason="no_label",tenant=%[1]q} 0
thanos_receive_ha_dedup_passthrough_total{reason="replica_table_full",tenant=%[1]q} 1
thanos_receive_ha_dedup_passthrough_total{reason="replica_value_too_long",tenant=%[1]q} 0
# HELP thanos_receive_ha_dedup_tracked_series Number of series tracked by HA deduplication.
# TYPE thanos_receive_ha_dedup_tracked_series gauge
thanos_receive_ha_dedup_tracked_series{tenant=%[1]q} 2
`, tenancy.DefaultTenant)), "thanos_receive_ha_dedup_forgotten_total", "thanos_receive_ha_dedup_passthrough_total", "thanos_receive_ha_dedup_tracked_series"))
			})
		}
	}
}

// haReplicaRequests returns a request of 1000 series with a single sample for each of two HA replicas.
func haReplicaRequests(instancePrefix string) [][]prompb.TimeSeries {
	const numSeries = 1000
	replicas := make([][]prompb.TimeSeries, 2)
	for r := range replicas {
		replicas[r] = make([]prompb.TimeSeries, numSeries)
		for i := range numSeries {
			replicas[r][i] = prompb.TimeSeries{
				Labels: withReplicaLabel([]labelpb.ZLabel{
					{Name: "__name__", Value: "bench"},
					{Name: "instance", Value: fmt.Sprintf("%s-%d", instancePrefix, i)},
					{Name: "job", Value: "node"},
				}, fmt.Sprintf("prometheus-%d", r)),
				Samples: []prompb.Sample{{Value: 1}},
			}
		}
	}
	return replicas
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

			replicas := haReplicaRequests("host")
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

func BenchmarkWriterHADedupParallel(b *testing.B) {
	m := newHADedupMultiTSDB(b, prometheus.NewRegistry(), labels.FromStrings("replica", "01"), WithHADedup(testHADedupConfig()))
	w := NewWriter(log.NewNopLogger(), m, &WriterOptions{})

	start := time.Now().Add(-time.Hour).UnixMilli()
	var worker atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every goroutine writes the requests of both replicas for its own targets, like the Prometheus pairs of
		// different teams writing to the same tenant.
		replicas := haReplicaRequests(fmt.Sprintf("host-%d", worker.Add(1)))

		ts := start
		for i := 0; pb.Next(); i++ {
			r := i % len(replicas)
			if r == 0 {
				ts += 15000
			}
			for j := range replicas[r] {
				replicas[r][j].Samples[0].Timestamp = ts + int64(r)*5000
			}
			if err := w.Write(context.Background(), tenancy.DefaultTenant, replicas[r]); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
