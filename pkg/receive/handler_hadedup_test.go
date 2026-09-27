// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestHandlerHADedup(t *testing.T) {
	t.Parallel()

	const (
		numIngestors      = 3
		replicationFactor = 2
		numSeries         = 20
		numScrapes        = 10
		interval          = int64(15000)
		offset            = int64(5000)
	)

	for _, tc := range []struct {
		name          string
		algorithm     HashringAlgorithm
		routerIgnores bool
		ingestorDedup bool
	}{
		{name: "hashmod", algorithm: AlgorithmHashmod, routerIgnores: true, ingestorDedup: true},
		{name: "ketama", algorithm: AlgorithmKetama, routerIgnores: true, ingestorDedup: true},
		{name: "router without ignored label", algorithm: AlgorithmKetama, ingestorDedup: true},
		{name: "ingestor without dedup", algorithm: AlgorithmKetama, routerIgnores: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handlers, hashring, closeFunc, err := newTestHandlerHashring(tc.name, make([]*fakeAppendable, numIngestors), replicationFactor, tc.algorithm, false)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, closeFunc())
				for _, h := range handlers {
					h.Close()
				}
			})

			var hashringOpts []HashringOption
			if tc.routerIgnores {
				hashringOpts = append(hashringOpts, WithHashIgnoredLabel(testHADedupReplicaLabel))
			}
			ring, err := NewMultiHashring(tc.algorithm, replicationFactor, []HashringConfig{{Hashring: "test", Endpoints: hashring.Nodes()}}, prometheus.NewRegistry(), hashringOpts...)
			require.NoError(t, err)

			var (
				dbs = make([]*MultiTSDB, 0, numIngestors)
				reg = prometheus.NewRegistry()
			)
			for i, h := range handlers {
				var opts []MultiTSDBOption
				if tc.ingestorDedup {
					opts = append(opts, WithHADedup(testHADedupConfig()))
				}
				ingestorReg := prometheus.WrapRegistererWith(prometheus.Labels{"ingestor": fmt.Sprintf("%d", i)}, reg)
				m := newHADedupMultiTSDB(t, ingestorReg, labels.FromStrings("receive_replica", fmt.Sprintf("%d", i)), opts...)
				dbs = append(dbs, m)
				h.writer = NewWriter(log.NewNopLogger(), m, &WriterOptions{})
				h.Hashring(ring)
			}

			// Routers return once a write quorum is reached. Wait for all copies of a request to be processed before
			// sending the next one, so that concurrent replication doesn't reorder samples.
			processedMetric := "prometheus_tsdb_head_samples_appended_total"
			if tc.ingestorDedup {
				processedMetric = "thanos_receive_ha_dedup_samples_total"
			}
			var expectedProcessed float64

			base := time.Now().Add(-time.Hour).UnixMilli()
			base -= base % interval
			for i := range int64(numScrapes) {
				for r := range 2 {
					ts := base + i*interval + int64(r)*offset
					wreq := &prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, 0, numSeries)}
					for s := range numSeries {
						wreq.Timeseries = append(wreq.Timeseries, prompb.TimeSeries{
							Labels:  withReplicaLabel([]labelpb.ZLabel{{Name: "__name__", Value: "up"}, {Name: "instance", Value: fmt.Sprintf("host-%d", s)}}, fmt.Sprintf("prometheus-%d", r)),
							Samples: []prompb.Sample{{Value: 1, Timestamp: ts}},
						})
					}
					// Each Prometheus replica writes through a different router.
					rec, err := makeRequest(handlers[r], tenancy.DefaultTenant, wreq)
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

					expectedProcessed += numSeries * replicationFactor
					require.Eventually(t, func() bool {
						return sumCounter(t, reg, processedMetric) == expectedProcessed
					}, 10*time.Second, time.Millisecond)
				}
			}

			copies := map[string]int{}
			for _, m := range dbs {
				for lset, s := range readTenantSeries(t, m) {
					copies[lset]++

					// No stored series may interleave samples of both replicas.
					require.Len(t, s.floats, numScrapes, lset)
					for _, ts := range s.floats {
						require.Equal(t, s.floats[0]%interval, ts%interval, lset)
					}
				}
			}

			switch {
			case tc.routerIgnores && tc.ingestorDedup:
				require.Len(t, copies, numSeries)
				for lset, n := range copies {
					require.Equal(t, replicationFactor, n, lset)
					require.NotContains(t, lset, testHADedupReplicaLabel)
				}
			case tc.ingestorDedup:
				// Twins may land on different ingestors: duplicates as today, but never mixed into one series.
				require.Len(t, copies, numSeries)
				var total int
				for lset, n := range copies {
					require.NotContains(t, lset, testHADedupReplicaLabel)
					total += n
				}
				require.Greater(t, total, numSeries*replicationFactor)
			default:
				require.Len(t, copies, 2*numSeries)
				for lset, n := range copies {
					require.Equal(t, replicationFactor, n, lset)
					require.Contains(t, lset, testHADedupReplicaLabel)
				}
			}
		})
	}
}

func sumCounter(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()

	mfs, err := g.Gather()
	require.NoError(t, err)

	var sum float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
	}
	return sum
}
