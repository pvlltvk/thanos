// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusStore_Series_MemorySpill(t *testing.T) {
	t.Run("configuration validates before registration", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			threshold units.Base2Bytes
			dir       string
			budget    units.Base2Bytes
		}{
			{name: "negative threshold", threshold: -1, dir: t.TempDir()},
			{name: "negative disk budget", dir: t.TempDir(), budget: -1},
			{name: "enabled without directory", threshold: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				reg := prometheus.NewRegistry()
				_, err := newSpillTestStore(reg, nil, WithPrometheusStoreSorting(tc.threshold, tc.dir, tc.budget))
				testutil.Assert(t, err != nil, "invalid sorting configuration was accepted")
				families, gatherErr := reg.Gather()
				testutil.Ok(t, gatherErr)
				testutil.Equals(t, 0, len(families))
			})
		}
	})

	t.Run("disabled and below threshold do not touch directory", func(t *testing.T) {
		for _, threshold := range []units.Base2Bytes{0, 64 * units.MiB} {
			t.Run(fmt.Sprint(threshold), func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "never-created")
				fixture := spillTestFixture(t, "metadata", 4, false)
				store, err := newSpillTestStore(prometheus.NewRegistry(), spillTestClient(fixture, nil), WithPrometheusStoreSorting(threshold, dir, 0))
				testutil.Ok(t, err)
				_, err = os.Stat(dir)
				testutil.Assert(t, os.IsNotExist(err), "constructor touched spill directory: %v", err)
				srv := newStoreSeriesServer(context.Background())
				testutil.Ok(t, store.Series(compactRequest(true), srv))
				testutil.Equals(t, 4, len(srv.SeriesSet))
				_, err = os.Stat(dir)
				testutil.Assert(t, os.IsNotExist(err), "request touched spill directory below threshold: %v", err)
			})
		}
	})

	t.Run("disk matches RAM across input paths and batches", func(t *testing.T) {
		for _, input := range []string{"metadata", "sampled", "streamed"} {
			for _, batch := range []int64{0, 1, 64} {
				t.Run(fmt.Sprintf("%s/batch_%d", input, batch), func(t *testing.T) {
					fixture := spillTestFixture(t, input, 40, true)
					req := compactRequest(input == "metadata")
					req.ResponseBatchSize = batch
					req.WithoutReplicaLabels = []string{"replica"}
					ram, err := newSpillTestStore(prometheus.NewRegistry(), spillTestClient(fixture, nil))
					testutil.Ok(t, err)
					want := newStoreSeriesServer(context.Background())
					testutil.Ok(t, ram.Series(req, want))
					testutil.Equals(t, 40, len(want.SeriesSet))
					assertSpillPayloads(t, want.SeriesSet, input)

					dir := t.TempDir()
					probe := &spillTestProbe{t: t, dir: dir}
					reg := prometheus.NewRegistry()
					disk, err := newSpillTestStore(reg, spillTestClient(fixture, probe.observe),
						WithPrometheusStoreSorting(1, dir, 0),
						WithPrometheusStoreLimits(SeriesSelectLimits{SeriesPerRequest: 40, SamplesPerRequest: 100000}),
					)
					testutil.Ok(t, err)
					got := &spillTestServer{storeSeriesServer: newStoreSeriesServer(context.Background()), onSend: probe.observe}
					testutil.Ok(t, disk.Series(req, got))
					testutil.Equals(t, want.SeriesSet, got.SeriesSet)
					testutil.Assert(t, probe.sawFiles, "enabled sorting never created a run for 40 oversized records")
					testutil.Equals(t, 0.0, selectDropCount(t, reg, "series"))
					testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
					assertSpillDirectoryEmpty(t, dir)
				})
			}
		}
	})

	t.Run("upstream and limit errors after spills emit nothing", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			series     uint64
			samples    uint64
			readErr    error
			diagnostic string
			reason     string
		}{
			{name: "upstream", readErr: errors.New("spill upstream stopped"), diagnostic: "spill upstream stopped"},
			{name: "series limit", series: 3, diagnostic: "limit", reason: "series"},
			{name: "sample limit", samples: 360, diagnostic: "limit", reason: "chunks"},
		} {
			for _, batch := range []int64{0, 1, 64} {
				t.Run(fmt.Sprintf("%s/batch_%d", tc.name, batch), func(t *testing.T) {
					dir := t.TempDir()
					probe := &spillTestProbe{t: t, dir: dir}
					fixture := spillTestFixture(t, "streamed", 8, false)
					body := &spillTestBody{data: fixture.data, observe: probe.observe, readErr: tc.readErr}
					reg := prometheus.NewRegistry()
					store, err := newSpillTestStore(reg, roundTripFunc(func(*http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{fixture.contentType}}, Body: body}, nil
					}), WithPrometheusStoreSorting(1, dir, 0), WithPrometheusStoreLimits(SeriesSelectLimits{SeriesPerRequest: tc.series, SamplesPerRequest: tc.samples}))
					testutil.Ok(t, err)
					srv := newStoreSeriesServer(context.Background())
					req := compactRequest(false)
					req.ResponseBatchSize = batch
					err = store.Series(req, srv)
					testutil.Assert(t, err != nil && strings.Contains(err.Error(), tc.diagnostic), "got error %v, want %q", err, tc.diagnostic)
					testutil.Equals(t, 0, len(srv.SeriesSet))
					testutil.Assert(t, body.closed, "input was not closed")
					testutil.Assert(t, probe.sawFiles, "error was not exercised after an earlier spill")
					for _, reason := range []string{"series", "chunks"} {
						want := 0.0
						if tc.reason == reason {
							want = 1
						}
						testutil.Equals(t, want, selectDropCount(t, reg, reason))
					}
					assertSpillDirectoryEmpty(t, dir)
				})
			}
		}
	})

	t.Run("metadata reset and final error precedence after spill", func(t *testing.T) {
		const rows = `[{"job":"api","series":"z"},{"job":"api","series":"y"},{"job":"api","series":"x"},{"job":"api","series":"w"}]`
		for _, tc := range []struct {
			name, body, diagnostic string
			want                   []string
			drops                  float64
			wantErr                bool
		}{
			{name: "duplicate replacement resets limit and old runs", body: `{"data":` + rows + `,"data":[{"job":"api","series":"b"},{"job":"api","series":"a"}],"status":"success"}`, want: []string{"a", "b"}},
			{name: "null replacement resets limit and old runs", body: `{"data":` + rows + `,"data":null,"status":"success"}`},
			{name: "valid overflow", body: `{"data":` + rows + `,"status":"success"}`, drops: 1, wantErr: true, diagnostic: "failed to send series"},
			{name: "status wins over limit", body: `{"data":` + rows + `,"status":"error","error":"spill metadata rejected"}`, wantErr: true, diagnostic: "spill metadata rejected"},
			{name: "syntax wins over limit", body: `{"data":` + rows + `,"status":"success"`, wantErr: true},
			{name: "schema wins over limit", body: `{"data":[{"job":"api","series":"z"},{"job":"api","series":"y"},{"job":"api","series":"x"},{"job":true}],"status":"success"}`, wantErr: true, diagnostic: "cannot unmarshal"},
		} {
			for _, batch := range []int64{0, 1, 64} {
				t.Run(fmt.Sprintf("%s/batch_%d", tc.name, batch), func(t *testing.T) {
					dir := t.TempDir()
					probe := &spillTestProbe{t: t, dir: dir}
					fixture := spillFixture{data: []byte(tc.body)}
					reg := prometheus.NewRegistry()
					store, err := newSpillTestStore(reg, spillTestClient(fixture, probe.observe), WithPrometheusStoreSorting(1, dir, 0), WithPrometheusStoreLimits(SeriesSelectLimits{SeriesPerRequest: 2}))
					testutil.Ok(t, err)
					req := compactRequest(true)
					req.ResponseBatchSize = batch
					srv := newStoreSeriesServer(context.Background())
					err = store.Series(req, srv)
					testutil.Equals(t, tc.wantErr, err != nil)
					if tc.diagnostic != "" {
						testutil.Assert(t, err != nil && strings.Contains(err.Error(), tc.diagnostic), "error %v lacks %q", err, tc.diagnostic)
					}
					testutil.Equals(t, len(tc.want), len(srv.SeriesSet))
					for i, want := range tc.want {
						assertLabelValue(t, srv.SeriesSet[i].Labels, "series", want)
					}
					testutil.Equals(t, tc.drops, selectDropCount(t, reg, "series"))
					testutil.Equals(t, 0.0, selectDropCount(t, reg, "chunks"))
					testutil.Assert(t, probe.sawFiles, "metadata error/reset did not follow a spill")
					assertSpillDirectoryEmpty(t, dir)
				})
			}
		}
	})

	t.Run("disk budget rejects and cleans up", func(t *testing.T) {
		dir := t.TempDir()
		fixture := spillTestFixture(t, "streamed", 8, true)
		store, err := newSpillTestStore(prometheus.NewRegistry(), spillTestClient(fixture, nil), WithPrometheusStoreSorting(1, dir, 1))
		testutil.Ok(t, err)
		srv := newStoreSeriesServer(context.Background())
		err = store.Series(compactRequest(false), srv)
		testutil.Assert(t, err != nil, "one-byte disk budget accepted oversized run")
		testutil.Equals(t, 0, len(srv.SeriesSet))
		assertSpillDirectoryEmpty(t, dir)
	})

	t.Run("send failure and cancellation clean up", func(t *testing.T) {
		for _, cancelRequest := range []bool{false, true} {
			t.Run(fmt.Sprintf("cancel_%t", cancelRequest), func(t *testing.T) {
				dir := t.TempDir()
				probe := &spillTestProbe{t: t, dir: dir}
				fixture := spillTestFixture(t, "streamed", 40, false)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				body := &spillTestBody{data: fixture.data, observe: probe.observe}
				if cancelRequest {
					body.atEOF = cancel
				}
				store, err := newSpillTestStore(prometheus.NewRegistry(), roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{fixture.contentType}}, Body: body}, nil
				}), WithPrometheusStoreSorting(1, dir, 0))
				testutil.Ok(t, err)
				wantErr := errors.New("spill downstream stopped")
				srv := &spillTestServer{storeSeriesServer: newStoreSeriesServer(ctx), onSend: probe.observe, sendErr: wantErr}
				err = store.Series(compactRequest(false), srv)
				testutil.Assert(t, err != nil, "request ignored send failure or cancellation")
				if cancelRequest {
					testutil.Equals(t, 0, srv.sends)
				} else {
					testutil.Assert(t, strings.Contains(err.Error(), wantErr.Error()), "unexpected send error: %v", err)
					testutil.Equals(t, 1, srv.sends)
				}
				testutil.Assert(t, probe.sawFiles, "cleanup was not exercised after spilling")
				testutil.Assert(t, body.closed, "input was not closed")
				assertSpillDirectoryEmpty(t, dir)
			})
		}
	})

	t.Run("concurrent requests use isolated directories", func(t *testing.T) {
		dir := t.TempDir()
		fixture := spillTestFixture(t, "streamed", 40, false)
		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		store, err := newSpillTestStore(prometheus.NewRegistry(), roundTripFunc(func(*http.Request) (*http.Response, error) {
			body := &spillTestBody{data: fixture.data, atEOF: func() { ready <- struct{}{}; <-release }}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{fixture.contentType}}, Body: body}, nil
		}), WithPrometheusStoreSorting(1, dir, 0))
		testutil.Ok(t, err)
		type result struct {
			err error
			set []storepb.Series
		}
		results := make(chan result, 2)
		for range 2 {
			go func() {
				srv := newStoreSeriesServer(context.Background())
				err := store.Series(compactRequest(false), srv)
				results <- result{err: err, set: srv.SeriesSet}
			}()
		}
		for range 2 {
			select {
			case <-ready:
			case <-time.After(10 * time.Second):
				t.Fatal("requests did not reach upstream EOF")
			}
		}
		entries, err := os.ReadDir(dir)
		testutil.Ok(t, err)
		testutil.Equals(t, 2, len(entries))
		for _, entry := range entries {
			testutil.Assert(t, entry.IsDir(), "request spill path is not a directory")
		}
		once.Do(func() { close(release) })
		first, second := <-results, <-results
		testutil.Ok(t, first.err)
		testutil.Ok(t, second.err)
		testutil.Equals(t, 40, len(first.set))
		testutil.Equals(t, first.set, second.set)
		assertSpillDirectoryEmpty(t, dir)
	})
}

type spillFixture struct {
	data        []byte
	contentType string
}

func spillTestFixture(t *testing.T, input string, count int, large bool) spillFixture {
	t.Helper()
	var rows []map[string]string
	var frames [][]byte
	var sampled []*prompb.TimeSeries
	for i := count - 1; i >= 0; i-- {
		name := fmt.Sprintf("series-%03d", i/2)
		padding := "small"
		if large && i == count-1 {
			padding = strings.Repeat("large-label", 1024)
		}
		lbls := []labelpb.ZLabel{{Name: "job", Value: "api"}, {Name: "padding", Value: padding}, {Name: "replica", Value: "local"}, {Name: "series", Value: name}}
		rows = append(rows, map[string]string{"job": "api", "padding": padding, "replica": "local", "series": name})
		samples := []prompb.Sample{{Timestamp: int64(i), Value: float64(i) + .5}}
		if large && i == count-1 {
			for j := 1; j < 4096; j++ {
				samples = append(samples, prompb.Sample{Timestamp: int64(i + j), Value: float64(i*j) + .25})
			}
		}
		sampled = append(sampled, &prompb.TimeSeries{Labels: lbls, Samples: samples})
		chunk := chunkenc.NewXORChunk()
		appender, err := chunk.Appender()
		testutil.Ok(t, err)
		for _, sample := range samples {
			appender.Append(sample.Timestamp, sample.Value)
		}
		frames = append(frames, compactReadFrame(t, &prompb.ChunkedSeries{Labels: lbls, Chunks: []prompb.Chunk{{MinTimeMs: samples[0].Timestamp, MaxTimeMs: samples[len(samples)-1].Timestamp, Type: prompb.Chunk_XOR, Data: chunk.Bytes()}}}))
	}
	switch input {
	case "metadata":
		data, err := json.Marshal(struct {
			Data   []map[string]string `json:"data"`
			Status string              `json:"status"`
		}{Data: rows, Status: "success"})
		testutil.Ok(t, err)
		return spillFixture{data: data}
	case "sampled":
		data, err := proto.Marshal(&prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: sampled}}})
		testutil.Ok(t, err)
		return spillFixture{data: snappy.Encode(nil, data), contentType: "application/x-protobuf; proto=prometheus.ReadResponse"}
	default:
		return spillFixture{data: bytes.Join(frames, nil), contentType: streamedProtoContentType}
	}
}

func newSpillTestStore(reg *prometheus.Registry, client promclient.HTTPClient, options ...PrometheusStoreOption) (*PrometheusStore, error) {
	base := &url.URL{Scheme: "http", Host: "prometheus.test"}
	return NewPrometheusStore(nil, reg, promclient.NewClient(client, nil, ""), base, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("cluster", "sidecar", "replica", "external") },
		func() (int64, int64) { return 0, -1 }, nil, options...)
}

func spillTestClient(fixture spillFixture, observe func()) promclient.HTTPClient {
	return roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{fixture.contentType}}, Body: &spillTestBody{data: fixture.data, observe: observe}}, nil
	})
}

type spillTestBody struct {
	data     []byte
	position int
	closed   bool
	observe  func()
	atEOF    func()
	readErr  error
}

func (b *spillTestBody) Read(p []byte) (int, error) {
	if b.observe != nil {
		b.observe()
	}
	if b.position == len(b.data) {
		if b.atEOF != nil {
			b.atEOF()
			b.atEOF = nil
		}
		if b.readErr != nil {
			return 0, b.readErr
		}
		return 0, io.EOF
	}
	n := copy(p, b.data[b.position:min(b.position+17, len(b.data))])
	b.position += n
	return n, nil
}

func (b *spillTestBody) Close() error {
	b.closed = true
	return nil
}

type spillTestProbe struct {
	t        *testing.T
	dir      string
	sawFiles bool
}

func (p *spillTestProbe) observe() {
	err := filepath.WalkDir(p.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || path == p.dir {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		wantMode := os.FileMode(0600)
		if entry.IsDir() {
			wantMode = 0700
		} else {
			p.sawFiles = true
		}
		if info.Mode().Perm() != wantMode {
			p.t.Errorf("spill path %s mode=%o, want %o", path, info.Mode().Perm(), wantMode)
		}
		return nil
	})
	if err != nil {
		p.t.Errorf("inspect spill paths: %v", err)
	}
}

type spillTestServer struct {
	*storeSeriesServer
	onSend  func()
	sendErr error
	sends   int
}

func (s *spillTestServer) Send(response *storepb.SeriesResponse) error {
	s.sends++
	if s.onSend != nil {
		s.onSend()
	}
	if s.sendErr != nil {
		return s.sendErr
	}
	return s.storeSeriesServer.Send(response)
}

func assertSpillDirectoryEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(entries))
}

func assertSpillPayloads(t *testing.T, series []storepb.Series, input string) {
	t.Helper()
	for i, item := range series {
		if i > 0 {
			testutil.Assert(t, labels.Compare(labelpb.ZLabelsToPromLabels(series[i-1].Labels), labelpb.ZLabelsToPromLabels(item.Labels)) <= 0, "series are not sorted")
		}
		assertLabelValue(t, item.Labels, "cluster", "sidecar")
		if input != "metadata" {
			assertNoLabel(t, item.Labels, "replica")
		}
		if input == "metadata" {
			testutil.Equals(t, 0, len(item.Chunks))
			continue
		}
		testutil.Assert(t, len(item.Chunks) > 0, "sampled/chunked payload disappeared")
		for _, chunk := range item.Chunks {
			testutil.Assert(t, chunk.Raw != nil, "raw chunk disappeared")
			testutil.Equals(t, xxhash.Sum64(chunk.Raw.Data), chunk.Raw.Hash)
		}
	}
}
