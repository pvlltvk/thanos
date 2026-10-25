// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusSeriesSorterReview(t *testing.T) {
	t.Run("buffers small record writes", func(t *testing.T) {
		sorter := newPrometheusExternalSorter(context.Background(), t.TempDir(), 0)
		defer sorter.Close()
		writes := 0
		open := sorter.ops.openFile
		sorter.ops.openFile = func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
			file, err := open(name, flags, mode)
			if err != nil {
				return nil, err
			}
			return &reviewSortWriteCounter{ReadWriteCloser: file, writes: &writes}, nil
		}
		records := make([]prometheusSeries, 100)
		for i := range records {
			records[i] = sortTestRecord("a", uint64(i))
		}
		testutil.Ok(t, sorter.writeRun(records))
		testutil.Assert(t, writes < len(records), "made %d file writes for %d small records", writes, len(records))
	})
	for _, corruption := range []string{"key size", "payload size", "key checksum", "payload checksum"} {
		t.Run(corruption, func(t *testing.T) {
			dir := t.TempDir()
			sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
			defer sorter.Close()
			testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("a", 1)}))
			file := sortRunFiles(t, dir)[0]
			data, err := os.ReadFile(file)
			testutil.Ok(t, err)
			keySize := binary.LittleEndian.Uint64(data[16:24])
			want := corruption
			switch corruption {
			case "key size":
				binary.LittleEndian.PutUint64(data[16:24], ^uint64(0))
				want = "record size"
			case "payload size":
				binary.LittleEndian.PutUint64(data[24:32], ^uint64(0))
				want = "record size"
			case "key checksum":
				data[48+keySize-1] ^= 1
			case "payload checksum":
				data[48+keySize] ^= 1
			}
			testutil.Ok(t, os.WriteFile(file, data, 0600))
			err = sorter.merge(func(prometheusSeries) error { t.Fatal("corrupt record emitted"); return nil })
			testutil.Assert(t, err != nil && strings.Contains(err.Error(), want), "error %v lacks %q", err, want)
		})
	}
	t.Run("failed cleanup is attempted once", func(t *testing.T) {
		dir := t.TempDir()
		sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
		testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("a", 1)}))
		want := errors.New("remove failed")
		calls := 0
		sorter.ops.removeAll = func(path string) error { calls++; testutil.Ok(t, os.RemoveAll(path)); return want }
		testutil.Assert(t, errors.Is(sorter.Close(), want), "cleanup failure lost")
		testutil.Ok(t, sorter.Close())
		testutil.Equals(t, 1, calls)
		assertSortDirectoryEmpty(t, dir)
	})
}

func TestPrometheusSeriesBufferReview(t *testing.T) {
	t.Run("sampled inputs release before spilling each record", func(t *testing.T) {
		buffer := newTestSeriesBuffer(&reviewSeriesServer{send: func(*storepb.SeriesResponse) error { return nil }}, 0, false)
		buffer.store.logger = log.NewNopLogger()
		buffer.store.seriesSortThreshold = 1
		dir := t.TempDir()
		buffer.store.seriesSortDirectory = dir
		buffer.sorter = newPrometheusExternalSorter(context.Background(), dir, 0)
		entries := []*prompb.TimeSeries{
			{Labels: bufferLabels("a"), Samples: []prompb.Sample{{Timestamp: 1, Value: 1}}},
			{Labels: bufferLabels("b"), Samples: []prompb.Sample{{Timestamp: 2, Value: 2}}},
		}
		originals := append([]*prompb.TimeSeries(nil), entries...)
		open := buffer.sorter.ops.openFile
		seen := 0
		buffer.sorter.ops.openFile = func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
			if flags&os.O_CREATE != 0 {
				for i := 0; i <= seen; i++ {
					testutil.Assert(t, entries[i] == nil, "sampled input %d still retained at spill %d", i, seen)
					testutil.Assert(t, originals[i].Samples == nil, "sample array %d still retained at spill %d", i, seen)
				}
				if seen+1 < len(entries) {
					testutil.Assert(t, entries[seen+1] != nil && len(originals[seen+1].Samples) > 0, "unprocessed input was cleared early")
				}
				seen++
			}
			return open(name, flags, mode)
		}
		testutil.Ok(t, buffer.store.sendSampledResponse(buffer, entries, labels.EmptyLabels(), nil))
		testutil.Equals(t, 2, seen)
	})
	t.Run("slots release before handoff", func(t *testing.T) {
		var buffer *prometheusSeriesBuffer
		var retained []prometheusSeries
		seen := 0
		server := &reviewSeriesServer{send: func(*storepb.SeriesResponse) error {
			for i := 0; i <= seen; i++ {
				testutil.Equals(t, prometheusSeries{}, retained[i])
			}
			seen++
			return nil
		}}
		buffer = newTestSeriesBuffer(server, 0, false)
		for _, value := range []string{"a", "b", "c"} {
			testutil.Ok(t, buffer.add(bufferLabels(value), nil))
		}
		retained = buffer.series
		testutil.Ok(t, buffer.Flush())
		testutil.Equals(t, 3, seen)
	})
	t.Run("cleanup preserves primary grpc status and runs once", func(t *testing.T) {
		primary := status.Error(codes.ResourceExhausted, "downstream full")
		server := &reviewSeriesServer{send: func(*storepb.SeriesResponse) error { return primary }}
		buffer := newTestSeriesBuffer(server, 0, false)
		buffer.store.seriesSortThreshold = 1
		dir := t.TempDir()
		buffer.store.seriesSortDirectory = dir
		buffer.sorter = newPrometheusExternalSorter(context.Background(), dir, 0)
		cleanup := errors.New("remove failed")
		calls := 0
		buffer.sorter.ops.removeAll = func(path string) error { calls++; testutil.Ok(t, os.RemoveAll(path)); return cleanup }
		testutil.Ok(t, buffer.add(bufferLabels("a"), nil))
		err := buffer.Flush()
		testutil.Equals(t, codes.ResourceExhausted, status.Code(err))
		testutil.Assert(t, errors.Is(err, primary) && errors.Is(err, cleanup), "lost primary or cleanup error: %v", err)
		testutil.Ok(t, buffer.Close())
		testutil.Equals(t, 1, calls)
	})
}

type reviewSortWriteCounter struct {
	io.ReadWriteCloser
	writes *int
}

func (f *reviewSortWriteCounter) Write(p []byte) (int, error) {
	*f.writes++
	return f.ReadWriteCloser.Write(p)
}

type reviewSeriesServer struct {
	storepb.Store_SeriesServer
	send func(*storepb.SeriesResponse) error
}

func (s *reviewSeriesServer) Send(r *storepb.SeriesResponse) error { return s.send(r) }
func (s *reviewSeriesServer) Context() context.Context             { return context.Background() }
