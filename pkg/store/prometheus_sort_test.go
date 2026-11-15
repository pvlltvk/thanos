// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestPrometheusSeriesSorter(t *testing.T) {
	t.Run("empty sorter never creates resources", func(t *testing.T) {
		dir := t.TempDir()
		sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
		calls := 0
		sorter.ops.mkdirTemp = func(string, string) (string, error) { calls++; return "", errors.New("unexpected mkdir") }
		sorter.ops.openFile = func(string, int, os.FileMode) (io.ReadWriteCloser, error) {
			calls++
			return nil, errors.New("unexpected open")
		}
		testutil.Ok(t, sorter.writeRun(nil))
		testutil.Ok(t, sorter.merge(func(prometheusSeries) error { t.Fatal("empty sorter emitted a record"); return nil }))
		testutil.Ok(t, sorter.reset())
		testutil.Ok(t, sorter.Close())
		testutil.Equals(t, 0, calls)
		assertSortDirectoryEmpty(t, dir)
	})

	t.Run("sorts complete labels and stable fragments and owns output", func(t *testing.T) {
		dir := t.TempDir()
		sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
		defer sorter.Close()
		inputs := []prometheusSeries{
			sortTestRecord("z", 0), sortTestRecord("a", 4), sortTestRecord("a", 2),
			{labels: []labelpb.ZLabel{{Name: "a", Value: "x"}, {Name: "z", Value: "x"}}, sequence: 3},
			{labels: []labelpb.ZLabel{{Name: "a", Value: "x"}}, sequence: 5},
		}
		want := append([]prometheusSeries(nil), inputs...)
		slices.SortFunc(want, compareSortTestRecords)
		var got []prometheusSeries
		testutil.Ok(t, sorter.writeRun(inputs[:2]))
		testutil.Ok(t, sorter.writeRun(inputs[2:]))
		testutil.Equals(t, 0, len(got))
		assertSortPrivateResources(t, dir)
		testutil.Ok(t, sorter.merge(func(record prometheusSeries) error { got = append(got, record); return nil }))
		testutil.Equals(t, want, got)
		testutil.Ok(t, sorter.reset())
		testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("replacement", 90)}))
		testutil.Ok(t, sorter.Close())
		testutil.Equals(t, want, got)
		assertSortDirectoryEmpty(t, dir)
	})

	t.Run("many runs bound file descriptors across merge passes", func(t *testing.T) {
		dir := t.TempDir()
		sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
		tracker := trackSortFiles(sorter)
		var want []prometheusSeries
		for i := 0; i < prometheusSortMergeFanIn*prometheusSortMergeFanIn+3; i++ {
			record := sortTestRecord(fmt.Sprintf("%03d", i%19), uint64(i))
			want = append(want, record)
			testutil.Ok(t, sorter.writeRun([]prometheusSeries{record}))
		}
		slices.SortFunc(want, compareSortTestRecords)
		var got []prometheusSeries
		testutil.Ok(t, sorter.merge(func(record prometheusSeries) error { got = append(got, record); return nil }))
		testutil.Equals(t, want, got)
		testutil.Ok(t, sorter.Close())
		testutil.Assert(t, tracker.maxReaders <= prometheusSortMergeFanIn, "opened %d simultaneous input files", tracker.maxReaders)
		testutil.Assert(t, tracker.maxOpen <= prometheusSortMergeFanIn+1, "opened %d simultaneous files", tracker.maxOpen)
		testutil.Assert(t, tracker.readOpens > prometheusSortMergeFanIn*prometheusSortMergeFanIn+3, "fixture did not exercise intermediate merge reads: %d", tracker.readOpens)
		testutil.Equals(t, 0, tracker.open)
		assertSortDirectoryEmpty(t, dir)
	})

	t.Run("merge reads only selected payload before output", func(t *testing.T) {
		dir := t.TempDir()
		sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
		tracker := trackSortFiles(sorter)
		const payloadBytes = 128 * 1024
		for i := range 8 {
			record := sortTestRecord(fmt.Sprintf("%02d", i), uint64(i))
			record.chunks[0].Data = []byte(strings.Repeat("x", payloadBytes))
			testutil.Ok(t, sorter.writeRun([]prometheusSeries{record}))
		}
		calls := 0
		testutil.Ok(t, sorter.merge(func(record prometheusSeries) error {
			if calls == 0 {
				testutil.Assert(t, tracker.readBytes < 3*payloadBytes, "read %d bytes before first output, eagerly consuming unselected payloads", tracker.readBytes)
			}
			calls++
			return nil
		}))
		testutil.Equals(t, 8, calls)
		testutil.Ok(t, sorter.Close())
		assertSortDirectoryEmpty(t, dir)
	})

	t.Run("reset removes pending runs and permits reuse", func(t *testing.T) {
		dir := t.TempDir()
		sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
		testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("discard", 0)}))
		testutil.Ok(t, sorter.reset())
		assertSortDirectoryEmpty(t, dir)
		testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("keep", 1)}))
		var got []prometheusSeries
		testutil.Ok(t, sorter.merge(func(record prometheusSeries) error { got = append(got, record); return nil }))
		testutil.Equals(t, []prometheusSeries{sortTestRecord("keep", 1)}, got)
		testutil.Ok(t, sorter.Close())
		assertSortDirectoryEmpty(t, dir)
	})

	t.Run("requests use separate private directories", func(t *testing.T) {
		dir := t.TempDir()
		const requests = 4
		sorters := make([]*prometheusExternalSorter, requests)
		results := make(chan error, requests)
		var wg sync.WaitGroup
		for i := range sorters {
			sorters[i] = newPrometheusExternalSorter(context.Background(), dir, 0)
			wg.Go(func() { results <- sorters[i].writeRun([]prometheusSeries{sortTestRecord(fmt.Sprint(i), uint64(i))}) })
		}
		wg.Wait()
		close(results)
		for err := range results {
			testutil.Ok(t, err)
		}
		entries, err := os.ReadDir(dir)
		testutil.Ok(t, err)
		testutil.Equals(t, requests, len(entries))
		for i, sorter := range sorters {
			var got []prometheusSeries
			testutil.Ok(t, sorter.merge(func(record prometheusSeries) error { got = append(got, record); return nil }))
			testutil.Equals(t, []prometheusSeries{sortTestRecord(fmt.Sprint(i), uint64(i))}, got)
			testutil.Ok(t, sorter.Close())
		}
		assertSortDirectoryEmpty(t, dir)
	})

	t.Run("budget covers actual files and simultaneous merge output", func(t *testing.T) {
		record := sortTestRecord(strings.Repeat("label", 40), 10)
		record.chunks[0].Data = []byte(strings.Repeat("payload", 100))
		measureDir := t.TempDir()
		measure := newPrometheusExternalSorter(context.Background(), measureDir, 0)
		testutil.Ok(t, measure.writeRun([]prometheusSeries{record}))
		files := sortRunFiles(t, measureDir)
		testutil.Equals(t, 1, len(files))
		info, err := os.Stat(files[0])
		testutil.Ok(t, err)
		runBytes := info.Size()
		testutil.Assert(t, runBytes > 0, "run file is empty")
		testutil.Ok(t, measure.Close())
		for _, mergeBudget := range []bool{false, true} {
			t.Run(fmt.Sprintf("merge_%t", mergeBudget), func(t *testing.T) {
				dir := t.TempDir()
				budget := runBytes - 1
				if mergeBudget {
					budget = runBytes * (prometheusSortMergeFanIn + 1)
				}
				sorter := newPrometheusExternalSorter(context.Background(), dir, budget)
				tracker := trackSortFiles(sorter)
				var err error
				if mergeBudget {
					for i := 0; i < prometheusSortMergeFanIn+1; i++ {
						testutil.Ok(t, sorter.writeRun([]prometheusSeries{record}))
					}
					err = sorter.merge(func(prometheusSeries) error { t.Fatal("budget failure emitted data"); return nil })
				} else {
					err = sorter.writeRun([]prometheusSeries{record})
				}
				testutil.NotOk(t, err)
				testutil.Assert(t, tracker.maxBytes <= budget, "temporary bytes %d exceeded budget %d", tracker.maxBytes, budget)
				testutil.Ok(t, sorter.Close())
				testutil.Equals(t, 0, tracker.open)
				assertSortDirectoryEmpty(t, dir)
			})
		}
	})

	for _, operation := range []string{"mkdir", "create", "write", "read", "close"} {
		t.Run("propagates "+operation+" errors and cleans up", func(t *testing.T) {
			dir := t.TempDir()
			sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
			tracker := trackSortFiles(sorter)
			injected := errors.New("injected " + operation + " failure")
			originalOpen := sorter.ops.openFile
			if operation == "mkdir" {
				sorter.ops.mkdirTemp = func(string, string) (string, error) { return "", injected }
			}
			sorter.ops.openFile = func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
				if operation == "create" && flags&os.O_CREATE != 0 {
					return nil, injected
				}
				file, err := originalOpen(name, flags, mode)
				if err != nil {
					return nil, err
				}
				return &sortFaultFile{ReadWriteCloser: file, operation: operation, err: injected}, nil
			}
			err := sorter.writeRun([]prometheusSeries{sortTestRecord("a", 0)})
			if operation == "read" {
				testutil.Ok(t, err)
				err = sorter.merge(func(prometheusSeries) error { t.Fatal("failed read emitted data"); return nil })
			}
			testutil.Assert(t, errors.Is(err, injected), "lost %s error: %v", operation, err)
			testutil.Ok(t, sorter.Close())
			testutil.Equals(t, 0, tracker.open)
			assertSortDirectoryEmpty(t, dir)
		})
	}

	for _, corrupt := range []string{"truncate", "oversized header"} {
		t.Run("rejects "+corrupt, func(t *testing.T) {
			dir := t.TempDir()
			sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
			testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("a", 0)}))
			files := sortRunFiles(t, dir)
			testutil.Equals(t, 1, len(files))
			content, err := os.ReadFile(files[0])
			testutil.Ok(t, err)
			testutil.Assert(t, len(content) > 1, "run too short for corruption fixture")
			if corrupt == "truncate" {
				content = content[:len(content)-1]
			} else {
				for i := range min(32, len(content)) {
					content[i] = 0xff
				}
			}
			testutil.Ok(t, os.WriteFile(files[0], content, 0600))
			testutil.NotOk(t, sorter.merge(func(prometheusSeries) error { t.Fatal("corrupt record emitted"); return nil }))
			testutil.Ok(t, sorter.Close())
			assertSortDirectoryEmpty(t, dir)
		})
	}

	t.Run("truncated run never reports a clean end of file", func(t *testing.T) {
		dir := t.TempDir()
		for size := int64(0); ; size++ {
			sorter := newPrometheusExternalSorter(context.Background(), dir, 0)
			testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord("a", 0), sortTestRecord("b", 1)}))
			files := sortRunFiles(t, dir)
			testutil.Equals(t, 1, len(files))
			info, err := os.Stat(files[0])
			testutil.Ok(t, err)
			testutil.Ok(t, os.Truncate(files[0], size))
			err = sorter.merge(func(prometheusSeries) error { return nil })
			testutil.Assert(t, !errors.Is(err, io.EOF), "run truncated to %d bytes reported a clean end of file: %v", size, err)
			testutil.Ok(t, sorter.Close())
			assertSortDirectoryEmpty(t, dir)
			if size == info.Size() {
				testutil.Ok(t, err)
				break
			}
			testutil.Assert(t, errors.Is(err, io.ErrUnexpectedEOF) && strings.Contains(err.Error(), "read series sort run"), "run truncated to %d bytes returned %v", size, err)
		}
	})

	for _, phase := range []string{"before write", "during write", "during read", "callback", "callback error"} {
		t.Run("cleans up cancellation or error "+phase, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sorter := newPrometheusExternalSorter(ctx, dir, 0)
			tracker := trackSortFiles(sorter)
			originalOpen := sorter.ops.openFile
			sorter.ops.openFile = func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
				file, err := originalOpen(name, flags, mode)
				if err != nil {
					return nil, err
				}
				return &sortCancelFile{ReadWriteCloser: file, cancel: cancel, onWrite: phase == "during write", onRead: phase == "during read"}, nil
			}
			if phase == "before write" {
				cancel()
			}
			err := sorter.writeRun([]prometheusSeries{sortTestRecord("a", 0), sortTestRecord("b", 1)})
			wantErr := error(context.Canceled)
			if phase == "during read" || phase == "callback" || phase == "callback error" {
				testutil.Ok(t, err)
				if phase == "callback error" {
					wantErr = errors.New("consumer stopped")
				}
				err = sorter.merge(func(prometheusSeries) error {
					if phase == "callback error" {
						return wantErr
					}
					cancel()
					return nil
				})
			}
			testutil.Assert(t, errors.Is(err, wantErr), "lost %s error: %v", phase, err)
			testutil.Ok(t, sorter.Close())
			testutil.Equals(t, 0, tracker.open)
			assertSortDirectoryEmpty(t, dir)
		})
	}

	for _, operation := range []string{"create", "write", "read", "close", "remove", "cancel"} {
		t.Run("intermediate merge pass propagates "+operation+" and cleans up", func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sorter := newPrometheusExternalSorter(ctx, dir, 0)
			tracker := trackSortFiles(sorter)
			for i := 0; i < prometheusSortMergeFanIn+1; i++ {
				testutil.Ok(t, sorter.writeRun([]prometheusSeries{sortTestRecord(fmt.Sprintf("%02d", i), uint64(i))}))
			}
			injected := errors.New("injected " + operation + " failure")
			wantErr := injected
			if operation == "cancel" {
				wantErr = context.Canceled
			}
			originalOpen := sorter.ops.openFile
			sorter.ops.openFile = func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
				if operation == "create" && flags&os.O_CREATE != 0 {
					return nil, injected
				}
				file, err := originalOpen(name, flags, mode)
				if err != nil {
					return nil, err
				}
				if operation == "cancel" && flags&os.O_CREATE != 0 {
					cancel()
				}
				return &sortFaultFile{ReadWriteCloser: file, operation: operation, err: injected}, nil
			}
			if operation == "remove" {
				sorter.ops.remove = func(string) error { return injected }
			}
			err := sorter.merge(func(prometheusSeries) error { t.Fatal("failed intermediate pass emitted data"); return nil })
			testutil.Assert(t, errors.Is(err, wantErr), "lost %s error: %v", operation, err)
			testutil.Ok(t, sorter.Close())
			testutil.Equals(t, 0, tracker.open)
			assertSortDirectoryEmpty(t, dir)
		})
	}
}

func sortTestRecord(value string, sequence uint64) prometheusSeries {
	return prometheusSeries{labels: []labelpb.ZLabel{{Name: "series", Value: value}}, sequence: sequence, chunks: []prompb.Chunk{
		{MinTimeMs: int64(sequence), MaxTimeMs: int64(sequence) + 100, Type: prompb.Chunk_XOR, Data: []byte{byte(sequence), 1, 2}},
		{MinTimeMs: 10, MaxTimeMs: 20, Type: prompb.Chunk_HISTOGRAM, Data: []byte{3, 4}},
		{MinTimeMs: 30, MaxTimeMs: 40, Type: prompb.Chunk_FLOAT_HISTOGRAM, Data: []byte{5, 6}},
	}}
}

func compareSortTestRecords(a, b prometheusSeries) int {
	if c := labels.Compare(labelpb.ZLabelsToPromLabels(a.labels), labelpb.ZLabelsToPromLabels(b.labels)); c != 0 {
		return c
	}
	if a.sequence < b.sequence {
		return -1
	}
	if a.sequence > b.sequence {
		return 1
	}
	return 0
}

func sortRunFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	testutil.Ok(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	}))
	return files
}

func assertSortDirectoryEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(entries))
}

func assertSortPrivateResources(t *testing.T, dir string) {
	t.Helper()
	testutil.Ok(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		want := os.FileMode(0600)
		if entry.IsDir() {
			want = 0700
		}
		testutil.Equals(t, want, info.Mode().Perm())
		return nil
	}))
}

type sortFileTracker struct {
	sync.Mutex
	open, readers, maxOpen, maxReaders, readOpens int
	maxBytes, readBytes                           int64
	directory                                     string
}

func trackSortFiles(sorter *prometheusExternalSorter) *sortFileTracker {
	tracker := &sortFileTracker{directory: sorter.directory}
	original := sorter.ops.openFile
	sorter.ops.openFile = func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
		file, err := original(name, flags, mode)
		if err != nil {
			return nil, err
		}
		reader := flags&(os.O_WRONLY|os.O_RDWR) == 0
		tracker.Lock()
		tracker.open++
		tracker.maxOpen = max(tracker.maxOpen, tracker.open)
		if reader {
			tracker.readers++
			tracker.readOpens++
			tracker.maxReaders = max(tracker.maxReaders, tracker.readers)
		}
		tracker.Unlock()
		return &sortTrackedFile{ReadWriteCloser: file, tracker: tracker, reader: reader}, nil
	}
	return tracker
}

type sortTrackedFile struct {
	io.ReadWriteCloser
	tracker        *sortFileTracker
	reader, closed bool
}

func (f *sortTrackedFile) Read(p []byte) (int, error) {
	n, err := f.ReadWriteCloser.Read(p)
	f.tracker.Lock()
	f.tracker.readBytes += int64(n)
	f.tracker.Unlock()
	return n, err
}

func (f *sortTrackedFile) Write(p []byte) (int, error) {
	n, err := f.ReadWriteCloser.Write(p)
	var size int64
	walkErr := filepath.WalkDir(f.tracker.directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	if walkErr != nil && err == nil {
		return n, walkErr
	}
	f.tracker.Lock()
	f.tracker.maxBytes = max(f.tracker.maxBytes, size)
	f.tracker.Unlock()
	return n, err
}
func (f *sortTrackedFile) Close() error {
	err := f.ReadWriteCloser.Close()
	if !f.closed {
		f.closed = true
		f.tracker.Lock()
		f.tracker.open--
		if f.reader {
			f.tracker.readers--
		}
		f.tracker.Unlock()
	}
	return err
}

type sortFaultFile struct {
	io.ReadWriteCloser
	operation string
	err       error
}

func (f *sortFaultFile) Write(p []byte) (int, error) {
	if f.operation == "write" {
		return 0, f.err
	}
	return f.ReadWriteCloser.Write(p)
}
func (f *sortFaultFile) Read(p []byte) (int, error) {
	if f.operation == "read" {
		return 0, f.err
	}
	return f.ReadWriteCloser.Read(p)
}
func (f *sortFaultFile) Close() error {
	err := f.ReadWriteCloser.Close()
	if f.operation == "close" {
		return f.err
	}
	return err
}

type sortCancelFile struct {
	io.ReadWriteCloser
	cancel          context.CancelFunc
	onWrite, onRead bool
}

func (f *sortCancelFile) Write(p []byte) (int, error) {
	n, err := f.ReadWriteCloser.Write(p)
	if f.onWrite {
		f.cancel()
	}
	return n, err
}
func (f *sortCancelFile) Read(p []byte) (int, error) {
	n, err := f.ReadWriteCloser.Read(p)
	if f.onRead {
		f.cancel()
	}
	return n, err
}
