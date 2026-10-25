// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"cmp"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

const prometheusSortMergeFanIn = 16
const prometheusSortMagic = "THSORT01"

type prometheusSortFileOps struct {
	openFile  func(string, int, os.FileMode) (io.ReadWriteCloser, error)
	mkdirTemp func(string, string) (string, error)
	remove    func(string) error
	removeAll func(string) error
}

type prometheusExternalSorter struct {
	ctx                 context.Context
	directory           string
	maxBytes            int64
	ops                 prometheusSortFileOps
	requestDir          string
	pass, runs, records uint64
	bytes               int64
	maxKey, maxPayload  uint64
}

func newPrometheusExternalSorter(ctx context.Context, dir string, maxBytes int64) *prometheusExternalSorter {
	return &prometheusExternalSorter{
		ctx: ctx, directory: dir, maxBytes: maxBytes,
		ops: prometheusSortFileOps{
			openFile: func(name string, flags int, mode os.FileMode) (io.ReadWriteCloser, error) {
				return os.OpenFile(name, flags, mode)
			},
			mkdirTemp: os.MkdirTemp, remove: os.Remove, removeAll: os.RemoveAll,
		},
	}
}

func comparePrometheusSeries(a, b prometheusSeries) int {
	if c := labels.Compare(labelpb.ZLabelsToPromLabels(a.labels), labelpb.ZLabelsToPromLabels(b.labels)); c != 0 {
		return c
	}
	return cmp.Compare(a.sequence, b.sequence)
}

func (s *prometheusExternalSorter) path(pass, index uint64) string {
	return filepath.Join(s.requestDir, fmt.Sprintf("%d-%d", pass, index))
}

func (s *prometheusExternalSorter) write(w io.Writer, data []byte) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if int64(len(data)) > int64(^uint64(0)>>1)-s.bytes || s.maxBytes > 0 && int64(len(data)) > s.maxBytes-s.bytes {
		return errors.New("series sort spill size limit exceeded")
	}
	n, err := w.Write(data)
	s.bytes += int64(n)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return s.ctx.Err()
}

func (s *prometheusExternalSorter) createRun(pass, index, count uint64) (_ io.ReadWriteCloser, err error) {
	if err = s.ctx.Err(); err != nil {
		return nil, err
	}
	if s.requestDir == "" {
		s.requestDir, err = s.ops.mkdirTemp(s.directory, "thanos-series-sort-")
		if err != nil {
			return nil, err
		}
	}
	file, err := s.ops.openFile(s.path(pass, index), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	var header [16]byte
	copy(header[:], prometheusSortMagic)
	binary.LittleEndian.PutUint64(header[8:], count)
	if err = s.write(file, header[:]); err != nil {
		closePrometheusSeriesResource(&err, file)
		return nil, err
	}
	return file, nil
}

func (s *prometheusExternalSorter) writeRun(records []prometheusSeries) (err error) {
	if len(records) == 0 {
		return nil
	}
	if err = s.ctx.Err(); err != nil {
		return err
	}
	slices.SortFunc(records, comparePrometheusSeries)
	file, err := s.createRun(0, s.runs, uint64(len(records)))
	if err != nil {
		return err
	}
	defer closePrometheusSeriesResource(&err, file)
	for _, record := range records {
		if err = s.ctx.Err(); err != nil {
			return err
		}
		key, err := (&prompb.ChunkedSeries{Labels: record.labels}).Marshal()
		if err != nil {
			return err
		}
		payload, err := (&prompb.ChunkedSeries{Chunks: record.chunks}).Marshal()
		if err != nil {
			return err
		}
		s.maxKey = max(s.maxKey, uint64(len(key)))
		s.maxPayload = max(s.maxPayload, uint64(len(payload)))
		var header [32]byte
		binary.LittleEndian.PutUint64(header[:], uint64(len(key)))
		binary.LittleEndian.PutUint64(header[8:], uint64(len(payload)))
		binary.LittleEndian.PutUint64(header[16:], record.sequence)
		binary.LittleEndian.PutUint32(header[24:], crc32.Update(crc32.ChecksumIEEE(header[:24]), crc32.IEEETable, key))
		binary.LittleEndian.PutUint32(header[28:], crc32.ChecksumIEEE(payload))
		for _, part := range [][]byte{header[:], key, payload} {
			if err = s.write(file, part); err != nil {
				return err
			}
		}
	}
	s.runs++
	s.records += uint64(len(records))
	return s.ctx.Err()
}

type prometheusSortReader struct {
	file      io.ReadWriteCloser
	reader    *bufio.Reader
	remaining uint64
	count     uint64
	bytesRead int64
	header    [32]byte
	key       []byte
	record    prometheusSeries
}

func (s *prometheusExternalSorter) read(r *prometheusSortReader, data []byte) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	n, err := io.ReadFull(r.reader, data)
	r.bytesRead += int64(n)
	if err != nil {
		return err
	}
	return s.ctx.Err()
}

func (s *prometheusExternalSorter) next(r *prometheusSortReader) error {
	if r.remaining == 0 {
		var trailing [1]byte
		err := s.read(r, trailing[:])
		if err == io.EOF {
			r.key, r.record = nil, prometheusSeries{}
			return nil
		}
		if err != nil {
			return err
		}
		return errors.New("trailing data in series sort run")
	}
	if err := s.read(r, r.header[:]); err != nil {
		return err
	}
	keySize := binary.LittleEndian.Uint64(r.header[:])
	payloadSize := binary.LittleEndian.Uint64(r.header[8:])
	if keySize > s.maxKey || payloadSize > s.maxPayload {
		return errors.New("invalid series sort record size")
	}
	key := make([]byte, int(keySize))
	if err := s.read(r, key); err != nil {
		return err
	}
	if crc32.Update(crc32.ChecksumIEEE(r.header[:24]), crc32.IEEETable, key) != binary.LittleEndian.Uint32(r.header[24:]) {
		return errors.New("invalid series sort key checksum")
	}
	var decoded prompb.ChunkedSeries
	if err := decoded.Unmarshal(key); err != nil {
		return errors.Wrap(err, "decode series sort key")
	}
	if len(decoded.Chunks) != 0 {
		return errors.New("invalid series sort key")
	}
	r.key = key
	r.record = prometheusSeries{labels: decoded.Labels, sequence: binary.LittleEndian.Uint64(r.header[16:])}
	r.remaining--
	return nil
}

type prometheusSortHeap []*prometheusSortReader

func (h prometheusSortHeap) Len() int { return len(h) }
func (h prometheusSortHeap) Less(i, j int) bool {
	return comparePrometheusSeries(h[i].record, h[j].record) < 0
}
func (h prometheusSortHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *prometheusSortHeap) Push(x any)   { *h = append(*h, x.(*prometheusSortReader)) }
func (h *prometheusSortHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return x
}

func (s *prometheusExternalSorter) mergeRuns(start, count uint64, outputIndex uint64, emit func(prometheusSeries) error) (err error) {
	readers := make([]*prometheusSortReader, 0, int(count))
	defer func() {
		for _, reader := range readers {
			if reader.file != nil {
				closePrometheusSeriesResource(&err, reader.file)
			}
		}
	}()
	var total uint64
	for i := start; i < start+count; i++ {
		file, err := s.ops.openFile(s.path(s.pass, i), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		r := &prometheusSortReader{file: file, reader: bufio.NewReaderSize(file, 16*1024)}
		readers = append(readers, r)
		var header [16]byte
		if err = s.read(r, header[:]); err != nil {
			return err
		}
		r.count = binary.LittleEndian.Uint64(header[8:])
		if string(header[:8]) != prometheusSortMagic || r.count == 0 || r.count > s.records-total {
			return errors.New("invalid series sort run header")
		}
		total += r.count
		r.remaining = r.count
		if err = s.next(r); err != nil {
			return err
		}
	}
	var output io.ReadWriteCloser
	if emit == nil {
		output, err = s.createRun(s.pass+1, outputIndex, total)
		if err != nil {
			return err
		}
		defer func() {
			if output != nil {
				closePrometheusSeriesResource(&err, output)
			}
		}()
	}
	heads := prometheusSortHeap(append([]*prometheusSortReader(nil), readers...))
	heap.Init(&heads)
	copyBuffer := make([]byte, 32*1024)
	for len(heads) > 0 {
		r := heap.Pop(&heads).(*prometheusSortReader)
		record := r.record
		payloadSize := binary.LittleEndian.Uint64(r.header[8:])
		checksum := crc32.NewIEEE()
		var payload []byte
		if output != nil {
			if err = s.write(output, r.header[:]); err != nil {
				return err
			}
			if err = s.write(output, r.key); err != nil {
				return err
			}
			for remaining := payloadSize; remaining > 0; {
				part := copyBuffer[:min(uint64(len(copyBuffer)), remaining)]
				if err = s.read(r, part); err != nil {
					return err
				}
				_, _ = checksum.Write(part)
				if err = s.write(output, part); err != nil {
					return err
				}
				remaining -= uint64(len(part))
			}
		} else {
			payload = make([]byte, int(payloadSize))
			if err = s.read(r, payload); err != nil {
				return err
			}
			_, _ = checksum.Write(payload)
		}
		if checksum.Sum32() != binary.LittleEndian.Uint32(r.header[28:]) {
			return errors.New("invalid series sort payload checksum")
		}
		if err = s.next(r); err != nil {
			return err
		}
		if r.key != nil {
			if comparePrometheusSeries(record, r.record) > 0 {
				return errors.New("out of order series sort run")
			}
			heap.Push(&heads, r)
		}
		if output == nil {
			var decoded prompb.ChunkedSeries
			if err = decoded.Unmarshal(payload); err != nil {
				return errors.Wrap(err, "decode series sort payload")
			}
			if len(decoded.Labels) != 0 {
				return errors.New("invalid series sort payload")
			}
			record.chunks = decoded.Chunks
			if err = emit(record); err != nil {
				return err
			}
		}
		if err = s.ctx.Err(); err != nil {
			return err
		}
	}
	if output != nil {
		closePrometheusSeriesResource(&err, output)
		output = nil
		for _, reader := range readers {
			closePrometheusSeriesResource(&err, reader.file)
			reader.file = nil
		}
		if err != nil {
			return err
		}
		for i, reader := range readers {
			if err = s.ops.remove(s.path(s.pass, start+uint64(i))); err != nil {
				return err
			}
			s.bytes -= reader.bytesRead
		}
	}

	return nil
}

func (s *prometheusExternalSorter) merge(emit func(prometheusSeries) error) error {
	if s.runs == 0 {
		return s.ctx.Err()
	}
	for s.runs > prometheusSortMergeFanIn {
		var nextRuns uint64
		for start := uint64(0); start < s.runs; start += prometheusSortMergeFanIn {
			count := min(uint64(prometheusSortMergeFanIn), s.runs-start)
			// Inputs stay on disk until their merged output has been closed successfully.
			if err := s.mergeRuns(start, count, nextRuns, nil); err != nil {
				return err
			}
			nextRuns++
		}
		s.pass++
		s.runs = nextRuns
	}
	return s.mergeRuns(0, s.runs, 0, emit)
}

func (s *prometheusExternalSorter) Close() error {
	if s.requestDir != "" {
		if err := s.ops.removeAll(s.requestDir); err != nil {
			return err
		}
	}
	s.requestDir = ""
	s.pass, s.runs, s.records, s.bytes, s.maxKey, s.maxPayload = 0, 0, 0, 0, 0, 0
	return nil
}

func (s *prometheusExternalSorter) reset() error { return s.Close() }

func closePrometheusSeriesResource(err *error, closer io.Closer) {
	if closeErr := closer.Close(); closeErr != nil {
		var errs errutil.MultiError
		errs.Add(*err)
		errs.Add(errors.Wrap(closeErr, "close Prometheus series resource"))
		*err = errs.Err()
	}
}
