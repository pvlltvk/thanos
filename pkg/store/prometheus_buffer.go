// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"hash"
	"slices"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type prometheusSeries struct {
	labels   []labelpb.ZLabel
	chunks   []prompb.Chunk
	sequence uint64
}

// Metadata callbacks precede envelope validation, so their limit failures remain provisional.
type prometheusDeferredCounter struct {
	prometheus.Counter
	pending bool
}

func (c *prometheusDeferredCounter) Inc() {
	c.pending = true
}

type prometheusSeriesBuffer struct {
	store              *PrometheusStore
	upstream           storepb.Store_SeriesServer
	series             []prometheusSeries
	seriesLimiter      SeriesLimiter
	samplesLimiter     ChunksLimiter
	nextSequence       uint64
	calculateChecksums bool
	metadata           bool
	limitCounters      *[2]prometheusDeferredCounter
	sorter             *prometheusExternalSorter
	sortSpilled        bool
	sortFailed         bool
	sortUnstreamable   bool
	retainedBytes      uint64
	err                error
	hasher             hash.Hash64
	prevLocal, local   []labelpb.ZLabel
	maxHeld            int
}

func (p *PrometheusStore) newSeriesBuffer(srv storepb.Store_SeriesServer, batchSize int, metadata bool) *prometheusSeriesBuffer {
	b := &prometheusSeriesBuffer{
		store:              p,
		upstream:           newBatchableServer(srv, batchSize),
		calculateChecksums: !metadata && enableChunkHashCalculation,
		metadata:           metadata,
	}
	b.newLimiters()
	return b
}

func (b *prometheusSeriesBuffer) add(lset []labelpb.ZLabel, chunks []prompb.Chunk) error {
	if b.err != nil {
		return b.err
	}
	if b.err = b.Context().Err(); b.err != nil {
		return b.err
	}
	if b.err = reserveSeriesAndSamples(b.seriesLimiter, b.samplesLimiter, 1, uint64(len(chunks))); b.err != nil {
		return b.err
	}
	record := prometheusSeries{labels: lset, chunks: chunks, sequence: b.nextSequence}
	var size uint64
	if b.store.seriesSortThreshold > 0 {
		size = uint64(unsafe.Sizeof(record)) + uint64(cap(lset))*uint64(unsafe.Sizeof(labelpb.ZLabel{})) + uint64(cap(chunks))*uint64(unsafe.Sizeof(prompb.Chunk{}))
		for _, label := range lset {
			size += uint64(len(label.Name)) + uint64(len(label.Value))
		}
		for _, chunk := range chunks {
			size += uint64(cap(chunk.Data))
		}
		if len(b.series) > 0 && size+b.retainedBytes > uint64(b.store.seriesSortThreshold) {
			if b.err = b.spill(); b.err != nil {
				return b.err
			}
		}
	}
	b.series = append(b.series, record)
	b.retainedBytes += size
	b.nextSequence++
	if b.store.seriesSortThreshold > 0 && b.retainedBytes >= uint64(b.store.seriesSortThreshold) {
		b.err = b.spill()
	}
	return b.err
}

func (b *prometheusSeriesBuffer) spill() error {
	if len(b.series) == 0 {
		return nil
	}
	if b.sorter == nil {
		b.sorter = newPrometheusExternalSorter(b.Context(), b.store.seriesSortDirectory, int64(b.store.seriesSortMaxBytes))
	}
	if err := b.sorter.writeRun(b.series); err != nil {
		b.countSortFailure(err)
		return err
	}
	clear(b.series)
	b.series = nil
	b.retainedBytes = 0
	return nil
}

func (b *prometheusSeriesBuffer) Flush() (err error) {
	defer closePrometheusSeriesResource(&err, b)
	if b.err != nil {
		return b.err
	}
	if b.err = b.Context().Err(); b.err != nil {
		return b.err
	}
	if b.sorter != nil {
		if b.err = b.spill(); b.err != nil {
			return b.err
		}
		var sendErr error
		b.err = b.sorter.merge(func(record prometheusSeries) error {
			sendErr = b.emit(record)
			return sendErr
		})
		if b.err != nil {
			if sendErr == nil {
				b.countSortFailure(b.err)
			}
			return b.err
		}
	} else if b.err = b.emitSorted(b.series); b.err != nil {
		return b.err
	}
	b.series = nil
	if upstream, ok := b.upstream.(flushableServer); ok {
		b.err = upstream.Flush()
	}
	return b.err
}

func (b *prometheusSeriesBuffer) emitSorted(records []prometheusSeries) error {
	slices.SortFunc(records, comparePrometheusSeries)
	for i := range records {
		record := records[i]
		records[i] = prometheusSeries{}
		if err := b.emit(record); err != nil {
			return err
		}
	}
	return nil
}

func (b *prometheusSeriesBuffer) emit(record prometheusSeries) error {
	if err := b.Context().Err(); err != nil {
		return err
	}
	series := &storepb.Series{Labels: record.labels}
	if len(record.chunks) > 0 {
		series.Chunks = make([]storepb.AggrChunk, len(record.chunks))
		if b.calculateChecksums && b.hasher == nil {
			b.hasher = hashPool.Get().(hash.Hash64)
		}
	}
	for j, chunk := range record.chunks {
		series.Chunks[j] = storepb.AggrChunk{
			MinTime: chunk.MinTimeMs,
			MaxTime: chunk.MaxTimeMs,
			Raw: &storepb.Chunk{
				Type: storepb.Chunk_Encoding(chunk.Type - 1),
				Data: chunk.Data,
				Hash: hashChunk(b.hasher, chunk.Data, b.calculateChecksums),
			},
		}
	}
	return b.upstream.Send(storepb.NewSeriesResponse(series))
}

func (b *prometheusSeriesBuffer) Close() error {
	if b.hasher != nil {
		hashPool.Put(b.hasher)
		b.hasher = nil
	}
	b.prevLocal, b.local = nil, nil
	clear(b.series)
	b.series = nil
	b.retainedBytes = 0
	if upstream, ok := b.upstream.(*batchableServer); ok {
		upstream.series = nil
	}
	if b.sorter != nil {
		sorter := b.sorter
		b.sorter = nil
		if b.store.seriesSortSpilledRequests != nil {
			if sorter.runs > 0 && !b.sortSpilled {
				b.sortSpilled = true
				b.store.seriesSortSpilledRequests.Inc()
			}
			b.store.seriesSortSpilledBytes.Add(float64(sorter.written))
		}
		err := sorter.Close()
		if err != nil {
			b.countSortFailure(err)
		}
		return err
	}
	return nil
}

func (b *prometheusSeriesBuffer) countSortFailure(err error) {
	if b.sortFailed || b.store.seriesSortFailedRequests == nil || errors.Is(err, b.Context().Err()) {
		return
	}
	b.sortFailed = true
	reason := "io"
	if errors.Is(err, errPrometheusSortSpillLimit) {
		reason = "spill_size"
	}
	b.store.seriesSortFailedRequests.WithLabelValues(reason).Inc()
}

func (b *prometheusSeriesBuffer) reset() error {
	if len(b.series) == 0 && b.nextSequence == 0 && b.err == nil {
		return nil
	}
	if err := b.Close(); err != nil {
		return err
	}
	b.err = nil
	b.nextSequence = 0
	b.newLimiters()
	return nil
}

func (b *prometheusSeriesBuffer) newLimiters() {
	b.seriesLimiter, b.samplesLimiter = nil, nil
	b.limitCounters = nil
	if b.store.newSeriesLimiter == nil {
		return
	}
	seriesCounter := b.store.failedRequestsCounter.WithLabelValues("series")
	samplesCounter := b.store.failedRequestsCounter.WithLabelValues("chunks")
	if b.metadata {
		b.limitCounters = &[2]prometheusDeferredCounter{{Counter: seriesCounter}, {Counter: samplesCounter}}
		seriesCounter = &b.limitCounters[0]
		samplesCounter = &b.limitCounters[1]
	}
	b.seriesLimiter = b.store.newSeriesLimiter(seriesCounter)
	b.samplesLimiter = b.store.newSamplesLimiter(samplesCounter)
}

func (b *prometheusSeriesBuffer) finishMetadata(err error) error {
	if err != nil && b.err != nil && b.limitCounters != nil && errors.Is(err, b.err) {
		for i := range b.limitCounters {
			counter := &b.limitCounters[i]
			if counter.pending {
				counter.Counter.Inc()
				counter.pending = false
			}
		}
	}
	return err
}

func (b *prometheusSeriesBuffer) Context() context.Context {
	return b.upstream.Context()
}
