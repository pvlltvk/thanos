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
	retainedBytes      uint64
	err                error
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
	var hasher hash.Hash64
	defer func() {
		if hasher != nil {
			hashPool.Put(hasher)
		}
	}()
	emit := func(record prometheusSeries) error {
		if err := b.Context().Err(); err != nil {
			return err
		}
		series := &storepb.Series{Labels: record.labels}
		if len(record.chunks) > 0 {
			series.Chunks = make([]storepb.AggrChunk, len(record.chunks))
			if b.calculateChecksums && hasher == nil {
				hasher = hashPool.Get().(hash.Hash64)
			}
		}
		for j, chunk := range record.chunks {
			series.Chunks[j] = storepb.AggrChunk{
				MinTime: chunk.MinTimeMs,
				MaxTime: chunk.MaxTimeMs,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_Encoding(chunk.Type - 1),
					Data: chunk.Data,
					Hash: hashChunk(hasher, chunk.Data, b.calculateChecksums),
				},
			}
		}
		return b.upstream.Send(storepb.NewSeriesResponse(series))
	}
	if b.sorter != nil {
		if b.err = b.spill(); b.err != nil {
			return b.err
		}
		b.err = b.sorter.merge(emit)
		if b.err != nil {
			return b.err
		}
	} else {
		slices.SortFunc(b.series, comparePrometheusSeries)
		for i := range b.series {
			record := b.series[i]
			b.series[i] = prometheusSeries{}
			if b.err = emit(record); b.err != nil {
				return b.err
			}
		}
	}
	b.series = nil
	if upstream, ok := b.upstream.(flushableServer); ok {
		b.err = upstream.Flush()
	}
	return b.err
}

func (b *prometheusSeriesBuffer) Close() error {
	clear(b.series)
	b.series = nil
	b.retainedBytes = 0
	if upstream, ok := b.upstream.(*batchableServer); ok {
		upstream.series = nil
	}
	if b.sorter != nil {
		sorter := b.sorter
		b.sorter = nil
		return sorter.Close()
	}
	return nil
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
