// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"cmp"
	"context"
	"hash"
	"slices"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type prometheusSeries struct {
	labels   []labelpb.ZLabel
	chunks   []prompb.Chunk
	sequence uint64
}

type prometheusSeriesBuffer struct {
	store              *PrometheusStore
	upstream           storepb.Store_SeriesServer
	series             []prometheusSeries
	seriesLimiter      SeriesLimiter
	samplesLimiter     ChunksLimiter
	nextSequence       uint64
	calculateChecksums bool
	err                error
}

func (p *PrometheusStore) newSeriesBuffer(srv storepb.Store_SeriesServer, batchSize int, metadata bool) *prometheusSeriesBuffer {
	b := &prometheusSeriesBuffer{
		store:              p,
		upstream:           newBatchableServer(srv, batchSize),
		calculateChecksums: !metadata && enableChunkHashCalculation,
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
	b.series = append(b.series, prometheusSeries{labels: lset, chunks: chunks, sequence: b.nextSequence})
	b.nextSequence++
	return nil
}

func (b *prometheusSeriesBuffer) Flush() error {
	if b.err != nil {
		return b.err
	}
	defer b.Close()
	if b.err = b.Context().Err(); b.err != nil {
		return b.err
	}
	slices.SortFunc(b.series, func(a, b prometheusSeries) int {
		if c := labels.Compare(labelpb.ZLabelsToPromLabels(a.labels), labelpb.ZLabelsToPromLabels(b.labels)); c != 0 {
			return c
		}
		return cmp.Compare(a.sequence, b.sequence)
	})
	var hasher hash.Hash64
	defer func() {
		if hasher != nil {
			hashPool.Put(hasher)
		}
	}()
	for i := range b.series {
		if b.err = b.Context().Err(); b.err != nil {
			return b.err
		}
		record := &b.series[i]
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
		*record = prometheusSeries{}
		if b.err = b.upstream.Send(storepb.NewSeriesResponse(series)); b.err != nil {
			return b.err
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
	if upstream, ok := b.upstream.(*batchableServer); ok {
		upstream.series = nil
	}
	return nil
}

func (b *prometheusSeriesBuffer) reset() error {
	if err := b.Close(); err != nil {
		return err
	}
	b.err = nil
	b.nextSequence = 0
	b.newLimiters()
	return nil
}

func (b *prometheusSeriesBuffer) newLimiters() {
	if b.store.newSeriesLimiter != nil {
		b.seriesLimiter = b.store.newSeriesLimiter(b.store.failedRequestsCounter.WithLabelValues("series"))
		b.samplesLimiter = b.store.newSamplesLimiter(b.store.failedRequestsCounter.WithLabelValues("chunks"))
	}
}

func (b *prometheusSeriesBuffer) Context() context.Context {
	return b.upstream.Context()
}
