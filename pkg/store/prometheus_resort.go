// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

const (
	streamingSortExternalLabel = "external_label"
	streamingSortReplicaLabel  = "replica_label"
	streamingSortUnsorted      = "unsorted"

	streamingSortWarnInterval = time.Minute
)

// addSorted sends series as soon as no later series can sort before them instead of
// retaining the whole response. This is only sound when the response is sorted by the
// series' own labels and those never use an external or removed label name: lset is
// then the own labels plus the same labels for every series, so a series can only be
// overtaken by series extending its own labels and b.series stays a short stack with
// descending final labels. received may carry the external labels already, because
// Prometheus merges its configured ones into every frame without overriding own labels.
func (b *prometheusSeriesBuffer) addSorted(received, lset []labelpb.ZLabel, chunks []prompb.Chunk, extLset labels.Labels, extLsetToRemove map[string]struct{}) error {
	if b.err != nil {
		return b.err
	}
	if b.err = b.Context().Err(); b.err != nil {
		return b.err
	}
	if b.err = reserveSeriesAndSamples(b.seriesLimiter, b.samplesLimiter, 1, uint64(len(chunks))); b.err != nil {
		return b.err
	}
	if reason, message := b.streamingSortViolation(received, lset, extLset, extLsetToRemove); reason != "" {
		b.countStreamingSortViolation(reason)
		b.err = status.Error(codes.FailedPrecondition, "streaming series sort cannot be used for this response: "+message)
		return b.err
	}

	held := len(b.series)
	for held > 0 && labels.Compare(labelpb.ZLabelsToPromLabels(b.series[held-1].labels), labelpb.ZLabelsToPromLabels(lset)) < 0 {
		held--
	}
	if b.err = b.emitSorted(b.series[held:]); b.err != nil {
		return b.err
	}
	b.series = append(b.series[:held], prometheusSeries{labels: lset, chunks: chunks, sequence: b.nextSequence})
	b.nextSequence++
	b.maxHeld = max(b.maxHeld, len(b.series))
	return nil
}

// checkStreamingSort reports the first series of a buffered response that addSorted
// would reject, without changing how the response is handled.
func (b *prometheusSeriesBuffer) checkStreamingSort(received, lset []labelpb.ZLabel, extLset labels.Labels, extLsetToRemove map[string]struct{}) {
	if b.sortUnstreamable {
		return
	}
	reason, message := b.streamingSortViolation(received, lset, extLset, extLsetToRemove)
	if reason == "" {
		return
	}
	b.countStreamingSortViolation(reason)
	b.store.warnStreamingSortViolation(reason, labelpb.ZLabelsToPromLabels(received), message)
}

// streamingSortViolation returns why addSorted cannot accept the series after the ones
// seen so far, or an empty reason. The own labels kept for the next call are taken from
// lset, because received is only valid until the next frame is read.
func (b *prometheusSeriesBuffer) streamingSortViolation(received, lset []labelpb.ZLabel, extLset labels.Labels, extLsetToRemove map[string]struct{}) (reason, message string) {
	for _, l := range received {
		if extLset.Has(l.Name) {
			if extLset.Get(l.Name) != l.Value {
				return streamingSortExternalLabel, fmt.Sprintf("series %s has its own value for external label %q", labelpb.ZLabelsToPromLabels(received), l.Name)
			}
			continue
		}
		if _, ok := extLsetToRemove[l.Name]; ok {
			return streamingSortReplicaLabel, fmt.Sprintf("series %s has removed replica label %q as its own label", labelpb.ZLabelsToPromLabels(received), l.Name)
		}
	}
	local := b.local[:0]
	for _, l := range lset {
		if !extLset.Has(l.Name) {
			local = append(local, l)
		}
	}
	if labels.Compare(labelpb.ZLabelsToPromLabels(b.prevLocal), labelpb.ZLabelsToPromLabels(local)) > 0 {
		return streamingSortUnsorted, fmt.Sprintf("response is not sorted by the series' own labels, %s follows %s", labelpb.ZLabelsToPromLabels(local), labelpb.ZLabelsToPromLabels(b.prevLocal))
	}
	b.local, b.prevLocal = b.prevLocal, local
	return "", ""
}

func (b *prometheusSeriesBuffer) countStreamingSortViolation(reason string) {
	if b.sortUnstreamable {
		return
	}
	b.sortUnstreamable = true
	if b.store.seriesSortUnstreamable != nil {
		b.store.seriesSortUnstreamable.WithLabelValues(reason).Inc()
	}
}

func (p *PrometheusStore) warnStreamingSortViolation(reason string, lset labels.Labels, message string) {
	now := time.Now().UnixNano()
	last := p.seriesSortUnstreamableWarnedAt.Load()
	if now-last < int64(streamingSortWarnInterval) || !p.seriesSortUnstreamableWarnedAt.CompareAndSwap(last, now) {
		return
	}
	level.Warn(p.logger).Log(
		"msg", "streamed response cannot be handled by streaming series sort, the request would fail with it enabled",
		"reason", reason,
		"lset", lset.String(),
		"details", message,
	)
}
