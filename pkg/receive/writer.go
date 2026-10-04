// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender(ctx context.Context) (storage.Appender, error)
}

type TenantStorage interface {
	TenantAppendable(string) (Appendable, error)
}

// Wraps storage.Appender to add validation and logging.
type ReceiveAppender struct {
	tLogger        log.Logger
	tooFarInFuture int64 // Unit: nanoseconds
	storage.Appender
}

func (ra *ReceiveAppender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if err := ra.checkTooFarInFuture(lset, t); err != nil {
		return 0, err
	}
	return ra.Appender.Append(ref, lset, t, v)
}

func (ra *ReceiveAppender) checkTooFarInFuture(lset labels.Labels, t int64) error {
	if ra.tooFarInFuture > 0 {
		tooFar := model.Now().Add(time.Duration(ra.tooFarInFuture))
		if tooFar.Before(model.Time(t)) {
			level.Warn(ra.tLogger).Log("msg", "block metric too far in the future", "lset", lset,
				"timestamp", t, "bound", tooFar)
			// now + tooFarInFutureTimeWindow < sample timestamp
			return storage.ErrOutOfBounds
		}
	}
	return nil
}

type WriterOptions struct {
	TooFarInFutureTimeWindow int64 // Unit: nanoseconds
}

type Writer struct {
	logger    log.Logger
	multiTSDB TenantStorage
	opts      *WriterOptions
}

func NewWriter(logger log.Logger, multiTSDB TenantStorage, opts *WriterOptions) *Writer {
	if opts == nil {
		opts = &WriterOptions{}
	}
	return &Writer{
		logger:    logger,
		multiTSDB: multiTSDB,
		opts:      opts,
	}
}

func (r *Writer) Write(ctx context.Context, tenantID string, wreq []prompb.TimeSeries) error {
	tLogger := log.With(r.logger, "tenant", tenantID)

	s, err := r.multiTSDB.TenantAppendable(tenantID)
	if err != nil {
		return errors.Wrap(err, "get tenant appendable")
	}
	dedup, err := newHADedupWriter(r.multiTSDB, tenantID)
	if err == tsdb.ErrNotReady {
		return err
	}
	if err != nil {
		return errors.Wrap(err, "get HA dedup tracker")
	}
	defer dedup.flush()

	app, err := s.Appender(ctx)
	if err == tsdb.ErrNotReady {
		return err
	}
	if err != nil {
		return errors.Wrap(err, "get appender")
	}
	getRef := app.(storage.GetRef)
	var (
		ref          storage.SeriesRef
		errorTracker writeErrorTracker
	)
	ra := &ReceiveAppender{
		tLogger:        tLogger,
		tooFarInFuture: r.opts.TooFarInFutureTimeWindow,
		Appender:       app,
	}
	app = ra

	for _, t := range wreq {
		// Check if time series labels are valid. If not, skip the time series
		// and report the error.
		if err := labelpb.ValidateLabels(t.Labels); err != nil {
			lset := &labelpb.ZLabelSet{Labels: t.Labels}
			errorTracker.addLabelsError(err, lset, tLogger)
			continue
		}

		zlbls, dedupSeries := dedup.prepare(t.Labels, false)
		lset := labelpb.ZLabelsToPromLabels(zlbls)

		// Check if the TSDB has cached reference for those labels.
		ref, lset = getRef.GetRef(lset, lset.Hash())
		if ref == 0 {
			// If not, copy labels, as TSDB will hold those strings long term. Given no
			// copy unmarshal we don't want to keep memory for whole protobuf, only for labels.
			// Do the reallocation here instead of one level higher because this ensures that we
			// do _not_ intern all strings even if they are already exist. This is a high likelihood
			// that this is the case because new series are created much rarer.
			lbls := append([]labelpb.ZLabel(nil), zlbls...)
			labelpb.ReAllocZLabelsStrings(&lbls)
			lset = labelpb.ZLabelsToPromLabels(lbls)
		}

		// Append as many valid samples as possible, but keep track of the errors.
		for _, s := range t.Samples {
			if dedupSeries.dedup {
				// A rejected sample must not take part in the ownership decision, see haDedupWriter.appended.
				if err := ra.checkTooFarInFuture(lset, s.Timestamp); err != nil {
					errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
					continue
				}
			}
			if !dedup.accept(&dedupSeries, ref, s.Timestamp, value.IsStaleNaN(s.Value)) {
				continue
			}
			ref, err = app.Append(ref, lset, s.Timestamp, s.Value)
			dedup.appended(&dedupSeries, ref)
			errorTracker.addSampleError(err, tLogger, lset, s.Timestamp, s.Value)
		}

		for _, hp := range t.Histograms {
			if dedupSeries.dedup {
				if err := ra.checkTooFarInFuture(lset, hp.Timestamp); err != nil {
					errorTracker.addHistogramError(err, tLogger, lset, hp.Timestamp)
					continue
				}
			}
			if !dedup.accept(&dedupSeries, ref, hp.Timestamp, value.IsStaleNaN(hp.Sum)) {
				continue
			}
			var (
				h  *histogram.Histogram
				fh *histogram.FloatHistogram
			)

			if hp.IsFloatHistogram() {
				fh = prompb.FloatHistogramProtoToFloatHistogram(hp)
			} else {
				h = prompb.HistogramProtoToHistogram(hp)
			}

			ref, err = app.AppendHistogram(ref, lset, hp.Timestamp, h, fh)
			dedup.appended(&dedupSeries, ref)
			errorTracker.addHistogramError(err, tLogger, lset, hp.Timestamp)
		}

		if dedup.tableFilled {
			return dedup.abort(app)
		}

		// Current implementation of app.AppendExemplar doesn't create a new series, so it must be already present.
		// We drop the exemplars in case the series doesn't exist.
		if ref != 0 && len(t.Exemplars) > 0 && dedup.acceptExemplars(&dedupSeries, ref) {
			for _, ex := range t.Exemplars {
				labelpb.ReAllocZLabelsStrings(&ex.Labels)
				exLset := labelpb.ZLabelsToPromLabels(ex.Labels)
				exLogger := log.With(tLogger, "exemplarLset", exLset, "exemplar", ex.String())

				if _, err = app.AppendExemplar(ref, lset, exemplar.Exemplar{
					Labels: exLset,
					Value:  ex.Value,
					Ts:     ex.Timestamp,
					HasTs:  true,
				}); err != nil {
					errorTracker.addExemplarError(err, exLogger)
				}
			}
		}
	}

	errs := errorTracker.collectErrors(tLogger)
	if err := app.Commit(); err != nil {
		dedup.forget()
		errs.Add(errors.Wrap(err, "commit samples"))
	}
	return errs.ErrOrNil()
}
