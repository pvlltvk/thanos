// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/time/rate"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	thanosmodel "github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rulesbackfill"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type rulesBackfillConfig struct {
	queryURL         *url.URL
	ruleFiles        []string
	start            *thanosmodel.TimeOrDurationValue
	end              *thanosmodel.TimeOrDurationValue
	evalInterval     time.Duration
	maxBlockDuration time.Duration
	externalLabels   []string
	dryRun           bool
	tmpDir           string
	dedup            bool
	partialResponse  bool
	maxSourceRes     string
	queryTimeout     time.Duration
	retryAttempts    int
	queryRateLimit   float64
	hashFunc         string
	tenantHeader     string
	tenantID         string
}

func registerRulesBackfill(app extkingpin.AppClause) {
	cmd := app.Command("rules-backfill", "Backfill recording rules against historical Thanos data and upload blocks to object storage.")

	conf := &rulesBackfillConfig{}

	cmd.Flag("rules", "The rule files glob to backfill (repeated).").
		Required().StringsVar(&conf.ruleFiles)

	queryURLStr := cmd.Flag("query", "Thanos Query HTTP endpoint URL.").
		Required().String()

	conf.start = thanosmodel.TimeOrDuration(
		cmd.Flag("start", "Start of the backfill time range (RFC3339 or relative duration like -180d).").Required())

	conf.end = thanosmodel.TimeOrDuration(
		cmd.Flag("end", "End of the backfill time range (RFC3339 or relative duration like -3h). Default is 3h ago.").
			Default("-3h"))

	cmd.Flag("eval-interval", "Evaluation interval for rules.").
		Default("60s").DurationVar(&conf.evalInterval)

	cmd.Flag("max-block-duration", "Maximum duration for a single TSDB block.").
		Default("2h").DurationVar(&conf.maxBlockDuration)

	cmd.Flag("label", "External labels to attach to blocks (repeated, format key=\"value\").").
		Required().StringsVar(&conf.externalLabels)

	cmd.Flag("dry-run", "Create blocks locally but do not upload to object storage.").
		Default("false").BoolVar(&conf.dryRun)

	cmd.Flag("tmp-dir", "Temporary directory for block creation.").
		Default(os.TempDir()).StringVar(&conf.tmpDir)

	cmd.Flag("dedup", "Enable deduplication in queries.").
		Default("true").BoolVar(&conf.dedup)

	cmd.Flag("partial-response", "Enable partial response in queries.").
		Default("false").BoolVar(&conf.partialResponse)

	cmd.Flag("max-source-resolution", "Maximum source resolution for queries (0s, 5m, 1h).").
		Default("0s").StringVar(&conf.maxSourceRes)

	cmd.Flag("query-timeout", "Timeout for individual query requests.").
		Default("5m").DurationVar(&conf.queryTimeout)

	cmd.Flag("retry-attempts", "Number of retry attempts for failed queries.").
		Default("3").IntVar(&conf.retryAttempts)

	cmd.Flag("query-rate-limit", "Maximum queries per second against the Query API.").
		Default("10").Float64Var(&conf.queryRateLimit)

	cmd.Flag("hash-func", "Hash function to use for block verification (e.g. SHA256). Empty means none.").
		Default("").StringVar(&conf.hashFunc)

	cmd.Flag("tenant-header", "HTTP header name for multi-tenancy (e.g. THANOS-TENANT). Empty means no tenant header.").
		Default("").StringVar(&conf.tenantHeader)

	cmd.Flag("tenant", "Tenant ID value to send via the tenant header.").
		Default("").StringVar(&conf.tenantID)

	// Object store config is not marked as required at flag-registration time because
	// --dry-run mode does not need it. Validation happens inside Setup.
	objStoreConfig := extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		// Parse query URL.
		parsedURL, err := url.Parse(*queryURLStr)
		if err != nil {
			return errors.Wrap(err, "parse query URL")
		}
		conf.queryURL = parsedURL

		// Parse external labels.
		if len(conf.externalLabels) == 0 {
			return errors.New("no external labels configured; uniquely identifying external labels must be configured")
		}
		lset, err := parseFlagLabels(conf.externalLabels)
		if err != nil {
			return errors.Wrap(err, "parse external labels")
		}

		// Resolve time range.
		startTime := conf.start.PrometheusTimestamp()
		endTime := conf.end.PrometheusTimestamp()
		if startTime >= endTime {
			return errors.Errorf("start time (%d) must be before end time (%d)", startTime, endTime)
		}

		// Build query options.
		partialResponseStrategy := storepb.PartialResponseStrategy_ABORT
		if conf.partialResponse {
			partialResponseStrategy = storepb.PartialResponseStrategy_WARN
		}

		queryOpts := promclient.QueryOptions{
			Deduplicate:             conf.dedup,
			PartialResponseStrategy: partialResponseStrategy,
			MaxSourceResolution:     conf.maxSourceRes,
		}

		// Add tenant header if configured.
		if conf.tenantHeader != "" {
			if conf.tenantID == "" {
				return errors.New("--tenant is required when --tenant-header is set")
			}
			queryOpts.HTTPHeaders = http.Header{}
			queryOpts.HTTPHeaders.Set(conf.tenantHeader, conf.tenantID)
		}

		// Create promclient (no tracing — this is a batch CLI tool).
		promClient := promclient.NewClient(&http.Client{
			Timeout: conf.queryTimeout,
		}, logger, "thanos-rules-backfill")

		// Wrap QueryRange with retries and exponential backoff.
		queryFunc := func(ctx context.Context, query string, startTimeMs, endTimeMs, stepSec int64) (model.Matrix, []string, error) {
			var (
				matrix   model.Matrix
				warnings []string
				lastErr  error
			)
			for attempt := 0; attempt <= conf.retryAttempts; attempt++ {
				if attempt > 0 {
					backoff := time.Duration(1<<uint(attempt-1)) * time.Second // 1s, 2s, 4s, ...
					level.Debug(logger).Log("msg", "retrying query", "attempt", attempt, "backoff", backoff, "query", query)
					select {
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					case <-time.After(backoff):
					}
				}
				matrix, warnings, _, lastErr = promClient.QueryRange(ctx, conf.queryURL, query, startTimeMs, endTimeMs, stepSec, queryOpts)
				if lastErr == nil {
					return matrix, warnings, nil
				}
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
			}
			return nil, nil, errors.Wrapf(lastErr, "query failed after %d attempts", conf.retryAttempts+1)
		}

		// Create bucket client (may be nil for dry-run).
		var bkt objstore.Bucket
		if !conf.dryRun {
			confContentYaml, err := objStoreConfig.Content()
			if err != nil {
				return errors.Wrap(err, "parse objstore config")
			}
			if len(confContentYaml) == 0 {
				return errors.New("object storage configuration is required when not using --dry-run")
			}
			bkt, err = client.NewBucket(logger, confContentYaml, component.RulesBackfill.String(), nil)
			if err != nil {
				return errors.Wrap(err, "create bucket client")
			}
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			bkt = objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))
		}

		// Configure hash function.
		var hashFunc metadata.HashFunc
		switch conf.hashFunc {
		case "SHA256":
			hashFunc = metadata.SHA256Func
		default:
			hashFunc = metadata.NoneFunc
		}

		// Build Backfiller options.
		var opts []rulesbackfill.Option
		opts = append(opts, rulesbackfill.WithMaxBlockDuration(conf.maxBlockDuration))
		opts = append(opts, rulesbackfill.WithEvalInterval(conf.evalInterval))
		opts = append(opts, rulesbackfill.WithDryRun(conf.dryRun))
		opts = append(opts, rulesbackfill.WithHashFunc(hashFunc))
		opts = append(opts, rulesbackfill.WithRateLimiter(rate.NewLimiter(rate.Limit(conf.queryRateLimit), 1)))

		backfiller := rulesbackfill.New(logger, queryFunc, bkt, lset, conf.tmpDir, opts...)

		startT := time.UnixMilli(startTime)
		endT := time.UnixMilli(endTime)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			ids, err := backfiller.Run(ctx, conf.ruleFiles, startT, endT)
			if err != nil {
				return errors.Wrap(err, "rules backfill")
			}
			level.Info(logger).Log("msg", "rules backfill completed successfully", "blocks", len(ids))
			return nil
		}, func(error) {
			cancel()
		})

		return nil
	})
}
