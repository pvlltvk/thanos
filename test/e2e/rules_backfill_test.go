// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

// TestRulesBackfill verifies the end-to-end flow of `thanos tools rules-backfill`:
//   - Prometheus scrapes itself so the `up` metric is available.
//   - A Thanos sidecar exposes the Prometheus TSDB over gRPC.
//   - A Thanos Store GW holds any previously uploaded blocks.
//   - A Thanos Querier fans out to both the sidecar and store GW.
//   - The backfill tool evaluates `sum(up)` over the scraped range, creates
//     TSDB blocks, and uploads them to MinIO.
//   - The test asserts that at least one block with the expected external labels
//     appears in the bucket.
//   - A second identical run verifies that no duplicate blocks are created
//     (idempotency check).
func TestRulesBackfill(t *testing.T) {
	t.Parallel()

	// Docker network names are limited in length; keep the environment name short.
	e, err := e2e.NewDockerEnvironment("rbf")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// -------------------------------------------------------------------------
	// MinIO
	// -------------------------------------------------------------------------
	const bucket = "rules-backfill"
	m := e2edb.NewMinio(e, "thanos", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type:   objstore.S3,
		Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("http"), m.InternalDir()),
	}

	// -------------------------------------------------------------------------
	// Prometheus (scrapes itself) + sidecar
	// -------------------------------------------------------------------------
	promCfg := e2ethanos.DefaultPromConfig("prom1", 0, "", "", e2ethanos.LocalPrometheusTarget)
	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(
		e, "prom1", promCfg, "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	// -------------------------------------------------------------------------
	// Store GW + Querier
	// -------------------------------------------------------------------------
	storeGW := e2ethanos.NewStoreGW(e, "1", svcConfig, "", "", nil)
	testutil.Ok(t, e2e.StartAndWaitReady(storeGW))

	querier := e2ethanos.NewQuerierBuilder(e, "1",
		sidecar.InternalEndpoint("grpc"),
		storeGW.InternalEndpoint("grpc"),
	).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	// -------------------------------------------------------------------------
	// Wait until the `up` metric is available through the querier.
	// -------------------------------------------------------------------------
	logger := log.NewLogfmtLogger(os.Stdout)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(waitCancel)

	querierURL, err := url.Parse("http://" + querier.Endpoint("http"))
	testutil.Ok(t, err)

	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, waitCtx.Done(), func() error {
		res, _, _, queryErr := promclient.NewDefaultClient().QueryInstant(
			waitCtx, querierURL, "up", time.Now(), promclient.QueryOptions{Deduplicate: true})
		if queryErr != nil {
			return queryErr
		}
		if len(res) == 0 {
			return fmt.Errorf("no results yet for 'up' query through querier")
		}
		return nil
	}))

	// -------------------------------------------------------------------------
	// Rule file — written to the shared directory so it is visible inside
	// containers (SharedDir is mounted identically on host and containers).
	// -------------------------------------------------------------------------
	rulesDir := filepath.Join(e.SharedDir(), "rules")
	testutil.Ok(t, os.MkdirAll(rulesDir, 0755))

	ruleContent := `groups:
  - name: test_group
    rules:
      - record: test:up:sum
        expr: sum(up)
`
	rulesFilePath := filepath.Join(rulesDir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(rulesFilePath, []byte(ruleContent), 0644))

	// -------------------------------------------------------------------------
	// Bucket config YAML for the backfill tool (uses the internal endpoint so
	// the tool container can reach MinIO over the shared Docker network).
	// -------------------------------------------------------------------------
	bktCfgBytes, err := yaml.Marshal(svcConfig)
	testutil.Ok(t, err)

	// -------------------------------------------------------------------------
	// Time range: cover the last ~10 minutes minus a 3-minute consistency
	// buffer so that Prometheus has had time to write the data.
	// -------------------------------------------------------------------------
	backfillEnd := time.Now().UTC().Add(-3 * time.Minute)
	backfillStart := backfillEnd.Add(-7 * time.Minute)

	startFlag := backfillStart.Format(time.RFC3339)
	endFlag := backfillEnd.Format(time.RFC3339)

	// The querier's internal endpoint is used because the runner container is
	// on the same Docker network as all other services.
	queryURL := "http://" + querier.InternalEndpoint("http")

	// -------------------------------------------------------------------------
	// Runner container — kept alive so we can exec commands inside it.
	// -------------------------------------------------------------------------
	runner := e.Runnable("backfill-runner").
		Init(e2e.StartOptions{
			Image:   e2ethanos.DefaultImage(),
			Command: e2e.NewCommandRunUntilStop(),
			User:    fmt.Sprintf("%d", os.Getuid()),
		})
	testutil.Ok(t, e2e.StartAndWaitReady(runner))

	backfillArgs := []string{
		"rules-backfill",
		"--rules=" + rulesFilePath,
		"--query=" + queryURL,
		"--start=" + startFlag,
		"--end=" + endFlag,
		`--label=cluster="e2e-backfill"`,
		"--eval-interval=30s",
		"--objstore.config=" + string(bktCfgBytes),
	}

	// -------------------------------------------------------------------------
	// First run
	// -------------------------------------------------------------------------
	testutil.Ok(t, runner.Exec(e2e.NewCommand("tools", backfillArgs...)))

	// -------------------------------------------------------------------------
	// Verify blocks were uploaded — use a host-accessible endpoint for MinIO.
	// -------------------------------------------------------------------------
	hostBktCfg := e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.Dir())
	bkt, err := s3.NewBucketWithConfig(logger, hostBktCfg, "test-verify", nil)
	testutil.Ok(t, err)

	var uploadedIDs []ulid.ULID
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(verifyCancel)

	// The tool may have just finished; give MinIO a moment and then poll until
	// at least one block directory appears.
	testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, verifyCtx.Done(), func() error {
		uploadedIDs = nil
		iterErr := bkt.Iter(verifyCtx, "", func(name string) error {
			rawID := strings.TrimSuffix(name, "/")
			id, parseErr := ulid.Parse(rawID)
			if parseErr != nil {
				// Not a ULID-named directory (e.g. MinIO internals) — skip.
				return nil
			}
			uploadedIDs = append(uploadedIDs, id)
			return nil
		})
		if iterErr != nil {
			return iterErr
		}
		if len(uploadedIDs) == 0 {
			return fmt.Errorf("no blocks found in bucket yet; waiting for upload to complete")
		}
		return nil
	}))

	testutil.Assert(t, len(uploadedIDs) > 0,
		"expected at least one block to be uploaded to the bucket after backfill")

	// Verify each uploaded block carries the expected external label.
	for _, id := range uploadedIDs {
		meta, dlErr := block.DownloadMeta(verifyCtx, logger, bkt, id)
		testutil.Ok(t, dlErr)
		testutil.Equals(t, "e2e-backfill", meta.Thanos.Labels["cluster"])
	}

	// -------------------------------------------------------------------------
	// Verify that the backfilled metric is actually queryable through the
	// Querier once the Store GW syncs the newly uploaded blocks.
	// -------------------------------------------------------------------------
	queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(queryCancel)

	// The Store GW needs to resync to discover the new blocks. Poll until
	// the metric appears or we time out.
	testutil.Ok(t, runutil.RetryWithLog(logger, 10*time.Second, queryCtx.Done(), func() error {
		res, _, _, queryErr := promclient.NewDefaultClient().QueryInstant(
			queryCtx, querierURL, `test:up:sum{cluster="e2e-backfill"}`, backfillEnd, promclient.QueryOptions{Deduplicate: true})
		if queryErr != nil {
			return queryErr
		}
		if len(res) == 0 {
			return fmt.Errorf("backfilled metric test:up:sum not yet queryable through Querier; waiting for Store GW sync")
		}
		return nil
	}))

	// -------------------------------------------------------------------------
	// Idempotency: second identical run must not create new blocks.
	// -------------------------------------------------------------------------
	testutil.Ok(t, runner.Exec(e2e.NewCommand("tools", backfillArgs...)))

	// Allow a brief window for any (unexpected) writes to land.
	time.Sleep(3 * time.Second)

	var secondRunIDs []ulid.ULID
	idempCtx, idempCancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(idempCancel)

	testutil.Ok(t, bkt.Iter(idempCtx, "", func(name string) error {
		rawID := strings.TrimSuffix(name, "/")
		id, parseErr := ulid.Parse(rawID)
		if parseErr != nil {
			return nil
		}
		secondRunIDs = append(secondRunIDs, id)
		return nil
	}))

	testutil.Equals(t, len(uploadedIDs), len(secondRunIDs),
		"idempotency check: second backfill run must not upload additional blocks")
}
