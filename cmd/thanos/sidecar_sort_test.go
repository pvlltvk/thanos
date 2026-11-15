// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"net/url"
	"strings"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/alecthomas/units"
	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store"
)

func TestSidecarSeriesSortFlags(t *testing.T) {
	for _, tc := range []struct {
		name                string
		args                []string
		threshold, maxBytes units.Base2Bytes
		directory           string
		invalid             bool
	}{
		{name: "default disabled"},
		{name: "enabled", args: []string{"--store.series-sort-spill-threshold=64MB", "--store.series-sort-spill-dir=/chosen/sort", "--store.series-sort-max-spill-size=4GB"}, threshold: 64 * units.MiB, maxBytes: 4 * units.GiB, directory: "/chosen/sort"},
		{name: "explicit disabled", args: []string{"--store.series-sort-spill-threshold=0", "--store.series-sort-max-spill-size=0"}},
		{name: "threshold at floor", args: []string{"--store.series-sort-spill-threshold=1MB", "--store.series-sort-spill-dir=/chosen/sort"}, threshold: units.MiB, directory: "/chosen/sort"},
		{name: "threshold below floor", args: []string{"--store.series-sort-spill-threshold=1023KB", "--store.series-sort-spill-dir=/chosen/sort"}, invalid: true},
		{name: "missing directory", args: []string{"--store.series-sort-spill-threshold=1MB"}, invalid: true},
		{name: "negative threshold", args: []string{"--store.series-sort-spill-threshold=-1"}, invalid: true},
		{name: "negative disk budget", args: []string{"--store.series-sort-max-spill-size=-1"}, invalid: true},
		{name: "invalid bytes", args: []string{"--store.series-sort-spill-threshold=banana"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := kingpin.New("test", "test")
			var conf sidecarConfig
			conf.registerFlag(app)
			_, err := app.Parse(tc.args)
			if err == nil {
				err = conf.validateSeriesSort()
			}
			if err == nil {
				_, err = store.NewPrometheusStore(nil, prometheus.NewRegistry(), nil, &url.URL{}, component.Sidecar, nil, nil, nil,
					store.WithPrometheusStoreSorting(conf.seriesSortThreshold, conf.seriesSortDirectory, conf.seriesSortMaxBytes))
			}
			testutil.Equals(t, tc.invalid, err != nil)
			if tc.invalid {
				return
			}
			testutil.Equals(t, tc.threshold, conf.seriesSortThreshold)
			testutil.Equals(t, tc.maxBytes, conf.seriesSortMaxBytes)
			testutil.Equals(t, tc.directory, conf.seriesSortDirectory)
		})
	}
}

func TestSidecarSeriesSortNegativeUnits(t *testing.T) {
	for _, flag := range []string{"--store.series-sort-spill-threshold=-1B", "--store.series-sort-max-spill-size=-1MB"} {
		t.Run(flag, func(t *testing.T) {
			app := kingpin.New("test", "test")
			var conf sidecarConfig
			conf.registerFlag(app)
			_, err := app.Parse([]string{flag})
			testutil.Ok(t, err)
			_, err = store.NewPrometheusStore(nil, prometheus.NewRegistry(), nil, &url.URL{}, component.Sidecar, nil, nil, nil,
				store.WithPrometheusStoreSorting(conf.seriesSortThreshold, conf.seriesSortDirectory, conf.seriesSortMaxBytes))
			testutil.Assert(t, err != nil && strings.Contains(err.Error(), "must not be negative"), "unexpected validation result: %v", err)
		})
	}
}

func TestSidecarSeriesSortThresholdFloor(t *testing.T) {
	for _, tc := range []struct {
		threshold string
		invalid   bool
	}{
		{threshold: "0"},
		{threshold: "1B", invalid: true},
		{threshold: "1048575B", invalid: true},
		{threshold: "1MB"},
		{threshold: "64MB"},
	} {
		t.Run(tc.threshold, func(t *testing.T) {
			app := kingpin.New("test", "test")
			var conf sidecarConfig
			conf.registerFlag(app)
			_, err := app.Parse([]string{"--store.series-sort-spill-threshold=" + tc.threshold})
			testutil.Ok(t, err)
			err = conf.validateSeriesSort()
			if !tc.invalid {
				testutil.Ok(t, err)
				return
			}
			testutil.Assert(t, err != nil && strings.Contains(err.Error(), "--store.series-sort-spill-threshold must be 0 or at least 1MiB"), "unexpected validation result: %v", err)
		})
	}
}
