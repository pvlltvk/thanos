// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"net/url"
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
