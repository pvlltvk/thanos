// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
)

func TestSharedStoreGatewayLabels(t *testing.T) {
	t.Run("empty external labels preserve empty local values without removals", func(t *testing.T) {
		local := labels.FromStrings("empty", "", "job", "api")
		client := &blockSeriesClient{lset: local, extLset: labels.EmptyLabels()}
		got := client.seriesLabels()
		testutil.Equals(t, local, got)
	})

	t.Run("non-nil empty removal map trims empty local values", func(t *testing.T) {
		client := &blockSeriesClient{
			lset:            labels.FromStrings("empty", "", "job", "api"),
			extLset:         labels.EmptyLabels(),
			extLsetToRemove: map[string]struct{}{},
		}
		testutil.Equals(t, labels.FromStrings("job", "api"), client.seriesLabels())
	})

	t.Run("external labels override then removals apply", func(t *testing.T) {
		client := &blockSeriesClient{
			lset:            labels.FromStrings("cluster", "local", "job", "api", "replica", "one"),
			extLset:         labels.FromStrings("cluster", "external", "region", "west"),
			extLsetToRemove: map[string]struct{}{"replica": {}},
		}
		testutil.Equals(t, labels.FromStrings("cluster", "external", "job", "api", "region", "west"), client.seriesLabels())
	})

	t.Run("constructor-filtered empty external values preserve local values", func(t *testing.T) {
		client := &blockSeriesClient{
			lset:            labels.FromStrings("cluster", "local", "job", "api"),
			extLset:         labels.EmptyLabels(),
			extLsetToRemove: map[string]struct{}{},
		}
		testutil.Equals(t, labels.FromStrings("cluster", "local", "job", "api"), client.seriesLabels())
	})

	t.Run("result remains independent while client labels and decode builder change", func(t *testing.T) {
		client := &blockSeriesClient{
			lset:    labels.FromStrings("cluster", "local", "job", "api"),
			extLset: labels.EmptyLabels(),
			b:       labels.NewBuilder(labels.FromStrings("decode", "original")),
		}
		firstInput := client.lset
		first := client.seriesLabels()
		client.lset = labels.FromStrings("cluster", "next", "job", "another")
		second := client.seriesLabels()
		client.b.Set("decode", "changed")
		client.b.Set("job", "builder value")

		testutil.Equals(t, labels.FromStrings("cluster", "local", "job", "api"), first)
		testutil.Equals(t, labels.FromStrings("cluster", "next", "job", "another"), second)
		first[0].Value = "changed output"
		testutil.Equals(t, labels.FromStrings("cluster", "next", "job", "another"), second)
		testutil.Equals(t, labels.FromStrings("cluster", "local", "job", "api"), firstInput)
		testutil.Equals(t, labels.FromStrings("cluster", "next", "job", "another"), client.lset)
	})

	t.Run("external-label builder reuse preserves prior results and inputs", func(t *testing.T) {
		client := &blockSeriesClient{
			lset:            labels.FromStrings("cluster", "local", "job", "api", "replica", "one"),
			extLset:         labels.FromStrings("cluster", "external", "region", "west"),
			extLsetToRemove: map[string]struct{}{"replica": {}},
			b:               labels.NewBuilder(labels.FromStrings("decode", "original")),
		}
		firstInput := client.lset
		first := client.seriesLabels()
		wantFirst := labels.FromStrings("cluster", "external", "job", "api", "region", "west")
		testutil.Equals(t, wantFirst, first)

		client.lset = labels.FromStrings("cluster", "next", "job", "another", "replica", "two")
		secondInput := client.lset
		second := client.seriesLabels()
		wantSecond := labels.FromStrings("cluster", "external", "job", "another", "region", "west")
		testutil.Equals(t, wantSecond, second)
		client.b.Set("decode", "changed")
		client.b.Set("job", "builder value")

		first[0].Value = "changed output"
		testutil.Equals(t, wantSecond, second)
		testutil.Equals(t, labels.FromStrings("cluster", "local", "job", "api", "replica", "one"), firstInput)
		testutil.Equals(t, labels.FromStrings("cluster", "next", "job", "another", "replica", "two"), secondInput)
	})
}
