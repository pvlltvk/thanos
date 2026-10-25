// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import "github.com/prometheus/prometheus/model/labels"

func transformStoreLabels(builder *labels.Builder, lset, externalLabels labels.Labels, labelsToRemove map[string]struct{}) labels.Labels {
	builder.Reset(lset)
	externalLabels.Range(func(l labels.Label) {
		builder.Set(l.Name, l.Value)
	})
	for name := range labelsToRemove {
		builder.Del(name)
	}
	lset = builder.Labels()
	if externalLabels.IsEmpty() && len(labelsToRemove) == 0 {
		return lset.Copy()
	}
	return lset
}
