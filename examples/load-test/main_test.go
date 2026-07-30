package main

import (
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	values := make([]time.Duration, 100)
	for i := range values {
		values[i] = time.Duration(i+1) * time.Millisecond
	}

	tests := []struct {
		name   string
		values []time.Duration
		p      float64
		want   time.Duration
	}{
		{name: "empty", p: 0.95},
		{name: "single", values: values[:1], p: 0.99, want: time.Millisecond},
		{name: "p95", values: values, p: 0.95, want: 95 * time.Millisecond},
		{name: "p99", values: values, p: 0.99, want: 99 * time.Millisecond},
		{name: "lower boundary", values: values, p: 0, want: time.Millisecond},
		{name: "upper boundary", values: values, p: 1, want: 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := percentile(tt.values, tt.p); got != tt.want {
				t.Fatalf("percentile() = %s, want %s", got, tt.want)
			}
		})
	}
}
