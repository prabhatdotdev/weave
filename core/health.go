package core

import (
	"context"
	"time"
)

// HealthStatus represents coarse-grained component health.
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// HealthReport captures a runtime health snapshot.
type HealthReport struct {
	Timestamp time.Time
	Status    HealthStatus
	Backend   string
	Component string
	Connected bool
	Started   bool
	Details   map[string]any
}

// HealthReporter receives health snapshots for external reporting.
type HealthReporter interface {
	ReportHealth(ctx context.Context, report HealthReport)
}

// HealthHook receives health snapshots as a lightweight callback.
type HealthHook func(ctx context.Context, report HealthReport)

// EmitHealth sends a health report to all configured health sinks.
func (c *Config) EmitHealth(ctx context.Context, report HealthReport) {
	if c == nil {
		return
	}
	if report.Timestamp.IsZero() {
		report.Timestamp = time.Now().UTC()
	}
	report.Details = cloneFields(report.Details)
	if c.HealthReporter != nil {
		c.HealthReporter.ReportHealth(ctx, report)
	}
	if c.HealthHook != nil {
		c.HealthHook(ctx, report)
	}
}
