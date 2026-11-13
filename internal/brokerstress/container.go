package brokerstress

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type container struct {
	id      string
	stopped bool
}

func docker(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %v: %w: %s", args, err, output)
	}
	return strings.TrimSpace(string(output)), nil
}

func newContainer(t *testing.T, service string) *container {
	t.Helper()
	id := os.Getenv("WEAVE_STRESS_" + strings.ToUpper(service) + "_CONTAINER")
	if id == "" {
		t.Fatal("missing isolated stress container ID; run scripts/test_stress.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	label, err := docker(ctx, "inspect", "--format", `{{index .Config.Labels "dev.weave.stress"}}`, id)
	if err != nil || label != "true" {
		t.Fatalf("refusing to stop a container without the dev.weave.stress label: %q, %v", label, err)
	}
	c := &container{id: id}
	t.Cleanup(func() {
		if c.stopped {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := docker(ctx, "start", c.id); err != nil {
				t.Error(err)
			}
		}
	})
	return c
}

func (c *container) kill(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := docker(ctx, "kill", "--signal", "KILL", c.id); err != nil {
		t.Fatal(err)
	}
	c.stopped = true
}

func (c *container) start(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := docker(ctx, "start", c.id); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, time.Minute, "broker container health", func() bool {
		status, err := docker(ctx, "inspect", "--format", "{{.State.Health.Status}}", c.id)
		return err == nil && status == "healthy"
	})
	c.stopped = false
}
