package brokerstress

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

// ClusterEnabled keeps destructive cluster tests opt-in and separate from single-node fixtures.
func ClusterEnabled(t testing.TB) {
	t.Helper()
	if os.Getenv("WEAVE_CLUSTER_E2E") != "1" {
		t.Skip("run scripts/test_clusters.sh to enable isolated replicated-cluster tests")
	}
}

type clusterSnapshot struct {
	Config struct{ Labels map[string]string }
	State  struct {
		Running bool
		Pid     int
	}
	NetworkSettings struct {
		Networks map[string]struct {
			IPAddress string
			Aliases   []string
		}
	}
}

func inspectClusterNode(ctx context.Context, id string) (clusterSnapshot, error) {
	output, err := docker(ctx, "inspect", id)
	var snapshots []clusterSnapshot
	if err == nil {
		err = json.Unmarshal([]byte(output), &snapshots)
	}
	if err != nil {
		return clusterSnapshot{}, err
	}
	if len(snapshots) != 1 {
		return clusterSnapshot{}, fmt.Errorf("expected one cluster container, got %d", len(snapshots))
	}
	return snapshots[0], nil
}

func validateClusterLabels(containerLabels, networkLabels map[string]string, project, service string) error {
	if !strings.HasPrefix(project, "weave-cluster-") || service == "" {
		return fmt.Errorf("missing isolated cluster project or service")
	}
	for _, labels := range []map[string]string{containerLabels, networkLabels} {
		if labels["dev.weave.cluster"] != "true" || labels["com.docker.compose.project"] != project {
			return fmt.Errorf("refusing to disrupt a container or network outside labeled project %q", project)
		}
	}
	if containerLabels["com.docker.compose.service"] != service {
		return fmt.Errorf("cluster container does not belong to service %q", service)
	}
	return nil
}

// ClusterNode controls only the runner's labeled container and dedicated peer network.
type ClusterNode struct{ id, network, address, alias string }

func NewClusterNode(t *testing.T, backend string, number int) *ClusterNode {
	t.Helper()
	ClusterEnabled(t)
	prefix := backend
	if backend == "rabbitmq" {
		prefix = "rabbit"
	} else if backend != "kafka" {
		t.Fatalf("unknown cluster backend %q", backend)
	}
	ids := strings.Split(os.Getenv("WEAVE_CLUSTER_"+strings.ToUpper(backend)+"_CONTAINERS"), ",")
	if len(ids) != 3 || number < 1 || number > 3 || ids[number-1] == "" {
		t.Fatal("missing three cluster container IDs; run scripts/test_clusters.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := &ClusterNode{id: ids[number-1], network: os.Getenv("WEAVE_CLUSTER_PEER_NETWORK"), alias: fmt.Sprintf("%s%d-peer", prefix, number)}
	snapshot, err := inspectClusterNode(ctx, c.id)
	if err != nil {
		t.Fatal(err)
	}
	output, err := docker(ctx, "network", "inspect", "--format", "{{json .Labels}}", c.network)
	var labels map[string]string
	if err == nil {
		err = json.Unmarshal([]byte(output), &labels)
	}
	if err == nil {
		err = validateClusterLabels(snapshot.Config.Labels, labels, os.Getenv("WEAVE_CLUSTER_PROJECT"), fmt.Sprintf("%s%d", prefix, number))
	}
	if err != nil {
		t.Fatal(err)
	}
	endpoint := snapshot.NetworkSettings.Networks[c.network]
	if net.ParseIP(endpoint.IPAddress) == nil || !slices.Contains(endpoint.Aliases, c.alias) || !snapshot.State.Running {
		t.Fatal("cluster container is not running on its expected peer endpoint")
	}
	c.address = endpoint.IPAddress
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := c.Restore(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}

func (c *ClusterNode) Kill(ctx context.Context) error {
	_, err := docker(ctx, "kill", "--signal", "KILL", c.id)
	return err
}

// Partition removes only peer connectivity; the process and its client network stay up.
func (c *ClusterNode) Partition(ctx context.Context) error {
	before, err := inspectClusterNode(ctx, c.id)
	if err != nil {
		return err
	}
	if _, err := docker(ctx, "network", "disconnect", c.network, c.id); err != nil {
		return err
	}
	after, err := inspectClusterNode(ctx, c.id)
	if err != nil {
		return err
	}
	if !after.State.Running || after.State.Pid != before.State.Pid || len(after.NetworkSettings.Networks) != 1 {
		return fmt.Errorf("partition failed to preserve the running broker and client network")
	}
	return nil
}

// Restore is idempotent and also runs during cleanup after a failed assertion.
func (c *ClusterNode) Restore(ctx context.Context) error {
	snapshot, err := inspectClusterNode(ctx, c.id)
	if err != nil {
		return err
	}
	if _, connected := snapshot.NetworkSettings.Networks[c.network]; !connected {
		if _, err := docker(ctx, "network", "connect", "--ip", c.address, "--alias", c.alias, c.network, c.id); err != nil {
			return err
		}
	}
	if !snapshot.State.Running {
		_, err = docker(ctx, "start", c.id)
	}
	return err
}

// Eventually bounds metadata and replica-convergence checks in both transports.
func Eventually(t *testing.T, ctx context.Context, what string, condition func() bool) {
	t.Helper()
	wait(t, ctx, 90*time.Second, what, condition)
}

// ReplicatedFailure requires progress and delivery before the failed member rejoins.
func ReplicatedFailure(t *testing.T, f Fixture, disrupt, heal func(context.Context)) {
	t.Helper()
	w := start(t, f, 5*time.Minute, true)
	wait(t, w.ctx, 30*time.Second, "healthy traffic before cluster disruption", func() bool {
		return w.published.Load() >= 10 && w.rpcs.Load() >= 10
	})
	resume, entered := w.pause()
	t.Cleanup(resume)
	marker, err := w.publish()
	if err != nil {
		t.Fatalf("publish replicated marker: %v", err)
	}
	select {
	case <-entered:
	case <-w.ctx.Done():
		t.Fatal("consumer did not enter the paused handler")
	}
	started := time.Now()
	disrupt(w.ctx)
	resume()
	Eventually(t, w.ctx, "new confirmed business publish before healing", func() bool {
		_, err := w.publish()
		return err == nil
	})
	Eventually(t, w.ctx, "RPC progress with the failed member still unavailable", func() bool { return w.call(2*time.Second) == nil })
	readyCtx, cancel := context.WithTimeout(w.ctx, 90*time.Second)
	if err := f.Broker.(core.ConsumerReadiness).WaitReady(readyCtx); err != nil {
		cancel()
		t.Fatalf("consumer readiness after leader change: %v", err)
	}
	cancel()
	Eventually(t, w.ctx, "pre-fault confirmed marker delivery before healing", func() bool { return !w.ledger.contains(marker) })
	w.drain(t)
	t.Logf("majority_progress_before_heal=true confirmed_marker_delivered=true failover=%s", time.Since(started).Round(time.Millisecond))
	heal(w.ctx)
	readyCtx, cancel = context.WithTimeout(w.ctx, 90*time.Second)
	if err := f.Broker.(core.ConsumerReadiness).WaitReady(readyCtx); err != nil {
		cancel()
		t.Fatalf("consumer readiness after cluster healing: %v", err)
	}
	cancel()
	Eventually(t, w.ctx, "confirmed publish after cluster healing", func() bool {
		_, err := w.publish()
		return err == nil
	})
	Eventually(t, w.ctx, "RPC recovery after cluster healing", func() bool { return w.call(2*time.Second) == nil })
	w.finish(t)
}
