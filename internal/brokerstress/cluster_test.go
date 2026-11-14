package brokerstress

import (
	"maps"
	"testing"
)

func TestClusterDisruptionRequiresFixtureOwnership(t *testing.T) {
	project := "weave-cluster-test"
	container := map[string]string{"dev.weave.cluster": "true", "com.docker.compose.project": project, "com.docker.compose.service": "kafka1"}
	network := map[string]string{"dev.weave.cluster": "true", "com.docker.compose.project": project}
	if err := validateClusterLabels(container, network, project, "kafka1"); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(map[string]string, map[string]string){
		func(c, n map[string]string) { delete(c, "dev.weave.cluster") },
		func(c, n map[string]string) { delete(n, "dev.weave.cluster") },
		func(c, n map[string]string) { c["com.docker.compose.project"] = "another-project" },
		func(c, n map[string]string) { n["com.docker.compose.project"] = "another-project" },
		func(c, n map[string]string) { c["com.docker.compose.service"] = "kafka2" },
	} {
		c, n := maps.Clone(container), maps.Clone(network)
		mutate(c, n)
		if err := validateClusterLabels(c, n, project, "kafka1"); err == nil {
			t.Fatal("accepted a foreign or unlabeled container/network")
		}
	}
	if err := validateClusterLabels(container, network, "", "kafka1"); err == nil {
		t.Fatal("accepted a missing project")
	}
	if err := validateClusterLabels(container, network, "production", "kafka1"); err == nil {
		t.Fatal("accepted a non-fixture project")
	}
}
