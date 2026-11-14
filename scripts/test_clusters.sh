#!/usr/bin/env bash
set -euo pipefail

WEAVE_ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$WEAVE_ROOT_DIR"
case "${1:-all}" in
  all) backends=(amqp kafka) ;;
  amqp|kafka) backends=("$1") ;;
  *) echo "Usage: $0 [all|amqp|kafka]" >&2; exit 2 ;;
esac

export WEAVE_CLUSTER_E2E=1
export WEAVE_CLUSTER_PROJECT="weave-cluster-$$"
export WEAVE_CLUSTER_PEER_NETWORK="${WEAVE_CLUSTER_PROJECT}_peers"
compose=(docker compose -p "$WEAVE_CLUSTER_PROJECT" -f testdata/cluster/compose.yaml)
cleanup() {
  cluster_status=$?
  trap - EXIT
  if [[ "$cluster_status" != 0 ]]; then
    "${compose[@]}" logs --no-color --tail=80 >&2 || true
  fi
  "${compose[@]}" down --volumes --remove-orphans || true
  exit "$cluster_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for backend in "${backends[@]}"; do
  if [[ "$backend" == amqp ]]; then
    services=(rabbit1 rabbit2 rabbit3)
    "${compose[@]}" build rabbit1
    "${compose[@]}" up -d --wait rabbit1
    "${compose[@]}" up -d --wait rabbit2 rabbit3
  else
    services=(kafka1 kafka2 kafka3)
    "${compose[@]}" up -d --wait "${services[@]}"
  fi
  ids=()
  for service in "${services[@]}"; do
    ids+=("$("${compose[@]}" ps -q "$service")")
  done
  container_ids=$(IFS=,; echo "${ids[*]}")
  if [[ "$backend" == amqp ]]; then
    export WEAVE_CLUSTER_RABBITMQ_CONTAINERS="$container_ids"
  else
    export WEAVE_CLUSTER_KAFKA_CONTAINERS="$container_ids"
  fi
  go test -race -count=1 -timeout="${WEAVE_CLUSTER_TIMEOUT:-12m}" -v "./transport/$backend" \
    -run '^Test(ReplicatedFailover|NetworkPartition|EndpointFailover)WithCluster'
  # Keep peak memory bounded by running one three-node cluster at a time.
  "${compose[@]}" down --volumes --remove-orphans
done
