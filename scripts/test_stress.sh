#!/usr/bin/env bash
set -euo pipefail

WEAVE_ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$WEAVE_ROOT_DIR"
mode=${1:-test}
case "$mode" in
  test) services=(rabbitmq kafka) ;;
  bench) services=(rabbitmq) ;;
  *) echo "Usage: $0 [test|bench]" >&2; exit 2 ;;
esac
project="weave-stress-$$"
compose=(docker compose -p "$project" -f testdata/stress/compose.yaml)
trap '"${compose[@]}" down --remove-orphans' EXIT
"${compose[@]}" up -d --wait "${services[@]}"

export WEAVE_STRESS_E2E=1
export WEAVE_STRESS_AMQP_ADDRESS=127.0.0.1:5674
export WEAVE_STRESS_RABBITMQ_CONTAINER
WEAVE_STRESS_RABBITMQ_CONTAINER=$("${compose[@]}" ps -q rabbitmq)
if [[ "$mode" == test ]]; then
  export WEAVE_KAFKA_E2E=1
  export WEAVE_KAFKA_BROKERS=127.0.0.1:19092
  export WEAVE_STRESS_KAFKA_CONTAINER
  WEAVE_STRESS_KAFKA_CONTAINER=$("${compose[@]}" ps -q kafka)
  go test -race -count=1 -timeout="${WEAVE_STRESS_TIMEOUT:-15m}" -v ./transport/amqp ./transport/kafka -run '^Test(SustainedLoad|RepeatedBrokerFailures)WithLive'
else
  go test ./transport/amqp -run '^$' -bench '^BenchmarkConfirmedPublishWithLiveRabbitMQ$' \
    -cpu=4 -benchtime="${WEAVE_AMQP_BENCH_TIME:-3s}" -count="${WEAVE_AMQP_BENCH_COUNT:-3}" -benchmem
fi
