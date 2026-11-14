# Load, failure recovery, and AMQP throughput

## Live stress tests

Run from the repository root with Docker Compose available:

```sh
./scripts/test_stress.sh
```

The runner creates a separate Compose project using RabbitMQ 4.3.4 and Kafka
4.3.1 on loopback ports 5674 and 19092. It removes its containers and network
on exit. Failure tests refuse to stop containers without the fixture's
`dev.weave.stress=true` label.

Both transports run the same workload: four publishers and four RPC callers,
approximately 1 KiB bodies, message identity and payload verification, manual
consumer acknowledgments, persistent AMQP messages, and a pending RPC limit
of 16. The sustained phase must complete without publish or RPC errors.
The fixtures use full-jitter connection backoff from 50 ms up to a 1 s cap so
the crash/recovery phase exercises configurable reconnect timing.
The Kafka fixture has one broker and one partition per topic.
Every confirmed event must reach its handler before the final drain. The
delivery ledger retains unfinished attempts rather than the full run history.
Traffic must finish with no registered RPCs, pending RPC metrics, or active
handlers before shutdown; graceful shutdown must also complete within its deadline.

The failure phase runs three cycles. Each cycle pauses an active event handler,
publishes a confirmed marker, sends SIGKILL to the actual broker process, and
checks that an RPC fails within its deadline. Traffic continues through the
outage. After restarting the same container and waiting for health, the test
requires successful RPCs and delivery of the confirmed marker. Errors and
duplicate deliveries are allowed during outages; missing confirmed messages,
corrupted responses, failed recovery, and stranded work fail the test.

The default load duration is 30 seconds per transport. Longer runs use the same
assertions:

```sh
WEAVE_STRESS_DURATION=10m WEAVE_STRESS_CYCLES=5 WEAVE_STRESS_TIMEOUT=20m \
  ./scripts/test_stress.sh
```

CI runs 30 seconds of healthy traffic and three failures per transport with
the race detector. These single-node process crashes preserve container data;
they do not test host power loss, disk loss, replication failover, or a network
partition between cluster members.

## Replicated-cluster failover and network partitions

```sh
./scripts/test_clusters.sh
# Run one transport:
./scripts/test_clusters.sh amqp
./scripts/test_clusters.sh kafka
```

These opt-in tests create a separately labeled, three-node cluster per transport
using RabbitMQ 4.3.4 and Kafka 4.3.1. The runner requires Docker Compose 2.33.1
or later, runs the clusters sequentially to bound memory, and removes their
containers, networks, and fixture volumes on exit. CI runs each transport as a
separate matrix job with the race detector. Unit tests skip these fixtures unless
`WEAVE_CLUSTER_E2E=1`; use the runner to supply their ownership and network IDs.

RabbitMQ exposes AMQP on loopback ports 25671–25673 and its majority-side
management API on 25682. Business queues are explicitly provisioned as quorum
queues with three members. The fixture enables RabbitMQ's documented
[queue-type redeclaration compatibility setting](https://www.rabbitmq.com/docs/quorum-queues#relaxed-property-equivalence)
because the library's subscription declarations omit queue-type arguments.
RPC reply queues remain connection-local classic queues. The leader-crash and
peer-partition cases connect to node 2 while node 1 initially leads the quorum
queues, exercising queue failover through a surviving connection. A third AMQP
case configures all three endpoints, verifies the actual socket is connected to
node 1, and kills that connected node. It requires a connection to a different
configured endpoint, restored subscriptions, confirmed delivery, readiness, and
working RPCs while node 1 stays stopped. RPC reply consumers must be recreated
on the new connection. Healing must preserve the healthy replacement connection.
This case allows 90 seconds to drain confirmed events because RabbitMQ's
[disconnected-consumer timeout](https://www.rabbitmq.com/docs/quorum-queues#disconnected-consumer-node-timeout)
can retain outstanding deliveries for 60 seconds after a consumer node is lost.
It still requires every confirmed event to complete before restarting that node.

Kafka uses three combined KRaft broker/controller nodes and exposes client
listeners on loopback ports 29091–29093. Event, request,
reply, and probe topics each have one partition, three replicas, and
`min.insync.replicas=2`. The library publishes with `RequiredAcks=-1`. Explicit
replica placement makes node 1 the initial partition leader, and internal
consumer offsets are also replicated across three brokers.

Both transports run these failures under concurrent publish/RPC traffic:

1. Kill the actual initial leader with SIGKILL and leave it stopped while the
   remaining two members elect leaders and process confirmed work.
2. Disconnect that leader only from the dedicated peer network. Its process
   stays running, and its separate client network and published port remain
   reachable. Verify majority leadership and replication, reject a minority
   write, and require majority traffic to recover before reconnecting the peer
   network. Kafka's minority probe sends directly to the isolated broker so
   metadata routing cannot send the probe to a healthy member.

All cases retain a confirmed pre-fault marker behind a paused handler, then
require its delivery, intact identity and payload, new confirmed business
publishes, successful RPCs, consumer readiness, delivery of all confirmed events,
and zero pending RPCs and active handlers
before healing. The restarted or reconnected member must then rejoin every
quorum queue or return to every topic's in-sync replica set. A fresh publish
and RPC must succeed after healing, followed by graceful shutdown.
Failure cleanup restores disrupted nodes even when an assertion fails. Docker
control refuses containers or networks without the fixture label and matching
Compose project/service ownership.

This covers one failed or isolated member with a surviving majority. It does
not cover majority loss, asymmetric packet loss, disk/host loss, or TLS during
cluster faults. Process-crash fixtures retain broker data. The single-node load
and benchmark results below describe their original fixtures, not these clusters.

## AMQP publisher benchmark

```sh
./scripts/test_stress.sh bench
```

This runs Go benchmarks without the race detector, with `GOMAXPROCS=4`, three
samples per scenario, and at least three measured seconds per sample. Override
`WEAVE_AMQP_BENCH_TIME` or `WEAVE_AMQP_BENCH_COUNT` for longer measurements.

The benchmark publishes persistent messages with mandatory routing to a durable
classic queue and waits for every broker confirmation. There is no consumer;
queue inspection must account for all `b.N` confirmed messages. Broker setup,
queue inspection, and cleanup are outside the timer. Results include library
encoding, ownership copies, contention, socket I/O, and broker confirmation;
they measure accepted publishes, not RPC or consumer processing throughput.
Connections use plaintext AMQP over Docker's loopback port mapping.

Scenarios use 1 KiB bodies with 1, 8, or 32 concurrent publishers on one
connection, 32 publishers across four connections, and 64 KiB bodies with eight
publishers on one connection. AMQP currently permits one outstanding confirmed
publish per connection, so the benchmark makes this limit visible without
changing delivery guarantees.

CI runs one short sample per scenario and checks correctness. Throughput has no
hard CI threshold because runner resources and storage latency vary.

## Local verification, 13 November 2026

This archived run used the original fixed reconnect delays. Current stress
fixtures enable jittered backoff; reruns can have different recovery timings.

The one-minute run used an Apple M4 host (10 cores, 24 GiB RAM), macOS 27.0.1,
Go 1.25.5 on arm64, and Docker Engine 29.8.0 on Linux/aarch64. Docker had ten
CPUs and approximately 7.75 GiB RAM available. Both transports ran concurrently
with the race detector. Their workload totals are test evidence, not a
comparison of isolated transport throughput.

```sh
WEAVE_STRESS_DURATION=60s ./scripts/test_stress.sh
```

| Transport | Confirmed events | Successful RPCs | Healthy-run errors | Undelivered confirmed events |
| --- | ---: | ---: | ---: | ---: |
| AMQP | 23,171 | 11,585 | 0 | 0 |
| Kafka | 255,082 | 58,364 | 0 | 0 |

All three hard-restart cycles passed for each transport. RabbitMQ recovery took
5.842–5.931 seconds per cycle; Kafka took 25.068–28.535 seconds. These intervals
include the deliberate two-second hold, startup, client recovery, an RPC probe,
and confirmed-marker completion. Expected failed attempts during outages are
excluded from the healthy-run table. Pending RPC registrations, pending RPC
metrics, and active handlers reached zero before shutdown and remained zero
after shutdown.

The AMQP benchmark ran separately from the load/failure tests, using the same
host and Docker resources, `GOMAXPROCS=4`, and no race detector. Transport
implementation: `2d45f18`. Values below are medians and ranges of three samples;
each sample measured at least three seconds of publishing.

| Body | Concurrent publishers | Connections | Median confirmed messages/s | Sample range |
| --- | ---: | ---: | ---: | ---: |
| 1 KiB | 1 | 1 | 2,606 | 2,602–2,621 |
| 1 KiB | 8 | 1 | 2,583 | 2,557–2,591 |
| 1 KiB | 32 | 1 | 2,611 | 2,590–2,612 |
| 1 KiB | 32 | 4 | 7,439 | 7,243–7,578 |
| 64 KiB | 8 | 1 | 1,865 | 1,861–1,906 |

Increasing concurrency on one connection did not improve throughput in this
setup. Four connections at concurrency 32 achieved approximately 2.85 times
the throughput of one connection at the same concurrency. The 64 KiB case
delivered approximately 122 MB/s of payload at its median. In concurrent Go
benchmark output, `ns/op` is amortized elapsed time per confirmed message;
it is not an individual caller's latency.

Raw results: [load and restart tests](benchmarks/stress-2026-11-13.txt),
[AMQP benchmark](benchmarks/amqp-2026-11-13.txt). These local measurements
describe this single-node Docker setup; repeat them with deployment storage,
network latency, payloads, and durability settings before using them for sizing.
