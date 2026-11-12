#!/usr/bin/env bash
set -euo pipefail

/opt/kafka/bin/kafka-storage.sh format --ignore-formatted \
  --cluster-id 5L6g3nShT-eMCtK--X86sw \
  --config /tmp/weave-kafka.properties \
  --add-scram 'SCRAM-SHA-256=[name=weave,password=weave-test-password]' \
  --add-scram 'SCRAM-SHA-512=[name=weave,password=weave-test-password]'
exec /opt/kafka/bin/kafka-server-start.sh /tmp/weave-kafka.properties
