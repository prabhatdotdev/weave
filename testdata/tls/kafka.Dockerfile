FROM apache/kafka:4.3.1
COPY --chown=1000:1000 testdata/tls/ca.crt testdata/tls/server.crt testdata/tls/server.key /tmp/weave-certs/
COPY testdata/tls/kafka.properties /tmp/weave-kafka.properties
COPY testdata/tls/start-kafka.sh /tmp/weave-start-kafka.sh
RUN cat /tmp/weave-certs/server.key /tmp/weave-certs/server.crt > /tmp/weave-server.pem
ENTRYPOINT ["/bin/bash", "/tmp/weave-start-kafka.sh"]
