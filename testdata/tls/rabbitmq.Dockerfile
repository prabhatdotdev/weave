FROM rabbitmq:4.3.4-management
COPY --chown=rabbitmq:rabbitmq testdata/tls/ca.crt testdata/tls/server.crt testdata/tls/server.key /etc/rabbitmq/weave-certs/
COPY testdata/tls/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
