FROM rabbitmq:3.8.11-management

COPY --chown=999:999 etc_rabbitmq/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY etc_rabbitmq/enabled_plugins /etc/rabbitmq/enabled_plugins
