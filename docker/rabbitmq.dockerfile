FROM rabbitmq:3.7.17-management

RUN apt-get update && apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# from https://www.rabbitmq.com/prometheus.html
RUN mkdir -p /usr/lib/rabbitmq/plugins && \
    cd /usr/lib/rabbitmq/plugins && \
    base_url='https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.9.1' && \
    curl -LO "$base_url/accept-0.3.5.ez" && \
    curl -LO "$base_url/prometheus-4.3.0.ez" && \
    curl -LO "$base_url/prometheus_cowboy-0.1.7.ez" && \
    curl -LO "$base_url/prometheus_httpd-2.1.10.ez" && \
    curl -LO "$base_url/prometheus_rabbitmq_exporter-3.7.9.1.ez"

COPY etc_rabbitmq/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY etc_rabbitmq/enabled_plugins /etc/rabbitmq/enabled_plugins
