FROM rabbitmq:3-management

RUN apt-get update && apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# from https://www.rabbitmq.com/prometheus.html
RUN mkdir -p /usr/lib/rabbitmq/plugins && \
    cd /usr/lib/rabbitmq/plugins && \
    base_url='https://github.com/deadtrickster/prometheus_rabbitmq_exporter/releases/download/v3.7.2.4' && \
    curl -LO "$base_url/accept-0.3.3.ez" && \
    curl -LO "$base_url/prometheus-3.5.1.ez" && \
    curl -LO "$base_url/prometheus_cowboy-0.1.4.ez" && \
    curl -LO "$base_url/prometheus_httpd-2.1.8.ez" && \
    curl -LO "$base_url/prometheus_rabbitmq_exporter-3.7.2.4.ez"
