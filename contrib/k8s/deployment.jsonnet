local auctus = import 'auctus.libsonnet';

local config = {
  image: 'auctus:latest',
  frontend_image: 'auctus_frontend:latest',
  app_domain: 'localhost',
  //frontend_url: 'https://%s' % self.app_domain, // If using Ingress
  frontend_url: 'http://localhost:30808',  // If using KinD
  //api_url: 'https://%s/api/v1' % self.app_domain, // If using Ingress
  api_url: 'http://localhost:30808/api/v1',  // If using KinD
  nominatim_url: 'http://nominatim:8080/',
  minio_domain: 'files.localhost',
  object_store: {
    s3_url: 'http://minio:9000',
    s3_client_url: 'http://%s:30808' % $.minio_domain,
    s3_bucket_prefix: 'auctus-dev-',
    //gcs_project: 'auctus',
    //gcs_bucket_prefix: 'auctus-dev-',
  },
  //smtp: {
  //  host: 'mail.example.org',
  //  from_name: 'Auctus',
  //  from_address: 'auctus@example.org',
  //},
  custom_fields: {},
  //custom_fields: {
  //  specialId: { label: 'Special ID', type: 'integer' },
  //  dept: { label: 'Department', type: 'keyword', required: true },
  //},
  // Addresses to exclude from SSRF protection
  request_whitelist: ['test-discoverer'],
  request_blacklist: [],
  log_format: 'json',
  // Storage class for volumes (except cache)
  storage_class: 'standard',
  cache: {
    size: '55Gi',
    high_mark_bytes: 50000000000,  // 50 GB
    path: '/var/lib/auctus/prod/cache',
    // Node selector for nodes where the cache volume is available
    node_selector: [
      //{ key: 'kubernetes.io/os', operator: 'In', values: ['linux'] },
      { key: 'auctus-prod-cache-volume', operator: 'Exists' },
    ],
  },
  // Label on nodes where databases will be run (can be set to null)
  db_node_label: {
    default: null,
    redis: self.default,
    elasticsearch: self.default,
    rabbitmq: self.default,
    minio: self.default,
    lazo: self.default,
    prometheus: self.default,
    grafana: self.default,
    jaeger: self.default,
    nominatim: self.default,
  },
  // Public domain for the coordinator (can be set to null to disable Ingress)
  coordinator_domain: 'coordinator.auctus.vida-nyu.org',
  // Whether Grafana can be access read-only by the public
  grafana_anonymous_access: true,
  // Public domain for Grafana (can be set to null to disable Ingress)
  grafana_domain: 'grafana.auctus.vida-nyu.org',
  // OpenTelemetry configuration (can be null)
  //opentelemetry: null,
  opentelemetry: {
    OTEL_TRACES_EXPORTER: 'jaeger_thrift',
    OTEL_EXPORTER_JAEGER_AGENT_HOST: 'jaeger',
    OTEL_EXPORTER_JAEGER_AGENT_PORT: '6831',
  },
  // Protect the frontend and API with a password
  // If true, the corresponding secret has to be set
  private_app: false,
  redis: {
    max_memory: '500mb',
  },
  elasticsearch: {
    prefix: 'auctusdev_',
    replicas: 1,
    heap_size: '2g',
  },
  nominatim: {
    data_url: 'https://www.googleapis.com/download/storage/v1/b/nominatim-data-nyu/o/nominatim-postgres-data.tar?alt=media',
  },
  lazo: {
    memory: 2000000000,  // 2 GB
  },
  socrata: {
    domains: ['data.cityofnewyork.us', 'finances.worldbank.org'],
    schedule: '30 1 * * 1,3,5',
  },
  zenodo: {
    schedule: '40 0 * * 1,3,5',
    keyword_query: 'covid',
  },
  ckan: {
    domains: ['data.humdata.org'],
    schedule: '10 1 * * 1,3,5',
  },
  // Wrapper for Kubernetes objects
  kube: function(version, kind, payload) (
    {
      apiVersion: version,
      kind: kind,
    }
    + payload
  ),
};

auctus(config)
