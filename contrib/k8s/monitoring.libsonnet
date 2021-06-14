local utils = import 'utils.libsonnet';

function(config) (
  local prometheus_config = utils.hashed_config_map(
    name='monitoring',
    data={
      prometheus: std.manifestYamlDoc(
        {
          global: {
            scrape_interval: '15s',
          },
          scrape_configs: [
            {
              job_name: 'prometheus',
              static_configs: [
                {
                  targets: [
                    'localhost:9090',
                  ],
                },
              ],
            },
            {
              job_name: 'elasticsearch',
              scrape_interval: '30s',
              scrape_timeout: '10s',
              static_configs: [
                {
                  targets: [
                    'elasticsearch-scrape:9114',
                  ],
                },
              ],
            },
            {
              job_name: 'rabbitmq',
              scrape_timeout: '5s',
              metrics_path: '/metrics',
              static_configs: [
                {
                  targets: [
                    'rabbitmq-scrape:15692',
                  ],
                },
              ],
            },
            {
              job_name: 'apiserver',
              dns_sd_configs: [
                {
                  names: [
                    'apiserver-scrape',
                  ],
                  type: 'A',
                  port: 8000,
                  refresh_interval: '60s',
                },
              ],
            },
            {
              job_name: 'coordinator',
              static_configs: [
                {
                  targets: [
                    'coordinator-scrape:8000',
                  ],
                },
              ],
            },
            {
              job_name: 'cache-cleaner',
              dns_sd_configs: [
                {
                  names: [
                    'cache-cleaner-scrape',
                  ],
                  type: 'A',
                  port: 8000,
                  refresh_interval: '60s',
                },
              ],
            },
            {
              job_name: 'profiler',
              dns_sd_configs: [
                {
                  names: [
                    'profiler-scrape',
                  ],
                  type: 'A',
                  port: 8000,
                  refresh_interval: '60s',
                },
              ],
            },
            {
              job_name: 'lazo',
              dns_sd_configs: [
                {
                  names: [
                    'lazo-scrape',
                  ],
                  type: 'A',
                  port: 8000,
                  refresh_interval: '60s',
                },
              ],
            },
            {
              job_name: 'nominatim',
              scrape_timeout: '5s',
              metrics_path: '/metrics',
              static_configs: [
                {
                  targets: [
                    'nominatim',
                  ],
                },
              ],
            },
          ],
        }
      ),
    },
    labels={
      app: 'auctus',
    },
  );
  [
    prometheus_config,
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'elasticsearch-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'elasticsearch-exporter',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 9114,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'rabbitmq-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'rabbitmq',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 15692,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'lazo-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'lazo',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 8000,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'profiler-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'profiler',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 8000,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'apiserver-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'apiserver',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 8000,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'coordinator-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'coordinator',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 8000,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'cache-cleaner-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'cache-cleaner',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 8000,
          },
        ],
      },
    },
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: 'elasticsearch-exporter',
        labels: {
          app: 'auctus',
          what: 'elasticsearch-exporter',
        },
      },
      spec: {
        replicas: 1,
        strategy: {
          type: 'Recreate',
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'elasticsearch-exporter',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'elasticsearch-exporter',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'elasticsearch-exporter',
                image: 'justwatch/elasticsearch_exporter:1.1.0',
                securityContext: {
                  runAsUser: 999,
                },
                args: [
                  '--es.uri=http://elasticsearch:9200',
                  '--es.cluster_settings',
                  '--es.indices',
                  '--es.indices_settings',
                ],
                ports: [
                  {
                    containerPort: 9114,
                  },
                ],
              },
            ],
          } + utils.affinity(node=config.db_node_label.elasticsearch),
        },
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'prometheus',
        labels: {
          app: 'auctus',
          what: 'prometheus',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'prometheus',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 9090,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'PersistentVolumeClaim',
      metadata: {
        name: 'prometheus',
      },
      spec: {
        accessModes: [
          'ReadWriteOnce',
        ],
        resources: {
          requests: {
            storage: '2Gi',
          },
        },
      },
    },
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: 'prometheus',
        labels: {
          app: 'auctus',
          what: 'prometheus',
        },
      },
      spec: {
        replicas: 1,
        strategy: {
          type: 'Recreate',
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'prometheus',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'prometheus',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            initContainers: [
              {
                name: 'fix-permissions',
                image: 'busybox',
                securityContext: {
                  runAsNonRoot: false,
                },
                command: [
                  'sh',
                  '-c',
                  'chown -R 65534:65534 /prometheus',
                ],
                volumeMounts: [
                  {
                    mountPath: '/prometheus',
                    name: 'data',
                  },
                ],
              },
            ],
            containers: [
              {
                name: 'prometheus',
                image: 'prom/prometheus:v2.22.0',
                securityContext: {
                  runAsUser: 65534,
                },
                ports: [
                  {
                    containerPort: 9090,
                  },
                ],
                volumeMounts: [
                  {
                    mountPath: '/prometheus',
                    name: 'data',
                  },
                  {
                    mountPath: '/etc/prometheus/prometheus.yml',
                    subPath: 'prometheus',
                    name: 'config',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'data',
                persistentVolumeClaim: {
                  claimName: 'prometheus',
                },
              },
              {
                name: 'config',
                configMap: {
                  name: prometheus_config.metadata.name,
                },
              },
            ],
          } + utils.affinity(node=config.db_node_label.prometheus),
        },
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'grafana',
        labels: {
          app: 'auctus',
          what: 'grafana',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'grafana',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 3000,
          },
        ],
      },
    },
    {
      apiVersion: 'v1',
      kind: 'PersistentVolumeClaim',
      metadata: {
        name: 'grafana',
      },
      spec: {
        accessModes: [
          'ReadWriteOnce',
        ],
        resources: {
          requests: {
            storage: '100Mi',
          },
        },
      },
    },
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: 'grafana',
        labels: {
          app: 'auctus',
          what: 'grafana',
        },
      },
      spec: {
        replicas: 1,
        strategy: {
          type: 'Recreate',
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'grafana',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'grafana',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            initContainers: [
              {
                name: 'fix-permissions',
                image: 'busybox',
                securityContext: {
                  runAsNonRoot: false,
                },
                command: [
                  'sh',
                  '-c',
                  'chown -R 472:472 /grafana',
                ],
                volumeMounts: [
                  {
                    mountPath: '/grafana',
                    name: 'data',
                  },
                ],
              },
            ],
            containers: [
              {
                name: 'grafana',
                image: 'quay.io/remram44/grafana:7.3.4-image_renderer',
                securityContext: {
                  runAsUser: 472,
                },
                env: (
                  if std.objectHas(config, 'smtp') then [
                    {
                      name: 'GF_SMTP_ENABLED',
                      value: 'true',
                    },
                    {
                      name: 'GF_SMTP_HOST',
                      value: config.smtp.host,
                    },
                    {
                      name: 'GF_SMTP_FROM_NAME',
                      value: config.smtp.from_name,
                    },
                    {
                      name: 'GF_SERVER_ROOT_URL',
                      value: 'https://grafana.%s/' % config.domain,
                    },
                    {
                      name: 'GF_SMTP_USER',
                      valueFrom: {
                        secretKeyRef: {
                          name: 'secrets',
                          key: 'smtp.user',
                        },
                      },
                    },
                    {
                      name: 'GF_SMTP_PASSWORD',
                      valueFrom: {
                        secretKeyRef: {
                          name: 'secrets',
                          key: 'smtp.password',
                        },
                      },
                    },
                  ]
                  else []
                ),
                ports: [
                  {
                    containerPort: 3000,
                  },
                ],
                volumeMounts: [
                  {
                    mountPath: '/var/lib/grafana',
                    name: 'data',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'data',
                persistentVolumeClaim: {
                  claimName: 'grafana',
                },
              },
            ],
          } + utils.affinity(node=config.db_node_label.grafana),
        },
      },
    },
  ]
)
