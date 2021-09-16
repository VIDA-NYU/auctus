local utils = import 'utils.libsonnet';

function(config) (
  local prometheus_config = utils.hashed_config_map(
    config.kube,
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
    config.kube('v1', 'Service', {
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
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'prometheus',
      },
      spec: {
        storageClassName: config.storage_class,
        accessModes: [
          'ReadWriteOnce',
        ],
        resources: {
          requests: {
            storage: '2Gi',
          },
        },
      },
    }),
    config.kube('apps/v1', 'Deployment', {
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
    }),
    config.kube('v1', 'Service', {
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
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'grafana',
      },
      spec: {
        storageClassName: config.storage_class,
        accessModes: [
          'ReadWriteOnce',
        ],
        resources: {
          requests: {
            storage: '100Mi',
          },
        },
      },
    }),
    config.kube('apps/v1', 'Deployment', {
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
                env: utils.env(
                  (
                    if config.grafana_anonymous_access then {
                      GF_AUTH_ANONYMOUS_ENABLED: 'true',
                    }
                    else {}
                  ) + (
                    if std.objectHas(config, 'smtp') then {
                      GF_SMTP_ENABLED: 'true',
                      GF_SMTP_HOST: config.smtp.host,
                      GF_SMTP_FROM_NAME: config.smtp.from_name,
                      GF_SMTP_FROM_ADDRESS: config.smtp.from_address,
                      GF_SMTP_USER: {
                        secretKeyRef: {
                          name: 'secrets',
                          key: 'smtp.user',
                        },
                      },
                      GF_SMTP_PASSWORD: {
                        secretKeyRef: {
                          name: 'secrets',
                          key: 'smtp.password',
                        },
                      },
                    }
                    else {}
                  ) + (
                    if config.grafana_domain != null then {
                      GF_SERVER_ROOT_URL: 'https://%s/' % config.grafana_domain,
                    }
                    else {}
                  )
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
    }),
  ]
)
