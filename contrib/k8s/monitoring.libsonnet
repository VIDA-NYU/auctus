local utils = import 'utils.libsonnet';

function(config) {
  'prometheus-config': utils.hashed_config_map(
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
  ) + { file:: 'monitoring.yml' },
  'prometheus-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  'prometheus-pvc': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes.yml',
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
  'prometheus-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'monitoring.yml',
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
                name: $['prometheus-config'].metadata.name,
              },
            },
          ],
        } + utils.affinity(node=config.db_node_label.prometheus),
      },
    },
  }),
  'grafana-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  'grafana-pvc': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes.yml',
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
  'grafana-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'monitoring.yml',
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
              image: 'quay.io/remram44/grafana:7.5.15-image_renderer',
              securityContext: {
                runAsUser: 472,
              },
              env: utils.env(
                {
                  GF_AUTH_ANONYMOUS_ENABLED: if config.grafana_anonymous_access then 'true',
                  GF_SERVER_ROOT_URL: if config.grafana_domain != null then 'https://%s/' % config.grafana_domain,
                  GF_SECURITY_DATA_SOURCE_PROXY_WHITELIST: 'prometheus:9090',
                } + (
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
  'grafana-ingress': (
    if config.grafana_domain != null then
      config.kube('networking.k8s.io/v1', 'Ingress', {
        file:: 'ingress.yml',
        metadata: {
          name: 'grafana',
        },
        spec: {
          ingressClassName: 'nginx',
          rules: [
            {
              host: config.grafana_domain,
              http: {
                paths: [
                  {
                    path: '/',
                    pathType: 'Prefix',
                    backend: {
                      service: {
                        name: 'grafana',
                        port: {
                          number: 3000,
                        },
                      },
                    },
                  },
                ],
              },
            },
          ],
        },
      })
    else null
  ),
}
