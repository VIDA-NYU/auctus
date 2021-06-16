local utils = import 'utils.libsonnet';

local request_whitelist = function(config) (
  if config.request_whitelist != null && std.length(config.request_whitelist) > 0 then [
    {
      name: 'AUCTUS_REQUEST_WHITELIST',
      value: std.join(',', config.request_whitelist),
    },
  ]
  else []
);

{
  lazo: function(
    config,
    lazo_memory,
       ) [
    config.kube('apps/v1', 'Deployment', {
      metadata: {
        name: 'lazo',
        labels: {
          app: 'auctus',
          what: 'lazo',
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
            what: 'lazo',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'lazo',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'lazo',
                image: 'registry.gitlab.com/vida-nyu/auctus/lazo-index-service:0.7.1',
                env: [
                  {
                    name: 'DATABASE',
                    value: 'elasticsearch',
                  },
                  {
                    name: 'PORT',
                    value: '50051',
                  },
                  {
                    name: 'ELASTICSEARCH_HOST',
                    value: 'elasticsearch',
                  },
                  {
                    name: 'ELASTICSEARCH_PORT',
                    value: '9200',
                  },
                  {
                    name: 'ELASTICSEARCH_INDEX',
                    value: config.elasticsearch_prefix + 'lazo',
                  },
                  {
                    name: 'JAVA_OPTS',
                    value: '-Xmx%s -Xms%s' % [lazo_memory, lazo_memory],
                  },
                ],
                ports: [
                  {
                    containerPort: 50051,
                  },
                ],
              },
            ],
          } + utils.affinity(node=config.db_node_label.lazo),
        },
      },
    }),
    config.kube('v1', 'Service', {
      metadata: {
        name: 'lazo',
        labels: {
          app: 'auctus',
          what: 'lazo',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'lazo',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 50051,
          },
        ],
      },
    }),
  ],
  frontend: function(
    config,
    replicas=1,
    max_surge=1,
    max_unavailable=0,
           ) [
    config.kube('apps/v1', 'Deployment', {
      metadata: {
        name: 'frontend',
        labels: {
          app: 'auctus',
          what: 'frontend',
        },
      },
      spec: {
        replicas: replicas,
        strategy: {
          type: 'RollingUpdate',
          rollingUpdate: {
            maxSurge: max_surge,
            maxUnavailable: max_unavailable,
          },
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'frontend',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'frontend',
            },
          },
          spec: {
            containers: [
              {
                name: 'nginx',
                image: config.image('frontend'),
                imagePullPolicy: 'IfNotPresent',
                env: [
                  {
                    name: 'API_URL',
                    value: config.api_url,
                  },
                ],
                ports: [
                  {
                    containerPort: 80,
                  },
                ],
              },
            ],
          },
        },
      },
    }),
    config.kube('v1', 'Service', {
      metadata: {
        name: 'frontend',
        labels: {
          app: 'auctus',
          what: 'frontend',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'frontend',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 80,
          },
        ],
      },
    }),
  ],
  apiserver: function(
    config,
    replicas=4,
    max_surge=2,
    max_unavailable=0,
            ) [
    config.kube('apps/v1', 'Deployment', {
      metadata: {
        name: 'apiserver',
        labels: {
          app: 'auctus',
          what: 'apiserver',
        },
      },
      spec: {
        replicas: replicas,
        strategy: {
          type: 'RollingUpdate',
          rollingUpdate: {
            maxSurge: max_surge,
            maxUnavailable: max_unavailable,
          },
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'apiserver',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'apiserver',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'apiserver',
                image: config.image('apiserver'),
                imagePullPolicy: 'IfNotPresent',
                env: [
                  {
                    name: 'LOG_FORMAT',
                    value: config.log_format,
                  },
                  {
                    name: 'OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES',
                    value: '1',
                  },
                  {
                    name: 'ELASTICSEARCH_HOSTS',
                    value: 'elasticsearch:9200',
                  },
                  {
                    name: 'ELASTICSEARCH_PREFIX',
                    value: config.elasticsearch_prefix,
                  },
                  {
                    name: 'AMQP_HOST',
                    value: 'rabbitmq',
                  },
                  {
                    name: 'AMQP_PORT',
                    value: '5672',
                  },
                  {
                    name: 'AMQP_USER',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'amqp.user',
                      },
                    },
                  },
                  {
                    name: 'AMQP_PASSWORD',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'amqp.password',
                      },
                    },
                  },
                  {
                    name: 'REDIS_HOST',
                    value: 'redis:6379',
                  },
                  {
                    name: 'LAZO_SERVER_HOST',
                    value: 'lazo',
                  },
                  {
                    name: 'LAZO_SERVER_PORT',
                    value: '50051',
                  },
                  {
                    name: 'NOMINATIM_URL',
                    value: config.nominatim_url,
                  },
                  {
                    name: 'FRONTEND_URL',
                    value: config.frontend_url,
                  },
                  {
                    name: 'API_URL',
                    value: config.api_url,
                  },
                  {
                    name: 'CUSTOM_FIELDS',
                    value: std.manifestJsonEx(config.custom_fields, '  '),
                  },
                ] + (
                  utils.object_store_env(config.object_store)
                  + request_whitelist(config)
                  + utils.env(config.opentelemetry)
                ),
                ports: [
                  {
                    containerPort: 8002,
                  },
                ],
                volumeMounts: [
                  {
                    mountPath: '/cache',
                    name: 'cache',
                  },
                  {
                    mountPath: '/usr/src/app/lib_geo/data',
                    name: 'geo-data',
                  },
                ],
                readinessProbe: {
                  httpGet: {
                    path: '/health',
                    port: 8002,
                  },
                  periodSeconds: 1,
                  failureThreshold: 2,
                },
              },
            ],
            volumes: [
              {
                name: 'cache',
                persistentVolumeClaim: {
                  claimName: 'cache',
                },
              },
              {
                name: 'geo-data',
                persistentVolumeClaim: {
                  claimName: 'geo-data',
                  readOnly: true,
                },
              },
            ],
            terminationGracePeriodSeconds: 600,
          },
        },
      },
    }),
    config.kube('v1', 'Service', {
      metadata: {
        name: 'apiserver',
        labels: {
          app: 'auctus',
          what: 'apiserver',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'apiserver',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 8002,
          },
        ],
      },
    }),
  ],
  coordinator: function(config) [
    config.kube('apps/v1', 'Deployment', {
      metadata: {
        name: 'coordinator',
        labels: {
          app: 'auctus',
          what: 'coordinator',
        },
      },
      spec: {
        replicas: 1,
        strategy: {
          type: 'RollingUpdate',
          rollingUpdate: {
            maxSurge: 1,
            maxUnavailable: 0,
          },
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'coordinator',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'coordinator',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'web',
                image: config.image('coordinator'),
                imagePullPolicy: 'IfNotPresent',
                env: [
                  {
                    name: 'LOG_FORMAT',
                    value: config.log_format,
                  },
                  {
                    name: 'ELASTICSEARCH_HOSTS',
                    value: 'elasticsearch:9200',
                  },
                  {
                    name: 'ELASTICSEARCH_PREFIX',
                    value: config.elasticsearch_prefix,
                  },
                  {
                    name: 'AMQP_HOST',
                    value: 'rabbitmq',
                  },
                  {
                    name: 'AMQP_PORT',
                    value: '5672',
                  },
                  {
                    name: 'AMQP_USER',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'amqp.user',
                      },
                    },
                  },
                  {
                    name: 'AMQP_PASSWORD',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'amqp.password',
                      },
                    },
                  },
                  {
                    name: 'LAZO_SERVER_HOST',
                    value: 'lazo',
                  },
                  {
                    name: 'LAZO_SERVER_PORT',
                    value: '50051',
                  },
                  {
                    name: 'ADMIN_PASSWORD',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'admin.password',
                      },
                    },
                  },
                  {
                    name: 'FRONTEND_URL',
                    value: config.frontend_url,
                  },
                  {
                    name: 'API_URL',
                    value: config.api_url,
                  },
                  {
                    name: 'CUSTOM_FIELDS',
                    value: std.manifestJsonEx(config.custom_fields, '  '),
                  },
                ] + utils.object_store_env(config.object_store),
                ports: [
                  {
                    containerPort: 8003,
                  },
                ],
                volumeMounts: [
                  {
                    mountPath: '/cache',
                    name: 'cache',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'cache',
                persistentVolumeClaim: {
                  claimName: 'cache',
                },
              },
            ],
          },
        },
      },
    }),
    config.kube('v1', 'Service', {
      metadata: {
        name: 'coordinator',
        labels: {
          app: 'auctus',
          what: 'coordinator',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'coordinator',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 8003,
          },
        ],
      },
    }),
  ],
  cache_cleaner: function(
    config,
    cache_max_bytes=50000000000,  // 50 GB
                ) [
    config.kube('apps/v1', 'DaemonSet', {
      metadata: {
        name: 'cache-cleaner',
        labels: {
          app: 'auctus',
          what: 'cache-cleaner',
        },
      },
      spec: {
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'cache-cleaner',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'cache-cleaner',
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
                  'chown -R 998 /cache',
                ],
                volumeMounts: [
                  {
                    mountPath: '/cache',
                    name: 'cache',
                  },
                ],
              },
            ],
            containers: [
              {
                name: 'cleaner',
                image: config.image('cache-cleaner'),
                imagePullPolicy: 'IfNotPresent',
                env: [
                  {
                    name: 'LOG_FORMAT',
                    value: config.log_format,
                  },
                  {
                    name: 'MAX_CACHE_BYTES',
                    value: '%d' % cache_max_bytes,
                  },
                ],
                volumeMounts: [
                  {
                    mountPath: '/cache',
                    name: 'cache',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'cache',
                persistentVolumeClaim: {
                  claimName: 'cache',
                },
              },
            ],
          },
        },
      },
    }),
  ],
  profiler: function(
    config,
    replicas=4,
    max_surge=0,
    max_unavailable=2,
           ) [
    config.kube('apps/v1', 'Deployment', {
      metadata: {
        name: 'profiler',
        labels: {
          app: 'auctus',
          what: 'profiler',
        },
      },
      spec: {
        replicas: replicas,
        strategy: {
          type: 'RollingUpdate',
          rollingUpdate: {
            maxSurge: max_surge,
            maxUnavailable: max_unavailable,
          },
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'profiler',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'profiler',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'profiler',
                image: config.image('profiler'),
                imagePullPolicy: 'IfNotPresent',
                env: [
                  {
                    name: 'LOG_FORMAT',
                    value: config.log_format,
                  },
                  {
                    name: 'OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES',
                    value: '1',
                  },
                  {
                    name: 'ELASTICSEARCH_HOSTS',
                    value: 'elasticsearch:9200',
                  },
                  {
                    name: 'ELASTICSEARCH_PREFIX',
                    value: config.elasticsearch_prefix,
                  },
                  {
                    name: 'AMQP_HOST',
                    value: 'rabbitmq',
                  },
                  {
                    name: 'AMQP_PORT',
                    value: '5672',
                  },
                  {
                    name: 'AMQP_USER',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'amqp.user',
                      },
                    },
                  },
                  {
                    name: 'AMQP_PASSWORD',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 'amqp.password',
                      },
                    },
                  },
                  {
                    name: 'LAZO_SERVER_HOST',
                    value: 'lazo',
                  },
                  {
                    name: 'LAZO_SERVER_PORT',
                    value: '50051',
                  },
                  {
                    name: 'NOMINATIM_URL',
                    value: config.nominatim_url,
                  },
                ] + (
                  utils.object_store_env(config.object_store)
                  + request_whitelist(config)
                ),
                volumeMounts: [
                  {
                    mountPath: '/cache',
                    name: 'cache',
                  },
                  {
                    mountPath: '/usr/src/app/lib_geo/data',
                    name: 'geo-data',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'cache',
                persistentVolumeClaim: {
                  claimName: 'cache',
                },
              },
              {
                name: 'geo-data',
                persistentVolumeClaim: {
                  claimName: 'geo-data',
                  readOnly: true,
                },
              },
            ],
          },
        },
      },
    }),
  ],
}
