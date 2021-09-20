local utils = import 'utils.libsonnet';

local request_whitelist = function(config) (
  if config.request_whitelist != null && std.length(config.request_whitelist) > 0 then
    {
      AUCTUS_REQUEST_WHITELIST: std.join(',', config.request_whitelist),
    }
  else {}
);
local request_blacklist = function(config) (
  if config.request_blacklist != null && std.length(config.request_blacklist) > 0 then
    {
      AUCTUS_REQUEST_BLACKLIST: std.join(',', config.request_blacklist),
    }
  else {}
);

function(config) {
  local cache_pv_name = 'cache-%s' % std.substr(
    std.md5(config.cache.path),
    0,
    6,
  ),
  'cache-dir-ds': config.kube('apps/v1', 'DaemonSet', {
    file:: 'volumes.yml',
    metadata: {
      name: 'create-cache-dir',
      labels: {
        app: 'auctus',
        what: 'create-cache-dir',
      },
    },
    spec: {
      selector: {
        matchLabels: {
          app: 'auctus',
          what: 'create-cache-dir',
        },
      },
      template: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'create-cache-dir',
          },
        },
        spec: {
          securityContext: {
            runAsNonRoot: true,
          },
          initContainers: [
            {
              name: 'create-volume',
              image: 'busybox',
              securityContext: {
                runAsNonRoot: false,
              },
              command: [
                'sh',
                '-c',
                'mkdir -p /mnt/%s' % utils.basename(config.cache.path),
              ],
              volumeMounts: [
                {
                  mountPath: '/mnt',
                  name: 'parentpath',
                },
              ],
            },
          ],
          containers: [
            {
              name: 'wait',
              image: 'busybox',
              securityContext: {
                runAsUser: 999,
              },
              command: [
                'sh',
                '-c',
                'while true; do sleep 3600; done',
              ],
            },
          ],
          volumes: [
            {
              name: 'parentpath',
              hostPath: {
                path: utils.dirname(config.cache.path),
              },
            },
          ],
        },
      },
    },
  }),
  'cache-dir-ds-local': config.kube('apps/v1', 'DaemonSet', {
    file:: 'volumes-local.yml',
    metadata: {
      name: 'create-cache-dir',
      labels: {
        app: 'auctus',
        what: 'create-cache-dir',
      },
    },
    spec: {
      selector: {
        matchLabels: {
          app: 'auctus',
          what: 'create-cache-dir',
        },
      },
      template: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'create-cache-dir',
          },
        },
        spec: {
          securityContext: {
            runAsNonRoot: true,
          },
          initContainers: [
            {
              name: 'create-volume',
              image: 'busybox',
              securityContext: {
                runAsNonRoot: false,
              },
              command: [
                'sh',
                '-c',
                'mkdir -p /mnt/cache',
              ],
              volumeMounts: [
                {
                  mountPath: '/mnt',
                  name: 'parentpath',
                },
              ],
            },
          ],
          containers: [
            {
              name: 'wait',
              image: 'busybox',
              securityContext: {
                runAsUser: 999,
              },
              command: [
                'sh',
                '-c',
                'while true; do sleep 3600; done',
              ],
            },
          ],
          volumes: [
            {
              name: 'parentpath',
              hostPath: {
                path: '/var/lib/auctus/prod',
              },
            },
          ],
        },
      },
    },
  }),
  'cache-pv': config.kube('v1', 'PersistentVolume', {
    file:: 'volumes.yml',
    metadata: {
      name: cache_pv_name,
      labels: {
        type: 'local',
        app: 'auctus',
        what: 'cache',
      },
    },
    spec: {
      storageClassName: 'manual',
      capacity: {
        storage: config.cache.size,
      },
      accessModes: [
        'ReadWriteMany',
      ],
      'local': {
        path: config.cache.path,
      },
      nodeAffinity: {
        required: {
          nodeSelectorTerms: [
            {
              matchExpressions: config.cache.node_selector,
            },
          ],
        },
      },
    },
  }),
  'cache-pvc': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes.yml',
    metadata: {
      name: 'cache',
    },
    spec: {
      storageClassName: 'manual',
      volumeName: cache_pv_name,
      accessModes: [
        'ReadWriteMany',
      ],
      resources: {
        requests: {
          storage: config.cache.size,
        },
      },
    },
  }),
  'cache-pv-local': config.kube('v1', 'PersistentVolume', {
    file:: 'volumes-local.yml',
    metadata: {
      name: 'cache',
      labels: {
        type: 'local',
        app: 'auctus',
        what: 'cache',
      },
    },
    spec: {
      storageClassName: 'manual',
      capacity: {
        storage: config.cache.size,
      },
      accessModes: [
        'ReadWriteMany',
      ],
      'local': {
        path: '/var/lib/auctus/prod/cache',
      },
      nodeAffinity: {
        required: {
          nodeSelectorTerms: [
            {
              matchExpressions: config.cache.node_selector,
            },
          ],
        },
      },
    },
  }),
  'cache-pvc-local': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes-local.yml',
    metadata: {
      name: 'cache',
    },
    spec: {
      storageClassName: 'manual',
      volumeName: 'cache',
      accessModes: [
        'ReadWriteMany',
      ],
      resources: {
        requests: {
          storage: config.cache.size,
        },
      },
    },
  }),
  'lazo-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'lazo.yml',
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
              image: 'registry.gitlab.com/vida-nyu/auctus/lazo-index-service:0.7.2',
              imagePullPolicy: 'IfNotPresent',
              env: utils.env({
                DATABASE: 'elasticsearch',
                PORT: '50051',
                ELASTICSEARCH_HOST: 'elasticsearch',
                ELASTICSEARCH_PORT: '9200',
                ELASTICSEARCH_INDEX: config.elasticsearch.prefix + 'lazo',
                JAVA_OPTS: '-Xmx%d -Xms%d' % [config.lazo.memory, config.lazo.memory],
              }),
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
  'lazo-svc': config.kube('v1', 'Service', {
    file:: 'lazo.yml',
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
  'lazo-scrape-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  }),
  'frontend-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'auctus.yml',
    metadata: {
      name: 'frontend',
      labels: {
        app: 'auctus',
        what: 'frontend',
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
              image: config.frontend_image,
              imagePullPolicy: 'IfNotPresent',
              env: utils.env({
                API_URL: config.api_url,
              }),
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
  'frontend-svc': config.kube('v1', 'Service', {
    file:: 'auctus.yml',
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
  'apiserver-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'auctus.yml',
    metadata: {
      name: 'apiserver',
      labels: {
        app: 'auctus',
        what: 'apiserver',
      },
    },
    spec: {
      replicas: 4,
      strategy: {
        type: 'RollingUpdate',
        rollingUpdate: {
          maxSurge: 2,
          maxUnavailable: 0,
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
              image: config.image,
              imagePullPolicy: 'IfNotPresent',
              args: ['datamart-apiserver'],
              env: utils.env(
                {
                  LOG_FORMAT: config.log_format,
                  OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES: '1',
                  AUCTUS_OTEL_SERVICE: 'apiserver',
                  ELASTICSEARCH_HOSTS: 'elasticsearch:9200',
                  ELASTICSEARCH_PREFIX: config.elasticsearch.prefix,
                  AMQP_HOST: 'rabbitmq',
                  AMQP_PORT: '5672',
                  AMQP_USER: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'amqp.user',
                    },
                  },
                  AMQP_PASSWORD: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'amqp.password',
                    },
                  },
                  REDIS_HOST: 'redis:6379',
                  LAZO_SERVER_HOST: 'lazo',
                  LAZO_SERVER_PORT: '50051',
                  NOMINATIM_URL: config.nominatim_url,
                  FRONTEND_URL: config.frontend_url,
                  API_URL: config.api_url,
                  CUSTOM_FIELDS: std.manifestJsonEx(config.custom_fields, '  '),
                }
                + utils.object_store_env(config.object_store)
                + request_whitelist(config)
                + request_blacklist(config)
                + config.opentelemetry
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
          ],
          terminationGracePeriodSeconds: 600,
        },
      },
    },
  }),
  'apiserver-svc': config.kube('v1', 'Service', {
    file:: 'auctus.yml',
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
  'apiserver-scrape-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  }),
  'coordinator-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'auctus.yml',
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
              image: config.image,
              imagePullPolicy: 'IfNotPresent',
              args: ['coordinator'],
              env: utils.env(
                {
                  LOG_FORMAT: config.log_format,
                  ELASTICSEARCH_HOSTS: 'elasticsearch:9200',
                  ELASTICSEARCH_PREFIX: config.elasticsearch.prefix,
                  AMQP_HOST: 'rabbitmq',
                  AMQP_PORT: '5672',
                  AMQP_USER: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'amqp.user',
                    },
                  },
                  AMQP_PASSWORD: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'amqp.password',
                    },
                  },
                  LAZO_SERVER_HOST: 'lazo',
                  LAZO_SERVER_PORT: '50051',
                  ADMIN_PASSWORD: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'admin.password',
                    },
                  },
                  FRONTEND_URL: config.frontend_url,
                  API_URL: config.api_url,
                  CUSTOM_FIELDS: std.manifestJsonEx(config.custom_fields, '  '),
                }
                + utils.object_store_env(config.object_store)
              ),
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
  'coordinator-svc': config.kube('v1', 'Service', {
    file:: 'auctus.yml',
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
  'coordinator-scrape-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  }),
  'cache-cleaner-ds': config.kube('apps/v1', 'DaemonSet', {
    file:: 'auctus.yml',
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
              image: config.image,
              imagePullPolicy: 'IfNotPresent',
              args: ['cache_cleaner'],
              env: utils.env({
                LOG_FORMAT: config.log_format,
                MAX_CACHE_BYTES: '%d' % config.cache.high_mark_bytes,
              }),
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
  'cache-cleaner-scrape-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  }),
  'profiler-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'auctus.yml',
    metadata: {
      name: 'profiler',
      labels: {
        app: 'auctus',
        what: 'profiler',
      },
    },
    spec: {
      replicas: 4,
      strategy: {
        type: 'RollingUpdate',
        rollingUpdate: {
          maxSurge: 0,
          maxUnavailable: 2,
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
              image: config.image,
              imagePullPolicy: 'IfNotPresent',
              args: ['profiler'],
              env: utils.env(
                {
                  LOG_FORMAT: config.log_format,
                  AUCTUS_OTEL_SERVICE: 'profiler',
                  OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES: '1',
                  ELASTICSEARCH_HOSTS: 'elasticsearch:9200',
                  ELASTICSEARCH_PREFIX: config.elasticsearch.prefix,
                  AMQP_HOST: 'rabbitmq',
                  AMQP_PORT: '5672',
                  AMQP_USER: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'amqp.user',
                    },
                  },
                  AMQP_PASSWORD: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 'amqp.password',
                    },
                  },
                  LAZO_SERVER_HOST: 'lazo',
                  LAZO_SERVER_PORT: '50051',
                  NOMINATIM_URL: config.nominatim_url,
                }
                + utils.object_store_env(config.object_store)
                + request_whitelist(config)
                + request_blacklist(config)
                + config.opentelemetry
              ),
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
  'profiler-scrape': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
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
  }),
}
