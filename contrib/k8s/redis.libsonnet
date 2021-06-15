local utils = import 'utils.libsonnet';

function(
  config,
  maxmemory='500mb',
) (
  local redis_config = utils.hashed_config_map(
    name='redis-config',
    labels={
      app: 'auctus',
      what: 'redis',
    },
    data={
      'redis.conf': 'maxmemory %s\nmaxmemory-policy allkeys-lru\n' % maxmemory,
    },
  );

  [
    redis_config,
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: 'redis',
        labels: {
          app: 'auctus',
          what: 'redis',
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
            what: 'redis',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'redis',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'redis',
                image: 'redis:6.2',
                securityContext: {
                  runAsUser: 999,
                },
                args: [
                  'redis-server',
                  '/usr/local/etc/redis/redis.conf',
                ],
                ports: [
                  {
                    containerPort: 6379,
                  },
                ],
                volumeMounts: [
                  {
                    name: 'config',
                    mountPath: '/usr/local/etc/redis',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'config',
                configMap: {
                  name: redis_config.metadata.name,
                  items: [
                    {
                      key: 'redis.conf',
                      path: 'redis.conf',
                    },
                  ],
                },
              },
            ],
          } + utils.affinity(node=config.db_node_label.redis),
        },
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'redis',
        labels: {
          app: 'auctus',
          what: 'redis',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'redis',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 6379,
          },
        ],
      },
    },
  ]
)
