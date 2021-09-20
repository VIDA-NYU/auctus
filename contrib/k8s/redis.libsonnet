local utils = import 'utils.libsonnet';

function(config) {
  'redis-config': utils.hashed_config_map(
    config.kube,
    name='redis-config',
    labels={
      app: 'auctus',
      what: 'redis',
    },
    data={
      'redis.conf': 'maxmemory %s\nmaxmemory-policy allkeys-lru\n' % config.redis.max_memory,
    },
  ) + { file:: 'redis.yml' },
  'redis-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'redis.yml',
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
                name: $['redis-config'].metadata.name,
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
  }),
  'redis-svc': config.kube('v1', 'Service', {
    file:: 'redis.yml',
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
  }),
}
