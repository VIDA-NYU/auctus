local utils = import 'utils.libsonnet';

function(
  config,
  cache_size,
  local_cache_path,
) (
  local cache_pv_name = 'cache-%s' % std.substr(
    std.md5(local_cache_path),
    0,
    6,
  );
  [
    config.kube('apps/v1', 'DaemonSet', {
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
                  'mkdir -p /mnt/%s' % utils.basename(local_cache_path),
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
                  path: utils.dirname(local_cache_path),
                },
              },
            ],
          },
        },
      },
    }),
    config.kube('v1', 'PersistentVolume', {
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
          storage: cache_size,
        },
        accessModes: [
          'ReadWriteMany',
        ],
        'local': {
          path: local_cache_path,
        },
        nodeAffinity: {
          required: {
            nodeSelectorTerms: [
              {
                matchExpressions: config.local_cache_node_selector,
              },
            ],
          },
        },
      },
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
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
            storage: cache_size,
          },
        },
      },
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'minio',
      },
      spec: {
        storageClassName: config.storage_class,
        accessModes: [
          'ReadWriteOnce',
        ],
        resources: {
          requests: {
            storage: '5Gi',
          },
        },
      },
    }),
  ]
)
