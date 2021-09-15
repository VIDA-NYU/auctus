function(config) (
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
    config.kube('v1', 'PersistentVolume', {
      metadata: {
        name: 'elasticsearch',
        labels: {
          type: 'local',
          app: 'auctus',
          what: 'elasticsearch',
        },
      },
      spec: {
        storageClassName: 'manual',
        capacity: {
          storage: '5Gi',
        },
        accessModes: [
          'ReadWriteOnce',
        ],
        hostPath: {
          path: '/var/lib/auctus/prod/elasticsearch',
        },
      },
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'elasticsearch-elasticsearch-0',
      },
      spec: {
        storageClassName: 'manual',
        volumeName: 'elasticsearch',
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
    config.kube('v1', 'PersistentVolume', {
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
          storage: '5Gi',
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
                matchExpressions: [
                  {
                    key: 'auctus-prod-cache-volume',
                    operator: 'Exists',
                  },
                ],
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
        volumeName: 'cache',
        accessModes: [
          'ReadWriteMany',
        ],
        resources: {
          requests: {
            storage: '5Gi',
          },
        },
      },
    }),
    config.kube('v1', 'PersistentVolume', {
      metadata: {
        name: 'minio',
        labels: {
          type: 'local',
          app: 'auctus',
          what: 'minio',
        },
      },
      spec: {
        storageClassName: 'manual',
        capacity: {
          storage: '5Gi',
        },
        accessModes: [
          'ReadWriteOnce',
        ],
        hostPath: {
          path: '/var/lib/auctus/minio',
        },
      },
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'minio',
      },
      spec: {
        storageClassName: 'manual',
        volumeName: 'minio',
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
