function() (
  [
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
        name: 'elasticsearch',
      },
      spec: {
        storageClassName: '',
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
        name: 'geo-data',
        labels: {
          type: 'local',
          app: 'auctus',
          what: 'geo-data',
        },
      },
      spec: {
        capacity: {
          storage: '300Mi',
        },
        accessModes: [
          'ReadOnlyMany',
        ],
        hostPath: {
          path: '/var/lib/auctus/geo-data',
        },
      },
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'geo-data',
      },
      spec: {
        storageClassName: '',
        volumeName: 'geo-data',
        accessModes: [
          'ReadOnlyMany',
        ],
        resources: {
          requests: {
            storage: '300Mi',
          },
        },
      },
    }),
    config.kube('v1', 'PersistentVolume', {
      metadata: {
        name: 'es-synonyms',
        labels: {
          type: 'local',
          app: 'auctus',
          what: 'es-synonyms',
        },
      },
      spec: {
        capacity: {
          storage: '5Mi',
        },
        accessModes: [
          'ReadOnlyMany',
        ],
        hostPath: {
          path: '/var/lib/auctus/es-synonyms',
        },
      },
    }),
    config.kube('v1', 'PersistentVolumeClaim', {
      metadata: {
        name: 'es-synonyms',
      },
      spec: {
        storageClassName: '',
        volumeName: 'es-synonyms',
        accessModes: [
          'ReadOnlyMany',
        ],
        resources: {
          requests: {
            storage: '5Mi',
          },
        },
      },
    }),
  ]
)
