local utils = import 'utils.libsonnet';

function(config) {
  'jaeger-pvc': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes.yml',
    metadata: {
      name: 'jaeger',
    },
    spec: {
      storageClassName: config.storage_class,
      accessModes: [
        'ReadWriteOnce',
      ],
      resources: {
        requests: {
          storage: '10Gi',
        },
      },
    },
  }),
  'jaeger-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'jaeger.yml',
    metadata: {
      name: 'jaeger',
      labels: {
        app: 'auctus',
        what: 'jaeger',
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
          what: 'jaeger',
        },
      },
      template: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'jaeger',
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
                'chown -R 999 /badger',
              ],
              volumeMounts: [
                {
                  mountPath: '/badger',
                  name: 'badger',
                },
              ],
            },
          ],
          containers: [
            {
              name: 'jaeger',
              image: 'jaegertracing/all-in-one:1.31',
              securityContext: {
                runAsUser: 999,
              },
              env: utils.env({
                SPAN_STORAGE_TYPE: 'badger',
                BADGER_EPHEMERAL: 'false',
                BADGER_DIRECTORY_KEY: '/badger/key',
                BADGER_DIRECTORY_VALUE: '/badger/value',
              }),
              ports: [
                {
                  containerPort: 16686,
                },
                {
                  containerPort: 6831,
                  protocol: 'UDP',
                },
              ],
              volumeMounts: [
                {
                  mountPath: '/badger',
                  name: 'badger',
                },
              ],
            },
          ],
          volumes: [
            {
              name: 'badger',
              persistentVolumeClaim: {
                claimName: 'jaeger',
              },
            },
          ],
        } + utils.affinity(node=config.db_node_label.jaeger),
      },
    },
  }),
  'jaeger-svc': config.kube('v1', 'Service', {
    file:: 'jaeger.yml',
    metadata: {
      name: 'jaeger',
      labels: {
        app: 'auctus',
        what: 'jaeger',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'jaeger',
      },
      clusterIP: 'None',
      ports: [
        {
          protocol: 'UDP',
          port: 6831,
        },
      ],
    },
  }),
  'jaeger-ui-svc': config.kube('v1', 'Service', {
    file:: 'jaeger.yml',
    metadata: {
      name: 'jaeger-ui',
      labels: {
        app: 'auctus',
        what: 'jaeger',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'jaeger',
      },
      ports: [
        {
          protocol: 'TCP',
          port: 16686,
        },
      ],
    },
  }),
}
