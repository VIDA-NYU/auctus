local utils = import 'utils.libsonnet';

function(config) {
  'minio-pvc': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes.yml',
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
  'minio-pv-local': config.kube('v1', 'PersistentVolume', {
    file:: 'volumes-local.yml',
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
  'minio-pvc-local': config.kube('v1', 'PersistentVolumeClaim', {
    file:: 'volumes-local.yml',
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
  'minio-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'minio.yml',
    metadata: {
      name: 'minio',
      labels: {
        app: 'auctus',
        what: 'minio',
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
          what: 'minio',
        },
      },
      template: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'minio',
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
                'chown -R 1000:1000 /export',
              ],
              volumeMounts: [
                {
                  name: 'data',
                  mountPath: '/export',
                },
              ],
            },
          ],
          containers: [
            {
              name: 'minio',
              image: 'minio/minio:RELEASE.2020-10-18T21-54-12Z',
              securityContext: {
                runAsUser: 1000,
              },
              args: ['server', '/export'],
              ports: [
                {
                  containerPort: 9000,
                },
              ],
              imagePullPolicy: 'IfNotPresent',
              env: utils.env({
                MINIO_ACCESS_KEY: {
                  secretKeyRef: {
                    name: 'secrets',
                    key: 's3.key',
                  },
                },
                MINIO_SECRET_KEY: {
                  secretKeyRef: {
                    name: 'secrets',
                    key: 's3.secret',
                  },
                },
              }),
              volumeMounts: [
                {
                  name: 'data',
                  mountPath: '/export',
                },
              ],
            },
          ],
          volumes: [
            {
              name: 'data',
              persistentVolumeClaim: {
                claimName: 'minio',
              },
            },
          ],
        } + utils.affinity(node=config.db_node_label.minio),
      },
    },
  }),
  'minio-svc': config.kube('v1', 'Service', {
    file:: 'minio.yml',
    metadata: {
      name: 'minio',
      labels: {
        app: 'auctus',
        what: 'minio',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'minio',
      },
      clusterIP: 'None',
      ports: [
        {
          protocol: 'TCP',
          port: 9000,
        },
      ],
    },
  }),
  'minio-ingress': config.kube('networking.k8s.io/v1', 'Ingress', {
    file:: 'ingress.yml',
    metadata: {
      name: 'minio',
      annotations: {
        'nginx.ingress.kubernetes.io/proxy-send-timeout': '1200',
        'nginx.ingress.kubernetes.io/proxy-read-timeout': '1200',
        'nginx.ingress.kubernetes.io/proxy-body-size': '1024M',
      },
    },
    spec: {
      ingressClassName: 'nginx',
      rules: [
        {
          host: config.minio_domain,
          http: {
            paths: [
              {
                path: '/',
                pathType: 'Prefix',
                backend: {
                  service: {
                    name: 'minio',
                    port: {
                      number: 9000,
                    },
                  },
                },
              },
            ],
          },
        },
      ],
    },
  }),
}
