function(
  config,
) (
  [
    config.kube('batch/v1', 'Job', {
      metadata: {
        name: 'get-geo-data',
        labels: {
          app: 'auctus',
        },
      },
      spec: {
        template: {
          spec: {
            restartPolicy: 'Never',
            containers: [
              {
                name: 'get-data',
                image: config.image('profiler'),
                imagePullPolicy: 'IfNotPresent',
                securityContext: {
                  runAsNonRoot: false,
                  runAsUser: 0,
                },
                args: [
                  'sh',
                  '-c',
                  'python -m datamart_geo --update /geo_data && ls -l /geo_data',
                ],
                volumeMounts: [
                  {
                    name: 'geo-data',
                    mountPath: '/geo_data',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'geo-data',
                persistentVolumeClaim: {
                  claimName: 'geo-data',
                  readOnly: false,
                },
              },
            ],
          },
        },
      },
    }),
    config.kube('batch/v1', 'Job', {
      metadata: {
        name: 'get-es-synonyms',
        labels: {
          app: 'auctus',
        },
      },
      spec: {
        template: {
          spec: {
            restartPolicy: 'Never',
            containers: [
              {
                name: 'get-data',
                image: config.image('profiler'),
                imagePullPolicy: 'IfNotPresent',
                securityContext: {
                  runAsNonRoot: false,
                  runAsUser: 0,
                },
                args: [
                  'sh',
                  '-c',
                  'curl -Lo /synonyms/synonyms.txt https://gitlab.com/ViDA-NYU/auctus/auctus/-/raw/master/docker/synonyms.txt?inline=false && ls -l /synonyms',
                ],
                volumeMounts: [
                  {
                    name: 'synonyms',
                    mountPath: '/synonyms',
                  },
                ],
              },
            ],
            volumes: [
              {
                name: 'synonyms',
                persistentVolumeClaim: {
                  claimName: 'es-synonyms',
                  readOnly: false,
                },
              },
            ],
          },
        },
      },
    }),
  ]
)
