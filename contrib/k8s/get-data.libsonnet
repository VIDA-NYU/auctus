function(
  config,
) (
  [
    config.kube('batch/v1', 'Job', {
      metadata: {
        name: 'get-geo-data',
        labels: {
          app: 'auctus',
          what: 'get-geo-data',
        },
      },
      spec: {
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'get-geo-data',
            },
          },
          spec: {
            restartPolicy: 'Never',
            containers: [
              {
                name: 'get-data',
                image: config.image,
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
  ]
)
