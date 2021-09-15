local utils = import 'utils.libsonnet';

function(
  config,
  schedule='20 0 * * 5',
) (
  [
    config.kube('batch/v1beta1', 'CronJob', {
      metadata: {
        name: 'snapshotter',
        labels: {
          app: 'auctus',
          what: 'snapshotter',
        },
      },
      spec: {
        schedule: schedule,
        jobTemplate: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'snapshotter',
            },
          },
          spec: {
            template: {
              metadata: {
                labels: {
                  app: 'auctus',
                  what: 'snapshotter',
                },
              },
              spec: {
                restartPolicy: 'Never',
                securityContext: {
                  runAsNonRoot: true,
                },
                containers: [
                  {
                    name: 'snapshotter',
                    image: config.image,
                    imagePullPolicy: 'IfNotPresent',
                    args: ['snapshotter'],
                    env: [
                      {
                        name: 'LOG_FORMAT',
                        value: config.log_format,
                      },
                      {
                        name: 'ELASTICSEARCH_HOSTS',
                        value: 'elasticsearch:9200',
                      },
                      {
                        name: 'ELASTICSEARCH_PREFIX',
                        value: config.elasticsearch_prefix,
                      },
                    ] + utils.object_store_env(config.object_store),
                  },
                ],
              },
            },
          },
        },
      },
    }),
  ]
)
