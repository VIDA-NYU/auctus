local utils = import 'utils.libsonnet';

function(
  config,
  schedule='20 0 * * 5',
) {
  'snapshotter-cronjob': config.kube('batch/v1beta1', 'CronJob', {
    file:: 'snapshotter.yml',
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
                  env: utils.env(
                    {
                      LOG_FORMAT: config.log_format,
                      ELASTICSEARCH_HOSTS: 'elasticsearch:9200',
                      ELASTICSEARCH_PREFIX: config.elasticsearch.prefix,
                    }
                    + utils.object_store_env(config.object_store)
                  ),
                },
              ],
            },
          },
        },
      },
    },
  }),
}
