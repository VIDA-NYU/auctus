local utils = import '../utils.libsonnet';

function(
  config,
  schedule='0 1 * * 1,3,5',
) {
  'worldbank-cronjob': config.kube('batch/v1beta1', 'CronJob', {
    file:: 'discovery.yml',
    metadata: {
      name: 'worldbank',
      labels: {
        app: 'auctus',
        what: 'worldbank',
      },
    },
    spec: {
      schedule: schedule,
      jobTemplate: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'worldbank',
          },
        },
        spec: {
          template: {
            metadata: {
              labels: {
                app: 'auctus',
                what: 'worldbank',
              },
            },
            spec: {
              restartPolicy: 'Never',
              securityContext: {
                runAsNonRoot: true,
              },
              containers: [
                {
                  name: 'worldbank',
                  image: config.image,
                  imagePullPolicy: 'IfNotPresent',
                  args: ['python', '-m', 'worldbank_discovery'],
                  env: utils.env(
                    {
                      LOG_FORMAT: config.log_format,
                      ELASTICSEARCH_HOSTS: 'elasticsearch:9200',
                      ELASTICSEARCH_PREFIX: config.elasticsearch.prefix,
                      AMQP_HOST: 'rabbitmq',
                      AMQP_PORT: '5672',
                      AMQP_USER: {
                        secretKeyRef: {
                          name: 'secrets',
                          key: 'amqp.user',
                        },
                      },
                      AMQP_PASSWORD: {
                        secretKeyRef: {
                          name: 'secrets',
                          key: 'amqp.password',
                        },
                      },
                      LAZO_SERVER_HOST: 'lazo',
                      LAZO_SERVER_PORT: '50051',
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
