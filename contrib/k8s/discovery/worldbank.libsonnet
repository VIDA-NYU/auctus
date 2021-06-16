local utils = import '../utils.libsonnet';

function(
  config,
  schedule='0 1 * * 1,3,5',
) (
  [
    config.kube('batch/v1beta1', 'CronJob', {
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
                    image: config.image('worldbank'),
                    imagePullPolicy: 'IfNotPresent',
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
                      {
                        name: 'AMQP_HOST',
                        value: 'rabbitmq',
                      },
                      {
                        name: 'AMQP_PORT',
                        value: '5672',
                      },
                      {
                        name: 'AMQP_USER',
                        valueFrom: {
                          secretKeyRef: {
                            name: 'secrets',
                            key: 'amqp.user',
                          },
                        },
                      },
                      {
                        name: 'AMQP_PASSWORD',
                        valueFrom: {
                          secretKeyRef: {
                            name: 'secrets',
                            key: 'amqp.password',
                          },
                        },
                      },
                      {
                        name: 'LAZO_SERVER_HOST',
                        value: 'lazo',
                      },
                      {
                        name: 'LAZO_SERVER_PORT',
                        value: '50051',
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
