local utils = import '../utils.libsonnet';

function(
  config,
  keyword_query,
  schedule='40 0 * * 1,3,5',
) (
  local zenodo_config = utils.hashed_config_map(
    config.kube,
    name='zenodo',
    data={
      'zenodo.json': std.manifestJsonEx(
        {
          keyword_query: keyword_query,
        },
        '  ',
      ),
    },
    labels={
      app: 'auctus',
    },
  );

  [
    zenodo_config,
    config.kube('batch/v1beta1', 'CronJob', {
      metadata: {
        name: 'zenodo',
        labels: {
          app: 'auctus',
          what: 'zenodo',
        },
      },
      spec: {
        schedule: schedule,
        jobTemplate: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'zenodo',
            },
          },
          spec: {
            template: {
              metadata: {
                labels: {
                  app: 'auctus',
                  what: 'zenodo',
                },
              },
              spec: {
                restartPolicy: 'Never',
                securityContext: {
                  runAsNonRoot: true,
                },
                containers: [
                  {
                    name: 'zenodo',
                    image: config.image('zenodo'),
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
                    volumeMounts: [
                      {
                        name: 'config',
                        mountPath: '/usr/src/app/zenodo.json',
                        subPath: 'zenodo.json',
                      },
                    ],
                  },
                ],
                volumes: [
                  {
                    name: 'config',
                    configMap: {
                      name: zenodo_config.metadata.name,
                    },
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
