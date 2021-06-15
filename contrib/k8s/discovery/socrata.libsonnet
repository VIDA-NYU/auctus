local utils = import '../utils.libsonnet';

function(
  config,
  domains,
  schedule='30 1 * * 1,3,5',
) (
  local socrata_config = utils.hashed_config_map(
    name='socrata',
    data={
      'socrata.json': std.manifestJsonEx(
        [
          { url: d }
          for d in domains
        ],
        '  ',
      ),
    },
    labels={
      app: 'auctus',
    },
  );

  [
    socrata_config,
    {
      apiVersion: 'batch/v1beta1',
      kind: 'CronJob',
      metadata: {
        name: 'socrata',
        labels: {
          app: 'auctus',
          what: 'socrata',
        },
      },
      spec: {
        schedule: schedule,
        jobTemplate: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'socrata',
            },
          },
          spec: {
            template: {
              metadata: {
                labels: {
                  app: 'auctus',
                  what: 'socrata',
                },
              },
              spec: {
                restartPolicy: 'Never',
                securityContext: {
                  runAsNonRoot: true,
                },
                containers: [
                  {
                    name: 'socrata',
                    image: config.image('socrata'),
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
                        mountPath: '/usr/src/app/socrata.json',
                        subPath: 'socrata.json',
                      },
                    ],
                  },
                ],
                volumes: [
                  {
                    name: 'config',
                    configMap: {
                      name: socrata_config.metadata.name,
                    },
                  },
                ],
              },
            },
          },
        },
      },
    },
  ]
)
