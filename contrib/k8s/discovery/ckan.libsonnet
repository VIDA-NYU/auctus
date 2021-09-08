local utils = import '../utils.libsonnet';

function(
  config,
  domains,
  schedule='10 1 * * 1,3,5',
) (
  local ckan_config = utils.hashed_config_map(
    config.kube,
    name='ckan',
    data={
      'ckan.json': std.manifestJsonEx(
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
    ckan_config,
    config.kube('batch/v1beta1', 'CronJob', {
      metadata: {
        name: 'ckan',
        labels: {
          app: 'auctus',
          what: 'ckan',
        },
      },
      spec: {
        schedule: schedule,
        jobTemplate: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'ckan',
            },
          },
          spec: {
            template: {
              metadata: {
                labels: {
                  app: 'auctus',
                  what: 'ckan',
                },
              },
              spec: {
                restartPolicy: 'Never',
                securityContext: {
                  runAsNonRoot: true,
                },
                containers: [
                  {
                    name: 'ckan',
                    image: config.image,
                    imagePullPolicy: 'IfNotPresent',
                    args: ['python', '-m', 'ckan_discovery'],
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
                        mountPath: '/usr/src/app/ckan.json',
                        subPath: 'ckan.json',
                      },
                    ],
                  },
                ],
                volumes: [
                  {
                    name: 'config',
                    configMap: {
                      name: ckan_config.metadata.name,
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
