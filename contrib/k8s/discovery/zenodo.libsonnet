local utils = import '../utils.libsonnet';

function(config) {
  'zenodo-config': utils.hashed_config_map(
    config.kube,
    name='zenodo',
    data={
      'zenodo.json': std.manifestJsonEx(
        {
          keyword_query: config.zenodo.keyword_query,
        },
        '  ',
      ),
    },
    labels={
      app: 'auctus',
    },
  ) + { file:: 'discovery.yml' },
  'zenodo-cronjob': config.kube('batch/v1beta1', 'CronJob', {
    file:: 'discovery.yml',
    metadata: {
      name: 'zenodo',
      labels: {
        app: 'auctus',
        what: 'zenodo',
      },
    },
    spec: {
      schedule: config.zenodo.schedule,
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
                  image: config.image,
                  imagePullPolicy: 'IfNotPresent',
                  args: ['python', '-m', 'zenodo_discovery'],
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
                    name: $['zenodo-config'].metadata.name,
                  },
                },
              ],
            },
          },
        },
      },
    },
  }),
}
