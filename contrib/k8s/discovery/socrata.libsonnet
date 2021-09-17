local utils = import '../utils.libsonnet';

function(config) {
  'socrata-config': utils.hashed_config_map(
    config.kube,
    name='socrata',
    data={
      'socrata.json': std.manifestJsonEx(
        [
          { url: d }
          for d in config.socrata.domains
        ],
        '  ',
      ),
    },
    labels={
      app: 'auctus',
    },
  ) + { file:: 'discovery.yml' },
  'socrata-cronjob': config.kube('batch/v1beta1', 'CronJob', {
    file:: 'discovery.yml',
    metadata: {
      name: 'socrata',
      labels: {
        app: 'auctus',
        what: 'socrata',
      },
    },
    spec: {
      schedule: config.socrata.schedule,
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
                  image: config.image,
                  imagePullPolicy: 'IfNotPresent',
                  args: ['python', '-m', 'socrata_discovery'],
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
                    name: $['socrata-config'].metadata.name,
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
