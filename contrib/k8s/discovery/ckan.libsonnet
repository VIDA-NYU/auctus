local utils = import '../utils.libsonnet';

function(config) {
  'ckan-config': utils.hashed_config_map(
    config.kube,
    name='ckan',
    data={
      'ckan.json': std.manifestJsonEx(
        [
          { url: d }
          for d in config.ckan.domains
        ],
        '  ',
      ),
    },
    labels={
      app: 'auctus',
      what: 'ckan',
    },
  ) + { file:: 'discovery.yml' },
  'ckan-cronjob': config.kube('batch/v1beta1', 'CronJob', {
    file:: 'discovery.yml',
    metadata: {
      name: 'ckan',
      labels: {
        app: 'auctus',
        what: 'ckan',
      },
    },
    spec: {
      schedule: config.ckan.schedule,
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
                    name: $['ckan-config'].metadata.name,
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
