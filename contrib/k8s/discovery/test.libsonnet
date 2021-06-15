local utils = import '../utils.libsonnet';

function(config) (
  [
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'test-discoverer',
        labels: {
          app: 'auctus',
          what: 'test-discoverer',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'test-discoverer',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 7000,
          },
        ],
      },
    },
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: 'test-discoverer',
        labels: {
          app: 'auctus',
          what: 'test-discoverer',
          test: 'true',
        },
      },
      spec: {
        replicas: 1,
        strategy: {
          type: 'Recreate',
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'test-discoverer',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'test-discoverer',
              test: 'true',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'discoverer',
                image: config.image('test-discoverer'),
                args: [
                  'testsuite',
                ],
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
                    name: 'S3_KEY',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 's3.key',
                      },
                    },
                  },
                  {
                    name: 'S3_SECRET',
                    valueFrom: {
                      secretKeyRef: {
                        name: 'secrets',
                        key: 's3.secret',
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
  ]
)
