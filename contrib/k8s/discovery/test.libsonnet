local utils = import '../utils.libsonnet';

function(config) {
  'test-discoverer-svc': config.kube('v1', 'Service', {
    file:: 'test-discoverer.yml',
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
          port: 8080,
        },
      ],
    },
  }),
  'test-discoverer-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'test-discoverer.yml',
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
              image: config.image,
              args: [
                'python',
                'discovery/test_discovery.py',
              ],
              imagePullPolicy: 'IfNotPresent',
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
                  S3_KEY: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 's3.key',
                    },
                  },
                  S3_SECRET: {
                    secretKeyRef: {
                      name: 'secrets',
                      key: 's3.secret',
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
  }),
}
