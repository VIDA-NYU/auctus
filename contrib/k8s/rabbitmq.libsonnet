local utils = import 'utils.libsonnet';

function(config) {
  'rabbitmq-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'rabbitmq.yml',
    metadata: {
      name: 'rabbitmq',
      labels: {
        app: 'auctus',
        what: 'rabbitmq',
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
          what: 'rabbitmq',
        },
      },
      template: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'rabbitmq',
          },
        },
        spec: {
          securityContext: {
            runAsNonRoot: true,
          },
          containers: [
            {
              name: 'rabbitmq',
              image: 'quay.io/remram44/rabbitmq:3.8.11',
              securityContext: {
                runAsUser: 999,
              },
              env: utils.env({
                RABBITMQ_DEFAULT_USER: {
                  secretKeyRef: {
                    name: 'secrets',
                    key: 'amqp.user',
                  },
                },
                RABBITMQ_DEFAULT_PASS: {
                  secretKeyRef: {
                    name: 'secrets',
                    key: 'amqp.password',
                  },
                },
              }),
              ports: [
                {
                  containerPort: 5672,
                },
                {
                  containerPort: 15672,
                },
                {
                  containerPort: 15692,
                },
              ],
            },
          ],
        } + utils.affinity(node=config.db_node_label.rabbitmq),
      },
    },
  }),
  'rabbitmq-svc': config.kube('v1', 'Service', {
    file:: 'rabbitmq.yml',
    metadata: {
      name: 'rabbitmq',
      labels: {
        app: 'auctus',
        what: 'rabbitmq',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'rabbitmq',
      },
      ports: [
        {
          protocol: 'TCP',
          port: 5672,
        },
      ],
    },
  }),
  'rabbitmq-management-svc': config.kube('v1', 'Service', {
    file:: 'rabbitmq.yml',
    metadata: {
      name: 'rabbitmq-management',
      labels: {
        app: 'auctus',
        what: 'rabbitmq',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'rabbitmq',
      },
      ports: [
        {
          protocol: 'TCP',
          port: 15672,
        },
      ],
    },
  }),
  'rabbitmq-scrape-svc': config.kube('v1', 'Service', {
    file:: 'monitoring.yml',
    metadata: {
      name: 'rabbitmq-scrape',
      labels: {
        app: 'auctus',
        what: 'monitoring',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'rabbitmq',
      },
      clusterIP: 'None',
      ports: [
        {
          protocol: 'TCP',
          port: 15692,
        },
      ],
    },
  }),
}
