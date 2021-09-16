local utils = import 'utils.libsonnet';

function(config) {
  'jaeger-deploy': config.kube('apps/v1', 'Deployment', {
    file:: 'jaeger.yml',
    metadata: {
      name: 'jaeger',
      labels: {
        app: 'auctus',
        what: 'jaeger',
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
          what: 'jaeger',
        },
      },
      template: {
        metadata: {
          labels: {
            app: 'auctus',
            what: 'jaeger',
          },
        },
        spec: {
          securityContext: {
            runAsNonRoot: true,
          },
          containers: [
            {
              name: 'jaeger',
              image: 'jaegertracing/all-in-one',
              securityContext: {
                runAsUser: 999,
              },
              ports: [
                {
                  containerPort: 16686,
                },
                {
                  containerPort: 6831,
                  protocol: 'UDP',
                },
              ],
            },
          ],
        } + utils.affinity(node=config.db_node_label.jaeger),
      },
    },
  }),
  'jaeger-svc': config.kube('v1', 'Service', {
    file:: 'jaeger.yml',
    metadata: {
      name: 'jaeger',
      labels: {
        app: 'auctus',
        what: 'jaeger',
      },
    },
    spec: {
      selector: {
        app: 'auctus',
        what: 'jaeger',
      },
      clusterIP: 'None',
      ports: [
        {
          protocol: 'UDP',
          port: 6831,
        },
      ],
    },
  }),
}
