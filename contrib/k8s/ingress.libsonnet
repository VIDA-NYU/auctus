function(
  config,
) (
  [
    {
      apiVersion: 'extensions/v1beta1',
      kind: 'Ingress',
      metadata: {
        name: 'ingress-app',
        annotations: {
          'kubernetes.io/ingress.class': 'nginx',
          'nginx.ingress.kubernetes.io/proxy-send-timeout': '1200',
          'nginx.ingress.kubernetes.io/proxy-read-timeout': '1200',
          'nginx.ingress.kubernetes.io/proxy-body-size': '1024M',
        },
      },
      spec: {
        rules: [
          {
            host: config.domain,
            http: {
              paths: [
                {
                  path: '/api/v1/',
                  pathType: 'Prefix',
                  backend: {
                    serviceName: 'apiserver',
                    servicePort: 8002,
                  },
                },
                {
                  path: '/',
                  pathType: 'Prefix',
                  backend: {
                    serviceName: 'frontend',
                    servicePort: 80,
                  },
                },
              ],
            },
          },
        ],
      },
    },
    {
      apiVersion: 'extensions/v1beta1',
      kind: 'Ingress',
      metadata: {
        name: 'ingress-coordinator',
        annotations: {
          'kubernetes.io/ingress.class': 'nginx',
        },
      },
      spec: {
        rules: [
          {
            host: 'coordinator.%s' % config.domain,
            http: {
              paths: [
                {
                  path: '/',
                  pathType: 'Prefix',
                  backend: {
                    serviceName: 'coordinator',
                    servicePort: 8003,
                  },
                },
              ],
            },
          },
        ],
      },
    },
    {
      apiVersion: 'extensions/v1beta1',
      kind: 'Ingress',
      metadata: {
        name: 'ingress-grafana',
        annotations: {
          'kubernetes.io/ingress.class': 'nginx',
        },
      },
      spec: {
        rules: [
          {
            host: 'grafana.%s' % config.domain,
            http: {
              paths: [
                {
                  path: '/',
                  pathType: 'Prefix',
                  backend: {
                    serviceName: 'grafana',
                    servicePort: 3000,
                  },
                },
              ],
            },
          },
        ],
      },
    },
  ]
)
