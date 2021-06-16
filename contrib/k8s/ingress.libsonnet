function(
  config,
) (
  [
    config.kube('extensions/v1beta1', 'Ingress', {
      metadata: {
        name: 'ingress-app',
        annotations: {
          'kubernetes.io/ingress.class': 'nginx',
          'nginx.ingress.kubernetes.io/proxy-send-timeout': '1200',
          'nginx.ingress.kubernetes.io/proxy-read-timeout': '1200',
          'nginx.ingress.kubernetes.io/proxy-body-size': '1024M',
        } + if config.private_app then {
          'nginx.ingress.kubernetes.io/auth-type': 'basic',
          'nginx.ingress.kubernetes.io/auth-secret': 'basic-auth',
          'nginx.ingress.kubernetes.io/auth-realm': 'Private instance',
        } else {},
      },
      spec: {
        rules: [
          {
            host: config.app_domain,
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
    }),
  ] + (
    if config.coordinator_domain != null then [
      config.kube('extensions/v1beta1', 'Ingress', {
        metadata: {
          name: 'ingress-coordinator',
          annotations: {
            'kubernetes.io/ingress.class': 'nginx',
          },
        },
        spec: {
          rules: [
            {
              host: config.coordinator_domain,
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
      }),
    ]
    else []
  ) + (
    if config.grafana_domain != null then [
      config.kube('extensions/v1beta1', 'Ingress', {
        metadata: {
          name: 'ingress-grafana',
          annotations: {
            'kubernetes.io/ingress.class': 'nginx',
          },
        },
        spec: {
          rules: [
            {
              host: config.grafana_domain,
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
      }),
    ]
    else []
  )
)
