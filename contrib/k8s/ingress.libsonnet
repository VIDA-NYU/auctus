function(
  config,
) (
  [
    config.kube('networking.k8s.io/v1', 'Ingress', {
      metadata: {
        name: 'ingress-app',
        annotations: {
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
        ingressClassName: 'nginx',
        rules: [
          {
            host: config.app_domain,
            http: {
              paths: [
                {
                  path: '/api/v1/',
                  pathType: 'Prefix',
                  backend: {
                    service: {
                      name: 'apiserver',
                      port: {
                        number: 8002,
                      },
                    },
                  },
                },
                {
                  path: '/snapshot/',
                  pathType: 'Prefix',
                  backend: {
                    service: {
                      name: 'apiserver',
                      port: {
                        number: 8002,
                      },
                    },
                  },
                },
                {
                  path: '/',
                  pathType: 'Prefix',
                  backend: {
                    service: {
                      name: 'frontend',
                      port: {
                        number: 80,
                      },
                    },
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
      config.kube('networking.k8s.io/v1', 'Ingress', {
        metadata: {
          name: 'ingress-coordinator',
        },
        spec: {
          ingressClassName: 'nginx',
          rules: [
            {
              host: config.coordinator_domain,
              http: {
                paths: [
                  {
                    path: '/',
                    pathType: 'Prefix',
                    backend: {
                      service: {
                        name: 'coordinator',
                        port: {
                          number: 8003,
                        },
                      },
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
      config.kube('networking.k8s.io/v1', 'Ingress', {
        metadata: {
          name: 'ingress-grafana',
        },
        spec: {
          ingressClassName: 'nginx',
          rules: [
            {
              host: config.grafana_domain,
              http: {
                paths: [
                  {
                    path: '/',
                    pathType: 'Prefix',
                    backend: {
                      service: {
                        name: 'grafana',
                        port: {
                          number: 3000,
                        },
                      },
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
