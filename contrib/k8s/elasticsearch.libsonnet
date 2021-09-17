local utils = import 'utils.libsonnet';

function(
  config,
  replicas,
  heap_size,
  cluster_name='docker-cluster',
) (
  [
    // Headless Service governing the StatefulSet
    config.kube('v1', 'Service', {
      metadata: {
        name: 'elasticsearch-cluster',
        labels: {
          app: 'auctus',
          what: 'elasticsearch',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'elasticsearch',
        },
        ports: [
          {
            name: 'transport',
            protocol: 'TCP',
            port: 9300,
          },
        ],
        clusterIP: 'None',
      },
    }),
    config.kube('apps/v1', 'StatefulSet', {
      metadata: {
        name: 'elasticsearch',
        labels: {
          app: 'auctus',
          what: 'elasticsearch',
        },
      },
      spec: {
        serviceName: 'elasticsearch-cluster',
        replicas: replicas,
        updateStrategy: {
          type: 'RollingUpdate',
        },
        selector: {
          matchLabels: {
            app: 'auctus',
            what: 'elasticsearch',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'elasticsearch',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            initContainers: [
              {
                name: 'fix-permissions',
                image: 'busybox',
                securityContext: {
                  runAsNonRoot: false,
                },
                command: [
                  'sh',
                  '-c',
                  'chown -R 1000:1000 /usr/share/elasticsearch/data',
                ],
                volumeMounts: [
                  {
                    name: 'elasticsearch',
                    mountPath: '/usr/share/elasticsearch/data',
                  },
                ],
              },
              {
                name: 'increase-vm-max-map',
                image: 'busybox',
                command: [
                  'sysctl',
                  '-w',
                  'vm.max_map_count=262144',
                ],
                securityContext: {
                  runAsNonRoot: false,
                  privileged: true,
                },
              },
              {
                name: 'download-synonyms',
                image: config.image,
                securityContext: {
                  runAsNonRoot: false,
                  runAsUser: 0,
                },
                args: [
                  'sh',
                  '-c',
                  'curl -Lo /synonyms/synonyms.txt https://gitlab.com/ViDA-NYU/auctus/auctus/-/raw/master/docker/synonyms.txt?inline=false && ls -l /synonyms',
                ],
                volumeMounts: [
                  {
                    name: 'synonyms',
                    mountPath: '/synonyms',
                  },
                ],
              },
            ],
            containers: [
              {
                name: 'elasticsearch',
                image: 'docker.elastic.co/elasticsearch/elasticsearch:7.10.2',
                securityContext: {
                  runAsUser: 1000,
                },
                env: [
                  {
                    name: 'cluster.name',
                    value: cluster_name,
                  },
                  {
                    name: 'network.host',
                    value: '0.0.0.0',
                  },
                  {
                    name: 'ES_JAVA_OPTS',
                    value: '-Xmx%s -Xms%s -Des.enforce.bootstrap.checks=true' % [heap_size, heap_size],
                  },
                  {
                    name: 'discovery.zen.ping.unicast.hosts',
                    value: 'elasticsearch-cluster:9300',
                  },
                  {
                    name: 'discovery.zen.minimum_master_nodes',
                    value: '1',
                  },
                  {
                    name: 'xpack.security.enabled',
                    value: 'false',
                  },
                  {
                    name: 'xpack.monitoring.enabled',
                    value: 'false',
                  },
                  {
                    name: 'cluster.initial_master_nodes',
                    value: 'elasticsearch-0',
                  },
                  {
                    name: 'ES_HEAP_SIZE',
                    value: heap_size,
                  },
                ],
                ports: [
                  {
                    containerPort: 9200,
                  },
                ],
                volumeMounts: [
                  {
                    mountPath: '/usr/share/elasticsearch/data',
                    name: 'elasticsearch',
                  },
                  {
                    name: 'synonyms',
                    mountPath: '/usr/share/elasticsearch/config/synonyms',
                  },
                ],
                readinessProbe: {
                  httpGet: {
                    scheme: 'HTTP',
                    path: '/_cluster/health?local=true',
                    port: 9200,
                  },
                  initialDelaySeconds: 5,
                },
              },
            ],
            volumes: [
              {
                name: 'synonyms',
                emptyDir: {},
              },
            ],
          } + utils.affinity(node=config.db_node_label.elasticsearch),
        },
        volumeClaimTemplates: [
          {
            metadata: {
              name: 'elasticsearch',
            },
            spec: {
              storageClassName: config.storage_class,
              accessModes: [
                'ReadWriteOnce',
              ],
              resources: {
                requests: {
                  storage: '5Gi',
                },
              },
            },
          },
        ],
      },
    }),
    config.kube('v1', 'Service', {
      metadata: {
        name: 'elasticsearch',
        labels: {
          app: 'auctus',
          what: 'elasticsearch',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'elasticsearch',
        },
        ports: [
          {
            protocol: 'TCP',
            port: 9200,
          },
        ],
      },
    }),
    // Prometheus exporter
    config.kube('apps/v1', 'Deployment', {
      metadata: {
        name: 'elasticsearch-exporter',
        labels: {
          app: 'auctus',
          what: 'elasticsearch-exporter',
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
            what: 'elasticsearch-exporter',
          },
        },
        template: {
          metadata: {
            labels: {
              app: 'auctus',
              what: 'elasticsearch-exporter',
            },
          },
          spec: {
            securityContext: {
              runAsNonRoot: true,
            },
            containers: [
              {
                name: 'elasticsearch-exporter',
                image: 'justwatch/elasticsearch_exporter:1.1.0',
                securityContext: {
                  runAsUser: 999,
                },
                args: [
                  '--es.uri=http://elasticsearch:9200',
                  '--es.cluster_settings',
                  '--es.indices',
                  '--es.indices_settings',
                ],
                ports: [
                  {
                    containerPort: 9114,
                  },
                ],
              },
            ],
          } + utils.affinity(node=config.db_node_label.elasticsearch),
        },
      },
    }),
    config.kube('v1', 'Service', {
      metadata: {
        name: 'elasticsearch-scrape',
        labels: {
          app: 'auctus',
          what: 'monitoring',
        },
      },
      spec: {
        selector: {
          app: 'auctus',
          what: 'elasticsearch-exporter',
        },
        clusterIP: 'None',
        ports: [
          {
            protocol: 'TCP',
            port: 9114,
          },
        ],
      },
    }),
  ]
)
