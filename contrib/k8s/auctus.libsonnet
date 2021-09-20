local app = import 'app.libsonnet';
local ckan = import 'discovery/ckan.libsonnet';
local socrata = import 'discovery/socrata.libsonnet';
local test_discoverer = import 'discovery/test.libsonnet';
local uaz_indicators = import 'discovery/uaz-indicators.libsonnet';
local worldbank = import 'discovery/worldbank.libsonnet';
local zenodo = import 'discovery/zenodo.libsonnet';
local elasticsearch = import 'elasticsearch.libsonnet';
local ingress = import 'ingress.libsonnet';
local jaeger = import 'jaeger.libsonnet';
local minio = import 'minio.libsonnet';
local monitoring = import 'monitoring.libsonnet';
local nominatim = import 'nominatim.libsonnet';
local rabbitmq = import 'rabbitmq.libsonnet';
local redis = import 'redis.libsonnet';
local snapshotter = import 'snapshotter.libsonnet';
local volumes_local = import 'volumes-local.libsonnet';
local volumes = import 'volumes.libsonnet';

function(config) (
  local data = (
    {}
    + redis(config)
    + elasticsearch(config)
    + rabbitmq(config)
    + nominatim(config)
    + app(config)
    + snapshotter(config)
    + ingress(config)
    + minio(config)
    + monitoring(config)
    + jaeger(config)
    + ckan(config)
    + socrata(config)
    + uaz_indicators(config)
    + worldbank(config)
    + zenodo(config)
    //+ test_discoverer(config)
  );

  local files = std.set([data[k].file for k in std.objectFields(data)]);

  {
    [file]: std.manifestYamlStream([
      data[k]
      for k in std.objectFields(data)
      if data[k] != null && data[k].file == file
    ])
    for file in files
  }
)
