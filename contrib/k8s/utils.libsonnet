{
  dirname: function(path) (
    local normpath = std.rstripChars(path, '/');
    local slashes = std.findSubstr('/', normpath);
    local last_slash = slashes[std.length(slashes) - 1];
    std.substr(normpath, 0, last_slash)
  ),
  basename: function(path) (
    local normpath = std.rstripChars(path, '/');
    local slashes = std.findSubstr('/', normpath);
    local last_slash = slashes[std.length(slashes) - 1];
    std.substr(normpath, last_slash + 1, std.length(normpath))
  ),
  hashed_config_map: function(kube, name, data, labels={}) (
    local full_hash = std.md5(std.manifestJsonEx(data, '  '));
    local short_hash = std.substr(full_hash, 0, 6);
    kube('v1', 'ConfigMap', {
      metadata: {
        name: name + '-' + short_hash,
        labels: labels,
      },
      data: data,
    })
  ),
  object_store_env: function(object_store) (
    local s3_props = ['s3_url', 's3_client_url', 's3_bucket_prefix'];
    local gcs_props = ['gcs_project', 'gcs_bucket_prefix'];

    local has_s3_props = std.length(std.filter(function(p) std.objectHas(object_store, p), s3_props));
    local has_gcs_props = std.length(std.filter(function(p) std.objectHas(object_store, p), gcs_props));
    assert has_s3_props > 0 || has_gcs_props > 0;
    assert !(has_s3_props > 0 && has_gcs_props > 0);
    assert has_s3_props == 0 || has_s3_props == 3;
    assert has_gcs_props == 0 || has_gcs_props == 2;

    if has_s3_props > 0 then
      {
        S3_URL: object_store.s3_url,
        S3_CLIENT_URL: object_store.s3_client_url,
        S3_BUCKET_PREFIX: object_store.s3_bucket_prefix,
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
      }
    else
      {
        GCS_PROJECT: object_store.gcs_project,
        GCS_CREDS: {
          secretKeyRef: {
            name: 'secrets',
            key: 'gcs.creds',
          },
        },
        GCS_BUCKET_PREFIX: object_store.gcs_bucket_prefix,
      }
  ),
  env: function(object) (
    if object == null then []
    else [
      { name: k }
      + (
        if std.type(object[k]) == 'string' then { value: object[k] }
        else { valueFrom: object[k] }
      )
      for k in std.objectFields(object)
      if object[k] != null
    ]
  ),
  affinity: function(node=null) (
    if node != null then {
      affinity: {
        nodeAffinity: {
          requiredDuringSchedulingIgnoredDuringExecution: {
            nodeSelectorTerms: [
              {
                matchExpressions: [
                  {
                    key: node,
                    operator: 'Exists',
                  },
                ],
              },
            ],
          },
        },
      },
    }
    else {}
  ),
}
