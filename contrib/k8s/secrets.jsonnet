local amqp_user = 'auctus';
local amqp_password = 'auctus';
local admin_password = 'auctus';
local s3_key = '';
local s3_secret = '';
local gcs_creds = std.base64('');

{
  'secrets.yml': std.manifestYamlStream([{
    apiVersion: 'v1',
    kind: 'Secret',
    type: 'Opaque',
    metadata: {
      name: 'secrets',
    },
    local data = {
      'amqp.user': amqp_user,
      'amqp.password': amqp_password,
      'admin.password': admin_password,
      's3.key': s3_key,
      's3.secret': s3_secret,
      'gcs.creds': gcs_creds,
      'smtp.user': 'auctus',
      'smtp.password': 'auctus',
    },
    data: {
      [k]: std.base64(data[k])
      for k in std.objectFields(data)
    },
  }]),
}
