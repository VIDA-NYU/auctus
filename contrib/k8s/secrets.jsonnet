// Set 'private_app: true' in the config to password-protect frontend & API
// You can create this file using the htpasswd tool
local private_app_password = |||
  auctus:$apr1$ECD/OaHB$CMBSkoEdcA/2uX8gPZM3y1
|||;

local amqp_user = 'auctususer';
local amqp_password = 'auctuspassword';
local admin_password = 'auctuspassword';
local s3_key = 'devkey';
local s3_secret = 'devpassword';
local gcs_creds = std.base64('');

{
  'secrets.yml': std.manifestYamlStream([
    {
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
        'smtp.user': 'auctususer',
        'smtp.password': 'auctuspassword',
      },
      data: {
        [k]: std.base64(data[k])
        for k in std.objectFields(data)
      },
    },
    {
      apiVersion: 'v1',
      kind: 'Secret',
      type: 'Opaque',
      metadata: {
        name: 'basic-auth',
      },
      data: {
        auth: std.base64(private_app_password),
      },
    },
  ]),
}
