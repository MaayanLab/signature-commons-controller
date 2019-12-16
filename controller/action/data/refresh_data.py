from urllib.request import Request, urlopen
from ...util import first
import minio
import json

def requirements(actions=[], uri=[], **kwargs):
  if actions and 'refresh_data' not in actions:
    return False
  return set([s for u in uri for s in u.scheme.split('+')]) >= set(['data', 's3'])

def apply(uri=[], **kwargs):
  # Get s3 and data api uris
  s3_uri = first(u for u in uri if 's3' in u.scheme.split('+'))
  data_uri = first(u for u in uri if 'data' in u.scheme.split('+'))
  s3_username = s3_uri.username
  s3_password = s3_uri.password
  del s3_uri.username
  # Connect to s3
  s3_client = minio.Minio(
    s3_uri.netloc,
    access_key=s3_username,
    secret_key=s3_password,
    secure='https' in s3_uri.scheme,
  )
  s3_bucket = s3_uri.path[1:]
  # Get data uri token
  data_token = data_uri.username
  # Format the data_uri
  del data_uri.username
  data_uri.path = data_uri.path + '/api/v1/load'
  data_uri.scheme = ''.join(set(['http', 'https']) & set(data_uri.scheme.split('+')))
  # For each object in the s3 bucket
  for obj in s3_client.list_objects(s3_bucket):
    # Only take .so files
    if obj.object_name.endswith('.so'):
      # Trigger data-api load
      urlopen(
        Request(
          str(data_uri),
          headers={
            'Authorization': 'Token {}'.format(data_token)
          },
          data=json.dumps({
            'bucket': s3_bucket,
            'file': obj.object_name,
            'datasetname': obj.object_name,
          }).encode(),
        )
      )
