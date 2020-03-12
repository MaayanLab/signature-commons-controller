import os
import minio
from controller.util.first import first

outputs = (
  '*.data.tsv.so',
)

def requirements(uri=[], **kwargs):
  return 's3' in set([s for u in uri for s in u.scheme.split('+')])

def extract(path=None, uri=[], **kwargs):
  # Get s3 uri
  s3_uri = first(u for u in uri if 's3' in u.scheme.split('+'))
  s3_bucket = s3_uri.path[1:]
  s3_access_key = s3_uri.username
  s3_secret_key = s3_uri.password
  del s3_uri.username
  # Establish s3 connection
  s3_client = minio.Minio(
    s3_uri.netloc,
    access_key=s3_access_key,
    secret_key=s3_secret_key,
    secure='https' in s3_uri.scheme,
  )
  # Find objects
  for obj in s3_client.list_objects(s3_bucket):
    if obj.object_name.endswith('.data.tsv.so'):
      s3_client.fget_object(
        s3_bucket,
        obj.object_name,
        os.path.join(path, obj.object_name)
      )
