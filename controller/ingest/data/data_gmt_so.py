import os
import minio
from ...util.first import first

inputs = (
  '*.data.gmt.so',
)

def requirements(uri=[], **kwargs):
  return 's3' in set([s for u in uri for s in u.scheme.split('+')])

def ingest(input_files, uri=[], **kwargs):
  so, = input_files
  # Get s3 uri
  s3_uri = first(u for u in uri if 's3' in u.scheme.split('+'))
  s3_bucket = s3_uri.path[1:]
  s3_access_key = s3_uri.username
  s3_secret_key = s3_uri.password
  del s3_uri.username
  # Establish s3 connection
  client = minio.Minio(
    s3_uri.netloc,
    access_key=s3_access_key,
    secret_key=s3_secret_key,
    secure='https' in s3_uri.scheme,
  )
  # Upload object
  client.fput_object(
    s3_bucket,
    os.path.basename(so),
    so,
  )
