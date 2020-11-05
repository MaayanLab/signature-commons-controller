import os
import minio
from sigcom.util.first import first

inputs = (
  '*.data.tsv.so',
)

def requirements(uri=[], **kwargs):
  return 's3' in set([s for u in uri for s in u.scheme.split('+')])

def ingest(input_files, uri=[], **kwargs):
  so, = input_files
  # Get s3 uri
  s3_uri = first(u for u in uri if 's3' in u.scheme.split('+'))
  [_, s3_bucket, *subpath] = s3_uri.path.split('/')
  s3_prefix = '/'.join(subpath)
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
  # Upload object
  s3_client.fput_object(
    s3_bucket,
    '/'.join(filter(None, [s3_prefix, os.path.basename(so)])),
    so,
  )
