import os
import minio
from ...util.first import first

inputs = (
  '*.data.gmt.so',
)

def requirements(uri=[], **kwargs):
  return 'data' in set([u.scheme for u in uri])

def ingest(input_files, uri=[], **kwargs):
  so, = input_files
  # Get s3 uri
  s3_uri = first(u for u in uri if 's3' in u.scheme.split('+'))
  s3_bucket = s3_uri.path[1:]
  # Establish s3 connection
  client = minio.Minio(
    s3_uri.netloc,
    access_key=s3_uri.username,
    secret_key=s3_uri.password,
    secure='https' in s3_uri.scheme,
  )
  # Upload object
  client.fput_object(
    s3_bucket,
    os.path.basename(so),
    so,
  )
