import os
import minio
from urllib.parse import urlparse
from dotenv import load_dotenv

inputs = (
  '*.data.gmt.so',
)

after = tuple()

def ingest(input_files):
  so, = input_files
  load_dotenv()

  endpoint_parsed = urlparse(os.environ['AWS_ENDPOINT_URL'])
  
  client = minio.Minio(
    endpoint_parsed.netloc,
    access_key=os.environ['MINIO_ACCESS_KEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    secure=endpoint_parsed.scheme == 'https',
  )
  client.fput_object(
    os.environ['MINIO_BUCKET'],
    os.path.basename(so),
    so,
  )
  # TODO: inform data API
