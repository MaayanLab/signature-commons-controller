import os
import minio
from urllib.parse import urlparse
from dotenv import load_dotenv

inputs = (
  '*.data.tsv.so',
)

after = tuple()

def ingest(input_files):
  so, = input_files
  load_dotenv()

  endpoint_parsed = urlparse(os.environ['AWS_ENDPOINT_URL'])
  
  client = minio.Minio(
    endpoint_parsed.netloc,
    access_key=os.environ['AWS_ACCESS_KEY_ID'],
    secret_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    secure=endpoint_parsed.scheme == 'https',
  )
  client.fput_object(
    os.environ['AWS_BUCKET'],
    os.path.basename(so),
    so,
  )
