from dotenv import load_dotenv
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import minio
import base64
import os
import json

inputs = tuple()

after = (
  'data_gmt_so',
  'data_tsv_so',
)

def ingest(input_files):
  load_dotenv()
  
  # Connect to s3
  s3_endpoint_parsed = urlparse(os.environ['AWS_ENDPOINT_URL'])
  s3_client = minio.Minio(
    s3_endpoint_parsed.netloc,
    access_key=os.environ['AWS_ACCESS_KEY_ID'],
    secret_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    secure=s3_endpoint_parsed.scheme == 'https',
  )
  s3_bucket = os.environ['AWS_BUCKET']

  data_uri = urlparse(os.environ['DATA_API'])
  data_token = os.environ['TOKEN']

  # List all objects in s3
  objs = s3_client.list_objects(s3_bucket)
  for obj in objs:
    # Trigger data-api load
    urlopen(
      Request(
        '{scheme}://{host}{path}/api/v1/load'.format(
          scheme=data_uri.scheme,
          host=data_uri.hostname,
          path=data_uri.path,
        ),
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
