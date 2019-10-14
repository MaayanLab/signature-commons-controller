from dotenv import load_dotenv
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import base64
import os

inputs = tuple()

after = (
  'schemas_jsonld',
)

def ingest(input_files):
  load_dotenv()
  metadata_uri = urlparse(os.environ['METADATA_API'])
  metadata_token = base64.b64encode('{username}:{password}'.format(
    username=metadata_uri.username, password=metadata_uri.password
  ).encode()).decode()
  return urlopen(
    Request(
      '{scheme}://{hostname}{path}/summary/refresh'.format(
        scheme=metadata_uri.scheme,
        hostname=metadata_uri.hostname,
        path=metadata_uri.path,
      ),
      headers={
        'Content-Type': 'application/json',
        'Authorization': 'Basic {}'.format(metadata_token)
      },
    )
  )
