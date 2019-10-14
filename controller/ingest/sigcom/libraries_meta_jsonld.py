from dotenv import load_dotenv
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import base64
import json
import os

inputs = (
  '*.libraries.jsonld',
)

after = tuple()

def ingest(input_files):
  signatures_meta, = input_files
  load_dotenv()
  with open(signatures_meta, 'r') as fr:
    for libs in _chunk(map(json.loads, fr)):
      _sigcom_meta_bulk([
        {
          'operationId': 'Library.find_or_create',
          'requestBody': [
            _prepare_lib(lib)
            for lib in libs
          ],
        }
      ])

def _prepare_lib(lib):
  lib['id'] = lib['@id']
  del lib['@id']
  lib['$validator'] = '/dcic/signature-commons-schema/v5/core/library.json'
  if lib.get('@type'):
    del lib['@type']
  lib['meta']['$validator'] = '/dcic/signature-commons-schema/v5/core/unknown.json'
  return lib

def _sigcom_meta_bulk(data):
  metadata_uri = urlparse(os.environ['METADATA_API'])
  metadata_token = base64.b64encode('{username}:{password}'.format(
    username=metadata_uri.username, password=metadata_uri.password
  ).encode()).decode()
  return urlopen(
    Request(
      '{scheme}://{hostname}{path}/bulk'.format(
        scheme=metadata_uri.scheme,
        hostname=metadata_uri.hostname,
        path=metadata_uri.path,
      ),
      data=json.dumps(data).encode(),
      headers={
        'Content-Type': 'application/json',
        'Authorization': 'Basic {}'.format(metadata_token)
      },
    )
  )

def _chunk(iterable, chunks=50):
  buffer = []
  for i, element in enumerate(iterable, start=1):
    buffer.append(element)
    if i % chunks == 0:
      yield buffer
      buffer = []
  if buffer:
    yield buffer
