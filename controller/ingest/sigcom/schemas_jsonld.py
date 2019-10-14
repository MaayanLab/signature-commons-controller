from dotenv import load_dotenv
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import base64
import json
import os

inputs = (
  '*.schemas.jsonld',
)

after = tuple()

def ingest(input_files):
  schemas_meta, = input_files
  load_dotenv()
  with open(schemas_meta, 'r') as fr:
    for ents in _chunk(map(json.loads, fr)):
      _sigcom_meta_bulk([
        {
          'operationId': 'Schema.find_or_create',
          'requestBody': [
            _prepare_ent(ent)
            for ent in ents
          ],
        }
      ])

def _prepare_ent(ent):
  if '@id' in ent:
    ent['id'] = ent['@id']
    del ent['@id']
  ent['$validator'] = '/dcic/signature-commons-schema/v5/core/schema.json'
  if ent.get('@type'):
    del ent['@type']
  if '$validator' not in ent['meta']:
    ent['meta']['$validator'] = '/dcic/signature-commons-schema/v5/core/unknown.json'
  return ent

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
