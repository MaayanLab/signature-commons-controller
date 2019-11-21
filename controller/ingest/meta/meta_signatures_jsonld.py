from ...util import first, chunk
from urllib.request import Request, urlopen
import base64
import json
import os

name = 'meta_signatures_jsonld'

after = (
  'meta_libraries_jsonld',
)

inputs = (
  '*.signatures.jsonld',
)

def requirements(uri=[], **kwargs):
  return 'meta' in set([s for u in uri for s in u.scheme.split('+')])

def ingest(input_files, uri=[], limit=1000, **kwargs):
  signatures_meta, = input_files
  # Get the meta_uri only
  meta_uri = first(u for u in uri if 'meta' in u.scheme.split('+'))
  # Extract token
  metadata_token = base64.b64encode('{username}:{password}'.format(
    username=meta_uri.username, password=meta_uri.password
  ).encode()).decode()
  # Prepare uri
  del meta_uri.username
  meta_uri.scheme = ''.join(set(['http', 'https']) & set(meta_uri.scheme.split('+')))
  meta_uri.path = meta_uri.path + '/bulk'
  #
  with open(signatures_meta, 'r') as fr:
    for objs in chunk(map(json.loads, fr), limit=limit):
      urlopen(
        Request(
          str(meta_uri),
          data=json.dumps([
            {
              'operationId': 'Signature.find_or_create',
              'requestBody': [
                _prepare_obj(obj)
                for obj in objs
              ],
            }
          ]).encode(),
          headers={
            'Content-Type': 'application/json',
            'Authorization': 'Basic {}'.format(metadata_token)
          },
        )
      )

def _prepare_obj(obj):
  obj['id'] = obj['@id']
  del obj['@id']
  obj['$validator'] = '/dcic/signature-commons-schema/v5/core/signature.json'
  if obj.get('@type'):
    del obj['@type']
  obj['meta']['$validator'] = '/dcic/signature-commons-schema/v5/core/unknown.json'
  return obj
