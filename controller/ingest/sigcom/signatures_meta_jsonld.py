from dotenv import load_dotenv
from urllib.request import Request, urlopen
from urllib.parse import urlparse
import base64
import json
import os

inputs = (
  '*.signatures.jsonld',
)

after = (
  'libraries_meta_jsonld',
)

def ingest(input_files):
  signatures_meta, = input_files
  load_dotenv()
  with open(signatures_meta, 'r') as fr:
    for sigs in _chunk(map(json.loads, fr)):
      print(
        json.dumps(json.load(_sigcom_meta_bulk([
          {
            'operationId': 'Signature.find_or_create',
            'requestBody': [
              _prepare_sig(sig)
              for sig in sigs
            ],
          }
        ])), indent=2)
      )

def _prepare_sig(sig):
  sig['id'] = sig['@id']
  del sig['@id']
  sig['$validator'] = '/dcic/signature-commons-schema/v5/core/signature.json'
  if sig.get('@type'):
    del sig['@type']
  sig['meta']['$validator'] = '/dcic/signature-commons-schema/v5/core/unknown.json'
  return sig

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
