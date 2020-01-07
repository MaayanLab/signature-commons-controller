import os, os.path
import json
import base64
from controller.util import first, pagination
from urllib.request import Request, urlopen

def requirements(uri=[], **kwargs):
  return 'meta' in set([s for u in uri for s in u.scheme.split('+')])

outputs = (
  '*.resources.jsonld',
)

def extract(path=None, uri=[], limit=1000, **kwargs):
  # Get the meta_uri only
  meta_uri = first(u for u in uri if 'meta' in u.scheme.split('+'))
  # Extract the credentials
  metadata_token = base64.b64encode('{username}:{password}'.format(
    username=meta_uri.username, password=meta_uri.password
  ).encode()).decode()
  # Format the meta_uri
  del meta_uri.username
  meta_uri.scheme = ''.join(set(['http', 'https']) & set(meta_uri.scheme.split('+')))
  meta_base_path = meta_uri.path
  #
  tbl = 'resources'
  with open(os.path.join(path, '_.{}.jsonld'.format(tbl)), 'w') as fw:
    meta_uri.path = meta_base_path + '/{}/count'.format(tbl)
    #
    n_objs = json.load(urlopen(
      Request(
        str(meta_uri),
        headers={
          'Content-Type': 'application/json',
          'Authorization': 'Basic {}'.format(metadata_token)
        },
      )
    ))['count']
    #
    meta_uri.path = meta_base_path + '/{}'.format(tbl)
    #
    for skip, limit in pagination(n_objs, limit=limit):
      meta_uri.query = { 'filter': json.dumps({ 'skip': skip, 'limit': limit }) }
      objs = json.load(urlopen(
        Request(
          str(meta_uri),
          headers={
            'Content-Type': 'application/json',
            'Authorization': 'Basic {}'.format(metadata_token)
          },
        )
      ))
      for obj in objs:
        print(json.dumps(_process_obj(obj)), file=fw)
    #
    del meta_uri.query
  #

def _process_obj(obj):
  if obj.get('id'):
    obj['@id'] = obj['id']
    del obj['id']
  if obj.get('$validator'):
    obj['@type'] = obj['$validator']
    del obj['$validator']
  if obj.get('meta') and obj['meta'].get('$validator'):
    obj['meta']['@type'] = obj['meta']['$validator']
    del obj['meta']['$validator']
  return obj
