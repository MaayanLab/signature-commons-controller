import os
import pymongo
import json
from sigcom.util import first
from urllib.request import Request, urlopen

def requirements(uri=[], **kwargs):
  return 'data' in set([s for u in uri for s in u.scheme.split('+')])

outputs = (
  '*.datasets.meta.jsonl',
)

def extract(path=None, uri=[], **kwargs):
  #
  # Get the data_uri
  data_uri = first(u for u in uri if 'data' in u.scheme.split('+'))
  data_token = data_uri.username
  del data_uri.username
  data_uri.scheme = ''.join(set(['http', 'https']) & set(data_uri.scheme.split('+')))
  data_uri_base = data_uri.path
  # Get repositories
  data_uri.path = data_uri_base + '/api/v1/listdata'
  repos = json.load(
    urlopen(
      Request(
        str(data_uri),
        headers={
          'Content-Type': 'application/json',
          'Authorization': 'Token {}'.format(data_token)
        },
      )
    )
  )['repositories']
  with open(os.path.join(path, '_.datasets.meta.jsonl'), 'w') as fw:
    for repo in repos:
      print(json.dumps({
        '@id': repo['uuid'],
        '@type': repo['datatype'],
      }), file=fw)
  #
