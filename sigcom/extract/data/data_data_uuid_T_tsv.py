import os.path
import json
import base64
from sigcom.util import first, pagination
from urllib.request import Request, urlopen

def requirements(uri=[], **kwargs):
  return set(['meta', 'data']) <= set([s for u in uri for s in u.scheme.split('+')])

outputs = (
  '*.data.uuid.T.tsv',
)

def extract(path=None, uri=[], limit=1000, **kwargs):
  # Get the meta_uri
  meta_uri = first(u for u in uri if 'meta' in u.scheme.split('+'))
  metadata_token = base64.b64encode('{username}:{password}'.format(
    username=meta_uri.username, password=meta_uri.password
  ).encode()).decode()
  del meta_uri.username
  meta_uri.scheme = ''.join(set(['http', 'https']) & set(meta_uri.scheme.split('+')))
  meta_uri_base = meta_uri.path
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
  #
  # Get genelist data
  #
  data_uri.path = data_uri_base + '/api/v1/fetch/rank'
  for repo in repos:
    if repo['datatype'] != 'rank_matrix':
      continue
    repo_id = repo['uuid']
    with open(os.path.join(path, '{}.data.uuid.T.tsv'.format(repo_id)), 'w') as fw:
      header = False
      meta_uri.path = meta_uri_base + '/libraries'
      meta_uri.query = {
        'filter': json.dumps({
          'fields': ['id'],
          'where': { 'dataset': repo_id },
        }),
      }
      library_ids = [
        lib['id']
        for lib in json.load(urlopen(
          Request(
            str(meta_uri),
            headers={
              'Content-Type': 'application/json',
              'Authorization': 'Basic {}'.format(metadata_token)
            },
          )
        ))
      ]
      meta_uri.path = meta_uri_base + '/signatures/count'
      meta_uri.query = {
        'where': json.dumps({
          'library': { 'inq': library_ids }
        })
      }
      n_signatures = json.load(urlopen(
        Request(
          str(meta_uri),
          headers={
            'Content-Type': 'application/json',
            'Authorization': 'Basic {}'.format(metadata_token)
          },
        )
      ))['count']
      meta_uri.path = meta_uri_base + '/signatures'
      for skip, limit in pagination(n_signatures, limit=limit):
        meta_uri.query = {
          'filter': json.dumps({
            'fields': ['id'],
            'where': {
              'library': { 'inq': library_ids },
            },
            'skip': skip,
            'limit': limit,
          }),
        }
        signature_ids = [
          sig['id']
          for sig in json.load(urlopen(
            Request(
              str(meta_uri),
              headers={
                'Content-Type': 'application/json',
                'Authorization': 'Basic {}'.format(metadata_token)
              },
            )
          ))
        ]
        signature_data = json.load(urlopen(
          Request(
            str(data_uri),
            data=json.dumps({
              'database': repo_id,
              'signatures': signature_ids,
              'entities': [],
            }).encode('utf8'),
            headers={
              'Content-Type': 'application/json',
              'Authorization': 'Token {}'.format(data_token)
            },
          )
        ))
        if not header:
          print('', *signature_data['entities'], sep='\t', file=fw)
          header = True
        for signature in signature_data['signatures']:
          rank_lookup = dict(zip(*reversed(list(zip(*enumerate(sorted(signature['ranks'])))))))
          print(signature['uid'], *[
            rank_lookup[rank]
            for rank in signature['ranks']
          ], sep='\t', file=fw)
    #
    del meta_uri.query
