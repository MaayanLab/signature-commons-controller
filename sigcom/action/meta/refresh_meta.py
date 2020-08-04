from urllib.request import Request, urlopen
from urllib.error import HTTPError
from sigcom.util import first
import base64
import time

def requirements(actions=[], uri=[], **kwargs):
  if actions and 'refresh_meta' not in actions:
    return False
  return 'meta' in set([s for u in uri for s in u.scheme.split('+')])

def apply(uri=[], **kwargs):
  # Get the meta_uri only
  meta_uri = first(u for u in uri if 'meta' in u.scheme.split('+'))
  # Extract the credentials
  metadata_token = base64.b64encode('{username}:{password}'.format(
    username=meta_uri.username, password=meta_uri.password
  ).encode()).decode()
  # Format the meta_uri
  del meta_uri.username
  meta_uri.path = meta_uri.path + '/optimize/refresh'
  meta_uri.scheme = ''.join(set(['http', 'https']) & set(meta_uri.scheme.split('+')))
  # Make the request
  backoff = 2
  while True:
    try:
      req = urlopen(
        Request(
          str(meta_uri),
          headers={
            'Content-Type': 'application/json',
            'Authorization': 'Basic {}'.format(metadata_token)
          },
        )
      )
      print('[refresh_meta]: done')
      return req
    except HTTPError as e:
      if e.code == 409:
        backoff *= 2
        print('[refresh_meta]: An operation is already in progress: {}, trying again in {}s'.format(e.read(), backoff))
        time.sleep(backoff)
        continue
      else:
        print('[refresh_meta]: HTTP Error {}: {}'.format(e.code, e.read()))
      break
