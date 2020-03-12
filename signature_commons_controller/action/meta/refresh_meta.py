from urllib.request import Request, urlopen
from controller.util import first
import base64

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
  return urlopen(
    Request(
      str(meta_uri),
      headers={
        'Content-Type': 'application/json',
        'Authorization': 'Basic {}'.format(metadata_token)
      },
    )
  )
