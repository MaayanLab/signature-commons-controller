import os
import pymongo
import json
from ...util import first

def requirements(uri=[], **kwargs):
  return 'mongo' in set([s for u in uri for s in u.scheme.split('+')])

outputs = (
  '*.datasets.meta.jsonld',
  '*.resources.meta.jsonld',
  '*.libraries.meta.jsonld',
  '*.signatures.meta.jsonld',
  '*.entities.meta.jsonld',
  '*.schemas.meta.jsonld',
)

def extract(uri=[], **kwargs):
  # Get mongo uri
  mongo_uri = first(u for u in uri if 'mongo' in u.scheme.split('+'))
  # Get extract mongo db name
  db_path = mongo_uri.path[1:]
  del mongo_uri.path
  # Instantiate mongo client
  mongo = pymongo.MongoClient(str(mongo_uri))
  # Get mongo db
  db = getattr(mongo, db_path)
  #
  for tbl in [
    'datasets',
    'resources',
    'libraries',
    'signature_meta',
    'entities',
    'schemas',
  ]:
    with open('{}.{}.meta.jsonld'.format(db_path, tbl), 'w') as fw:
      collection = getattr(db, tbl)
      for signature in collection.find():
        print(json.dumps(_process_obj(signature)), file=fw)
  #

def _process_obj(obj):
  obj['@id'] = obj['_id']
  del obj['_id']
  return obj
