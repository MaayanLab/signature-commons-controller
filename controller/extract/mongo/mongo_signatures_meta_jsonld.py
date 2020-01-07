import os.path
import pymongo
import json
from controller.util import first

def requirements(uri=[], **kwargs):
  return 'mongodb' in set([s for u in uri for s in u.scheme.split('+')])

outputs = (
  '*.signatures.jsonld',
)

def extract(path=None, uri=[], **kwargs):
  # Get mongo uri
  mongo_uri = first(u for u in uri if 'mongodb' in u.scheme.split('+'))
  # Get extract mongo db name
  db_path = mongo_uri.path[1:]
  del mongo_uri.path
  # Instantiate mongo client
  mongo = pymongo.MongoClient(str(mongo_uri))
  # Get mongo db
  db = getattr(mongo, db_path)
  #
  with open(os.path.join(path, '{}.signatures.jsonld'.format(db_path)), 'w') as fw:
    collection = getattr(db, 'signature_meta')
    for signature in collection.find():
      print(json.dumps(_process_obj(signature)), file=fw)
  #

def _process_obj(obj):
  obj['@id'] = str(obj['_id'])
  del obj['_id']
  if obj.get('library'):
    obj['library'] = str(obj['library'])
  return obj
