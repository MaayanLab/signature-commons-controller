import os
import pymongo
from sigcom.util import first, mongo_bulk_upsert

inputs = (
  '*.data.uuid.gmt',
)

def requirements(uri=[], **kwargs):
  return 'mongodb' in set([u.scheme for u in uri])

def ingest(input_files, uri=[], limit=1000, **kawrgs):
  input_file, = input_files
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
  def generate_signatures():
    with open(input_file, 'r') as fr:
      for line in fr:
        sigid, ents = line.split('\t\t', maxsplit=1)
        entids = ents.strip().split('\t')
        yield {
          '_id': sigid,
        }, {
          '$set': {
            'data.set': {
              entid: 1
              for entid in entids
            },
            'data.size': len(entids),
          },
        }
  #
  mongo_bulk_upsert(
    db.signature_data,
    generate_signatures(),
    limit=limit,
  )
