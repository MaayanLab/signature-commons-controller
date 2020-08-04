import os.path
import pymongo
import csv
from controller.util import first

def requirements(uri=[], **kwargs):
  return 'mongodb' in set([s for u in uri for s in u.scheme.split('+')])

outputs = (
  '*.data.uuid.T.tsv',
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
  for dataset in db.datasets.find({'@type': 'rank'}, {'_id': 1, 'entities': 1}):
    with open(os.path.join(path, '{}_{}.data.uuid.T.tsv'.format(db_path, dataset['_id'])), 'w') as fw:
      writer = csv.writer(fw, delimiter='\t')
      writer.writerow(['', *dataset['entities']])
      for signature in db.signature_data.find({
        'dataset': dataset['_id'],
        'data.rank': { '$ne': None }
      }, {
        '_id': 1,
        'data.rank': 1
      }):
        print(
          str(signature['_id']),
          *[
            signature['data']['rank'].get(ent, '')
            for ent in dataset['entities']
          ],
          sep='\t',
          file=fw,
        )
  #
