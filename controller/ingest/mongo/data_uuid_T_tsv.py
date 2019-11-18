import os
import pymongo
import csv
from urllib.parse import urlparse
from dotenv import load_dotenv

inputs = (
  '*.data.uuid.T.tsv',
)

after = tuple()

def ingest(input_files):
  input_file, = input_files
  #
  load_dotenv()
  #
  mongo_uri = urlparse(os.environ['MONGO_URI'])
  mongo = pymongo.MongoClient('{scheme}://{netloc}'.format(
    scheme=mongo_uri.scheme,
    netloc=mongo_uri.netloc,
  ))
  #
  db = getattr(mongo, mongo_uri.path[1:])
  #
  def generate_signatures():
    with open(input_file, 'r') as fr:
      reader = csv.reader(fr, delimiter='\t')
      entities = next(iter(reader))[1:]
      #
      for line in reader:
        sigid = line[0]
        yield {
          '_id': sigid,
        }, {
          '$set': {
            'data.expression': {
              entid: float(expression)
              for entid, expression in zip(entities, line[1:])
            },
          },
        }
  #
  bulk_upsert(
    db.signature_data,
    generate_signatures(),
  )


def chunk(iterable, chunks=1000):
  ''' Chunk iterators helper
  '''
  buffer = []
  for i, element in enumerate(iterable, start=1):
    buffer.append(element)
    if i % chunks == 0:
      yield buffer
      buffer = []
  if buffer:
    yield buffer

def bulk_upsert(collection, iterable, chunks=1000):
  ''' mongo bulk insertion helper
  '''
  for it in chunk(iterable, chunks=chunks):
    collection.bulk_write([
      pymongo.UpdateOne(filter, update, upsert=True)
      for filter, update in it
    ], ordered=False)
