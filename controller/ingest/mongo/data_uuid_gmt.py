import os
import pymongo
from urllib.parse import urlparse
from dotenv import load_dotenv

inputs = (
  '*.data.uuid.gmt',
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
      for line in fr:
        sigid, ents = line.strip().split('\t\t', maxsplit=1)
        entids = ents.split('\t')
        yield {
          '_id': sigid,
          '$set': {
            'data.set': {
              entid: 1
              for entid in entids
            },
            'data.size': len(entids),
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
