import pymongo
from sigcom.util.chunk import chunk

def mongo_bulk_upsert(collection, iterable, limit=1000):
  ''' mongo bulk insertion helper
  '''
  for it in chunk(iterable, limit=limit):
    collection.bulk_write([
      pymongo.UpdateOne(filter, update, upsert=True)
      for filter, update in it
    ], ordered=False)
