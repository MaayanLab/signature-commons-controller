import os
import sh
import json
import uuid
import csv
import more_itertools as mit
from functools import partial

data_dir = 'output'
sourcefile = 'Homo_sapiens.gene_info'
entfile = 'entities.jsonl'
dls = {
  f"{sourcefile}.gz": "https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz",
}

curl = sh.Command('curl')
gunzip = sh.Command('gunzip')


def dict_drop(d, values=set()):
  ''' Remove key/values from dict where value is in `values`
  '''
  for k in list(d.keys()):
    if d[k] in values: del d[k]
  return d

def apply_converters(d, converters={}, default=lambda v: v):
  ''' Apply converter based on key, otherwise apply the default
  '''
  return {
    k: converters.get(k, default)(v)
    for k, v in d.items()
  }

def apply_rename(d, rename={}):
  ''' Rename keys in a dictionary
  '''
  return {
    rename.get(k, k): v
    for k, v in d.items()
  }

def try_json_loads(v):
  ''' Interpret string as json if it's valid, otherwise treat as string
  '''
  try:
    return json.loads(v)
  except:
    return v

def pipe_split(v):
  return v.split('|')

def uuid_stable(v):
  ''' Produce uuids that will remain stable across runs
  '''
  return str(uuid.uuid5(
    uuid.UUID(int=0),
    json.dumps(v, sort_keys=True)
  ))

# ensure sourcefile
if not os.path.exists(os.path.join(data_dir, sourcefile)):
  if not os.path.exists(os.path.join(data_dir, f"{sourcefile}.gz")):
    curl(dls[f"{sourcefile}.gz"], '-o', os.path.join(data_dir, f"{sourcefile}.gz"))
  gunzip(os.path.join(data_dir, f"{sourcefile}.gz"))


# load and parse ncbi table
with open(os.path.join(data_dir, sourcefile), 'r') as fr:
  records = csv.DictReader(fr, delimiter='\t')
  # remove na values
  records = map(partial(dict_drop, values={'', '-', None}), records)
  # parse values
  records = map(partial(apply_converters,
    converters={
      'Synonyms': pipe_split,
      'dbXrefs': pipe_split,
      'Other_designations': pipe_split,
    },
    default=try_json_loads,
  ), records)
  # rename columns
  records = map(partial(apply_rename,
    rename={
      '#tax_id': 'taxonomy', # identifiers.org
      'GeneID': 'ncbigene', # identifiers.org
    },
  ), records)
  # add stable uuid identifier, put record in meta
  records = (dict(id=uuid_stable(record), meta=record) for record in records)
  # write records as json
  records = (json.dumps(record, sort_keys=True) for record in records)
  # save to file
  with open(os.path.join(data_dir, entfile), 'w') as fw:
    mit.consume(mit.side_effect(partial(print, file=fw), records))
