import h5py
import json
import uuid
import csv
import itertools
import os

inputs = (
  '*.data.tsv',
  '*.signatures.jsonld',
  '*.entities.jsonld',
)
outputs = (
  '*.data.uuid.tsv',
)

def transform(input_files, output_files):
  tsv, signature_meta, entity_meta = input_files
  signature_data, = output_files

  sig_id_lookup = {
    sig['meta']['id']: sig['@id']
    for sig in map(json.loads, open(signature_meta, 'r'))
  }
  ent_id_lookup = {
    sym: ent['@id']
    for ent in map(json.loads, open(entity_meta, 'r'))
    for sym in ([ent['meta']['Name']] + ent['meta'].get('Synonyms', []))
  }

  with open(tsv, 'r') as fr:
    reader = csv.reader(fr, delimiter='\t')
    header = next(iter(reader))
    try:
      with open(signature_data, 'w') as fw:
        print('', *[
          sig_id_lookup[col]
          for col in header[1:]
        ], sep='\t', file=fw)
        for row in reader:
          print(ent_id_lookup[row[0]], *row[1:], sep='\t', file=fw)
    except Exception as e:
      os.remove(signature_data)
      raise e