import json
import os

inputs = (
  '*.data.gmt',
  '*.signatures.jsonl',
  '*.entities.jsonl',
)

outputs = (
  '*.data.h5',
)

def transform(input_files, output_files, **kwargs):
  gmt, signature_meta, entity_meta = input_files
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
  try:
    gmt_parsed = {}
    with open(gmt, 'r') as fr:
      for line in fr:
        sig_id, ents = line.split('\t\t', maxsplit=1)
        gmt_parsed[sig_id_lookup[sig_id]] = [
          ent_id_lookup[ent_id]
          for ent_id in ents.split('\t')
        ]
      # TODO
  except Exception as e:
    os.remove(signature_data)
    raise e
