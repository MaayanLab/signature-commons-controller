import csv
import uuid
import json
import os

inputs = (
  '*.data.gmt',
  '*.signatures.jsonld',
  '*.entities.jsonld',
)

outputs = (
  '*.data.uuid.gmt',
)

def transform(input_files, output_files):
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

  with open(gmt, 'r') as fr:
    try:
      with open(signature_data, 'w') as fw:
        for line in fr:
          sig_id, ents = line.split('\t\t', maxsplit=1)
          # resolve entities (with/without expression)
          ents_split = ents.split('\t')
          ents_resolved = [
            ent_id_lookup[ent]
            for ent in ents_split
            if ent_id_lookup.get(ent)
          ]
          print(
            sig_id_lookup[sig_id],
            '',
            *ents_resolved,
            sep='\t',
            file=fw,
          )
    except Exception as e:
      os.remove(signature_data)
      raise e
