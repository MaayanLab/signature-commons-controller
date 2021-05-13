import json
import csv
import os

inputs = (
  '*.data.T.tsv',
  '*.signatures.jsonl',
  '*.entities.jsonl',
)
outputs = (
  '*.data.uuid.T.tsv',
)

def transform(input_files, output_files, **kwargs):
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
    cols = [
      ent_id_lookup[col]
      for col in header[1:]
      if ent_id_lookup.get(col)
    ]
    try:
      with open(signature_data, 'w') as fw:
        print('', *[
          ent_id_lookup[col]
          for col in header[1:]
          if ent_id_lookup.get(col)
        ], sep='\t', file=fw)
        for row in reader:
          print(
            sig_id_lookup[row[0]],
            *[
              cell
              for col, cell in zip(header[1:], row[1:])
              if ent_id_lookup.get(col)
            ],
            sep='\t',
            file=fw,
          )
    except Exception as e:
      os.remove(signature_data)
      raise e