import json
import csv
import uuid
import os

inputs = (
  '*.entities.tsv',
)
outputs = (
  '*.entities.jsonl',
)

U = uuid.UUID('00000000-0000-0000-0000-000000000000')
def canonical_uuid(obj):
  return str(uuid.uuid5(U, str(obj)))

def try_json_loads(v):
  try:
    return json.loads(v)
  except:
    return v

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files

  ents = set()

  with open(input_file, 'r') as fr:
    reader = csv.reader(fr, delimiter='\t')
    header = next(iter(reader))
    try:
      with open(output_file, 'w') as fw:
        for ent in reader:
          ent_meta = {
            k: v
            for k, v in zip(header, map(try_json_loads, ent))
            if v and v != float('nan')
          }
          ent_id = canonical_uuid(ent_meta)
          if ent_id not in ents:
            print(
              json.dumps({
                '@id': ent_id,
                '@type': 'Entity',
                'meta': ent_meta,
              }),
              file=fw
            )
            ents.add(ent_id)
    except Exception as e:
      os.remove(output_file)
      raise e
