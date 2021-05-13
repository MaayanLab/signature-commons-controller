import json
import csv
import os
from sigcom.util import canonical_uuid, try_json_loads

inputs = (
  '*.signatures.tsv',
)
outputs = (
  '*.signatures.jsonl',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files

  sigs = set()

  with open(input_file, 'r') as fr:
    reader = csv.reader(fr, delimiter='\t')
    header = next(iter(reader))
    try:
      with open(output_file, 'w') as fw:
        for sig in reader:
          sig_meta = {
            k: v
            for k, v in zip(header, map(try_json_loads, sig))
            if v and v != float('nan')
          }
          sig_id = canonical_uuid(sig_meta)
          if sig_id not in sigs:
            print(
              json.dumps({
                '@id': sig_id,
                '@type': 'Signature',
                'meta': sig_meta,
              }),
              file=fw
            )
            sigs.add(sig_id)
    except Exception as e:
      os.remove(output_file)
      raise e
