import json
import csv
import os
from sigcom.util import canonical_uuid, try_json_loads

inputs = (
  '*.libraries.tsv',
)
outputs = (
  '*.libraries.jsonl',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files

  libs = set()

  with open(input_file, 'r') as fr:
    reader = csv.reader(fr, delimiter='\t')
    header = next(iter(reader))
    try:
      with open(output_file, 'w') as fw:
        for lib in reader:
          lib_meta = {
            k: v
            for k, v in zip(header, map(try_json_loads, lib))
            if v and v != float('nan')
          }
          lib_id = canonical_uuid(lib_meta)
          if lib_id not in libs:
            print(
              json.dumps({
                '@id': lib_id,
                '@type': 'Library',
                'meta': lib_meta,
              }),
              file=fw
            )
            libs.add(lib_id)
    except Exception as e:
      os.remove(output_file)
      raise e
