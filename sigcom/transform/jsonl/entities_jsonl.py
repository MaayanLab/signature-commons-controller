import json
from sigcom.util.get_one_of import get_one_of

inputs = (
  '*.entities.jsonl',
)
outputs = (
  '*.entities.psql.tsv',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files
  with open(input_file, 'r') as fr:
    with open(output_file, 'w') as fw:
      for doc in map(json.loads, fr):
        print(
          get_one_of(doc, ('@id', 'id', '_id')),
          json.dumps(doc.get('meta', doc)),
          sep='\t', file=fw
        )
