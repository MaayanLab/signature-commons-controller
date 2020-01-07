import json
import os

inputs = (
  '*.libraries.jsonld',
)
outputs = (
  '*.libraries.psql.tsv',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files
  with open(input_file, 'r') as fr:
    with open(output_file, 'w') as fw:
      for doc in map(json.loads, fr):
        print(
          doc['@id'], doc['resource'], doc['dataset'], doc['dataset_type'], doc['meta'],
          sep='\t', file=fw
        )
