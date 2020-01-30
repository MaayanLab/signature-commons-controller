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
          doc['@id'],
          '' if doc.get('resource') is None else doc['resource'],
          '' if doc.get('dataset') is None else doc['dataset'],
          '' if doc.get('dataset_type') is None else doc['dataset_type'],
          doc['meta'],
          sep='\t', file=fw
        )
