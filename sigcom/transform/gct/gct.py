import h5py
import json
import uuid
import csv
import itertools
import os

inputs = (
  '*.gct',
)

outputs = (
  '*.data.uuid.tsv',
  '*.signatures.jsonl',
  '*.entities.jsonl',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  signature_data, signature_meta, entity_meta = output_files

  with open(input_file, 'r') as fr:
    version = next(iter(next(iter(csv.reader(fr, delimiter='\t')))))
    if version == '#1.3':
      _gct1_3(fr, signature_data, signature_meta, entity_meta)
    else:
      raise Exception('Unhandled GCT version {}'.format(version))

def _gct1_3(input_file_stream, signature_data, signature_meta, entity_meta):
  input_file_reader = csv.reader(input_file_stream, delimiter='\t')
  _, _, n_meta_rows, n_meta_cols = map(int, next(iter(input_file_reader)))

  # grab the first line
  header = next(iter(input_file_reader))

  # construct column metadata dictionaries
  col_meta = {
    ind: {
      '@id': str(uuid.uuid4()),
      '@type': 'Signature',
      'meta': {
        header[0]: col
      }
    }
    for ind, col in enumerate(header[1+n_meta_rows:])
  }
  for meta_cols in itertools.islice(input_file_reader, None, n_meta_cols):
    for ind, col in enumerate(meta_cols[1+n_meta_rows:]):
      col_meta[ind]['meta'][meta_cols[0]] = col

  # write column metadata
  try:
    with open(signature_meta, 'w') as fw:
      for col in col_meta.values():
        print(json.dumps(col), file=fw)
  except Exception as e:
    os.remove(signature_meta)
    raise e

  # Walk down the file collecting row metadata and expression values
  try:
    with open(signature_data, 'w') as fw_data:
      # write the header of the matrix
      print('', *[
        col_meta[ind]['@id']
        for ind in range(len(col_meta))
      ], sep='\t', file=fw_data)

      with open(entity_meta, 'w') as fw_entity:
        for row in input_file_reader:
          row_meta = {
            '@id': str(uuid.uuid4()), # TODO: resolve existing entities
            '@type': 'Entity',
            'meta': {
              header[ind]: r
              for ind, r in enumerate(row[:1+n_meta_rows])
            }
          }
          # write the entity json
          print(json.dumps(row_meta), file=fw_entity)
          # write the expression
          print(row_meta['@id'], *row[1+n_meta_rows:], sep='\t', file=fw_data)
  except Exception as e:
    os.remove(signature_data)
    raise e
