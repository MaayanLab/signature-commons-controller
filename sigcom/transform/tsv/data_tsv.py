import json
import h5py
import numpy as np
import pandas as pd
from sigcom.util import generate_slices

inputs = (
  '*.data.tsv',
  '*.signatures.jsonl',
  '*.entities.jsonl',
)
outputs = (
  '*.data.rankmatrix.h5',
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

  fr = pd.read_csv(tsv, sep='\t', index_col=0)
  fw = h5py.File(signature_data, 'w')
  for ind, (start, end) in enumerate(generate_slices(fr.shape[1], 10000)):
    ranked = fr.iloc[:, start:end].rank(axis=0, method='first').astype(np.int16)
    fw.create_dataset('data/expression/{}'.format(ind), data=ranked.values)
  fw.create_dataset('meta/colid', data=fr.columns.map(lambda col: sig_id_lookup[col]).values, dtype=h5py.string_dtype('utf-8'))
  fw.create_dataset('meta/rowid', data=fr.index.map(lambda row: ent_id_lookup[row]).values, dtype=h5py.string_dtype('utf-8'))
  fw.create_dataset('meta/orig_colid', data=fr.columns.values, dtype=h5py.string_dtype('utf-8'))
  fw.create_dataset('meta/orig_rowid', data=fr.index.values, dtype=h5py.string_dtype('utf-8'))
  fw.close()
