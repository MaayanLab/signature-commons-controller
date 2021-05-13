import os
import numpy as np
import pandas as pd

inputs = (
  '*.data.rankmatrix.h5',
)
outputs = (
  '*.data.tsv',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files
  fr = pd.read_csv(input_file, 'r')
  keys = sorted(map(int, fr['data']['expression'].keys()))
  shape = (
    fr['data']['expression']['0'].shape[0],
    sum(fr['data']['expression'][str(k)].shape[1] for k in keys),
  )
  data = pd.DataFrame(
    np.zeros(shape=shape, dtype=np.int16),
    index=fr['data']['meta']['rowid'][:],
    columns=fr['data']['meta']['colid'][:],
  )
  colid = 0
  for ind in sorted(map(int, fr['data']['expression'].keys())):
    chunk = fr['data']['expression'][str(ind)]
    data.iloc[:, colid:colid+chunk.shape[1]] = chunk[:, :]
    colid += chunk.shape[1]
  data.to_csv(output_file, sep='\t')
