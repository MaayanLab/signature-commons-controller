import os

inputs = (
  '*.data.geneset.h5',
)
outputs = (
  '*.data.gmt',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files
  # TODO