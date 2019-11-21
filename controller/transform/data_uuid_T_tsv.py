import os
from subprocess import check_output, check_call

inputs = (
  '*.data.uuid.T.tsv',
)
outputs = (
  '*.data.tsv.so',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files

  check_call([
    'java', '-Xmx20G', #TODO automatically determine this value by file size
    '-jar', os.path.join(os.path.dirname(__file__), '..', 'SignatureCommonsDataIngestion.jar'),
    '-m', 'expression',
    '-r', '-t',
    '-i', input_file,
    '-o', output_file,
  ])
