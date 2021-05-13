import os
from subprocess import check_call

inputs = (
  '*.data.T.tsv.so',
)
outputs = (
  '*.data.uuid.T.tsv',
)

def transform(input_files, output_files, **kwargs):
  input_file, = input_files
  output_file, = output_files
  statinfo =  os.stat(input_file)
  statinfo.st_size
  # estimate required ram
  required_ram = int(max(1e9, statinfo.st_size * 1.5))
  check_call([
    'java', '-Xmx%d' % (required_ram),
    '-jar', os.path.join(os.path.dirname(__file__), '..', 'SignatureCommonsDataIngestion.jar'),
    '-d', '-t',
    '-i', input_file,
    '-o', output_file,
  ])
