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
  statinfo =  os.stat(input_file)
  statinfo.st_size
  # estimate required ram ~ 50% more than base file size
  required_ram = int(statinfo.st_size * 1.5)
  check_call([
    'java', '-Xmx%d' % (required_ram),
    '-jar', os.path.join(os.path.dirname(__file__), '..', 'SignatureCommonsDataIngestion.jar'),
    '-m', 'expression',
    '-r', '-t',
    '-i', input_file,
    '-o', output_file,
  ])
