import json
from subprocess import Popen, PIPE

def validate(records):
  with Popen([
    'npx', '@dcic/signature-commons-schema',
    '/dcic/signature-commons-schema/v6/core/meta.json',
  ], stdin=PIPE, stdout=PIPE) as proc:
    for record in records:
      proc.stdin.writelines((json.dumps(record),))
      yield json.loads(proc.stdout.readline())
    proc.terminate()
