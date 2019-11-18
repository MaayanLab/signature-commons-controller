import h5py
import json
import uuid
import os

inputs = (
  '*.gctx',
)

outputs = (
  '*.data.uuid.tsv',
  '*.signatures.jsonld',
  '*.entities.jsonld',
)

def transform(input_files, output_files):
  input_file, = input_files
  signature_data, signature_meta, entity_meta = output_files

  fr = h5py.File(input_file)

  rids = {}
  try:
    with open(signature_meta, 'w') as fw:
      for ind, row in enumerate(fr.get('META').get('ROW')):
        rid = str(uuid.uuid4())
        rids[ind] = rid
        print(json.dumps({ '@id': rid, '@type': 'Signature', 'meta': dict(row) }), file=fw)
  except Exception as e:
    os.remove(signature_meta)
    raise e

  cids = {}
  try:
    with open(entity_meta, 'w') as fw:
      for ind, row in enumerate(fr.get('META').get('ROW')):
        cid = str(uuid.uuid4())
        cids[ind] = cid
        print(json.dumps({ '@id': cid, '@type': 'Entity', 'meta': dict(row) }), file=fw)
  except Exception as e:
    os.remove(entity_meta)
    raise e

  try:
    with open(signature_data, 'w') as fw:
      print(*[cids[cid] for cid in range(len(cids))], sep='\t', file=fw)
      for rid, expr in enumerate(fr.get('DATA')):
        print(rids[rid], *expr, sep='\t', file=fw)
  except Exception as e:
    os.remove(signature_data)
    raise e
