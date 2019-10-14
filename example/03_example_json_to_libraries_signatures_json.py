#!/usr/bin/env python

import os
import json
import uuid

INPUT='input'
OUTPUT='output'

U = uuid.UUID('00000000-0000-0000-0000-000000000000')
def canonical_uuid(obj):
  return str(uuid.uuid5(U, str(obj)))
def with_canonical_uuid(obj):
  return dict(obj, **{
    '@id': canonical_uuid(obj)
  })

common_attrs = None

# Step 1. Pass 1--identify common attributes in all signatures
for file in os.listdir(INPUT):
  if file.endswith('.json'):
    sig_meta = json.load(open(os.path.join(INPUT, file), 'r'))
    if common_attrs is None:
      common_attrs = set([
        (k, v)
        for k, v in sig_meta.items()
      ])
    else:
      common_attrs = common_attrs & set([
        (k, v)
        for k, v in sig_meta.items()
      ])

# Step 2. Create library based on common attributes
full_library = with_canonical_uuid({
  'dataset': 'example.data.tsv.so',
  'dataset_type': 'rank_matrix',
  'meta': {
    k: v
    for k, v in common_attrs
  }
})
geneset_library = with_canonical_uuid({
  'dataset': 'example.data.gmt.so',
  'dataset_type': 'geneset_library',
  'meta': {
    k: v
    for k, v in common_attrs
  }
})
with open(os.path.join(OUTPUT, 'example.libraries.jsonld'), 'w') as fw:
  print(json.dumps(full_library), file=fw)
  print(json.dumps(geneset_library), file=fw)


# Step 1. Pass 2--create proper signature with library association
with open(os.path.join(OUTPUT, 'example.signatures.jsonld'), 'w') as fw:
  for file in os.listdir(INPUT):
    if file.endswith('.json'):
      base_sig_id = file[:-len('.json')]
      sig_meta = json.load(open(os.path.join(INPUT, file), 'r'))
      # full signature
      sig_meta_reduced = dict(
        **{
          'id': base_sig_id,
        },
        **{
          k: v
          for k, v in sig_meta.items()
          if (k, v) not in common_attrs
        }
      )
      sig_meta_complete = with_canonical_uuid({
        'library': full_library['@id'],
        'meta': sig_meta_reduced,
      })
      print(json.dumps(sig_meta_complete), file=fw)
      # up geneset
      sig_meta_reduced = dict(
        **{
          'id': base_sig_id + '_up',
        },
        **{
          k: v
          for k, v in sig_meta.items()
          if (k, v) not in common_attrs
        }
      )
      sig_meta_complete = with_canonical_uuid({
        'library': geneset_library['@id'],
        'meta': sig_meta_reduced,
      })
      print(json.dumps(sig_meta_complete), file=fw)
      # down geneset
      sig_meta_reduced = dict(
        **{
          'id': base_sig_id + '_down',
        },
        **{
          k: v
          for k, v in sig_meta.items()
          if (k, v) not in common_attrs
        }
      )
      sig_meta_complete = with_canonical_uuid({
        'library': geneset_library['@id'],
        'meta': sig_meta_reduced,
      })
      print(json.dumps(sig_meta_complete), file=fw)
