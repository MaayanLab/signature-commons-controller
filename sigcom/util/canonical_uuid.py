import json
import uuid

def canonical_uuid(obj):
  return str(
    uuid.uuid5(
      uuid.UUID(int=0),
      json.dumps(obj, sort_keys=True)
    )
  )

def with_canonical_uuid(obj):
  return dict(obj, **{ '@id': canonical_uuid(obj) })
