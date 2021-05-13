import json

def try_json_loads(v):
  try:
    return json.loads(v)
  except:
    return v
