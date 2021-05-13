def get_one_of(obj, keys):
  for k in keys:
    if k in obj:
      return obj[k]
  raise Exception('Missing any of {} in object'.format(', '.join(keys)))
