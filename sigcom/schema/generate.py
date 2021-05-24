import functools
import collections

def type_walk(obj, path=tuple()):
  ''' Recursive walk through object, returning a flat typeset of the form:
  (subject_type, key, object_type)
  Examples:
  (dict, 'a', str)
  (list, '[]', int)
  (dict, 'a', dict, 'b', list, '[]', str)
  '''
  if type(obj) == list:
    for v in obj:
      for leaf in type_walk(v, path=(*path, list, '[]')):
        yield leaf
  elif type(obj) == dict:
    for k, v in obj.items():
      for leaf in type_walk(v, path=(*path, dict, k)):
        yield leaf
  else:
    yield (*path, type(obj))

def filter_trim_prefix(tupleset, prefix):
  ''' Given a tupleset, filter & trim it by a given prefix
  example
  tupleset:
    (a, b, c)
    (a, b)
    (a, c)
    (a,)
  filter_trim_prefix(tupleset, ('a',)) == (b, c), (b,), (c,), tuple()
  filter_trim_prefix(tupleset, ('a',b,)) == (c), tuple()
  '''
  for info in tupleset:
    if len(info) < len(prefix): continue
    if any(a != b for a, b in zip(info, prefix)): continue
    yield info[len(prefix):]

def create_partial_json_schema(union_typeset, intersection_typeset):
  ''' Using a union & intersection typeset,
  produce a partial json schema specification
  '''
  schemas = {}
  for (object_type, *rest) in union_typeset:
    if object_type == dict:
      key, value_type, *_ = rest
      if object_type not in schemas:
        schemas[object_type] = {}
      if key not in schemas[object_type]:
        schemas[object_type][key] = {}
      if value_type not in schemas[object_type][key]:
        schemas[object_type][key][value_type] = create_partial_json_schema(
          frozenset(filter_trim_prefix(union_typeset, (object_type, key, value_type))),
          frozenset(filter_trim_prefix(intersection_typeset, (object_type, key, value_type))),
        )
    elif object_type == list:
      key, value_type, *_ = rest
      if object_type not in schemas:
        schemas[object_type] = {}
      if key not in schemas[object_type]:
        schemas[object_type][key] = {}
      if value_type not in schemas[object_type][key]:
        schemas[object_type][key][value_type] = create_partial_json_schema(
          frozenset(filter_trim_prefix(union_typeset, (object_type, key, value_type))),
          frozenset(filter_trim_prefix(intersection_typeset, (object_type, key, value_type))),
        )
    elif object_type in {int, float, str}:
      if object_type not in schemas:
        schemas[object_type] = {}
    else:
      raise NotImplementedError
  #
  schema = {}
  for object_type, obj in schemas.items():
    _schema = {}
    if object_type == dict:
      _schema['type'] = 'object'
      _schema['properties'] = {}
      for key, value_types in obj.items():
        if len(value_types) > 1:
          _schema['properties'][key] = {"oneOf": list(value_types.values())}
        else:
          _schema['properties'][key] = next(iter(value_types.values()))
        #
    elif object_type == list:
      _schema['type'] = 'array'
      _schema['items'] = {}
      if len(obj['[]']) > 1:
        _schema['items'] = {"oneOf": list(obj['[]'].values())}
      else:
        _schema['items'] = next(iter(obj['[]'].values()))
    elif object_type in {int, float}:
      _schema['type'] = 'number'
    elif object_type == str:
      _schema['type'] = 'string'
    #
    if len(schemas) > 1:
      if 'oneOf' not in schemas: schemas['oneOf'] = []
      schema['oneOf'].append(_schema)
    else:
      schema.update(_schema)
  return schema

def create_complete_json_schema(id, data):
  '''
  1. Walk through the data collecting typesets
  2. Find the "union_typeset" or the set of deep types which satisfies at least one record
  3. Find the "intersection_typeset" or the set of deep types which is satisfied by all records
  4. Supplement create_partial_json_schema with front-matter to finalize json-schema spec
  '''
  typeset = collections.Counter(map(frozenset, map(type_walk, data)))
  union_typeset = functools.reduce(frozenset.union, typeset.keys())
  intersection_typeset = functools.reduce(frozenset.intersection, typeset.keys())
  schema = create_partial_json_schema(union_typeset, intersection_typeset)
  assert schema['type'] == 'object', 'An error occured, we should have object records..'
  schema['properties']['$schema'] = {
    'type': 'string',
    'enum': [id],
  }
  return {
    "$id": id,
    "$schema": "http://json-schema.org/draft-04/schema#",
    "allOf": [
      {"$ref": "/dcic/signature-commons-schema/v5/core/meta.json"},
      schema,
    ]
  }
