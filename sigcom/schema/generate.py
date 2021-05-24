import functools
import collections

def type_walk(obj, path=tuple()):
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

def create_schema(union_type, intersection_type):
  schemas = {}
  for (object_type, *rest) in union_type:
    if object_type == dict:
      key, value_type, *_ = rest
      if object_type not in schemas:
        schemas[object_type] = {}
      if key not in schemas[object_type]:
        schemas[object_type][key] = {}
      if value_type not in schemas[object_type][key]:
        schemas[object_type][key][value_type] = create_schema(
          frozenset(
            (_value_type, *__rest)
            for (_object_type, *_rest) in union_type
            if object_type == _object_type
            for (_key, _value_type, *__rest) in (_rest,)
            if key == _key and value_type == _value_type
          ),
          frozenset(
            (_value_type, *__rest)
            for (_object_type, *_rest) in intersection_type
            if object_type == _object_type
            for (_key, _value_type, *__rest) in (_rest,)
            if key == _key and value_type == _value_type
          ),
        )
    elif object_type == list:
      key, value_type, *_ = rest
      if object_type not in schemas:
        schemas[object_type] = {}
      if key not in schemas[object_type]:
        schemas[object_type][key] = {}
      if value_type not in schemas[object_type][key]:
        schemas[object_type][key][value_type] = create_schema(
          frozenset(
            (_value_type, *_rest)
            for (_object_type, _key, _value_type, *_rest) in union_type
            if object_type == _object_type and key == _key and value_type == _value_type
          ),
          frozenset(
            (_value_type, *_rest)
            for (_object_type, _key, _value_type, *_rest) in intersection_type
            if object_type == _object_type and key == _key and value_type == _value_type
          ),
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

def generate(name, data):
  type_counts = collections.Counter(map(frozenset, map(type_walk, data)))
  union_type = functools.reduce(frozenset.union, type_counts.keys())
  intersection_type = functools.reduce(frozenset.intersection, type_counts.keys())
  return {
    "$id": "/{name}.json".format(name=name),
    "$schema": "http://json-schema.org/draft-04/schema#",
    "allOf": [
      {"$ref": "/dcic/signature-commons-schema/v5/core/meta.json"},
      create_schema(union_type, intersection_type),
    ]
  }
