import os, os.path
import importlib
import logging

def try_import_transformer(f):
  try:
    modname = f[:-len('.py')]
    print('loading `transformer.{modname}`...'.format(**locals()))
    mod = importlib.import_module('.{modname}'.format(**locals()), __package__)
    assert type(mod.inputs) == tuple
    assert type(mod.outputs) == tuple
    assert callable(mod.transform)
    yield mod
  except Exception as e:
    logging.warn('Error loading {f}: {e}'.format(**locals()))
    pass

transformers = [
  transformer
  for file in os.listdir(os.path.dirname(__file__))
  if file.endswith('.py') and not file.startswith('_')
  for transformer in try_import_transformer(file)
]
