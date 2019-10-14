import os, os.path
import importlib
import logging

def try_import_ingest(f):
  try:
    modname = f[:-len('.py')]
    print('loading ingest.sigcom.{modname}...'.format(**locals()))
    mod = importlib.import_module('.{modname}'.format(**locals()), __package__)
    assert type(mod.inputs) == tuple
    assert type(mod.after) == tuple
    assert callable(mod.ingest)
    yield mod
  except Exception as e:
    logging.warn('Error loading {f}: {e}'.format(**locals()))
    pass

ingests = [
  ingest
  for file in os.listdir(os.path.dirname(__file__))
  if file.endswith('.py') and not file.startswith('_')
  for ingest in try_import_ingest(file)
]
