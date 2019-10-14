import os, os.path
import importlib
import logging

def try_import_ingests(d):
  try:
    mod = importlib.import_module('.{}'.format(d), __package__)
    assert type(mod.ingests) == list
    for ingest in ingests:
      ingest.to = d
      yield ingest
  except Exception as e:
    logging.warn('Error loading {d}: {e}'.format(**locals()))
    pass

ingests = [
  ingest
  for d in os.listdir(os.path.dirname(__file__))
  if os.path.isdir(d) and not d.startswith('_')
  for ingest in try_import_ingests(d)
]
