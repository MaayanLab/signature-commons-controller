import logging
from copy import deepcopy

def get_actions(**kwargs):
  import sigcom.action as actions
  for _, mod in sorted(actions.__dict__.items()):
    if str(type(mod)) != "<class 'module'>":
      continue
    try:
      assert all([
        callable(mod.requirements),
        callable(mod.apply),
      ])
      logging.info('Found action: {}'.format(mod))
      if mod.requirements(**kwargs):
        yield mod
      else:
        logging.info('Unsatisfied requirements')
    except Exception as e:
      logging.debug('Rejected: {}, {}'.format(mod, e))

def action(action=None, **kwargs):
  for action in get_actions(**kwargs):
    logging.debug('Applying action {}'.format(action))
    action.apply(**deepcopy(kwargs))


def get_extracts(**kwargs):
  import sigcom.extract as extracts
  for _, mod in sorted(extracts.__dict__.items()):
    if str(type(mod)) != "<class 'module'>":
      continue
    try:
      logging.debug('Checking: {}'.format(mod))
      assert all([
        callable(mod.requirements),
        type(mod.outputs) == tuple,
        callable(mod.extract),
      ])
      logging.info('Found extract: {}'.format(mod))
      if mod.requirements(**kwargs):
        yield mod
      else:
        logging.info('Unsatisfied requirements')
    except Exception as e:
      logging.debug('Rejected: {}, {}'.format(mod, e))

def relevant_extracts(path=None, **kwargs):
  ''' Relevant extracts are those whos outputs are not yet satisfied.
  '''
  import glob
  import os.path
  #
  if os.path.isdir(path):
    files = [
      os.path.join(path, f)
      for f in os.listdir(path)
    ]
  else:
    files = glob.glob(path)
  #
  for e in get_extracts(path=path, **kwargs):
    P = {}
    for f in files:
      for out in e.outputs:
        if glob.fnmatch.fnmatch(f, out):
          P[out] = P.get(out, set()) | set([f])
    if set(e.outputs) > set(P.keys()):
      logging.debug('Found relevant extract: {}'.format(e))
      yield e

def extract(**kwargs):
  for extract in relevant_extracts(**kwargs):
    logging.debug('Extracting with {}'.format(extract))
    extract.extract(**deepcopy(kwargs))


def get_ingests(**kwargs):
  import sigcom.ingest as ingests
  for _, mod in sorted(ingests.__dict__.items()):
    if str(type(mod)) != "<class 'module'>":
      continue
    try:
      assert all([
        callable(mod.requirements),
        type(mod.inputs) == tuple,
        callable(mod.ingest),
      ])
      logging.info('Found ingest: {}'.format(mod))
      if mod.requirements(**kwargs):
        yield mod
      else:
        logging.info('Unsatisfied requirements')
    except Exception as e:
      logging.debug('Rejected: {}, {}'.format(mod, e))

def relevant_ingests(paths=[], **kwargs):
  ''' Relevant ingests are those whos inputs are satisfiable
  '''
  # TODO: Holdout with after
  import glob
  import os.path
  import itertools
  #
  files = []
  for p in paths:
    if os.path.isdir(p):
      files += [
        os.path.join(p, f)
        for f in os.listdir(p)
      ]
    else:
      files += glob.glob(p)
  #
  for i in get_ingests(paths=paths, **kwargs):
    basenames = set(f.split('.', maxsplit=1)[0] for f in files)
    print(basenames)
    for basename in basenames:
      P = {}
      for f in files:
        if f.split('.', maxsplit=1)[0] != basename:
          continue
        for inp in i.inputs:
          if glob.fnmatch.fnmatch(f, inp):
            P[inp] = P.get(inp, set()) | set([f])
      print(i.inputs, P)
      if set(i.inputs) == set(P.keys()):
        for fs in itertools.product(*P.values()):
          logging.debug('Found relevant ingest: {}({})'.format(i, fs))
          yield (
            i,
            dict(zip(P.keys(), fs)),
          )

def ingest(**kwargs):
  for ingest, input_files in relevant_ingests(**kwargs):
    print(ingest, input_files)
    ingest.ingest(
      tuple(
        input_files[k]
        for k in ingest.inputs
      ),
      **deepcopy(kwargs)
    )

def get_transformers(**kwargs):
  import sigcom.transform as transformers
  for _, mod in sorted(transformers.__dict__.items()):
    if str(type(mod)) != "<class 'module'>":
      continue
    try:
      assert all([
        type(mod.inputs) == tuple,
        type(mod.outputs) == tuple,
        callable(mod.transform),
      ])
      logging.info('Using transformer: {}'.format(mod))
      yield mod
    except:
      logging.debug('Rejected: {}'.format(mod))

def relevant_transformers(paths=[], **kwargs):
  ''' Relevant transformers are those whos outputs are not satisfied but inputs are.
  '''
  import glob
  import os.path
  import itertools
  #
  files = []
  for p in paths:
    if os.path.isdir(p):
      files += [
        os.path.join(p, f)
        for f in os.listdir(p)
      ]
    else:
      files += glob.glob(p)
  #
  for t in get_transformers(paths=paths, **kwargs):
    basenames = set(f.split('.', maxsplit=1)[0] for f in files)
    for basename in basenames:
      P = {}
      for f in files:
        if f.split('.', maxsplit=1)[0] != basename:
          continue
        for inp in t.inputs:
          if glob.fnmatch.fnmatch(f, inp):
            P[inp] = P.get(inp, set()) | set([f])
      if set(t.inputs) == set(P.keys()):
        for fs in itertools.product(*P.values()):
          outputs = {
            out: out.replace('*', basename)
            for out in t.outputs
          }
          if set(outputs.values()) - set(files) != set():
            logging.debug('Found relevant transformer: {}'.format(t))
            yield (
              t,
              dict(zip(P.keys(), fs)),
              outputs,
            )

def transform(**kwargs):
  for transform, input_files, output_files in relevant_transformers(**kwargs):
    print(transform, input_files, output_files)
    transform.transform(
      tuple(
        input_files[k]
        for k in transform.inputs
      ),
      tuple(
        output_files[k]
        for k in transform.outputs
      ),
      **deepcopy(kwargs),
    )
