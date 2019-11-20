import logging

def get_actions(**kwargs):
  from . import action as actions
  for mod in actions.__dict__.values():
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


def get_extracts(**kwargs):
  from . import extract as extracts
  for mod in extracts.__dict__.values():
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
  import itertools
  #
  files = [
    f
    for f in (os.listdir(path) if os.path.isdir(path) else glob.glob(path))
  ]
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
    extract.extract(**kwargs)


def get_ingests(**kwargs):
  from . import ingest as ingests
  for mod in ingests.__dict__.values():
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
  import glob
  import os.path
  import itertools
  #
  files = [
    f
    for p in paths
    for f in (os.listdir(p) if os.path.isdir(p) else glob.glob(p))
  ]
  #
  for i in get_ingests(paths=paths, **kwargs):
    basenames = set(f.split('.', maxsplit=1)[0] for f in files)
    for basename in basenames:
      P = {}
      for f in files:
        if not f.startswith(basename):
          continue
        for inp in i.inputs:
          if glob.fnmatch.fnmatch(f, inp):
            P[inp] = P.get(inp, set()) | set([f])
      if set(i.inputs) == set(P.keys()):
        for fs in itertools.product(*P.values()):
          logging.debug('Found relevant ingest: {}'.format(i))
          yield (
            i,
            dict(zip(P.keys(), fs)),
          )

def ingest(**kwargs):
  # TODO: worry about potential inf. loops
  ingest_queue = list(relevant_ingests(**kwargs))
  ingest_completed = set()
  while ingest_queue != []:
    ingest, input_files = ingest_queue.pop()
    if set(ingest.after) - ingest_completed:
      ingest_queue.append((ingest, input_files))
    else:
      ingest.ingest(input_files, **kwargs)


def get_transformers(**kwargs):
  from . import transform as transformers
  for mod in transformers.__dict__.values():
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
  files = [
    f
    for p in paths
    for f in (os.listdir(p) if os.path.isdir(p) else glob.glob(p))
  ]
  #
  for t in get_transformers(paths=paths, **kwargs):
    basenames = set(f.split('.', maxsplit=1)[0] for f in files)
    for basename in basenames:
      P = {}
      for f in files:
        if not f.startswith(basename):
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
    transform.transform(input_files, output_files, **kwargs)
