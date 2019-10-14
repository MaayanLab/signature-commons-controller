import os
import sys
import glob
import itertools
from .ingest import sigcom
from . import transform

def potential_transfomers(files):
  for t in transform.transformers:
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
            yield (
              t,
              dict(zip(P.keys(), fs)),
              outputs,
            )

def do_transform(files):
  for t, inp_kwargs, out_kwargs in potential_transfomers(files):
    print('{} ({}, {})'.format(t, inp_kwargs, out_kwargs))
    t.transform(
      tuple(inp_kwargs.get(inp) for inp in t.inputs),
      tuple(out_kwargs.get(out) for out in t.outputs),
    )


def potential_ingests(files):
  for i in sigcom.ingests:
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
          yield (
            i,
            dict(zip(P.keys(), fs)),
          )

def do_ingest(files):
  Q = list(potential_ingests(files))
  D = set()
  while Q != []:
    i, inp_kwargs = Q.pop(0)
    if set(i.after) - D: # Send back to queue if we haven't completed all "afters"
      Q.append((i, inp_kwargs))
    print('{} ({})'.format(i, inp_kwargs))
    i.ingest(
      tuple(inp_kwargs.get(inp) for inp in i.inputs),
    )

if sys.argv[1] == '-t':
  do_transform(sys.argv[2:])
elif sys.argv[1] == '-i':
  do_ingest(sys.argv[2:])
else:
  print('Usage: {} <-t | -i> <files ...>'.format(sys.argv[0]))
