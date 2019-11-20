import os, os.path
import importlib

def importdir(_file_, _package_, _globals_):
  for f in os.listdir(os.path.dirname(_file_)):
    if f.startswith('_'):
      continue
    if f.endswith('.py'):
      modname = f[:-len('.py')]
    elif os.path.isdir(os.path.join(os.path.dirname(_file_), f)):
      modname = f
    else:
      continue
    mod = importlib.import_module('.{}'.format(modname), _package_)
    _globals_.update(**{modname: mod})

def importdir_deep(_file_, _package_, _globals_):
  for f in os.listdir(os.path.dirname(_file_)):
    if f.startswith('_'):
      continue
    if f.endswith('.py'):
      modname = f[:-len('.py')]
    elif os.path.isdir(os.path.join(os.path.dirname(_file_), f)):
      modname = f
    else:
      continue
    mod = importlib.import_module('.{}'.format(modname), _package_)
    _globals_.update(**{
      k: v
      for k, v in mod.__dict__.items()
      if not k.startswith('_')
    })
