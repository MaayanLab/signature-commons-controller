import click
import logging
from functools import wraps
from sigcom import router
from sigcom.util import ParsedUrl

def handle_verbosity(func):
  @click.option('-v', '--verbose', count=True, help='Increase logging level')
  @wraps(func)
  def wrapper(verbose=0, **kwargs):
    logging.basicConfig(level=max(0, 40 - verbose*10))
    return func(**kwargs)
  return wrapper

def handle_dry_run(func):
  @click.option('--dry-run', is_flag=True, help='Show which modules will be executed without executing them')
  @wraps(func)
  def wrapper(**kwargs):
    return func(**kwargs)
  return wrapper

def handle_uris(func):
  @click.option('--uri', type=str, multiple=True, help='Specify the URI(s) of the database(s) to access')
  @wraps(func)
  def wrapper(uri=[], **kwargs):
    return func(uri=[ParsedUrl(u) for u in uri], **kwargs)
  return wrapper

def handle_paths(func):
  @click.option('--paths', type=click.Path(), multiple=True, default=['.'], help='Path(s) containing files or directories to be processed', show_default=True)
  @wraps(func)
  def wrapper(paths=[], **kwargs):
    assert len(paths) > 0, 'Expected at least one --paths'
    return func(paths=paths, **kwargs)
  return wrapper

def handle_path(func):
  @click.option('--path', type=click.Path(), default='.', help='Path containing directory to be processed', show_default=True)
  @wraps(func)
  def wrapper(**kwargs):
    return func(**kwargs)
  return wrapper

def handle_actions(func):
  @click.option('--actions', type=str, multiple=True, help='Restrict actions to apply')
  @wraps(func)
  def wrapper(actions=[], **kwargs):
    for action in actions:
      logging.info('Processing action:{action}'.format(action=action))
    return func(actions=actions, **kwargs)
  return wrapper

@click.group()
@handle_verbosity
@click.version_option()
def cli():
  pass

@cli.command(help='Export data to directory from database')
@handle_verbosity
@handle_uris
@handle_path
@handle_dry_run
def extract(dry_run=False, **kwargs):
  if dry_run:
    for item in router.relevant_ingests(**kwargs):
      click.echo(item)
  else:
    router.extract(**kwargs)

@cli.command(help='Ingest data from directory to database')
@handle_verbosity
@handle_uris
@handle_path
@handle_dry_run
def ingest(dry_run=False, **kwargs):
  if dry_run:
    for item in router.relevant_ingests(**kwargs):
      click.echo(item)
  else:
    router.ingest(**kwargs)

@cli.command(help='Apply transformations to data in directory')
@handle_verbosity
@handle_paths
@handle_dry_run
def transform(dry_run=False, **kwargs):
  if dry_run:
    for item in router.relevant_transformers(**kwargs):
      click.echo(item)
  else:
    router.transform(**kwargs)

@cli.command(help='Trigger API action such as data reloading')
@handle_verbosity
@handle_uris
@handle_actions
@handle_dry_run
def action(dry_run=False, **kwargs):
  if dry_run:
    for item in router.get_actions(**kwargs):
      click.echo(item)
  else:
    router.action(**kwargs)
