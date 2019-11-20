import argparse
import logging
from . import router
from .util import ParsedUrl

def _add_uris(parser):
  uriGroup = parser.add_argument_group('uri')
  uriGroup.add_argument(
    '--uri', metavar='URI', dest='uri', type=str,
    nargs='+', action='append', required=True,
    help='Specify the URI(s) of the database(s) to access',
  )
#
def _add_paths(parser):
  parser.add_argument(
    '--paths', metavar='PATH', type=str,
    nargs=argparse.REMAINDER, action='append', required=True,
    help='Path(s) containing files or directories to be processed (default to current directory)',
  )
#
def _add_path(parser):
  parser.add_argument(
    '--path', metavar='PATH', type=str,
    nargs='?', default='.',
    help='Path containing directory to be processed (default to current directory)',
  )
#
def main():
  import sys
  import itertools
  #
  parser = argparse.ArgumentParser(
    prog='Signature Commons Controller',
    description='Simplify data ingestion, migration, and transformations for signature commons',
  )
  parser.add_argument(
    '--verbose', '-v', action='count', default=0,
    help='Increase logging level'
  )
  parser.add_argument(
    '--version', '-V', action='version', version='%(prog)s 1.0',
    help='Display application version',
  )
  subparsers = parser.add_subparsers(title='Action', dest='action')
  subparsers.required = True
  #
  extract_parser = subparsers.add_parser('extract', help='Export data to directory from database')
  _add_uris(extract_parser)
  _add_path(extract_parser)
  ingest_parser = subparsers.add_parser('ingest', help='Ingest data from directory to database')
  _add_uris(ingest_parser)
  _add_paths(ingest_parser)
  transform_parser = subparsers.add_parser('transform', help='Apply transformations to data in directory')
  _add_uris(transform_parser)
  _add_paths(transform_parser)
  #
  args = parser.parse_args(sys.argv[1:])
  args.uri = [ParsedUrl(u) for us in args.uri for u in us]
  logging.basicConfig(level=max(0, 40 - args.verbose*10))
  logging.info('Processing action:{action}'.format(action=args.action))
  return getattr(router, args.action)(**args.__dict__)
