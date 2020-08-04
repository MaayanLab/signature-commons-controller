import psycopg2
import os.path
from sigcom.util import first

outputs = (
  '*.resources.psql.tsv',
)

def requirements(uri=[], **kwargs):
  return 'psql' in set([s for u in uri for s in u.scheme.split('+')])

def extract(path=None, uri=[], **kwargs):
  # Get the psql_uri only
  psql_uri = first(u for u in uri if 'psql' in u.scheme.split('+'))
  # Connect to db
  con = psycopg2.connect(
    database=psql_uri.path[1:],
    user=psql_uri.username,
    password=psql_uri.password,
    host=psql_uri.hostname,
    port=psql_uri.port,
  )
  cur = con.cursor()
  tbl = 'resources'
  with open(os.path.join(path, '{}.{}.psql.tsv'.format(psql_uri.path[1:], tbl)), 'w') as fw:
    cur.copy_to(fw, tbl,
      columns=('uuid', 'meta'),
      sep='\t',
    )
