import psycopg2
import os.path
from sigcom.util import first

inputs = (
  '*.entities.psql.tsv',
)

def requirements(uri=[], **kwargs):
  return 'psql' in set([s for u in uri for s in u.scheme.split('+')])

def ingest(input_files, uri=[], limit=1000, **kwargs):
  input_file, = input_files
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
  cur.execute('''
    create table entities_tmp
    as table entities
    with no data;
  ''')
  with open(input_file, 'r') as fr:
    cur.copy_from(fr, 'entities_tmp',
      columns=('uuid', 'meta'),
      null='',
      sep='\t',
    )
  cur.execute('''
    insert into entities (uuid, meta)
      select uuid, meta
      from entities_tmp
      on conflict (uuid)
        do update
        set meta = excluded.meta
    ;
  ''')
  cur.execute('drop table entities_tmp;')
  con.commit()
