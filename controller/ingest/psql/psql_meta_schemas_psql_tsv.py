import psycopg2
import os.path
from controller.util import first

inputs = (
  '*.schemas.psql.tsv',
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
    create table schemas_tmp
    as table schemas
    with no data;
  ''')
  with open(input_file, 'r') as fr:
    cur.copy_from(fr, 'schemas_tmp',
      columns=('uuid', 'meta'),
      sep='\t',
    )
  cur.execute('''
    insert into schemas (uuid, meta)
      select uuid, meta
      from schemas_tmp
      on conflict (uuid)
        do update
        set meta = excluded.meta
    ;
  ''')
  cur.execute('drop table schemas_tmp;')
  con.commit()
