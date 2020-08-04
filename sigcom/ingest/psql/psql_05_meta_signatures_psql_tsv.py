import psycopg2
import os.path
from sigcom.util import first

inputs = (
  '*.signatures.psql.tsv',
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
    create table signatures_tmp
    as table signatures
    with no data;
  ''')
  with open(input_file, 'r') as fr:
    cur.copy_from(fr, 'signatures_tmp',
      columns=('uuid', 'libid', 'meta'),
      null='',
      sep='\t',
    )
  cur.execute('''
    insert into signatures (uuid, libid, meta)
      select uuid, libid, meta
      from signatures_tmp
      on conflict (uuid)
        do update
        set libid = excluded.libid,
            meta = excluded.meta
    ;
  ''')
  cur.execute('drop table signatures_tmp;')
  con.commit()
