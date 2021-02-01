
import psycopg2
import os.path
from sigcom.util import first

inputs = (
  '*.signatures.entities.psql.tsv',
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
    create table signatures_entities_tmp
    as table signatures_entities
    with no data;
  ''')
  with open(input_file, 'r') as fr:
    cur.copy_from(fr, 'signatures_entities_tmp',
      columns=('signature', 'entitiy'),
      null='',
      sep='\t',
    )
  cur.execute('''
    insert into signatures_entities (signature, entity)
      select signature, entity
      from signatures_entities_tmp
      on conflict (signature, entity)
        do update
        set signature = excluded.signature,
            entitiy = excluded.entity
    ;
  ''')
  cur.execute('drop table signatures_entities_tmp;')
  con.commit()
