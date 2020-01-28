import psycopg2
import os.path
from controller.util import first

inputs = (
  '*.libraries.psql.tsv',
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
    create table libraries_tmp
    as table libraries
    with no data;
  ''')
  with open(input_file, 'r') as fr:
    cur.copy_from(fr, 'libraries_tmp',
      columns=('uuid', 'resource', 'dataset', 'dataset_type', 'meta'),
      sep='\t',
    )
  cur.execute('''
    insert into libraries (uuid, resource, dataset, dataset_type, meta)
      select uuid, resource, dataset, dataset_type, meta
      from libraries_tmp
      on conflict (uuid)
        do update
        set 
          resource = excluded.resource,
          dataset = excluded.dataset,
          dataset_type = excluded.dataset_type,
          meta = excluded.meta
    ;
  ''')
  cur.execute('drop table libraries_tmp;')
  con.commit()
