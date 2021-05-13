#!/usr/bin/env python3

import json
import numpy as np
import os
import csv
import pandas as pd
import uuid

INPUT='input'
OUTPUT='output'

U = uuid.UUID('00000000-0000-0000-0000-000000000000')
def canonical_uuid(obj):
  return str(uuid.uuid5(U, str(obj)))

def filter_none(d, **kwargs):
  return {
    k: v
    for k, v in dict(d, **kwargs).items()
    if v and (type(v) != float or not np.isnan(v))
  }

# Obtain all entities from files
genes = set()
for file in os.listdir(INPUT):
  if file.endswith('_full.txt'):
    reader = csv.reader(open(os.path.join(INPUT, file), 'r'), delimiter='\t')
    # Read header and find geneid col
    header = next(iter(reader))
    geneid_ind = header.index('geneid')
    # Add all genes that occur to genes set
    for row in reader:
      genes.add(row[geneid_ind])

# Load NCBI Genes
df = pd.read_csv('ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz', sep='\t')
# Ensure nulls are treated as such
df = df.applymap(lambda v: float('nan') if type(v) == str and v == '-' else v)
# Break up lists
split_list = lambda v: v.split('|') if type(v) == str else []
df['dbXrefs'] = df['dbXrefs'].apply(split_list)
df['Synonyms'] = df['Synonyms'].apply(split_list)
df['LocusTag'] = df['LocusTag'].apply(split_list)
df['Other_designations'] = df['Other_designations'].apply(split_list)
df['Name'] = df['Symbol']

# Map existing entities to NCBI Genes
ncbi_lookup = {
  sym: row['GeneID']
  for _, row in df.iterrows()
  for sym in [row['Name']] + row['Synonyms']
}

# Resolve geneID
resolved = filter_none({
  gene: ncbi_lookup.get(gene.split(',')[0])
  for gene in genes
})

# Report coverage
print('Resolved {} / {} symbols'.format(
  len(resolved),
  len(genes)
))

# Setup symbols for adding as synonyms
extra_synonyms = {}
for gene, geneid in resolved.items():
  extra_synonyms[geneid] = extra_synonyms.get(geneid, set()) | set([gene])

# Write to entities.jsonld
with open(os.path.join(OUTPUT, 'example.entities.jsonl'), 'w') as fw:
  for _, row in df.iterrows():
    row_json = row.to_dict()
    print(
      json.dumps({
        '@id': canonical_uuid(row_json),
        'meta': filter_none({
            k: v
            for k, v in row_json.items()
          },
          Synonyms=row_json.get('Synonyms', []) + list(
            extra_synonyms.get(row_json['GeneID'], [])
          ),
        )
      }),
      file=fw,
    )
