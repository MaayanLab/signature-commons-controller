#!/usr/bin/env python3

import os
import csv
import json

INPUT='input'
OUTPUT='output'

genes = set()

# First pass--identify all dimensions
for file in os.listdir(INPUT):
  if file.endswith('_full.txt'):
    reader = csv.reader(open(os.path.join(INPUT, file), 'r'), delimiter='\t')
    # Read header and find geneid col
    header = next(iter(reader))
    geneid_ind = header.index('geneid')
    # Add all genes that occur to genes set
    for row in reader:
      genes.add(row[geneid_ind])

# Second pass--logfc + adjpval to expr
with open(os.path.join(OUTPUT, 'example.data.T.tsv'), 'w') as fw:
  print('', *genes, sep='\t', file=fw)
  for file in os.listdir(INPUT):
    if file.endswith('_full.txt'):
      sig_id = file[:-len('_full.txt')]
      reader = csv.reader(open(os.path.join(INPUT, file), 'r'), delimiter='\t')
      # Read header and find relevant col indecies
      header = next(iter(reader))
      geneid_ind = header.index('geneid')
      logfc_ind = header.index('logfc')
      adjpval_ind = header.index('adjpval')
      # Construct gene_expression dict for expression lookup for a given gene
      gene_expr = {
        row[geneid_ind]: 1 + (1 - float(row[adjpval_ind])) if float(row[logfc_ind]) > 0 else float(row[adjpval_ind])
        for row in reader
      }
      # Write gene expression for each gene
      print(sig_id,
        *[
          gene_expr.get(gene, '')
          for gene in genes
        ],
        sep='\t',
        file=fw,
      )
