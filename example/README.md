# Example

```bash
export DIR="$(pwd)"

# Fastest way to extract everything
python3 -m controller extract \
  --uri psql://signaturestore:signaturestore@localhost:5432/signaturestore \
  --uri s3+http://signaturestore:signaturestore@localhost:9000/signaturestore \
  --path "${DIR}"

# Transform files (gct => ld, etc...)
python3 -m controller transform --paths "${DIR}"

# Fastest way to ingest everything
python3 -m controller ingest \
  --uri psql://signaturestore:signaturestore@localhost:5432/signaturestore \
  --uri s3+http://signaturestore:signaturestore@localhost:9000/signaturestore \
  --paths "${DIR}"

# Prepare APIs after ingest
python3 -m controller action \
  --uri meta+http://signaturestore:signaturestore@localhost/signature-commons-metadata-api \
  --uri data+http://signaturestore@localhost/enrichmentapi \
  --actions refresh_data \
  --actions refresh_meta \
  --actions refresh_summary

# refresh_data: load s3 => data api
# refresh_meta: ensure indexes (FTS, etc.) are up to date
# refresh_summary: compute summary statistics
```
