# Transformations

Each file here contains a file format that it is responsible for handling transforming into another form which is closer to getting it ingested into signature commons. This makes the process of ingestion likely less error prone as any intermediate states can be inspected. Ideally, as much as possible, the scripts should operate on streaming data (as to eliminate huge memory requirements for large datasets).
