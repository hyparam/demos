# icebird-grep demo

Full-text search over an Apache Iceberg table, accelerated by a sibling
[parquetindex](https://github.com/hyparam/parquetindex) iceberg table.

Mashes up two patterns:

- [icebird](https://github.com/hyparam/icebird) for reading the main Iceberg
  table (`llm_logs`).
- [parquetindex](https://github.com/hyparam/parquetindex) for full-text search,
  with the index stored as a sibling Iceberg table (`llm_logs.index`) whose
  single data file is itself a parquetindex parquet.

## Build

```bash
npm i
npm run build
```

## Create the Iceberg tables on S3

```bash
npm run create-table
```

This uses the `iceberg` AWS profile to write to
`s3://hyperparam-iceberg/icebird-grep/llm_logs` (data) and
`s3://hyperparam-iceberg/icebird-grep/llm_logs.index` (the parquetindex
sibling).
