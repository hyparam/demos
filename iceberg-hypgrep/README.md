# iceberg-hypgrep demo

Full-text search over an Apache Iceberg table, accelerated by a sibling
[hypgrep](https://github.com/hyparam/hypgrep) iceberg table.

Mashes up two patterns:

- [icebird](https://github.com/hyparam/icebird) for reading the main Iceberg
  table (`llm_logs`).
- [hypgrep](https://github.com/hyparam/hypgrep) for full-text search,
  with the index stored as a sibling Iceberg table (`llm_logs.index`) whose
  single data file is itself a hypgrep parquet.

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
`s3://hyperparam-iceberg/iceberg-hypgrep/llm_logs` (data) and
`s3://hyperparam-iceberg/iceberg-hypgrep/llm_logs.index` (the hypgrep
sibling).
