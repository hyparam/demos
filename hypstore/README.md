# hypstore demo

One Apache Iceberg table of real ChatGPT conversations, queryable with SQL
and grep — entirely in the browser.

[hypstore](https://github.com/hyparam/hypstore) is a serverless lakehouse for
JavaScript: named Iceberg tables in object storage, queryable with:

- **SQL** via [squirreling](https://github.com/hyparam/squirreling)
- **full-text grep** via [hypgrep](https://github.com/hyparam/hypgrep)

The demo points at `s3://hyperparam-public/hypstore/wildchat`, built from the
[allenai/WildChat-4.8M](https://huggingface.co/datasets/allenai/WildChat-4.8M)
dataset with one row per conversation, the raw model input stored as a JSON
string. Every query runs client-side over HTTP range requests.

## Build

```bash
npm i
npm run build
```

## Create the Iceberg table on S3

```bash
node --max-old-space-size=110000 scripts/createTable.js
```

This downloads the WildChat parquet files from Hugging Face (~10 GB), keeps
one row per conversation with the raw model input as a JSON string, and uses
hypstore itself (`createTable` + `append`) to commit all rows in a single
append with a hypgrep text index built alongside the data. The full run needs
a machine with lots of RAM and disk; use `FILE_LIMIT=1 CONV_LIMIT=1000
TABLE=wildchat-test` for a small test run.
