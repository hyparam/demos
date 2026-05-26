# hypvector demo

Example project showing how to use [hypvector](https://github.com/hyparam/hypvector) for in-browser vector search against a parquet file in cloud storage.

The demo embeds search queries in the browser with
[transformers.js](https://huggingface.co/docs/transformers.js) (`Xenova/all-MiniLM-L6-v2`,
384 dimensions) and runs `searchVectors` directly against a 249 MB parquet on S3 — no backend,
no vector database. Each query reads ~6 MB across ~160 ranged HTTP fetches.

## Build

```bash
npm i
npm run build
```

The build artifacts will be stored in the `dist/` directory and can be served using any static server, eg. `http-server`:

```bash
npm i -g http-server
http-server dist/
```

## Notes

- `hypvector` is currently consumed via a local `file:` dependency (`../../hypvector`).
  Once it is published to npm this can be switched to a version range.
- The vector parquet on S3 (`s3.hyperparam.app/hypvector/wiki_en.vectors.parquet`) was
  built by running `npm run data:embed` in the `hypvector` repo against the 156k
  English Wikipedia sample and uploaded with `aws s3 cp ... --profile hyperparam-platypii`.
