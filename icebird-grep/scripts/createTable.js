#!/usr/bin/env node
// Build the icebird-grep demo tables on S3:
//
//   s3://hyperparam-iceberg/icebird-grep/llm_logs        - iceberg table with LLM logs
//   s3://hyperparam-iceberg/icebird-grep/llm_logs.index  - iceberg table whose data file
//                                                          is a parquetindex over llm_logs
//
// The index table's iceberg schema matches the parquetindex columnar layout
// (term, blockId, docCount, termFreq). parquetindex's required kv metadata
// (block_size, text_columns, source_rows, source_bytelength, version) lives in
// the iceberg table's `properties`, so the data file ends up as a normal
// iceberg parquet file. The demo client merges those properties back into the
// parquet's key_value_metadata before calling parquetFind.
//
// Run with `npm run create-table`. Uses the `iceberg` AWS profile to write.

import { fromIni } from '@aws-sdk/credential-providers'
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { ByteWriter, parquetWriteBuffer } from 'hyparquet-writer'
import {
  fileCatalog,
  icebergAppend,
  icebergCreateTable,
  icebergDropTable,
  icebergManifests,
  icebergMetadata,
  s3Lister,
  s3SignedResolver,
} from 'icebird'
import { splitManifestEntries } from 'icebird/src/manifest.js'
import { createIndex } from 'parquetindex'

const BUCKET = 'hyperparam-iceberg'
const PREFIX = 'icebird-grep'
const REGION = 'us-east-1'

const mainTableUrl = `s3://${BUCKET}/${PREFIX}/llm_logs`
const indexTableUrl = `s3://${BUCKET}/${PREFIX}/llm_logs.index`

const mainSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'timestamp', required: true, type: 'string' },
    { id: 3, name: 'model', required: true, type: 'string' },
    { id: 4, name: 'prompt', required: true, type: 'string' },
    { id: 5, name: 'response', required: true, type: 'string' },
    { id: 6, name: 'tokens', required: true, type: 'int' },
  ],
}

const indexSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'term', required: true, type: 'string' },
    { id: 2, name: 'blockId', required: true, type: 'int' },
    { id: 3, name: 'docCount', required: true, type: 'int' },
    { id: 4, name: 'termFreq', required: true, type: 'int' },
  ],
}

const MODELS = ['claude-opus-4-7', 'claude-sonnet-4-6', 'claude-haiku-4-5', 'gpt-5', 'gpt-5-mini', 'gemini-3-pro']

const PROMPT_TEMPLATES = [
  ['How does {topic} affect modern {field}?', 'Explore the implications of {topic} on contemporary {field} practice.'],
  ['Summarize {topic} in two sentences.', 'Provide a brief overview of {topic} suitable for a beginner.'],
  ['Write a haiku about {topic}.', 'Compose a 5-7-5 syllable poem on {topic}.'],
  ['What are the trade-offs of using {topic} for {field}?', 'Compare pros and cons of {topic} in {field}.'],
  ['Explain {topic} like I am five years old.', 'A simple, friendly explanation of {topic} for a young child.'],
  ['Generate test data for a {topic} system.', 'Produce realistic sample records for a {topic} application.'],
  ['Debug this {field} error involving {topic}.', 'Help me figure out why my {field} code keeps crashing when {topic} is enabled.'],
  ['Refactor my {topic} module to be more {adjective}.', 'Rewrite the {topic} module emphasizing {adjective} design.'],
]

const TOPICS = [
  'iceberg tables', 'parquet files', 'full-text search', 'columnar storage', 'vector embeddings',
  'serverless functions', 'edge caching', 'distributed consensus', 'Merkle trees', 'bloom filters',
  'gradient descent', 'transformer attention', 'reinforcement learning', 'prompt injection', 'tool use',
  'graph databases', 'event sourcing', 'CRDT replication', 'feature flags', 'observability',
  'cold starts', 'JIT compilation', 'WebAssembly', 'service workers', 'TLS handshakes',
  'CSS Grid layouts', 'React hooks', 'Rust ownership', 'Go channels', 'Python asyncio',
  'PostgreSQL indexes', 'DuckDB queries', 'Iceberg manifests', 'S3 list-objects-v2',
  'data lineage', 'schema evolution', 'time-travel queries', 'snapshot isolation',
]

const FIELDS = ['data engineering', 'machine learning', 'web development', 'systems programming', 'research', 'product design']
const ADJECTIVES = ['modular', 'performant', 'readable', 'composable', 'testable', 'idiomatic']

const RESPONSE_PHRASES = [
  'Looking at this more carefully, the key insight is that {topic} sits at the intersection of {field} and pragmatism.',
  'The short answer: yes, {topic} works well for {field}, but watch out for the cold-start latency.',
  'I would start by sketching a {adjective} prototype, then layer in {topic} once the data model is stable.',
  'One subtle gotcha with {topic}: the snapshot isolation only kicks in if you commit through the catalog, not the resolver.',
  'For a {field} workload, parquetindex over an iceberg table gives you grep-like search without standing up Elastic.',
  'Try this: bisect the failing snapshot, then re-run with {topic} disabled to confirm it is the regression.',
  'A {adjective} approach is to push down the predicate into the parquet read so you never decode rows you do not need.',
  'Honestly, the trick is to treat {topic} as a derived view of the main table — never the source of truth.',
]

function pick(arr, i) {
  return arr[i % arr.length]
}

function fill(template, ctx) {
  return template.replace(/\{(\w+)\}/g, (_, k) => ctx[k] ?? `{${k}}`)
}

function generateRecords(n) {
  const baseTs = Date.UTC(2026, 0, 1)
  const records = []
  for (let i = 0; i < n; i++) {
    const topic = pick(TOPICS, i * 7 + 3)
    const field = pick(FIELDS, i * 11)
    const adjective = pick(ADJECTIVES, i * 13)
    const ctx = { topic, field, adjective }
    const [promptShort, promptLong] = pick(PROMPT_TEMPLATES, i * 5)
    const prompt = `${fill(promptShort, ctx)} ${fill(promptLong, ctx)}`
    const responseBits = []
    for (let r = 0; r < 3; r++) {
      responseBits.push(fill(pick(RESPONSE_PHRASES, i * 17 + r), ctx))
    }
    records.push({
      id: BigInt(i + 1),
      timestamp: new Date(baseTs + i * 60_000).toISOString(),
      model: pick(MODELS, i * 3 + 1),
      prompt,
      response: responseBits.join(' '),
      tokens: 40 + ((i * 31) % 220),
    })
  }
  return records
}

function bytesAsyncBuffer(bytes) {
  const buf = bytes.buffer
  const offset = bytes.byteOffset ?? 0
  const length = bytes.byteLength
  return {
    byteLength: length,
    async slice(start, end = length) {
      return buf.slice(offset + start, offset + end)
    },
  }
}

async function buildIndexBytes(records) {
  const sourceBytes = parquetWriteBuffer({
    columnData: [
      { name: 'id', data: records.map(r => r.id), type: 'INT64' },
      { name: 'timestamp', data: records.map(r => r.timestamp), type: 'STRING' },
      { name: 'model', data: records.map(r => r.model), type: 'STRING' },
      { name: 'prompt', data: records.map(r => r.prompt), type: 'STRING' },
      { name: 'response', data: records.map(r => r.response), type: 'STRING' },
      { name: 'tokens', data: records.map(r => r.tokens), type: 'INT32' },
    ],
  })
  const sourceFile = bytesAsyncBuffer(new Uint8Array(sourceBytes))
  const indexWriter = new ByteWriter()
  await createIndex({ sourceFile, indexFile: indexWriter })
  return new Uint8Array(indexWriter.getBytes())
}

async function dropIfExists(catalog, tableUrl, lister) {
  try {
    await icebergDropTable({ catalog, tableUrl, lister, purgeRequested: true })
    console.log(`  dropped ${tableUrl}`)
  } catch (err) {
    if (err && err.status === 404) return
    // Best-effort drop — surface anything that is not "not found".
    console.log(`  drop ${tableUrl}: ${err.message ?? err}`)
  }
}

async function main() {
  console.log(`Reading AWS credentials from profile "iceberg"...`)
  const credsProvider = fromIni({ profile: 'iceberg' })
  const creds = await credsProvider()
  const resolver = s3SignedResolver({
    accessKeyId: creds.accessKeyId,
    secretAccessKey: creds.secretAccessKey,
    sessionToken: creds.sessionToken,
    region: REGION,
  })
  const lister = s3Lister()
  const catalog = fileCatalog({ resolver, lister })

  console.log('Dropping any existing tables...')
  await dropIfExists(catalog, mainTableUrl, lister)
  await dropIfExists(catalog, indexTableUrl, lister)

  const records = generateRecords(500)
  console.log(`Generated ${records.length} LLM-log records`)

  console.log('Building parquetindex over the records...')
  const indexBytes = await buildIndexBytes(records)
  const indexAsyncBuf = bytesAsyncBuffer(indexBytes)
  const indexMetadata = await parquetMetadataAsync(indexAsyncBuf)
  const indexRows = await parquetReadObjects({ file: indexAsyncBuf, metadata: indexMetadata })
  console.log(`  parquetindex: ${indexBytes.length.toLocaleString()} bytes, ${indexRows.length.toLocaleString()} term-blocks`)

  const indexKv = {}
  for (const { key, value } of indexMetadata.key_value_metadata ?? []) {
    if (key.startsWith('parquetindex.')) indexKv[key] = value
  }

  console.log(`Creating + appending ${mainTableUrl}`)
  await icebergCreateTable({ catalog, tableUrl: mainTableUrl, schema: mainSchema })
  await icebergAppend({ catalog, tableUrl: mainTableUrl, records })

  // Look up the data file's true size from the iceberg manifest — parquetindex
  // needs `source_bytelength` to match the actual data file on S3.
  const mainMd = await icebergMetadata({ tableUrl: mainTableUrl, resolver })
  const manifestList = await icebergManifests({ metadata: mainMd, resolver })
  const { dataEntries } = splitManifestEntries(manifestList)
  if (dataEntries.length !== 1) {
    throw new Error(`expected exactly one data file in ${mainTableUrl}, got ${dataEntries.length}`)
  }
  const sourceFilePath = dataEntries[0].data_file.file_path
  const sourceByteLength = Number(dataEntries[0].data_file.file_size_in_bytes)
  indexKv['parquetindex.source_bytelength'] = String(sourceByteLength)
  console.log(`  data file: ${sourceFilePath} (${sourceByteLength.toLocaleString()} bytes)`)

  console.log(`Creating + appending ${indexTableUrl}`)
  await icebergCreateTable({
    catalog,
    tableUrl: indexTableUrl,
    schema: indexSchema,
    properties: indexKv,
  })
  await icebergAppend({ catalog, tableUrl: indexTableUrl, records: indexRows })

  const httpsUrl = `https://${BUCKET}.s3.amazonaws.com/${PREFIX}/llm_logs`
  console.log('Done.')
  console.log(`Open the demo at: ?key=${encodeURIComponent(httpsUrl)}`)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
