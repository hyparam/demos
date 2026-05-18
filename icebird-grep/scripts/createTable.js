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
import { asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
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
    { id: 4, name: 'category', required: true, type: 'string' },
    { id: 5, name: 'prompt', required: true, type: 'string' },
    { id: 6, name: 'response', required: true, type: 'string' },
    { id: 7, name: 'tokens', required: true, type: 'int' },
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

// Real conversation data from HuggingFaceH4/no_robots — 9.5K hand-written
// prompt/response pairs across categories (Generation, Brainstorm, Chat,
// Coding, Classify, Closed QA, Open QA, Extract, Rewrite, Summarize). Pulled
// straight from the parquet on Hugging Face's refs/convert/parquet branch.
const HF_PARQUET_URL = 'https://huggingface.co/datasets/HuggingFaceH4/no_robots/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet'

// Synthetic envelope columns for the LLM-log framing. The conversations
// themselves are real; the model/timestamp framing is invented.
const MODELS = ['claude-opus-4-7', 'claude-sonnet-4-6', 'claude-haiku-4-5', 'gpt-5', 'gpt-5-mini', 'gemini-3-pro', 'llama-4-405b', 'mistral-large-2']

function pick(arr, i) {
  return arr[((i % arr.length) + arr.length) % arr.length]
}

// Rough char-per-token estimate. Good enough for demo display; not used for
// search.
function estimateTokens(text) {
  return Math.max(1, Math.round(text.length / 4))
}

/**
 * Fetch the no_robots parquet from HuggingFace, decode it, and flatten each
 * row's `messages` list into a single prompt + response pair.
 */
async function fetchConversations() {
  console.log(`Downloading no_robots train parquet from HuggingFace...`)
  const buffer = cachedAsyncBuffer(await asyncBufferFromUrl({ url: HF_PARQUET_URL }))
  const metadata = await parquetMetadataAsync(buffer)
  const rows = await parquetReadObjects({ file: buffer, metadata })
  console.log(`  fetched ${rows.length.toLocaleString()} rows`)

  // `messages` is a list<struct{content,role}>. We want the first user turn as
  // the prompt and the assistant's reply as the response. A handful of rows
  // are multi-turn — join those into a transcript so the index still sees
  // every utterance.
  const baseTs = Date.UTC(2026, 0, 1)
  const records = []
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i]
    const messages = Array.isArray(r.messages) ? r.messages : []
    const userTurns = []
    const assistantTurns = []
    for (const m of messages) {
      const content = typeof m.content === 'string' ? m.content : ''
      if (!content) continue
      if (m.role === 'user') userTurns.push(content)
      else if (m.role === 'assistant') assistantTurns.push(content)
    }
    const prompt = userTurns.length ? userTurns.join('\n\n') : (typeof r.prompt === 'string' ? r.prompt : '')
    const response = assistantTurns.join('\n\n')
    if (!prompt || !response) continue
    records.push({
      id: BigInt(records.length + 1),
      timestamp: new Date(baseTs + records.length * 47_000).toISOString(),
      model: pick(MODELS, i * 7 + 3),
      category: typeof r.category === 'string' ? r.category : 'Unknown',
      prompt,
      response,
      tokens: estimateTokens(prompt) + estimateTokens(response),
    })
  }
  console.log(`  kept ${records.length.toLocaleString()} prompt/response pairs`)
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
      { name: 'category', data: records.map(r => r.category), type: 'STRING' },
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

  const records = await fetchConversations()

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
