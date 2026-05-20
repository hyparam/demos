#!/usr/bin/env node
// Build the iceberg-hypgrep demo tables on S3:
//
//   s3://hyperparam-iceberg/iceberg-hypgrep/llm_logs        - iceberg table with LLM logs
//   s3://hyperparam-iceberg/iceberg-hypgrep/llm_logs.index  - iceberg table whose data files
//                                                             are hypgrep indices over llm_logs
//
// The script appends 30 daily snapshots to llm_logs (day N gets roughly
// N * BASE rows), then commits a 31st snapshot that REPLACES the manifest-
// list with a single consolidated parquet containing all rows. Snapshots
// 1-30 remain readable via time-travel (their manifest-lists are
// independent), so the table demonstrates both growth-over-time AND a
// compact head state. llm_logs.index is then (re)created as a single index
// over that consolidated file, so live grep queries hit one (data, index)
// pair instead of 30.
//
// The index table's iceberg schema matches the hypgrep columnar layout
// (term, blockId, docCount, termFreq). The "constant" hypgrep kv metadata
// (block_size, text_columns, version) lives in the index iceberg table's
// `properties`; the per-file values (source_rows, source_bytelength) are
// derived at read time from the corresponding main-table manifest entry.
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
import { prepareAppend } from 'icebird/src/write/stage.js'
import { buildSnapshotUpdate, loadPriorManifests } from 'icebird/src/write/snapshot.js'
import { fileCatalogCommit } from 'icebird/src/write/commit.js'
import { createIndex } from 'hypgrep'

const BUCKET = 'hyperparam-iceberg'
const PREFIX = 'iceberg-hypgrep'
const REGION = 'us-east-1'

const mainTableUrl = `s3://${BUCKET}/${PREFIX}/llm_logs`
const indexTableUrl = `s3://${BUCKET}/${PREFIX}/llm_logs.index`

const NUM_DAYS = 30
// Day N (1..30) gets DAY_BASE * N rows. Total ≈ DAY_BASE * 465. With shard 0
// of WildChat-1M (~60K rows) we set DAY_BASE so we use most of the shard.
const DAY_BASE = 120

const mainSchema = {
  type: 'struct',
  'schema-id': 0,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'timestamp', required: true, type: 'string' },
    { id: 3, name: 'model', required: true, type: 'string' },
    { id: 4, name: 'language', required: false, type: 'string' },
    { id: 5, name: 'country', required: false, type: 'string' },
    { id: 6, name: 'prompt', required: true, type: 'string' },
    { id: 7, name: 'response', required: true, type: 'string' },
    { id: 8, name: 'tokens', required: true, type: 'int' },
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

// Real conversation data from allenai/WildChat-1M — 838K real ChatGPT
// conversations with timestamps, models, geolocation. Shard 0 has ~60K rows,
// enough for 30 days of growing batches.
const HF_PARQUET_URL = 'https://huggingface.co/datasets/allenai/WildChat-1M/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet'

// Rough char-per-token estimate. Good enough for demo display; not used for
// search.
function estimateTokens(text) {
  return Math.max(1, Math.round(text.length / 4))
}

/**
 * Fetch the WildChat-1M parquet shard, flatten each conversation's turns into
 * a single prompt + response pair, and keep WildChat's real model/language/
 * country/timestamp framing.
 */
async function fetchConversations() {
  console.log(`Downloading WildChat-1M shard 0 from HuggingFace...`)
  const buffer = cachedAsyncBuffer(await asyncBufferFromUrl({ url: HF_PARQUET_URL }))
  const metadata = await parquetMetadataAsync(buffer)
  const totalRows = Number(metadata.num_rows)
  // We only need enough conversations to fill 30 daily batches.
  const needed = DAY_BASE * (NUM_DAYS * (NUM_DAYS + 1)) / 2
  console.log(`  shard has ${totalRows.toLocaleString()} conversations, want ${needed.toLocaleString()}`)

  // Read in chunks of CHUNK rows, projecting only the columns we use. The
  // shard's full record (with moderation arrays + headers) is huge — projecting
  // drops it by ~10x and lets us stay under Node's 4GB default heap.
  const COLUMNS = ['conversation', 'model', 'timestamp', 'language', 'country']
  const CHUNK = 2000
  const records = []
  for (let start = 0; start < totalRows && records.length < needed; start += CHUNK) {
    const end = Math.min(start + CHUNK, totalRows)
    const rows = await parquetReadObjects({ file: buffer, metadata, columns: COLUMNS, rowStart: start, rowEnd: end })
    for (const r of rows) {
      const turns = Array.isArray(r.conversation) ? r.conversation : []
      const userTurns = []
      const assistantTurns = []
      for (const t of turns) {
        const content = typeof t.content === 'string' ? t.content : ''
        if (!content) continue
        if (t.role === 'user') userTurns.push(content)
        else if (t.role === 'assistant') assistantTurns.push(content)
      }
      const prompt = userTurns.join('\n\n')
      const response = assistantTurns.join('\n\n')
      if (!prompt || !response) continue
      const ts = r.timestamp instanceof Date ? r.timestamp.toISOString()
        : typeof r.timestamp === 'string' ? r.timestamp
          : new Date().toISOString()
      records.push({
        id: BigInt(records.length + 1),
        timestamp: ts,
        model: typeof r.model === 'string' ? r.model : 'unknown',
        language: typeof r.language === 'string' ? r.language : null,
        country: typeof r.country === 'string' ? r.country : null,
        prompt,
        response,
        tokens: estimateTokens(prompt) + estimateTokens(response),
      })
      if (records.length >= needed) break
    }
    if (start % (CHUNK * 5) === 0) {
      console.log(`  read ${end.toLocaleString()} / ${totalRows.toLocaleString()} rows, kept ${records.length.toLocaleString()}`)
    }
  }
  console.log(`  kept ${records.length.toLocaleString()} prompt/response pairs`)
  if (records.length < needed) {
    throw new Error(`need ${needed} records, only got ${records.length} from shard 0`)
  }
  return records
}

/**
 * Split records into 30 daily batches with linearly-growing sizes:
 * day N gets DAY_BASE * N rows. Each row's `timestamp` is rewritten to fall
 * inside day N so the data spans 30 calendar days ending today.
 */
function splitIntoDailyBatches(records, today = new Date()) {
  const dayMs = 86_400_000
  // Anchor day 30 to "today at 00:00 UTC", day 1 is 29 days earlier.
  const day30Start = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate())
  const batches = []
  let offset = 0
  for (let day = 1; day <= NUM_DAYS; day++) {
    const want = DAY_BASE * day
    const slice = records.slice(offset, offset + want)
    if (slice.length === 0) {
      throw new Error(`ran out of source records at day ${day} — need ${want} more`)
    }
    offset += slice.length
    const dayStart = day30Start - (NUM_DAYS - day) * dayMs
    // Spread the day's rows evenly across the 24-hour window.
    const stepMs = dayMs / slice.length
    const dated = slice.map((r, j) => ({
      ...r,
      timestamp: new Date(dayStart + Math.floor(j * stepMs)).toISOString(),
    }))
    batches.push(dated)
  }
  const totalUsed = batches.reduce((n, b) => n + b.length, 0)
  console.log(`  built ${batches.length} daily batches, ${totalUsed.toLocaleString()} rows total`)
  console.log(`  day 1 = ${batches[0].length} rows, day ${NUM_DAYS} = ${batches[NUM_DAYS - 1].length} rows`)
  return batches
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

/**
 * Build a hypgrep index over `records` and return its rows plus the constant
 * hypgrep KV metadata. Per-file KV (source_rows, source_bytelength) is *not*
 * returned — the client recomputes those from iceberg manifests at read time.
 */
async function buildIndexRows(records) {
  const sourceBytes = parquetWriteBuffer({
    columnData: [
      { name: 'id', data: records.map(r => r.id), type: 'INT64' },
      { name: 'timestamp', data: records.map(r => r.timestamp), type: 'STRING' },
      { name: 'model', data: records.map(r => r.model), type: 'STRING' },
      { name: 'language', data: records.map(r => r.language), type: 'STRING' },
      { name: 'country', data: records.map(r => r.country), type: 'STRING' },
      { name: 'prompt', data: records.map(r => r.prompt), type: 'STRING' },
      { name: 'response', data: records.map(r => r.response), type: 'STRING' },
      { name: 'tokens', data: records.map(r => r.tokens), type: 'INT32' },
    ],
  })
  const sourceFile = bytesAsyncBuffer(new Uint8Array(sourceBytes))
  const indexWriter = new ByteWriter()
  await createIndex({ sourceFile, indexFile: indexWriter })
  const indexBytes = new Uint8Array(indexWriter.getBytes())
  const indexAsyncBuf = bytesAsyncBuffer(indexBytes)
  const indexMetadata = await parquetMetadataAsync(indexAsyncBuf)
  const indexRows = await parquetReadObjects({ file: indexAsyncBuf, metadata: indexMetadata })
  const constKv = {}
  for (const { key, value } of indexMetadata.key_value_metadata ?? []) {
    // Strip per-file keys; the demo client recomputes them from manifests.
    if (key === 'hypgrep.source_rows' || key === 'hypgrep.source_bytelength') continue
    if (key.startsWith('hypgrep.')) constKv[key] = value
  }
  return { indexRows, indexBytes, constKv }
}

/**
 * Commit a snapshot that REPLACES the current manifest-list with a single new
 * manifest containing one consolidated data file. Old data files stay on S3
 * and remain reachable through prior snapshots' manifest-lists, so time-travel
 * to the daily snapshots still works — but the current snapshot scans just
 * the one consolidated file.
 *
 * Built on icebird internals (prepareAppend + buildSnapshotUpdate's
 * skipPriorManifestPaths + fileCatalogCommit) because the public API only
 * exposes append + row-level delete, not file-level replace.
 */
async function icebergReplaceWithSingleFile({ tableUrl, resolver, records }) {
  const metadata = await icebergMetadata({ tableUrl, resolver })
  const prepared = await prepareAppend({ tableUrl, metadata, records, resolver })
  const priorManifests = await loadPriorManifests(metadata, resolver)
  const skipPriorManifestPaths = new Set(priorManifests.map(m => m.manifest_path))

  // Counts being dropped — only data manifests (content === 0). Delete-file
  // manifests would show up as content !== 0 but this demo has none.
  let removedFiles = 0
  let removedRows = 0n
  let removedBytes = 0n
  for (const m of priorManifests) {
    if (m.content !== 0) continue
    removedFiles += Number(m.added_files_count ?? 0) + Number(m.existing_files_count ?? 0) - Number(m.deleted_files_count ?? 0)
    removedRows += BigInt(m.added_rows_count ?? 0) + BigInt(m.existing_rows_count ?? 0) - BigInt(m.deleted_rows_count ?? 0)
  }

  const sequenceNumber = BigInt(metadata['last-sequence-number'] ?? 0) + 1n
  const newManifest = {
    manifest_path: prepared.manifestPath,
    manifest_length: prepared.manifestLength,
    partition_spec_id: prepared.partitionSpecId,
    content: 0,
    sequence_number: sequenceNumber,
    min_sequence_number: sequenceNumber,
    added_snapshot_id: prepared.snapshotId,
    added_files_count: prepared.addedDataFilesCount,
    existing_files_count: 0,
    deleted_files_count: 0,
    added_rows_count: prepared.addedRowCount,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions: prepared.partitions,
  }

  const summary = {
    operation: 'overwrite',
    'added-data-files': String(prepared.addedDataFilesCount),
    'deleted-data-files': String(removedFiles),
    'added-records': String(prepared.recordsCount),
    'deleted-records': String(removedRows),
    'added-files-size': String(prepared.addedFilesSize),
    'removed-files-size': String(removedBytes),
    'total-records': String(prepared.recordsCount),
    'total-files-size': String(prepared.addedFilesSize),
    'total-data-files': String(prepared.addedDataFilesCount),
    'total-delete-files': '0',
    'total-position-deletes': '0',
    'total-equality-deletes': '0',
  }

  const staged = await buildSnapshotUpdate({
    tableUrl, metadata, resolver,
    snapshotId: prepared.snapshotId,
    sequenceNumber,
    manifestUuid: prepared.manifestUuid,
    timestampMs: Date.now(),
    formatVersion: prepared.formatVersion,
    newManifests: [newManifest],
    summary,
    writtenFiles: prepared.writtenFiles,
    priorManifests,
    skipPriorManifestPaths,
  })
  return await fileCatalogCommit({ tableUrl, metadata, staged, resolver })
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
  const profile = process.env.AWS_PROFILE || 'hyperparam-platypii'
  console.log(`Reading AWS credentials from profile "${profile}"...`)
  const credsProvider = fromIni({ profile })
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
  const batches = splitIntoDailyBatches(records)

  console.log(`Creating ${mainTableUrl}`)
  await icebergCreateTable({ catalog, tableUrl: mainTableUrl, schema: mainSchema })

  for (let day = 1; day <= NUM_DAYS; day++) {
    const batch = batches[day - 1]
    console.log(`Day ${day}/${NUM_DAYS}: appending ${batch.length.toLocaleString()} rows`)
    await icebergAppend({ catalog, tableUrl: mainTableUrl, records: batch })
  }

  // Consolidate: commit a 31st snapshot whose manifest-list contains only one
  // new manifest with the single consolidated parquet. The 30 historical
  // snapshots remain queryable via time-travel.
  console.log(`Consolidating ${mainTableUrl} → single data file (snapshot ${NUM_DAYS + 1})`)
  await icebergReplaceWithSingleFile({ tableUrl: mainTableUrl, resolver, records })

  // Build the hypgrep index over the consolidated dataset and write a fresh
  // single-file llm_logs.index.
  console.log('Building hypgrep index over consolidated data...')
  const { indexRows, indexBytes, constKv } = await buildIndexRows(records)
  console.log(`  ${indexRows.length.toLocaleString()} term-blocks, ${indexBytes.length.toLocaleString()} bytes`)

  console.log(`Creating ${indexTableUrl} (properties carry constant hypgrep KV)`)
  await icebergCreateTable({
    catalog,
    tableUrl: indexTableUrl,
    schema: indexSchema,
    properties: constKv,
  })
  console.log(`Appending index data file to ${indexTableUrl}`)
  await icebergAppend({ catalog, tableUrl: indexTableUrl, records: indexRows })

  // Sanity check the current snapshot of each table.
  const mainMd = await icebergMetadata({ tableUrl: mainTableUrl, resolver })
  const indexMd = await icebergMetadata({ tableUrl: indexTableUrl, resolver })
  const mainManifests = await icebergManifests({ metadata: mainMd, resolver })
  const indexManifests = await icebergManifests({ metadata: indexMd, resolver })
  const mainData = splitManifestEntries(mainManifests).dataEntries
  const indexData = splitManifestEntries(indexManifests).dataEntries
  const snapshotCount = mainMd.snapshots?.length ?? 0
  console.log(`Final HEAD: ${mainData.length} main data file, ${indexData.length} index data file`)
  console.log(`Snapshot history: ${snapshotCount} snapshots (30 daily appends + 1 consolidating overwrite)`)
  if (mainData.length !== 1 || indexData.length !== 1) {
    throw new Error(`expected exactly 1 data file at HEAD of each table`)
  }
  if (snapshotCount !== NUM_DAYS + 1) {
    throw new Error(`expected ${NUM_DAYS + 1} snapshots, got ${snapshotCount}`)
  }

  const httpsUrl = `https://${BUCKET}.s3.amazonaws.com/${PREFIX}/llm_logs`
  console.log('Done.')
  console.log(`Open the demo at: ?key=${encodeURIComponent(httpsUrl)}`)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
