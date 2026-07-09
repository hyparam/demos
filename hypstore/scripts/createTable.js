#!/usr/bin/env node
// Build the hypstore demo warehouse on S3 from WildChat:
//
//   s3://hyperparam-public/hypstore/wildchat - one row per conversation
//
// Downloads the allenai/WildChat-4.8M parquet files from Hugging Face, keeps
// one row per conversation with the raw model input stored as a JSON string,
// then creates the table and appends ALL rows in a single commit, so the
// table is one data file with one hypgrep text index built alongside it.
//
// A single append means files larger than the 5 GB S3 single-PUT limit, so
// the resolver's writer spools bytes to a local temp file and uploads it
// with a multipart upload on finish. Expect the full run to need ~25 GB of
// disk and a large node heap:
//
//   node --max-old-space-size=110000 scripts/createTable.js
//
// Env knobs: AWS_PROFILE (default hyperparam-platypii), DATA_DIR (default
// ~/wildchat-data), FILE_LIMIT / CONV_LIMIT to cap input for test runs,
// TABLE to write a test table name.

import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { fromIni } from '@aws-sdk/credential-providers'
import { Upload } from '@aws-sdk/lib-storage'
import { asyncBufferFromFile, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { ByteWriter } from 'hyparquet-writer'
import { append, createStore, createTable } from 'hypstore'
import { fileCatalog, icebergDropTable, s3Lister, s3SignedResolver } from 'icebird'

const BUCKET = 'hyperparam-public'
const PREFIX = 'hypstore'
const REGION = 'us-east-1'
const TABLE = process.env.TABLE || 'wildchat'

const warehouseUrl = `s3://${BUCKET}/${PREFIX}`

const DATASET = 'allenai/WildChat-4.8M'
const NUM_FILES = 86
const DATA_DIR = process.env.DATA_DIR || path.join(os.homedir(), 'wildchat-data')
const FILE_LIMIT = process.env.FILE_LIMIT ? Number(process.env.FILE_LIMIT) : NUM_FILES
const CONV_LIMIT = process.env.CONV_LIMIT ? Number(process.env.CONV_LIMIT) : Infinity
const DOWNLOAD_CONCURRENCY = 4

// One row per conversation. The conversation column holds the raw model
// input (the role/content messages) as a JSON string, and is text-indexed
// for grep. No vector index: this demo is sql + grep only, so no embedder
// is needed.
const schema = {
  id: 'string',
  timestamp: 'timestamp',
  model: 'string',
  turn: 'int',
  language: 'string',
  country: 'string',
  conversation: 'string',
}
const index = { text: ['conversation'] }

function sourceFileName(i) {
  return `train-${String(i).padStart(5, '0')}-of-${String(NUM_FILES).padStart(5, '0')}.parquet`
}

/** Download the WildChat parquet files that are not already on disk. */
async function downloadDataset() {
  fs.mkdirSync(DATA_DIR, { recursive: true })
  const names = Array.from({ length: FILE_LIMIT }, (_, i) => sourceFileName(i))
  let next = 0
  let done = 0
  async function worker() {
    for (;;) {
      const i = next++
      if (i >= names.length) return
      const name = names[i]
      const dest = path.join(DATA_DIR, name)
      if (fs.existsSync(dest) && fs.statSync(dest).size > 0) {
        done++
        continue
      }
      const url = `https://huggingface.co/datasets/${DATASET}/resolve/main/data/${name}`
      const res = await fetch(url)
      if (!res.ok || !res.body) throw new Error(`GET ${url}: ${res.status} ${res.statusText}`)
      const tmp = `${dest}.download`
      await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmp))
      fs.renameSync(tmp, dest)
      done++
      console.log(`  downloaded ${name} (${done}/${names.length})`)
    }
  }
  await Promise.all(Array.from({ length: DOWNLOAD_CONCURRENCY }, worker))
  console.log(`Dataset ready in ${DATA_DIR} (${names.length} files)`)
  return names
}

/**
 * Build one record per WildChat conversation. The conversation is stored
 * unsplit as a JSON string of the raw model input (role/content messages),
 * which is what the grep index runs over.
 */
function conversationRecord(row) {
  const conversation = row.conversation ?? []
  const messages = conversation.map(message => ({
    role: message.role,
    content: message.content,
  }))
  return {
    id: row.conversation_hash,
    timestamp: row.timestamp,
    model: row.model,
    turn: Number(row.turn ?? conversation.length),
    language: row.language,
    country: row.country,
    conversation: JSON.stringify(messages),
  }
}

/** Read the downloaded parquet files into one record per conversation. */
async function loadRecords(names) {
  const records = []
  for (const name of names) {
    if (records.length >= CONV_LIMIT) break
    const file = await asyncBufferFromFile(path.join(DATA_DIR, name))
    const rows = await parquetReadObjects({
      file,
      columns: ['conversation_hash', 'model', 'timestamp', 'conversation', 'turn', 'language', 'country'],
      compressors,
    })
    for (const row of rows) {
      if (records.length >= CONV_LIMIT) break
      records.push(conversationRecord(row))
    }
    const heapGb = (process.memoryUsage().heapUsed / 2 ** 30).toFixed(1)
    console.log(`  ${name}: ${records.length.toLocaleString()} conversations, heap ${heapGb} GB`)
  }
  return records
}

/**
 * Writer that spools bytes to a local temp file (same chunked-flush pattern
 * as hyparquet-writer's fileWriter), then uploads to S3 on finish. Small
 * files and conditional writes (metadata commits) go up as a single
 * PutObject; anything larger uses a multipart upload, which is what lets a
 * single append exceed the 5 GB single-PUT limit.
 */
function spoolWriter({ s3, url, spoolDir, spoolId, options }) {
  const { bucket, key } = parseS3Url(url)
  const spoolPath = path.join(spoolDir, `spool-${spoolId}`)
  fs.writeFileSync(spoolPath, '', { flag: 'w' })
  const writer = new ByteWriter()
  const chunkSize = 1_000_000

  function flushToDisk() {
    const chunk = new Uint8Array(writer.buffer, 0, writer.index)
    fs.writeFileSync(spoolPath, chunk, { flag: 'a' })
    writer.index = 0
  }

  writer.ensure = function(size) {
    if (writer.index > chunkSize) flushToDisk()
    if (writer.index + size > writer.buffer.byteLength) {
      const newSize = Math.max(writer.buffer.byteLength * 2, writer.index + size)
      const newBuffer = new ArrayBuffer(newSize)
      new Uint8Array(newBuffer).set(new Uint8Array(writer.buffer))
      writer.buffer = newBuffer
      writer.view = new DataView(writer.buffer)
    }
  }
  writer.getBuffer = function() {
    throw new Error('getBuffer not supported for spoolWriter')
  }
  writer.getBytes = function() {
    throw new Error('getBytes not supported for spoolWriter')
  }
  writer.finish = async function() {
    flushToDisk()
    const size = fs.statSync(spoolPath).size
    try {
      if (options?.ifNoneMatch || size < 100_000_000) {
        await s3.send(new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: fs.readFileSync(spoolPath),
          ...options?.ifNoneMatch ? { IfNoneMatch: options.ifNoneMatch } : {},
        }))
      } else {
        console.log(`  multipart upload of ${(size / 2 ** 30).toFixed(2)} GB to ${url}`)
        const upload = new Upload({
          client: s3,
          params: { Bucket: bucket, Key: key, Body: fs.createReadStream(spoolPath) },
          partSize: 64 * 2 ** 20,
          queueSize: 4,
        })
        await upload.done()
      }
    } catch (err) {
      // icebird's conditional metadata commits expect err.status
      if (err?.$metadata?.httpStatusCode && err.status === undefined) {
        err.status = err.$metadata.httpStatusCode
      }
      throw err
    } finally {
      fs.rmSync(spoolPath, { force: true })
    }
  }
  return writer
}

function parseS3Url(url) {
  const match = /^s3:\/\/([^/]+)\/(.+)$/.exec(url)
  if (!match) throw new Error(`expected s3:// url, got ${url}`)
  return { bucket: match[1], key: match[2] }
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
  const creds = await fromIni({ profile })()
  const signed = s3SignedResolver({
    accessKeyId: creds.accessKeyId,
    secretAccessKey: creds.secretAccessKey,
    sessionToken: creds.sessionToken,
    region: REGION,
  })
  const s3 = new S3Client({ region: REGION, credentials: creds })
  const spoolDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hypstore-spool-'))
  let spoolId = 0
  const resolver = {
    reader: signed.reader,
    deleter: signed.deleter,
    writer: (url, options) => spoolWriter({ s3, url, spoolDir, spoolId: spoolId++, options }),
  }
  const lister = s3Lister()

  console.log(`Downloading ${DATASET} (${FILE_LIMIT} of ${NUM_FILES} files)...`)
  const names = await downloadDataset()

  console.log('Loading conversations...')
  const records = await loadRecords(names)
  console.log(`Loaded ${records.length.toLocaleString()} conversations`)

  console.log('Dropping any existing table...')
  const catalog = fileCatalog({ resolver, lister })
  await dropIfExists(catalog, `${warehouseUrl}/${TABLE}`, lister)

  const store = createStore({ warehouseUrl, resolver, lister })

  console.log(`Creating ${warehouseUrl}/${TABLE}`)
  await createTable({ store, table: TABLE, schema, index })

  console.log(`Appending ${records.length.toLocaleString()} records in a single commit...`)
  console.log('(writes the data file, uploads it, then builds the grep index)')
  const start = Date.now()
  await append({ store, table: TABLE, records })
  console.log(`Append took ${((Date.now() - start) / 60000).toFixed(1)} minutes`)

  fs.rmSync(spoolDir, { recursive: true, force: true })
  console.log('Done.')
  console.log(`Table: ${warehouseUrl}/${TABLE}`)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
