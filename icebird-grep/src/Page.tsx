import HighTable, { CellContentProps, sortableDataFrame } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { AsyncBuffer, FileMetaData, KeyValue, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { parquetDataFrame } from 'hyperparam'
import { icebergManifests, icebergMetadata } from 'icebird'
import { translateS3Url } from 'icebird/src/fetch.js'
import { splitManifestEntries } from 'icebird/src/manifest.js'
import type { ManifestList } from 'icebird/src/manifest.js'
import type { ManifestEntry } from 'icebird/src/types.js'
import { parquetFind } from 'parquetindex'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'

const PARQUETINDEX_KV_KEYS = [
  'parquetindex.version',
  'parquetindex.block_size',
  'parquetindex.text_columns',
  'parquetindex.source_rows',
  'parquetindex.source_bytelength',
] as const

interface LoadedTables {
  df: DataFrame
  columns: string[]
  numRows: number
  sourceUrl: string
  sourceFile: AsyncBuffer
  sourceMetadata: FileMetaData
  indexFile: AsyncBuffer
  patchedIndexMetadata: FileMetaData
}

interface PageProps {
  tableUrl: string
  initialQuery: string
  setError: (e: unknown) => void
}

// Dedupe iceberg + parquet loads across React StrictMode's double-invocation
// of effects, HMR remounts, and re-mounts due to URL changes. A whole demo
// session typically only touches one or two tables.
const loadCache = new Map<string, Promise<LoadedTables>>()
const asyncBufferCache = new Map<string, Promise<AsyncBuffer>>()

function getAsyncBuffer({ url, byteLength }: { url: string; byteLength?: number }): Promise<AsyncBuffer> {
  let p = asyncBufferCache.get(url)
  if (!p) {
    p = asyncBufferFromUrl({ url, byteLength }).then(cachedAsyncBuffer)
    asyncBufferCache.set(url, p)
  }
  return p
}

function oneDataFile(manifests: ManifestList): ManifestEntry {
  const { dataEntries } = splitManifestEntries(manifests)
  if (dataEntries.length === 0) throw new Error('no data files in iceberg table')
  if (dataEntries.length > 1) throw new Error('icebird-grep demo expects exactly one data file per table')
  return dataEntries[0]
}

function loadTables(tableUrl: string): Promise<LoadedTables> {
  const cached = loadCache.get(tableUrl)
  if (cached) return cached
  const promise = doLoadTables(tableUrl).catch((err: unknown) => {
    loadCache.delete(tableUrl)
    throw err
  })
  loadCache.set(tableUrl, promise)
  return promise
}

/**
 * Single-pass load of the iceberg main + sibling-index tables.
 *
 * Each round is one Promise.all so we make exactly one batch of requests per
 * step: 2 metadata reads, then 2 manifest reads, then 2 parquet footer reads.
 * Without this dedupe the demo issues every step twice — once to find data
 * file URLs and again inside `icebergRead` / `parquetFind` — and StrictMode
 * doubles it again in dev.
 */
async function doLoadTables(tableUrl: string): Promise<LoadedTables> {
  const indexTableUrl = tableUrl.replace(/\/+$/, '') + '.index'

  const [mainMd, indexMd] = await Promise.all([
    icebergMetadata({ tableUrl }),
    icebergMetadata({ tableUrl: indexTableUrl }),
  ])

  const [mainManifests, indexManifests] = await Promise.all([
    icebergManifests({ metadata: mainMd }),
    icebergManifests({ metadata: indexMd }),
  ])

  const mainEntry = oneDataFile(mainManifests).data_file
  const indexEntry = oneDataFile(indexManifests).data_file
  const sourceUrl = translateS3Url(mainEntry.file_path)
  const sourceByteLength = Number(mainEntry.file_size_in_bytes)
  const indexDataUrl = translateS3Url(indexEntry.file_path)
  const indexByteLength = Number(indexEntry.file_size_in_bytes)

  const [sourceFile, indexFile] = await Promise.all([
    getAsyncBuffer({ url: sourceUrl, byteLength: sourceByteLength }),
    getAsyncBuffer({ url: indexDataUrl, byteLength: indexByteLength }),
  ])
  const [sourceMetadata, rawIndexMetadata] = await Promise.all([
    parquetMetadataAsync(sourceFile),
    parquetMetadataAsync(indexFile),
  ])

  const schema = mainMd.schemas.find(s => s['schema-id'] === mainMd['current-schema-id'])
  if (!schema) throw new Error('iceberg current schema missing')
  const columns = schema.fields.map(f => f.name)

  // parquetDataFrame fetches rows in 1000-row chunks on demand via HTTP range
  // requests — initial render only needs the parquet footer plus the first
  // visible page, never the full 7-8MB source.
  const df = sortableDataFrame(parquetDataFrame(
    { url: sourceUrl, byteLength: sourceByteLength },
    sourceMetadata,
  ))

  // parquetindex's kv metadata (block_size, text_columns, source_rows,
  // source_bytelength, version) lives in the iceberg table's `properties` so
  // the data file is a normal iceberg parquet. Splice those back into the
  // parquet metadata so `queryIndex` can find them.
  const props = indexMd.properties ?? {}
  const propKv: KeyValue[] = []
  for (const k of PARQUETINDEX_KV_KEYS) {
    const v = props[k]
    if (v) propKv.push({ key: k, value: v })
  }
  const existingKv = rawIndexMetadata.key_value_metadata ?? []
  const patchedIndexMetadata: FileMetaData = {
    ...rawIndexMetadata,
    key_value_metadata: [
      ...existingKv.filter(kv => !PARQUETINDEX_KV_KEYS.includes(kv.key as never)),
      ...propKv,
    ],
  }

  return {
    df,
    columns,
    numRows: Number(sourceMetadata.num_rows),
    sourceUrl,
    sourceFile,
    sourceMetadata,
    indexFile,
    patchedIndexMetadata,
  }
}

export default function Page({ tableUrl, initialQuery, setError }: PageProps): ReactNode {
  const [loaded, setLoaded] = useState<LoadedTables>()
  const [query, setQuery] = useState(initialQuery)
  const [results, setResults] = useState<{ rows: Record<string, unknown>[]; rowNumbers: number[] } | undefined>()
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()

  useEffect(() => {
    let cancelled = false
    queueMicrotask(() => {
      if (cancelled) return
      setLoaded(undefined)
      setResults(undefined)
    })
    loadTables(tableUrl)
      .then(tables => { if (!cancelled) setLoaded(tables) })
      .catch((err: unknown) => { if (!cancelled) setError(err) })
    return () => { cancelled = true }
  }, [tableUrl, setError])

  const isQuerying = query.trim().length > 0

  useEffect(() => {
    const params = new URLSearchParams(location.search)
    if (isQuerying) params.set('q', query)
    else params.delete('q')
    history.replaceState({}, '', `${location.pathname}?${params}`)
  }, [query, isQuerying])

  useEffect(() => {
    if (!loaded || !isQuerying) {
      queueMicrotask(() => {
        setResults(undefined)
        setQueryTime(undefined)
        setFirstRowTime(undefined)
      })
      return
    }
    const controller = new AbortController()
    const { signal } = controller

    async function search(tables: LoadedTables) {
      setQueryTime(undefined)
      setFirstRowTime(undefined)
      const startTime = performance.now()
      const collected: Record<string, unknown>[] = []
      const rowNumbers: number[] = []
      const rowGen = parquetFind({
        query,
        url: tables.sourceUrl,
        limit: 100,
        asyncBufferFactory: getAsyncBuffer,
        sourceFile: tables.sourceFile,
        sourceMetadata: tables.sourceMetadata,
        indexFile: tables.indexFile,
        indexMetadata: tables.patchedIndexMetadata,
        signal,
      })
      for await (const row of rowGen) {
        if (signal.aborted) return
        if (collected.length === 0) {
          setFirstRowTime(performance.now() - startTime)
        }
        const idx = row.__index__ as number
        delete row.__index__
        rowNumbers.push(idx)
        collected.push(row)
      }
      setResults({ rows: collected, rowNumbers })
      setQueryTime(performance.now() - startTime)
    }
    search(loaded).catch((err: unknown) => {
      if (signal.aborted) return
      setError(err)
    })
    return () => { controller.abort() }
  }, [query, isQuerying, loaded, setError])

  const df = useMemo<DataFrame | undefined>(() => {
    if (!loaded) return undefined
    if (results) {
      const columnDescriptors = loaded.columns.map(name => ({ name }))
      return arrayDataFrame(results.rows, results.rowNumbers, { columnDescriptors })
    }
    return loaded.df
  }, [loaded, results])

  const renderCellContent = useCallback(({ cell, stringify }: CellContentProps) => {
    if (!isQuerying) return stringify(cell?.value)
    const value: unknown = cell?.value
    if (typeof value !== 'string') return stringify(value)
    const tokens = query.toLowerCase().split(/\W+/).filter(t => t.length > 0)
    if (tokens.length === 0) return stringify(value)
    const lower = value.toLowerCase()
    let firstIndex = -1
    let firstLength = 0
    for (const t of tokens) {
      const idx = lower.indexOf(t)
      if (idx >= 0 && (firstIndex === -1 || idx < firstIndex)) {
        firstIndex = idx
        firstLength = t.length
      }
    }
    if (firstIndex < 0) return stringify(value)
    const truncateBefore = firstIndex > 20 ? '...' : ''
    return <>
      {truncateBefore}
      {value.slice(0, firstIndex).slice(-20)}
      <mark>{value.slice(firstIndex, firstIndex + firstLength)}</mark>
      {value.slice(firstIndex + firstLength)}
    </>
  }, [isQuerying, query])

  return <>
    <div className='top-header'>
      <span className='file-name'>{tableUrl}</span>
      <div className='view-meta'>
        {loaded && <span>{loaded.numRows.toLocaleString()} rows</span>}
        {results && <span className='matches'>{results.rows.length} match{results.rows.length === 1 ? '' : 'es'}</span>}
        {queryTime !== undefined && <span>query: {queryTime.toFixed(0)} ms</span>}
        {firstRowTime !== undefined && <span>first: {firstRowTime.toFixed(0)} ms</span>}
        <input
          type='text'
          placeholder='Search...'
          autoFocus
          value={query}
          onChange={e => { setQuery(e.target.value) }}
        />
      </div>
    </div>
    {df && <HighTable
      focus={false}
      cacheKey={tableUrl + (isQuerying ? `?${query}` : '')}
      className='hightable'
      data={df}
      maxRowNumber={loaded?.numRows}
      onError={setError}
      renderCellContent={renderCellContent}
    />}
  </>
}
