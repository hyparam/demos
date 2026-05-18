import HighTable, { CellContentProps } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { AsyncBuffer, FileMetaData, KeyValue, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { icebergManifests, icebergMetadata, icebergRead, urlResolver } from 'icebird'
import { splitManifestEntries } from 'icebird/src/manifest.js'
import type { TableMetadata } from 'icebird/src/types.js'
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
  rows: Record<string, unknown>[]
  columns: string[]
  sourceUrl: string
  sourceByteLength: number
  indexUrl: string
  indexProperties: Record<string, string>
}

interface PageProps {
  tableUrl: string
  initialQuery: string
  setError: (e: unknown) => void
}

const asyncBufferCache = new Map<string, Promise<AsyncBuffer>>()
function asyncBufferFactory({ url, byteLength }: { url: string; byteLength?: number }): Promise<AsyncBuffer> {
  let cached = asyncBufferCache.get(url)
  if (!cached) {
    cached = asyncBufferFromUrl({ url, byteLength }).then(cachedAsyncBuffer)
    asyncBufferCache.set(url, cached)
  }
  return cached
}

function translateS3(url: string): string {
  if (url.startsWith('s3://') || url.startsWith('s3a://')) {
    const rest = url.slice(url.indexOf('://') + 3)
    const slash = rest.indexOf('/')
    if (slash === -1) throw new Error(`invalid S3 URL: ${url}`)
    return `https://${rest.slice(0, slash)}.s3.amazonaws.com${rest.slice(slash)}`
  }
  return url
}

/**
 * Pull the table's single data file URL from its manifests. We rely on a
 * single data file per table for this demo so the parquetindex row offsets
 * (within one parquet) line up with the iceberg row indexes.
 */
async function singleDataFileUrl(metadata: TableMetadata): Promise<{ url: string; byteLength: number }> {
  const resolver = urlResolver()
  const manifestList = await icebergManifests({ metadata, resolver })
  const { dataEntries } = splitManifestEntries(manifestList)
  if (dataEntries.length === 0) throw new Error('no data files in iceberg table')
  if (dataEntries.length > 1) throw new Error('icebird-grep demo expects exactly one data file per table')
  const entry = dataEntries[0]
  return {
    url: translateS3(entry.data_file.file_path),
    byteLength: Number(entry.data_file.file_size_in_bytes),
  }
}

export default function Page({ tableUrl, initialQuery, setError }: PageProps): ReactNode {
  const [loaded, setLoaded] = useState<LoadedTables>()
  const [query, setQuery] = useState(initialQuery)
  const [results, setResults] = useState<{ rows: Record<string, unknown>[]; rowNumbers: number[] } | undefined>()
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()

  const indexUrl = useMemo(() => tableUrl.replace(/\/+$/, '') + '.index', [tableUrl])

  // Load both iceberg tables on mount/url change.
  useEffect(() => {
    let cancelled = false
    queueMicrotask(() => {
      if (cancelled) return
      setLoaded(undefined)
      setResults(undefined)
    })

    async function load() {
      const [mainMetadata, indexMetadata] = await Promise.all([
        icebergMetadata({ tableUrl }),
        icebergMetadata({ tableUrl: indexUrl }),
      ])
      const [source, index] = await Promise.all([
        singleDataFileUrl(mainMetadata),
        singleDataFileUrl(indexMetadata),
      ])
      const rows = await icebergRead({ tableUrl, metadata: mainMetadata })
      const schema = mainMetadata.schemas.find(s => s['schema-id'] === mainMetadata['current-schema-id'])
      if (!schema) throw new Error('current schema not found')
      const columns = schema.fields.map(f => f.name)
      if (cancelled) return
      setLoaded({
        rows,
        columns,
        sourceUrl: source.url,
        sourceByteLength: source.byteLength,
        indexUrl: index.url,
        indexProperties: indexMetadata.properties ?? {},
      })
    }
    load().catch((err: unknown) => { if (!cancelled) setError(err) })
    return () => { cancelled = true }
  }, [tableUrl, indexUrl, setError])

  const isQuerying = query.trim().length > 0

  // Persist the search term to the URL so deep links work.
  useEffect(() => {
    const params = new URLSearchParams(location.search)
    if (isQuerying) params.set('q', query)
    else params.delete('q')
    history.replaceState({}, '', `${location.pathname}?${params}`)
  }, [query, isQuerying])

  // Run parquetFind whenever the query or loaded tables change.
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

      // The parquetindex iceberg table stores parquetindex's required kv
      // metadata as iceberg table properties so the bytes-on-disk look like a
      // normal iceberg data file. Reconstruct the kv-metadata view that
      // queryIndex expects by pulling those keys out of `properties`.
      const indexFile = await asyncBufferFactory({ url: tables.indexUrl })
      const indexMetadata: FileMetaData = await parquetMetadataAsync(indexFile)
      const propKv: KeyValue[] = []
      for (const key of PARQUETINDEX_KV_KEYS) {
        const value = tables.indexProperties[key]
        if (value) propKv.push({ key, value })
      }
      const existingKv = indexMetadata.key_value_metadata ?? []
      const merged = [...existingKv.filter(kv => !PARQUETINDEX_KV_KEYS.includes(kv.key as never)), ...propKv]
      const patchedIndexMetadata: FileMetaData = { ...indexMetadata, key_value_metadata: merged }

      const collected: Record<string, unknown>[] = []
      const rowNumbers: number[] = []
      const rowGen = parquetFind({
        url: tables.sourceUrl,
        query,
        limit: 100,
        asyncBufferFactory,
        indexFile,
        indexMetadata: patchedIndexMetadata,
        signal,
      })
      for await (const row of rowGen) {
        if (signal.aborted) return
        if (collected.length === 0) {
          setFirstRowTime(performance.now() - startTime)
        }
        const index = row.__index__ as number
        delete row.__index__
        rowNumbers.push(index)
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
    const columnDescriptors = loaded.columns.map(name => ({ name }))
    if (results) {
      return arrayDataFrame(results.rows, results.rowNumbers, { columnDescriptors })
    }
    const indices = loaded.rows.map((_, i) => i)
    return arrayDataFrame(loaded.rows, indices, { columnDescriptors })
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
        {loaded && <span>{loaded.rows.length.toLocaleString()} rows</span>}
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
      maxRowNumber={loaded?.rows.length}
      onError={setError}
      renderCellContent={renderCellContent}
    />}
  </>
}
