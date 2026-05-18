import HighTable, { CellContentProps, sortableDataFrame } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { AsyncBuffer, FileMetaData, KeyValue, parquetMetadataAsync } from 'hyparquet'
import { cachingResolver, icebergDataSource, icebergManifests, icebergMetadata, urlResolver } from 'icebird'
import { translateS3Url } from 'icebird/src/fetch.js'
import { splitManifestEntries } from 'icebird/src/manifest.js'
import type { ManifestList } from 'icebird/src/manifest.js'
import type { ManifestEntry } from 'icebird/src/types.js'
import { parquetFind } from 'parquetindex'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'
import { icebergDataFrame } from './icebirdAdapter.js'

const PARQUETINDEX_KV_KEYS = [
  'parquetindex.version',
  'parquetindex.block_size',
  'parquetindex.text_columns',
  'parquetindex.source_rows',
  'parquetindex.source_bytelength',
] as const

interface LoadedTables {
  df: DataFrame
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

const loadCache = new Map<string, Promise<LoadedTables>>()

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

function oneDataFile(manifests: ManifestList): ManifestEntry {
  const { dataEntries } = splitManifestEntries(manifests)
  if (dataEntries.length === 0) throw new Error('no data files in iceberg table')
  if (dataEntries.length > 1) throw new Error('icebird-grep demo expects exactly one data file per table')
  return dataEntries[0]
}

/**
 * Build the demo state. Everything routes through a single `cachingResolver`,
 * so even though the high-level icebird APIs each load metadata + manifests
 * themselves, the underlying avro/parquet bytes are fetched once and reused.
 *
 * Specifically: `icebergDataSource` internally calls `icebergManifests`, then
 * we also call `icebergManifests` to discover the data file URLs for
 * `parquetFind` — but the second call hits the resolver cache for the same
 * manifest-list + manifest avros, so it's free.
 */
async function doLoadTables(tableUrl: string): Promise<LoadedTables> {
  const indexTableUrl = tableUrl.replace(/\/+$/, '') + '.index'
  const resolver = cachingResolver(urlResolver())

  const [mainMd, indexMd] = await Promise.all([
    icebergMetadata({ tableUrl, resolver }),
    icebergMetadata({ tableUrl: indexTableUrl, resolver }),
  ])

  // Native icebird DataSource for the display. It loads manifests + delete
  // maps once at construction (via the caching resolver) and streams rows
  // lazily through scan().
  const [dataSource, mainManifests, indexManifests] = await Promise.all([
    icebergDataSource({ tableUrl, metadata: mainMd, resolver }),
    icebergManifests({ metadata: mainMd, resolver }),
    icebergManifests({ metadata: indexMd, resolver }),
  ])

  if (dataSource.numRows === undefined) {
    throw new Error('icebird-grep demo expects a table without row-level deletes')
  }

  const mainEntry = oneDataFile(mainManifests).data_file
  const indexEntry = oneDataFile(indexManifests).data_file
  const sourceUrl = translateS3Url(mainEntry.file_path)
  const sourceByteLength = Number(mainEntry.file_size_in_bytes)
  const indexByteLength = Number(indexEntry.file_size_in_bytes)

  // Resolve through the same caching resolver — the source AsyncBuffer is the
  // exact one the data source's scan() uses, and the index buffer is shared
  // between footer-read and parquetFind's range reads.
  const [sourceFile, indexFile] = await Promise.all([
    resolver.reader(mainEntry.file_path, sourceByteLength),
    resolver.reader(indexEntry.file_path, indexByteLength),
  ])
  const [sourceMetadata, rawIndexMetadata] = await Promise.all([
    parquetMetadataAsync(sourceFile),
    parquetMetadataAsync(indexFile),
  ])

  const df = sortableDataFrame(icebergDataFrame(dataSource))

  // parquetindex's kv metadata (block_size, text_columns, source_rows,
  // source_bytelength, version) lives in the iceberg table's `properties` so
  // the data file is a normal iceberg parquet. Splice those back into the
  // parquet metadata so queryIndex can find them.
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
    numRows: dataSource.numRows,
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
      const columnDescriptors = loaded.df.columnDescriptors
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
