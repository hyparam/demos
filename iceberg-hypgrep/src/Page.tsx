import HighTable, { CellContentProps, sortableDataFrame } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { AsyncBuffer, FileMetaData, KeyValue, parquetMetadataAsync } from 'hyparquet'
import { cachingResolver, icebergDataSource, icebergManifests, icebergMetadata, urlResolver } from 'icebird'
import { translateS3Url } from 'icebird/src/fetch.js'
import { splitManifestEntries } from 'icebird/src/manifest.js'
import type { ManifestList } from 'icebird/src/manifest.js'
import type { ManifestEntry } from 'icebird/src/types.js'
import { parquetFind } from 'hypgrep'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'
import { icebergDataFrame } from './icebirdAdapter.js'

const HYPGREP_KV_KEYS = [
  'hypgrep.version',
  'hypgrep.block_size',
  'hypgrep.text_columns',
] as const

interface FilePair {
  sourceUrl: string
  sourceFile: AsyncBuffer
  sourceMetadata: FileMetaData
  indexFile: AsyncBuffer
  patchedIndexMetadata: FileMetaData
  rowOffset: number
  rowCount: number
}

interface LoadedTables {
  df: DataFrame
  numRows: number
  pairs: FilePair[]
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

/** Sort dataEntries by sequence_number ascending so file i is the i'th append. */
function orderedDataEntries(manifests: ManifestList): ManifestEntry[] {
  const { dataEntries } = splitManifestEntries(manifests)
  return dataEntries.slice().sort((a, b) => {
    const sa = BigInt(a.sequence_number ?? 0)
    const sb = BigInt(b.sequence_number ?? 0)
    return sa < sb ? -1 : sa > sb ? 1 : 0
  })
}

/**
 * Build the demo state. The main and index iceberg tables each carry N data
 * files written in lockstep — main file i ↔ index file i (sorted by
 * sequence_number). For each pair we synthesize the parquet kv_metadata that
 * hypgrep's queryIndex needs: the constant fields come from the index table's
 * `properties`, the per-file fields (source_rows, source_bytelength) come
 * from the corresponding main-table manifest entry.
 */
async function doLoadTables(tableUrl: string): Promise<LoadedTables> {
  const indexTableUrl = tableUrl.replace(/\/+$/, '') + '.index'
  const resolver = cachingResolver(urlResolver())

  const [mainMd, indexMd] = await Promise.all([
    icebergMetadata({ tableUrl, resolver }),
    icebergMetadata({ tableUrl: indexTableUrl, resolver }),
  ])

  const [dataSource, mainManifests, indexManifests] = await Promise.all([
    icebergDataSource({ tableUrl, metadata: mainMd, resolver }),
    icebergManifests({ metadata: mainMd, resolver }),
    icebergManifests({ metadata: indexMd, resolver }),
  ])

  if (dataSource.numRows === undefined) {
    throw new Error('iceberg-hypgrep demo expects a table without row-level deletes')
  }

  const mainEntries = orderedDataEntries(mainManifests)
  const indexEntries = orderedDataEntries(indexManifests)
  if (mainEntries.length === 0) throw new Error('no data files in main table')
  if (mainEntries.length !== indexEntries.length) {
    throw new Error(`main/index data-file count mismatch: ${mainEntries.length} vs ${indexEntries.length}`)
  }

  // Constant hypgrep kv from index table properties.
  const props = indexMd.properties ?? {}
  const constKv: KeyValue[] = []
  for (const k of HYPGREP_KV_KEYS) {
    const v = props[k]
    if (v) constKv.push({ key: k, value: v })
  }

  // Build all pairs in parallel (resolver + parquet metadata fetches).
  let rowOffset = 0
  const pairPromises = mainEntries.map(async (mainEntry, i) => {
    const myOffset = rowOffset
    const rowCount = Number(mainEntry.data_file.record_count)
    rowOffset += rowCount
    const indexEntry = indexEntries[i]
    const mainBytes = Number(mainEntry.data_file.file_size_in_bytes)
    const indexBytes = Number(indexEntry.data_file.file_size_in_bytes)
    const sourceUrl = translateS3Url(mainEntry.data_file.file_path)

    const [sourceFile, indexFile] = await Promise.all([
      resolver.reader(mainEntry.data_file.file_path, mainBytes),
      resolver.reader(indexEntry.data_file.file_path, indexBytes),
    ])
    const [sourceMetadata, rawIndexMetadata] = await Promise.all([
      parquetMetadataAsync(sourceFile),
      parquetMetadataAsync(indexFile),
    ])

    // Per-file hypgrep kv: source_rows and source_bytelength come from the
    // main-table manifest, not the index parquet itself.
    const perFileKv: KeyValue[] = [
      { key: 'hypgrep.source_rows', value: String(rowCount) },
      { key: 'hypgrep.source_bytelength', value: String(mainBytes) },
    ]
    const existingKv = rawIndexMetadata.key_value_metadata ?? []
    const allKeys = new Set([...HYPGREP_KV_KEYS, 'hypgrep.source_rows', 'hypgrep.source_bytelength'])
    const patchedIndexMetadata: FileMetaData = {
      ...rawIndexMetadata,
      key_value_metadata: [
        ...existingKv.filter(kv => !allKeys.has(kv.key)),
        ...constKv,
        ...perFileKv,
      ],
    }

    const pair: FilePair = {
      sourceUrl,
      sourceFile,
      sourceMetadata,
      indexFile,
      patchedIndexMetadata,
      rowOffset: myOffset,
      rowCount,
    }
    return pair
  })
  const pairs = await Promise.all(pairPromises)

  const df = sortableDataFrame(icebergDataFrame(dataSource))

  return {
    df,
    numRows: dataSource.numRows,
    pairs,
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

  // hypgrep's tokenizer drops tokens shorter than 2 chars, so a single-letter
  // query produces zero search terms and the index returns no matches. Fall
  // back to showing the unfiltered table in that case.
  const isQuerying = /[a-zA-Z0-9]{2,}/.test(query)

  useEffect(() => {
    const params = new URLSearchParams(location.search)
    if (query) params.set('q', query)
    else params.delete('q')
    history.replaceState({}, '', `${location.pathname}?${params}`)
  }, [query])

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
      const LIMIT = 100

      // Search each (main, index) pair in append order — earliest snapshot
      // first — and stop once we have LIMIT matches.
      for (const pair of tables.pairs) {
        signal.throwIfAborted()
        if (collected.length >= LIMIT) break
        const rowGen = parquetFind({
          query,
          url: pair.sourceUrl,
          limit: LIMIT - collected.length,
          sourceFile: pair.sourceFile,
          sourceMetadata: pair.sourceMetadata,
          indexFile: pair.indexFile,
          indexMetadata: pair.patchedIndexMetadata,
          signal,
        })
        for await (const row of rowGen) {
          if (signal.aborted) return
          if (collected.length === 0) {
            setFirstRowTime(performance.now() - startTime)
          }
          const localIdx = row.__index__ as number
          delete row.__index__
          rowNumbers.push(pair.rowOffset + localIdx)
          collected.push(row)
          if (collected.length >= LIMIT) break
        }
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
