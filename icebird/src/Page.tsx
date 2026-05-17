import HighTable, { DataFrame } from 'hightable'
import { icebergDataSource, icebergMetadata, icebergQuery } from 'icebird'
import type { Snapshot, TableMetadata } from 'icebird/src/types.js'
import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { AsyncDataSource, extractTables, parseSql } from 'squirreling'
import { HighlightedTextArea } from './HighlightedTextArea.js'
import SnapshotSlider from './SnapshotSlider.js'
import { highlightSql } from './sqlHighlight.js'
import { squirrelingDataFrame } from './squirrelingDataFrame.js'

interface SqlErrorInfo {
  message: string
  positionStart?: number
  positionEnd?: number
}

export interface PageProps {
  databaseUrl: string
  initialQuery?: string
  setError: (e: unknown) => void
}

const DEFAULT_QUERY = 'SELECT * FROM events LIMIT 500'

const empty: DataFrame = {
  columnDescriptors: [],
  numRows: 0,
  getRowNumber: () => undefined,
  getCell: () => undefined,
}

/**
 * Icebird demo viewer page. Resolves every table referenced in the SQL by
 * joining `databaseUrl` with the SQL identifier; icebergQuery loads each
 * table lazily. The snapshot slider applies to the first table referenced
 * in the query — that one is pre-built as a snapshot-pinned dataSource so
 * the slider has something to move.
 */
export default function Page({
  databaseUrl,
  initialQuery,
  setError,
}: PageProps): ReactNode {
  const [query, setQuery] = useState(initialQuery ?? DEFAULT_QUERY)
  const [queryDf, setQueryDf] = useState<DataFrame>(empty)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [runtimeError, setRuntimeError] = useState<SqlErrorInfo | undefined>()
  // Bumped on numrowschange events so we re-read queryDf.numRows in render
  const [, forceUpdate] = useState(0)

  const highlights = useMemo(() => highlightSql(query), [query])

  // Parse upfront so we can derive table refs and the slider's anchor table.
  // Parse failures land in `parseError`; later effects skip if there's no AST.
  const parseResult = useMemo((): {
    parsedQuery?: ReturnType<typeof parseSql>
    refs: string[]
    parseError?: SqlErrorInfo
  } => {
    try {
      const parsed = parseSql({ query })
      return { parsedQuery: parsed, refs: extractTables(parsed) }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      return { refs: [], parseError: { message, positionStart, positionEnd } }
    }
  }, [query])
  const { parsedQuery, refs, parseError } = parseResult
  const sqlError = parseError ?? runtimeError

  const firstRef = refs[0]
  const firstRefUrl = useMemo(
    () => firstRef ? databaseUrl.replace(/\/+$/, '') + '/' + firstRef : undefined,
    [databaseUrl, firstRef],
  )

  // Snapshot metadata for the first-referenced table (drives the slider).
  const [metadata, setMetadata] = useState<TableMetadata>()
  const [snapshots, setSnapshots] = useState<Snapshot[]>()
  const [snapshotId, setSnapshotId] = useState<bigint>()

  useEffect(() => {
    if (!firstRefUrl) return
    let cancelled = false
    queueMicrotask(() => {
      if (cancelled) return
      setMetadata(undefined)
      setSnapshots(undefined)
      setSnapshotId(undefined)
    })
    icebergMetadata({ tableUrl: firstRefUrl })
      .then(md => {
        if (cancelled) return
        if (!md.snapshots?.length) throw new Error('No iceberg snapshots found')
        const sorted = [...md.snapshots].sort((a, b) => a['timestamp-ms'] - b['timestamp-ms'])
        setMetadata(md)
        setSnapshots(sorted)
        const current = md['current-snapshot-id']
        const initial = current ?? sorted[sorted.length - 1]['snapshot-id']
        setSnapshotId(BigInt(initial))
      })
      .catch((err: unknown) => { if (!cancelled) setError(err) })
    return () => { cancelled = true }
  }, [firstRefUrl, setError])

  // Cache one snapshot-pinned dataSource per snapshotId so slider backtracking
  // is free. Reset when the first-ref table or its metadata changes.
  const sourceCache = useRef(new Map<string, Promise<AsyncDataSource>>())
  useEffect(() => {
    sourceCache.current = new Map()
  }, [firstRefUrl, metadata])

  const [firstDataSource, setFirstDataSource] = useState<AsyncDataSource>()

  useEffect(() => {
    if (!firstRefUrl || !metadata || snapshotId === undefined) return
    let cancelled = false
    const cache = sourceCache.current
    const key = snapshotId.toString()
    let promise = cache.get(key)
    if (!promise) {
      promise = icebergDataSource({ tableUrl: firstRefUrl, metadata, snapshotId })
      cache.set(key, promise)
    }
    promise
      .then(source => { if (!cancelled) setFirstDataSource(source) })
      .catch((err: unknown) => {
        // Don't cache failures — let the next attempt retry.
        cache.delete(key)
        if (!cancelled) setError(err)
      })
    return () => { cancelled = true }
  }, [firstRefUrl, metadata, snapshotId, setError])

  const sourceColumns = useMemo(
    () => firstDataSource?.columns ?? [],
    [firstDataSource],
  )

  const handleQueryChange = useCallback((newQuery: string) => {
    setQueryTime(undefined)
    setFirstRowTime(undefined)
    setQuery(newQuery)
    setError(undefined)
    setRuntimeError(undefined)
    const params = new URLSearchParams(location.search)
    if (params.has('key')) {
      if (newQuery && newQuery !== DEFAULT_QUERY) {
        params.set('query', newQuery)
      } else {
        params.delete('query')
      }
      history.replaceState({}, '', `${location.pathname}?${params}`)
    }
  }, [setError])

  // Run the SQL query. Tables map: the first-referenced table is the
  // snapshot-pinned dataSource; every other ref is a URL string that
  // icebergQuery resolves to the latest snapshot on demand.
  useEffect(() => {
    if (parseError || !parsedQuery) {
      queueMicrotask(() => { setQueryDf(empty) })
      return
    }
    if (refs.length && !firstDataSource) return // wait for snapshot-pinned source

    const abortController = new AbortController()
    queueMicrotask(() => {
      if (!abortController.signal.aborted) setQueryDf(empty)
    })

    const tables: Record<string, string | AsyncDataSource> = {}
    for (const ref of refs) {
      tables[ref] = ref === firstRef && firstDataSource
        ? firstDataSource
        : databaseUrl.replace(/\/+$/, '') + '/' + ref
    }

    icebergQuery({ query, tables, signal: abortController.signal })
      .then(results => {
        if (abortController.signal.aborted) return
        const resultsDf = squirrelingDataFrame({
          rowGen: results.rows(),
          query: parsedQuery,
          sourceColumns,
        })
        setQueryDf(resultsDf)
      })
      .catch((err: unknown) => {
        if (abortController.signal.aborted) return
        const message = err instanceof Error ? err.message : String(err)
        const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
        setRuntimeError({ message, positionStart, positionEnd })
        setQueryDf(empty)
      })

    return () => { abortController.abort() }
  }, [query, parsedQuery, parseError, refs, firstRef, firstDataSource, databaseUrl, sourceColumns])

  // Track row count + timing on the active queryDf
  useEffect(() => {
    const target = queryDf.eventTarget
    if (!target) return
    const startTime = performance.now()
    let firstRowTracked = false
    function onNumRowsChange() {
      forceUpdate(c => c + 1)
      if (!firstRowTracked) {
        firstRowTracked = true
        setFirstRowTime(performance.now() - startTime)
      }
    }
    function onResolve() {
      setQueryTime(performance.now() - startTime)
    }
    target.addEventListener('numrowschange', onNumRowsChange)
    target.addEventListener('resolve', onResolve)
    return () => {
      target.removeEventListener('numrowschange', onNumRowsChange)
      target.removeEventListener('resolve', onResolve)
    }
  }, [queryDf])

  return <>
    <div className='top-header'>
      <span className='file-name'>{databaseUrl}</span>
      {snapshots && snapshotId !== undefined && <SnapshotSlider
        snapshots={snapshots}
        value={snapshotId}
        onChange={setSnapshotId}
        rowCount={queryDf === empty ? '?' : queryDf.numRows.toLocaleString()}
      />}
    </div>
    <div className='sql-container'>
      <div className='sql-input-area'>
        <HighlightedTextArea
          value={query}
          onChange={handleQueryChange}
          placeholder='SQL query...'
          className={sqlError ? 'sql-error' : ''}
          highlights={highlights}
          errorStart={sqlError?.positionStart}
          errorEnd={sqlError?.positionEnd}
        />
        <div className='query-stats'>
          {sqlError && <span className='sql-error-msg'>{sqlError.message}</span>}
          <span className='query-times'>
            {queryTime !== undefined && <span>query: {queryTime.toFixed(0)} ms</span>}
            {firstRowTime !== undefined && <span>first: {firstRowTime.toFixed(0)} ms</span>}
          </span>
        </div>
      </div>
    </div>
    <HighTable
      focus={false}
      cacheKey={databaseUrl}
      className='hightable'
      data={queryDf}
      onError={setError}
    />
  </>
}
