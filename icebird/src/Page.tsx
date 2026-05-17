import HighTable, { DataFrame } from 'hightable'
import { icebergQuery } from 'icebird'
import type { Snapshot, TableMetadata } from 'icebird/src/types.js'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'
import { AsyncDataSource, parseSql } from 'squirreling'
import { quoteIdentifier } from './database.js'
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
  tableUrl: string
  tableName: string
  metadata: TableMetadata
  dataSource: AsyncDataSource
  snapshots: Snapshot[]
  snapshotId: bigint
  setSnapshotId: (id: bigint) => void
  initialQuery?: string
  setError: (e: unknown) => void
}

const empty: DataFrame = {
  columnDescriptors: [],
  numRows: 0,
  getRowNumber: () => undefined,
  getCell: () => undefined,
}

/**
 * Icebird demo viewer page. Executes SQL via icebergQuery against a pre-built
 * icebergDataSource that is pinned to the selected snapshot id.
 */
export default function Page({
  tableUrl,
  tableName,
  metadata,
  dataSource,
  snapshots,
  snapshotId,
  setSnapshotId,
  initialQuery,
  setError,
}: PageProps): ReactNode {
  const name = metadata.location
  const sourceColumns = useMemo(() => dataSource.columns, [dataSource])
  const defaultQuery = useMemo(
    () => `SELECT * FROM ${quoteIdentifier(tableName)} LIMIT 500`,
    [tableName],
  )

  const [query, setQuery] = useState(initialQuery ?? defaultQuery)
  const [queryDf, setQueryDf] = useState<DataFrame>(empty)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [sqlError, setSqlError] = useState<SqlErrorInfo | undefined>()
  // Bumped on numrowschange events so we re-read queryDf.numRows in render
  const [, forceUpdate] = useState(0)

  const highlights = useMemo(() => highlightSql(query), [query])

  const handleQueryChange = useCallback((newQuery: string) => {
    setQueryTime(undefined)
    setFirstRowTime(undefined)
    setQuery(newQuery)
    setError(undefined)
    setSqlError(undefined)
    const params = new URLSearchParams(location.search)
    if (params.has('key')) {
      if (newQuery && newQuery !== defaultQuery) {
        params.set('query', newQuery)
      } else {
        params.delete('query')
      }
      history.replaceState({}, '', `${location.pathname}?${params}`)
    }
  }, [setError, defaultQuery])

  // Run the SQL query through icebergQuery against the pinned data source.
  useEffect(() => {
    if (query.length <= 2) {
      queueMicrotask(() => { setQueryDf(empty) })
      return
    }
    const abortController = new AbortController()
    // Parse separately so we can derive column descriptors and surface
    // position info on parse errors. icebergQuery parses internally too;
    // parsing twice is cheap and keeps the error path consistent.
    let parsedQuery
    try {
      parsedQuery = parseSql({ query })
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      queueMicrotask(() => {
        if (!abortController.signal.aborted) {
          setSqlError({ message, positionStart, positionEnd })
          setQueryDf(empty)
        }
      })
      return () => { abortController.abort() }
    }

    // Clear the displayed row count immediately so the previous snapshot's
    // total doesn't linger while the new query is loading.
    queueMicrotask(() => {
      if (!abortController.signal.aborted) setQueryDf(empty)
    })

    icebergQuery({
      query,
      tables: { [tableName]: dataSource },
      signal: abortController.signal,
    }).then(results => {
      if (abortController.signal.aborted) return
      const resultsDf = squirrelingDataFrame({
        rowGen: results.rows(),
        query: parsedQuery,
        sourceColumns,
      })
      setQueryDf(resultsDf)
    }).catch((err: unknown) => {
      if (abortController.signal.aborted) return
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      setSqlError({ message, positionStart, positionEnd })
      setQueryDf(empty)
    })

    return () => { abortController.abort() }
  }, [query, tableName, dataSource, sourceColumns])

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
      <span className='file-name'>{name}</span>
      <SnapshotSlider
        snapshots={snapshots}
        value={snapshotId}
        onChange={setSnapshotId}
        rowCount={queryDf === empty ? '?' : queryDf.numRows.toLocaleString()}
      />
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
      cacheKey={tableUrl}
      className='hightable'
      data={queryDf}
      onError={setError}
    />
  </>
}
