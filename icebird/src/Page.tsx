import HighTable, { DataFrame } from 'hightable'
import type { TableMetadata } from 'icebird/src/types.js'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'
import { AsyncDataSource, executePlan, parseSql, planSql } from 'squirreling'
import { HighlightedTextArea } from './HighlightedTextArea.js'
import { highlightSql } from './sqlHighlight.js'
import { squirrelingDataFrame } from './squirrelingDataFrame.js'
import VersionSlider from './VersionSlider.js'

interface SqlErrorInfo {
  message: string
  positionStart?: number
  positionEnd?: number
}

export interface PageProps {
  tableUrl: string
  metadata: TableMetadata
  dataSource: AsyncDataSource
  versions: string[]
  version: string
  setVersion: (version: string) => void
  initialQuery?: string
  setError: (e: unknown) => void
}

const DEFAULT_QUERY = 'SELECT * FROM table LIMIT 500'

const empty: DataFrame = {
  columnDescriptors: [],
  numRows: 0,
  getRowNumber: () => undefined,
  getCell: () => undefined,
}

/**
 * Icebird demo viewer page. Supports SQL queries against the Iceberg table
 * via squirreling executePlan over an icebergDataSource.
 */
export default function Page({
  tableUrl,
  metadata,
  dataSource,
  versions,
  version,
  setVersion,
  initialQuery,
  setError,
}: PageProps): ReactNode {
  const name = metadata.location
  const sourceColumns = useMemo(() => dataSource.columns, [dataSource])

  const [query, setQuery] = useState(initialQuery ?? DEFAULT_QUERY)
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
      if (newQuery && newQuery !== DEFAULT_QUERY) {
        params.set('query', newQuery)
      } else {
        params.delete('query')
      }
      history.replaceState({}, '', `${location.pathname}?${params}`)
    }
  }, [setError])

  // Parse and execute the SQL query on every change of query/dataSource
  useEffect(() => {
    if (query.length <= 2) {
      queueMicrotask(() => { setQueryDf(empty) })
      return
    }
    // We use parseSql + planSql + executePlan here, mirroring the path that
    // icebergQuery (icebird 0.5.0) takes internally. icebergQuery itself
    // requires a REST catalog; this demo uses tableUrl, so we skip that step.
    const abortController = new AbortController()
    try {
      const parsedQuery = parseSql({ query })
      const plan = planSql({ query: parsedQuery })
      const results = executePlan({
        plan,
        context: {
          tables: { table: dataSource },
          signal: abortController.signal,
        },
      })
      const resultsDf = squirrelingDataFrame({
        rowGen: results.rows(),
        query: parsedQuery,
        sourceColumns,
      })
      queueMicrotask(() => {
        if (!abortController.signal.aborted) {
          setQueryDf(resultsDf)
        }
      })
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      queueMicrotask(() => {
        if (!abortController.signal.aborted) {
          setSqlError({ message, positionStart, positionEnd })
          setQueryDf(empty)
        }
      })
    }
    return () => { abortController.abort() }
  }, [query, dataSource, sourceColumns])

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
      <div className='file-info'>
        <span>{queryDf.numRows.toLocaleString()} rows</span>
      </div>
    </div>
    <div className='view-header'>
      <VersionSlider
        versions={versions}
        value={version}
        onChange={setVersion}
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
