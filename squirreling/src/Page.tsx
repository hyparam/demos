import HighTable, { DataFrame } from 'hightable'
import { FileMetaData, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { AsyncBufferFrom, asyncBufferFrom } from 'hyperparam'
import { compressors } from 'hyparquet-compressors'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'
import { AsyncDataSource, executePlan, parseSql, planSql } from 'squirreling'
import { parquetDataSource } from './parquetDataSource.js'
import { type ByteRange, countingBuffer } from './countingBuffer.js'
import { HighlightedTextArea } from './HighlightedTextArea.js'
import { highlightSql } from './sqlHighlight.js'
import { squirrelingDataFrame } from './squirrelingDataFrame.js'
import ParquetGridMini from './ParquetGridMini.js'

interface SqlErrorInfo {
  message: string
  positionStart?: number
  positionEnd?: number
}

export interface PageProps {
  metadata: FileMetaData
  df: DataFrame
  name: string
  from: AsyncBufferFrom
  byteLength?: number
  setError: (e: unknown) => void
}

/**
 * Squirreling demo page
 * Enter SQL queries to filter the data.
 *
 * @param {Object} props
 * @returns {ReactNode}
 */
export default function Page({ metadata, df, name, from, byteLength, setError }: PageProps): ReactNode {
  const [query, setQuery] = useState<string>('SELECT * FROM table LIMIT 500')
  const [queryDf, setQueryDf] = useState<DataFrame>(df)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [table, setTable] = useState<AsyncDataSource | undefined>()
  const [sqlError, setSqlError] = useState<SqlErrorInfo | undefined>()
  const [networkBytes, setNetworkBytes] = useState<number>(0)
  const [downloadedRanges, setDownloadedRanges] = useState<ByteRange[]>([])

  // Compute syntax highlighting
  const highlights = useMemo(() => highlightSql(query), [query])

  // Wrap setQuery to clear errors and timing on query change
  const handleQueryChange = useCallback((newQuery: string) => {
    setQueryTime(undefined)
    setFirstRowTime(undefined)
    setQuery(newQuery)
    setError(undefined)
    setSqlError(undefined)
    if (newQuery.length <= 2) {
      setQueryDf(df)
    }
  }, [setError, df])

  useEffect(() => {
    if (query.length <= 2 || !table) {
      return
    }

    const abortController = new AbortController()

    try {
      // parse the query and execute it
      const parsedQuery = parseSql({ query })
      const plan = planSql({ query: parsedQuery })
      const rowGen = executePlan({
        plan,
        context: {
          tables: { table },
          signal: abortController.signal,
        },
      })
      const sourceColumns = df.columnDescriptors.map(c => c.name)
      const resultsDf = squirrelingDataFrame({
        rowGen,
        columns: parsedQuery.columns,
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
          setQueryDf(df)
        }
      })
    }

    return () => {
      abortController.abort()
    }
  }, [query, df, table])

  useEffect(() => {
    if (query.length <= 2 || !table || sqlError) return
    const { eventTarget } = queryDf
    if (!eventTarget) return

    const startTime = performance.now()
    let firstRowTracked = false
    function handleNumRowsChange() {
      if (!firstRowTracked) {
        firstRowTracked = true
        setFirstRowTime(performance.now() - startTime)
      }
    }
    function handleResolve() {
      setQueryTime(performance.now() - startTime)
    }

    eventTarget.addEventListener('numrowschange', handleNumRowsChange)
    eventTarget.addEventListener('resolve', handleResolve)

    return () => {
      eventTarget.removeEventListener('numrowschange', handleNumRowsChange)
      eventTarget.removeEventListener('resolve', handleResolve)
    }
  }, [query, table, queryDf, sqlError])

  // prepare parquet data source
  useEffect(() => {
    async function fetchData() {
      const asyncBuffer = await asyncBufferFrom(from)
      const counted = countingBuffer(asyncBuffer, ranges => {
        const totalBytes = ranges.reduce((sum, r) => sum + (r.end - r.start), 0)
        setNetworkBytes(totalBytes)
        setDownloadedRanges([...ranges])
      })
      const file = cachedAsyncBuffer(counted)
      const metadata = await parquetMetadataAsync(file)
      const table = parquetDataSource(file, metadata, compressors)
      setTable(table)
    }
    void fetchData().catch(setError)
  }, [from, setError])

  return <>
    <div className='top-header'>
      <span className='file-name'>{name}</span>
      <div className='file-info'>
        {byteLength !== undefined && <span title={`${networkBytes.toLocaleString()} / ${byteLength.toLocaleString()} bytes`}>{formatFileSize(networkBytes)} / {formatFileSize(byteLength)}</span>}
        <span>{df.numRows.toLocaleString()} rows</span>
      </div>
    </div>
    <div className='sql-container'>
      <div className='sql-input-area'>
        <HighlightedTextArea
          value={query}
          onChange={handleQueryChange}
          placeholder="SQL query..."
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
      <ParquetGridMini metadata={metadata} downloadedRanges={downloadedRanges} />
    </div>
    <HighTable
      focus={false}
      cacheKey={name}
      data={queryDf}
      onError={setError}
      className="hightable"
      maxRowNumber={df.numRows}
    />
  </>
}

/**
 * Returns the file size in human readable format.
 *
 * @param {number} bytes file size in bytes
 * @returns {string} formatted file size string
 */
function formatFileSize(bytes: number): string {
  const sizes = ['b', 'kb', 'mb', 'gb', 'tb']
  if (bytes === 0) return '0 b'
  const i = Math.floor(Math.log2(bytes) / 10)
  if (i === 0) return `${bytes} b`
  const base = bytes / Math.pow(1024, i)
  return `${base < 10 ? base.toFixed(1) : Math.round(base)} ${sizes[i]}`
}
