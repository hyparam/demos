import HighTable, { DataFrame } from 'hightable'
import { FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { ReactNode, useEffect, useMemo, useState } from 'react'
import { AsyncDataSource, executeSql, parseSql } from 'squirreling'
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
  byteLength?: number
  setError: (e: unknown) => void
}

/**
 * Squireeling demo page
 * Enter SQL queries to filter the data.
 *
 * @param {Object} props
 * @returns {ReactNode}
 */
export default function Page({ metadata, df, name, byteLength, setError }: PageProps): ReactNode {
  const [query, setQuery] = useState<string>('SELECT * FROM table LIMIT 5')
  const [queryDf, setQueryDf] = useState<DataFrame>(df)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [table, setTable] = useState<AsyncDataSource | undefined>()
  const [sqlError, setSqlError] = useState<SqlErrorInfo | undefined>()
  const [networkBytes, setNetworkBytes] = useState<number>(0)
  const [downloadedRanges, setDownloadedRanges] = useState<ByteRange[]>([])

  // Compute syntax highlighting
  const highlights = useMemo(() => highlightSql(query), [query])

  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller
    setSqlError(undefined)

    if (query.length > 2) {
      if (!table) return
      console.log(`Running SQL query "${query}"...`)
      setQueryTime(undefined)
      setFirstRowTime(undefined)

      try {
        const parsedQuery = parseSql({ query })
        const rowGen = executeSql({
          tables: { table },
          query: parsedQuery,
        })
        const resultsDf = squirrelingDataFrame(rowGen)

        // Track timing via events
        const startTime = performance.now()
        let firstRowTracked = false
        resultsDf.eventTarget?.addEventListener('numrowschange', () => {
          if (!firstRowTracked) {
            firstRowTracked = true
            setFirstRowTime(performance.now() - startTime)
          }
        })
        resultsDf.eventTarget?.addEventListener('resolve', () => {
          setQueryTime(performance.now() - startTime)
        })

        setQueryDf(resultsDf)
      } catch (err: unknown) {
        if (signal.aborted) return
        const message = err instanceof Error ? err.message : String(err)
        const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
        setSqlError({ message, positionStart, positionEnd })
        console.warn('SQL error:', err)
      }
    } else {
      setQueryDf(df)
      setQueryTime(undefined)
      setFirstRowTime(undefined)
    }

    return () => { controller.abort(new Error('Query aborted')) }
  }, [query, df, table])

  // prepare parquet data source
  useEffect(() => {
    async function fetchData() {
      const asyncBuffer = await asyncBufferFromUrl({ url: name })
      const counted = countingBuffer(asyncBuffer, ranges => {
        // Calculate total bytes from all ranges
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
  }, [name, setError])

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
          onChange={setQuery}
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
