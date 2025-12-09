import HighTable, { DataFrame, arrayDataFrame } from 'hightable'
import { FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { AsyncDataSource, executeSql } from 'squirreling'
import { ReactNode, useEffect, useState } from 'react'
import { parquetDataSource } from './parquetDataSource.js'
import { countingBuffer } from './countingBuffer.js'

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
export default function Page({ df, name, byteLength, setError }: PageProps): ReactNode {
  const [query, setQuery] = useState<string>('SELECT * FROM table LIMIT 5')
  const [queryDf, setQueryDf] = useState<DataFrame>(df)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [table, setTable] = useState<AsyncDataSource | undefined>()
  const [sqlError, setSqlError] = useState<string | undefined>()
  const [networkBytes, setNetworkBytes] = useState<number>(0)
  const [countedBuffer, setCountedBuffer] = useState<ReturnType<typeof countingBuffer> | undefined>()

  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller
    setSqlError(undefined)

    if (query.length > 2) {
      async function updateQuery() {
        if (!table) return
        console.log(`Running SQL query "${query}"...`)
        setQueryTime(undefined)
        setFirstRowTime(undefined)
        setQueryDf(arrayDataFrame([]))
        let results: Record<string, unknown>[] = []
        const startTime = performance.now()
        // TODO: Wrap data async from parquet
        const rowGen = executeSql({
          tables: { table },
          query,
        })
        for await (const asyncRow of rowGen) {
          const row: Record<string, unknown> = {}
          for (const [key, value] of Object.entries(asyncRow)) {
            row[key] = await value()
          }
          if (!results.length) {
            const elapsed = performance.now() - startTime
            console.log(`First result for "${query}" in ${elapsed.toFixed(2)} ms`, row)
            setFirstRowTime(elapsed)
            const resultsDf = arrayDataFrame([row])
            results = resultsDf._array
            setQueryDf(resultsDf)
          } else {
            results.push(row)
          }
        }
        const elapsed = performance.now() - startTime
        setQueryTime(elapsed)
        if (countedBuffer) setNetworkBytes(countedBuffer.bytes)
        console.log(`Query result for "${query}" in ${elapsed.toFixed(2)} ms, ${results.length} results`, results)
      }
      void updateQuery().catch((err: unknown) => {
        if (signal.aborted) return
        setSqlError(err instanceof Error ? err.message : String(err))
        console.warn('SQL error:', err)
      })
    } else {
      setQueryDf(df)
      setQueryTime(undefined)
      setFirstRowTime(undefined)
    }

    return () => { controller.abort(new Error('Query aborted')) }
  }, [query, df, name, table, setError, countedBuffer])

  // prepare parquet data source
  useEffect(() => {
    async function fetchData() {
      const asyncBuffer = await asyncBufferFromUrl({ url: name })
      const counted = countingBuffer(asyncBuffer)
      setCountedBuffer(counted)
      const file = cachedAsyncBuffer(counted)
      const metadata = await parquetMetadataAsync(file)
      const table = parquetDataSource(file, metadata)
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
      <textarea
        className={`sql-input ${sqlError ? 'sql-error' : ''}`}
        placeholder="SQL query..."
        onChange={e => { setQuery(e.target.value) }}
        value={query}
      />
      <div className='query-stats'>
        {sqlError && <span className='sql-error-msg'>{sqlError}</span>}
        {queryTime !== undefined && <span>query: {queryTime.toFixed(0)} ms</span>}
        {firstRowTime !== undefined && <span>first: {firstRowTime.toFixed(0)} ms</span>}
      </div>
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
