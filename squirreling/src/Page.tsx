import HighTable, { DataFrame, arrayDataFrame } from 'hightable'
import { FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetReadObjects } from 'hyparquet'
import { executeSql } from 'squirreling'
import { ReactNode, useEffect, useState } from 'react'

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
  const [query, setQuery] = useState<string>('SELECT title FROM table')
  const [queryDf, setQueryDf] = useState<DataFrame>(df)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [rows, setRows] = useState<Record<string, any>[] | undefined>()
  const [sqlError, setSqlError] = useState<string | undefined>()

  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller
    setSqlError(undefined)

    if (query.length > 2) {
      async function updateQuery() {
        console.log(`Running SQL query "${query}"...`)
        setQueryTime(undefined)
        setFirstRowTime(undefined)
        setQueryDf(arrayDataFrame([]))
        let results: Record<string, any>[] = []
        const startTime = performance.now()
        // TODO: Wrap data async from parquet
        const rowGen = executeSql({
          tables: { table: rows ?? [] },
          query,
        })
        for await (const asyncRow of rowGen) {
          const row: Record<string, any> = {}
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
        console.log(`Query result for "${query}" in ${elapsed.toFixed(2)} ms, ${results.length} results`, results)
      }
      void updateQuery().catch((err: unknown) => {
        if (signal.aborted) return
        setSqlError(err instanceof Error ? err.message : String(err))
        console.error(err)
      })
    } else {
      setQueryDf(df)
      setQueryTime(undefined)
      setFirstRowTime(undefined)
    }

    return () => { controller.abort(new Error('Query aborted')) }
  }, [query, df, name, rows, setError])

  // preload parquet data first 100 rows
  useEffect(() => {
    async function fetchData() {
      const asyncBuffer = await asyncBufferFromUrl({ url: name })
      const file = cachedAsyncBuffer(asyncBuffer)
      const rows = await parquetReadObjects({
        file,
        rowEnd: 1000,
      })
      setRows(rows)
    }
    void fetchData().catch(setError)
  }, [name, setError])

  return <>
    <div className='top-header'>
      <span className='file-name'>{name}</span>
      <div className='file-info'>
        {byteLength !== undefined && <span title={byteLength.toLocaleString() + ' bytes'}>{formatFileSize(byteLength)}</span>}
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
