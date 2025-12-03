import HighTable, { DataFrame, arrayDataFrame } from 'hightable'
import { FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { AsyncDataSource, executeSql } from 'squirreling'
import { ReactNode, useEffect, useState } from 'react'
import { parquetDataSource } from './parquetDataSource.js'

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
  const [sqlError, setSqlError] = useState(false)

  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller
    setSqlError(false)

    if (query.length > 2) {
      async function updateQuery() {
        if (!table) return
        console.log(`Running SQL query "${query}"...`)
        setQueryTime(undefined)
        setFirstRowTime(undefined)
        setQueryDf(arrayDataFrame([]))
        let results: Record<string, any>[] = []
        const startTime = performance.now()
        // TODO: Wrap data async from parquet
        const rowGen = executeSql({
          tables: { table },
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
        setSqlError(true)
        console.error(err)
      })
    } else {
      setQueryDf(df)
      setQueryTime(undefined)
      setFirstRowTime(undefined)
    }

    return () => { controller.abort(new Error('Query aborted')) }
  }, [query, df, name, table, setError])

  // prepare parquet data source
  useEffect(() => {
    async function fetchData() {
      const asyncBuffer = await asyncBufferFromUrl({ url: name })
      const file = cachedAsyncBuffer(asyncBuffer)
      const metadata = await parquetMetadataAsync(file)
      const table = parquetDataSource(file, metadata)
      setTable(table)
    }
    void fetchData().catch(setError)
  }, [name, setError])

  return <>
    <div className='top-header'>
      {name}
    </div>
    <div className='view-header'>
      {byteLength !== undefined && <span title={byteLength.toLocaleString() + ' bytes'}>{formatFileSize(byteLength)}</span>}
      <span>{df.numRows.toLocaleString()} rows</span>
      <div className="spacer">
        {queryTime !== undefined && <span>query time: {queryTime.toFixed(0)} ms</span>}
        {firstRowTime !== undefined && <span>first result: {firstRowTime.toFixed(0)} ms</span>}
        <input
          type="text"
          placeholder="SQL query..."
          onChange={e => { setQuery(e.target.value) }}
          value={query}
          style={{
            borderColor: sqlError ? 'red' : undefined,
            marginLeft: 'auto',
            padding: '2px 10px',
            height: '24px',
            width: '300px',
          }}
        />
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
