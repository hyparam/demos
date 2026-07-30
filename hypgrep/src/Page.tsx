import HighTable, { CellContentProps } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { AsyncBuffer, FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { parquetFind } from 'hypgrep'
import { ReactNode, useCallback, useEffect, useState } from 'react'

export interface PageProps {
  metadata: FileMetaData
  df: DataFrame
  name: string
  byteLength?: number
  setError: (e: unknown) => void
}

const asyncBufferCache = new Map<string, Promise<AsyncBuffer>>()
function asyncBufferFactory({ url, byteLength }: { url: string; byteLength?: number }): Promise<AsyncBuffer> {
  let cached = asyncBufferCache.get(url)
  if (!cached) {
    cached = asyncBufferFromUrl({ url, byteLength }).then(cachedAsyncBuffer)
    asyncBufferCache.set(url, cached)
  }
  return cached
}

const exampleQueries = ['certain', 'javascript', 'python', 'import (numpy|pandas)']

// A grep query with regex metacharacters that compiles is run as a RegExp;
// anything else is a plain substring.
function parseGrepQuery(input: string): string | RegExp {
  if (!/[[\](){}.*+?^$|\\]/.test(input)) return input
  try {
    return new RegExp(input)
  } catch {
    return input
  }
}

function grepMatch(value: string, rawQuery: string): { index: number; length: number } | undefined {
  const query = parseGrepQuery(rawQuery)
  if (query instanceof RegExp) {
    const match = new RegExp(query.source, query.flags).exec(value)
    return match ? { index: match.index, length: match[0].length } : undefined
  }
  const index = value.toLowerCase().indexOf(rawQuery.toLowerCase())
  return index < 0 ? undefined : { index, length: rawQuery.length }
}

/**
 * Hypgrep demo page
 *
 * @param {Object} props
 * @returns {ReactNode}
 */
export default function Page({ df, name, byteLength, setError }: PageProps): ReactNode {
  const [query, setQuery] = useState<string>('')
  const [queryResultsDf, setQueryResultsDf] = useState<DataFrame>(df)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [indexMetadata, setIndexMetadata] = useState<FileMetaData | undefined>()

  const isQuerying = query.length > 2
  const filteredDf = isQuerying ? queryResultsDf : df
  const displayQueryTime = isQuerying ? queryTime : undefined
  const displayFirstRowTime = isQuerying ? firstRowTime : undefined

  useEffect(() => {
    if (!isQuerying) return
    const controller = new AbortController()
    const { signal } = controller

    async function updateQuery() {
      console.log(`Querying for "${query}"...`)
      setQueryTime(undefined)
      setFirstRowTime(undefined)
      const resultsDf = arrayDataFrame([], [], {
        columnDescriptors: df.columnDescriptors.map(({ name }) => ({ name })),
      })
      const results = resultsDf._array
      const rowNumbers = resultsDf._rowNumbers
      setQueryResultsDf(resultsDf)
      const startTime = performance.now()
      // Query against the hypgrep index
      const url = name
      const rowGen = parquetFind({
        url,
        query: parseGrepQuery(query),
        limit: 20,
        asyncBufferFactory,
        indexMetadata,
        sourceMetadata: df.metadata?.parquet as FileMetaData | undefined,
        signal,
      })
      for await (const row of rowGen) {
        // Extract row numbers from __index__
        if (!results.length) {
          const elapsed = performance.now() - startTime
          console.log(`First result for "${query}" in ${elapsed.toFixed(2)} ms`)
          setFirstRowTime(elapsed)
        }
        rowNumbers?.push(row.__index__ as number)
        delete row.__index__
        results.push(row)
      }
      const elapsed = performance.now() - startTime
      setQueryTime(elapsed)
      console.log(`Query result for "${query}" in ${elapsed.toFixed(2)} ms, ${results.length} results`, results)
    }
    void updateQuery().catch((err: unknown) => {
      if (signal.aborted) return
      setError(err)
    })

    return () => { controller.abort(new Error('Query aborted')) }
  }, [isQuerying, query, indexMetadata, df, name, setError])

  // preload index metadata
  useEffect(() => {
    const url = name.replace(/\.parquet$/i, '.index.parquet')
    asyncBufferFactory({ url })
      .then(buffer => parquetMetadataAsync(buffer))
      .then(setIndexMetadata)
      .catch(setError)
  }, [name, setError])

  const renderCellContent = useCallback(({ cell, stringify }: CellContentProps) => {
    const value: unknown = cell?.value
    if (typeof value !== 'string') return stringify(value)
    const match = grepMatch(value, query)
    if (!match) return stringify(value)
    const { index, length } = match
    const truncateBefore = index > 20 ? '...' : ''
    return <>
      {truncateBefore}
      {value.slice(0, index).slice(-20)}
      <mark>{value.slice(index, index + length)}</mark>
      {value.slice(index + length)}
    </>
  }, [query])

  return <>
    <div className='top-header'>
      {name}
    </div>
    <div className='view-header'>
      {byteLength !== undefined && <span title={byteLength.toLocaleString() + ' bytes'}>{formatFileSize(byteLength)}</span>}
      <span>{df.numRows.toLocaleString()} rows</span>
      <div className="spacer">
        {displayQueryTime !== undefined && <span>query time: {displayQueryTime.toFixed(0)} ms</span>}
        {displayFirstRowTime !== undefined && <span>first result: {displayFirstRowTime.toFixed(0)} ms</span>}
        <input
          type="text"
          placeholder="Search..."
          onChange={e => { setQuery(e.target.value) }}
          value={query}
          style={{ padding: '2px 10px', marginLeft: 'auto', height: '24px' }}
        />
      </div>
    </div>
    {!query && <div className='examples-row'>
      <span className='examples-label'>Try:</span>
      <div className='examples'>
        {exampleQueries.map(example =>
          <button key={example} className='example-chip' onClick={() => { setQuery(example) }}>{example}</button>,
        )}
      </div>
    </div>}
    <HighTable
      focus={false}
      cacheKey={name}
      data={filteredDf}
      onError={setError}
      className="hightable"
      maxRowNumber={df.numRows}
      renderCellContent={renderCellContent}
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
