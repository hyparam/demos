import HighTable, { CellContentProps } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { AsyncBuffer, FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import { parquetFind } from 'parquetindex'
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

/**
 * Parquetindex demo page
 * Try "Vongphachanh" or "gratianopolitanus" as a search term
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
      // Query against the parquetindex
      const url = name
      const rowGen = parquetFind({
        url,
        query,
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
    // Find first keyword match and highlight it
    const queryKeys = query.split(' ').map(q => q.toLowerCase())
    const value: unknown = cell?.value
    if (typeof value === 'string' && queryKeys.length > 0) {
      const lowerValue = value.toLowerCase()
      let firstIndex = -1
      let firstLength = 0
      for (const q of queryKeys) {
        const index = lowerValue.indexOf(q)
        if (index >= 0 && (firstIndex === -1 || index < firstIndex)) {
          firstIndex = index
          firstLength = q.length
        }
      }
      if (firstIndex >= 0) {
        const truncateBefore = firstIndex > 20 ? '...' : ''
        return <>
          {truncateBefore}
          {value.slice(0, firstIndex).slice(-20)}
          <mark>{value.slice(firstIndex, firstIndex + firstLength)}</mark>
          {value.slice(firstIndex + firstLength)}
        </>
      }
    }
    return stringify(value)
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
