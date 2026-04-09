import { FileMetaData, cachedAsyncBuffer, parquetMetadataAsync } from 'hyparquet'
import type { Geometry } from 'hyparquet/src/types.js'
import { AsyncBufferFrom, asyncBufferFrom } from 'hyperparam'
import { compressors } from 'hyparquet-compressors'
import { ReactNode, useCallback, useEffect, useMemo, useState } from 'react'
import { AsyncDataSource, AsyncRow, cachedDataSource, executePlan, parseSql, planSql } from 'squirreling'
import { parquetDataSource } from './parquetDataSource.js'
import { countingBuffer } from './countingBuffer.js'
import { HighlightedTextArea } from './HighlightedTextArea.js'
import { highlightSql } from './sqlHighlight.js'
import LeafletMap, { MapFeature } from './LeafletMap.js'

const exampleQueries = [
  {
    label: 'Seattle',
    query: 'SELECT * FROM table\nWHERE ST_WITHIN(\n  geometry,\n  ST_MAKEENVELOPE(-122.46, 47.48, -122.22, 47.73)\n)\nLIMIT 1000',
  },
  {
    label: 'Austin, TX',
    query: 'SELECT * FROM table\nWHERE ST_WITHIN(\n  geometry,\n  ST_GEOMFROMTEXT(\'POLYGON((-97.8 30.2, -97.8 30.4, -97.6 30.4, -97.6 30.2, -97.8 30.2))\')\n)\nLIMIT 2000',
  },
  {
    label: 'Manhattan, NYC',
    query: 'SELECT * FROM table\nWHERE ST_WITHIN(\n  geometry,\n  ST_MAKEENVELOPE(-74.02, 40.7, -73.93, 40.8)\n)\nLIMIT 2000',
  },
  {
    label: 'San Francisco',
    query: 'SELECT * FROM table\nWHERE ST_WITHIN(\n  geometry,\n  ST_MAKEENVELOPE(-122.52, 37.7, -122.35, 37.82)\n)\nLIMIT 2000',
  },
]

interface SqlErrorInfo {
  message: string
  positionStart?: number
  positionEnd?: number
}

export interface PageProps {
  metadata: FileMetaData
  name: string
  from: AsyncBufferFrom
  byteLength?: number
  setError: (e: unknown) => void
}

/**
 * Squirreling GIS demo page.
 * Enter SQL queries to filter geospatial data and view results on a map.
 */
export default function Page({ metadata, name, from, byteLength, setError }: PageProps): ReactNode {
  const [query, setQuery] = useState<string>(exampleQueries[0].query)
  const [features, setFeatures] = useState<MapFeature[]>([])
  const [featureCount, setFeatureCount] = useState(0)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [table, setTable] = useState<AsyncDataSource | undefined>()
  const [sqlError, setSqlError] = useState<SqlErrorInfo | undefined>()
  const [networkBytes, setNetworkBytes] = useState<number>(0)

  const numRows = metadata.row_groups.reduce((sum, rg) => sum + Number(rg.num_rows), 0)

  // Compute syntax highlighting
  const highlights = useMemo(() => highlightSql(query), [query])

  const handleQueryChange = useCallback((newQuery: string) => {
    setQueryTime(undefined)
    setQuery(newQuery)
    setError(undefined)
    setSqlError(undefined)
  }, [setError])

  // Execute query and collect features
  useEffect(() => {
    if (query.length <= 2 || !table) return

    const abortController = new AbortController()

    try {
      const tables = { table }
      const parsedQuery = parseSql({ query })
      const plan = planSql({ query: parsedQuery, tables })
      const result = executePlan({
        plan,
        context: {
          tables,
          signal: abortController.signal,
        },
      })

      const startTime = performance.now()

      void collectFeatures(result.rows(), abortController.signal)
        .then(result => {
          if (!abortController.signal.aborted) {
            setFeatures(result)
            setFeatureCount(result.length)
            setQueryTime(performance.now() - startTime)
          }
        })
        .catch((err: unknown) => {
          if (!abortController.signal.aborted) {
            setError(err)
          }
        })
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      queueMicrotask(() => {
        if (!abortController.signal.aborted) {
          setSqlError({ message, positionStart, positionEnd })
          setFeatures([])
          setFeatureCount(0)
        }
      })
    }

    return () => {
      abortController.abort()
    }
  }, [query, table, setError])

  // Prepare parquet data source
  useEffect(() => {
    async function fetchData() {
      const asyncBuffer = await asyncBufferFrom(from)
      const counted = countingBuffer(asyncBuffer, ranges => {
        const totalBytes = ranges.reduce((sum, r) => sum + (r.end - r.start), 0)
        setNetworkBytes(totalBytes)
      })
      const file = cachedAsyncBuffer(counted)
      const metadata = await parquetMetadataAsync(file)
      const table = parquetDataSource(file, metadata, compressors)
      const cached = cachedDataSource(table)
      setTable(cached)
    }
    void fetchData().catch(setError)
  }, [from, setError])

  return <>
    <div className='top-header'>
      <span className='file-name'>{name}</span>
      <div className='file-info'>
        {byteLength !== undefined && <span title={`${networkBytes.toLocaleString()} / ${byteLength.toLocaleString()} bytes`}>{formatFileSize(networkBytes)} / {formatFileSize(byteLength)}</span>}
        <span>{numRows.toLocaleString()} rows</span>
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
            {featureCount > 0 && <span>{featureCount.toLocaleString()} features</span>}
            {queryTime !== undefined && <span>query: {queryTime.toFixed(0)} ms</span>}
          </span>
        </div>
      </div>
      <div className='example-queries'>
        {exampleQueries.map(({ label, query: ex }) => {
          return (
            <button key={label} className={query === ex ? 'active' : ''} onClick={() => { handleQueryChange(ex) }}>
              {label}
            </button>
          )
        })}
      </div>
    </div>
    <LeafletMap features={features} />
  </>
}

function isGeometry(value: unknown): value is Geometry {
  return typeof value === 'object' && value !== null && 'type' in value
}

/**
 * Collect all rows from the async generator and convert to map features.
 * Hyparquet auto-converts WKB geometry columns to GeoJSON objects.
 */
async function collectFeatures(
  rowGen: AsyncGenerator<AsyncRow>,
  signal: AbortSignal,
  geometryColumn = 'geometry',
): Promise<MapFeature[]> {
  const features: MapFeature[] = []

  for await (const row of rowGen) {
    if (signal.aborted) break
    // TODO: auto-detect geometry column from first row?

    const geometry = await row.cells[geometryColumn]()
    if (!isGeometry(geometry)) continue

    const properties: Record<string, unknown> = {}
    for (const col of row.columns) {
      if (col === geometryColumn) continue
      properties[col] = await row.cells[col]()
    }

    features.push({ geometry, properties })
  }

  return features
}

function formatFileSize(bytes: number): string {
  const sizes = ['b', 'kb', 'mb', 'gb', 'tb']
  if (bytes === 0) return '0 b'
  const i = Math.floor(Math.log2(bytes) / 10)
  if (i === 0) return `${bytes} b`
  const base = bytes / Math.pow(1024, i)
  return `${base < 10 ? base.toFixed(1) : Math.round(base)} ${sizes[i]}`
}
