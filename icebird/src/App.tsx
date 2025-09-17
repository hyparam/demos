import { ReactNode } from 'react'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

import type { DataFrame, DataFrameEvents, ResolvedValue } from 'hightable'
import { checkSignal, createEventTarget, sortableDataFrame, validateFetchParams, validateGetCellParams, validateGetRowNumberParams } from 'hightable'
import { icebergListVersions, icebergMetadata, icebergRead } from 'icebird'
import type { Snapshot, TableMetadata } from 'icebird/src/types.js'
import { useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'

const empty: DataFrame = {
  columnDescriptors: [],
  numRows: 0,
  eventTarget: createEventTarget<DataFrameEvents>(),
  getRowNumber: () => undefined,
  getCell: () => undefined,
  fetch: () => Promise.resolve(undefined),
}

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [tableUrl, setTableUrl] = useState(queryUrl)
  const [version, setVersion] = useState<string>()
  const [versions, setVersions] = useState<string[] | undefined>()

  const setUnknownError = useCallback((e: unknown) => {
    if (e instanceof Error && e.message === 'No iceberg snapshots found') {
      console.warn('No iceberg snapshots found for version', version)
      setPageProps(props => props ? { ...props, df: empty } : undefined)
    } else {
      setError(e instanceof Error ? e : new Error(String(e)))
    }
  }, [version])

  useEffect(() => {
    // List metadata versions
    if (!tableUrl || versions) return
    icebergListVersions({ tableUrl })
      .then(versions => {
        setVersions(versions)
        if (versions.length === 0) throw new Error('No iceberg metadata versions found')
        setVersion(versions[versions.length - 1])
      })
      .catch(setUnknownError)
  }, [tableUrl, versions, setVersions, setUnknownError])

  useEffect(() => {
    if (!version) return
    setPageProps(props => props ? { ...props, version } : undefined)
  }, [setPageProps, version])

  useEffect(() => {
    if (!tableUrl || !versions || !version) return
    // Get the metadata from the iceberg table
    const metadataFileName = `${version}.metadata.json`
    icebergMetadata({ tableUrl: tableUrl, metadataFileName }).then((metadata: TableMetadata) => {
      const df = icebergDataFrame(tableUrl, metadataFileName, metadata)
      setPageProps({ df, metadata, versions, version, setVersion, setError: setUnknownError })
    }).catch(setUnknownError)
  }, [tableUrl, versions, version, setUnknownError])

  const setUrlAndHistory = useCallback(
    (url: string) => {
      // Add key=url to query string
      const params = new URLSearchParams(location.search)
      params.set('key', url)
      history.pushState({}, '', `${location.pathname}?${params}`)
      setTableUrl(url)
    },
    [setTableUrl],
  )

  return <Layout error={error}>
    {pageProps ? <Page {...pageProps} /> : <Welcome setTableUrl={setUrlAndHistory} />}
  </Layout>
}

function icebergDataFrame(tableUrl: string, metadataFileName: string, metadata: TableMetadata): DataFrame {
  if (!metadata.snapshots?.length) throw new Error('No iceberg snapshots found')

  const snapshot: Snapshot = metadata.snapshots[metadata.snapshots.length - 1]
  // Warning: this is not exactly the number of rows
  const numRows = Number(snapshot.summary['total-records'])
  const currentSchemaId = metadata['current-schema-id']
  const schema = metadata.schemas.find(s => s['schema-id'] === currentSchemaId)
  if (!schema) throw new Error('Current schema not found in metadata')
  const columnDescriptors = schema.fields.map(({ name }) => ({ name }))
  const eventTarget = createEventTarget<DataFrameEvents>()

  type CachedValue<T> = {
    kind: 'fetched'
    value: ResolvedValue<T>
  } | {
    kind: 'fetching'
  } | undefined

  const rowNumberCache: CachedValue<number>[] = []
  const cellCache = new Map<string, CachedValue<unknown>[]>()
  columnDescriptors.forEach(({ name }) => cellCache.set(name, []))

  function getRowNumber({ row }: { row: number }): ResolvedValue<number> | undefined {
    validateGetRowNumberParams({ row, data: { numRows, columnDescriptors } })
    const cachedValue = rowNumberCache[row]
    return cachedValue?.kind === 'fetched' ? cachedValue.value : undefined
  }
  function getCell({ row, column }: { row: number, column: string }): ResolvedValue<unknown> | undefined {
    validateGetCellParams({ row, column, data: { numRows, columnDescriptors } })
    const cachedValue = cellCache.get(column)?.[row]
    return cachedValue?.kind === 'fetched' ? cachedValue.value : undefined
  }
  function isCachedOrFetching({ row, columns }: {row: number, columns?: string[]}): boolean {
    return rowNumberCache[row] !== undefined && (!columns || columns.length === 0 || columns.every(column => cellCache.get(column)?.[row] !== undefined))
  }

  // TODO: fetch by row groups, to avoid fetching row by row when we scroll

  const unsortableDataFrame: DataFrame = {
    columnDescriptors,
    numRows,
    eventTarget,
    getRowNumber,
    getCell,
    async fetch({ rowStart, rowEnd, columns, signal }) {
      validateFetchParams({ rowStart, rowEnd, columns, data: { numRows, columnDescriptors } })
      checkSignal(signal)

      const ranges = []
      let currentRange: [number, number] | undefined = undefined
      for (let row = rowStart; row < rowEnd; row++) {
        if (isCachedOrFetching({ row, columns })) {
          if (currentRange) {
            ranges.push(currentRange)
            currentRange = undefined
          }
        } else {
          if (!currentRange) {
            currentRange = [row, row + 1]
          } else {
            currentRange[1] = row + 1
          }
        }
      }
      if (currentRange) {
        ranges.push(currentRange)
      }
      console.log(`Fetching rows ${rowStart} - ${rowEnd} (${ranges.length} ranges)`, { ranges, columns, cache: { rowNumberCache, cellCache } })

      const promises = ranges.map(async ([start, end]) => {
        for (let row = start; row < end; row++) {
          rowNumberCache[row] = { kind: 'fetching' }
          for (const column of columns ?? []) {
            const array = cellCache.get(column)
            if (!array) throw new Error(`Column ${column} not found in cache`)
            array[row] = { kind: 'fetching' }
          }
        }

        const rows = await icebergRead({
          tableUrl,
          rowStart: start,
          rowEnd: end,
          metadataFileName,
          metadata,
        })

        const rowsEnd = rows.length + start

        for (const [i, cells] of rows.entries()) {
          const row = i + start
          rowNumberCache[row] = { kind: 'fetched', value: { value: row } }
          for (const column of columns ?? []) {
            const array = cellCache.get(column)
            if (!array) throw new Error(`Column ${column} not found in cache`)
            array[row] = { kind: 'fetched', value: { value: cells[column] } }
          }
        }
        // Not sure if it's the best way to handle the missing rows
        for (let row = start + rowsEnd; row < end; row++) {
          rowNumberCache[row] = { kind: 'fetched', value: { value: -1 } } // Indicating that the row is not available - totally not a perfect idea, but it works for now
          for (const column of columns ?? []) {
            const array = cellCache.get(column)
            if (!array) throw new Error(`Column ${column} not found in cache`)
            array[row] = { kind: 'fetched', value: { value: undefined } }
          }
        }

        eventTarget.dispatchEvent(new CustomEvent('resolve'))
      })

      await Promise.all(promises)
    },

  }
  return sortableDataFrame(unsortableDataFrame)

}
