
import type { DataFrame, DataFrameEvents, ResolvedValue } from 'hightable'
import { checkSignal, createEventTarget, sortableDataFrame, validateFetchParams, validateGetCellParams, validateGetRowNumberParams } from 'hightable'
import { icebergRead } from 'icebird'
import type { Snapshot, TableMetadata } from 'icebird/src/types.js'

export function icebergDataFrame(tableUrl: string, metadataFileName: string, metadata: TableMetadata): DataFrame {
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
