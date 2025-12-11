import type { AsyncRow } from 'squirreling'
import type { ColumnDescriptor, DataFrame, DataFrameEvents } from 'hightable/dataframe'
import { createEventTarget } from 'hightable/dataframe'

/**
 * Creates a hightable DataFrame from a squirreling async row generator.
 * Rows and cells are loaded lazily - only when fetch() is called.
 */
export function squirrelingDataFrame(
  rowGen: AsyncGenerator<AsyncRow>,
): DataFrame {
  const asyncRows: AsyncRow[] = []
  const resolvedCells = new Map<string, unknown>()
  let columnDescriptors: ColumnDescriptor[] = []
  let generatorDone = false
  const eventTarget = createEventTarget<DataFrameEvents>()

  // Advance generator to discover more rows (called by fetch)
  async function discoverRows(targetRow: number, signal?: AbortSignal) {
    while (asyncRows.length <= targetRow && !generatorDone) {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
      const { value, done } = await rowGen.next()
      if (done) {
        generatorDone = true
        eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
        break
      }
      // Set columns from first row
      if (asyncRows.length === 0) {
        columnDescriptors = value.columns.map(name => ({ name }))
      }
      asyncRows.push(value)
      eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
    }
  }

  return {
    get numRows() { return generatorDone ? asyncRows.length : asyncRows.length + 100 },
    get columnDescriptors() { return columnDescriptors },
    eventTarget,

    getCell({ row, column }) {
      const key = `${row}:${column}`
      if (!resolvedCells.has(key)) return undefined
      return { value: resolvedCells.get(key) }
    },

    getRowNumber({ row }) {
      if (row >= asyncRows.length) return undefined
      return { value: row }
    },

    async fetch({ rowStart, rowEnd, columns, signal }) {
      // Discover rows up to rowEnd-1
      await discoverRows(rowEnd - 1, signal)

      const end = Math.min(rowEnd, asyncRows.length)
      const cols = columns ?? columnDescriptors.map(c => c.name)

      for (let row = rowStart; row < end; row++) {
        if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
        const { cells } = asyncRows[row]
        for (const col of cols) {
          const key = `${row}:${col}`
          if (!resolvedCells.has(key)) {
            resolvedCells.set(key, await cells[col]())
          }
        }
      }
      eventTarget.dispatchEvent(new CustomEvent('resolve'))
    },
  }
}
