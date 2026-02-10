import type { AsyncRow } from 'squirreling'
import type { SelectColumn } from 'squirreling/src/types.js'
import type { ColumnDescriptor, DataFrame, DataFrameEvents } from 'hightable/dataframe'
import { createEventTarget, stringify } from 'hightable/dataframe'

interface SquirrelingDataFrameOptions {
  rowGen: AsyncGenerator<AsyncRow>
  columns?: SelectColumn[]
  sourceColumns?: string[]
}

/**
 * Resolve select columns to column names deterministically.
 * Handles both * (star) columns and derived columns with aliases.
 */
function resolveColumnNames(columns: SelectColumn[], sourceColumns: string[]): string[] {
  const names: string[] = []
  for (const col of columns) {
    if (col.kind === 'star') {
      // SELECT * - use all source columns
      names.push(...sourceColumns)
    } else {
      // Derived column - use alias or expression name
      if (col.alias) {
        names.push(col.alias)
      } else if (col.expr.type === 'identifier') {
        names.push(col.expr.name)
      } else if (col.expr.type === 'function') {
        names.push(col.expr.name)
      } else if (col.expr.type === 'literal') {
        names.push(stringify(col.expr.value) ?? '?')
      } else {
        names.push('?')
      }
    }
  }
  return names
}

/**
 * Creates a hightable DataFrame from a squirreling async row generator.
 * Rows and cells are loaded lazily - only when fetch() is called.
 */
export function squirrelingDataFrame({
  rowGen,
  columns,
  sourceColumns,
}: SquirrelingDataFrameOptions): DataFrame {
  const asyncRows: AsyncRow[] = []
  const resolvedCells = new Map<string, unknown>()
  // Compute column descriptors deterministically if columns are provided
  let columnDescriptors: ColumnDescriptor[] = columns && sourceColumns
    ? resolveColumnNames(columns, sourceColumns).map(name => ({ name }))
    : []
  let generatorDone = false
  const eventTarget = createEventTarget<DataFrameEvents>()

  // Advance generator to discover more rows (called by fetch)
  async function discoverRows(targetRow: number, signal?: AbortSignal) {
    while (asyncRows.length <= targetRow && !generatorDone) {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
      const result = await rowGen.next()
      if (result.done) {
        generatorDone = true
        eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
        break
      }
      const row: AsyncRow = result.value
      // Set columns from first row (fallback if not provided upfront)
      if (asyncRows.length === 0 && columnDescriptors.length === 0) {
        columnDescriptors = row.columns.map(name => ({ name }))
      }
      asyncRows.push(row)
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
            try {
              resolvedCells.set(key, await cells[col]())
            } catch (err) {
              console.error(`Error fetching cell ${col} in row ${row}:`, err, cells, columns)
              resolvedCells.set(key, new Error(String(err)))
            }
          }
        }
      }
      eventTarget.dispatchEvent(new CustomEvent('resolve'))
    },
  }
}
