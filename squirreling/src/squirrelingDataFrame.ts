import { derivedAlias } from 'squirreling'
import type { AsyncRow, Statement } from 'squirreling'
import type { ColumnDescriptor, DataFrame, DataFrameEvents } from 'hightable/dataframe'
import { createEventTarget } from 'hightable/dataframe'

interface SquirrelingDataFrameOptions {
  rowGen: AsyncGenerator<AsyncRow>
  query: Statement
  sourceColumns: string[]
}

/**
 * Resolve select columns to column names deterministically.
 * Handles *, derived columns, CTEs, and set operations.
 */
function resolveColumnNames(
  query: Statement,
  sourceColumns: string[],
  cteColumns = new Map<string, string[]>(),
): string[] {
  if (query.type === 'with') {
    const scopedCteColumns = new Map(cteColumns)
    for (const cte of query.ctes) {
      scopedCteColumns.set(
        cte.name.toLowerCase(),
        resolveColumnNames(cte.query, sourceColumns, scopedCteColumns),
      )
    }
    return resolveColumnNames(query.query, sourceColumns, scopedCteColumns)
  } else if (query.type === 'compound') {
    return resolveColumnNames(query.left, sourceColumns, cteColumns)
  }

  const names: string[] = []
  const starColumns = query.from.type === 'table'
    ? cteColumns.get(query.from.table.toLowerCase()) ?? sourceColumns
    : sourceColumns

  for (const col of query.columns) {
    if (col.type === 'star') {
      names.push(...starColumns)
    } else {
      names.push(col.alias ?? derivedAlias(col.expr))
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
  query,
  sourceColumns,
}: SquirrelingDataFrameOptions): DataFrame {
  const asyncRows: AsyncRow[] = []
  const resolvedCells = new Map<string, unknown>()
  let columnDescriptors: ColumnDescriptor[] = resolveColumnNames(query, sourceColumns)
    .map(name => ({ name }))
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
