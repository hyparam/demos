import type { ColumnDescriptor, DataFrame, DataFrameEvents, ResolvedValue } from 'hightable/dataframe'
import { createEventTarget } from 'hightable/dataframe'
import type { AsyncDataSource, AsyncRow } from 'squirreling'

/**
 * Adapt a squirreling `AsyncDataSource` (what `icebergDataSource` returns)
 * into a hightable `DataFrame`. One scan streams rows lazily; cells are
 * resolved as `fetch()` is called over a window. Works with
 * `sortableDataFrame` on top.
 */
export function icebergDataFrame(source: AsyncDataSource): DataFrame {
  const asyncRows: AsyncRow[] = []
  const resolvedCells = new Map<string, unknown>()
  const columnDescriptors: ColumnDescriptor[] = source.columns.map(name => ({ name }))
  const eventTarget = createEventTarget<DataFrameEvents>()
  const iter = source.scan({}).rows()[Symbol.asyncIterator]()
  let generatorDone = false

  async function discoverUpTo(targetRow: number, signal?: AbortSignal): Promise<void> {
    while (asyncRows.length <= targetRow && !generatorDone) {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
      const result = await iter.next()
      if (result.done) {
        generatorDone = true
        eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
        break
      }
      asyncRows.push(result.value)
      eventTarget.dispatchEvent(new CustomEvent('numrowschange'))
    }
  }

  return {
    get numRows() {
      if (source.numRows !== undefined) return source.numRows
      // Show a soft upper bound while the scan is still streaming so the
      // virtual table has something to render against. Lands on the real
      // count once the generator finishes.
      return generatorDone ? asyncRows.length : asyncRows.length + 100
    },
    columnDescriptors,
    eventTarget,

    getCell({ row, column }): ResolvedValue | undefined {
      const key = `${row}:${column}`
      if (!resolvedCells.has(key)) return undefined
      return { value: resolvedCells.get(key) }
    },

    getRowNumber({ row }): ResolvedValue<number> | undefined {
      if (source.numRows !== undefined) {
        if (row >= source.numRows) return undefined
      } else if (row >= asyncRows.length) return undefined
      return { value: row }
    },

    async fetch({ rowStart, rowEnd, columns, signal }) {
      await discoverUpTo(rowEnd - 1, signal)
      const end = Math.min(rowEnd, asyncRows.length)
      const cols = columns ?? columnDescriptors.map(c => c.name)
      // Resolve cells in parallel and dispatch `resolve` per cell so HighTable
      // can re-render incrementally instead of waiting for the whole window.
      const promises: Promise<void>[] = []
      for (let row = rowStart; row < end; row++) {
        const { cells } = asyncRows[row]
        for (const col of cols) {
          const key = `${row}:${col}`
          if (resolvedCells.has(key)) continue
          promises.push((async () => {
            resolvedCells.set(key, await cells[col]())
            if (!signal?.aborted) {
              eventTarget.dispatchEvent(new CustomEvent('resolve'))
            }
          })())
        }
      }
      await Promise.all(promises)
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
    },
  }
}
