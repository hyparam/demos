import { AsyncBuffer, Compressors, FileMetaData, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { whereToParquetFilter } from './parquetFilter.js'
import { AsyncDataSource, AsyncRow, SqlPrimitive } from 'squirreling'
import { AsyncCells } from 'squirreling/src/types.js'

/**
 * Creates a parquet data source for use with squirreling SQL engine.
 */
export function parquetDataSource(file: AsyncBuffer, metadata: FileMetaData | undefined, compressors: Compressors): AsyncDataSource {
  return {
    async *scan({ hints, signal }) {
      metadata ??= await parquetMetadataAsync(file)

      // Convert WHERE AST to hyparquet filter format
      const whereFilter = hints?.where && whereToParquetFilter(hints.where)
      /** @type {ParquetQueryFilter | undefined} */
      const filter = hints?.where ? whereFilter : undefined
      const filterApplied = !filter || whereFilter

      // Emit rows by row group
      let groupStart = 0
      let remainingLimit = hints?.limit ?? Infinity
      for (const rowGroup of metadata.row_groups) {
        if (signal?.aborted) break
        const rowCount = Number(rowGroup.num_rows)

        // Skip row groups by offset if where is fully applied
        let safeOffset = 0
        let safeLimit = rowCount
        if (filterApplied) {
          if (hints?.offset !== undefined && groupStart < hints.offset) {
            safeOffset = Math.min(rowCount, hints.offset - groupStart)
          }
          safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
          if (safeLimit <= 0 && safeOffset < rowCount) break
        }
        for (let i = 0; i < safeOffset; i++) {
          // yield empty rows
          yield asyncRow({})
        }
        if (safeOffset === rowCount) {
          // no rows from this group, continue to next
          groupStart += rowCount
          continue
        }

        // Read objects from this row group
        const data = await parquetReadObjects({
          file,
          metadata,
          rowStart: groupStart + safeOffset,
          rowEnd: groupStart + safeOffset + safeLimit,
          columns: hints?.columns,
          filter,
          filterStrict: false,
          compressors,
          useOffsetIndex: true,
        })

        // Yield each row
        for (const row of data) {
          yield asyncRow(row)
        }

        remainingLimit -= data.length
        groupStart += rowCount
      }
    },
  }
}

/**
 * Creates an async row accessor that wraps a plain JavaScript object
 */
function asyncRow(obj: Record<string, SqlPrimitive>): AsyncRow {
  const cells: AsyncCells = {}
  for (const [key, value] of Object.entries(obj)) {
    cells[key] = () => Promise.resolve(value)
  }
  return { columns: Object.keys(obj), cells }
}
