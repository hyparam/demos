import { AsyncBuffer, Compressors, FileMetaData, parquetReadObjects, parquetSchema } from 'hyparquet'
import { whereToParquetFilter } from './parquetFilter.js'
import { AsyncDataSource, AsyncRow, SqlPrimitive } from 'squirreling'
import { AsyncCells } from 'squirreling/src/types.js'

/**
 * Creates a parquet data source for use with squirreling SQL engine.
 */
export function parquetDataSource(file: AsyncBuffer, metadata: FileMetaData, compressors: Compressors): AsyncDataSource {
  return {
    scan({ columns, where, limit, offset, signal }) {
      // Convert WHERE AST to hyparquet filter format
      const whereFilter = where && whereToParquetFilter(where)
      /** @type {ParquetQueryFilter | undefined} */
      const filter = where ? whereFilter : undefined
      const appliedWhere = Boolean(filter && whereFilter)
      const appliedLimitOffset = !where || appliedWhere

      // Ensure columns exist in metadata if provided
      if (columns) {
        const schema = parquetSchema(metadata)
        for (const col of columns) {
          if (!schema.children.some(child => child.element.name === col)) {
            throw new Error(`Column "${col}" not found in parquet schema`)
          }
        }
      }

      return {
        rows: (async function* () {
          // Emit rows by row group
          let groupStart = 0
          let remainingLimit = limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (signal?.aborted) break
            const rowCount = Number(rowGroup.num_rows)

            // Skip row groups by offset if where is fully applied
            let safeOffset = 0
            let safeLimit = rowCount
            if (appliedLimitOffset) {
              if (offset !== undefined && groupStart < offset) {
                safeOffset = Math.min(rowCount, offset - groupStart)
              }
              safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
              if (safeLimit <= 0 && safeOffset < rowCount) break
            }
            if (safeOffset === rowCount) {
              // no rows from this group, continue to next
              groupStart += rowCount
              continue
            }

            // Read objects from this row group
            // TODO: move to worker
            const data = await parquetReadObjects({
              file,
              metadata,
              rowStart: groupStart + safeOffset,
              rowEnd: groupStart + safeOffset + safeLimit,
              columns,
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
        })(),
        appliedWhere,
        appliedLimitOffset,
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
