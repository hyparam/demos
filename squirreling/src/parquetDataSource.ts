import { AsyncBuffer, Compressors, FileMetaData, parquetReadObjects, parquetSchema } from 'hyparquet'
import { parquetReadAsync } from 'hyparquet/src/read.js'
import { assembleAsync } from 'hyparquet/src/rowgroup.js'
import { whereToParquetFilter } from './parquetFilter.js'
import { asyncRow } from 'squirreling'
import type { AsyncDataSource, SqlPrimitive } from 'squirreling'

/**
 * Creates a parquet data source for use with squirreling SQL engine.
 */
export function parquetDataSource(file: AsyncBuffer, metadata: FileMetaData, compressors: Compressors): AsyncDataSource {
  const schema = parquetSchema(metadata)
  return {
    numRows: Number(metadata.num_rows),
    columns: schema.children.map(c => c.element.name),
    scan({ columns, where, limit, offset, signal }) {
      // Convert WHERE AST to hyparquet filter format
      const whereFilter = where && whereToParquetFilter(where)
      /** @type {ParquetQueryFilter | undefined} */
      const filter = where ? whereFilter : undefined
      const appliedWhere = Boolean(filter && whereFilter)
      const appliedLimitOffset = !where || appliedWhere

      // Ensure columns exist in metadata if provided
      if (columns) {
        for (const col of columns) {
          if (!schema.children.some(child => child.element.name === col)) {
            throw new Error(`Column "${col}" not found in parquet schema`)
          }
        }
      }

      return {
        async *rows() {
          // Emit rows by row group
          let groupStart = 0
          let remainingLimit = limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
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
            const data = await parquetReadObjects({
              file,
              metadata,
              rowStart: groupStart + safeOffset,
              rowEnd: groupStart + safeOffset + safeLimit,
              columns,
              filter,
              filterStrict: false,
              compressors,
              useOffsetIndex: safeOffset > 0 || safeLimit < rowCount,
            })

            // Yield each row
            if (data.length > 0) {
              const rowColumns = Object.keys(data[0])
              for (const row of data) {
                yield asyncRow(row as Record<string, SqlPrimitive>, rowColumns)
              }
            }

            remainingLimit -= data.length
            groupStart += rowCount
          }
        },
        appliedWhere,
        appliedLimitOffset,
      }
    },

    async *scanColumn({ column, limit, offset, signal }) {
      const rowStart = offset ?? 0
      const rowEnd = limit !== undefined ? rowStart + limit : undefined
      const asyncGroups = parquetReadAsync({
        file,
        metadata,
        rowStart,
        rowEnd,
        columns: [column],
        compressors,
      })
      // assemble struct columns
      const schemaTree = parquetSchema(metadata)
      const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

      for (const rg of assembled) {
        if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
        const { skipped, data } = await rg.asyncColumns[0].data
        if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
        let dataStart = rg.groupStart + skipped
        for (const page of data) {
          const pageRows = page.length
          const selectStart = Math.max(rowStart - dataStart, 0)
          const selectEnd = Math.min((rowEnd ?? Infinity) - dataStart, pageRows)
          if (selectEnd > selectStart) {
            yield selectStart > 0 || selectEnd < pageRows
              ? page.slice(selectStart, selectEnd)
              : page
          }
          dataStart += pageRows
        }
      }
    },
  }
}
