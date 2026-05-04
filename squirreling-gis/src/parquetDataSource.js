import { parquetReadObjects, parquetSchema } from 'hyparquet'
import { parquetReadAsync } from 'hyparquet/src/read.js'
import { assembleAsync } from 'hyparquet/src/rowgroup.js'
import { whereToParquetFilter } from './parquetFilter.js'
import { asyncRow } from 'squirreling/src/backend/dataSource.js'
import { bbox } from 'squirreling/src/spatial/bbox.js'
import { parseWkt } from 'squirreling/src/spatial/wkt.js'
import { extractSpatialFilter, rowGroupOverlaps } from './parquetSpatial.js'

/**
 * @import { AsyncBuffer, Compressors, FileMetaData, ParquetQueryFilter, RowGroup } from 'hyparquet'
 * @import { AsyncDataSource, ExprNode, ScanOptions, ScanResults } from 'squirreling'
 * @import { BBox } from 'squirreling/src/spatial/geometry.js'
 */

/**
 * Creates a parquet data source for use with squirreling SQL engine.
 *
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {Compressors} compressors
 * @returns {AsyncDataSource}
 */
export function parquetDataSource(file, metadata, compressors) {
  const schema = parquetSchema(metadata)
  return {
    numRows: Number(metadata.num_rows),
    columns: schema.children.map(c => c.element.name),
    /**
     * @param {ScanOptions} hints
     * @returns {ScanResults}
     */
    scan(hints) {
      // Convert WHERE AST to hyparquet filter format
      const whereFilter = hints.where && whereToParquetFilter(hints.where)
      /** @type {ParquetQueryFilter | undefined} */
      const filter = hints.where ? whereFilter : undefined
      const appliedWhere = Boolean(filter && whereFilter)
      const appliedLimitOffset = !hints.where || appliedWhere

      // Extract spatial filter for row group pruning
      const spatialFilter = extractSpatialFilter(hints.where)

      return {
        async *rows() {
          metadata ??= await parquetMetadataAsync(file)

          // Emit rows by row group
          let groupStart = 0
          let remainingLimit = hints.limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (hints.signal?.aborted) throw new DOMException('Aborted', 'AbortError')
            const rowCount = Number(rowGroup.num_rows)

            // Skip row groups using geospatial statistics
            if (spatialFilter && !rowGroupOverlaps(rowGroup, spatialFilter)) {
              groupStart += rowCount
              continue
            }

            // Skip row groups by offset if where is fully applied
            let safeOffset = 0
            let safeLimit = rowCount
            if (appliedLimitOffset) {
              if (hints.offset !== undefined && groupStart < hints.offset) {
                safeOffset = Math.min(rowCount, hints.offset - groupStart)
              }
              safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
              if (safeLimit <= 0 && safeOffset < rowCount) break
            }
            // no rows from this group, continue to next
            if (safeOffset === rowCount) {
              groupStart += rowCount
              continue
            }

            // Read objects from this row group
            const data = await parquetReadObjects({
              file,
              metadata,
              rowStart: groupStart + safeOffset,
              rowEnd: groupStart + safeOffset + safeLimit,
              columns: hints.columns,
              filter,
              filterStrict: false,
              compressors,
              useOffsetIndex: true,
            })

            // Yield each row
            for (const row of data) {
              yield asyncRow(row, Object.keys(row))
            }

            remainingLimit -= data.length
            groupStart += rowCount
          }
        },
        appliedWhere,
        appliedLimitOffset,
      }
    },

    /**
     * @param {{ column: string, limit?: number, offset?: number, signal?: AbortSignal }} options
     */
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
