import { AsyncBuffer, FileMetaData, parquetSchema } from 'hyparquet'
import type { AsyncDataSource, AsyncRow } from 'squirreling'
import { whereToParquetFilter } from './parquetFilter'
import { parquetPlan } from 'hyparquet/src/plan.js'
import { asyncGroupToRows, readRowGroup } from 'hyparquet/src/rowgroup.js'
import type { AsyncRowGroup } from 'hyparquet/src/types.js'
import type { AsyncCells } from 'squirreling/src/types'

export function parquetDataSource(file: AsyncBuffer, metadata: FileMetaData): AsyncDataSource {
  return {
    async *scan(hints): AsyncGenerator<AsyncRow> {
      const options = {
        file,
        metadata,
        columns: hints?.columns,
        filter: whereToParquetFilter(hints?.where),
      }

      // TODO: check that columns exist in parquet file

      let columns = options.columns
      if (!columns?.length) {
        const schema = parquetSchema(metadata)
        columns = schema.children.map(col => col.element.name)
      }
      console.log('Reading parquet with columns', options.columns, 'filter', options.filter, 'limit', hints?.limit)
      const plan = parquetPlan(options)
      for (const subplan of plan.groups) {
        const rg: AsyncRowGroup = readRowGroup(options, plan, subplan)
        const rows = await asyncGroupToRows(rg, 0, rg.groupRows, undefined, 'object')
        for (const row of rows) {
          const cells: AsyncCells = {}
          for (const [key, value] of Object.entries(row)) {
            cells[key] = () => Promise.resolve(value)
          }
          yield { columns, cells }
        }
      }
    },
  }
}
