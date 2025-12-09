import type { AsyncBuffer, FileMetaData } from 'hyparquet'
import type { AsyncDataSource, AsyncRow } from 'squirreling'
import { whereToParquetFilter } from './parquetFilter'
import { parquetPlan } from 'hyparquet/src/plan.js'
import { asyncGroupToRows, readRowGroup } from 'hyparquet/src/rowgroup.js'
import { AsyncRowGroup } from 'hyparquet/src/types.js'

export function parquetDataSource(file: AsyncBuffer, metadata: FileMetaData): AsyncDataSource {
  return {
    async *getRows(hints): AsyncGenerator<AsyncRow> {
      const options = {
        file,
        metadata,
        columns: hints?.columns,
        filter: whereToParquetFilter(hints?.where),
      }

      console.log('Reading parquet with columns', options.columns, 'filter', options.filter, 'limit', hints?.limit)
      const plan = parquetPlan(options)
      for (const subplan of plan.groups) {
        const rg: AsyncRowGroup = readRowGroup(options, plan, subplan)
        const rows = await asyncGroupToRows(rg, 0, rg.groupRows, undefined, 'object')
        for (const asyncRow of rows) {
          const row: AsyncRow = {}
          for (const [key, value] of Object.entries(asyncRow)) {
            row[key] = () => Promise.resolve(value)
          }
          yield row
        }
      }
    },
  }
}
