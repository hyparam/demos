import type { FileMetaData } from 'hyparquet'
import type { ReactNode } from 'react'

interface GridProps {
  metadata: FileMetaData
}

/**
 * Visualize parquet row groups and column chunks with proportional widths
 */
export default function ParquetGrid({ metadata }: GridProps): ReactNode {
  const numCols = metadata.row_groups[0]?.columns.length ?? 0

  // Calculate total and max size per column across all row groups
  const columnTotals = Array<number>(numCols).fill(0)
  const columnMaxes = Array<number>(numCols).fill(0)
  for (const rg of metadata.row_groups) {
    for (let j = 0; j < rg.columns.length; j++) {
      const size = Number(rg.columns[j].meta_data?.total_compressed_size ?? 0n)
      columnTotals[j] += size
      columnMaxes[j] = Math.max(columnMaxes[j], size)
    }
  }

  // Get column names from first row group
  const columnNames = metadata.row_groups[0]?.columns
    .map((col, j) => col.meta_data?.path_in_schema.join('.') ?? `col${j}`) ?? []

  // Build grid-template-columns with minmax for minimum size
  const gridTemplateColumns = columnTotals
    .map(size => `minmax(4px, ${size}fr)`)
    .join(' ')

  return (
    <div className="parquet-grid">
      {/* Header row */}
      <div className="grid-label">Row group</div>
      <div className="grid-row" style={{ gridTemplateColumns }}>
        {columnNames.map((name, j) =>
          <div
            key={j}
            className={`column-label ${j % 2 === 0 ? 'even' : 'odd'}`}
            title={`Column: ${name}\n${columnTotals[j].toLocaleString()} bytes`}
          >
            {name}
          </div>,
        )}
      </div>

      {/* Data rows */}
      {metadata.row_groups.map((rowGroup, i) =>
        <div key={i} className="grid-row-wrapper">
          <div className="grid-label">{i}</div>
          <div className="grid-row" style={{ gridTemplateColumns }}>
            {rowGroup.columns.map((column, j) => {
              const numRows = Number(rowGroup.num_rows)
              const numValues = Number(column.meta_data?.num_values ?? 0n)
              const colSize = Number(column.meta_data?.total_compressed_size ?? 0n)
              const colName = column.meta_data?.path_in_schema.join('.') ?? `col${j}`
              const widthPercent = columnMaxes[j] > 0 ? colSize / columnMaxes[j] * 100 : 0
              let title = `Row Group: ${i}\n${numRows} rows\nColumn: ${colName}`
              if (numValues !== numRows) {
                title += `\n${numValues} values`
              }
              title += `\n${colSize.toLocaleString()} bytes`
              title += `\n${(colSize / numRows).toFixed(2)} bytes/cell`
              return (
                <div
                  key={j}
                  className={`column-cell ${j % 2 === 0 ? 'even' : 'odd'}`}
                  title={title}
                >
                  <div
                    className="column-bar"
                    style={{ width: `${widthPercent}%` }}
                  />
                </div>
              )
            })}
          </div>
        </div>,
      )}
    </div>
  )
}
