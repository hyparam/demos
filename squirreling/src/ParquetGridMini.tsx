import type { FileMetaData } from 'hyparquet'
import type { ReactNode } from 'react'
import { type ByteRange, getRangeStatus } from './countingBuffer.js'

interface GridProps {
  metadata: FileMetaData
  downloadedRanges?: ByteRange[]
}

/**
 * Mini visualization of parquet row groups and column chunks.
 * No text labels, just colored bars showing relative sizes.
 * Shows downloaded chunks as solid, undownloaded with border and light fill.
 */
export default function ParquetGridMini({ metadata, downloadedRanges = [] }: GridProps): ReactNode {
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

  // Build grid-template-columns with minmax for minimum size
  const gridTemplateColumns = columnTotals
    .map(size => `minmax(2px, ${size}fr)`)
    .join(' ')

  return (
    <div className="parquet-grid-mini">
      {metadata.row_groups.map((rowGroup, i) =>
        <div key={i} className="grid-row-mini" style={{ gridTemplateColumns }}>
          {rowGroup.columns.map((column, j) => {
            const numRows = Number(rowGroup.num_rows)
            const colSize = Number(column.meta_data?.total_compressed_size ?? 0n)
            const colName = column.meta_data?.path_in_schema.join('.') ?? `col${j}`
            const widthPercent = columnMaxes[j] > 0 ? colSize / columnMaxes[j] * 100 : 0

            // Calculate byte range for this chunk
            // Use dictionary_page_offset if present (comes before data pages)
            const dictOffset = column.meta_data?.dictionary_page_offset
            const dataOffset = column.meta_data?.data_page_offset
            const chunkStart = Number(dictOffset ?? dataOffset)
            const chunkEnd = chunkStart + colSize
            const status = getRangeStatus(downloadedRanges, chunkStart, chunkEnd)

            const statusLabel = status === 'downloaded' ? '(downloaded)' : status === 'partial' ? '(partial)' : ''
            const title = `Column: ${colName}\nRow Group ${i}\n${numRows.toLocaleString()} rows\n${colSize.toLocaleString()} bytes\nRange: ${chunkStart.toLocaleString()}-${chunkEnd.toLocaleString()}${statusLabel ? '\n' + statusLabel : ''}`
            return (
              <div
                key={j}
                className={`cell-mini ${j % 2 === 0 ? 'even' : 'odd'} ${status}`}
                title={title}
              >
                <div
                  className="bar-mini"
                  style={{ width: `${widthPercent}%` }}
                />
              </div>
            )
          })}
        </div>,
      )}
    </div>
  )
}
