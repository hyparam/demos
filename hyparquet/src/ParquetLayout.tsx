import type { ColumnChunk, ColumnMetaData, FileMetaData } from 'hyparquet'
import type { ReactNode } from 'react'

interface LayoutProps {
  byteLength: number
  metadata: FileMetaData
}

/**
 * Renders the file layout of a parquet file as nested rowgroups and columns.
 * @param {Object} props
 * @param {number} props.byteLength
 * @param {FileMetaData} props.metadata
 * @returns {ReactNode}
 */
export default function ParquetLayout({ byteLength, metadata }: LayoutProps): ReactNode {
  const metadataStart = byteLength - metadata.metadata_length - 4
  const metadataEnd = byteLength - 4

  // Collect all layout items with their byte ranges
  const layoutItems = collectLayoutItems(metadata)

  return <div className='viewer'>
    <div className='layout'>
      <Cell name='PAR1' start={0n} end={4n} className="magic" />
      {layoutItems.map((item, index) => (
        <LayoutItem key={index} item={item} />
      ))}
      <Cell name='Metadata' start={metadataStart} end={metadataEnd} className="metadata" />
      <Cell name='PAR1' start={metadataEnd} end={byteLength} className="magic" />
    </div>
  </div>
}

interface CellProps<N extends bigint | number> {
  name: string
  start: N
  end: N
  className?: string
}

function Cell<N extends bigint | number>({ name, start, end, className }: CellProps<N>) {
  const bytes = end - start
  return <div className={className ? `cell ${className}` : 'cell'}>
    <label>{name}</label>
    <ul>
      <li>start {start.toLocaleString()}</li>
      <li>bytes {bytes.toLocaleString()}</li>
      <li>end {end.toLocaleString()}</li>
    </ul>
  </div>
}

function Group({ children, name, bytes }: { children: ReactNode, name?: string, bytes?: bigint }) {
  return <div className="group">
    <div className="group-header">
      <label>{name}</label>
      <span>{bytes === undefined ? '' : `bytes ${bytes.toLocaleString()}`}</span>
    </div>
    {children}
  </div>
}

interface RowGroupItem {
  type: 'rowgroup'
  start: bigint
  groupIndex: number
  columns: ColumnChunk[]
  totalByteSize: bigint
}

interface IndexItem {
  type: 'index'
  start: bigint
  end: bigint
  name: string
}

type LayoutItemType = RowGroupItem | IndexItem

function collectLayoutItems(metadata: FileMetaData): LayoutItemType[] {
  const items: LayoutItemType[] = []

  // Add row groups
  for (let groupIndex = 0; groupIndex < metadata.row_groups.length; groupIndex++) {
    const rowGroup = metadata.row_groups[groupIndex]
    // Find the earliest start offset among all columns in the row group
    let start = BigInt(Number.MAX_SAFE_INTEGER)
    for (const column of rowGroup.columns) {
      if (column.meta_data) {
        const [colStart] = getColumnRange(column.meta_data)
        if (colStart < start) start = colStart
      }
    }
    items.push({
      type: 'rowgroup',
      start,
      groupIndex,
      columns: rowGroup.columns,
      totalByteSize: rowGroup.total_byte_size,
    })
  }

  // Add column and offset indexes
  for (let groupIndex = 0; groupIndex < metadata.row_groups.length; groupIndex++) {
    const rowGroup = metadata.row_groups[groupIndex]
    for (const column of rowGroup.columns) {
      const columnName = column.meta_data?.path_in_schema.join('.')
      if (column.column_index_offset) {
        items.push({
          type: 'index',
          start: column.column_index_offset,
          end: column.column_index_offset + BigInt(column.column_index_length ?? 0),
          name: `ColumnIndex\nRowGroup ${groupIndex}, Column '${columnName}'`,
        })
      }
      if (column.offset_index_offset) {
        items.push({
          type: 'index',
          start: column.offset_index_offset,
          end: column.offset_index_offset + BigInt(column.offset_index_length ?? 0),
          name: `OffsetIndex\nRowGroup ${groupIndex}, Column '${columnName}'`,
        })
      }
    }
  }

  // Sort all items by start offset
  items.sort((a, b) => Number(a.start - b.start))
  return items
}

function LayoutItem({ item }: { item: LayoutItemType }) {
  if (item.type === 'rowgroup') {
    return (
      <Group name={`RowGroup ${item.groupIndex}`} bytes={item.totalByteSize}>
        {item.columns.map((column, j) =>
          <Column key={j} column={column} />,
        )}
      </Group>
    )
  } else {
    return <Cell name={item.name} start={item.start} end={item.end} className="index" />
  }
}

function Column({ column }: { column: ColumnChunk }) {

  if (!column.meta_data) return null
  const { meta_data } = column
  const { dictionary_page_offset, data_page_offset, index_page_offset } = meta_data
  const end = getColumnRange(column.meta_data)[1]
  const pages = [
    { name: 'Dictionary', offset: dictionary_page_offset },
    { name: 'Data', offset: data_page_offset },
    { name: 'Index', offset: index_page_offset },
    { name: 'End', offset: end },
  ]
    .filter((page): page is {name: string, offset: bigint} => page.offset !== undefined)
    .sort((a, b) => Number(a.offset - b.offset))

  const children = pages.slice(0, -1).map(({ name, offset }, index) =>
    <Cell key={name} name={name} start={offset} end={pages[index + 1].offset} />,
  )

  return <Group
    name={`Column '${column.meta_data.path_in_schema.join('.')}'`}
    bytes={column.meta_data.total_compressed_size}>
    {children}
  </Group>
}

/**
 * Find the start byte offset for a column chunk.
 *
 * @param {ColumnMetaData} columnMetadata
 * @returns {[bigint, bigint]} byte offset range
 */
function getColumnRange({ dictionary_page_offset, data_page_offset, total_compressed_size }: ColumnMetaData): [bigint, bigint] {
  /// Copied from hyparquet because it's not exported
  let columnOffset = dictionary_page_offset
  if (!columnOffset || data_page_offset < columnOffset) {
    columnOffset = data_page_offset
  }
  return [columnOffset, columnOffset + total_compressed_size]
}
