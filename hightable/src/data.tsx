import { DataFrameEvents, ResolvedValue, UnsortableDataFrame, createEventTarget, sortableDataFrame } from 'hightable'

export function checkSignal(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new DOMException('The operation was aborted.', 'AbortError')
  }
}

function lorem(rand: number, length: number): string {
  const words = 'lorem ipsum dolor sit amet consectetur adipiscing elit'.split(' ')
  const str = Array.from({ length }, (_, i) => words[Math.floor(i + rand * 8) % 8]).join(' ')
  return str[0].toUpperCase() + str.slice(1)
}

function delayResolve<T>({ value, ms, signal }: {value: T, ms?: number, signal?: AbortSignal}): Promise<ResolvedValue<T>> {
  return new Promise(resolve => setTimeout(
    () => {
      checkSignal(signal)
      resolve({ value })
    },
    ms ?? 100 * Math.floor(10 * Math.random()),
  ))
}

const numRows = 10_000
const header = ['ID', 'Name', 'Age', 'UUID', 'Text', 'JSON']
const eventTarget = createEventTarget<DataFrameEvents>()
const cellCache = new Map<string, ResolvedValue[]>(header.map(column => [column, []]))
const rowNumberCache : ResolvedValue<number>[] = []

function generateValue({ row, column }: { row: number, column: string }): string | number {
  switch (column) {
  case 'ID':
    return row + 1
  case 'Name':
    return `Name${row}`
  case 'Age':
    return 20 + row % 80
  case 'UUID':
    return crypto.randomUUID()
  case 'Text':
    return lorem( Math.abs(Math.sin(row + 1)), 10)
  case 'JSON':
    return JSON.stringify({ row, column })
  default:
    throw new Error(`Unknown column: ${column}`)
  }}

const mockData: UnsortableDataFrame = {
  header,
  numRows,
  getCell: ({ row, column }) => {
    return cellCache.get(column)?.[row]
  },
  getRowNumber: ({ row }) => {
    return rowNumberCache[row]
  },
  fetch: async ({ rowEnd, rowStart, columns, signal }) => {
    checkSignal(signal)
    const promises: Promise<void>[] = []
    for (let row = rowStart; row < rowEnd; row++) {
      // fetch row number
      if (!rowNumberCache[row]) {
        promises.push(
          delayResolve({ value: row, signal })
            .then(resolved => {
              rowNumberCache[row] = resolved
              eventTarget.dispatchEvent(new CustomEvent('resolve'))
            }),
        )
      }

      // fetch cells
      for (const column of columns ?? []) {
        if (!header.includes(column)) {
          throw new Error(`Unknown column: ${column}`)
        }
        if (!cellCache.get(column)?.[row]) {
          promises.push(
            delayResolve({ value: generateValue({ row, column }), signal })
              .then(resolved => {
                const columnCache = cellCache.get(column)
                if (!columnCache) {
                  throw new Error(`Column cache not found for: ${column}`)
                }
                columnCache[row] = resolved
                eventTarget.dispatchEvent(new CustomEvent('resolve'))
              }),
          )
        }
      }
    }

    await Promise.all(promises)
  },
  eventTarget,
}

export const data = sortableDataFrame(mockData)
