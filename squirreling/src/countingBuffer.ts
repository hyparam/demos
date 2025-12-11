import type { AsyncBuffer } from 'hyparquet'

/**
 * Wraps an AsyncBuffer to count the number of fetches and total bytes fetched.
 */
export function countingBuffer(asyncBuffer: AsyncBuffer, onFetch?: (start: number, end?: number) => void): AsyncBuffer {
  return {
    ...asyncBuffer,
    slice(start: number, end?: number) {
      onFetch?.(start, end)
      return asyncBuffer.slice(start, end)
    },
  }
}
