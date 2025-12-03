import type { AsyncBuffer } from 'hyparquet'

/**
 * Wraps an AsyncBuffer to count the number of fetches and total bytes fetched.
 */
export function countingBuffer(asyncBuffer: AsyncBuffer): AsyncBuffer & { fetches: number; bytes: number } {
  const wrapper = {
    ...asyncBuffer,
    fetches: 0,
    bytes: 0,
    slice(start: number, end?: number) {
      wrapper.fetches++
      wrapper.bytes += (end ?? asyncBuffer.byteLength) - start
      return asyncBuffer.slice(start, end)
    },
  }
  return wrapper
}
