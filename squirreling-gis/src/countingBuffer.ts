import type { AsyncBuffer } from 'hyparquet'

export interface ByteRange {
  start: number
  end: number
}

/**
 * Wraps an AsyncBuffer to track downloaded byte ranges.
 */
export function countingBuffer(
  asyncBuffer: AsyncBuffer,
  onFetch?: (ranges: ByteRange[]) => void,
): AsyncBuffer {
  const downloadedRanges: ByteRange[] = []

  function addRange(start: number, end: number) {
    // Insert and merge overlapping/adjacent ranges
    const newRange = { start, end }
    const merged: ByteRange[] = []
    let inserted = false

    for (const range of downloadedRanges) {
      if (range.end < newRange.start) {
        // Range is before new range
        merged.push(range)
      } else if (range.start > newRange.end) {
        // Range is after new range
        if (!inserted) {
          merged.push(newRange)
          inserted = true
        }
        merged.push(range)
      } else {
        // Ranges overlap or are adjacent, merge them
        newRange.start = Math.min(newRange.start, range.start)
        newRange.end = Math.max(newRange.end, range.end)
      }
    }

    if (!inserted) {
      merged.push(newRange)
    }

    downloadedRanges.length = 0
    downloadedRanges.push(...merged)
    onFetch?.(downloadedRanges)
  }

  return {
    ...asyncBuffer,
    byteLength: asyncBuffer.byteLength,
    slice(start: number, end?: number) {
      const actualEnd = end ?? asyncBuffer.byteLength
      addRange(start, actualEnd)
      return asyncBuffer.slice(start, end)
    },
  }
}

export type DownloadStatus = 'pending' | 'partial' | 'downloaded'

/**
 * Check the download status of a byte range.
 * Returns 'downloaded' if fully downloaded, 'partial' if partly downloaded, 'pending' if not started.
 */
export function getRangeStatus(ranges: ByteRange[], start: number, end: number): DownloadStatus {
  let downloadedBytes = 0
  const totalBytes = end - start

  for (const range of ranges) {
    // Calculate overlap between the chunk and this downloaded range
    const overlapStart = Math.max(start, range.start)
    const overlapEnd = Math.min(end, range.end)
    if (overlapStart < overlapEnd) {
      downloadedBytes += overlapEnd - overlapStart
    }
  }

  if (downloadedBytes >= totalBytes) {
    return 'downloaded'
  } else if (downloadedBytes > 0) {
    return 'partial'
  }
  return 'pending'
}
