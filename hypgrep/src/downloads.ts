import { useSyncExternalStore } from 'react'

/**
 * Tracks how many bytes have been downloaded per url, by wrapping global fetch.
 * Range requests report their size in content-length, so partial parquet reads
 * are counted accurately. HEAD requests (byte length probes) are ignored.
 */
const downloaded = new Map<string, number>()
const listeners = new Set<() => void>()

function add(url: string, bytes: number): void {
  downloaded.set(url, (downloaded.get(url) ?? 0) + bytes)
  for (const listener of listeners) listener()
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  return () => { listeners.delete(listener) }
}

let patched = false

/**
 * Wrap globalThis.fetch to count downloaded bytes. Safe to call more than once.
 */
export function trackDownloads(): void {
  if (patched) return
  patched = true
  const originalFetch = globalThis.fetch.bind(globalThis)
  globalThis.fetch = async function trackedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const response = await originalFetch(input, init)
    const method = init?.method ?? (input instanceof Request ? input.method : 'GET')
    if (method !== 'HEAD') {
      const url = input instanceof Request ? input.url : input.toString()
      const bytes = Number(response.headers.get('content-length'))
      if (bytes > 0) add(url, bytes)
    }
    return response
  }
}

/**
 * Number of bytes downloaded so far for a url.
 *
 * @param {string} [url] url to watch
 * @returns {number} bytes downloaded
 */
export function useDownloaded(url?: string): number {
  return useSyncExternalStore(subscribe, () => url ? downloaded.get(url) ?? 0 : 0)
}
