import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { icebergDataSource, icebergMetadata } from 'icebird'
import type { Snapshot, TableMetadata } from 'icebird/src/types.js'
import type { AsyncDataSource } from 'squirreling'
import { parseTableUrl } from './database.js'
import Layout from './Layout.js'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined
  const initialQuery = params.get('query') ?? undefined

  const [error, setError] = useState<Error>()
  const [tableUrl, setTableUrl] = useState(queryUrl)
  const [metadata, setMetadata] = useState<TableMetadata>()
  const [snapshots, setSnapshots] = useState<Snapshot[]>()
  const [snapshotId, setSnapshotId] = useState<bigint>()

  const { tableName } = useMemo(
    () => tableUrl ? parseTableUrl(tableUrl) : { tableName: '' },
    [tableUrl],
  )

  const setUnknownError = useCallback((e: unknown) => {
    if (e === undefined || e instanceof Error) {
      setError(e)
    } else {
      setError(new Error(typeof e === 'string' ? e : JSON.stringify(e)))
    }
  }, [])

  // Load the latest metadata once per tableUrl.
  useEffect(() => {
    if (!tableUrl || metadata) return
    icebergMetadata({ tableUrl })
      .then(md => {
        if (!md.snapshots?.length) throw new Error('No iceberg snapshots found')
        const sorted = [...md.snapshots].sort((a, b) => a['timestamp-ms'] - b['timestamp-ms'])
        setMetadata(md)
        setSnapshots(sorted)
        const current = md['current-snapshot-id']
        const initial = current ?? sorted[sorted.length - 1]['snapshot-id']
        setSnapshotId(BigInt(initial))
      })
      .catch(setUnknownError)
  }, [tableUrl, metadata, setUnknownError])

  // Cache one AsyncDataSource per snapshotId so slider backtracking is free.
  // Reset when the table or metadata changes — caches are scoped to a
  // specific metadata fetch.
  const sourceCache = useRef(new Map<string, Promise<AsyncDataSource>>())
  useEffect(() => {
    sourceCache.current = new Map()
  }, [tableUrl, metadata])

  const [dataSource, setDataSource] = useState<AsyncDataSource>()

  // Build (or fetch from cache) the data source for the selected snapshot.
  useEffect(() => {
    if (!tableUrl || !metadata || !snapshots || snapshotId === undefined) return
    let cancelled = false
    const cache = sourceCache.current
    const key = snapshotId.toString()
    let promise = cache.get(key)
    if (!promise) {
      promise = icebergDataSource({ tableUrl, metadata, snapshotId })
      cache.set(key, promise)
    }
    promise
      .then((source: AsyncDataSource) => {
        if (cancelled) return
        setDataSource(source)
      })
      .catch((err: unknown) => {
        // Don't cache failures - let the next attempt retry.
        cache.delete(key)
        setUnknownError(err)
      })
    return () => { cancelled = true }
  }, [tableUrl, metadata, snapshots, snapshotId, setUnknownError])

  const setUrlAndHistory = useCallback(
    (url: string) => {
      const params = new URLSearchParams(location.search)
      params.set('key', url)
      history.pushState({}, '', `${location.pathname}?${params}`)
      setTableUrl(url)
    },
    [setTableUrl],
  )

  // Slider position (`snapshotId`) updates instantly on input; `dataSource`
  // lags by however long the icebergDataSource build takes for that snapshot
  // (cached → microtask, uncached → manifest fetch). The Page renders both.
  const pageProps: PageProps | undefined =
    tableUrl && metadata && snapshots && snapshotId !== undefined && dataSource
      ? {
        tableUrl,
        tableName,
        metadata,
        snapshots,
        snapshotId,
        setSnapshotId,
        dataSource,
        initialQuery,
        setError: setUnknownError,
      }
      : undefined

  return <Layout error={error}>
    {pageProps ? <Page {...pageProps} /> : <Welcome setTableUrl={setUrlAndHistory} />}
  </Layout>
}
