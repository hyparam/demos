import { ReactNode, useCallback, useEffect, useState } from 'react'
import { icebergDataSource, icebergMetadata } from 'icebird'
import type { Snapshot, TableMetadata } from 'icebird/src/types.js'
import type { AsyncDataSource } from 'squirreling'
import Layout from './Layout.js'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined
  const initialQuery = params.get('query') ?? undefined

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [tableUrl, setTableUrl] = useState(queryUrl)
  const [metadata, setMetadata] = useState<TableMetadata>()
  const [snapshots, setSnapshots] = useState<Snapshot[]>()
  const [snapshotId, setSnapshotId] = useState<bigint>()

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

  // Build a fresh data source whenever the selected snapshot changes.
  useEffect(() => {
    if (!tableUrl || !metadata || !snapshots || snapshotId === undefined) return
    let cancelled = false
    icebergDataSource({ tableUrl, metadata, snapshotId })
      .then((dataSource: AsyncDataSource) => {
        if (cancelled) return
        setPageProps({
          tableUrl,
          metadata,
          snapshots,
          snapshotId,
          setSnapshotId,
          dataSource,
          initialQuery,
          setError: setUnknownError,
        })
      })
      .catch(setUnknownError)
    return () => { cancelled = true }
  }, [tableUrl, metadata, snapshots, snapshotId, initialQuery, setUnknownError])

  const setUrlAndHistory = useCallback(
    (url: string) => {
      const params = new URLSearchParams(location.search)
      params.set('key', url)
      history.pushState({}, '', `${location.pathname}?${params}`)
      setTableUrl(url)
    },
    [setTableUrl],
  )

  return <Layout error={error}>
    {pageProps ? <Page {...pageProps} /> : <Welcome setTableUrl={setUrlAndHistory} />}
  </Layout>
}
