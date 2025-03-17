import { ReactNode } from 'react'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

import { DataFrame, asyncRows, rowCache } from 'hightable'
import { icebergLatestVersion, icebergMetadata, icebergRead } from 'icebird'
import { IcebergMetadata, Snapshot } from 'icebird/src/types.js'
import { useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [url, setUrl] = useState(queryUrl)
  const [version, setVersion] = useState<number>()
  const [numVersions, setNumVersions] = useState(0)

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)))
  }, [])

  useEffect(() => {
    // Get number of versions
    if (!url) return
    icebergLatestVersion(url)
      .then(version => {
        setNumVersions(version)
        setVersion(version)
      })
      .catch(setUnknownError)
  }, [url, setNumVersions, setUnknownError])

  useEffect(() => {
    if (!pageProps || !version) return
    setPageProps({ ...pageProps, version })
  }, [version])

  useEffect(() => {
    if (!url || !numVersions || !version) return
    // Get the metadata from the iceberg table
    const metadataFileName = `v${version}.metadata.json`
    icebergMetadata(url, metadataFileName).then((metadata: IcebergMetadata) => {
      const df = icebergDataFrame(url, metadataFileName, metadata)
      setPageProps({ df, metadata, numVersions, version, setVersion, setError })
    }).catch(setUnknownError)
  }, [numVersions, url, version, setUnknownError])

  const onUrlDrop = useCallback(
    (url: string) => {
      // Add key=url to query string
      const params = new URLSearchParams(location.search)
      params.set('key', url)
      history.pushState({}, '', `${location.pathname}?${params}`)
      setUrl(url)
    },
    [setUrl],
  )

  useEffect(() => {
    if (!pageProps && url) {
      onUrlDrop(url)
    }
  }, [ url, pageProps, onUrlDrop])

  return <Layout error={error}>
    {pageProps ? <Page {...pageProps} /> : <Welcome setUrl={setUrl} />}
  </Layout>
}

function icebergDataFrame(tableUrl: string, metadataFileName: string, metadata: IcebergMetadata): DataFrame {
  if (metadata.snapshots.length === 0) {
    throw new Error('No iceberg snapshots found')
  }
  const snapshot: Snapshot = metadata.snapshots[metadata.snapshots.length - 1]
  // Warning: this is not exactly the number of rows
  const numRows = Number(snapshot.summary['total-records'])
  const schema = metadata.schemas[metadata.schemas.length - 1]
  const header = schema.fields.map(f => f.name)
  return rowCache({
    header,
    numRows,
    rows({ start, end }) {
      const rows = icebergRead({ tableUrl, metadataFileName, rowStart: start, rowEnd: end })
        .then(rows => rows.map((cells, index) => ({ cells, index })))
      return asyncRows(rows, numRows, header)
    },
  })
}
