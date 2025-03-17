import { ReactNode } from 'react'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

import { DataFrame, asyncRows, rowCache, sortableDataFrame } from 'hightable'
import { icebergLatestVersion, icebergMetadata, icebergRead } from 'icebird'
import { IcebergMetadata, Snapshot } from 'icebird/src/types.js'
import { useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [tableUrl, setTableUrl] = useState(queryUrl)
  const [version, setVersion] = useState<number>()
  const [numVersions, setNumVersions] = useState(0)

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)))
  }, [])

  useEffect(() => {
    // Get number of versions
    if (!tableUrl) return
    icebergLatestVersion({ tableUrl })
      .then(version => {
        setNumVersions(version)
        setVersion(version)
      })
      .catch(setUnknownError)
  }, [tableUrl, setNumVersions, setUnknownError])

  useEffect(() => {
    if (!pageProps || !version) return
    setPageProps({ ...pageProps, version })
  }, [pageProps, version])

  useEffect(() => {
    if (!tableUrl || !numVersions || !version) return
    // Get the metadata from the iceberg table
    const metadataFileName = `v${version}.metadata.json`
    icebergMetadata({ tableUrl: tableUrl, metadataFileName }).then((metadata: IcebergMetadata) => {
      const df = icebergDataFrame(tableUrl, metadataFileName, metadata)
      setPageProps({ df, metadata, numVersions, version, setVersion, setError })
    }).catch(setUnknownError)
  }, [numVersions, tableUrl, version, setUnknownError])

  const onUrlDrop = useCallback(
    (url: string) => {
      // Add key=url to query string
      const params = new URLSearchParams(location.search)
      params.set('key', url)
      history.pushState({}, '', `${location.pathname}?${params}`)
      setTableUrl(url)
    },
    [setTableUrl],
  )

  useEffect(() => {
    if (!pageProps && tableUrl) {
      onUrlDrop(tableUrl)
    }
  }, [ tableUrl, pageProps, onUrlDrop])

  return <Layout error={error}>
    {pageProps ? <Page {...pageProps} /> : <Welcome setTableUrl={setTableUrl} />}
  </Layout>
}

function icebergDataFrame(tableUrl: string, metadataFileName: string, metadata: IcebergMetadata): DataFrame {
  if (metadata.snapshots.length === 0) {
    throw new Error('No iceberg snapshots found')
  }
  const snapshot: Snapshot = metadata.snapshots[metadata.snapshots.length - 1]
  // Warning: this is not exactly the number of rows
  const numRows = Number(snapshot.summary['total-records'])
  const currentSchemaId = metadata['current-schema-id']
  const schema = metadata.schemas.find(s => s['schema-id'] === currentSchemaId)
  if (!schema) throw new Error('Current schema not found in metadata')
  const header = schema.fields.map(f => f.name)
  return sortableDataFrame(rowCache({
    header,
    numRows,
    rows({ start, end }) {
      const rows = icebergRead({ tableUrl, metadataFileName, metadata, rowStart: start, rowEnd: end })
        .then(rows => rows.map((cells, index) => ({ cells, index })))
      return asyncRows(rows, numRows, header)
    },
  }))
}
