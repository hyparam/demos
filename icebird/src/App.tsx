import { ReactNode } from 'react'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

import type { DataFrame } from 'hightable'
import { icebergListVersions, icebergMetadata } from 'icebird'
import type { TableMetadata } from 'icebird/src/types.js'
import { useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'
import { icebergDataFrame } from './data.js'

const empty: DataFrame = {
  columnDescriptors: [],
  numRows: 0,
  getRowNumber: () => undefined,
  getCell: () => undefined,
}

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [tableUrl, setTableUrl] = useState(queryUrl)
  const [version, setVersion] = useState<string>()
  const [versions, setVersions] = useState<string[] | undefined>()

  const setUnknownError = useCallback((e: unknown) => {
    if (e instanceof Error && e.message === 'No iceberg snapshots found') {
      console.warn('No iceberg snapshots found for version', version)
      setPageProps(props => props ? { ...props, df: empty } : undefined)
    } else {
      setError(e instanceof Error ? e : new Error(String(e)))
    }
  }, [version])

  useEffect(() => {
    // List metadata versions
    if (!tableUrl || versions) return
    icebergListVersions({ tableUrl })
      .then(versions => {
        setVersions(versions)
        if (versions.length === 0) throw new Error('No iceberg metadata versions found')
        setVersion(versions[versions.length - 1])
      })
      .catch(setUnknownError)
  }, [tableUrl, versions, setVersions, setUnknownError])

  if (pageProps && version && pageProps.version !== version) {
    setPageProps({ ...pageProps, version })
  }

  useEffect(() => {
    if (!tableUrl || !versions || !version) return
    // Get the metadata from the iceberg table
    const metadataFileName = `${version}.metadata.json`
    icebergMetadata({ tableUrl: tableUrl, metadataFileName }).then((metadata: TableMetadata) => {
      const df = icebergDataFrame(tableUrl, metadataFileName, metadata)
      setPageProps({ df, metadata, versions, version, setVersion, setError: setUnknownError })
    }).catch(setUnknownError)
  }, [tableUrl, versions, version, setUnknownError])

  const setUrlAndHistory = useCallback(
    (url: string) => {
      // Add key=url to query string
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
