import { ReactNode } from 'react'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

import { icebergListVersions, icebergMetadata } from 'icebird'
import type { TableMetadata } from 'icebird/src/types.js'
import { useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'
import { buildIcebergDataSource } from './data.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined
  const initialQuery = params.get('query') ?? undefined

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [tableUrl, setTableUrl] = useState(queryUrl)
  const [version, setVersion] = useState<string>()
  const [versions, setVersions] = useState<string[] | undefined>()

  const setUnknownError = useCallback((e: unknown) => {
    setError(e === undefined || e instanceof Error ? e : new Error(String(e)))
  }, [])

  useEffect(() => {
    if (!tableUrl || versions) return
    icebergListVersions({ tableUrl })
      .then(versions => {
        setVersions(versions)
        if (versions.length === 0) throw new Error('No iceberg metadata versions found')
        setVersion(versions[versions.length - 1])
      })
      .catch(setUnknownError)
  }, [tableUrl, versions, setUnknownError])

  if (pageProps && version && pageProps.version !== version) {
    setPageProps({ ...pageProps, version })
  }

  useEffect(() => {
    if (!tableUrl || !versions || !version) return
    let cancelled = false
    const metadataFileName = `${version}.metadata.json`
    icebergMetadata({ tableUrl, metadataFileName })
      .then(async (metadata: TableMetadata) => {
        if (!metadata.snapshots?.length) {
          throw new Error('No iceberg snapshots found')
        }
        const dataSource = await buildIcebergDataSource(tableUrl, metadata)
        if (cancelled) return
        setPageProps({
          tableUrl,
          metadata,
          dataSource,
          versions,
          version,
          setVersion,
          initialQuery,
          setError: setUnknownError,
        })
      })
      .catch(setUnknownError)
    return () => { cancelled = true }
  }, [tableUrl, versions, version, initialQuery, setUnknownError])

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
