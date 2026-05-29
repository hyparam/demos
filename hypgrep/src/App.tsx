import { sortableDataFrame } from 'hightable'
import { byteLengthFromUrl, parquetMetadataAsync } from 'hyparquet'
import { asyncBufferFrom, parquetDataFrame } from 'hyperparam'
import { ReactNode, useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

const exampleUrl = 'https://s3.hyperparam.app/hypgrep/wiki_en100.parquet'
const welcomeDismissedKey = 'hypgrep-welcome-dismissed'

function hasDismissedWelcome(): boolean {
  return localStorage.getItem(welcomeDismissedKey) === '1'
}

function setWelcomeDismissed(): void {
  localStorage.setItem(welcomeDismissedKey, '1')
}

export default function App(): ReactNode {
  // Source parquet to grep: `?key=https://...` overrides the Wikipedia default
  // (e.g. a FineGrep shard in S3). The matching `.index.parquet` is inferred
  // downstream in Page.
  const sourceUrl = new URLSearchParams(location.search).get('key') ?? exampleUrl

  const [error, setError] = useState<Error>()
  const [pageProps, setPageProps] = useState<PageProps>()
  const [showWelcome, setShowWelcome] = useState(() => !hasDismissedWelcome())

  const closeWelcome = useCallback(() => {
    setWelcomeDismissed()
    setShowWelcome(false)
  }, [])

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)))
  }, [])

  useEffect(() => {
    let cancelled = false
    async function load() {
      const byteLength = await byteLengthFromUrl(sourceUrl)
      const from = { url: sourceUrl, byteLength }
      const asyncBuffer = await asyncBufferFrom(from)
      const metadata = await parquetMetadataAsync(asyncBuffer)
      const df = sortableDataFrame(parquetDataFrame(from, metadata))
      if (cancelled) return
      setPageProps({ metadata, df, name: sourceUrl, byteLength, setError: setUnknownError })
    }
    load().catch(setUnknownError)
    return () => { cancelled = true }
  }, [setUnknownError, sourceUrl])

  return <Layout error={error} onShowAbout={() => { setShowWelcome(true) }}>
    {pageProps && <Page {...pageProps} />}
    {showWelcome && <Welcome onClose={closeWelcome} />}
  </Layout>
}
