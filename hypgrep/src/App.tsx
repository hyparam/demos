import { sortableDataFrame } from 'hightable'
import { byteLengthFromUrl, parquetMetadataAsync } from 'hyparquet'
import { asyncBufferFrom, parquetDataFrame } from 'hyperparam'
import { ReactNode, useCallback, useEffect, useState } from 'react'
import Layout from './Layout.js'
import Page, { PageProps } from './Page.js'
import Welcome from './Welcome.js'

const exampleUrl = 'https://s3.hyperparam.app/hypgrep/wiki_en100.parquet'
const welcomeDismissedCookie = 'hypgrep-welcome-dismissed'

function hasDismissedWelcome(): boolean {
  return document.cookie.split('; ').some(c => c.startsWith(`${welcomeDismissedCookie}=`))
}

function setWelcomeDismissed(): void {
  const oneYear = 60 * 60 * 24 * 365
  document.cookie = `${welcomeDismissedCookie}=1; max-age=${oneYear}; path=/; SameSite=Lax`
}

export default function App(): ReactNode {
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
      const byteLength = await byteLengthFromUrl(exampleUrl)
      const from = { url: exampleUrl, byteLength }
      const asyncBuffer = await asyncBufferFrom(from)
      const metadata = await parquetMetadataAsync(asyncBuffer)
      const df = sortableDataFrame(parquetDataFrame(from, metadata))
      if (cancelled) return
      setPageProps({ metadata, df, name: exampleUrl, byteLength, setError: setUnknownError })
    }
    load().catch(setUnknownError)
    return () => { cancelled = true }
  }, [setUnknownError])

  return <Layout error={error} onShowAbout={() => { setShowWelcome(true) }}>
    {pageProps && <Page {...pageProps} />}
    {showWelcome && <Welcome onClose={closeWelcome} />}
  </Layout>
}
