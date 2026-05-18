import { ReactNode, useCallback, useState } from 'react'
import Layout from './Layout.js'
import Page from './Page.js'
import Welcome from './Welcome.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined
  const initialQuery = params.get('q') ?? ''

  const [error, setError] = useState<Error>()
  const [tableUrl, setTableUrl] = useState(queryUrl)

  const setUnknownError = useCallback((e: unknown) => {
    if (e === undefined || e instanceof Error) {
      setError(e)
    } else {
      setError(new Error(typeof e === 'string' ? e : JSON.stringify(e)))
    }
  }, [])

  const setUrlAndHistory = useCallback((url: string) => {
    const next = new URLSearchParams(location.search)
    next.set('key', url)
    history.pushState({}, '', `${location.pathname}?${next}`)
    setTableUrl(url)
  }, [])

  return <Layout error={error}>
    {tableUrl
      ? <Page tableUrl={tableUrl} initialQuery={initialQuery} setError={setUnknownError} />
      : <Welcome setTableUrl={setUrlAndHistory} />}
  </Layout>
}
