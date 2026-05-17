import { ReactNode, useCallback, useState } from 'react'
import Layout from './Layout.js'
import Page from './Page.js'
import Welcome from './Welcome.js'

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? undefined
  const initialQuery = params.get('query') ?? undefined

  const [error, setError] = useState<Error>()
  const [databaseUrl, setDatabaseUrl] = useState(queryUrl)

  const setUnknownError = useCallback((e: unknown) => {
    if (e === undefined || e instanceof Error) {
      setError(e)
    } else {
      setError(new Error(typeof e === 'string' ? e : JSON.stringify(e)))
    }
  }, [])

  const setUrlAndHistory = useCallback((url: string) => {
    const params = new URLSearchParams(location.search)
    params.set('key', url)
    history.pushState({}, '', `${location.pathname}?${params}`)
    setDatabaseUrl(url)
  }, [])

  return <Layout error={error}>
    {databaseUrl
      ? <Page databaseUrl={databaseUrl} initialQuery={initialQuery} setError={setUnknownError} />
      : <Welcome setTableUrl={setUrlAndHistory} />}
  </Layout>
}
