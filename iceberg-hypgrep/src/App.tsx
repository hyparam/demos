import { ReactNode, useCallback, useState } from 'react'
import Layout from './Layout.js'
import Page from './Page.js'
import Welcome from './Welcome.js'

const exampleUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/iceberg-hypgrep/llm_logs'
const welcomeDismissedCookie = 'iceberg-hypgrep-welcome-dismissed'

function hasDismissedWelcome(): boolean {
  return document.cookie.split('; ').some(c => c.startsWith(`${welcomeDismissedCookie}=`))
}

function setWelcomeDismissed(): void {
  const oneYear = 60 * 60 * 24 * 365
  document.cookie = `${welcomeDismissedCookie}=1; max-age=${oneYear}; path=/; SameSite=Lax`
}

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const initialQuery = params.get('q') ?? ''

  const [error, setError] = useState<Error>()
  const [showWelcome, setShowWelcome] = useState(() => !hasDismissedWelcome())

  const closeWelcome = useCallback(() => {
    setWelcomeDismissed()
    setShowWelcome(false)
  }, [])

  const setUnknownError = useCallback((e: unknown) => {
    if (e === undefined || e instanceof Error) {
      setError(e)
    } else {
      setError(new Error(typeof e === 'string' ? e : JSON.stringify(e)))
    }
  }, [])

  return <Layout error={error} onShowAbout={() => { setShowWelcome(true) }}>
    <Page tableUrl={exampleUrl} initialQuery={initialQuery} setError={setUnknownError} />
    {showWelcome && <Welcome onClose={closeWelcome} />}
  </Layout>
}
