import { ReactNode, useCallback, useState } from 'react'
import Layout from './Layout.js'
import Page from './Page.js'
import Welcome from './Welcome.js'

const welcomeDismissedKey = 'hypvector-welcome-dismissed'

function hasDismissedWelcome(): boolean {
  return localStorage.getItem(welcomeDismissedKey) === '1'
}

function setWelcomeDismissed(): void {
  localStorage.setItem(welcomeDismissedKey, '1')
}

export default function App(): ReactNode {
  const [error, setError] = useState<Error>()
  const [showWelcome, setShowWelcome] = useState(() => !hasDismissedWelcome())

  const closeWelcome = useCallback(() => {
    setWelcomeDismissed()
    setShowWelcome(false)
  }, [])

  const setUnknownError = useCallback((e: unknown) => {
    setError(e instanceof Error ? e : new Error(String(e)))
  }, [])

  return <Layout error={error} onShowAbout={() => { setShowWelcome(true) }}>
    <Page setError={setUnknownError} />
    {showWelcome && <Welcome onClose={closeWelcome} />}
  </Layout>
}
