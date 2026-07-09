import { ReactNode, useCallback, useState } from 'react'
import Layout from './Layout.js'
import Page, { Mode } from './Page.js'
import Welcome from './Welcome.js'

const warehouseUrl = 's3://hyperparam-public/hypstore'
const table = 'wildchat'
const welcomeDismissedKey = 'hypstore-welcome-dismissed'

function isMode(value: unknown): value is Mode {
  return value === 'sql' || value === 'grep'
}

function hasDismissedWelcome(): boolean {
  return localStorage.getItem(welcomeDismissedKey) === '1'
}

function setWelcomeDismissed(): void {
  localStorage.setItem(welcomeDismissedKey, '1')
}

export default function App(): ReactNode {
  const params = new URLSearchParams(location.search)
  const modeParam = params.get('mode')
  const initialMode: Mode = isMode(modeParam) ? modeParam : 'sql'
  const initialQuery = params.get('q') ?? undefined

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
    <Page
      warehouseUrl={warehouseUrl}
      table={table}
      initialMode={initialMode}
      initialQuery={initialQuery}
      setError={setUnknownError}
    />
    {showWelcome && <Welcome onClose={closeWelcome} />}
  </Layout>
}
