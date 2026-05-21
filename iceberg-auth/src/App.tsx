import { ReactNode, useCallback, useEffect, useState } from 'react'
import { config } from './auth/config.js'
import type { PopupSessionPayload, Session } from './auth/cognito.js'
import {
  POPUP_ERROR_TYPE,
  POPUP_MESSAGE_TYPE,
  handleRedirectCallback,
  importSession,
  postErrorToOpenerAndClose,
  restoreSession,
  signOutRedirect,
} from './auth/cognito.js'
import Layout from './Layout.js'
import LlmPanel from './LlmPanel.js'
import Page from './Page.js'
import SignIn from './SignIn.js'

/**
 * Top-level state machine:
 *
 *   loading → (no creds)         → SignIn
 *           → (?code= in URL)    → exchange + show Page
 *           → (stored session)   → show Page
 *
 * Sign-out blows away local tokens/creds and hands control back to Cognito's
 * `/logout`, which clears its own session cookie before bouncing the user back
 * to the SignIn screen.
 */
export default function App(): ReactNode {
  const [session, setSession] = useState<Session>()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<Error>()
  const [authError, setAuthError] = useState<string>()

  const setUnknownError = useCallback((e: unknown) => {
    if (e === undefined || e instanceof Error) {
      setError(e)
    } else {
      setError(new Error(typeof e === 'string' ? e : JSON.stringify(e)))
    }
  }, [])

  // In iframe mode signOutRedirect just clears storage (it can't navigate to
  // Cognito /logout) — so we have to reset state too. In top mode the
  // setSession is harmless: the page is about to unload.
  const onSignOut = useCallback(() => {
    signOutRedirect()
    setSession(undefined)
  }, [])

  // On first mount: drain any OAuth callback params, then try to restore.
  useEffect(() => {
    const ctl = new AbortController()
    handleRedirectCallback()
      .then(fresh => {
        if (ctl.signal.aborted) return undefined
        if (fresh) {
          if (fresh.email.toLowerCase() !== config.allowedEmail.toLowerCase()) {
            setAuthError(`Account ${fresh.email} is not whitelisted for this demo.`)
            return undefined
          }
          setSession(fresh)
          return undefined
        }
        return restoreSession()
      })
      .then(restored => {
        if (ctl.signal.aborted || !restored) return
        setSession(restored)
      })
      .catch((e: unknown) => {
        if (ctl.signal.aborted) return
        // If we're a popup that hit an error after the Cognito redirect, hand
        // the error back to the opener (the iframe) and close ourselves.
        postErrorToOpenerAndClose(e)
        setAuthError(e instanceof Error ? e.message : String(e))
      })
      .finally(() => { if (!ctl.signal.aborted) setLoading(false) })
    return () => { ctl.abort() }
  }, [])

  // Iframe-mode receiver: when the sign-in popup completes the OAuth exchange
  // it posts the tokens/credentials back here. Our localStorage is partitioned
  // away from the popup's, so we have to persist them ourselves.
  useEffect(() => {
    function onMessage(e: MessageEvent<{ type?: string; session?: PopupSessionPayload; error?: string }>) {
      if (e.origin !== location.origin) return
      const { data } = e
      if (data.type === POPUP_MESSAGE_TYPE && data.session) {
        const { tokens, credentials, email } = data.session
        if (email.toLowerCase() !== config.allowedEmail.toLowerCase()) {
          setAuthError(`Account ${email} is not whitelisted for this demo.`)
          return
        }
        importSession(data.session)
        setSession({ tokens, credentials, email })
        setAuthError(undefined)
      } else if (data.type === POPUP_ERROR_TYPE && data.error) {
        setAuthError(data.error)
      }
    }
    window.addEventListener('message', onMessage)
    return () => { window.removeEventListener('message', onMessage) }
  }, [])

  // Re-check credential expiry every minute; refresh via restoreSession,
  // which transparently rotates the AWS creds (and tokens if needed).
  useEffect(() => {
    if (!session) return
    const id = setInterval(() => {
      if (session.credentials.expiration > Date.now() + 5 * 60_000) return
      restoreSession()
        .then(next => { if (next) setSession(next) })
        .catch((e: unknown) => { console.warn('refresh failed', e) })
    }, 60_000)
    return () => { clearInterval(id) }
  }, [session])

  const params = new URLSearchParams(location.search)
  const queryUrl = params.get('key') ?? defaultTableUrl()
  // Hoisted so the LlmPanel's `sql_query` tool can replace the editor contents.
  // `undefined` means "fall back to Page's DEFAULT_QUERY".
  const [query, setQuery] = useState<string | undefined>(() => params.get('query') ?? undefined)

  // Mirror the query into ?query=... when the table URL is also pinned (?key=),
  // so the page is shareable. No write if we're at the default location.
  useEffect(() => {
    const p = new URLSearchParams(location.search)
    if (!p.has('key')) return
    if (query) p.set('query', query)
    else p.delete('query')
    history.replaceState({}, '', `${location.pathname}?${p}`)
  }, [query])

  if (loading) {
    return <Layout><div id="welcome"><div><h1>Loading…</h1></div></div></Layout>
  }

  if (!session) {
    return <Layout><SignIn error={authError} /></Layout>
  }

  return <Layout error={error}>
    <div className='session-bar'>
      <span>Signed in as <strong>{session.email}</strong></span>
      <button onClick={onSignOut}>Sign out</button>
    </div>
    <div className='split'>
      <div className='split-main'>
        <Page
          databaseUrl={queryUrl}
          query={query}
          setQuery={setQuery}
          credentials={session.credentials}
          setError={setUnknownError}
        />
      </div>
      <div className='split-side'>
        <LlmPanel credentials={session.credentials} setQuery={setQuery} />
      </div>
    </div>
  </Layout>
}

/**
 * Default database URL points at the configured private bucket + optional
 * prefix where Iceberg tables live (e.g. `s3://hyperparam-private/warehouse`).
 * The hosted-style HTTPS URL is what icebird's signed resolver actually fetches.
 */
function defaultTableUrl(): string {
  const prefix = config.s3TablePrefix.replace(/^\/+|\/+$/g, '')
  const base = `https://${config.s3Bucket}.s3.${config.region}.amazonaws.com`
  return prefix ? `${base}/${prefix}` : base
}
