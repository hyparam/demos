import { config, redirectUri } from './config.js'
import { challengeFromVerifier, randomVerifier } from './pkce.js'

/**
 * Cognito hosted-UI OAuth (authorization code + PKCE) and Identity Pool
 * credential exchange. No AWS SDK — just the two REST endpoints we need:
 *
 *   POST <domain>/oauth2/token            → idToken/accessToken/refreshToken
 *   POST cognito-identity.<region>.../    → AWS access key + session token
 *
 * `signIn()` redirects to the hosted UI; the redirect target is the same SPA
 * URL with `?code=...&state=...`. `handleRedirectCallback()` consumes those
 * params on next load and resolves to a `Session`. Tokens and AWS credentials
 * are cached in localStorage and refreshed automatically before expiry.
 */

export interface AwsCredentials {
  accessKeyId: string
  secretAccessKey: string
  sessionToken: string
  /** Epoch ms when these credentials expire. */
  expiration: number
}

export interface CognitoTokens {
  idToken: string
  accessToken: string
  refreshToken?: string
  /** Epoch ms when the idToken/accessToken expire. */
  expiration: number
}

export interface IdTokenClaims {
  email?: string
  sub: string
  exp: number
  [key: string]: unknown
}

export interface Session {
  tokens: CognitoTokens
  credentials: AwsCredentials
  email: string
}

const TOKEN_STORAGE_KEY = 'iceberg-auth.tokens'
const CREDS_STORAGE_KEY = 'iceberg-auth.creds'
const PKCE_STORAGE_KEY = 'iceberg-auth.pkce'

interface StoredPkce {
  verifier: string
  state: string
}

/**
 * Begin the OAuth flow: stash a fresh PKCE verifier + random state in
 * sessionStorage and navigate to the hosted UI. The browser comes back to
 * `redirectUri()` with `?code=...&state=...`, which `handleRedirectCallback`
 * processes.
 */
export async function signIn(): Promise<void> {
  const verifier = randomVerifier()
  const state = randomVerifier().slice(0, 32)
  const challenge = await challengeFromVerifier(verifier)
  sessionStorage.setItem(PKCE_STORAGE_KEY, JSON.stringify({ verifier, state } satisfies StoredPkce))

  const params = new URLSearchParams({
    client_id: config.clientId,
    response_type: 'code',
    scope: 'openid email profile',
    redirect_uri: redirectUri(),
    state,
    code_challenge: challenge,
    code_challenge_method: 'S256',
  })
  location.assign(`${config.domain}/oauth2/authorize?${params.toString()}`)
}

/**
 * Hosted-UI logout. Cognito's `/logout` endpoint clears its session cookie and
 * redirects back to one of the registered logout URIs.
 */
export function signOutRedirect(): void {
  localStorage.removeItem(TOKEN_STORAGE_KEY)
  localStorage.removeItem(CREDS_STORAGE_KEY)
  const params = new URLSearchParams({
    client_id: config.clientId,
    logout_uri: redirectUri(),
  })
  location.assign(`${config.domain}/logout?${params.toString()}`)
}

/**
 * If the current URL contains an OAuth `code` (from a hosted-UI redirect),
 * exchange it for tokens and AWS credentials, then strip the query string so
 * a reload doesn't try to re-redeem the now-spent code. Returns the resulting
 * Session, or undefined if there was no code.
 *
 * Memoized so that concurrent callers (e.g. React StrictMode double-invoking
 * the mount effect in dev) share one in-flight exchange — otherwise the second
 * call finds the PKCE verifier already consumed and throws.
 */
let redirectCallbackPromise: Promise<Session | undefined> | undefined
export function handleRedirectCallback(): Promise<Session | undefined> {
  redirectCallbackPromise ??= doHandleRedirectCallback()
  return redirectCallbackPromise
}

async function doHandleRedirectCallback(): Promise<Session | undefined> {
  const params = new URLSearchParams(location.search)
  const code = params.get('code')
  const state = params.get('state')
  if (!code) return undefined

  const raw = sessionStorage.getItem(PKCE_STORAGE_KEY)
  if (!raw) throw new Error('Missing PKCE verifier; restart sign in')
  const stored = JSON.parse(raw) as StoredPkce
  if (state !== stored.state) throw new Error('OAuth state mismatch')
  sessionStorage.removeItem(PKCE_STORAGE_KEY)

  const tokens = await exchangeCodeForTokens(code, stored.verifier)
  saveTokens(tokens)
  const session = await buildSessionFromTokens(tokens)

  // Strip the OAuth params from the URL so a reload doesn't re-redeem the code.
  const url = new URL(location.href)
  url.searchParams.delete('code')
  url.searchParams.delete('state')
  history.replaceState({}, '', url.toString())

  return session
}

/**
 * Restore a previous session from localStorage if both tokens and credentials
 * are present and not expired. Auto-refreshes credentials when only those have
 * expired (cheap call), and the full token+creds chain when the idToken has.
 */
export async function restoreSession(): Promise<Session | undefined> {
  const tokens = loadTokens()
  if (!tokens) return undefined
  let liveTokens = tokens
  if (tokens.expiration < Date.now() + 60_000) {
    if (!tokens.refreshToken) return undefined
    try {
      liveTokens = await refreshTokens(tokens.refreshToken)
      saveTokens(liveTokens)
    } catch {
      clearSession()
      return undefined
    }
  }
  return await buildSessionFromTokens(liveTokens)
}

export function clearSession(): void {
  localStorage.removeItem(TOKEN_STORAGE_KEY)
  localStorage.removeItem(CREDS_STORAGE_KEY)
}

async function buildSessionFromTokens(tokens: CognitoTokens): Promise<Session> {
  const claims = decodeJwtClaims(tokens.idToken)
  const email = typeof claims.email === 'string' ? claims.email : ''
  if (!email) throw new Error('id token has no email claim — request the "email" scope on the app client')

  // Reuse cached creds if they're still valid; otherwise call Cognito Identity.
  let credentials = loadCredentials()
  if (!credentials || credentials.expiration < Date.now() + 60_000) {
    credentials = await fetchAwsCredentials(tokens.idToken)
    saveCredentials(credentials)
  }
  return { tokens, credentials, email }
}

async function exchangeCodeForTokens(code: string, verifier: string): Promise<CognitoTokens> {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: config.clientId,
    code,
    redirect_uri: redirectUri(),
    code_verifier: verifier,
  })
  const res = await fetch(`${config.domain}/oauth2/token`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  })
  if (!res.ok) throw new Error(`Token exchange failed: ${res.status} ${await res.text()}`)
  const json = await res.json() as {
    id_token: string
    access_token: string
    refresh_token?: string
    expires_in: number
  }
  return {
    idToken: json.id_token,
    accessToken: json.access_token,
    refreshToken: json.refresh_token,
    expiration: Date.now() + json.expires_in * 1000,
  }
}

async function refreshTokens(refreshToken: string): Promise<CognitoTokens> {
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    client_id: config.clientId,
    refresh_token: refreshToken,
  })
  const res = await fetch(`${config.domain}/oauth2/token`, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  })
  if (!res.ok) throw new Error(`Refresh failed: ${res.status} ${await res.text()}`)
  const json = await res.json() as {
    id_token: string
    access_token: string
    expires_in: number
  }
  // The refresh response usually omits refresh_token; keep the old one.
  return {
    idToken: json.id_token,
    accessToken: json.access_token,
    refreshToken,
    expiration: Date.now() + json.expires_in * 1000,
  }
}

/**
 * Exchange a Cognito User Pool id token for temporary AWS credentials via the
 * Cognito Identity service. Two calls:
 *
 *   GetId                              → identity id (idempotent for the user)
 *   GetCredentialsForIdentity          → access key + session token
 *
 * Both are simple JSON POSTs with `X-Amz-Target` headers and require no
 * signature (the id token in the Logins map is the credential).
 */
async function fetchAwsCredentials(idToken: string): Promise<AwsCredentials> {
  const loginKey = `cognito-idp.${config.region}.amazonaws.com/${config.userPoolId}`
  const endpoint = `https://cognito-identity.${config.region}.amazonaws.com/`

  const idRes = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-amz-json-1.1',
      'x-amz-target': 'AWSCognitoIdentityService.GetId',
    },
    body: JSON.stringify({
      IdentityPoolId: config.identityPoolId,
      Logins: { [loginKey]: idToken },
    }),
  })
  if (!idRes.ok) throw new Error(`GetId failed: ${idRes.status} ${await idRes.text()}`)
  const { IdentityId } = await idRes.json() as { IdentityId: string }

  const credRes = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-amz-json-1.1',
      'x-amz-target': 'AWSCognitoIdentityService.GetCredentialsForIdentity',
    },
    body: JSON.stringify({
      IdentityId,
      Logins: { [loginKey]: idToken },
    }),
  })
  if (!credRes.ok) throw new Error(`GetCredentialsForIdentity failed: ${credRes.status} ${await credRes.text()}`)
  const { Credentials } = await credRes.json() as {
    Credentials: {
      AccessKeyId: string
      SecretKey: string
      SessionToken: string
      Expiration: number
    }
  }
  return {
    accessKeyId: Credentials.AccessKeyId,
    secretAccessKey: Credentials.SecretKey,
    sessionToken: Credentials.SessionToken,
    // Cognito returns Expiration as seconds since epoch.
    expiration: Credentials.Expiration * 1000,
  }
}

function decodeJwtClaims(jwt: string): IdTokenClaims {
  const parts = jwt.split('.')
  if (parts.length < 2) throw new Error('Malformed JWT')
  const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/')
  const padded = payload + '='.repeat((4 - payload.length % 4) % 4)
  return JSON.parse(atob(padded)) as IdTokenClaims
}

function saveTokens(tokens: CognitoTokens): void {
  localStorage.setItem(TOKEN_STORAGE_KEY, JSON.stringify(tokens))
}

function loadTokens(): CognitoTokens | undefined {
  const raw = localStorage.getItem(TOKEN_STORAGE_KEY)
  if (!raw) return undefined
  try {
    return JSON.parse(raw) as CognitoTokens
  } catch {
    return undefined
  }
}

function saveCredentials(creds: AwsCredentials): void {
  localStorage.setItem(CREDS_STORAGE_KEY, JSON.stringify(creds))
}

function loadCredentials(): AwsCredentials | undefined {
  const raw = localStorage.getItem(CREDS_STORAGE_KEY)
  if (!raw) return undefined
  try {
    return JSON.parse(raw) as AwsCredentials
  } catch {
    return undefined
  }
}
