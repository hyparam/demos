import { config } from './config.js'

/**
 * Cognito User Pool sign-in via the `InitiateAuth` API (USER_PASSWORD_AUTH
 * flow), plus the Identity Pool credential exchange. No AWS SDK and no Hosted
 * UI — three REST endpoints:
 *
 *   POST cognito-idp.<region>.../   AWSCognitoIdentityProviderService.InitiateAuth
 *     → idToken / accessToken / refreshToken (USER_PASSWORD_AUTH)
 *     → idToken / accessToken             (REFRESH_TOKEN_AUTH)
 *   POST cognito-identity.<region>.../    → IdentityId, then AWS creds
 *
 * The User Pool app client must have `ALLOW_USER_PASSWORD_AUTH` enabled.
 * Tokens and AWS credentials are cached in localStorage and refreshed before
 * expiry.
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

const IDP_ENDPOINT = `https://cognito-idp.${config.region}.amazonaws.com/`

interface CognitoError {
  __type?: string
  message?: string
}

/**
 * Sign in with email/password against Cognito's User Pool directly. Returns a
 * full Session (tokens + temporary AWS creds + email). The User Pool app
 * client must have ALLOW_USER_PASSWORD_AUTH enabled.
 */
export async function signInWithPassword(email: string, password: string): Promise<Session> {
  const tokens = await initiateAuth(email, password)
  saveTokens(tokens)
  return await buildSessionFromTokens(tokens)
}

/**
 * Restore a previous session from localStorage if tokens are present and live.
 * Auto-refreshes credentials if only those have expired (cheap), and the full
 * token+creds chain when the idToken has.
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

/**
 * Sign-out. There is no server-side session to invalidate — the User Pool
 * tokens just expire on their own — so this is a local-state clear.
 */
export function signOut(): void {
  clearSession()
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

interface AuthenticationResult {
  IdToken: string
  AccessToken: string
  RefreshToken?: string
  ExpiresIn: number
}

async function initiateAuth(email: string, password: string): Promise<CognitoTokens> {
  const json = await postIdp<{
    AuthenticationResult?: AuthenticationResult
    ChallengeName?: string
  }>('AWSCognitoIdentityProviderService.InitiateAuth', {
    AuthFlow: 'USER_PASSWORD_AUTH',
    ClientId: config.clientId,
    AuthParameters: { USERNAME: email, PASSWORD: password },
  })
  if (!json.AuthenticationResult) {
    throw new Error(`Unsupported auth challenge: ${json.ChallengeName ?? 'unknown'}`)
  }
  return tokensFromResult(json.AuthenticationResult)
}

async function refreshTokens(refreshToken: string): Promise<CognitoTokens> {
  const json = await postIdp<{ AuthenticationResult: AuthenticationResult }>(
    'AWSCognitoIdentityProviderService.InitiateAuth',
    {
      AuthFlow: 'REFRESH_TOKEN_AUTH',
      ClientId: config.clientId,
      AuthParameters: { REFRESH_TOKEN: refreshToken },
    },
  )
  // The refresh response omits refresh_token; keep the old one.
  return { ...tokensFromResult(json.AuthenticationResult), refreshToken }
}

function tokensFromResult(r: AuthenticationResult): CognitoTokens {
  return {
    idToken: r.IdToken,
    accessToken: r.AccessToken,
    refreshToken: r.RefreshToken,
    expiration: Date.now() + r.ExpiresIn * 1000,
  }
}

async function postIdp<T>(target: string, body: unknown): Promise<T> {
  const res = await fetch(IDP_ENDPOINT, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-amz-json-1.1',
      'x-amz-target': target,
    },
    body: JSON.stringify(body),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(parseCognitoError(text) ?? `${target} failed: ${res.status} ${text}`)
  return JSON.parse(text) as T
}

function parseCognitoError(text: string): string | undefined {
  try {
    const err = JSON.parse(text) as CognitoError
    return err.message ?? err.__type
  } catch {
    return undefined
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
