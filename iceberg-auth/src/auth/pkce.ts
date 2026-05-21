/**
 * PKCE helpers (RFC 7636). Cognito's hosted UI requires PKCE for public SPA
 * clients (no client secret). The verifier is generated locally, hashed into
 * the `code_challenge` sent on the /authorize redirect, and sent back to
 * /oauth2/token with the authorization code to prove possession.
 */

function base64UrlEncode(bytes: Uint8Array): string {
  let s = ''
  for (const b of bytes) s += String.fromCharCode(b)
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

export function randomVerifier(): string {
  const bytes = new Uint8Array(64)
  crypto.getRandomValues(bytes)
  return base64UrlEncode(bytes)
}

export async function challengeFromVerifier(verifier: string): Promise<string> {
  const data = new TextEncoder().encode(verifier)
  const digest = await crypto.subtle.digest('SHA-256', data)
  return base64UrlEncode(new Uint8Array(digest))
}
