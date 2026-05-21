import type { AwsCredentials } from './cognito.js'

/**
 * Minimal AWS SigV4 signer for arbitrary services. Icebird ships a SigV4 signer
 * but it's S3-only (hard-coded service name); for Bedrock we need to sign with
 * `service: 'bedrock'`, so we keep this small standalone signer here.
 *
 * Pattern follows the AWS docs almost line-for-line, using Web Crypto.
 */

const enc = new TextEncoder()

export interface SignedRequest {
  url: string
  headers: Record<string, string>
  body?: Uint8Array
  method: string
}

export interface SignOptions {
  credentials: AwsCredentials
  region: string
  service: string
  method: string
  url: string
  body?: Uint8Array | string
  headers?: Record<string, string>
}

export async function signRequest(opts: SignOptions): Promise<SignedRequest> {
  const { credentials, region, service, method, url, headers = {} } = opts
  const body = typeof opts.body === 'string' ? enc.encode(opts.body)
    : opts.body ?? new Uint8Array()

  const u = new URL(url)
  const now = new Date()
  const xAmzDate = now.toISOString().replace(/[-:]|\.\d{3}/g, '')
  const dStamp = xAmzDate.slice(0, 8)
  const payloadHash = await sha256hex(body)

  const lc: Record<string, string> = {}
  for (const [k, v] of Object.entries(headers)) lc[k.toLowerCase()] = v
  lc.host = u.host
  lc['x-amz-date'] = xAmzDate
  lc['x-amz-content-sha256'] = payloadHash
  if (credentials.sessionToken) lc['x-amz-security-token'] = credentials.sessionToken

  const sortedKeys = Object.keys(lc).sort()
  const canonicalHeaders = sortedKeys
    .map(k => `${k}:${lc[k].trim().replace(/\s+/g, ' ')}\n`)
    .join('')
  const signedHeaders = sortedKeys.join(';')

  // Non-S3 services require the canonical URI path segments to be URI-encoded twice.
  const canonicalUri = u.pathname
    .split('/')
    .map(seg => encodeRfc3986(encodeRfc3986(decodeURIComponent(seg))))
    .join('/')

  const params = [...u.searchParams.entries()].sort((a, b) => {
    if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1
    return a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0
  })
  const canonicalQuery = params
    .map(([k, v]) => `${encodeRfc3986(k)}=${encodeRfc3986(v)}`)
    .join('&')

  const canonicalRequest = [
    method, canonicalUri, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash,
  ].join('\n')
  const credentialScope = `${dStamp}/${region}/${service}/aws4_request`
  const stringToSign = [
    'AWS4-HMAC-SHA256', xAmzDate, credentialScope, await sha256hex(canonicalRequest),
  ].join('\n')

  const signingKey = await deriveSigningKey(credentials.secretAccessKey, dStamp, region, service)
  const sigBytes = await hmac(signingKey, stringToSign)
  const signature = bytesToHex(sigBytes)

  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(lc)) {
    if (k === 'host') continue // fetch sets host itself
    out[k] = v
  }
  out.Authorization = `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
  return { url, method, headers: out, body: body.length ? body : undefined }
}

async function sha256hex(data: string | Uint8Array): Promise<string> {
  const bytes = typeof data === 'string' ? enc.encode(data) : data
  const hash = await crypto.subtle.digest('SHA-256', bytes as BufferSource)
  return bytesToHex(new Uint8Array(hash))
}

async function hmac(key: string | Uint8Array, data: string | Uint8Array): Promise<Uint8Array> {
  const keyBytes = typeof key === 'string' ? enc.encode(key) : key
  const dataBytes = typeof data === 'string' ? enc.encode(data) : data
  const cryptoKey = await crypto.subtle.importKey(
    'raw', keyBytes as BufferSource, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  )
  const sig = await crypto.subtle.sign('HMAC', cryptoKey, dataBytes as BufferSource)
  return new Uint8Array(sig)
}

async function deriveSigningKey(
  secret: string, dateStamp: string, region: string, service: string,
): Promise<Uint8Array> {
  const kDate = await hmac(`AWS4${secret}`, dateStamp)
  const kRegion = await hmac(kDate, region)
  const kService = await hmac(kRegion, service)
  return await hmac(kService, 'aws4_request')
}

function encodeRfc3986(str: string): string {
  return encodeURIComponent(str).replace(
    /[!*'()]/g,
    c => '%' + c.charCodeAt(0).toString(16).toUpperCase(),
  )
}

function bytesToHex(bytes: Uint8Array): string {
  let s = ''
  for (const b of bytes) s += b.toString(16).padStart(2, '0')
  return s
}
