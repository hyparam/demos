/**
 * Cognito + AWS config sourced from Vite env. Provisioned out-of-band; see README.
 *
 * - `domain` is the Cognito Hosted UI URL (no trailing slash), e.g.
 *   `https://hyperparam-demo.auth.us-east-1.amazoncognito.com`.
 * - `userPoolId` looks like `us-east-1_aBcDeFgHi`; it's not used by the OAuth
 *   flow but is needed when constructing the `cognito-idp.<region>.amazonaws.com/...`
 *   Logins map for Cognito Identity (GetId / GetCredentialsForIdentity).
 * - `identityPoolId` looks like `us-east-1:00000000-1111-2222-3333-444444444444`.
 * - `allowedEmail` is a client-side gate for clarity only. Real enforcement is
 *   in the User Pool (only the whitelisted user can sign in) and the IAM role
 *   trust policy attached to the Identity Pool.
 */
export const config = {
  region: import.meta.env.VITE_AWS_REGION,
  domain: import.meta.env.VITE_COGNITO_DOMAIN.replace(/\/+$/, ''),
  clientId: import.meta.env.VITE_COGNITO_CLIENT_ID,
  userPoolId: import.meta.env.VITE_COGNITO_USER_POOL_ID,
  identityPoolId: import.meta.env.VITE_COGNITO_IDENTITY_POOL_ID,
  allowedEmail: import.meta.env.VITE_ALLOWED_EMAIL,
  s3Bucket: import.meta.env.VITE_S3_BUCKET,
  s3TablePrefix: import.meta.env.VITE_S3_TABLE_PREFIX ?? '',
  // Claude 4.x models on Bedrock require a cross-region inference profile (the
  // `us.` / `eu.` / `apac.` prefix); the bare model ID returns "on-demand
  // throughput isn't supported." Override via env if you need a different region.
  bedrockModelId: import.meta.env.VITE_BEDROCK_MODEL_ID ?? 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
}

/**
 * The OAuth redirect URI must match exactly what's registered as a callback URL
 * in the Cognito app client. We use the page itself so the SPA can pick up
 * `?code=...` on load without a separate /callback route.
 */
export function redirectUri(): string {
  return location.origin + location.pathname
}
