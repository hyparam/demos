/**
 * Cognito + AWS config sourced from Vite env. Provisioned out-of-band; see README.
 *
 * - `clientId` is the User Pool app client. Must have ALLOW_USER_PASSWORD_AUTH
 *   and ALLOW_REFRESH_TOKEN_AUTH enabled, no client secret.
 * - `userPoolId` looks like `us-east-1_aBcDeFgHi`; used in the
 *   `cognito-idp.<region>.amazonaws.com/<userPoolId>` Logins key for Cognito
 *   Identity (GetId / GetCredentialsForIdentity).
 * - `identityPoolId` looks like `us-east-1:00000000-1111-2222-3333-444444444444`.
 * - `allowedEmail` is a client-side gate for clarity only. Real enforcement is
 *   in the User Pool (only the whitelisted user can sign in) and the IAM role
 *   trust policy attached to the Identity Pool.
 */
export const config = {
  region: import.meta.env.VITE_AWS_REGION,
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
