/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_AWS_REGION: string
  readonly VITE_COGNITO_DOMAIN: string
  readonly VITE_COGNITO_CLIENT_ID: string
  readonly VITE_COGNITO_USER_POOL_ID: string
  readonly VITE_COGNITO_IDENTITY_POOL_ID: string
  readonly VITE_ALLOWED_EMAIL: string
  readonly VITE_S3_BUCKET: string
  readonly VITE_S3_TABLE_PREFIX?: string
  readonly VITE_BEDROCK_MODEL_ID?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
