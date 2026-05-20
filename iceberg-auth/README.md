# Iceberg-auth demo

Browser-only demo that combines:

- **Cognito OAuth** (hosted UI + PKCE) gating access to a single whitelisted user
- **Cognito Identity Pool** issuing temporary AWS credentials to the browser
- **Icebird** reading an Apache Iceberg table from a **private S3 bucket** via SigV4
- **Bedrock InvokeModel** called directly from the browser using the same creds

The Iceberg query UI is the same as the [icebird demo](../icebird), with all S3
reads SigV4-signed by the user's STS credentials. A side panel chats with a
Claude model on Bedrock — every prompt is a SigV4-signed POST from the browser.

There is **no backend** — only Cognito, IAM, S3, and Bedrock.

## Build

```bash
cp .env.example .env.local   # then fill in the VITE_* values
npm i
npm run build
```

The build artifacts land in `dist/` and can be served with any static server.

## AWS setup

You provision these once, out of band:

### 1. Cognito User Pool

- Create a User Pool with email sign-in.
- Create the one whitelisted user (`kenny@hyperparam.app`) — either by inviting
  them or by federating a corporate IdP and restricting allowed emails.
- Under **App integration**:
  - Set a hosted-UI **Domain prefix** → this becomes `VITE_COGNITO_DOMAIN`.
  - Create a **public app client** (no client secret).
  - Enable **Authorization code grant** + scopes `openid`, `email`, `profile`.
  - Add the deployed app URL (e.g. `https://hyparam.github.io/demos/iceberg-auth/`)
    and `http://localhost:5173/` to both **Allowed callback URLs** and
    **Allowed sign-out URLs**.

### 2. Cognito Identity Pool

- Create an Identity Pool **with authentication providers**: pick the User Pool
  + app client above. Disable unauthenticated identities.
- Cognito will offer to create two IAM roles (auth / unauth). Replace the
  authenticated role's policy with the JSON below.

### 3. IAM policy on the authenticated role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadPrivateBucket",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::hyperparam-private",
        "arn:aws:s3:::hyperparam-private/*"
      ]
    },
    {
      "Sid": "InvokeClaudeOnBedrock",
      "Effect": "Allow",
      "Action": ["bedrock:InvokeModel"],
      "Resource": [
        "arn:aws:bedrock:us-east-1:<account-id>:inference-profile/us.anthropic.claude-*",
        "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-*",
        "arn:aws:bedrock:us-east-2::foundation-model/anthropic.claude-*",
        "arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-*"
      ]
    }
  ]
}
```

Claude 4.x on Bedrock can't be invoked on-demand by its bare foundation-model
ID — you must call it through a **cross-region inference profile** (e.g.
`us.anthropic.claude-haiku-4-5-20251001-v1:0`). The IAM policy therefore needs
to allow both the inference-profile ARN (account-scoped) and the underlying
foundation-model ARNs in every region the profile can route to. For the `us.`
profile group that's us-east-1, us-east-2, and us-west-2.

For belt-and-suspenders enforcement of the single-user rule, tighten the trust
policy to require that `sub` claim:

```json
{
  "Condition": {
    "StringEquals": {
      "cognito-identity.amazonaws.com:aud": "us-east-1:<identity-pool-id>",
      "cognito-identity.amazonaws.com:sub": "<the-one-allowed-user-sub>"
    },
    "ForAnyValue:StringLike": {
      "cognito-identity.amazonaws.com:amr": "authenticated"
    }
  }
}
```

### 4. S3 CORS on `hyperparam-private`

```json
[
  {
    "AllowedHeaders": ["*"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedOrigins": [
      "https://hyparam.github.io",
      "http://localhost:5173"
    ],
    "ExposeHeaders": ["ETag", "x-amz-request-id"],
    "MaxAgeSeconds": 3000
  }
]
```

### 5. Bedrock model access

In the Bedrock console, under **Model access**, request access to the Claude
foundation model in every region the inference profile routes to (for `us.*`
profiles: us-east-1, us-east-2, us-west-2). Then set `VITE_BEDROCK_MODEL_ID`
to the inference-profile ID, **not** the bare foundation-model ID — e.g.
`us.anthropic.claude-haiku-4-5-20251001-v1:0`. Calling the bare ID returns
`Invocation … with on-demand throughput isn't supported`.

Bedrock's runtime endpoint allows CORS for SigV4-signed requests from any
origin, so no proxy is needed.

## How auth flows

1. Click **Sign in** → PKCE redirect to `<domain>/oauth2/authorize`.
2. After login, Cognito redirects back to this app with `?code=...&state=...`.
3. The SPA POSTs the code to `<domain>/oauth2/token`, receives id/access/refresh.
4. The id token is exchanged for AWS creds via Cognito Identity:
   - `GetId` → `IdentityId`
   - `GetCredentialsForIdentity` → `{ AccessKeyId, SecretKey, SessionToken, Expiration }`
5. The creds are cached in `localStorage` and used to:
   - Sign every S3 read (icebird's `s3SignedResolver`)
   - Sign every Bedrock `InvokeModel` POST
6. The app refreshes creds in the background before they expire.

The whitelisted-email check in `App.tsx` is a UX nicety only — real enforcement
is the User Pool only having one user, and the IAM role trust policy.
