import { ReactNode, useCallback } from 'react'
import { signIn } from './auth/cognito.js'
import { config } from './auth/config.js'

interface Props {
  error?: string
}

export default function SignIn({ error }: Props): ReactNode {
  const onClick = useCallback(() => {
    signIn().catch((err: unknown) => { console.error(err) })
  }, [])

  return <div id="welcome">
    <div>
      <h1>Iceberg-auth demo</h1>
      <h2>private Iceberg + Bedrock LLM from the browser</h2>
      <p>
        Sign in with the whitelisted account to receive temporary AWS credentials.
        The same credentials read Iceberg tables from <code>s3://{config.s3Bucket}</code>
        and call Bedrock InvokeModel directly from the browser — no proxy server.
      </p>
      <p>
        Only <code>{config.allowedEmail}</code> is permitted to sign in. The check is enforced
        by the Cognito User Pool and the IAM role attached to the Identity Pool.
      </p>
      <div className='inputGroup'>
        <button onClick={onClick}>Sign in with Cognito</button>
      </div>
      {error && <p className='auth-error'>{error}</p>}
    </div>
  </div>
}
