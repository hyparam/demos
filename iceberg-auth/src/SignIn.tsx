import { ReactNode, SyntheticEvent, useCallback, useState } from 'react'
import type { Session } from './auth/cognito.js'
import { signInWithPassword } from './auth/cognito.js'
import { config } from './auth/config.js'

interface Props {
  onSession: (s: Session) => void
}

export default function SignIn({ onSession }: Props): ReactNode {
  const [email, setEmail] = useState(config.allowedEmail)
  const [password, setPassword] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()

  const onSubmit = useCallback((e: SyntheticEvent<HTMLFormElement>) => {
    e.preventDefault()
    setBusy(true)
    setError(undefined)
    signInWithPassword(email.trim(), password)
      .then(session => {
        if (session.email.toLowerCase() !== config.allowedEmail.toLowerCase()) {
          setError(`Account ${session.email} is not whitelisted for this demo.`)
          return
        }
        onSession(session)
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err))
      })
      .finally(() => { setBusy(false) })
  }, [email, password, onSession])

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
      <form className='signin-form' onSubmit={onSubmit}>
        <input
          type='email'
          value={email}
          onChange={e => { setEmail(e.target.value) }}
          placeholder='email'
          autoComplete='username'
          required
        />
        <input
          type='password'
          value={password}
          onChange={e => { setPassword(e.target.value) }}
          placeholder='password'
          autoComplete='current-password'
          required
        />
        <button type='submit' disabled={busy || !password}>
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
      </form>
      {error && <p className='auth-error'>{error}</p>}
    </div>
  </div>
}
