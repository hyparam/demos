import { ReactNode, SubmitEvent, useCallback, useMemo, useRef, useState } from 'react'
import { type Tool, invokeClaude } from './auth/bedrock.js'
import type { AwsCredentials } from './auth/cognito.js'
import { config } from './auth/config.js'

interface Message {
  role: 'user' | 'assistant'
  content: string
}

/** "us.anthropic.claude-haiku-4-5-20251001-v1:0" → "claude-haiku-4-5". */
function shortModelName(id: string): string {
  return id.replace(/^(?:us|eu|apac)\./, '')
    .replace(/^anthropic\./, '')
    .replace(/-\d{8}-v\d+:\d+$/, '')
}

const SQL_QUERY_TOOL: Tool = {
  name: 'sql_query',
  description:
    'Replace the SQL editor contents with a new query. Use whenever the ' +
    'user wants to run, see, or modify a query against the Iceberg table.',
  input_schema: {
    type: 'object',
    properties: {
      query: { type: 'string', description: 'A full SQL SELECT statement.' },
    },
    required: ['query'],
  },
}

const SYSTEM_PROMPT =
  'You are a SQL assistant for an Iceberg table named `messages` with columns ' +
  '`id INT`, `user_prompt STRING`, `message STRING` (each row is one AI assistant reply). ' +
  'A user-defined function `LLM(...parts)` is available — it concatenates its ' +
  'string arguments and calls Claude on the resulting prompt, returning the ' +
  'model\'s reply as a string. Use `LLM(\'instruction…\', column)` to score or ' +
  'classify rows. The SQL dialect has no `||` operator; use multiple LLM args ' +
  'instead. Always use the `sql_query` tool to deliver queries — never paste ' +
  'SQL into chat. Keep queries short and use LIMIT.'

interface Props {
  credentials: AwsCredentials
  setQuery: (q: string | undefined) => void
}

/**
 * Tiny chat UI against Bedrock InvokeModel. State is in-memory only — refresh
 * clears the conversation. The whole message history is sent on every turn;
 * Bedrock's Anthropic adapter has no server-side memory.
 *
 * The `sql_query` tool lets Claude write into the SQL editor. The tool result
 * is just a confirmation; the actual side-effect is `setQuery(...)`.
 */
export default function LlmPanel({ credentials, setQuery }: Props): ReactNode {
  const [messages, setMessages] = useState<Message[]>([])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()
  const inputRef = useRef<HTMLTextAreaElement>(null)

  const tools = useMemo(() => [SQL_QUERY_TOOL], [])

  const onSubmit = useCallback((e: SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()
    const input = inputRef.current
    if (!input) return
    const text = input.value.trim()
    if (!text || busy) return
    input.value = ''
    setError(undefined)

    const next: Message[] = [...messages, { role: 'user', content: text }]
    setMessages(next)
    setBusy(true)
    invokeClaude({
      credentials,
      messages: next,
      system: SYSTEM_PROMPT,
      tools,
      onToolUse: call => {
        if (call.name === 'sql_query') {
          const q = call.input.query
          if (typeof q === 'string') {
            setQuery(q)
            return 'ok — set the editor to that query'
          }
          return 'error: missing string `query` arg'
        }
        return `error: unknown tool ${call.name}`
      },
    })
      .then(reply => {
        setMessages([...next, { role: 'assistant', content: reply || '(set the query)' }])
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err))
      })
      .finally(() => { setBusy(false) })
  }, [credentials, messages, busy, tools, setQuery])

  return <div className='llm-panel'>
    <div className='llm-header'>
      <span>Bedrock chat</span>
      <span className='llm-model' title={config.bedrockModelId}>{shortModelName(config.bedrockModelId)}</span>
    </div>
    <div className='llm-messages'>
      {messages.length === 0 && <div className='llm-empty'>
        Ask the model for a query — e.g. <em>&quot;show only the sycophantic replies&quot;</em>.
      </div>}
      {messages.map((m, i) => <div key={i} className={`llm-msg llm-msg-${m.role}`}>
        <span className='llm-role'>{m.role}</span>
        <span className='llm-content'>{m.content}</span>
      </div>)}
      {busy && <div className='llm-msg llm-msg-assistant'>
        <span className='llm-role'>assistant</span>
        <span className='llm-content llm-pending'>…</span>
      </div>}
      {error && <div className='llm-error'>{error}</div>}
    </div>
    <form onSubmit={onSubmit} className='llm-form'>
      <textarea
        ref={inputRef}
        placeholder='Type a message…'
        rows={2}
        disabled={busy}
        onKeyDown={e => {
          if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault()
            e.currentTarget.form?.requestSubmit()
          }
        }}
      />
      <button type='submit' disabled={busy}>Send</button>
    </form>
  </div>
}
