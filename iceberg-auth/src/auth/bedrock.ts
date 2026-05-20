import type { AwsCredentials } from './cognito.js'
import { config } from './config.js'
import { signRequest } from './sigv4.js'

/**
 * Bedrock Runtime InvokeModel for Anthropic Claude models. Bedrock's
 * Anthropic body format is the same Messages API as the direct Anthropic API,
 * minus the `model` field (the model is in the URL path) and with a required
 * `anthropic_version` field. The browser POSTs the signed request directly to
 * `bedrock-runtime.<region>.amazonaws.com` — no proxy.
 *
 * For long replies, switch to `invoke-with-response-stream` and parse the
 * AWS event-stream framing. We keep it simple with one-shot InvokeModel here
 * since these are short interactive prompts.
 */

export interface BedrockMessage {
  role: 'user' | 'assistant'
  content: string
}

export interface Tool {
  name: string
  description: string
  input_schema: {
    type: 'object'
    properties: Record<string, { type: string, description?: string }>
    required?: string[]
  }
}

export interface ToolUse {
  id: string
  name: string
  input: Record<string, unknown>
}

export interface InvokeOptions {
  credentials: AwsCredentials
  messages: BedrockMessage[]
  system?: string
  maxTokens?: number
  modelId?: string
  tools?: Tool[]
  /** Called for every tool_use block. Return a string to send back as tool_result. */
  onToolUse?: (call: ToolUse) => string | Promise<string>
}

type AnthropicBlock =
  | { type: 'text', text: string }
  | { type: 'tool_use', id: string, name: string, input: Record<string, unknown> }
  | { type: 'tool_result', tool_use_id: string, content: string }
interface AnthropicMessage { role: 'user' | 'assistant', content: AnthropicBlock[] | string }
interface AnthropicResponse {
  content?: AnthropicBlock[]
  stop_reason?: 'end_turn' | 'tool_use' | 'max_tokens' | 'stop_sequence'
  usage?: { input_tokens: number, output_tokens: number }
}

const MAX_TOOL_TURNS = 4

export async function invokeClaude(opts: InvokeOptions): Promise<string> {
  const {
    credentials, messages, system, maxTokens = 1024,
    modelId = config.bedrockModelId, tools, onToolUse,
  } = opts
  const url = `https://bedrock-runtime.${config.region}.amazonaws.com/model/${encodeURIComponent(modelId)}/invoke`

  // Convert our string-content history into Anthropic blocks. We'll append
  // assistant turns and tool_result user turns to this as the loop progresses.
  const convo: AnthropicMessage[] = messages.map(m => ({ role: m.role, content: m.content }))
  const textOut: string[] = []

  for (let turn = 0; turn < MAX_TOOL_TURNS; turn++) {
    const body = JSON.stringify({
      anthropic_version: 'bedrock-2023-05-31',
      max_tokens: maxTokens,
      system,
      tools,
      messages: convo,
    })

    const signed = await signRequest({
      credentials,
      region: config.region,
      service: 'bedrock',
      method: 'POST',
      url,
      body,
      headers: { 'content-type': 'application/json', accept: 'application/json' },
    })

    const res = await fetch(signed.url, {
      method: signed.method,
      headers: signed.headers,
      body: signed.body as BodyInit | undefined,
    })
    if (!res.ok) throw new Error(`Bedrock InvokeModel failed: ${res.status} ${await res.text()}`)
    const json = await res.json() as AnthropicResponse
    const blocks = json.content ?? []

    for (const b of blocks) {
      if (b.type === 'text') textOut.push(b.text)
    }

    if (json.stop_reason !== 'tool_use' || !onToolUse) {
      return textOut.join('').trim()
    }

    // Tool round: persist the assistant turn, run the tools, append a user turn
    // with the tool_results, and loop.
    convo.push({ role: 'assistant', content: blocks })
    const results: AnthropicBlock[] = []
    for (const b of blocks) {
      if (b.type !== 'tool_use') continue
      const result = await onToolUse({ id: b.id, name: b.name, input: b.input })
      results.push({ type: 'tool_result', tool_use_id: b.id, content: result })
    }
    convo.push({ role: 'user', content: results })
  }

  return textOut.join('').trim()
}
