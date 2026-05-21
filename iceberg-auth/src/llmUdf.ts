import type { SqlPrimitive, UserDefinedFunction } from 'squirreling'
import { invokeClaude } from './auth/bedrock.js'
import type { AwsCredentials } from './auth/cognito.js'

/**
 * SQL UDF that calls Bedrock InvokeModel once per input value. One Bedrock
 * round-trip per row, so it's slow and costs real money on large result sets —
 * always wrap in LIMIT.
 *
 * Credentials are read through a getter so the closure stays valid across the
 * hourly Cognito refresh without re-registering the UDF (which would otherwise
 * invalidate the parse-time memo and re-run the query).
 *
 * Usage: `SELECT user_id, LLM('one-word sentiment: ' || comment) AS s FROM events LIMIT 50`
 */
export function llmFunctions(
  getCredentials: () => AwsCredentials,
): Record<string, UserDefinedFunction> {
  return {
    LLM: {
      // Variadic: all args are stringified and concatenated to form the prompt.
      // Lets callers compose without `||` (squirreling has no string-concat op).
      arguments: { min: 1, signature: '...parts' },
      apply: async (...parts) => {
        if (parts.some(p => p === null)) return null
        const prompt = parts.map(stringify).join('')
        const reply = await invokeClaude({
          credentials: getCredentials(),
          messages: [{ role: 'user', content: prompt }],
          maxTokens: 256,
        })
        return reply.trim()
      },
    },
  }
}

function stringify(value: SqlPrimitive): string {
  if (typeof value === 'string') return value
  if (value === null || typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
