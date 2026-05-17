/**
 * From a table URL, derive its parent (database) URL and basename.
 * Trailing slashes on the input are tolerated.
 */
export function parseTableUrl(tableUrl: string): { tableName: string; databaseUrl: string } {
  const trimmed = tableUrl.replace(/\/+$/, '')
  const lastSlash = trimmed.lastIndexOf('/')
  if (lastSlash < 0) return { tableName: trimmed, databaseUrl: '' }
  return {
    tableName: trimmed.slice(lastSlash + 1),
    databaseUrl: trimmed.slice(0, lastSlash + 1),
  }
}

/**
 * Quote a table name for SQL when it isn't a bare identifier.
 */
export function quoteIdentifier(name: string): string {
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) return name
  return `"${name.replace(/"/g, '""')}"`
}
