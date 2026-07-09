import HighTable, { CellContentProps } from 'hightable'
import { DataFrame, arrayDataFrame } from 'hightable/dataframe'
import { collect, createStore, grep, sql } from 'hypstore'
import { KeyboardEvent, ReactNode, useCallback, useEffect, useMemo, useState } from 'react'

export type Mode = 'sql' | 'grep'

const modes: { key: Mode, label: string }[] = [
  { key: 'sql', label: 'SQL' },
  { key: 'grep', label: 'Grep' },
]

const grepLimit = 100

function defaultQuery(mode: Mode, table: string): string {
  return mode === 'sql' ? `SELECT * FROM ${table} LIMIT 50` : ''
}

function exampleQueries(mode: Mode, table: string): string[] {
  if (mode === 'sql') {
    return [
      `SELECT model, COUNT(*) AS conversations FROM ${table} GROUP BY model ORDER BY conversations DESC`,
      `SELECT language, COUNT(*) AS conversations FROM ${table} GROUP BY language ORDER BY conversations DESC LIMIT 20`,
    ]
  }
  return ['minecraft', 'sourdough', 'time travel']
}

const placeholders: Record<Mode, string> = {
  sql: 'SELECT * FROM wildchat ... (press Enter to run)',
  grep: 'Find a substring across all rows...',
}

interface QueryResult {
  mode: Mode
  query: string
  rows: Record<string, unknown>[]
  columns: string[]
  queryTime: number
  firstRowTime?: number
}

interface PageProps {
  warehouseUrl: string
  table: string
  initialMode: Mode
  initialQuery?: string
  setError: (e: unknown) => void
}

export default function Page({ warehouseUrl, table, initialMode, initialQuery, setError }: PageProps): ReactNode {
  const [mode, setMode] = useState<Mode>(initialMode)
  const [queries, setQueries] = useState<Record<Mode, string>>(() => ({
    sql: defaultQuery('sql', table),
    grep: '',
    [initialMode]: initialQuery ?? defaultQuery(initialMode, table),
  }))
  const query = queries[mode]
  const [result, setResult] = useState<QueryResult>()
  const [queryError, setQueryError] = useState<string>()
  const [running, setRunning] = useState(false)
  // sql runs on demand (Enter / example click / tab switch), not per keystroke
  const [sqlToRun, setSqlToRun] = useState<{ query: string } | undefined>(() =>
    initialMode === 'sql' ? { query: initialQuery ?? defaultQuery('sql', table) } : undefined,
  )

  const store = useMemo(() => createStore({ warehouseUrl }), [warehouseUrl])

  // Keep the URL shareable: ?mode=grep&q=...
  useEffect(() => {
    const params = new URLSearchParams(location.search)
    params.set('mode', mode)
    if (query && query !== defaultQuery(mode, table)) params.set('q', query)
    else params.delete('q')
    history.replaceState({}, '', `${location.pathname}?${params}`)
  }, [mode, query, table])

  const execute = useCallback(async (mode: Mode, query: string, signal: AbortSignal) => {
    const startTime = performance.now()
    let firstRowTime: number | undefined
    let rows: Record<string, unknown>[] = []
    let columns: string[] | undefined
    if (mode === 'sql') {
      const results = await sql({ store, query })
      columns = results.columns
      rows = await collect(results)
    } else {
      for await (const row of grep({ store, table, query, limit: grepLimit })) {
        if (signal.aborted) return
        firstRowTime ??= performance.now() - startTime
        rows.push(row)
      }
    }
    if (signal.aborted) return
    columns ??= rows.length ? Object.keys(rows[0]) : []
    setResult({ mode, query, rows, columns, queryTime: performance.now() - startTime, firstRowTime })
  }, [store, table])

  const run = useCallback((mode: Mode, query: string, signal: AbortSignal) => {
    if (!query.trim()) {
      setResult(undefined)
      setQueryError(undefined)
      setRunning(false)
      return
    }
    setRunning(true)
    setQueryError(undefined)
    execute(mode, query, signal)
      .then(() => { if (!signal.aborted) setRunning(false) })
      .catch((e: unknown) => {
        if (signal.aborted) return
        setRunning(false)
        setResult(undefined)
        setQueryError(e instanceof Error ? e.message : String(e))
      })
  }, [execute])

  // Run sql on each explicit trigger (initial load, Enter, example, tab switch).
  useEffect(() => {
    if (!sqlToRun) return
    const controller = new AbortController()
    queueMicrotask(() => {
      if (!controller.signal.aborted) run('sql', sqlToRun.query, controller.signal)
    })
    return () => { controller.abort() }
  }, [sqlToRun, run])

  // Grep runs live as the query is typed, debounced.
  useEffect(() => {
    if (mode === 'sql') return
    const controller = new AbortController()
    const timer = window.setTimeout(() => { run(mode, query, controller.signal) }, 300)
    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [mode, query, run])

  const setQuery = useCallback((value: string) => {
    setQueries(queries => ({ ...queries, [mode]: value }))
  }, [mode])

  const selectMode = useCallback((next: Mode) => {
    setMode(next)
    if (next === 'sql') setSqlToRun({ query: queries.sql })
  }, [queries.sql])

  const onKeyDown = useCallback((e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && mode === 'sql') setSqlToRun({ query: queries.sql })
  }, [mode, queries.sql])

  const pickExample = useCallback((example: string) => {
    setQueries(queries => ({ ...queries, [mode]: example }))
    if (mode === 'sql') setSqlToRun({ query: example })
  }, [mode])

  const df = useMemo<DataFrame | undefined>(() => {
    if (!result?.rows.length) return undefined
    const columnDescriptors = result.columns.map(name => ({ name }))
    return arrayDataFrame(result.rows, undefined, { columnDescriptors })
  }, [result])

  // Highlight the matched substring in grep results.
  const renderCellContent = useCallback(({ cell, stringify }: CellContentProps) => {
    const value: unknown = cell?.value
    if (result?.mode !== 'grep' || typeof value !== 'string') return stringify(value)
    const index = value.toLowerCase().indexOf(result.query.toLowerCase())
    if (index < 0) return stringify(value)
    const truncateBefore = index > 20 ? '...' : ''
    return <>
      {truncateBefore}
      {value.slice(0, index).slice(-20)}
      <mark>{value.slice(index, index + result.query.length)}</mark>
      {value.slice(index + result.query.length)}
    </>
  }, [result])

  return <>
    <div className='top-header'>
      <span className='file-name'>{warehouseUrl}/{table}</span>
      <div className='view-meta'>
        {running && <span className='spinner' role='status' aria-label='Running' />}
        {result && <span className='matches'>{result.rows.length.toLocaleString()} row{result.rows.length === 1 ? '' : 's'}</span>}
        {result && <span>query: {result.queryTime.toFixed(0)} ms</span>}
        {result?.firstRowTime !== undefined && <span>first: {result.firstRowTime.toFixed(0)} ms</span>}
      </div>
    </div>
    <div className='toolbar'>
      <div className='tabs' role='tablist'>
        {modes.map(m =>
          <button
            key={m.key}
            role='tab'
            aria-selected={mode === m.key}
            className={mode === m.key ? 'tab active' : 'tab'}
            onClick={() => { selectMode(m.key) }}
          >{m.label}</button>,
        )}
      </div>
      <input
        className='query-input'
        type='text'
        placeholder={placeholders[mode]}
        autoFocus
        value={query}
        onChange={e => { setQuery(e.target.value) }}
        onKeyDown={onKeyDown}
      />
    </div>
    {queryError && <div className='query-error'>{queryError}</div>}
    {!df && !running && <div className='examples-row'>
      <span className='examples-label'>Try:</span>
      <div className='examples'>
        {exampleQueries(mode, table).map(example =>
          <button key={example} className='example-chip' onClick={() => { pickExample(example) }}>{example}</button>,
        )}
      </div>
    </div>}
    {df ?
      <HighTable
        focus={false}
        cacheKey={`${warehouseUrl}/${table}?${result?.mode ?? ''}:${result?.query ?? ''}`}
        className='hightable'
        data={df}
        onError={setError}
        renderCellContent={renderCellContent}
      /> :
      !running && !queryError && <div className='results-empty'>
        {result ? 'No matching rows.' : 'Type a query to explore the table.'}
      </div>}
  </>
}
