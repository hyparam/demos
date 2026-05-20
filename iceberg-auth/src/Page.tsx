import HighTable, { ColumnConfiguration, DataFrame } from 'hightable'
import { icebergDataSource, icebergMetadata, s3SignedResolver } from 'icebird'
import type { Resolver } from 'icebird/src/types.js'
import type { IcebergType, TableMetadata } from 'icebird/src/types.js'
import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { AsyncDataSource, executeSql, extractTables, parseSql } from 'squirreling'
import type { AwsCredentials } from './auth/cognito.js'
import { config } from './auth/config.js'
import { HighlightedTextArea } from './HighlightedTextArea.js'
import { llmFunctions } from './llmUdf.js'
import { highlightSql } from './sqlHighlight.js'
import { squirrelingDataFrame } from './squirrelingDataFrame.js'

interface SqlErrorInfo {
  message: string
  positionStart?: number
  positionEnd?: number
}

export interface PageProps {
  databaseUrl: string
  query?: string
  setQuery: (q: string | undefined) => void
  credentials: AwsCredentials
  setError: (e: unknown) => void
}

const DEFAULT_QUERY = `SELECT
  id,
  message,
  LLM('Rate how sycophantic this AI assistant reply is on a 0-10 scale (0 = direct/honest, 10 = extremely flattering and uncritical). Reply with only the integer. Reply: ', message) AS sycophancy
FROM messages
ORDER BY id`

function formatIcebergType(type: IcebergType): string {
  if (typeof type === 'string') return type
  if (type.type === 'struct') return 'struct'
  if (type.type === 'list') return `list<${formatIcebergType(type.element)}>`
  return `map<${formatIcebergType(type.key)}, ${formatIcebergType(type.value)}>`
}

const empty: DataFrame = {
  columnDescriptors: [],
  numRows: 0,
  getRowNumber: () => undefined,
  getCell: () => undefined,
}

/**
 * Same query page as the icebird demo, but every read flows through a
 * SigV4-signing `s3SignedResolver` built from the current Cognito session's
 * temp credentials. The resolver is rebuilt whenever credentials rotate
 * (Cognito Identity returns ~1h creds; the App refreshes them in the
 * background and passes the new ones in via props).
 */
export default function Page({
  databaseUrl,
  query: queryProp,
  setQuery,
  credentials,
  setError,
}: PageProps): ReactNode {
  const query = queryProp ?? DEFAULT_QUERY
  const [queryDf, setQueryDf] = useState<DataFrame>(empty)
  const [queryTime, setQueryTime] = useState<number | undefined>()
  const [firstRowTime, setFirstRowTime] = useState<number | undefined>()
  const [runtimeError, setRuntimeError] = useState<SqlErrorInfo | undefined>()
  // Bumped on numrowschange events so we re-read queryDf.numRows in render
  const [, forceUpdate] = useState(0)

  const resolver = useMemo<Resolver>(() => s3SignedResolver({
    accessKeyId: credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
    sessionToken: credentials.sessionToken,
    region: config.region,
  }), [credentials])

  // Stable across credential rotation so the UDF map identity doesn't change
  // and force re-parse / re-execute. The getter is only invoked when the UDF
  // runs (inside async apply), never during render.
  const credentialsRef = useRef(credentials)
  useEffect(() => { credentialsRef.current = credentials }, [credentials])
  // eslint-disable-next-line react-hooks/refs
  const functions = useMemo(() => llmFunctions(() => credentialsRef.current), [])

  const highlights = useMemo(() => highlightSql(query), [query])

  const parseResult = useMemo((): {
    parsedQuery?: ReturnType<typeof parseSql>
    refs: string[]
    parseError?: SqlErrorInfo
  } => {
    try {
      const parsed = parseSql({ query, functions })
      return { parsedQuery: parsed, refs: extractTables(parsed) }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      return { refs: [], parseError: { message, positionStart, positionEnd } }
    }
  }, [query, functions])
  const { parsedQuery, refs, parseError } = parseResult
  const sqlError = parseError ?? runtimeError

  const firstRef = refs[0]
  const firstRefUrl = useMemo(
    () => firstRef ? databaseUrl.replace(/\/+$/, '') + '/' + firstRef : undefined,
    [databaseUrl, firstRef],
  )

  const [metadata, setMetadata] = useState<TableMetadata>()

  useEffect(() => {
    if (!firstRefUrl) return
    let cancelled = false
    queueMicrotask(() => {
      if (cancelled) return
      setMetadata(undefined)
    })
    icebergMetadata({ tableUrl: firstRefUrl, resolver })
      .then(md => {
        if (cancelled) return
        if (!md.snapshots?.length) throw new Error('No iceberg snapshots found')
        setMetadata(md)
      })
      .catch((err: unknown) => { if (!cancelled) setError(err) })
    return () => { cancelled = true }
  }, [firstRefUrl, resolver, setError])

  const [firstDataSource, setFirstDataSource] = useState<AsyncDataSource>()

  useEffect(() => {
    if (!firstRefUrl || !metadata) return
    let cancelled = false
    icebergDataSource({ tableUrl: firstRefUrl, metadata, resolver })
      .then(source => { if (!cancelled) setFirstDataSource(source) })
      .catch((err: unknown) => { if (!cancelled) setError(err) })
    return () => { cancelled = true }
  }, [firstRefUrl, metadata, resolver, setError])

  const sourceColumns = useMemo(
    () => firstDataSource?.columns ?? [],
    [firstDataSource],
  )

  const columnTypes = useMemo(() => {
    const map = new Map<string, string>()
    if (!metadata) return map
    const schema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
    if (!schema) return map
    for (const field of schema.fields) {
      map.set(field.name, formatIcebergType(field.type))
    }
    return map
  }, [metadata])

  const columnConfiguration = useMemo<ColumnConfiguration>(() => {
    const cfg: ColumnConfiguration = {}
    for (const { name } of queryDf.columnDescriptors) {
      const type = columnTypes.get(name)
      cfg[name] = {
        headerComponent: controls => <div className='col-header-row'>
          <div className='col-header'>
            <span className='col-name'>{name}</span>
            {type && <span className='col-type'>{type}</span>}
          </div>
          {controls}
        </div>,
      }
    }
    return cfg
  }, [queryDf, columnTypes])

  const handleQueryChange = useCallback((newQuery: string) => {
    setQueryTime(undefined)
    setFirstRowTime(undefined)
    setQuery(newQuery === DEFAULT_QUERY ? undefined : newQuery)
    setError(undefined)
    setRuntimeError(undefined)
  }, [setError, setQuery])

  useEffect(() => {
    if (parseError || !parsedQuery) {
      queueMicrotask(() => { setQueryDf(empty) })
      return
    }
    if (refs.length && !firstDataSource) return

    const abortController = new AbortController()
    queueMicrotask(() => {
      if (!abortController.signal.aborted) setQueryDf(empty)
    })

    const stmt = parsedQuery
    async function run() {
      // Yield once so the `setQueryDf(empty)` microtask above lands before
      // we set the new DF — otherwise a synchronous path (cached data source)
      // sets results first and the microtask immediately wipes them.
      await Promise.resolve()
      const sources: Record<string, AsyncDataSource> = {}
      for (const ref of refs) {
        if (ref === firstRef && firstDataSource) {
          sources[ref] = firstDataSource
        } else {
          const tableUrl = databaseUrl.replace(/\/+$/, '') + '/' + ref
          sources[ref] = await icebergDataSource({ tableUrl, resolver })
        }
      }
      if (abortController.signal.aborted) return
      const results = executeSql({
        tables: sources,
        query: stmt,
        functions,
        signal: abortController.signal,
      })
      const resultsDf = squirrelingDataFrame({
        rowGen: results.rows(),
        query: stmt,
        sourceColumns,
      })
      setQueryDf(resultsDf)
    }

    run().catch((err: unknown) => {
      if (abortController.signal.aborted) return
      const message = err instanceof Error ? err.message : String(err)
      const { positionStart, positionEnd } = err as { positionStart?: number, positionEnd?: number }
      setRuntimeError({ message, positionStart, positionEnd })
      setQueryDf(empty)
    })

    return () => { abortController.abort() }
  }, [parsedQuery, parseError, refs, firstRef, firstDataSource, databaseUrl, resolver, sourceColumns, functions])

  useEffect(() => {
    const target = queryDf.eventTarget
    if (!target) return
    const startTime = performance.now()
    let firstRowTracked = false
    function onNumRowsChange() {
      forceUpdate(c => c + 1)
      if (!firstRowTracked) {
        firstRowTracked = true
        setFirstRowTime(performance.now() - startTime)
      }
    }
    function onResolve() {
      setQueryTime(performance.now() - startTime)
    }
    target.addEventListener('numrowschange', onNumRowsChange)
    target.addEventListener('resolve', onResolve)
    return () => {
      target.removeEventListener('numrowschange', onNumRowsChange)
      target.removeEventListener('resolve', onResolve)
    }
  }, [queryDf])

  return <>
    <div className='top-header'>
      <span className='file-name'>{databaseUrl}</span>
    </div>
    <div className='sql-container'>
      <div className='sql-input-area'>
        <HighlightedTextArea
          value={query}
          onChange={handleQueryChange}
          placeholder='SQL query...'
          className={sqlError ? 'sql-error' : ''}
          highlights={highlights}
          errorStart={sqlError?.positionStart}
          errorEnd={sqlError?.positionEnd}
        />
        <div className='query-stats'>
          {sqlError && <span className='sql-error-msg'>{sqlError.message}</span>}
          <span className='query-times'>
            {queryTime !== undefined && <span>query: {queryTime.toFixed(0)} ms</span>}
            {firstRowTime !== undefined && <span>first: {firstRowTime.toFixed(0)} ms</span>}
          </span>
        </div>
      </div>
    </div>
    <HighTable
      focus={false}
      cacheKey={databaseUrl}
      className='hightable'
      columnConfiguration={columnConfiguration}
      data={queryDf}
      onError={setError}
    />
  </>
}
