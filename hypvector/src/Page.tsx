import { type FeatureExtractionPipeline, pipeline } from '@huggingface/transformers'
import { AsyncBuffer, FileMetaData, asyncBufferFromUrl, cachedAsyncBuffer, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { type SearchResult, searchVectors } from 'hypvector'
import { ReactNode, useCallback, useEffect, useRef, useState } from 'react'

const vectorsUrl = 'https://s3.hyperparam.app/hypvector/wiki_en.vectors.parquet'
const wikiUrl = 'https://s3.hyperparam.app/hypgrep/wiki_en.parquet'
const wikipediaUrl = 'https://en.wikipedia.org/wiki/'
const modelId = 'Xenova/all-MiniLM-L6-v2'
const topK = 10

const examples = [
  'how do neural networks learn',
  'ancient roman emperors',
  'jazz musicians from new orleans',
  'volcanic eruptions in history',
  'famous chess world champions',
]

interface PageProps {
  setError: (e: unknown) => void
}

interface DisplayResult extends SearchResult {
  title?: string
}

interface QueryStats {
  embedMs: number
  searchMs: number
  fetches: number
  bytes: number
}

/** Wrap an AsyncBuffer to count fetches and bytes read. */
function instrumented(buffer: AsyncBuffer, counter: { fetches: number; bytes: number }): AsyncBuffer {
  return {
    byteLength: buffer.byteLength,
    slice(start: number, end?: number): ArrayBuffer | Promise<ArrayBuffer> {
      counter.fetches += 1
      counter.bytes += (end ?? buffer.byteLength) - start
      return buffer.slice(start, end)
    },
  }
}

export default function Page({ setError }: PageProps): ReactNode {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<DisplayResult[]>([])
  const [stats, setStats] = useState<QueryStats>()
  const [modelStatus, setModelStatus] = useState<'loading' | 'ready' | 'error'>('loading')
  const [vectorsStatus, setVectorsStatus] = useState<'loading' | 'ready' | 'error'>('loading')
  const [querying, setQuerying] = useState(false)

  const extractorRef = useRef<FeatureExtractionPipeline | undefined>(undefined)
  const vectorsBufferRef = useRef<AsyncBuffer | undefined>(undefined)
  const vectorsMetaRef = useRef<FileMetaData | undefined>(undefined)
  const wikiBufferRef = useRef<AsyncBuffer | undefined>(undefined)
  const wikiMetaRef = useRef<FileMetaData | undefined>(undefined)

  // Load the embedding model (downloads ~25 MB the first time).
  useEffect(() => {
    let cancelled = false
    pipeline('feature-extraction', modelId)
      .then(p => {
        if (cancelled) return
        extractorRef.current = p
        setModelStatus('ready')
      })
      .catch((e: unknown) => {
        if (cancelled) return
        setModelStatus('error')
        setError(e)
      })
    return () => { cancelled = true }
  }, [setError])

  // Open the vectors parquet and parse its metadata once.
  useEffect(() => {
    let cancelled = false
    async function load() {
      const raw = await asyncBufferFromUrl({ url: vectorsUrl })
      const cached = cachedAsyncBuffer(raw)
      const meta = await parquetMetadataAsync(cached)
      if (cancelled) return
      vectorsBufferRef.current = cached
      vectorsMetaRef.current = meta
      setVectorsStatus('ready')
    }
    load().catch((e: unknown) => {
      if (cancelled) return
      setVectorsStatus('error')
      setError(e)
    })
    return () => { cancelled = true }
  }, [setError])

  // Open the wiki parquet lazily; titles are only fetched after a search.
  const ensureWiki = useCallback(async (): Promise<{ buffer: AsyncBuffer; metadata: FileMetaData }> => {
    if (wikiBufferRef.current && wikiMetaRef.current) {
      return { buffer: wikiBufferRef.current, metadata: wikiMetaRef.current }
    }
    const raw = await asyncBufferFromUrl({ url: wikiUrl })
    const cached = cachedAsyncBuffer(raw)
    const metadata = await parquetMetadataAsync(cached)
    wikiBufferRef.current = cached
    wikiMetaRef.current = metadata
    return { buffer: cached, metadata }
  }, [])

  const runQuery = useCallback(async (q: string, signal: AbortSignal) => {
    const extractor = extractorRef.current
    const buffer = vectorsBufferRef.current
    const metadata = vectorsMetaRef.current
    if (!extractor || !buffer || !metadata) return

    setQuerying(true)
    const embedStart = performance.now()
    const output = await extractor([q], { pooling: 'mean', normalize: true })
    const queryVec = (output.data as Float32Array).slice(0, 384)
    const embedMs = performance.now() - embedStart
    signal.throwIfAborted()

    const counter = { fetches: 0, bytes: 0 }
    const instrumentedBuffer = instrumented(buffer, counter)

    const searchStart = performance.now()
    const hits = await searchVectors({
      source: instrumentedBuffer,
      metadata,
      query: queryVec,
      topK,
      signal,
      compressors,
    })
    const searchMs = performance.now() - searchStart
    signal.throwIfAborted()

    // Show scores immediately, then fill in titles.
    const initial: DisplayResult[] = hits.map(h => ({ ...h }))
    setResults(initial)
    setStats({ embedMs, searchMs, fetches: counter.fetches, bytes: counter.bytes })

    // Look up titles in the wiki parquet using ids as row indices.
    const { buffer: wb, metadata: wm } = await ensureWiki()
    signal.throwIfAborted()
    const titles = await Promise.all(hits.map(async hit => {
      const rowIndex = Number(hit.id)
      if (!Number.isFinite(rowIndex)) return undefined
      const rows = await parquetReadObjects({
        file: wb,
        metadata: wm,
        rowStart: rowIndex,
        rowEnd: rowIndex + 1,
        columns: ['title'],
        compressors,
      })
      return rows[0]?.title as string | undefined
    }))
    signal.throwIfAborted()
    setResults(hits.map((h, i) => ({ ...h, title: titles[i] })))
  }, [ensureWiki])

  const handleQueryChange = useCallback((value: string) => {
    setQuery(value)
    if (value.trim()) return
    setResults([])
    setStats(undefined)
    setQuerying(false)
  }, [])

  // Run the query when it changes (debounced) and both model + index are ready.
  useEffect(() => {
    const trimmed = query.trim()
    if (!trimmed || modelStatus !== 'ready' || vectorsStatus !== 'ready') {
      return
    }
    const controller = new AbortController()
    const timer = window.setTimeout(() => {
      runQuery(trimmed, controller.signal)
        .catch((e: unknown) => {
          if (controller.signal.aborted) return
          setError(e)
        })
        .finally(() => {
          if (!controller.signal.aborted) setQuerying(false)
        })
    }, 250)
    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [query, modelStatus, vectorsStatus, runQuery, setError])

  const statusText = (() => {
    if (modelStatus === 'error') return <span className='error'>model failed to load</span>
    if (vectorsStatus === 'error') return <span className='error'>index failed to load</span>
    if (modelStatus === 'loading') return <span className='pending'>loading MiniLM model…</span>
    if (vectorsStatus === 'loading') return <span className='pending'>loading vector index…</span>
    if (querying) return <span className='pending'>searching…</span>
    return <span className='ok'>ready</span>
  })()

  return <>
    <div className='top-header'>{vectorsUrl}</div>

    <div className='search-bar'>
      <input
        type='text'
        placeholder='Search by meaning — e.g. "how do neural networks learn"'
        value={query}
        onChange={e => { handleQueryChange(e.target.value) }}
        autoFocus
      />
      <span className='status'>{statusText}</span>
    </div>

    {!query && <div className='search-bar' style={{ background: 'transparent', border: 'none', padding: '4px 16px' }}>
      <span style={{ color: '#666', fontSize: '11pt' }}>Try:</span>
      <div className='examples'>
        {examples.map(ex =>
          <button key={ex} className='example-chip' onClick={() => { setQuery(ex) }}>{ex}</button>,
        )}
      </div>
    </div>}

    <div className='stats-bar'>
      <span>156,289 wiki titles · 384-dim float32 · 249 MB</span>
      {stats && <>
        <span className='spacer' />
        <span>embed: <code>{stats.embedMs.toFixed(0)} ms</code></span>
        <span>search: <code>{stats.searchMs.toFixed(0)} ms</code></span>
        <span>fetches: <code>{stats.fetches}</code></span>
        <span>read: <code>{formatBytes(stats.bytes)}</code></span>
      </>}
    </div>

    <div className='results-list'>
      {!query && <div className='results-empty'>Pick an example or type a query.</div>}
      {query && !results.length && !querying && <div className='results-empty'>No results yet.</div>}
      {results.map((r, i) =>
        <div key={String(r.id)} className='result'>
          <span className='rank'>{i + 1}.</span>
          {r.title
            ? <a className='title' href={wikipediaUrl + encodeURIComponent(r.title.replace(/ /g, '_'))} target='_blank' rel='noreferrer'>{r.title}</a>
            : <span className='title' style={{ color: '#999' }}>row {String(r.id)}</span>}
          <span className='score'>{r.score.toFixed(4)}</span>
        </div>,
      )}
    </div>
  </>
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`
}
