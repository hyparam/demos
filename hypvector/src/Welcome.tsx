import { ReactNode, useEffect } from 'react'

interface Props {
  onClose: () => void
}

export default function Welcome({ onClose }: Props): ReactNode {
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => { window.removeEventListener('keydown', onKeyDown) }
  }, [onClose])

  return <div className="modal-overlay" onClick={onClose}>
    <div className="modal" onClick={e => { e.stopPropagation() }}>
      <button className="modal-close" aria-label="Close" onClick={onClose}>×</button>
      <h1>hypvector</h1>
      <h2>Serverless vector search against parquet files in cloud storage</h2>
      <div className='badges'>
        <a href="https://github.com/hyparam/hypvector"><img src="https://img.shields.io/github/stars/hyparam/hypvector?style=social" alt="star hypvector" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/hypvector">hypvector</a>: a library for
        storing embedding vectors compactly in Parquet and querying them directly over HTTP range
        requests — no vector database, no backend.
      </p>
      <p>
        This demo searches <strong>50,000 English Wikipedia article titles</strong> embedded with
        <code>all-MiniLM-L6-v2</code> (384 dimensions). Your query is embedded in the browser with
        <a href="https://huggingface.co/docs/transformers.js"> transformers.js</a>, then matched
        against a <strong>249 MB parquet file on S3</strong> via ranged HTTP fetches.
      </p>
      <p>
        Each top-10 query reads ~6 MB across a couple hundred small ranged fetches: phase 1
        prunes via Hamming distance on 1-bit codes inside the nearest k-means clusters, phase 2
        fetches only the candidate float32 vectors and reranks under cosine similarity.
      </p>
      <button className="modal-cta" onClick={onClose}>Try the demo</button>
    </div>
  </div>
}
