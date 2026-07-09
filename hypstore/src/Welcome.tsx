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
      <h1>hypstore</h1>
      <h2>SQL and grep over millions of chat logs</h2>
      <div className='badges'>
        <a href="https://github.com/hyparam/hypstore"><img src="https://img.shields.io/npm/v/hypstore" alt="npm hypstore" /></a>
        <a href="https://github.com/hyparam/icebird"><img src="https://img.shields.io/npm/v/icebird" alt="npm icebird" /></a>
        <a href="https://github.com/hyparam/squirreling"><img src="https://img.shields.io/npm/v/squirreling" alt="npm squirreling" /></a>
        <a href="https://github.com/hyparam/hypgrep"><img src="https://img.shields.io/npm/v/hypgrep" alt="npm hypgrep" /></a>
      </div>
      <p>
        <a href="https://github.com/hyparam/hypstore">hypstore</a> is a serverless lakehouse
        for JavaScript: named Apache Iceberg tables in object storage, queryable
        with <strong>SQL</strong> (<a href="https://github.com/hyparam/squirreling">squirreling</a>) and
        full-text <strong>grep</strong> (<a href="https://github.com/hyparam/hypgrep">hypgrep</a>).
      </p>
      <p>
        This demo runs entirely in your browser against the{' '}
        <a href="https://huggingface.co/datasets/allenai/WildChat-4.8M">WildChat</a> dataset
        of real ChatGPT conversations, stored as one Iceberg table on S3. Every
        query fetches only the bytes it needs via HTTP range requests — no
        server, no always-on infrastructure.
      </p>
      <button className="modal-cta" onClick={onClose}>Explore the demo</button>
    </div>
  </div>
}
