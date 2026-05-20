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
      <h1>hypgrep</h1>
      <h2>Full text search against cloud-stored parquet files</h2>
      <div className='badges'>
        <a href="https://www.npmjs.com/package/hypgrep"><img src="https://img.shields.io/npm/v/hypgrep" alt="npm hypgrep" /></a>
        <a href="https://github.com/hyparam/hypgrep"><img src="https://img.shields.io/github/stars/hyparam/hypgrep?style=social" alt="star hypgrep" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/hypgrep">hypgrep</a>: a library for building full text search indexes
        against parquet files stored in cloud object storage (S3, Azure Blob Storage, etc).
      </p>
      <p>
        This demo uses <a href="https://github.com/hyparam/hightable">hightable</a> for high performance table viewing.
      </p>
      <button className="modal-cta" onClick={onClose}>Explore the demo</button>
    </div>
  </div>
}
