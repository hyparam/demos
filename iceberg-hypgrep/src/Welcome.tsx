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
      <h1>iceberg-hypgrep</h1>
      <h2>Full-text search over Apache Iceberg tables</h2>
      <div className='badges'>
        <a href="https://github.com/hyparam/icebird"><img src="https://img.shields.io/npm/v/icebird" alt="npm icebird" /></a>
        <a href="https://github.com/hyparam/hypgrep"><img src="https://img.shields.io/npm/v/hypgrep" alt="npm hypgrep" /></a>
      </div>
      <p>
        A mash-up of <a href="https://github.com/hyparam/icebird">icebird</a> for reading Iceberg
        tables and <a href="https://github.com/hyparam/hypgrep">hypgrep</a> for
        full-text search. The main table is a regular Iceberg table; the search index lives
        in a sibling Iceberg table whose data file is itself a hypgrep parquet.
      </p>
      <button className="modal-cta" onClick={onClose}>Explore the demo</button>
    </div>
  </div>
}
