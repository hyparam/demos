import { type FormEvent, type ReactNode, useCallback, useRef } from 'react'

const exampleUrl = 'https://s3.hyperparam.app/wiki-en-00000-of-00041.parquet'

interface Props {
  setUrl: (url: string) => void
}

export default function Welcome({ setUrl }: Props): ReactNode {
  const urlRef = useRef<HTMLInputElement>(null)

  const onSubmit = useCallback((e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const value = urlRef.current?.value ?? ''
    const url = value === '' ? exampleUrl : value
    setUrl(url)
  }, [setUrl])

  return <div id="welcome">
    <div>
      <h1>Squirreling</h1>
      <h2>Async SQL engine for Parquet files</h2>
      <div className='badges'>
        <a href="https://www.npmjs.com/package/squirreling"><img src="https://img.shields.io/npm/v/squirreling" alt="npm squirreling" /></a>
        <a href="https://github.com/hyparam/squirreling"><img src="https://img.shields.io/github/stars/hyparam/squirreling?style=social" alt="star squirreling" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/squirreling">squirreling</a>: a library for building full text search indexes
        against parquet files stored in cloud object storage (S3, Azure Blob Storage, etc).
      </p>
      <p>
        This demo uses <a href="https://github.com/hyparam/hightable">hightable</a> for high performance table viewing.
      </p>
      <form onSubmit={onSubmit}>
        <label htmlFor="url">Drag and drop a parquet file (or url) to see your parquet data. 👀</label>
        <div className="inputGroup">
          <input id="url" type="url" ref={urlRef} required={false} placeholder={exampleUrl} />
          <button>Load</button>
        </div>
      </form>
      <p>
        Example files:
      </p>
      <ul className="quick-links">
        <li>
          <a
            className="aws"
            href="?key=https://s3.hyperparam.app/wiki-en-00000-of-00041.parquet">
            wiki_en.parquet
          </a>
        </li>
        <li>
          <a
            className="aws"
            href="?key=https://s3.hyperparam.app/squirreling/tpch-lineitem.parquet">
            tpch-lineitem.parquet
          </a>
        </li>
      </ul>
    </div>
  </div>
}
