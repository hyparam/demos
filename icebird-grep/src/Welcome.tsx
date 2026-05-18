import { ReactNode, SubmitEvent, useCallback, useRef } from 'react'

const exampleUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/icebird-grep/llm_logs'

interface Props {
  setTableUrl: (url: string) => void
}

export default function Welcome({ setTableUrl }: Props): ReactNode {
  const urlRef = useRef<HTMLInputElement>(null)

  const onSubmit = useCallback((e: SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()
    const url = urlRef.current?.value ?? ''
    setTableUrl(url === '' ? exampleUrl : url)
  }, [setTableUrl])

  return <div id="welcome">
    <div>
      <h1>icebird-grep</h1>
      <h2>Full-text search over Apache Iceberg tables</h2>
      <div className='badges'>
        <a href="https://github.com/hyparam/icebird"><img src="https://img.shields.io/npm/v/icebird" alt="npm icebird" /></a>
        <a href="https://github.com/hyparam/parquetindex"><img src="https://img.shields.io/npm/v/parquetindex" alt="npm parquetindex" /></a>
      </div>
      <p>
        A mash-up of <a href="https://github.com/hyparam/icebird">icebird</a> for reading Iceberg
        tables and <a href="https://github.com/hyparam/parquetindex">parquetindex</a> for
        full-text search. The main table is a regular Iceberg table; the search index lives
        in a sibling Iceberg table whose data file is itself a parquetindex parquet.
      </p>
      <p>Example table:</p>
      <ul className="quick-links">
        <li>
          <a className="aws" href={`?key=${encodeURIComponent(exampleUrl)}`}>
            s3://hyperparam-iceberg/icebird-grep/llm_logs
          </a>
        </li>
      </ul>
      <form onSubmit={onSubmit}>
        <label htmlFor="url">Or enter another Iceberg table URL (with a sibling <code>.index</code> table):</label>
        <div className="inputGroup">
          <input id="url" type="url" ref={urlRef} required={false} placeholder={exampleUrl} />
          <button>Load</button>
        </div>
      </form>
    </div>
  </div>
}
