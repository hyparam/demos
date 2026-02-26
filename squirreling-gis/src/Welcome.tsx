import { type ReactNode, type SyntheticEvent, useCallback, useRef } from 'react'

const exampleUrl = 'https://hyperparam-public.s3.us-east-1.amazonaws.com/microsoft-buildings_point.parquet'

interface Props {
  setUrl: (url: string) => void
}

export default function Welcome({ setUrl }: Props): ReactNode {
  const urlRef = useRef<HTMLInputElement>(null)

  const onSubmit = useCallback((e: SyntheticEvent<HTMLFormElement>) => {
    e.preventDefault()
    const value = urlRef.current?.value ?? ''
    const url = value === '' ? exampleUrl : value
    setUrl(url)
  }, [setUrl])

  return <div id="welcome">
    <div>
      <h1>Squirreling GIS</h1>
      <h2>Geospatial SQL engine for Parquet files</h2>
      <div className='badges'>
        <a href="https://www.npmjs.com/package/squirreling"><img src="https://img.shields.io/npm/v/squirreling" alt="npm squirreling" /></a>
        <a href="https://github.com/hyparam/squirreling"><img src="https://img.shields.io/github/stars/hyparam/squirreling?style=social" alt="star squirreling" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/squirreling">squirreling</a> geospatial support:
        run SQL queries with spatial functions like <code>ST_WITHIN</code> on GeoParquet files,
        and visualize results on a map.
      </p>
      <p>
        Drop a GeoParquet file with a geometry column, then use spatial SQL to filter and explore your data.
      </p>
      <form onSubmit={onSubmit}>
        <label htmlFor="url">Drag and drop a GeoParquet file (or url) to query your geospatial data.</label>
        <div className="inputGroup">
          <input id="url" type="url" ref={urlRef} required={false} placeholder={exampleUrl} />
          <button>Load</button>
        </div>
      </form>
      <h2>Example files:</h2>
      <ul className="quick-links">
        <li>
          <a
            className="aws"
            href="?key=https://hyperparam-public.s3.us-east-1.amazonaws.com/microsoft-buildings_point.parquet">
            microsoft-buildings_point.parquet
          </a>
        </li>
      </ul>
    </div>
  </div>
}
