import { ChangeEvent, FormEvent, ReactNode, useCallback, useState } from 'react'

const exampleUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'

interface Props {
  setTableUrl: (url: string) => void
}

export default function Welcome({ setTableUrl }: Props): ReactNode {
  const [url, setUrl] = useState('')

  const onUrlChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
    setUrl(e.target.value)
  }, [])

  function clickLoad(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    // Update url with key
    const key = url === '' ? exampleUrl : url
    const params = new URLSearchParams(location.search)
    params.set('key', key)
    history.pushState({}, '', `${location.pathname}?${params}`)
    setTableUrl(key)
  }

  return <div id="welcome">
    <div>
      <h1>Icebird: JavaScript Iceberg Parser</h1>
      <h2>in-browser Apache Iceberg parser</h2>
      <p>
        <a href="https://www.npmjs.com/package/icebird"><img src="https://img.shields.io/npm/v/icebird" alt="npm icebird" /></a>
        <a href="https://github.com/hyparam/icebird"><img src="https://img.shields.io/github/stars/hyparam/icebird?style=social" alt="star icebird" /></a>
      </p>
      <p>
        Online demo of <a href="https://github.com/hyparam/icebird">Icebird</a>: a parser for apache iceberg tables.
        Uses <a href="https://github.com/hyparam/hyparquet">hyparquet</a> for parquet file reading.
        Uses <a href="https://github.com/hyparam/hightable">hightable</a> for high performance windowed table viewing.
      </p>
      <form onSubmit={clickLoad}>
        <label htmlFor="url">Enter a URL to a public iceberg table:</label>
        <div className="inputGroup">
          <input id="url" type="url" required={false} placeholder={exampleUrl} value={url} onChange={onUrlChange} />
          <button>Load</button>
        </div>
      </form>
    </div>
  </div>
}
