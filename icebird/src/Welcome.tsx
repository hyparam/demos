import { ReactNode } from 'react'

const exampleUrl = 'https://s3.amazonaws.com/hyperparam-iceberg/spark/bunnies'

interface Props {
  setTableUrl: (url: string) => void
}

export default function Welcome({ setTableUrl }: Props): ReactNode {
  function clickLoad() {
    // Update url with key
    const input = document.querySelector('input')
    if (input instanceof HTMLInputElement) {
      const url = input.value === '' ? exampleUrl : input.value
      const params = new URLSearchParams(location.search)
      params.set('key', url)
      history.pushState({}, '', `${location.pathname}?${params}`)
      setTableUrl(url)
    }
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
      <p>
        Enter a URL to a public iceberg table:
      </p>
      <div className="inputGroup">
        <input type="text" placeholder={exampleUrl}/>
        <button onClick={clickLoad}>Load</button>
      </div>
    </div>
  </div>
}
