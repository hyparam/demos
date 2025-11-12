import { type FormEvent, ReactNode, useCallback, useRef } from 'react'
import audioSvg from './assets/audio.svg'
import hyparquetMp3 from './assets/hyparquet.mp3'

const exampleUrl = 'https://hyperparam-public.s3.amazonaws.com/wiki-en-00000-of-00041.parquet'

interface Props {
  setUrl: (url: string) => void
}

export default function Welcome({ setUrl }: Props): ReactNode {
  const audio = useRef<HTMLAudioElement>(null)
  const urlRef = useRef<HTMLInputElement>(null)

  function playAudio() {
    audio.current?.play().catch(() => {
      console.warn('Failed to play audio')
    })
  }

  const onSubmit = useCallback((e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const value = urlRef.current?.value ?? ''
    const url = value === '' ? exampleUrl : value
    setUrl(url)
  }, [setUrl])

  return <div id="welcome">
    <div>
      <h1>hyparquet</h1>
      <sub>/haɪ pɑːrˈkeɪ/<img src={audioSvg} alt="play hyparquet pronunciation" height="18" width="18" onClick={playAudio} /></sub>
      <audio ref={audio} id="audio" src={hyparquetMp3}></audio>
      <h2>in-browser parquet file reader</h2>
      <div className='badges'>
        <a href="https://www.npmjs.com/package/hyparquet"><img src="https://img.shields.io/npm/v/hyparquet" alt="npm hyparquet" /></a>
        <a href="https://github.com/hyparam/hyparquet"><img src="https://img.shields.io/github/stars/hyparam/hyparquet?style=social" alt="star hyparquet" /></a>
      </div>
      <p>
        Online demo of <a href="https://github.com/hyparam/hyparquet">hyparquet</a>: a parser for apache parquet files.
        Uses <a href="https://github.com/hyparam/hightable">hightable</a> for high performance windowed table viewing.
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
            href="?key=https://hyperparam-public.s3.amazonaws.com/wiki-en-00000-of-00041.parquet">
            s3://wiki-en-00000-of-00041.parquet
          </a>
        </li>
        <li>
          <a
            className="azure"
            href="?key=https://hyperparam.blob.core.windows.net/hyperparam/starcoderdata-js-00000-of-00065.parquet">
            azure://starcoderdata-js-00000-of-00065.parquet
          </a>
        </li>
        <li>
          <a
            className="huggingface"
            href="?key=https://huggingface.co/datasets/codeparrot/github-code/resolve/main/data/train-00000-of-01126.parquet?download=true">
            huggingface://github-code-00000-of-01126.parquet
          </a>
        </li>
        <li>
          <a
            className="github"
            href="?key=https://raw.githubusercontent.com/hyparam/hyparquet/master/test/files/rowgroups.parquet">
            github://rowgroups.parquet
          </a>
        </li>
      </ul>
    </div>
  </div>
}
