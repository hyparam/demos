import HighTable, { DataFrame } from 'hightable'
import { ReactNode } from 'react'
import Dropdown from './Dropdown.js'
import { IcebergMetadata } from 'icebird/src/types.js'

export interface PageProps {
  df: DataFrame
  metadata: IcebergMetadata
  numVersions: number
  version: number
  setVersion: (version: number) => void
  setError: (e: Error) => void
}

/**
 * Icebird demo viewer page
 * @param {Object} props
 * @returns {ReactNode}
 */
export default function Page({ df, metadata, numVersions, version, setVersion, setError }: PageProps): ReactNode {
  const versions = Array.from({ length: numVersions }, (_, i) => i + 1)
  const name = metadata.location

  return <>
    <div className='top-header'>{name}</div>
    <div className='view-header'>
      <span>{df.numRows.toLocaleString()} rows</span>
      <Dropdown label={`v${version}`}>
        {versions.map(v => <button key={v} onClick={() => { setVersion(v) }}>v{v}</button>)}
      </Dropdown>
    </div>
    <HighTable cacheKey={name} className='hightable' data={df} onError={setError} />
  </>
}
