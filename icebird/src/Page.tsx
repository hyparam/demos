import HighTable, { DataFrame } from 'hightable'
import type { TableMetadata } from 'icebird/src/types.js'
import type { ReactNode } from 'react'
import Dropdown from './Dropdown'

export interface PageProps {
  df: DataFrame
  metadata: TableMetadata
  versions: string[]
  version: string
  setVersion: (version: string) => void
  setError: (e: Error) => void
}

/**
 * Icebird demo viewer page
 * @param {Object} props
 * @returns {ReactNode}
 */
export default function Page({ df, metadata, versions, version, setVersion, setError }: PageProps): ReactNode {
  const name = metadata.location

  return <>
    <div className='top-header'>{name}</div>
    <div className='view-header'>
      <span>{df.numRows.toLocaleString()} rows</span>
      <Dropdown label={version}>
        {versions.map(v => <button key={v} onClick={() => { setVersion(v) }}>{v}</button>)}
      </Dropdown>
    </div>
    {/* The same cacheKey is used for all iceberg versions, preserving the column widths */}
    <HighTable cacheKey={name} className='hightable' data={df} onError={setError} />
  </>
}
