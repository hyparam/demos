import HighTable, { DataFrame } from 'hightable'
import type { TableMetadata } from 'icebird/src/types.js'
import type { ReactNode } from 'react'
import VersionSlider from './VersionSlider'

export interface PageProps {
  df: DataFrame
  metadata: TableMetadata
  versions: string[]
  version: string
  setVersion: (version: string) => void
  setError: (e: unknown) => void
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

      <VersionSlider
        versions={versions}
        value={version}
        onChange={setVersion}
      />
    </div>

    {/* Same cacheKey for all versions so column widths persist */}
    <HighTable
      cacheKey={name}
      className='hightable'
      data={df}
      onError={setError}
    />
  </>
}
