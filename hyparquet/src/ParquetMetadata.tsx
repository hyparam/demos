import type { FileMetaData } from 'hyparquet'
import { Json } from 'hyperparam'
import type { ReactNode } from 'react'

interface MetadataProps {
  metadata: FileMetaData
}

/**
 * Renders the metadata of a parquet file as JSON.
 * @param {Object} props
 * @param {FileMetaData} props.metadata
 * @returns {ReactNode}
 */
export default function ParquetMetadata({ metadata }: MetadataProps): ReactNode {
  return <code className='viewer json'>
    <Json json={metadata} />
  </code>
}
