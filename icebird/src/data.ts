import { icebergDataSource } from 'icebird'
import type { TableMetadata } from 'icebird/src/types.js'
import type { AsyncDataSource } from 'squirreling'

export function buildIcebergDataSource(tableUrl: string, metadata: TableMetadata): Promise<AsyncDataSource> {
  return icebergDataSource({ tableUrl, metadata })
}
