import { ReactNode, useMemo } from 'react'
import type { Snapshot } from 'icebird/src/types.js'

interface SnapshotSliderProps {
  snapshots: Snapshot[]
  value: bigint
  onChange: (snapshotId: bigint) => void
  rowCount?: string
}

/**
 * Time-travel slider over an iceberg table's snapshot history. Fires
 * `onChange` on every input event — the parent commits instantly and relies
 * on its data-source cache + abort handling to keep this responsive.
 */
export default function SnapshotSlider({
  snapshots,
  value,
  onChange,
  rowCount,
}: SnapshotSliderProps): ReactNode {
  const idx = useMemo(
    () => Math.max(0, snapshots.findIndex(s => BigInt(s['snapshot-id']) === value)),
    [snapshots, value],
  )
  const selected = snapshots[idx]
  const timestamp = new Date(selected['timestamp-ms']).toISOString().replace('T', ' ').slice(0, 19)

  return (
    <label className='snapshot-slider' title={String(selected['snapshot-id'])}>
      <input
        type='range'
        min={0}
        max={snapshots.length - 1}
        step={1}
        value={idx}
        onChange={e => {
          const next = snapshots[Number(e.currentTarget.value)]
          onChange(BigInt(next['snapshot-id']))
        }}
      />
      <div className='snapshot-meta'>
        <span>{timestamp}</span>
        {rowCount !== undefined && <span>({rowCount} rows)</span>}
      </div>
    </label>
  )
}
