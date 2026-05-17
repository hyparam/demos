import { ReactNode, useMemo } from 'react'
import type { Snapshot } from 'icebird/src/types.js'

interface SnapshotSliderProps {
  snapshots: Snapshot[]
  value: bigint
  onChange: (snapshotId: bigint) => void
}

/**
 * Time-travel slider over an iceberg table's snapshot history. The snapshots
 * are expected oldest → newest; the slider shows the selected snapshot's
 * timestamp and id.
 */
export default function SnapshotSlider({
  snapshots,
  value,
  onChange,
}: SnapshotSliderProps): ReactNode {
  const idx = useMemo(
    () => Math.max(0, snapshots.findIndex(s => BigInt(s['snapshot-id']) === value)),
    [snapshots, value],
  )
  const selected = snapshots[idx]
  const label = `${new Date(selected['timestamp-ms']).toISOString().replace('T', ' ').slice(0, 19)} · ${selected['snapshot-id']}`

  return (
    <label className='snapshot-slider'>
      <span>{label}</span>
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
    </label>
  )
}
