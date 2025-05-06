import { ReactNode, useMemo } from 'react'

interface VersionSliderProps {
  versions: string[]
  value: string
  onChange: (v: string) => void
}

export default function VersionSlider({
  versions,
  value,
  onChange,
}: VersionSliderProps): ReactNode {
  const idx = useMemo(
    () => Math.max(0, versions.indexOf(value)),
    [versions, value],
  )

  return (
    <label className='version-slider'>
      <span>{value}</span>
      <input
        type='range'
        min={0}
        max={versions.length - 1}
        step={1}
        value={idx}
        onChange={e => { onChange(versions[Number(e.currentTarget.value)]) }}
      />
    </label>
  )
}
