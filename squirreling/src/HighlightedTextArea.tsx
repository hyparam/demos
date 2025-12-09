import { ReactNode, useEffect, useRef } from 'react'

interface HighlightedTextAreaProps {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  className?: string
  highlightStart?: number
  highlightEnd?: number
}

/**
 * A textarea with overlay-based text highlighting.
 * Uses a transparent textarea on top of a styled backdrop div.
 */
export function HighlightedTextArea({
  value,
  onChange,
  placeholder,
  className,
  highlightStart,
  highlightEnd,
}: HighlightedTextAreaProps): ReactNode {
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const backdropRef = useRef<HTMLDivElement>(null)

  // Sync scroll position between textarea and backdrop
  function handleScroll() {
    if (textareaRef.current && backdropRef.current) {
      backdropRef.current.scrollTop = textareaRef.current.scrollTop
      backdropRef.current.scrollLeft = textareaRef.current.scrollLeft
    }
  }

  // Sync resize between textarea and backdrop
  useEffect(() => {
    const textarea = textareaRef.current
    const backdrop = backdropRef.current

    if (!textarea || !backdrop) return

    const resizeObserver = new ResizeObserver(() => {
      backdrop.style.height = `${textarea.offsetHeight}px`
    })

    resizeObserver.observe(textarea)

    return () => {
      resizeObserver.disconnect()
    }
  }, [])

  return (
    <div className="sql-input-wrapper">
      <div
        ref={backdropRef}
        className="sql-input-backdrop"
        aria-hidden="true"
      >
        {renderHighlightedText(value, highlightStart, highlightEnd)}
      </div>
      <textarea
        ref={textareaRef}
        className={`sql-input ${className ?? ''}`}
        placeholder={placeholder}
        onChange={e => { onChange(e.target.value) }}
        onScroll={handleScroll}
        spellCheck={false}
        value={value}
      />
    </div>
  )
}

function renderHighlightedText(
  text: string,
  highlightStart?: number,
  highlightEnd?: number,
): ReactNode {
  // If no highlight range, just render plain text
  if (highlightStart === undefined || highlightEnd === undefined) {
    return <>{text}{'\n'}</>
  }

  // Clamp positions to valid range
  const start = Math.max(0, Math.min(highlightStart, text.length))
  const end = Math.max(start, Math.min(highlightEnd, text.length))

  // If positions are equal or invalid, no highlight needed
  if (start >= end) {
    return <>{text}{'\n'}</>
  }

  const before = text.slice(0, start)
  const highlighted = text.slice(start, end)
  const after = text.slice(end)

  return (
    <>
      {before}
      <span className="sql-error-highlight">{highlighted}</span>
      {after}
      {'\n'}
    </>
  )
}
