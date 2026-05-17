import React, { ReactNode, useCallback, useEffect, useRef } from 'react'
import type { HighlightRange } from './sqlHighlight'

interface HighlightedTextAreaProps {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  className?: string
  highlights?: HighlightRange[]
  errorStart?: number
  errorEnd?: number
}

/**
 * A text input with syntax highlighting using contenteditable.
 */
export function HighlightedTextArea({
  value,
  onChange,
  placeholder,
  className,
  highlights,
  errorStart,
  errorEnd,
}: HighlightedTextAreaProps): ReactNode {
  const editableRef = useRef<HTMLDivElement>(null)
  const undoStack = useRef<string[]>([])
  const redoStack = useRef<string[]>([])
  const isUndoRedo = useRef(false)

  // Track changes for undo/redo
  const pushUndo = useCallback((prevValue: string) => {
    if (!isUndoRedo.current) {
      undoStack.current.push(prevValue)
      redoStack.current = [] // Clear redo on new input
    }
  }, [])

  // Update DOM when value or highlights change
  useEffect(() => {
    const el = editableRef.current
    if (!el) return

    // Get current cursor position as text offset
    const cursorOffset = getCursorOffset(el)

    // Update content with highlights
    const fragment = renderHighlightedText(value, highlights, errorStart, errorEnd)
    el.innerHTML = ''
    el.appendChild(fragment)

    // Restore cursor position
    if (cursorOffset !== null && document.activeElement === el) {
      setCursorOffset(el, cursorOffset)
    }
  }, [value, highlights, errorStart, errorEnd])

  function handleInput() {
    const el = editableRef.current
    if (!el) return
    // Extract plain text from contenteditable
    const text = el.innerText.replace(/\n$/, '') // Remove trailing newline
    if (text !== value) {
      pushUndo(value)
      onChange(text)
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    // Handle undo/redo
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z') {
      e.preventDefault()
      if (e.shiftKey) {
        // Redo
        const redoValue = redoStack.current.pop()
        if (redoValue !== undefined) {
          isUndoRedo.current = true
          undoStack.current.push(value)
          onChange(redoValue)
          isUndoRedo.current = false
        }
      } else {
        // Undo
        const undoValue = undoStack.current.pop()
        if (undoValue !== undefined) {
          isUndoRedo.current = true
          redoStack.current.push(value)
          onChange(undoValue)
          isUndoRedo.current = false
        }
      }
    }
    // Also handle Ctrl+Y for redo
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'y') {
      e.preventDefault()
      const redoValue = redoStack.current.pop()
      if (redoValue !== undefined) {
        isUndoRedo.current = true
        undoStack.current.push(value)
        onChange(redoValue)
        isUndoRedo.current = false
      }
    }
  }

  function handleBeforeInput(e: React.SyntheticEvent<HTMLDivElement>) {
    const inputEvent = e.nativeEvent as InputEvent
    // Block all formatting operations (bold, italic, underline, etc.)
    if (inputEvent.inputType.startsWith('format')) {
      e.preventDefault()
    }
  }

  function handlePaste(e: React.ClipboardEvent) {
    e.preventDefault()
    const text = e.clipboardData.getData('text/plain')
    const selection = window.getSelection()
    if (selection && selection.rangeCount > 0) {
      const range = selection.getRangeAt(0)
      range.deleteContents()
      range.insertNode(document.createTextNode(text))
      range.collapse(false)
      selection.removeAllRanges()
      selection.addRange(range)
      // Trigger input event to update state
      editableRef.current?.dispatchEvent(new Event('input', { bubbles: true }))
    }
  }

  return (
    <div
      ref={editableRef}
      contentEditable
      className={`sql-input ${className ?? ''}`}
      onBeforeInput={handleBeforeInput}
      onInput={handleInput}
      onKeyDown={handleKeyDown}
      onPaste={handlePaste}
      spellCheck={false}
      data-placeholder={placeholder}
      suppressContentEditableWarning
    />
  )
}

/**
 * Get cursor position as text offset from start of element.
 */
function getCursorOffset(el: HTMLElement): number | null {
  const selection = window.getSelection()
  if (!selection || selection.rangeCount === 0) return null

  const range = selection.getRangeAt(0)
  if (!el.contains(range.startContainer)) return null

  // Create a range from start of element to cursor
  const preRange = document.createRange()
  preRange.selectNodeContents(el)
  preRange.setEnd(range.startContainer, range.startOffset)
  return preRange.toString().length
}

/**
 * Set cursor position by text offset.
 */
function setCursorOffset(el: HTMLElement, offset: number): void {
  const range = document.createRange()
  const selection = window.getSelection()
  if (!selection) return

  let currentOffset = 0
  const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT)

  let node: Text | null = walker.nextNode() as Text | null
  while (node !== null) {
    const nodeLength = node.textContent.length
    if (currentOffset + nodeLength >= offset) {
      range.setStart(node, offset - currentOffset)
      range.collapse(true)
      selection.removeAllRanges()
      selection.addRange(range)
      return
    }
    currentOffset += nodeLength
    node = walker.nextNode() as Text | null
  }

  // If offset is beyond content, place at end
  range.selectNodeContents(el)
  range.collapse(false)
  selection.removeAllRanges()
  selection.addRange(range)
}

const highlightClasses: Record<HighlightRange['type'], string> = {
  keyword: 'sql-keyword',
  function: 'sql-function',
  string: 'sql-string',
  number: 'sql-number',
  operator: 'sql-operator',
  identifier: 'sql-identifier',
}

/**
 * Render highlighted text as a DocumentFragment.
 */
function renderHighlightedText(
  text: string,
  highlights?: HighlightRange[],
  errorStart?: number,
  errorEnd?: number,
): DocumentFragment {
  const fragment = document.createDocumentFragment()

  // If no highlights, render plain text (possibly with error)
  if (!highlights || highlights.length === 0) {
    if (errorStart !== undefined && errorEnd !== undefined && errorStart < errorEnd) {
      const start = Math.max(0, Math.min(errorStart, text.length))
      const end = Math.max(start, Math.min(errorEnd, text.length))
      fragment.appendChild(document.createTextNode(text.slice(0, start)))
      const errorSpan = document.createElement('span')
      errorSpan.className = 'sql-error-highlight'
      errorSpan.textContent = text.slice(start, end)
      fragment.appendChild(errorSpan)
      fragment.appendChild(document.createTextNode(text.slice(end)))
    } else {
      fragment.appendChild(document.createTextNode(text))
    }
    return fragment
  }

  // Sort highlights by start position
  const sorted = [...highlights].sort((a, b) => a.start - b.start)

  let pos = 0

  for (const h of sorted) {
    // Skip invalid or out-of-order highlights
    if (h.start < pos || h.start >= text.length) continue
    const end = Math.min(h.end, text.length)
    if (end <= h.start) continue

    // Add any unhighlighted text before this highlight
    if (h.start > pos) {
      appendSegmentWithError(fragment, text.slice(pos, h.start), pos, errorStart, errorEnd)
    }

    // Add the highlighted segment
    const segmentText = text.slice(h.start, end)
    const className = highlightClasses[h.type]
    const hasError = errorStart !== undefined && errorEnd !== undefined &&
      h.start < errorEnd && end > errorStart

    if (hasError) {
      appendHighlightWithError(fragment, segmentText, h.start, className, errorStart, errorEnd)
    } else {
      const span = document.createElement('span')
      span.className = className
      span.textContent = segmentText
      fragment.appendChild(span)
    }

    pos = end
  }

  // Add any remaining text after last highlight
  if (pos < text.length) {
    appendSegmentWithError(fragment, text.slice(pos), pos, errorStart, errorEnd)
  }

  return fragment
}

/**
 * Append a plain text segment, applying error highlighting if it overlaps.
 */
function appendSegmentWithError(
  fragment: DocumentFragment,
  text: string,
  textStart: number,
  errorStart?: number,
  errorEnd?: number,
): void {
  if (errorStart === undefined || errorEnd === undefined) {
    fragment.appendChild(document.createTextNode(text))
    return
  }

  const textEnd = textStart + text.length
  if (textEnd <= errorStart || textStart >= errorEnd) {
    fragment.appendChild(document.createTextNode(text))
    return
  }

  // Calculate overlap
  const overlapStart = Math.max(0, errorStart - textStart)
  const overlapEnd = Math.min(text.length, errorEnd - textStart)

  if (overlapStart > 0) {
    fragment.appendChild(document.createTextNode(text.slice(0, overlapStart)))
  }
  const errorSpan = document.createElement('span')
  errorSpan.className = 'sql-error-highlight'
  errorSpan.textContent = text.slice(overlapStart, overlapEnd)
  fragment.appendChild(errorSpan)
  if (overlapEnd < text.length) {
    fragment.appendChild(document.createTextNode(text.slice(overlapEnd)))
  }
}

/**
 * Append a syntax-highlighted segment with error underline overlay.
 */
function appendHighlightWithError(
  fragment: DocumentFragment,
  text: string,
  textStart: number,
  className: string,
  errorStart: number,
  errorEnd: number,
): void {
  // Calculate overlap within this segment
  const overlapStart = Math.max(0, errorStart - textStart)
  const overlapEnd = Math.min(text.length, errorEnd - textStart)

  const span = document.createElement('span')
  span.className = className

  // If error covers entire segment
  if (overlapStart === 0 && overlapEnd === text.length) {
    span.classList.add('sql-error-highlight')
    span.textContent = text
    fragment.appendChild(span)
    return
  }

  if (overlapStart > 0) {
    span.appendChild(document.createTextNode(text.slice(0, overlapStart)))
  }
  const errorSpan = document.createElement('span')
  errorSpan.className = 'sql-error-highlight'
  errorSpan.textContent = text.slice(overlapStart, overlapEnd)
  span.appendChild(errorSpan)
  if (overlapEnd < text.length) {
    span.appendChild(document.createTextNode(text.slice(overlapEnd)))
  }
  fragment.appendChild(span)
}
