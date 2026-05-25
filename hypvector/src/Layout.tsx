import { ErrorBar, cn } from 'hyperparam'
import type { ReactNode } from 'react'

interface LayoutProps {
  children: ReactNode
  className?: string
  progress?: number
  error?: Error
  onShowAbout?: () => void
}

export default function Layout({ children, className, progress, error, onShowAbout }: LayoutProps): ReactNode {
  return <>
    <div className='content-container'>
      <div className={cn('content', className)}>
        {children}
      </div>
      {onShowAbout && <button className='about-button' aria-label='About' onClick={onShowAbout}>?</button>}
      <ErrorBar error={error} />
    </div>
    {progress !== undefined && progress < 1 &&
      <div className={'progress-bar'} role='progressbar'>
        <div style={{ width: `${100 * progress}%` }} />
      </div>
    }
  </>
}
