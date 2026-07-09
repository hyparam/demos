import { ErrorBar, cn } from 'hyperparam'
import { ReactNode, useEffect } from 'react'

interface LayoutProps {
  children: ReactNode
  className?: string
  error?: Error
  onShowAbout?: () => void
}

export default function Layout({ children, className, error, onShowAbout }: LayoutProps): ReactNode {
  useEffect(() => {
    document.title = 'hypstore: SQL and grep over Iceberg tables'
  }, [])

  return <div className='content-container'>
    <div className={cn('content', className)}>
      {children}
    </div>
    {onShowAbout && <button className='about-button' aria-label='About' onClick={onShowAbout}>?</button>}
    <ErrorBar error={error} />
  </div>
}
