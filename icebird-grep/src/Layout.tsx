import { ErrorBar, cn } from 'hyperparam'
import { ReactNode, useEffect } from 'react'

interface LayoutProps {
  children: ReactNode
  className?: string
  error?: Error
}

export default function Layout({ children, className, error }: LayoutProps): ReactNode {
  useEffect(() => {
    document.title = 'icebird-grep: Full-text search over Iceberg tables'
  }, [])

  return <div className='content-container'>
    <div className={cn('content', className)}>
      {children}
    </div>
    <ErrorBar error={error} />
  </div>
}
