import { ReactNode } from 'react'
import { NavLink } from 'react-router'

export default function Layout({ children }: { children: ReactNode }) {
  return <div className="layout">
    <nav className="topbar">
      <span className="title">HighTable Demos</span>
      {/* NavLink makes it easy to show active states */}
      {
        [
          ['Basic', '/'],
          ['Delayed', '/delayed'],
          ['Selection', '/selection'],
          ['Controlled', '/controlled'],
          ['Mirror', '/mirror'],
          ['Unstyled', '/unstyled'],
          ['Custom Theme', '/custom-theme'],
          ['Large', '/large'],
        ].map(([label, path]) => <NavLink key={path} to={path}
          className={ ({ isActive }) => isActive ? 'link active' : 'link' }
        >{label}</NavLink>,
        )
      }
    </nav>
    <div className="content">
      {children}
    </div>
  </div>
}
