import 'hightable/src/HighTable.css'
import ReactDOM from 'react-dom/client'
import { HashRouter, Route, Routes } from 'react-router'
import Basic from './Basic.js'
import Controlled from './Controlled.js'
import CustomTheme from './CustomTheme.js'
import Large from './Large.js'
import Mirror from './Mirror.js'
import Selection from './Selection.js'
import Unstyled from './Unstyled.js'
import './index.css'

const app = document.getElementById('app')
if (!app) throw new Error('missing app element')
ReactDOM.createRoot(app).render(<HashRouter>
  <Routes>
    <Route path="/" element={<Basic />} />
    <Route path="/selection" element={<Selection />} />
    <Route path="/controlled" element={<Controlled />} />
    <Route path="/mirror" element={<Mirror />} />
    <Route path="/unstyled" element={<Unstyled />} />
    <Route path="/custom-theme" element={<CustomTheme />} />
    <Route path="/large" element={<Large />} />
  </Routes>
</HashRouter>)
