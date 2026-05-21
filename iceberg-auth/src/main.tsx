import 'hightable/src/HighTable.css'
import 'hyperparam/global.css'
import 'hyperparam/hyperparam.css'
import { StrictMode } from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.js'
import { POPUP_SIGNIN_HASH, postErrorToOpenerAndClose, signIn } from './auth/cognito.js'
import './index.css'

// When this page is opened as a sign-in popup by an iframe-mode parent, jump
// straight to the Cognito redirect without mounting the React app — there's
// nothing to show, and the popup is about to navigate away anyway.
if (location.hash === POPUP_SIGNIN_HASH && window.opener && window.opener !== window) {
  signIn().catch((err: unknown) => { postErrorToOpenerAndClose(err) })
} else {
  const app = document.getElementById('app')
  if (!app) throw new Error('missing app element')
  ReactDOM.createRoot(app).render(<StrictMode>
    <App />
  </StrictMode>)
}
