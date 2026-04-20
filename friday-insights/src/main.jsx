import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    const swUrl = new URL('./service-worker.js', import.meta.url)
    navigator.serviceWorker.register(swUrl, { type: 'module' }).catch(() => {
      // Service worker registration failures should not block app bootstrap.
    })
  })
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
