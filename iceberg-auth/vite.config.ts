import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  css: {
    lightningcss: {
      errorRecovery: true,
    },
  },
  base:'./',
  // strictPort: fail loud if 5173 is taken so the Cognito callback URL stays
  // valid (silent fallback to another port would break the OAuth redirect).
  server: { port: 5173, strictPort: true },
  preview: { port: 4173, strictPort: true },
})
