import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  css: {
    lightningcss: {
      errorRecovery: true,
    },
  },
  base: './',
})
