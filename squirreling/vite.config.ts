import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    exclude: ["hyperparam"],
  },
  css: {
    lightningcss: {
      errorRecovery: true,
    },
  },
  base: './',
})
