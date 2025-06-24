import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  base: process.env.NODE_ENV === 'production' ? '/fronetend/' : '/',
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
  },
  server: {
    proxy: {
      '/api': {
        target: 'http://cmdb-backend-final.eba-pp2diqpq.us-east-1.elasticbeanstalk.com',
        changeOrigin: true,
        secure: false,
      },
    },
  },
})
