import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'path';

// Build output goes into Flask's static directory so a single Railway
// deploy can serve both the Python API shell (currently just routes that
// render the SPA index) and the React bundle. In dev, the Vite server
// proxies static-data fetches to a running Flask server on :5000.
export default defineConfig({
  plugins: [react()],
  base: '/app/',
  build: {
    outDir: resolve(__dirname, '../app/static/dist'),
    emptyOutDir: true,
    sourcemap: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/static': 'http://localhost:5000',
      '/sw.js': 'http://localhost:5000',
    },
  },
});
