import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function expressPlugin() {
  let app;
  return {
    name: 'express-plugin',
    async configureServer(server) {
      // ✅ Load .env FIRST before any server module is imported
      // This ensures process.env is populated before chromaService, geminiService etc. initialize
      const dotenv = await import('dotenv');
      dotenv.config();

      const { default: expressApp } = await import('./server/app.js');
      app = expressApp;

      server.middlewares.use('/api', (req, res, next) => {
        // ✅ Patch SSE routes to flush immediately — prevents Vite buffering tokens
        if (req.url?.startsWith('/chat')) {
          res.setHeader('X-Accel-Buffering', 'no');
          const originalWrite = res.write.bind(res);
          res.write = (chunk) => {
            const result = originalWrite(chunk);
            if (typeof res.flush === 'function') res.flush();
            return result;
          };
        }
        app(req, res, next);
      });
    },
  };
}

function copyNetlifyFiles() {
  return {
    name: 'copy-netlify-files',
    closeBundle() {
      // Copy _redirects
      const redirectsSrc = path.resolve(__dirname, 'dist/_redirects');
      if (fs.existsSync(redirectsSrc)) {
        console.log('✅ _redirects exists in dist');
      }

      // Copy netlify.toml
      const netlifyToml = path.resolve(__dirname, 'netlify.toml');
      const netlifyTomlDest = path.resolve(__dirname, 'dist/netlify.toml');
      if (fs.existsSync(netlifyToml)) {
        fs.copyFileSync(netlifyToml, netlifyTomlDest);
        console.log('✅ netlify.toml copied to dist');
      }
    }
  };
}

export default defineConfig({
  plugins: [react(), expressPlugin(), copyNetlifyFiles()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
  },
});