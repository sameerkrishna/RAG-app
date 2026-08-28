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

function copyStaticAssets() {
  return {
    name: 'copy-static-assets',
    closeBundle() {
      // Copy seed_documents folder to dist
      const seedSrc = path.resolve(__dirname, 'seed_documents');
      const seedDest = path.resolve(__dirname, 'dist/seed_documents');
      if (fs.existsSync(seedSrc)) {
        fs.mkdirSync(seedDest, { recursive: true });
        const files = fs.readdirSync(seedSrc);
        files.forEach(file => {
          const srcFile = path.join(seedSrc, file);
          const destFile = path.join(seedDest, file);
          if (fs.statSync(srcFile).isFile()) {
            fs.copyFileSync(srcFile, destFile);
          }
        });
        console.log(`✅ seed_documents copied to dist (${files.length} files)`);
      }

    }
  };
}

export default defineConfig({
  plugins: [react(), expressPlugin(), copyStaticAssets()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
  },
});
