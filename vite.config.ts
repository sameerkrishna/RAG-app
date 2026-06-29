import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import { fileURLToPath } from 'url';

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

export default defineConfig({
  plugins: [react(), expressPlugin()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
  },
});