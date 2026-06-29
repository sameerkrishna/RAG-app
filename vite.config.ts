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
      const { default: expressApp } = await import('./server/app.js');
      app = expressApp;

      server.middlewares.use('/api', (req, res, next) => {
        // ✅ Disable Vite's response buffering for SSE chat endpoint
        if (req.url?.startsWith('/chat')) {
          res.setHeader('X-Accel-Buffering', 'no');
          // ✅ Patch res.write to flush immediately after every write
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