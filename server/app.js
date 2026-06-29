import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { EventEmitter } from 'events';

dotenv.config();

import healthRouter from './api/health.js';
import documentsRouter from './api/documents.js';
import chatRouter from './api/chat.js';
import feedbackRouter from './api/feedback.js';
import searchRouter from './api/search.js';
import { cleanupSessionCollections } from './services/chromaService.js'; // ✅ add this

const app = express();

// Progress callbacks
app.locals.progressCallbacks = new EventEmitter();

// ✅ Clean up stale session collections from previous runs on startup
// Run async, non-blocking — server starts immediately, cleanup happens in background
cleanupSessionCollections().then(() => {
  console.log('🚀 Server ready.');
}).catch(err => {
  console.warn('⚠️ Startup cleanup error (non-fatal):', err.message);
});

// Middleware
app.use(cors({
  origin: [
    'http://localhost:5173',
    'http://localhost:3000',
    'http://127.0.0.1:5173'
  ],
  credentials: true
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Request Logger
app.use((req, res, next) => {
  console.log(`${req.method} ${req.originalUrl}`);
  next();
});

// ===============================
// TEST ROUTE
// ===============================
app.get('/ping', (req, res) => {
  console.log('✅ PING ROUTE EXECUTED');

  res.json({
    success: true,
    message: 'Express backend is alive'
  });
});

// ===============================
// ROUTERS
// ===============================
console.log('Mounting routers...');

app.use('/health', healthRouter);
app.use('/documents', documentsRouter);
app.use('/chat', chatRouter);
app.use('/feedback', feedbackRouter);
app.use('/search', searchRouter);

console.log('✅ Routers mounted');

// ===============================
// ERROR HANDLER
// ===============================
app.use((err, req, res, next) => {
  console.error('ERROR MIDDLEWARE');
  console.error(err);

  res.status(500).json({
    error: err.message,
    stack: err.stack
  });
});

// ===============================
// 404
// ===============================
app.use((req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    code: 'NOT_FOUND'
  });
});

export default app;
