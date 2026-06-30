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
import { getOrCreateSession, initSessionWithGlobalDocs } from './services/sessionService.js';

const app = express();

// Progress callbacks
app.locals.progressCallbacks = new EventEmitter();

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
// SESSION INIT ROUTE
// Called by frontend on chat screen mount — seeds global docs into session
// before the user sends their first message, eliminating first-message latency
// ===============================
app.post('/session/init', async (req, res) => {
  console.log('A')
  const sessionId = req.headers['x-session-id'];
console.log('session id: '+ sessionId)
  if (!sessionId) {
    console.log('returning')
    return res.status(400).json({ error: 'Missing x-session-id header', code: 'MISSING_SESSION' });
  }

  getOrCreateSession(sessionId);

  // Fire and don't block — client doesn't need to wait for full seeding
  // But we do await so the client knows when it's ready
  try {
    console.log('calling initsession now')
    await initSessionWithGlobalDocs(sessionId);
    res.json({ ready: true, sessionId });
  } catch (err) {
    // Non-fatal — chat still works, seeding will retry on first message
    console.warn('Session init warning:', err.message);
    res.json({ ready: false, sessionId, warning: err.message });
  }
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