import serverless from 'serverless-http';
import express from 'express';
import cors from 'cors';
import { EventEmitter } from 'events';

// Import routers
import healthRouter from '../../server/api/health.js';
import documentsRouter from '../../server/api/documents.js';
import chatRouter from '../../server/api/chat.js';
import feedbackRouter from '../../server/api/feedback.js';
import { getOrCreateSession, initSessionWithGlobalDocs } from '../../server/services/sessionService.js';
import { addTurnWithCitations, clearMemory } from '../../server/services/memoryService.js';

const app = express();

// Progress callbacks
app.locals.progressCallbacks = new EventEmitter();

// Middleware
app.use(cors({
  origin: true,
  credentials: true
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Request Logger
app.use((req, res, next) => {
  console.log(`${req.method} ${req.originalUrl}`);
  next();
});

// TEST ROUTE
app.get('/ping', (req, res) => {
  console.log('PING ROUTE EXECUTED');
  res.json({
    success: true,
    message: 'Express backend is alive'
  });
});

// SESSION INIT ROUTE
app.post('/session/init', async (req, res) => {
  const sessionId = req.headers['x-session-id'];

  if (!sessionId) {
    return res.status(400).json({ error: 'Missing x-session-id header', code: 'MISSING_SESSION' });
  }

  getOrCreateSession(sessionId);

  try {
    await initSessionWithGlobalDocs(sessionId);
    res.json({ ready: true, sessionId });
  } catch (err) {
    console.warn('Session init warning:', err.message);
    res.json({ ready: false, sessionId, warning: err.message });
  }
});

// SESSION RESTORE MEMORY ROUTE
app.post('/session/restore-memory', (req, res) => {
  const { convId, messages } = req.body;

  if (!convId || !Array.isArray(messages)) {
    return res.status(400).json({ error: 'convId and messages are required', code: 'BAD_REQUEST' });
  }

  try {
    clearMemory(convId);

    for (const msg of messages) {
      if ((msg.role === 'user' || msg.role === 'assistant') && typeof msg.content === 'string') {
        addTurnWithCitations(convId, msg.role, msg.content);
      }
    }
    res.json({ ok: true, convId, restored: messages.length });
  } catch (err) {
    console.warn('Memory restore warning:', err.message);
    res.json({ ok: false, convId, warning: err.message });
  }
});

// ROUTERS
app.use('/health', healthRouter);
app.use('/documents', documentsRouter);
app.use('/chat', chatRouter);
app.use('/feedback', feedbackRouter);

// ERROR HANDLER
app.use((err, req, res, next) => {
  console.error('ERROR MIDDLEWARE');
  console.error(err);
  res.status(500).json({
    error: err.message,
    stack: err.stack
  });
});

// 404
app.use((req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    code: 'NOT_FOUND'
  });
});

export const handler = serverless(app, {
  basePath: '/api'
});
