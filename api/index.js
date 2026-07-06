import express from 'express';
import app from '../server/app.js';

// Vercel routes /api/* to this function. The Express app defines routes
// without the /api prefix (e.g. /session/init, /chat, /documents).
// Mount it under /api so the paths line up with what the frontend calls.
const vercelApp = express();
vercelApp.use('/api', app);

export default vercelApp;
