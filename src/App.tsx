import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { useState } from 'react';
import Assistant from './pages/Assistant';
import KnowledgeBase from './pages/KnowledgeBase';

const STORAGE_KEY = 'rag_session_id';
const STORAGE_EXPIRY_KEY = 'rag_session_expiry';
const SESSION_TTL_MS = 60 * 60 * 1000; // 1 hour — matches server-side session timeout

function App() {
  const [sessionId] = useState<string>(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    const expiry = localStorage.getItem(STORAGE_EXPIRY_KEY);

    // Reuse if exists and not expired
    if (stored && expiry && Date.now() < parseInt(expiry)) {
      return stored;
    }

    // Expired or missing — generate new session
    const newId = crypto.randomUUID();
    localStorage.setItem(STORAGE_KEY, newId);
    localStorage.setItem(STORAGE_EXPIRY_KEY, (Date.now() + SESSION_TTL_MS).toString());
    console.log(`🆕 New session created: ${newId}`);
    return newId;
  });

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Assistant sessionId={sessionId} />} />
        <Route path="/assistant" element={<Assistant sessionId={sessionId} />} />
        <Route path="/knowledge" element={<KnowledgeBase sessionId={sessionId} />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
