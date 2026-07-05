import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { useState } from 'react';
import Assistant from './pages/Assistant';
import KnowledgeBase from './pages/KnowledgeBase';
import { AppProvider } from './context/AppContext';

const STORAGE_KEY = 'rag_session_id';

function App() {
  const [sessionId] = useState<string>(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) return stored;
    const newId = crypto.randomUUID();
    localStorage.setItem(STORAGE_KEY, newId);
    return newId;
  });

  return (
    <AppProvider sessionId={sessionId}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Assistant sessionId={sessionId} />} />
          <Route path="/assistant" element={<Assistant sessionId={sessionId} />} />
          <Route path="/knowledge" element={<KnowledgeBase sessionId={sessionId} />} />
        </Routes>
      </BrowserRouter>
    </AppProvider>
  );
}

export default App;
