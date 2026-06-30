import { useState, useCallback, useRef } from 'react';
import type { ChatMessage, Citation, SearchResult, CoverageInfo } from '../types';

// ── localStorage helpers ───────────────────────────────────────────────────
const STORAGE_KEY = 'rag_conversations';

export interface StoredConversation {
  id: string;
  title: string;
  updatedAt: string;
  messages: ChatMessage[];
}

function loadAllConversations(): StoredConversation[] {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
  } catch {
    return [];
  }
}

function saveAllConversations(convs: StoredConversation[]) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(convs));
  } catch {}
}

export function getAllConversations(): StoredConversation[] {
  return loadAllConversations().sort(
    (a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
  );
}

export function deleteConversation(id: string) {
  saveAllConversations(loadAllConversations().filter(c => c.id !== id));
}

// ── hook ──────────────────────────────────────────────────────────────────
export function useChat(sessionId: string) {
  // Issue 2 fix: restore last conversation on mount so KB nav doesn't lose state
  const recentConv = getAllConversations()[0] ?? null;

  const [messages, setMessages] = useState<ChatMessage[]>(
    recentConv ? recentConv.messages : []
  );
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeConvId, setActiveConvId] = useState<string | null>(
    recentConv ? recentConv.id : null
  );

  // Issue 1 fix: ref for synchronous convId access — avoids stale closure on first message
  const activeConvIdRef = useRef<string | null>(recentConv ? recentConv.id : null);

  const abortControllerRef = useRef<AbortController | null>(null);

  const persist = useCallback((msgs: ChatMessage[], convId: string) => {
    if (msgs.length === 0) return;
    const firstUserMsg = msgs.find(m => m.role === 'user');
    if (!firstUserMsg) return;
    const title = firstUserMsg.content.slice(0, 60) + (firstUserMsg.content.length > 60 ? '...' : '');
    const all = loadAllConversations();
    const idx = all.findIndex(c => c.id === convId);
    const entry: StoredConversation = {
      id: convId,
      title,
      updatedAt: new Date().toISOString(),
      messages: msgs
    };
    if (idx >= 0) all[idx] = entry;
    else all.push(entry);
    saveAllConversations(all);
  }, []);

  const sendMessage = useCallback(async (query: string, waitFor?: Promise<any>) => {
    const userMessageId = crypto.randomUUID();
    const assistantMessageId = crypto.randomUUID();

    // Issue 1 fix: read ref synchronously so first message gets correct convId immediately
    const convId = activeConvIdRef.current ?? crypto.randomUUID();
    activeConvIdRef.current = convId;
    if (!activeConvId) setActiveConvId(convId);

    setMessages(prev => [...prev, {
      id: userMessageId,
      role: 'user',
      content: query,
      timestamp: new Date()
    }]);

    setMessages(prev => [...prev, {
      id: assistantMessageId,
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      isStreaming: true
    }]);

    setError(null);
    setIsLoading(true);

    abortControllerRef.current = new AbortController();

    try {
      if (waitFor) await waitFor;

      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-session-id': sessionId
        },
        // Issue 5 fix: send convId so server uses it as isolated memory key
        body: JSON.stringify({ query, sessionId, convId }),
        signal: abortControllerRef.current.signal
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const reader = response.body?.getReader();
      if (!reader) throw new Error('No response body');

      const decoder = new TextDecoder();
      let accumulatedText = '';
      let citations: Citation[] = [];
      let coverage: CoverageInfo | undefined;
      let sources: SearchResult[] = [];
      let buffer = '';
      let currentEventName = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEventName = line.slice(7).trim();
            continue;
          }

          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            try {
              const payload = JSON.parse(data);

              if (currentEventName === 'token' && payload.text) {
                const words = payload.text.split(/(\s+)/);
                let i = 0;
                while (i < words.length) {
                  const groupSize = Math.floor(Math.random() * 10) + 1;
                  const group = words.slice(i, i + groupSize).join('');
                  i += groupSize;
                  accumulatedText += group;
                  setMessages(prev => prev.map(m =>
                    m.id === assistantMessageId ? { ...m, content: accumulatedText } : m
                  ));
                  await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 71) + 20));
                }

              } else if (currentEventName === 'complete') {
                citations = payload.citations || [];
                coverage = payload.coverage;
                sources = payload.sources || [];
                const isRefusal = payload.action === 'refusal';

                setMessages(prev => {
                  const next = prev.map(m =>
                    m.id === assistantMessageId
                      ? { ...m, content: payload.response || accumulatedText, citations, coverage, sources, isRefusal, isStreaming: false }
                      : m
                  );
                  persist(next, convId);
                  return next;
                });

              } else if (currentEventName === 'error') {
                setError(payload.message);
                setMessages(prev => prev.map(m =>
                  m.id === assistantMessageId ? { ...m, content: payload.message, isStreaming: false } : m
                ));

              } else if (currentEventName === 'retrieval') {
                coverage = {
                  confidence: Math.round((payload.score ?? payload.topScore ?? 0) * 100),
                  topScore: payload.topScore,
                  level: payload.level,
                  score: payload.score
                };
              }

              currentEventName = '';
            } catch (e) { /* ignore parse errors */ }
          }
        }
      }

      setMessages(prev => {
        const next = prev.map(m => m.id === assistantMessageId ? { ...m, isStreaming: false } : m);
        persist(next, convId);
        return next;
      });

    } catch (err: any) {
      if (err.name === 'AbortError') {
        console.log('Request aborted');
        return;
      }
      const errorMessage = err.message || 'An error occurred';
      setError(errorMessage);
      setMessages(prev => prev.map(m =>
        m.id === assistantMessageId
          ? { ...m, content: 'I encountered an error. Please try again.', isStreaming: false }
          : m
      ));
    } finally {
      setIsLoading(false);
      abortControllerRef.current = null;
    }
  }, [sessionId, activeConvId, persist]);

  const cancel = useCallback(() => {
    abortControllerRef.current?.abort();
    setIsLoading(false);
  }, []);

  const clearMessages = useCallback(() => {
    // Issue 5 fix: reset ref to null so next sendMessage generates a fresh convId
    activeConvIdRef.current = null;
    setMessages([]);
    setError(null);
    setActiveConvId(null);
  }, []);

  const loadConversation = useCallback((conv: StoredConversation) => {
    // Issue 5 fix: sync ref so next message uses the loaded conversation's convId
    activeConvIdRef.current = conv.id;
    setMessages(conv.messages);
    setActiveConvId(conv.id);
    setError(null);
  }, []);

  return { messages, isLoading, error, activeConvId, sendMessage, cancel, clearMessages, loadConversation };
}
