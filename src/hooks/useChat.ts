import { useState, useCallback, useRef } from 'react';
import type { ChatMessage, Citation, SearchResult, CoverageInfo } from '../types';

// ── localStorage helpers ───────────────────────────────────────────────────
const STORAGE_KEY = 'rag_conversations';

export interface StoredConversation {
  id: string;           // uuid
  title: string;        // first user message (truncated)
  updatedAt: string;    // ISO timestamp — used for sorting
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
  } catch {
    // localStorage full — silently ignore
  }
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
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    const recent = getAllConversations()[0];
    return recent ? recent.messages : [];
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Issue 2 fix: restore activeConvId on mount
  const [activeConvId, setActiveConvId] = useState<string | null>(() => {
    const recent = getAllConversations()[0];
    return recent ? recent.id : null;
  });

  // Issue 1 fix: use a ref so convId is synchronously available inside sendMessage
  // even before setActiveConvId re-renders
  const activeConvIdRef = useRef<string | null>(() => {
    const recent = getAllConversations()[0];
    return recent ? recent.id : null;
  } as unknown as string | null);

  // Initialise ref in sync with state
  if (activeConvIdRef.current === undefined as unknown as null) {
    const recent = getAllConversations()[0];
    activeConvIdRef.current = recent ? recent.id : null;
  }

  const abortControllerRef = useRef<AbortController | null>(null);

  // Persist current messages to localStorage under activeConvId
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

    // Issue 1 fix: use ref for synchronous access — no stale closure on first message
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
        // Issue 5 fix: send convId so server uses it as memory key
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
    // Issue 5 fix: generate fresh convId so new conversation gets isolated server memory
    const newConvId = crypto.randomUUID();
    activeConvIdRef.current = newConvId;
    setMessages([]);
    setError(null);
    setActiveConvId(null); // null = no active conv yet, will be set on first message
    activeConvIdRef.current = null;
  }, []);

  // Restore a past conversation
  const loadConversation = useCallback((conv: StoredConversation) => {
    // Issue 5 fix: update ref so next message uses correct convId
    activeConvIdRef.current = conv.id;
    setMessages(conv.messages);
    setActiveConvId(conv.id);
    setError(null);
  }, []);

  return { messages, isLoading, error, activeConvId, sendMessage, cancel, clearMessages, loadConversation };
}
