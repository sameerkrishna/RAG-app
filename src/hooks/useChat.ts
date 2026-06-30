import { useState, useCallback, useRef } from 'react';
import type { ChatMessage, Citation, SearchResult, CoverageInfo } from '../types';

// ── localStorage helpers ───────────────────────────────────────────────────
const STORAGE_KEY = 'rag_conversations';
const ACTIVE_CONV_KEY = 'rag_active_conv_id';

// Module-level flags — live in JS memory only.
// Reset to false on hard reload/new tab (module re-executes).
// Survive SPA navigation (module scope stays alive).
let _isNavigationBack = false;

export function markNavigationToKB() {
  _isNavigationBack = true;
}

export function resetNavigationFlag() {
  _isNavigationBack = false;
}

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

function getInitialConversationState(): { messages: ChatMessage[]; activeConvId: string | null } {
  // Do NOT consume the flag here — StrictMode mounts twice, flag must survive both mounts.
  // Flag is only reset explicitly via resetNavigationFlag() when user starts a new conversation.
  if (!_isNavigationBack) {
    return { messages: [], activeConvId: null };
  }

  try {
    const savedConvId = sessionStorage.getItem(ACTIVE_CONV_KEY);
    if (savedConvId) {
      const conv = loadAllConversations().find(c => c.id === savedConvId);
      if (conv) return { messages: conv.messages, activeConvId: conv.id };
    }
  } catch {}
  return { messages: [], activeConvId: null };
}

// ── hook ──────────────────────────────────────────────────────────────────
export function useChat(sessionId: string) {
  const initialState = getInitialConversationState();

  const [messages, setMessages] = useState<ChatMessage[]>(initialState.messages);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeConvId, setActiveConvId] = useState<string | null>(initialState.activeConvId);

  const activeConvIdRef = useRef<string | null>(initialState.activeConvId);
  const restoredConvIdRef = useRef<string | null>(null);
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

    const convId = activeConvIdRef.current ?? crypto.randomUUID();
    activeConvIdRef.current = convId;

    const userMessage: ChatMessage = {
      id: userMessageId,
      role: 'user',
      content: query,
      timestamp: new Date()
    };

    setMessages(prev => {
      const next = [...prev, userMessage];
      persist(next, convId);
      return next;
    });
    setActiveConvId(convId);

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
  }, [sessionId, persist]);

  const cancel = useCallback(() => {
    abortControllerRef.current?.abort();
    setIsLoading(false);
  }, []);

  const clearMessages = useCallback(() => {
    activeConvIdRef.current = null;
    restoredConvIdRef.current = null;
    setMessages([]);
    setError(null);
    setActiveConvId(null);
  }, []);

  const loadConversation = useCallback(async (conv: StoredConversation) => {
    activeConvIdRef.current = conv.id;
    setMessages(conv.messages);
    setActiveConvId(conv.id);
    setError(null);

    if (restoredConvIdRef.current === conv.id) return;
    restoredConvIdRef.current = conv.id;

    try {
      await fetch('/api/session/restore-memory', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-session-id': sessionId },
        body: JSON.stringify({
          convId: conv.id,
          messages: conv.messages
            .filter(m => m.role === 'user' || m.role === 'assistant')
            .map(m => ({ role: m.role, content: m.content }))
        })
      });
    } catch (err) {
      console.warn('Memory restore failed (non-fatal):', err);
    }
  }, [sessionId]);

  return { messages, isLoading, error, activeConvId, sendMessage, cancel, clearMessages, loadConversation };
}
