import { createContext, useContext, useState, useCallback, useRef } from 'react';
import type { ReactNode } from 'react';
import type { ChatMessage, Citation, SearchResult, CoverageInfo } from '../types';

// ── localStorage helpers ───────────────────────────────────────────────────
const STORAGE_KEY = 'rag_conversations';
const ACTIVE_CONV_KEY = 'rag_active_conv_id';

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

export function loadAllConversations(): StoredConversation[] {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
  } catch {
    return [];
  }
}

export function saveAllConversations(convs: StoredConversation[]) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(convs));
  } catch { }
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
  if (!_isNavigationBack) {
    return { messages: [], activeConvId: null };
  }

  try {
    const savedConvId = sessionStorage.getItem(ACTIVE_CONV_KEY);
    if (savedConvId) {
      const conv = loadAllConversations().find(c => c.id === savedConvId);
      if (conv) return { messages: conv.messages, activeConvId: conv.id };
    }
  } catch { }
  return { messages: [], activeConvId: null };
}

// --- Seeding Context ---
interface SeedingContextValue {
  isSeeding: boolean;
  setIsSeeding: (value: boolean) => void;
}

const SeedingContext = createContext<SeedingContextValue>({
  isSeeding: false,
  setIsSeeding: () => { }
});

export function useSeeding() {
  return useContext(SeedingContext);
}

// --- Chat Context ---
interface ChatContextValue {
  messages: ChatMessage[];
  isLoading: boolean;
  isThinking: boolean;
  error: string | null;
  activeConvId: string | null;
  sendMessage: (query: string, waitFor?: Promise<any>) => void;
  sendFeedback: (answerKey: string, feedback: 'like' | 'dislike') => void;
  cancel: () => void;
  clearMessages: () => void;
  loadConversation: (conv: StoredConversation) => void;
}

const ChatContext = createContext<ChatContextValue | null>(null);

export function useChat() {
  const ctx = useContext(ChatContext);
  if (!ctx) throw new Error('useChat must be used within AppProvider');
  return ctx;
}

// --- App Provider ---
interface AppProviderProps {
  children: ReactNode;
  sessionId: string;
}

export function AppProvider({ children, sessionId }: AppProviderProps) {
  // Seeding State
  const [isSeeding, setIsSeeding] = useState(false);

  // Chat State
  const initialState = useRef(getInitialConversationState());
  const [messages, setMessages] = useState<ChatMessage[]>(initialState.current.messages);
  const [isLoading, setIsLoading] = useState(false);
  const [isThinking, setIsThinking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeConvId, setActiveConvId] = useState<string | null>(initialState.current.activeConvId);

  const activeConvIdRef = useRef<string | null>(initialState.current.activeConvId);
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
    if (waitFor) await waitFor;

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
    console.log("In callback");
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
    setIsThinking(true);

    abortControllerRef.current = new AbortController();

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-session-id': sessionId
        },
        body: JSON.stringify({ query, sessionId, convId, messageId: assistantMessageId }),
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

      console.log("before while")
      while (true) {
        const { done, value } = await reader.read();
        console.log("in  while")
        if (done) break;
        console.log("in  while B")
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEventName = line.slice(7).trim();
            continue;
          }
          console.log("in  while C")
          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            try {
              const payload = JSON.parse(data);
              console.log("Parsed event:", currentEventName, payload);

              if (currentEventName === 'token' && payload.text) {
                setIsThinking(false);
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
                setIsThinking(false);
                citations = payload.citations || [];
                coverage = payload.coverage;
                sources = payload.sources || [];
                const isRefusal = payload.action === 'refusal';
                const finalResponse = payload.response || accumulatedText;
                console.log("In event ===complete")
                setMessages(prev => {
                  const next = prev.map(m =>
                    m.id === assistantMessageId
                      ? { ...m, content: finalResponse, citations, coverage, sources, isRefusal, isStreaming: false }
                      : m
                  );
                  persist(next, convId);
                  return next;
                });

              } else if (currentEventName === 'error') {
                setIsThinking(false);
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
            } catch (e) {
              console.error('Error processing SSE data:', e, data);
            }
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
      setIsThinking(false);
      setIsLoading(false);
      abortControllerRef.current = null;
    }
  }, [sessionId, persist]);

  const sendFeedback = useCallback(async (answerKey: string, feedback: 'like' | 'dislike') => {
    try {
      const response = await fetch('/api/chat/feedback', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ answerId: answerKey, feedback })
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (err) {
      console.error('Error updating feedback:', err);
    }
  }, []);

  const cancel = useCallback(() => {
    abortControllerRef.current?.abort();
    setIsThinking(false);
    setIsLoading(false);
  }, []);

  const clearMessages = useCallback(() => {
    activeConvIdRef.current = null;
    restoredConvIdRef.current = null;
    setMessages([]);
    setError(null);
    setIsThinking(false);
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

  const chatContextValue = {
    messages,
    isLoading,
    isThinking,
    error,
    activeConvId,
    sendMessage,
    sendFeedback,
    cancel,
    clearMessages,
    loadConversation
  };

  return (
    <SeedingContext.Provider value={{ isSeeding, setIsSeeding }}>
      <ChatContext.Provider value={chatContextValue}>
        {children}
      </ChatContext.Provider>
    </SeedingContext.Provider>
  );
}
