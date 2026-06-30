import { useState, useCallback, useRef } from 'react';
import type { ChatMessage, Citation, SearchResult, CoverageInfo } from '../types';

export function useChat(sessionId: string) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const sendMessage = useCallback(async (query: string) => {
    const userMessageId = crypto.randomUUID();
    const assistantMessageId = crypto.randomUUID();

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
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-session-id': sessionId
        },
        body: JSON.stringify({ query, sessionId }),
        signal: abortControllerRef.current.signal
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error('No response body');
      }

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
                    m.id === assistantMessageId
                      ? { ...m, content: accumulatedText }
                      : m
                  ));
                  const delay = Math.floor(Math.random() * 71) + 20;
                  await new Promise(resolve => setTimeout(resolve, delay));
                }

              } else if (currentEventName === 'complete') {
                citations = payload.citations || [];
                coverage = payload.coverage;
                sources = payload.sources || [];
                const isRefusal = payload.action === 'refusal';

                setMessages(prev => prev.map(m =>
                  m.id === assistantMessageId
                    ? {
                        ...m,
                        content: payload.response || accumulatedText,
                        citations,
                        coverage,
                        sources,
                        isRefusal,
                        isStreaming: false
                      }
                    : m
                ));

              } else if (currentEventName === 'error') {
                setError(payload.message);
                setMessages(prev => prev.map(m =>
                  m.id === assistantMessageId
                    ? { ...m, content: payload.message, isStreaming: false }
                    : m
                ));

              } else if (currentEventName === 'retrieval') {
                coverage = {
                  confidence: payload.confidence,
                  topScore: payload.topScore
                };
              }

              currentEventName = '';

            } catch (e) {
              // Ignore parse errors
            }
          }
        }
      }

      setMessages(prev => prev.map(m =>
        m.id === assistantMessageId
          ? { ...m, isStreaming: false }
          : m
      ));

    } catch (err: any) {
      if (err.name === 'AbortError') {
        console.log('Request aborted');
        return;
      }

      const errorMessage = err.message || 'An error occurred';
      setError(errorMessage);

      setMessages(prev => prev.map(m =>
        m.id === assistantMessageId
          ? {
              ...m,
              content: 'I encountered an error. Please try again.',
              isStreaming: false
            }
          : m
      ));
    } finally {
      setIsLoading(false);
      abortControllerRef.current = null;
    }
  }, [sessionId]);

  const cancel = useCallback(() => {
    abortControllerRef.current?.abort();
    setIsLoading(false);
  }, []);

  const clearMessages = useCallback(() => {
    setMessages([]);
    setError(null);
  }, []);

  return { messages, isLoading, error, sendMessage, cancel, clearMessages };
}
