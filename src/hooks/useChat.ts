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

    // Add user message
    setMessages(prev => [...prev, {
      id: userMessageId,
      role: 'user',
      content: query,
      timestamp: new Date()
    }]);

    // Add placeholder assistant message
    setMessages(prev => [...prev, {
      id: assistantMessageId,
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      isStreaming: true
    }]);

    setError(null);
    setIsLoading(true);

    // Create abort controller
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
      let isRefusal = false;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const lines = chunk.split('\n');

        for (const line of lines) {
          if (line.startsWith('event: ')) continue;
          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            try {
              const event = JSON.parse(data);

              if (event.type === 'token' && event.text) {
                accumulatedText += event.text;
                setMessages(prev => prev.map(m =>
                  m.id === assistantMessageId
                    ? { ...m, content: accumulatedText }
                    : m
                ));
              } else if (event.type === 'complete') {
                // Final update
                citations = event.citations || [];
                coverage = event.coverage;
                sources = event.sources || [];
                isRefusal = event.action === 'refusal';

                setMessages(prev => prev.map(m =>
                  m.id === assistantMessageId
                    ? {
                      ...m,
                      content: event.response || accumulatedText,
                      citations,
                      coverage,
                      sources,
                      isRefusal,
                      isStreaming: false
                    }
                    : m
                ));
              } else if (event.type === 'error') {
                setError(event.message);
                setMessages(prev => prev.map(m =>
                  m.id === assistantMessageId
                    ? { ...m, content: event.message, isStreaming: false }
                    : m
                ));
              } else if (event.type === 'retrieval') {
                coverage = {
                  level: event.coverage,
                  score: event.coverageScore
                };
              }
            } catch (e) {
              // Ignore parse errors
            }
          }
        }
      }

      // Ensure streaming is marked as done
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
