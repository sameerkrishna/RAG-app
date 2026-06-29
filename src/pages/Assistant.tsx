import { useState, useRef, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useChat } from '../hooks/useChat';
import ChatMessage from '../components/ChatMessage';
import SourceDrawer from '../components/SourceDrawer';
import { Send, BookOpen, X, Bot, MessageSquarePlus } from 'lucide-react';
import type { SearchResult } from '../types';
import { Button } from '../components/ui/Button';

interface AssistantProps {
  sessionId: string;
}

export default function Assistant({ sessionId }: AssistantProps) {
  const { messages, isLoading, sendMessage, cancel, clearMessages } = useChat(sessionId);
  const [input, setInput] = useState('');
  const [sourceDrawerOpen, setSourceDrawerOpen] = useState(false);
  const [selectedSources, setSelectedSources] = useState<SearchResult[]>([]);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    sendMessage(input.trim());
    setInput('');
  };

  const handleShowSources = (sources: SearchResult[]) => {
    setSelectedSources(sources);
    setSourceDrawerOpen(true);
  };

  const handleWebSearch = async (lastQuery: string) => {
    try {
      const response = await fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-session-id': sessionId },
        body: JSON.stringify({ query: lastQuery })
      });

      const data = await response.json();
      if (data.success && data.answer) {
        window.dispatchEvent(new CustomEvent('websearch-result', {
          detail: { answer: data.answer, sources: data.sources }
        }));
      }
    } catch (err) {
      console.error('Web search error:', err);
    }
  };

  const getLastUserQuery = (): string => {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === 'user') return messages[i].content;
    }
    return '';
  };

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background">
      {/* Sidebar */}
      <aside className="hidden md:flex w-[280px] flex-shrink-0 flex-col border-r bg-secondary/30">
        {/* Logo */}
        <div className="flex h-14 flex-shrink-0 items-center gap-3 border-b px-4">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <Bot className="h-5 w-5" />
          </div>
          <div className="flex flex-col">
            <span className="text-sm font-semibold leading-tight">Knowledge Assistant</span>
            <span className="text-[11px] text-muted-foreground leading-tight">AI-Powered RAG</span>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 overflow-y-auto p-3">
          <Link
            to="/knowledge"
            className="flex items-center gap-2.5 rounded-lg px-3 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            <BookOpen className="h-4 w-4" />
            Knowledge Base
          </Link>
        </nav>

        {/* Bottom Actions */}
        <div className="flex flex-shrink-0 flex-col gap-2 border-t p-3">
          <Button
            variant="outline"
            size="sm"
            className="w-full justify-start gap-2"
            onClick={clearMessages}
          >
            <MessageSquarePlus className="h-4 w-4" />
            New Conversation
          </Button>
          <div className="flex items-center gap-2 px-3 py-1">
            <div className="h-2 w-2 rounded-full bg-success" />
            <span className="text-[11px] text-muted-foreground">Session active</span>
          </div>
        </div>
      </aside>

      {/* Main Chat Area */}
      <main className="flex flex-1 flex-col min-w-0">
        {/* Header */}
        <header className="flex h-14 flex-shrink-0 items-center justify-between border-b bg-background px-6">
          <h2 className="text-sm font-medium">Chat</h2>
          <span className="text-xs text-muted-foreground">
            Session: {sessionId.slice(0, 8)}...
          </span>
        </header>

        {/* Messages */}
        <div
          ref={messagesContainerRef}
          className="flex-1 overflow-y-auto px-6 pb-28 pt-6"
        >
          {messages.length === 0 && (
            <div className="flex h-full flex-col items-center justify-center text-center">
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-muted mb-6">
                <Bot className="h-8 w-8 text-muted-foreground/50" />
              </div>
              <h3 className="text-lg font-medium mb-1">Ask me anything</h3>
              <p className="text-sm text-muted-foreground max-w-sm">
                I will search your knowledge base for answers and cite my sources.
              </p>
            </div>
          )}

          <div className="mx-auto max-w-3xl space-y-6">
            {messages.map(msg => (
              <ChatMessage
                key={msg.id}
                message={msg}
                onShowSources={() => msg.sources && handleShowSources(msg.sources)}
                onWebSearch={() => handleWebSearch(getLastUserQuery())}
                onRetry={() => getLastUserQuery() && sendMessage(getLastUserQuery())}
              />
            ))}

            {isLoading && messages[messages.length - 1]?.role === 'user' && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                <span className="inline-flex gap-1">
                  <span className="h-2 w-2 rounded-full bg-muted-foreground animate-bounce" style={{ animationDelay: '0ms' }} />
                  <span className="h-2 w-2 rounded-full bg-muted-foreground animate-bounce" style={{ animationDelay: '150ms' }} />
                  <span className="h-2 w-2 rounded-full bg-muted-foreground animate-bounce" style={{ animationDelay: '300ms' }} />
                </span>
                <span>Thinking...</span>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>
        </div>

        {/* Input Area */}
        <div className="flex flex-shrink-0 flex-col border-t bg-background">
          <form
            onSubmit={handleSubmit}
            className="mx-auto flex w-full max-w-3xl items-end gap-2 px-6 py-4"
          >
            <div className="relative flex-1">
              <input
                type="text"
                value={input}
                onChange={e => setInput(e.target.value)}
                placeholder="Ask a question..."
                className="w-full rounded-xl border bg-background px-4 py-3 pr-12 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary"
                disabled={isLoading}
              />
            </div>
            {isLoading ? (
              <Button
                type="button"
                variant="outline"
                size="icon"
                onClick={cancel}
                className="h-11 w-11 flex-shrink-0 rounded-xl"
              >
                <X className="h-4 w-4" />
              </Button>
            ) : (
              <Button
                type="submit"
                disabled={!input.trim()}
                className="h-11 w-11 flex-shrink-0 rounded-xl"
                size="icon"
              >
                <Send className="h-4 w-4" />
              </Button>
            )}
          </form>
          <div className="pb-2 text-center">
            <span className="text-[11px] text-muted-foreground/60">
              AI-generated responses may be inaccurate. Verify important information.
            </span>
          </div>
        </div>
      </main>

      {/* Source Drawer */}
      <SourceDrawer
        isOpen={sourceDrawerOpen}
        onClose={() => setSourceDrawerOpen(false)}
        sources={selectedSources}
      />
    </div>
  );
}
