import { useState, useRef, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useChat } from '../hooks/useChat';
import ChatMessage from '../components/ChatMessage';
import SourceDrawer from '../components/SourceDrawer';
import { Send, BookOpen, X, Bot, MessageSquarePlus, ChevronLeft, ChevronRight, Settings } from 'lucide-react';
import type { SearchResult, Citation } from '../types';
import { Button } from '../components/ui/Button';
import { cn } from '../lib/utils';

interface AssistantProps {
  sessionId: string;
}

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return 'Good morning';
  if (hour < 17) return 'Good afternoon';
  return 'Good evening';
}

export default function Assistant({ sessionId }: AssistantProps) {
  const { messages, isLoading, sendMessage, cancel, clearMessages } = useChat(sessionId);
  const [input, setInput] = useState('');
  const [sourceDrawerOpen, setSourceDrawerOpen] = useState(false);
  const [selectedSources, setSelectedSources] = useState<SearchResult[]>([]);
  const [selectedCitations, setSelectedCitations] = useState<Citation[]>([]);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const sessionInitRef = useRef<Promise<any> | null>(null);
  const initFiredRef = useRef<boolean>(false);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    if (initFiredRef.current) return;
    initFiredRef.current = true;
    sessionInitRef.current = fetch('/api/session/init', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-session-id': sessionId }
    }).catch(err => console.warn('Session pre-init failed:', err.message));
  }, [sessionId]);

  const isFirstMessage = messages.length === 0;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    sendMessage(input.trim(), sessionInitRef.current ?? undefined);
    setInput('');
  };

  const handleNewConversation = () => {
    clearMessages();
    setSourceDrawerOpen(false);
    setTimeout(() => inputRef.current?.focus(), 50);
  };

  const handleShowSources = (sources: SearchResult[], citations: Citation[]) => {
    setSelectedSources(sources);
    setSelectedCitations(citations);
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
        window.dispatchEvent(new CustomEvent('websearch-result', { detail: { answer: data.answer, sources: data.sources } }));
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

  const userInitials = 'S';
  const userName = 'Sai';

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background">

      {/* ── Sidebar ─────────────────────────────────────────────── */}
      <aside
        className={cn(
          'hidden md:flex flex-shrink-0 flex-col border-r bg-secondary/30 transition-all duration-300',
          sidebarCollapsed ? 'w-[56px]' : 'w-[260px]'
        )}
      >
        {/* Logo row */}
        <div className={cn(
          'flex h-14 flex-shrink-0 items-center border-b px-3',
          sidebarCollapsed ? 'justify-center' : 'gap-3'
        )}>
          {!sidebarCollapsed && (
            <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg bg-primary text-primary-foreground">
              <Bot className="h-5 w-5" />
            </div>
          )}
          {!sidebarCollapsed && (
            <div className="flex flex-col min-w-0">
              <span className="text-sm font-semibold leading-tight truncate">Knowledge Assistant</span>
              <span className="text-[11px] text-muted-foreground leading-tight">AI-Powered RAG</span>
            </div>
          )}
          <button
            onClick={() => setSidebarCollapsed(c => !c)}
            className={cn(
              'ml-auto flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-md text-muted-foreground hover:bg-accent hover:text-foreground transition-colors',
              sidebarCollapsed && 'ml-0'
            )}
            title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {sidebarCollapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
          </button>
        </div>

        {/* Nav */}
        <nav className="flex-1 overflow-y-auto p-2">
          <button
            onClick={handleNewConversation}
            className={cn(
              'flex w-full items-center rounded-lg px-2 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground gap-2.5',
              sidebarCollapsed && 'justify-center px-0'
            )}
            title="New Conversation"
          >
            <MessageSquarePlus className="h-4 w-4 flex-shrink-0" />
            {!sidebarCollapsed && <span>New Conversation</span>}
          </button>

          <Link
            to="/knowledge"
            className={cn(
              'flex w-full items-center rounded-lg px-2 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground gap-2.5',
              sidebarCollapsed && 'justify-center px-0'
            )}
            title="Knowledge Base"
          >
            <BookOpen className="h-4 w-4 flex-shrink-0" />
            {!sidebarCollapsed && <span>Knowledge Base</span>}
          </Link>
        </nav>

        {/* User profile strip */}
        <div className={cn(
          'flex flex-shrink-0 items-center border-t p-3 gap-3',
          sidebarCollapsed && 'justify-center p-2'
        )}>
          <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-full bg-primary text-primary-foreground text-xs font-semibold select-none">
            {userInitials}
          </div>
          {!sidebarCollapsed && (
            <>
              <span className="flex-1 text-sm font-medium truncate">{userName}</span>
              <button className="text-muted-foreground hover:text-foreground transition-colors" title="Settings">
                <Settings className="h-4 w-4" />
              </button>
            </>
          )}
        </div>
      </aside>

      {/* ── Main + Sources side-by-side ──────────────────────────── */}
      <div className="flex flex-1 min-w-0 overflow-hidden">

        {/* Main Chat */}
        <main className="flex flex-1 flex-col min-w-0">

          {/* ── Empty / Greeting state ── */}
          {isFirstMessage ? (
            <div className="flex flex-1 flex-col items-center justify-center px-4">
              <h1 className="text-3xl font-semibold mb-8 tracking-tight">
                {getGreeting()}, {userName}
              </h1>
              <form onSubmit={handleSubmit} className="w-full max-w-2xl">
                <div className="relative flex items-center gap-2 rounded-2xl border bg-background shadow-md px-4 py-3">
                  <input
                    ref={inputRef}
                    type="text"
                    value={input}
                    onChange={e => setInput(e.target.value)}
                    placeholder="Ask me anything..."
                    className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
                    autoFocus
                    disabled={isLoading}
                  />
                  <Button
                    type="submit"
                    disabled={!input.trim() || isLoading}
                    size="icon"
                    className="h-8 w-8 rounded-xl flex-shrink-0"
                  >
                    <Send className="h-4 w-4" />
                  </Button>
                </div>
                <p className="mt-2 text-center text-[11px] text-muted-foreground/60">
                  AI-generated responses may be inaccurate. Verify important information.
                </p>
              </form>
            </div>
          ) : (
            <>
              {/* Messages */}
              <div
                ref={messagesContainerRef}
                className="flex-1 overflow-y-auto px-6 pb-28 pt-6"
              >
                <div className="mx-auto max-w-3xl space-y-6">
                  {messages.map(msg => (
                    <ChatMessage
                      key={msg.id}
                      message={msg}
                      onShowSources={() => msg.sources && handleShowSources(msg.sources, msg.citations || [])}
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

              {/* Sticky input */}
              <div className="flex flex-shrink-0 flex-col border-t bg-background">
                <form
                  onSubmit={handleSubmit}
                  className="mx-auto flex w-full max-w-3xl items-end gap-2 px-6 py-4"
                >
                  <div className="relative flex-1">
                    <input
                      ref={inputRef}
                      type="text"
                      value={input}
                      onChange={e => setInput(e.target.value)}
                      placeholder="Ask a question..."
                      className="w-full rounded-xl border bg-background px-4 py-3 pr-12 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary"
                      disabled={isLoading}
                    />
                  </div>
                  {isLoading ? (
                    <Button type="button" variant="outline" size="icon" onClick={cancel} className="h-11 w-11 flex-shrink-0 rounded-xl">
                      <X className="h-4 w-4" />
                    </Button>
                  ) : (
                    <Button type="submit" disabled={!input.trim()} className="h-11 w-11 flex-shrink-0 rounded-xl" size="icon">
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
            </>
          )}
        </main>

        {/* ── Inline Sources Panel ─────────────────────────────── */}
        {sourceDrawerOpen && (
          <SourceDrawer
            isOpen={sourceDrawerOpen}
            onClose={() => setSourceDrawerOpen(false)}
            sources={selectedSources}
            citations={selectedCitations}
          />
        )}
      </div>
    </div>
  );
}
