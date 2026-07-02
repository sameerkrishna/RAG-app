import { useState, useRef, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useChat, getAllConversations, deleteConversation, markNavigationToKB, resetNavigationFlag } from '../hooks/useChat';
import type { StoredConversation } from '../hooks/useChat';
import ChatMessage from '../components/ChatMessage';
import SourceDrawer from '../components/SourceDrawer';
import { Send, BookOpen, X, Bot, MessageSquarePlus, ChevronLeft, ChevronRight, Settings, Trash2 } from 'lucide-react';
import type { SearchResult, Citation } from '../types';
import { Button } from '../components/ui/Button';
import { cn } from '../lib/utils';
import { useSeeding } from '../context/SeedingContext';

const ACTIVE_CONV_KEY = 'rag_active_conv_id';

// Module-level guard: persists across remounts (KB navigation) because the
// JS module stays loaded, but is wiped on hard reload / new tab — unlike
// sessionStorage which survives hard reloads.
const initedSessions = new Set<string>();

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
  const navigate = useNavigate();
  const { messages, isLoading, isThinking, activeConvId, sendMessage, cancel, clearMessages, loadConversation } = useChat(sessionId);
  const { isSeeding, setIsSeeding } = useSeeding();
  const [input, setInput] = useState('');
  const [sourceDrawerOpen, setSourceDrawerOpen] = useState(false);
  const [selectedSources, setSelectedSources] = useState<SearchResult[]>([]);
  const [selectedCitations, setSelectedCitations] = useState<Citation[]>([]);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [conversations, setConversations] = useState<StoredConversation[]>(() => getAllConversations());
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const sessionInitRef = useRef<Promise<any> | null>(null);

  useEffect(() => {
    if (activeConvId) {
      const conv = getAllConversations().find(c => c.id === activeConvId);
      if (conv) loadConversation(conv);
    } else {
      clearMessages();
    }
  }, []);

  useEffect(() => {
    if (activeConvId) sessionStorage.setItem(ACTIVE_CONV_KEY, activeConvId);
  }, [activeConvId]);

  useEffect(() => {
    setConversations(getAllConversations());
  }, [activeConvId, messages.length]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    if (initedSessions.has(sessionId)) {
      console.log('[session] Already inited, skipping fetch');
      sessionInitRef.current = Promise.resolve();
      return;
    }

    console.log('[session] Firing session/init for', sessionId);
    initedSessions.add(sessionId);
    // Only set isSeeding on genuine first load — initedSessions guard above
    // ensures this branch never runs on remounts or KB navigation.
    setIsSeeding(true);
    sessionInitRef.current = fetch('/api/session/init', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-session-id': sessionId }
    })
      .then(() => setIsSeeding(false))
      .catch(err => {
        console.warn('Session pre-init failed:', err.message);
        setIsSeeding(false);
      });
  }, [sessionId]);

  const isFirstMessage = messages.length === 0;

  const activeConvTitle = activeConvId
    ? (conversations.find(c => c.id === activeConvId)?.title ?? 'New Conversation')
    : 'New Conversation';

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading || isSeeding) return;
    sendMessage(input.trim(), sessionInitRef.current ?? undefined);
    setInput('');
  };

  const handleNewConversation = () => {
    resetNavigationFlag();
    clearMessages();
    sessionStorage.removeItem(ACTIVE_CONV_KEY);
    setSourceDrawerOpen(false);
    setTimeout(() => inputRef.current?.focus(), 50);
  };

  const handleNavigateToKB = () => {
    markNavigationToKB();
    navigate('/knowledge');
  };

  const handleLoadConversation = (conv: StoredConversation) => {
    loadConversation(conv);
    setSourceDrawerOpen(false);
    setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' }), 50);
  };

  const handleDeleteConversation = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    deleteConversation(id);
    setConversations(getAllConversations());
    if (activeConvId === id) handleNewConversation();
  };

  const handleShowSources = (sources: SearchResult[], citations: Citation[]) => {
    setSelectedSources(sources);
    setSelectedCitations(citations);
    setSourceDrawerOpen(true);
  };

  const getLastUserQuery = (): string => {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === 'user') return messages[i].content;
    }
    return '';
  };

  const userInitials = 'U';
  const userName = 'You';

  return (
    <div className="flex h-screen w-full overflow-hidden bg-background">

      {/* Sidebar */}
      <aside
        className={cn(
          'hidden md:flex flex-shrink-0 flex-col border-r bg-secondary/30 transition-all duration-300',
          sidebarCollapsed ? 'w-[56px]' : 'w-[260px]'
        )}
      >
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

        <nav className="flex-1 overflow-y-auto p-2 space-y-1">
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

          <button
            onClick={handleNavigateToKB}
            className={cn(
              'flex w-full items-center rounded-lg px-2 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground gap-2.5',
              sidebarCollapsed && 'justify-center px-0'
            )}
            title="Knowledge Base"
          >
            <BookOpen className="h-4 w-4 flex-shrink-0" />
            {!sidebarCollapsed && <span>Knowledge Base</span>}
          </button>

          {!sidebarCollapsed && conversations.length > 0 && (
            <div className="pt-3">
              <p className="px-2 pb-1 text-[11px] font-medium text-muted-foreground uppercase tracking-wider">Recents</p>
              {conversations.map(conv => (
                <button
                  key={conv.id}
                  onClick={() => handleLoadConversation(conv)}
                  className={cn(
                    'group flex w-full items-center justify-between rounded-lg px-2 py-2 text-sm transition-colors hover:bg-accent',
                    activeConvId === conv.id
                      ? 'bg-accent text-accent-foreground font-medium'
                      : 'text-muted-foreground'
                  )}
                >
                  <span className="truncate text-left flex-1">{conv.title}</span>
                  <span
                    role="button"
                    onClick={(e) => handleDeleteConversation(e, conv.id)}
                    className="ml-1 flex-shrink-0 opacity-0 group-hover:opacity-100 p-0.5 rounded hover:text-destructive transition-opacity"
                    title="Delete"
                  >
                    <Trash2 className="h-3 w-3" />
                  </span>
                </button>
              ))}
            </div>
          )}
        </nav>

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

      {/* Main + Sources */}
      <div className="flex flex-1 min-w-0 overflow-hidden">
        <main className="flex flex-1 flex-col min-w-0">

          {isFirstMessage ? (
            <div className="flex flex-1 flex-col items-center justify-center px-4">
              <h1 className="text-3xl font-semibold mb-8 tracking-tight">
                {getGreeting()} 👋
              </h1>
              <form onSubmit={handleSubmit} className="w-full max-w-2xl">
                <div className={cn(
                  'relative flex items-center gap-2 rounded-2xl border bg-background shadow-md px-4 py-3 transition-opacity',
                  isSeeding && 'opacity-70'
                )}>
                  <input
                    ref={inputRef}
                    type="text"
                    value={input}
                    onChange={e => setInput(e.target.value)}
                    placeholder={isSeeding ? 'Setting up things...' : 'Ask me anything...'}
                    className="flex-1 bg-transparent text-sm outline-none placeholder:text-muted-foreground"
                    disabled={isSeeding}
                    autoFocus
                  />
                  <Button type="submit" disabled={!input.trim() || isLoading || isSeeding} size="icon" className="h-8 w-8 rounded-xl flex-shrink-0">
                    <Send className="h-4 w-4" />
                  </Button>
                </div>
                {isSeeding ? (
                  <p className="mt-2 text-center text-[11px] text-muted-foreground/70 flex items-center justify-center gap-1.5">
                    <span className="inline-block h-1.5 w-1.5 rounded-full bg-primary animate-pulse" />
                    Setting up things...
                  </p>
                ) : (
                  <p className="mt-2 text-center text-[11px] text-muted-foreground/60">
                    AI-generated responses may be inaccurate. Verify important information.
                  </p>
                )}
              </form>
            </div>
          ) : (
            <>
              <header className="flex h-14 flex-shrink-0 items-center border-b bg-background px-4">
                <span className="text-sm font-semibold truncate">{activeConvTitle}</span>
              </header>

              <div ref={messagesContainerRef} className="flex-1 overflow-y-auto px-2 pb-28 pt-6">
                <div className="mx-auto max-w-5xl space-y-6">
                  {messages.map(msg => (
                    <ChatMessage
                      key={msg.id}
                      message={msg}
                      onShowSources={() => msg.sources && handleShowSources(msg.sources, msg.citations || [])}
                      onRetry={() => getLastUserQuery() && sendMessage(getLastUserQuery())}
                    />
                  ))}

                  {isThinking && (
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

              <div className="flex flex-shrink-0 flex-col border-t bg-background">
                <form onSubmit={handleSubmit} className="mx-auto flex w-full max-w-5xl items-end gap-2 px-2 py-4">
                  <div className="relative flex-1">
                    <input
                      ref={inputRef}
                      type="text"
                      value={input}
                      onChange={e => setInput(e.target.value)}
                      placeholder="Ask a question..."
                      className="w-full rounded-xl border bg-background px-4 py-3 pr-12 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/30 focus:border-primary"
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
