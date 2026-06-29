import ReactMarkdown from 'react-markdown';
import { Copy, ThumbsUp, ThumbsDown, Search, FileText, AlertCircle, RefreshCw } from 'lucide-react';
import type { ChatMessage as ChatMessageType } from '../types';
import { cn } from '../lib/utils';
import { useState } from 'react';
import { Button } from './ui/Button';

interface ChatMessageProps {
  message: ChatMessageType;
  onShowSources: () => void;
  onWebSearch: () => void;
  onRetry: () => void;
}

export default function ChatMessage({ message, onShowSources, onWebSearch, onRetry }: ChatMessageProps) {
  const [copied, setCopied] = useState(false);
  const [feedbackSent, setFeedbackSent] = useState<'positive' | 'negative' | null>(null);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(message.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleFeedback = async (type: 'positive' | 'negative') => {
    setFeedbackSent(type);
  };

  const isUser = message.role === 'user';

  return (
    <div className={cn('flex gap-3', isUser ? 'flex-row-reverse' : 'flex-row')}>
      {/* Avatar */}
      <div className={cn(
        'flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-full text-xs font-medium',
        isUser ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'
      )}>
        {isUser ? 'U' : 'AI'}
      </div>

      {/* Content */}
      <div className={cn('flex min-w-0 flex-1 flex-col', isUser ? 'items-end' : 'items-start')}>
        {/* Message bubble */}
        <div className={cn(
          'max-w-[85%] rounded-2xl px-4 py-2.5',
          isUser
            ? 'bg-primary text-primary-foreground'
            : 'bg-muted text-foreground'
        )}>
          {isUser ? (
            <p className="whitespace-pre-wrap text-sm leading-relaxed">{message.content}</p>
          ) : (
            <div className={cn(
              'prose prose-sm max-w-none',
              message.isStreaming && 'streaming-text',
              message.isRefusal && 'text-muted-foreground'
            )}>
              <ReactMarkdown>{message.content}</ReactMarkdown>
            </div>
          )}
        </div>

        {/* Citations */}
        {!isUser && message.citations && message.citations.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5">
            {message.citations.map(citation => (
              <button
                key={citation.id}
                className="citation-chip"
                onClick={onShowSources}
                title={`${citation.filename} - Page ${citation.pageNumber || 'N/A'}`}
              >
                [{citation.index}] {citation.filename.slice(0, 15)}
                {citation.filename.length > 15 && '...'}
              </button>
            ))}
          </div>
        )}

        {/* Coverage Badge */}
        {!isUser && message.coverage && (
          <span className={cn(
            'coverage-badge mt-2',
            message.coverage.level === 'high' && 'coverage-high',
            message.coverage.level === 'medium' && 'coverage-medium',
            message.coverage.level === 'low' && 'coverage-low'
          )}>
            {message.coverage.score.toFixed(0)}% confidence
          </span>
        )}

        {/* Action buttons for assistant messages */}
        {!isUser && message.content && !message.isStreaming && (
          <div className="mt-2 flex flex-wrap items-center gap-1">
            {message.sources && message.sources.length > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={onShowSources}
                className="h-8 gap-1 text-xs text-muted-foreground hover:text-foreground"
              >
                <FileText className="h-3.5 w-3.5" />
                Sources
              </Button>
            )}

            <Button
              variant="ghost"
              size="sm"
              onClick={handleCopy}
              className="h-8 gap-1 text-xs text-muted-foreground hover:text-foreground"
            >
              <Copy className="h-3.5 w-3.5" />
              {copied ? 'Copied!' : 'Copy'}
            </Button>

            {!message.isRefusal && (
              <div className="flex items-center gap-0.5">
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => handleFeedback('positive')}
                  className={cn('h-8 w-8 text-muted-foreground hover:text-foreground', feedbackSent === 'positive' && 'text-success')}
                >
                  <ThumbsUp className="h-3.5 w-3.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => handleFeedback('negative')}
                  className={cn('h-8 w-8 text-muted-foreground hover:text-foreground', feedbackSent === 'negative' && 'text-destructive')}
                >
                  <ThumbsDown className="h-3.5 w-3.5" />
                </Button>
              </div>
            )}

            {message.isRefusal && (
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={onWebSearch}
                  className="h-8 gap-1 text-xs"
                >
                  <Search className="h-3.5 w-3.5" />
                  Search Web
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={onRetry}
                  className="h-8 gap-1 text-xs text-muted-foreground hover:text-foreground"
                >
                  <RefreshCw className="h-3.5 w-3.5" />
                  Retry
                </Button>
              </div>
            )}
          </div>
        )}

        {/* Error state */}
        {('error' in message) && (message as any).error && (
          <div className="mt-2 flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4" />
            <span>{message.content || 'An error occurred'}</span>
            <Button variant="outline" size="sm" onClick={onRetry} className="h-8 gap-1 text-xs">
              <RefreshCw className="h-3.5 w-3.5" />
              Retry
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
