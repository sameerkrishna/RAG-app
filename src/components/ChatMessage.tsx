import ReactMarkdown from 'react-markdown';
import { Copy, ThumbsUp, ThumbsDown, FileText, AlertCircle, RefreshCw } from 'lucide-react';
import type { ChatMessage as ChatMessageType } from '../types';
import { cn } from '../lib/utils';
import { useState } from 'react';
import { Button } from './ui/Button';

interface ChatMessageProps {
  message: ChatMessageType;
  onShowSources: () => void;
  onRetry: () => void;
  onFeedback?: (messageId: string, type: 'like' | 'dislike') => void;
}

export default function ChatMessage({ message, onShowSources, onRetry, onFeedback }: ChatMessageProps) {
  const [copied, setCopied] = useState(false);
  const [feedbackSent, setFeedbackSent] = useState<'like' | 'dislike' | null>(null);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(message.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleFeedback = async (type: 'like' | 'dislike') => {
    if (feedbackSent) return;
    setFeedbackSent(type);
    if (onFeedback) {
      onFeedback(message.id, type);
    }
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
        {!isUser && message.coverage && message.coverage.confidence > 0 && message.citations && message.citations.length > 0 && (
          <span className="mt-2 text-xs text-muted-foreground px-2 py-0.5 rounded-full bg-muted">
           Relevance Match: {message.coverage.confidence}% 
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
                  disabled={feedbackSent !== null}
                  onClick={() => handleFeedback('like')}
                  className={cn(
                    'h-8 w-8 transition-colors',
                    feedbackSent === 'like'
                      ? 'text-green-500 bg-green-500/10 hover:bg-green-500/10 hover:text-green-500'
                      : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <ThumbsUp className={cn('h-3.5 w-3.5', feedbackSent === 'like' && 'fill-green-500')} />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  disabled={feedbackSent !== null}
                  onClick={() => handleFeedback('dislike')}
                  className={cn(
                    'h-8 w-8 transition-colors',
                    feedbackSent === 'dislike'
                      ? 'text-red-500 bg-red-500/10 hover:bg-red-500/10 hover:text-red-500'
                      : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <ThumbsDown className={cn('h-3.5 w-3.5', feedbackSent === 'dislike' && 'fill-red-500')} />
                </Button>
              </div>
            )}

            {message.isRefusal && (
              <Button
                variant="ghost"
                size="sm"
                onClick={onRetry}
                className="h-8 gap-1 text-xs text-muted-foreground hover:text-foreground"
              >
                <RefreshCw className="h-3.5 w-3.5" />
                Retry
              </Button>
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
