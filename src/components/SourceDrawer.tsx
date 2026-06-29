import { X, Copy, ExternalLink } from 'lucide-react';
import { Button } from './ui/Button';
import type { SearchResult } from '../types';
import { useState } from 'react';
import { cn } from '../lib/utils';

interface SourceDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  sources: SearchResult[];
}

export default function SourceDrawer({ isOpen, onClose, sources }: SourceDrawerProps) {
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const handleCopy = async (text: string, id: string) => {
    await navigator.clipboard.writeText(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  return (
    <div className={cn(
      'source-drawer',
      isOpen ? 'z-50 open' : 'closed'
    )}>
      <div className="h-full flex flex-col">
        <div className="p-4 border-b flex items-center justify-between">
          <h2 className="font-semibold">Sources</h2>
          <Button variant="ghost" size="sm" onClick={onClose}>
            <X className="w-4 h-4" />
          </Button>
        </div>

        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {sources.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <p>No sources available</p>
            </div>
          ) : (
            sources.map((source, index) => (
              <div
                key={source.chunkId || index}
                className="border rounded-lg p-4 space-y-2"
              >
                <div className="flex items-start justify-between">
                  <div>
                    <h3 className="font-medium text-sm">{source.filename}</h3>
                    <p className="text-xs text-muted-foreground">
                      Page {source.pageNumber || 'N/A'}
                    </p>
                  </div>
                  <span className={cn(
                    'text-xs px-2 py-0.5 rounded',
                    source.sourceType === 'global' ? 'bg-primary/10 text-primary' : 'bg-secondary'
                  )}>
                    {source.sourceType === 'global' ? 'Seed' : 'Session'}
                  </span>
                </div>

                <p className="text-sm text-muted-foreground line-clamp-4">
                  {source.excerpt}
                </p>

                <div className="flex items-center gap-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleCopy(source.excerpt, source.chunkId)}
                  >
                    <Copy className="w-3 h-3 mr-1" />
                    {copiedId === source.chunkId ? 'Copied!' : 'Copy'}
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => window.open(`/api/documents/${source.documentId}/file`, '_blank')}
                  >
                    <ExternalLink className="w-3 h-3 mr-1" />
                    View PDF
                  </Button>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
