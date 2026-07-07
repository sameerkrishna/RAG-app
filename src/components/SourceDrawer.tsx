import { X, Copy, ExternalLink } from 'lucide-react';
import { Button } from './ui/Button';
import type { SearchResult, Citation } from '../types';
import { useState } from 'react';
import { cn } from '../lib/utils';

interface SourceDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  sources: SearchResult[];
  citations?: Citation[];
  sessionId?: string;
}

export default function SourceDrawer({ onClose, sources, citations = [], sessionId }: SourceDrawerProps) {
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const handleCopy = async (text: string, id: string) => {
    await navigator.clipboard.writeText(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const handleViewPdf = (source: SearchResult) => {
    let url = `/api/documents/${source.documentId}/file?filename=${encodeURIComponent(source.filename)}`;
    if (sessionId) {
      url += `&sessionId=${sessionId}`;
    }
    const pageUrl = source.pageNumber ? `${url}#page=${source.pageNumber}` : url;
    window.open(pageUrl, '_blank');
  };

  const citationIndexMap = new Map<string, number>();
  citations.forEach(c => {
    if (c.chunkId) citationIndexMap.set(c.chunkId, c.index);
  });

  return (
    <div className="flex flex-col w-[320px] flex-shrink-0 border-l bg-background overflow-hidden">
      <div className="p-4 border-b flex items-center justify-between">
        <h2 className="font-semibold text-sm">Sources</h2>
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
          sources.map((source, index) => {
            const citationNum = citationIndexMap.get(source.chunkId);
            return (
              <div
                key={source.chunkId || index}
                className="border rounded-lg p-4 space-y-2"
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-2">
                    {citationNum !== undefined && (
                      <span className="flex-shrink-0 inline-flex items-center justify-center w-6 h-6 rounded-full bg-primary text-primary-foreground text-xs font-bold">
                        {citationNum}
                      </span>
                    )}
                    <div>
                      <h3 className="font-medium text-sm leading-tight">{source.filename}</h3>
                      <p className="text-xs text-muted-foreground">Page {source.pageNumber || 'N/A'}</p>
                    </div>
                  </div>
                  <span className={cn(
                    'text-xs px-2 py-0.5 rounded flex-shrink-0',
                    source.sourceType === 'global' ? 'bg-primary/10 text-primary' : 'bg-secondary'
                  )}>
                    {source.sourceType === 'global' ? 'Seed' : 'Session'}
                  </span>
                </div>

                {source.excerpt && (
                  <blockquote className="border-l-2 border-muted pl-3 text-xs text-muted-foreground italic line-clamp-3">
                    "{source.excerpt}"
                  </blockquote>
                )}

                <div className="flex items-center gap-2">
                  <Button variant="ghost" size="sm" onClick={() => handleCopy(source.excerpt, source.chunkId)}>
                    <Copy className="w-3 h-3 mr-1" />
                    {copiedId === source.chunkId ? 'Copied!' : 'Copy'}
                  </Button>
                  <Button variant="ghost" size="sm" onClick={() => handleViewPdf(source)}>
                    <ExternalLink className="w-3 h-3 mr-1" />
                    View PDF
                  </Button>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
