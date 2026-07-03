import { useState, useCallback, useEffect } from 'react';
import type { Document, UploadState } from '../types';

export function useDocuments(sessionId: string) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [globalDocuments, setGlobalDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });

  // ─── Simple one‑shot fetch (no polling/retries) ────────────────────
  const fetchDocuments = useCallback(async () => {
    if (!sessionId) {
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const response = await fetch('/api/documents', {
        headers: { 'x-session-id': sessionId },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();

      setDocuments(data.sessionDocuments || []);
      setGlobalDocuments(data.globalDocuments || []);
    } catch (error) {
      console.error('[useDocuments] Failed to fetch documents:', error);
      // keep whatever we already have (don’t clear lists)
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  // Fetch on mount / when sessionId changes
  useEffect(() => {
    fetchDocuments();
  }, [fetchDocuments]);

  // ─── Upload (SSE stream, unchanged logic except no re‑fetch) ───────
  const uploadDocument = useCallback(async (file: File) => {
    console.log(`[useDocuments] Starting upload for ${file.name} (${file.size} bytes)`);
    setUploadState({ status: 'uploading', uploadProgress: 0 });

    const formData = new FormData();
    formData.append('file', file);
    formData.append('sessionId', sessionId);

    try {
      const response = await fetch('/api/documents/upload', {
        method: 'POST',
        headers: { 'x-session-id': sessionId },
        body: formData,
      });

      if (!response.ok || !response.body) {
        const text = await response.text();
        console.error('[useDocuments] Upload request failed:', response.status, text);
        setUploadState({ status: 'error', error: 'Upload failed' });
        return null;
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let currentEvent = '';
      let result: any = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEvent = line.slice(7).trim();
            continue;
          }
          if (line.startsWith('data: ')) {
            const rawData = line.slice(6);
            try {
              const payload = JSON.parse(rawData);

              if (currentEvent === 'upload_complete') {
                setUploadState({
                  status: 'upload_complete',
                  documentId: payload.documentId,
                  totalChunks: payload.totalChunks,
                  totalSets: payload.totalSets,
                  uploadProgress: 100,
                });
                const newDoc: Document = {
                  document_id: payload.documentId,
                  filename: payload.filename,
                  chunk_count: 0,
                  page_count: payload.pageCount,
                  upload_timestamp: new Date().toISOString(),
                  source_type: 'session_upload',
                  fileSize: payload.fileSize,
                  status: 'indexing',
                };
                setDocuments(prev => {
                  if (prev.some(d => d.document_id === payload.documentId)) {
                    return prev.map(d =>
                      d.document_id === payload.documentId
                        ? { ...d, status: 'indexing' as const }
                        : d
                    );
                  }
                  return [newDoc, ...prev];
                });
              } else if (currentEvent === 'embedding_progress') {
                const indexingProgress = Math.round(
                  (payload.processedChunks / payload.totalChunks) * 100
                );
                setUploadState({
                  status: 'indexing',
                  processedChunks: payload.processedChunks,
                  totalChunks: payload.totalChunks,
                  setIndex: payload.setIndex,
                  totalSets: payload.totalSets,
                  indexingProgress,
                });
              } else if (currentEvent === 'done') {
                result = payload;
                setUploadState({
                  status: 'done',
                  documentId: payload.document.documentId,
                });
                setDocuments(prev =>
                  prev.map(d =>
                    d.document_id === payload.document.documentId
                      ? {
                          ...d,
                          chunk_count: payload.document.chunkCount,
                          status: 'ready' as const,
                        }
                      : d
                  )
                );
                // ✅ No longer calling fetchDocuments() here – avoids unnecessary request.
                // The local state already shows the document as ready.
                // Seed documents can be refreshed manually via refreshDocuments().
                setTimeout(() => setUploadState({ status: 'idle' }), 3000);
              } else if (currentEvent === 'error') {
                console.error('[useDocuments] Server error:', payload);
                setUploadState({
                  status: 'error',
                  error: payload.message || 'Upload failed',
                });
              }
              currentEvent = '';
            } catch (e) {
              console.warn('[useDocuments] Failed to parse SSE data:', rawData);
            }
          }
        }
      }
      return result;
    } catch (error: any) {
      console.error('[useDocuments] Upload fetch error:', error);
      setUploadState({
        status: 'error',
        error: error.message || 'Upload failed',
      });
      return null;
    }
  }, [sessionId]); // note: fetchDocuments is no longer a dependency since we removed its call

  const deleteDocument = useCallback(
    async (documentId: string, filename: string) => {
      try {
        const response = await fetch(
          `/api/documents/${documentId}?filename=${encodeURIComponent(filename)}`,
          {
            method: 'DELETE',
            headers: { 'x-session-id': sessionId },
          }
        );
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        // Remove from local list immediately for a snappy UI
        setDocuments(prev => prev.filter(d => d.document_id !== documentId));
        return true;
      } catch (error) {
        console.error('[useDocuments] Failed to delete document:', error);
        return false;
      }
    },
    [sessionId]
  );

  const resetUploadState = useCallback(() => {
    setUploadState({ status: 'idle' });
  }, []);

  const refreshDocuments = useCallback(() => {
    // manual one‑shot refresh (useful after seed documents are added externally)
    fetchDocuments();
  }, [fetchDocuments]);

  return {
    documents,
    globalDocuments,
    loading,
    uploadState,
    uploadDocument,
    deleteDocument,
    resetUploadState,
    refreshDocuments,
  };
}