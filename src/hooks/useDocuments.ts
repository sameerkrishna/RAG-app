import { useState, useCallback, useEffect, useRef } from 'react';
import type { Document, UploadState } from '../types';

export function useDocuments(sessionId: string) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [globalDocuments, setGlobalDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });

  const eventSourceRef = useRef<EventSource | null>(null);
  const sseConnectedRef = useRef(false);
  const isFetchingRef = useRef(false);

  // ─── Cleanup SSE on unmount ────────────────────────────────────────────────
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, []);

  // ─── Main fetch function ────────────────────────────────────────────────────
  const fetchDocuments = useCallback(async () => {
    // Prevent multiple simultaneous fetches
    if (isFetchingRef.current) return;
    isFetchingRef.current = true;

    if (!sessionId) {
      setLoading(false);
      isFetchingRef.current = false;
      return;
    }

    // Show loader
    setLoading(true);

    // Close any existing SSE connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    sseConnectedRef.current = false;

    try {
      // First, try direct fetch (data might already be ready)
      const response = await fetch('/api/documents', {
        headers: { 'x-session-id': sessionId }
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();

      const sessionDocs = data.sessionDocuments || [];
      const globalDocs = data.globalDocuments || [];
      const hasData = sessionDocs.length > 0 || globalDocs.length > 0;

      if (hasData) {
        // ✅ Data ready – update and stop
        setDocuments(sessionDocs);
        setGlobalDocuments(globalDocs);
        setLoading(false);
        isFetchingRef.current = false;
        console.log('[useDocuments] Data ready, loaded directly.');
        return;
      }

      // ❌ No data – connect to SSE to wait for seeding
      console.log('[useDocuments] No data, connecting to SSE...');
      const url = `/api/documents/seeding-status?sessionId=${sessionId}`;
      eventSourceRef.current = new EventSource(url);
      sseConnectedRef.current = true;

      // ─── Listen for seeding_complete event ──────────────────────────────
      eventSourceRef.current.addEventListener('seeding_complete', (event: any) => {
        try {
          const data = JSON.parse(event.data);
          console.log('[useDocuments] ✅ Seeding complete!', data);
          // Close SSE connection
          if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
            sseConnectedRef.current = false;
          }
          // Re-fetch documents (reset fetching flag so it can run again)
          isFetchingRef.current = false;
          fetchDocuments();
        } catch (error) {
          console.error('[useDocuments] Failed to parse SSE event:', error);
        }
      });

      // ─── Listen for error events ─────────────────────────────────────────
      eventSourceRef.current.addEventListener('error', (event: any) => {
        console.error('[useDocuments] SSE error:', event);
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
          sseConnectedRef.current = false;
        }
        // Fallback: try direct fetch after 3 seconds
        setTimeout(() => {
          if (loading) {
            console.log('[useDocuments] SSE failed, trying direct fetch...');
            isFetchingRef.current = false;
            fetchDocuments();
          }
        }, 3000);
      });

      // ─── Listen for open event ───────────────────────────────────────────
      eventSourceRef.current.onopen = () => {
        console.log('[useDocuments] SSE connection opened');
      };

      // Safety timeout: if SSE doesn't respond in 30 seconds, try direct fetch again
      setTimeout(() => {
        if (loading && sseConnectedRef.current) {
          console.log('[useDocuments] SSE timeout, trying direct fetch...');
          if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
            sseConnectedRef.current = false;
          }
          isFetchingRef.current = false;
          fetchDocuments();
        }
      }, 30000);

      isFetchingRef.current = false;

    } catch (error) {
      console.error('[useDocuments] Fetch error:', error);

      // Try SSE as fallback if not already connected
      if (!sseConnectedRef.current) {
        console.log('[useDocuments] Direct fetch failed, trying SSE...');
        const url = `/api/documents/seeding-status?sessionId=${sessionId}`;
        eventSourceRef.current = new EventSource(url);
        sseConnectedRef.current = true;

        eventSourceRef.current.addEventListener('seeding_complete', (event: any) => {
          try {
            const data = JSON.parse(event.data);
            console.log('[useDocuments] ✅ Seeding complete!', data);
            if (eventSourceRef.current) {
              eventSourceRef.current.close();
              eventSourceRef.current = null;
              sseConnectedRef.current = false;
            }
            isFetchingRef.current = false;
            fetchDocuments();
          } catch (error) {
            console.error('[useDocuments] Failed to parse SSE event:', error);
          }
        });

        eventSourceRef.current.addEventListener('error', () => {
          console.error('[useDocuments] SSE error fallback');
          if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
            sseConnectedRef.current = false;
          }
          setLoading(false);
          isFetchingRef.current = false;
        });

        eventSourceRef.current.onopen = () => {
          console.log('[useDocuments] SSE connection opened (fallback)');
        };
      } else {
        // If SSE already connected but we got an error, just show empty
        setLoading(false);
        isFetchingRef.current = false;
      }
    }
  }, [sessionId, loading]);

  // ─── Initial fetch ──────────────────────────────────────────────────────────
  useEffect(() => {
    fetchDocuments();
  }, [sessionId, fetchDocuments]);

  // ─── Upload document ────────────────────────────────────────────────────────
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
        body: formData
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
                  uploadProgress: 100
                });
                const newDoc: Document = {
                  document_id: payload.documentId,
                  filename: payload.filename,
                  chunk_count: 0,
                  page_count: payload.pageCount,
                  upload_timestamp: new Date().toISOString(),
                  source_type: 'session_upload',
                  fileSize: payload.fileSize,
                  status: 'indexing'
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
                const indexingProgress = Math.round((payload.processedChunks / payload.totalChunks) * 100);
                setUploadState({
                  status: 'indexing',
                  processedChunks: payload.processedChunks,
                  totalChunks: payload.totalChunks,
                  setIndex: payload.setIndex,
                  totalSets: payload.totalSets,
                  indexingProgress
                });
              } else if (currentEvent === 'done') {
                result = payload;
                setUploadState({ status: 'done', documentId: payload.document.documentId });
                setDocuments(prev =>
                  prev.map(d =>
                    d.document_id === payload.document.documentId
                      ? { ...d, chunk_count: payload.document.chunkCount, status: 'ready' as const }
                      : d
                  )
                );
                // Refresh the list after upload
                // Reset fetching flag so it can run
                isFetchingRef.current = false;
                fetchDocuments();
                setTimeout(() => setUploadState({ status: 'idle' }), 3000);
              } else if (currentEvent === 'error') {
                console.error('[useDocuments] Server error:', payload);
                setUploadState({ status: 'error', error: payload.message || 'Upload failed' });
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
      setUploadState({ status: 'error', error: error.message || 'Upload failed' });
      return null;
    }
  }, [sessionId, fetchDocuments]);

  // ─── Delete document ────────────────────────────────────────────────────────
  const deleteDocument = useCallback(async (documentId: string, filename: string) => {
    try {
      const response = await fetch(`/api/documents/${documentId}?filename=${encodeURIComponent(filename)}`, {
        method: 'DELETE',
        headers: { 'x-session-id': sessionId }
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      setDocuments(prev => prev.filter(d => d.document_id !== documentId));
      return true;
    } catch (error) {
      console.error('[useDocuments] Failed to delete document:', error);
      return false;
    }
  }, [sessionId]);

  // ─── Reset upload state ────────────────────────────────────────────────────
  const resetUploadState = useCallback(() => {
    setUploadState({ status: 'idle' });
  }, []);

  // ─── Manual refresh ────────────────────────────────────────────────────────
  const refreshDocuments = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
      sseConnectedRef.current = false;
    }
    isFetchingRef.current = false;
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
    refreshDocuments
  };
}