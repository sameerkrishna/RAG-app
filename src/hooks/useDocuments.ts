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
  const dataLoadedRef = useRef(false);
  const mountedRef = useRef(true);
  const initDoneRef = useRef(false);
  const timeoutIdRef = useRef<number | null>(null);

  // ─── Cleanup on unmount ──────────────────────────────────────────────────────
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      if (timeoutIdRef.current) {
        clearTimeout(timeoutIdRef.current);
        timeoutIdRef.current = null;
      }
    };
  }, []);

  // ─── Update documents safely ──────────────────────────────────────────────
  const updateDocuments = useCallback((sessionDocs: Document[], globalDocs: Document[]) => {
    if (!mountedRef.current) return;
    setDocuments(sessionDocs);
    setGlobalDocuments(globalDocs);
    setLoading(false);
    dataLoadedRef.current = true;
  }, []);

  // ─── Close SSE connection ─────────────────────────────────────────────────
  const closeSSE = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    sseConnectedRef.current = false;
    if (timeoutIdRef.current) {
      clearTimeout(timeoutIdRef.current);
      timeoutIdRef.current = null;
    }
  }, []);

  // ─── Main fetch function ────────────────────────────────────────────────────
  const fetchDocuments = useCallback(async (isInitial = false) => {
    // Prevent multiple simultaneous fetches
    if (isFetchingRef.current) return;
    
    // If data is already loaded and this is not a forced refresh, skip
    if (dataLoadedRef.current && !isInitial) {
      console.log('[useDocuments] Data already loaded, skipping fetch.');
      return;
    }

    if (!sessionId || !mountedRef.current) {
      if (mountedRef.current) setLoading(false);
      return;
    }

    isFetchingRef.current = true;

    // Only show loader on initial load or when explicitly refreshing
    if (isInitial || !dataLoadedRef.current) {
      setLoading(true);
    }

    // Close any existing SSE connection
    closeSSE();

    try {
      // Try direct fetch first
      const response = await fetch('/api/documents', {
        headers: { 'x-session-id': sessionId }
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();

      const sessionDocs = data.sessionDocuments || [];
      const globalDocs = data.globalDocuments || [];
      const hasData = sessionDocs.length > 0 || globalDocs.length > 0;

      if (hasData && mountedRef.current) {
        // ✅ Data ready – update and stop
        updateDocuments(sessionDocs, globalDocs);
        isFetchingRef.current = false;
        console.log('[useDocuments] Data ready, loaded directly.');
        return;
      }

      // ❌ No data – connect to SSE to wait for seeding
      if (mountedRef.current) {
        console.log('[useDocuments] No data, connecting to SSE...');
        const url = `/api/documents/seeding-status?sessionId=${sessionId}`;
        eventSourceRef.current = new EventSource(url);
        sseConnectedRef.current = true;

        // ─── Listen for seeding_complete event ──────────────────────────────
        const onSeedingComplete = (event: any) => {
          if (!mountedRef.current) return;
          try {
            const data = JSON.parse(event.data);
            console.log('[useDocuments] ✅ Seeding complete!', data);
            closeSSE();
            // Re-fetch documents (but don't show loader again)
            isFetchingRef.current = false;
            fetchDocuments(false);
          } catch (error) {
            console.error('[useDocuments] Failed to parse SSE event:', error);
          }
        };

        // ─── Listen for error events ─────────────────────────────────────────
        const onError = (event: any) => {
          if (!mountedRef.current) return;
          console.error('[useDocuments] SSE error:', event);
          closeSSE();
          // Fallback: try direct fetch after 2 seconds
          setTimeout(() => {
            if (mountedRef.current && !dataLoadedRef.current) {
              console.log('[useDocuments] SSE failed, trying direct fetch...');
              isFetchingRef.current = false;
              fetchDocuments(false);
            }
          }, 2000);
        };

        // ─── Listen for open event ───────────────────────────────────────────
        const onOpen = () => {
          if (mountedRef.current) {
            console.log('[useDocuments] SSE connection opened');
          }
        };

        eventSourceRef.current.addEventListener('seeding_complete', onSeedingComplete);
        eventSourceRef.current.addEventListener('error', onError);
        eventSourceRef.current.onopen = onOpen;

        // Safety timeout: if SSE doesn't respond in 20 seconds, try direct fetch
        timeoutIdRef.current = setTimeout(() => {
          if (mountedRef.current && !dataLoadedRef.current && sseConnectedRef.current) {
            console.log('[useDocuments] SSE timeout, trying direct fetch...');
            closeSSE();
            isFetchingRef.current = false;
            fetchDocuments(false);
          }
        }, 20000) as unknown as number;
      }

      isFetchingRef.current = false;

    } catch (error) {
      console.error('[useDocuments] Fetch error:', error);
      
      if (mountedRef.current && !dataLoadedRef.current) {
        // Try SSE as fallback if not already connected
        if (!sseConnectedRef.current) {
          console.log('[useDocuments] Direct fetch failed, trying SSE...');
          const url = `/api/documents/seeding-status?sessionId=${sessionId}`;
          eventSourceRef.current = new EventSource(url);
          sseConnectedRef.current = true;

          eventSourceRef.current.addEventListener('seeding_complete', (event: any) => {
            if (!mountedRef.current) return;
            try {
              const data = JSON.parse(event.data);
              console.log('[useDocuments] ✅ Seeding complete!', data);
              closeSSE();
              isFetchingRef.current = false;
              fetchDocuments(false);
            } catch (error) {
              console.error('[useDocuments] Failed to parse SSE event:', error);
            }
          });

          eventSourceRef.current.addEventListener('error', () => {
            if (!mountedRef.current) return;
            console.error('[useDocuments] SSE error fallback');
            closeSSE();
            if (!dataLoadedRef.current) {
              setLoading(false);
            }
            isFetchingRef.current = false;
          });

          eventSourceRef.current.onopen = () => {
            if (mountedRef.current) {
              console.log('[useDocuments] SSE connection opened (fallback)');
            }
          };

          timeoutIdRef.current = setTimeout(() => {
            if (mountedRef.current && !dataLoadedRef.current && sseConnectedRef.current) {
              console.log('[useDocuments] SSE timeout, giving up.');
              closeSSE();
              setLoading(false);
              isFetchingRef.current = false;
            }
          }, 20000) as unknown as number;
        } else {
          // If SSE already connected but we got an error, just show empty
          if (!dataLoadedRef.current) {
            setLoading(false);
          }
          isFetchingRef.current = false;
        }
      } else {
        isFetchingRef.current = false;
      }
    }
  }, [sessionId, closeSSE, updateDocuments]);

  // ─── Initial fetch ──────────────────────────────────────────────────────────
  useEffect(() => {
    // Only run once per sessionId
    if (initDoneRef.current && sessionId) return;
    initDoneRef.current = true;
    dataLoadedRef.current = false;
    fetchDocuments(true);
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
                // Refresh the list after upload (force re-fetch)
                dataLoadedRef.current = false;
                isFetchingRef.current = false;
                fetchDocuments(true);
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
    closeSSE();
    dataLoadedRef.current = false;
    isFetchingRef.current = false;
    fetchDocuments(true);
  }, [closeSSE, fetchDocuments]);

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