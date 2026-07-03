import { useState, useCallback, useEffect } from 'react';
import type { Document, UploadState } from '../types';

export function useDocuments(sessionId: string) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [globalDocuments, setGlobalDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });
  const [retryCount, setRetryCount] = useState(0);

  const fetchDocuments = useCallback(async () => {
    if (!sessionId) {
      // If session is missing, don't hide the loader – we're waiting for it.
      // Only set loading false if we truly have no session and want to show empty state.
      // For now, keep loading true to avoid flashing empty.
      return;
    }

    // Show loader immediately
    setLoading(true);

    try {
      const response = await fetch('/api/documents', {
        headers: { 'x-session-id': sessionId }
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      console.log('[useDocuments] fetchDocuments — session:', data.sessionDocuments?.length, 'global:', data.globalDocuments?.length);

      const sessionDocs = data.sessionDocuments || [];
      const globalDocs = data.globalDocuments || [];
      const hasDocuments = sessionDocs.length > 0 || globalDocs.length > 0;

      // If we got an empty response AND we haven't retried too many times, retry.
      if (!hasDocuments && retryCount < 2) {
        setRetryCount(prev => prev + 1);
        console.log(`[useDocuments] Empty response (cold start?), retrying in 2s... (attempt ${retryCount + 1}/2)`);
        // Keep loading = true – loader stays visible during retry.
        setTimeout(() => {
          fetchDocuments();
        }, 2000);
        return;
      }

      // Either we have data, or we've exhausted retries.
      setDocuments(sessionDocs);
      setGlobalDocuments(globalDocs);
      setRetryCount(0); // Reset retry counter on success
      setLoading(false);

    } catch (error) {
      console.error('[useDocuments] Failed to fetch documents:', error);

      // Retry on network/backend errors too
      if (retryCount < 2) {
        setRetryCount(prev => prev + 1);
        console.log(`[useDocuments] Error, retrying in 3s... (attempt ${retryCount + 1}/2)`);
        setTimeout(() => {
          fetchDocuments();
        }, 3000);
        // Keep loading = true
      } else {
        console.error('[useDocuments] All retries exhausted.');
        setLoading(false);
      }
    }
  }, [sessionId, retryCount]);

  useEffect(() => {
    fetchDocuments();
  }, [fetchDocuments]);

  // ─── the rest of your hook (upload, delete, reset) is unchanged ──────────
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

      console.log('[useDocuments] SSE stream opened, reading events...');

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let currentEvent = '';
      let result: any = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          console.log('[useDocuments] SSE stream closed');
          break;
        }

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
              console.log(`[useDocuments] SSE event: ${currentEvent}`, payload);

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
                  const alreadyExists = prev.some(d => d.document_id === payload.documentId);
                  if (alreadyExists) {
                    console.log(`[useDocuments] Doc ${payload.documentId} already in list — updating status to indexing`);
                    return prev.map(d =>
                      d.document_id === payload.documentId
                        ? { ...d, status: 'indexing' as const }
                        : d
                    );
                  }
                  console.log(`[useDocuments] Doc ${payload.filename} optimistically added as indexing`);
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
                console.log(`[useDocuments] ✅ Upload complete for ${payload.document.filename} — ${payload.document.chunkCount} chunks indexed`);
                setUploadState({ status: 'done', documentId: payload.document.documentId });
                await fetchDocuments();
                setTimeout(() => setUploadState({ status: 'idle' }), 3000);

              } else if (currentEvent === 'error') {
                console.error(`[useDocuments] Server error event:`, payload);
                setUploadState({
                  status: 'error',
                  error: payload.message || 'Upload failed'
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
        error: error.message || 'Upload failed'
      });
      return null;
    }
  }, [sessionId, fetchDocuments]);

  const deleteDocument = useCallback(async (documentId: string, filename: string) => {
    try {
      const response = await fetch(`/api/documents/${documentId}?filename=${encodeURIComponent(filename)}`, {
        method: 'DELETE',
        headers: { 'x-session-id': sessionId }
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      await fetchDocuments();
      return true;
    } catch (error) {
      console.error('[useDocuments] Failed to delete document:', error);
      return false;
    }
  }, [sessionId, fetchDocuments]);

  const resetUploadState = useCallback(() => {
    setUploadState({ status: 'idle' });
  }, []);

  return {
    documents,
    globalDocuments,
    loading,
    uploadState,
    uploadDocument,
    deleteDocument,
    resetUploadState,
    refreshDocuments: fetchDocuments
  };
}