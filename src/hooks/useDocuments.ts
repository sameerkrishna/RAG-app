import { useState, useCallback, useEffect } from 'react';
import type { Document, UploadState } from '../types';

export function useDocuments(sessionId: string) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [globalDocuments, setGlobalDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });

  const fetchDocuments = useCallback(async () => {
    try {
      const response = await fetch('/api/documents', {
        headers: { 'x-session-id': sessionId }
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      setDocuments(data.sessionDocuments || []);
      setGlobalDocuments(data.globalDocuments || []);
    } catch (error) {
      console.error('[useDocuments] Failed to fetch documents:', error);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    fetchDocuments();
  }, [fetchDocuments]);

  const uploadDocument = useCallback(async (file: File) => {
    console.log(`[useDocuments] Starting upload for ${file.name} (${file.size} bytes)`);
    setUploadState({ status: 'uploading' });

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
        setUploadState({ status: 'error', error: 'Upload failed', code: 'HTTP_ERROR' });
        return null;
      }

      console.log('[useDocuments] SSE stream opened, reading events...');

      const reader  = response.body.getReader();
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
                // Phase 1 done — show doc immediately in list as 'indexing'
                setUploadState({
                  status: 'upload_complete',
                  documentId: payload.documentId,
                  totalChunks: payload.totalChunks,
                  totalSets: payload.totalSets
                });

                // Optimistically add doc to local list with status 'indexing'
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
                setDocuments(prev => [newDoc, ...prev]);
                console.log(`[useDocuments] Doc ${payload.filename} added to list as indexing`);

              } else if (currentEvent === 'embedding_progress') {
                setUploadState({
                  status: 'indexing',
                  processedChunks: payload.processedChunks,
                  totalChunks: payload.totalChunks,
                  setIndex: payload.setIndex,
                  totalSets: payload.totalSets
                });

              } else if (currentEvent === 'done') {
                result = payload;
                // Update the doc in the list: set final chunk count + status 'ready'
                setDocuments(prev => prev.map(d =>
                  d.document_id === payload.document.id
                    ? { ...d, chunk_count: payload.document.chunkCount, status: 'ready' }
                    : d
                ));
                setUploadState({ status: 'complete' });
                console.log(`[useDocuments] ✅ Upload complete for ${payload.document.filename} — ${payload.document.chunkCount} chunks indexed`);

                setTimeout(() => setUploadState({ status: 'idle' }), 3000);

              } else if (currentEvent === 'error') {
                console.error(`[useDocuments] Server error event:`, payload);
                setUploadState({
                  status: 'error',
                  error: payload.message || 'Upload failed',
                  code: payload.code || 'UNKNOWN'
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
        code: 'NETWORK_ERROR'
      });
      return null;
    }
  }, [sessionId]);

  const deleteDocument = useCallback(async (documentId: string) => {
    try {
      const response = await fetch(`/api/documents/${documentId}`, {
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
