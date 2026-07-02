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
      console.log('[useDocuments] fetchDocuments — session:', data.sessionDocuments?.length, 'global:', data.globalDocuments?.length);
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
    setUploadState({ status: 'uploading', uploadProgress: 0 } as any);

    const formData = new FormData();
    formData.append('file', file);
    formData.append('sessionId', sessionId);

    return new Promise<any>((resolve) => {
      const xhr = new XMLHttpRequest();
let lastIndex = 0;
let currentEvent = '';
let currentDataLines: string[] = [];
let resolved = false;

const safeResolve = (value: any) => {
  if (resolved) return;
  resolved = true;
  resolve(value);
};
const handleSseEvent = async (eventName: string, rawData: string) => {
  if (!rawData) return;

  try {
    const payload = JSON.parse(rawData);
    console.log(`[useDocuments] SSE event: ${eventName}`, payload);

    if (eventName === 'upload_complete') {
      setUploadState({
        status: 'upload_complete',
        uploadProgress: 100,
        documentId: payload.documentId,
        totalChunks: payload.totalChunks,
        totalSets: payload.totalSets
      } as any);

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
          return prev.map(d =>
            d.document_id === payload.documentId
              ? { ...d, status: 'indexing' as const }
              : d
          );
        }
        console.log(`[useDocuments] Doc ${payload.filename} optimistically added as indexing`);
        return [newDoc, ...prev];
      });

      return;
    }

    if (eventName === 'embedding_progress') {
      setUploadState({
        status: 'indexing',
        uploadProgress: 100,
        processedChunks: payload.processedChunks,
        totalChunks: payload.totalChunks,
        setIndex: payload.setIndex,
        totalSets: payload.totalSets
      } as any);
      return;
    }

    if (eventName === 'done') {
      console.log(`[useDocuments] ✅ Upload complete for ${payload.document?.filename ?? payload.filename}`);
      setUploadState({ status: 'complete' } as any);
      await fetchDocuments();
      setTimeout(() => setUploadState({ status: 'idle' } as any), 3000);
      safeResolve(payload);
      return;
    }

    if (eventName === 'error') {
      console.error('[useDocuments] Server error event:', payload);
      setUploadState({
        status: 'error',
        error: payload.message || 'Upload failed',
        code: payload.code || 'UNKNOWN'
      } as any);
      safeResolve(null);
    }
  } catch (e) {
    console.warn('[useDocuments] Failed to parse SSE data:', rawData);
  }
};

const flushCurrentEvent = async () => {
  if (!currentDataLines.length) {
    currentEvent = '';
    return;
  }

  const rawData = currentDataLines.join('\n');
  const eventName = currentEvent;
  currentEvent = '';
  currentDataLines = [];
  await handleSseEvent(eventName, rawData);
};

const parseIncrementalResponse = async () => {
  const nextChunk = xhr.responseText.slice(lastIndex);
  if (!nextChunk) return;

  lastIndex = xhr.responseText.length;
  const lines = nextChunk.split(/\r?\n/);

  for (const line of lines) {
    if (line.startsWith('event: ')) {
      currentEvent = line.slice(7).trim();
      continue;
    }

    if (line.startsWith('data: ')) {
      currentDataLines.push(line.slice(6));
      continue;
    }

    if (line.trim() === '') {
      await flushCurrentEvent();
    }
  }
};
      
      // Phase 1: track real upload bytes sent to server
      xhr.upload.addEventListener('progress', (e) => {
        if (e.lengthComputable) {
          const pct = Math.round((e.loaded / e.total) * 100);
          setUploadState({ status: 'uploading', uploadProgress: pct } as any);
        }
      });

      xhr.addEventListener('load', async () => {
        if (xhr.status < 200 || xhr.status >= 300) {
          console.error('[useDocuments] Upload failed:', xhr.status, xhr.responseText);
          setUploadState({ status: 'error', error: 'Upload failed', code: 'HTTP_ERROR' } as any);
          resolve(null);
          return;
        }

        // Phase 2: parse SSE events from the buffered XHR response
        // XHR buffers the full SSE text in responseText after the stream ends.
        console.log('[useDocuments] XHR done — parsing SSE events from responseText');
        setUploadState({ status: 'uploading', uploadProgress: 100 } as any);

        const lines = xhr.responseText.split('\n');
        let currentEvent = '';
        let result: any = null;

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
                  totalSets: payload.totalSets
                } as any);

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
                setUploadState({
                  status: 'indexing',
                  processedChunks: payload.processedChunks,
                  totalChunks: payload.totalChunks,
                  setIndex: payload.setIndex,
                  totalSets: payload.totalSets
                } as any);

              } else if (currentEvent === 'done') {
                result = payload;
                console.log(`[useDocuments] ✅ Upload complete for ${payload.document.filename}`);
                setUploadState({ status: 'complete' } as any);
                await fetchDocuments();
                setTimeout(() => setUploadState({ status: 'idle' } as any), 3000);

              } else if (currentEvent === 'error') {
                console.error('[useDocuments] Server error event:', payload);
                setUploadState({
                  status: 'error',
                  error: payload.message || 'Upload failed',
                  code: payload.code || 'UNKNOWN'
                } as any);
              }

              currentEvent = '';
            } catch (e) {
              console.warn('[useDocuments] Failed to parse SSE data:', rawData);
            }
          }
        }

        resolve(result);
      });

      xhr.addEventListener('error', () => {
        console.error('[useDocuments] XHR network error');
        setUploadState({ status: 'error', error: 'Network error', code: 'NETWORK_ERROR' } as any);
        resolve(null);
      });

      xhr.open('POST', '/api/documents/upload');
      xhr.setRequestHeader('x-session-id', sessionId);
      xhr.send(formData);
    });
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
    setUploadState({ status: 'idle' } as any);
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