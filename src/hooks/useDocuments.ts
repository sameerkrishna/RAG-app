import { useState, useCallback, useEffect, useRef } from 'react';
import type { Document, UploadState } from '../types';

export function useDocuments(sessionId: string) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [globalDocuments, setGlobalDocuments] = useState<Document[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });
  const initDoneRef = useRef(false);

  const fetchDocuments = useCallback(async () => {
    try {
      const response = await fetch('/api/documents', {
        headers: { 'x-session-id': sessionId }
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();
      setDocuments(data.sessionDocuments || []);
      setGlobalDocuments(data.globalDocuments || []);
    } catch (error) {
      console.error('Failed to fetch documents:', error);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    let cancelled = false;

    async function initThenFetch() {
      // Only call session init once per sessionId mount
      if (!initDoneRef.current) {
        initDoneRef.current = true;
        await fetch('/api/session/init', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-session-id': sessionId }
        }).catch(err => console.warn('Session init failed (non-fatal):', err.message));
      }
      if (!cancelled) await fetchDocuments();
    }

    initThenFetch();
    return () => { cancelled = true; };
  }, [sessionId]);

  const uploadDocument = useCallback(async (file: File) => {
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

      const data = await response.json();

      if (!response.ok) {
        setUploadState({
          status: 'error',
          error: data.error || 'Upload failed',
          code: data.code || 'UNKNOWN'
        });
        return null;
      }

      setUploadState({ status: 'complete' });
      await fetchDocuments();

      // Reset after a delay
      setTimeout(() => {
        setUploadState({ status: 'idle' });
      }, 2000);

      return data;

    } catch (error: any) {
      setUploadState({
        status: 'error',
        error: error.message || 'Upload failed',
        code: 'NETWORK_ERROR'
      });
      return null;
    }
  }, [sessionId, fetchDocuments]);

  const deleteDocument = useCallback(async (documentId: string) => {
    try {
      const response = await fetch(`/api/documents/${documentId}`, {
        method: 'DELETE',
        headers: { 'x-session-id': sessionId }
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      await fetchDocuments();
      return true;
    } catch (error) {
      console.error('Failed to delete document:', error);
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
