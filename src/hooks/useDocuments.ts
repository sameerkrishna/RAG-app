import { useCallback, useMemo, useState } from 'react';
import type {
  DocumentRecord,
  DocumentsResponse,
  UploadProgressSnapshot,
  UploadState,
} from '../types';

type UploadOptions = {
  onSuccess?: () => void;
};

type SsePayload =
  | {
      type: 'upload_complete';
      documentId: string;
      totalChunks: number;
      totalSets: number;
    }
  | {
      type: 'embedding_progress';
      processedChunks: number;
      totalChunks: number;
      setIndex: number;
      totalSets: number;
      documentId?: string;
    }
  | {
      type: 'done';
      documentId: string;
      totalChunks?: number;
    }
  | {
      type: 'error';
      error: string;
    };

function clampProgress(value: number) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

function tryParseJson<T>(value: string): T | null {
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

export function useDocuments() {
  const [documents, setDocuments] = useState<DocumentRecord[]>([]);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });
  const [isFetching, setIsFetching] = useState(false);

  const fetchDocuments = useCallback(async () => {
    setIsFetching(true);
    try {
      const response = await fetch('/api/documents');
      if (!response.ok) throw new Error('Failed to fetch documents');
      const data = (await response.json()) as DocumentsResponse;
      setDocuments(data.documents ?? []);
    } catch (error) {
      console.error('Failed to fetch documents:', error);
    } finally {
      setIsFetching(false);
    }
  }, []);

  const uploadDocument = useCallback(
    (file: File, options?: UploadOptions) =>
      new Promise<void>((resolve, reject) => {
        const formData = new FormData();
        formData.append('file', file);

        const xhr = new XMLHttpRequest();
        let lastResponseIndex = 0;
        let eventName = 'message';
        let dataLines: string[] = [];
        let settled = false;

        const finishSuccess = async () => {
          if (settled) return;
          settled = true;
          await fetchDocuments();
          options?.onSuccess?.();
          resolve();
        };

        const finishError = (message: string) => {
          if (settled) return;
          settled = true;
          setUploadState({ status: 'error', error: message });
          reject(new Error(message));
        };

        const parseEvent = (type: string, rawData: string) => {
          const payload = tryParseJson<SsePayload>(rawData);
          if (!payload) return;

          if (type === 'upload_complete' || payload.type === 'upload_complete') {
            const next = payload as Extract<SsePayload, { type: 'upload_complete' }>;
            setUploadState((prev) => ({
              status: 'upload_complete',
              documentId: next.documentId,
              totalChunks: next.totalChunks,
              totalSets: next.totalSets,
              uploadProgress: 100,
              uploadLengthComputable:
                'uploadLengthComputable' in prev && typeof prev.uploadLengthComputable === 'boolean'
                  ? prev.uploadLengthComputable
                  : true,
            }));
            return;
          }

          if (type === 'embedding_progress' || payload.type === 'embedding_progress') {
            const next = payload as Extract<SsePayload, { type: 'embedding_progress' }>;
            const indexingProgress =
              next.totalChunks > 0
                ? clampProgress((next.processedChunks / next.totalChunks) * 100)
                : 0;

            setUploadState((prev) => ({
              status: 'indexing',
              processedChunks: next.processedChunks,
              totalChunks: next.totalChunks,
              setIndex: next.setIndex,
              totalSets: next.totalSets,
              indexingProgress,
              uploadProgress:
                'uploadProgress' in prev && typeof prev.uploadProgress === 'number'
                  ? prev.uploadProgress
                  : 100,
              uploadLengthComputable:
                'uploadLengthComputable' in prev && typeof prev.uploadLengthComputable === 'boolean'
                  ? prev.uploadLengthComputable
                  : true,
              documentId:
                'documentId' in prev && typeof prev.documentId === 'string'
                  ? prev.documentId
                  : next.documentId,
            }));
            return;
          }

          if (type === 'done' || payload.type === 'done') {
            const next = payload as Extract<SsePayload, { type: 'done' }>;
            setUploadState((prev) => ({
              status: 'done',
              documentId: next.documentId,
              totalChunks: next.totalChunks,
              uploadProgress:
                'uploadProgress' in prev && typeof prev.uploadProgress === 'number'
                  ? prev.uploadProgress
                  : 100,
              indexingProgress: 100,
            }));
            void finishSuccess();
            return;
          }

          if (type === 'error' || payload.type === 'error') {
            const next = payload as Extract<SsePayload, { type: 'error' }>;
            finishError(next.error || 'Upload failed');
          }
        };

        const flushEvent = () => {
          if (dataLines.length === 0) {
            eventName = 'message';
            return;
          }
          parseEvent(eventName, dataLines.join('\n').trim());
          eventName = 'message';
          dataLines = [];
        };

        const parseStream = () => {
          const chunk = xhr.responseText.slice(lastResponseIndex);
          if (!chunk) return;
          lastResponseIndex = xhr.responseText.length;

          const lines = chunk.split(/\r?\n/);
          for (const line of lines) {
            if (line.startsWith('event:')) {
              eventName = line.slice(6).trim() || 'message';
              continue;
            }
            if (line.startsWith('data:')) {
              dataLines.push(line.slice(5).trimStart());
              continue;
            }
            if (line.trim() === '') {
              flushEvent();
            }
          }
        };

        xhr.open('POST', '/api/documents/upload', true);
        xhr.setRequestHeader('Accept', 'text/event-stream');

        xhr.upload.onprogress = (event) => {
          const uploadProgress =
            event.lengthComputable && event.total > 0
              ? clampProgress((event.loaded / event.total) * 100)
              : 0;

          setUploadState((prev) => {
            if (prev.status === 'indexing' || prev.status === 'done') return prev;

            const snapshot: UploadProgressSnapshot = {
              uploadProgress,
              uploadBytesLoaded: event.loaded,
              uploadBytesTotal: event.total,
              uploadLengthComputable: event.lengthComputable,
            };

            return {
              status: 'uploading',
              ...snapshot,
            };
          });
        };

        xhr.upload.onloadstart = () => {
          setUploadState({
            status: 'uploading',
            uploadProgress: 0,
            uploadBytesLoaded: 0,
            uploadBytesTotal: file.size,
            uploadLengthComputable: file.size > 0,
          });
        };

        xhr.onreadystatechange = () => {
          if (xhr.readyState === XMLHttpRequest.LOADING) {
            parseStream();
          }
        };

        xhr.onprogress = () => {
          if (xhr.readyState === XMLHttpRequest.LOADING) {
            parseStream();
          }
        };

        xhr.onload = () => {
          parseStream();
          flushEvent();

          if (xhr.status < 200 || xhr.status >= 300) {
            finishError(`Upload failed with status ${xhr.status}`);
            return;
          }

          void finishSuccess();
        };

        xhr.onerror = () => finishError('Network error while uploading document');
        xhr.onabort = () => finishError('Upload cancelled');

        xhr.send(formData);
      }),
    [fetchDocuments]
  );

  const deleteDocument = useCallback(async (documentId: string) => {
    const response = await fetch(`/api/documents/${documentId}`, { method: 'DELETE' });
    if (!response.ok) throw new Error('Failed to delete document');
    setDocuments((prev) => prev.filter((doc) => doc.id !== documentId));
  }, []);

  const resetUploadState = useCallback(() => {
    setUploadState({ status: 'idle' });
  }, []);

  return useMemo(
    () => ({
      documents,
      isFetching,
      uploadState,
      fetchDocuments,
      uploadDocument,
      deleteDocument,
      resetUploadState,
    }),
    [documents, isFetching, uploadState, fetchDocuments, uploadDocument, deleteDocument, resetUploadState]
  );
}