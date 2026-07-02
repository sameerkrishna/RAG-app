import { useCallback, useEffect, useMemo, useState } from 'react';
import type { Document, UploadState } from '../types';

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

function normalizeDocuments(payload: unknown): Document[] {
  if (Array.isArray(payload)) return payload as Document[];

  if (
    payload &&
    typeof payload === 'object' &&
    'documents' in payload &&
    Array.isArray((payload as { documents?: unknown }).documents)
  ) {
    return (payload as { documents: Document[] }).documents;
  }

  return [];
}

export function useDocuments(sessionId: string) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [globalDocuments, setGlobalDocuments] = useState<Document[]>([]);
  const [uploadState, setUploadState] = useState<UploadState>({ status: 'idle' });
  const [isFetching, setIsFetching] = useState(false);

  const fetchDocuments = useCallback(async () => {
    setIsFetching(true);

    try {
      const [sessionResponse, globalResponse] = await Promise.all([
        fetch(`/api/sessions/${sessionId}/documents`),
        fetch('/api/documents'),
      ]);

      if (!sessionResponse.ok) {
        throw new Error(`Failed to fetch session documents: ${sessionResponse.status}`);
      }

      if (!globalResponse.ok) {
        throw new Error(`Failed to fetch global documents: ${globalResponse.status}`);
      }

      const sessionJson = await sessionResponse.json();
      const globalJson = await globalResponse.json();

      setDocuments(normalizeDocuments(sessionJson));
      setGlobalDocuments(normalizeDocuments(globalJson));
    } catch (error) {
      console.error('Failed to fetch documents:', error);
      setDocuments([]);
      setGlobalDocuments([]);
    } finally {
      setIsFetching(false);
    }
  }, [sessionId]);

  useEffect(() => {
    void fetchDocuments();
  }, [fetchDocuments]);

  const uploadDocument = useCallback(
    (file: File, options?: UploadOptions) =>
      new Promise<boolean>((resolve) => {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('sessionId', sessionId);

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
          resolve(true);
        };

        const finishError = (message: string) => {
          if (settled) return;
          settled = true;
          setUploadState((prev) => {
            const prevAny = prev as {
              uploadProgress?: number;
              indexingProgress?: number;
            };

            return {
              status: 'error',
              error: message,
              uploadProgress: prevAny.uploadProgress,
              indexingProgress: prevAny.indexingProgress,
            };
          });
          resolve(false);
        };

        const parseEvent = (type: string, rawData: string) => {
          const payload = tryParseJson<SsePayload>(rawData);
          if (!payload) return;

          if (type === 'upload_complete' || payload.type === 'upload_complete') {
            const next = payload as Extract<SsePayload, { type: 'upload_complete' }>;

            setUploadState((prev) => {
              const prevAny = prev as { uploadLengthComputable?: boolean };

              return {
                status: 'upload_complete',
                documentId: next.documentId,
                totalChunks: next.totalChunks,
                totalSets: next.totalSets,
                uploadProgress: 100,
                uploadLengthComputable:
                  typeof prevAny.uploadLengthComputable === 'boolean'
                    ? prevAny.uploadLengthComputable
                    : true,
              };
            });

            return;
          }

          if (type === 'embedding_progress' || payload.type === 'embedding_progress') {
            const next = payload as Extract<SsePayload, { type: 'embedding_progress' }>;
            const indexingProgress =
              next.totalChunks > 0
                ? clampProgress((next.processedChunks / next.totalChunks) * 100)
                : 0;

            setUploadState((prev) => {
              const prevAny = prev as {
                uploadProgress?: number;
                uploadLengthComputable?: boolean;
                documentId?: string;
              };

              return {
                status: 'indexing',
                processedChunks: next.processedChunks,
                totalChunks: next.totalChunks,
                setIndex: next.setIndex,
                totalSets: next.totalSets,
                indexingProgress,
                uploadProgress:
                  typeof prevAny.uploadProgress === 'number'
                    ? prevAny.uploadProgress
                    : 100,
                uploadLengthComputable:
                  typeof prevAny.uploadLengthComputable === 'boolean'
                    ? prevAny.uploadLengthComputable
                    : true,
                documentId:
                  typeof prevAny.documentId === 'string'
                    ? prevAny.documentId
                    : next.documentId,
              };
            });

            return;
          }

          if (type === 'done' || payload.type === 'done') {
            const next = payload as Extract<SsePayload, { type: 'done' }>;

            setUploadState((prev) => {
              const prevAny = prev as { uploadProgress?: number };

              return {
                status: 'done',
                documentId: next.documentId,
                totalChunks: next.totalChunks,
                uploadProgress:
                  typeof prevAny.uploadProgress === 'number'
                    ? prevAny.uploadProgress
                    : 100,
                indexingProgress: 100,
              };
            });

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

        xhr.open('POST', `/api/sessions/${sessionId}/documents/upload`, true);
        xhr.setRequestHeader('Accept', 'text/event-stream');

        xhr.upload.onloadstart = () => {
          setUploadState({
            status: 'uploading',
            uploadProgress: 0,
            uploadBytesLoaded: 0,
            uploadBytesTotal: file.size,
            uploadLengthComputable: file.size > 0,
          });
        };

        xhr.upload.onprogress = (event) => {
          const uploadProgress =
            event.lengthComputable && event.total > 0
              ? clampProgress((event.loaded / event.total) * 100)
              : 0;

          setUploadState((prev) => {
            if (prev.status === 'indexing' || prev.status === 'done') return prev;

            return {
              status: 'uploading',
              uploadProgress,
              uploadBytesLoaded: event.loaded,
              uploadBytesTotal: event.total,
              uploadLengthComputable: event.lengthComputable,
            };
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

          if (!settled) {
            void finishSuccess();
          }
        };

        xhr.onerror = () => finishError('Network error while uploading document');
        xhr.onabort = () => finishError('Upload cancelled');

        xhr.send(formData);
      }),
    [fetchDocuments, sessionId]
  );

  const deleteDocument = useCallback(
    async (documentId: string, _filename?: string) => {
      const response = await fetch(`/api/sessions/${sessionId}/documents/${documentId}`, {
        method: 'DELETE',
      });

      if (!response.ok) {
        throw new Error('Failed to delete document');
      }

      setDocuments((prev) => prev.filter((doc) => doc.document_id !== documentId));
    },
    [sessionId]
  );

  const resetUploadState = useCallback(() => {
    setUploadState({ status: 'idle' });
  }, []);

  return useMemo(
    () => ({
      documents,
      globalDocuments,
      isFetching,
      uploadState,
      fetchDocuments,
      uploadDocument,
      deleteDocument,
      resetUploadState,
    }),
    [
      documents,
      globalDocuments,
      isFetching,
      uploadState,
      fetchDocuments,
      uploadDocument,
      deleteDocument,
      resetUploadState,
    ]
  );
}