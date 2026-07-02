import { useEffect, useMemo, useState } from 'react';
import { useDocuments } from '../hooks/useDocuments';
import type { UploadState } from '../types';

function cx(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(' ');
}

function ProgressBar({
  colorClass,
  progress,
  indeterminate = false,
}: {
  colorClass: string;
  progress: number;
  indeterminate?: boolean;
}) {
  return (
    <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
      {indeterminate ? (
        <div
          className={cx(
            'h-full w-1/3 rounded-full animate-[pulse_1.2s_ease-in-out_infinite]',
            colorClass
          )}
        />
      ) : (
        <div
          className={cx('h-full rounded-full transition-all duration-200', colorClass)}
          style={{ width: `${Math.max(0, Math.min(100, progress))}%` }}
        />
      )}
    </div>
  );
}

function renderProgressBars(uploadState: UploadState) {
  if (uploadState.status === 'idle') return null;
  if (uploadState.status === 'error') return null;
  if (uploadState.status === 'done') return null;

  const showUploadBar =
    uploadState.status === 'uploading' ||
    uploadState.status === 'upload_complete' ||
    uploadState.status === 'indexing';

  const showIndexingBar =
    uploadState.status === 'upload_complete' || uploadState.status === 'indexing';

  const uploadProgress =
    'uploadProgress' in uploadState && typeof uploadState.uploadProgress === 'number'
      ? uploadState.uploadProgress
      : 0;

  const uploadLengthComputable =
    uploadState.status === 'uploading'
      ? uploadState.uploadLengthComputable !== false
      : true;

  const indexingProgress =
    uploadState.status === 'indexing' ? uploadState.indexingProgress : 0;

  const processedChunks = uploadState.status === 'indexing' ? uploadState.processedChunks : 0;
  const totalChunks =
    uploadState.status === 'indexing'
      ? uploadState.totalChunks
      : uploadState.status === 'upload_complete'
        ? uploadState.totalChunks
        : 0;

  return (
    <div className="mt-4 space-y-3 rounded-xl border border-border bg-card p-4">
      {showUploadBar && (
        <div className="space-y-1.5">
          <div className="flex items-center justify-between text-sm">
            <span className="font-medium text-foreground">Uploading file...</span>
            <span className="text-muted-foreground">
              {uploadLengthComputable ? `${uploadProgress}%` : 'Uploading...'}
            </span>
          </div>
          <ProgressBar
            colorClass="bg-green-500"
            progress={uploadProgress}
            indeterminate={!uploadLengthComputable && uploadState.status === 'uploading'}
          />
        </div>
      )}

      {showIndexingBar && (
        <div className="space-y-1.5">
          <div className="flex items-center justify-between text-sm">
            <span className="font-medium text-foreground">Chunking and Embedding...</span>
            <span className="text-muted-foreground">
              {uploadState.status === 'indexing'
                ? `${indexingProgress}% · ${processedChunks} / ${totalChunks} chunks`
                : `0% · 0 / ${totalChunks} chunks`}
            </span>
          </div>
          <ProgressBar colorClass="bg-primary" progress={indexingProgress} />
        </div>
      )}
    </div>
  );
}

export default function KnowledgeBase() {
  const {
    documents,
    isFetching,
    uploadState,
    fetchDocuments,
    uploadDocument,
    deleteDocument,
    resetUploadState,
  } = useDocuments();

  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    void fetchDocuments();
  }, [fetchDocuments]);

  const sessionFileCount = useMemo(() => documents.length, [documents.length]);

  const handleFileSelection = async (file: File | null) => {
    if (!file) return;

    setErrorMessage(null);
    setSelectedFile(file);

    try {
      await uploadDocument(file);
      setSelectedFile(null);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'Upload failed');
    }
  };

  const onInputChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0] ?? null;
    await handleFileSelection(file);
    event.target.value = '';
  };

  const onDrop = async (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragging(false);

    const file = event.dataTransfer.files?.[0] ?? null;
    await handleFileSelection(file);
  };

  return (
    <div className="space-y-6">
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={onDrop}
        className={cx(
          'rounded-2xl border-2 border-dashed p-6 transition-colors',
          isDragging ? 'border-primary bg-primary/5' : 'border-border bg-card'
        )}
      >
        <div className="space-y-3 text-center">
          <div>
            <h2 className="text-lg font-semibold text-foreground">Upload PDF</h2>
            <p className="text-sm text-muted-foreground">
              Drag and drop a PDF here, or choose a file to add it to the knowledge base.
            </p>
          </div>

          <div className="flex justify-center">
            <label className="inline-flex cursor-pointer items-center rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:opacity-90">
              Choose file
              <input type="file" accept="application/pdf" className="hidden" onChange={onInputChange} />
            </label>
          </div>

          {selectedFile && (
            <div className="rounded-lg border border-border bg-muted/40 px-3 py-2 text-sm text-foreground">
              Selected: {selectedFile.name}
            </div>
          )}

          {renderProgressBars(uploadState)}

          {uploadState.status === 'done' && (
            <div className="rounded-lg border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
              File uploaded and indexed successfully.
            </div>
          )}

          {uploadState.status === 'error' && (
            <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {uploadState.error}
            </div>
          )}

          {errorMessage && uploadState.status !== 'error' && (
            <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {errorMessage}
            </div>
          )}
        </div>
      </div>

      <div className="rounded-2xl border border-border bg-card p-4">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h3 className="text-base font-semibold text-foreground">Session files</h3>
            <p className="text-sm text-muted-foreground">
              {sessionFileCount} file{sessionFileCount === 1 ? '' : 's'} in this knowledge base
            </p>
          </div>

          {uploadState.status !== 'idle' && uploadState.status !== 'uploading' && (
            <button
              type="button"
              onClick={resetUploadState}
              className="text-sm text-muted-foreground hover:text-foreground"
            >
              Clear status
            </button>
          )}
        </div>

        {isFetching ? (
          <div className="text-sm text-muted-foreground">Loading files...</div>
        ) : documents.length === 0 ? (
          <div className="text-sm text-muted-foreground">No files uploaded yet.</div>
        ) : (
          <div className="space-y-2">
            {documents.map((doc) => (
              <div
                key={doc.id}
                className="flex items-center justify-between rounded-lg border border-border px-3 py-2"
              >
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium text-foreground">{doc.filename}</div>
                  {typeof doc.chunkCount === 'number' && (
                    <div className="text-xs text-muted-foreground">{doc.chunkCount} chunks</div>
                  )}
                </div>

                <button
                  type="button"
                  onClick={() => void deleteDocument(doc.id)}
                  className="rounded-md px-2 py-1 text-sm text-red-600 hover:bg-red-50"
                >
                  Delete
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}