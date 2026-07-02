import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useDocuments } from '../hooks/useDocuments';
import { Upload, FileIcon, Trash2, AlertCircle, Loader2, CheckCircle, ArrowLeft, X } from 'lucide-react';
import { Button } from '../components/ui/Button';
import { formatBytes, formatTimestamp } from '../lib/utils';
import type { Document } from '../types';
import { useSeeding } from '../context/SeedingContext';

interface KnowledgeBaseProps {
  sessionId: string;
}

const MAX_UPLOAD_SIZE_MB = 5;
const MAX_PDFS = 3;

function DocumentSkeleton() {
  return (
    <div className="space-y-2 animate-pulse">
      {[1, 2, 3].map(i => (
        <div key={i} className="flex items-start gap-3 rounded-xl border bg-card p-4">
          <div className="mt-0.5 h-5 w-5 rounded bg-muted" />
          <div className="flex-1 space-y-2">
            <div className="h-4 w-2/5 rounded bg-muted" />
            <div className="h-3 w-1/3 rounded bg-muted" />
          </div>
        </div>
      ))}
    </div>
  );
}

export default function KnowledgeBase({ sessionId }: KnowledgeBaseProps) {
  const {
    documents,
    globalDocuments,
    uploadState,
    uploadDocument,
    deleteDocument,
    resetUploadState
  } = useDocuments(sessionId);

  const { isSeeding } = useSeeding();
  const [dragOver, setDragOver]         = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [fileError, setFileError]       = useState<string | null>(null);
  const [confirmDoc, setConfirmDoc]     = useState<Document | null>(null);

  const handleFileSelect = (file: File) => {
    setFileError(null);
    if (file.type !== 'application/pdf') {
      setFileError('Only PDF files are supported.');
      return;
    }
    if (file.size > MAX_UPLOAD_SIZE_MB * 1024 * 1024) {
      setFileError(`File exceeds ${MAX_UPLOAD_SIZE_MB}MB limit.`);
      return;
    }
    setSelectedFile(file);
  };

  const handleUpload = async () => {
    if (!selectedFile) return;
    const result = await uploadDocument(selectedFile);
    if (result) {
      setSelectedFile(null);
    }
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = () => setDragOver(false);

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFileSelect(file);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleFileSelect(file);
  };

  const handleDeleteConfirmed = async () => {
    if (!confirmDoc) return;
    await deleteDocument(confirmDoc.document_id, confirmDoc.filename);
    setConfirmDoc(null);
  };

  const renderUploadState = () => {
    switch (uploadState.status) {
      case 'uploading':
        return (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Uploading file...
          </div>
        );

      case 'upload_complete':
        return (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            File received — starting embedding...
          </div>
        );

      case 'indexing': {
        const processed = (uploadState as any).processedChunks ?? 0;
        const total     = (uploadState as any).totalChunks ?? 1;
        const pct       = Math.round((processed / total) * 100);
        return (
          <div className="space-y-1.5">
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>Indexing chunks...</span>
              <span>{processed} / {total}</span>
            </div>
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-1.5 rounded-full bg-primary transition-all duration-300"
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        );
      }

      case 'complete':
        return (
          <div className="flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
            <CheckCircle className="h-4 w-4" />
            Upload complete!
          </div>
        );

      case 'error':
        return (
          <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4" />
            {(uploadState as any).error}
            <Button variant="outline" size="sm" onClick={resetUploadState}>
              Retry
            </Button>
          </div>
        );

      default:
        return null;
    }
  };

  const allDocuments = [...documents, ...globalDocuments];

  const renderDocuments = () => {
    if (isSeeding && allDocuments.length === 0) {
      return (
        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
              Indexed Documents
            </h2>
            <span className="flex items-center gap-1 text-[11px] text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" />
              Loading knowledge base...
            </span>
          </div>
          <DocumentSkeleton />
        </section>
      );
    }

    if (allDocuments.length === 0) {
      return (
        <div className="rounded-xl border bg-card py-16 text-center">
          <Upload className="mx-auto mb-4 h-12 w-12 text-muted-foreground/40" />
          <h3 className="mb-2 text-lg font-medium">Empty Knowledge Base</h3>
          <p className="mb-4 text-sm text-muted-foreground">
            Upload PDF documents to start asking questions
          </p>
          <p className="text-xs text-muted-foreground">
            Or add seed documents to the seed_documents/ folder
          </p>
        </div>
      );
    }

    return (
      <section className="space-y-3">
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Indexed Documents
        </h2>
        <div className="grid gap-2">
          {allDocuments.map(doc => (
            <div key={doc.document_id}>
              <div className="flex items-start justify-between rounded-xl border bg-card p-4 transition-colors hover:bg-accent/30">
                <div className="flex items-start gap-3">
                  <FileIcon className="mt-0.5 h-5 w-5 text-muted-foreground" />
                  <div>
                    <h3 className="text-sm font-medium">{doc.filename}</h3>
                    <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                      <span>{doc.chunk_count} chunks</span>
                      <span>{doc.page_count} pages</span>
                      {doc.source_type === 'seed' && (
                        <span className="rounded bg-primary/10 px-2 py-0.5 text-[10px] font-medium text-primary uppercase">
                          Seed
                        </span>
                      )}
                      {doc.upload_timestamp && (
                        <span>Uploaded {formatTimestamp(doc.upload_timestamp)}</span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  {doc.source_type !== 'seed' && (
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setConfirmDoc(confirmDoc?.document_id === doc.document_id ? null : doc)}
                      className="h-8 w-8 text-destructive hover:text-destructive"
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  )}
                </div>
              </div>

              {/* Inline delete confirmation — no browser dialog */}
              {confirmDoc?.document_id === doc.document_id && (
                <div className="mx-1 flex items-center justify-between rounded-b-xl border border-t-0 bg-destructive/5 px-4 py-3">
                  <span className="text-sm text-destructive">
                    Delete &ldquo;{doc.filename}&rdquo;?
                  </span>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setConfirmDoc(null)}
                    >
                      Cancel
                    </Button>
                    <Button
                      size="sm"
                      className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                      onClick={handleDeleteConfirmed}
                    >
                      Delete
                    </Button>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </section>
    );
  };

  return (
    <div className="flex h-screen w-full flex-col overflow-hidden bg-background">
      {/* Header */}
      <header className="flex h-14 flex-shrink-0 items-center justify-between border-b bg-background px-6">
        <div className="flex items-center gap-3">
          <Link
            to="/assistant"
            className="flex h-8 w-8 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <h1 className="text-sm font-semibold">Knowledge Base</h1>
          {isSeeding && (
            <span className="flex items-center gap-1 rounded-full bg-primary/10 px-2 py-0.5 text-[11px] text-primary">
              <span className="h-1.5 w-1.5 rounded-full bg-primary animate-pulse" />
              Setting up...
            </span>
          )}
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 overflow-y-auto p-6">
        <div className="mx-auto max-w-3xl space-y-6">

          {/* Upload Section */}
          {documents.length < MAX_PDFS && (
            <section className="rounded-xl border bg-card p-6">
              <div
                className={`rounded-xl border-2 border-dashed p-8 text-center transition-colors ${
                  dragOver ? 'border-primary bg-primary/5' : 'border-muted-foreground/20'
                }`}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
              >
                <Upload className="mx-auto mb-4 h-8 w-8 text-muted-foreground" />
                <p className="mb-4 text-sm text-muted-foreground">
                  Drag and drop a PDF here, or click to browse
                </p>
                <input
                  type="file"
                  accept=".pdf"
                  onChange={handleInputChange}
                  className="hidden"
                  id="file-upload"
                />
                <label htmlFor="file-upload" className="cursor-pointer">
                  <span className="inline-flex h-10 items-center justify-center rounded-lg border border-input bg-background px-4 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground">
                    Browse Files
                  </span>
                </label>
                <p className="mt-2 text-xs text-muted-foreground">
                  Max {MAX_UPLOAD_SIZE_MB}MB, {MAX_PDFS} files per session
                </p>
              </div>

              {/* Inline file validation error */}
              {fileError && (
                <div className="mt-3 flex items-center gap-2 rounded-lg bg-destructive/10 px-4 py-3 text-sm text-destructive">
                  <AlertCircle className="h-4 w-4 shrink-0" />
                  {fileError}
                  <button onClick={() => setFileError(null)} className="ml-auto">
                    <X className="h-4 w-4" />
                  </button>
                </div>
              )}

              {selectedFile && (
                <div className="mt-4 rounded-lg bg-secondary/50 p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <FileIcon className="h-4 w-4" />
                      <span className="text-sm">{selectedFile.name}</span>
                      <span className="text-xs text-muted-foreground">
                        ({formatBytes(selectedFile.size)})
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Button size="sm" onClick={handleUpload} disabled={uploadState.status !== 'idle'}>
                        Upload
                      </Button>
                      <Button variant="outline" size="sm" onClick={() => setSelectedFile(null)}>
                        Cancel
                      </Button>
                    </div>
                  </div>
                  <div className="mt-3">
                    {renderUploadState()}
                  </div>
                </div>
              )}

              {/* Show progress even after selectedFile is cleared (during indexing) */}
              {!selectedFile && uploadState.status !== 'idle' && (
                <div className="mt-3">
                  {renderUploadState()}
                </div>
              )}
            </section>
          )}

          {documents.length >= MAX_PDFS && (
            <div className="flex items-center gap-2 rounded-lg bg-warning/10 p-4 text-warning-foreground">
              <AlertCircle className="h-4 w-4" />
              <span className="text-sm">
                Maximum {MAX_PDFS} PDF uploads reached. Delete existing uploads to add more.
              </span>
            </div>
          )}

          {renderDocuments()}
        </div>
      </main>
    </div>
  );
}