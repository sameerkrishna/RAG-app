import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useDocuments } from '../hooks/useDocuments';
import { Upload, FileIcon, Trash2, AlertCircle, Loader2, CheckCircle, ArrowLeft, Files } from 'lucide-react';
import { Button } from '../components/ui/Button';
import { formatBytes, formatTimestamp } from '../lib/utils';
import type { Document } from '../types';

interface KnowledgeBaseProps {
  sessionId: string;
}

const MAX_UPLOAD_SIZE_MB = parseInt(import.meta.env.VITE_MAX_UPLOAD_SIZE_MB || '5');
const MAX_PDFS = parseInt(import.meta.env.VITE_MAX_PDFS_PER_SESSION || '3');

function ProgressBar({
  label, progress, active, done
}: {
  label: string; progress: number; active: boolean; done: boolean;
}) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs">
        <span className={done ? 'text-success font-medium' : active ? 'text-foreground' : 'text-muted-foreground'}>
          {label}
        </span>
        <span className="tabular-nums text-muted-foreground">
          {done ? '100%' : active ? `${Math.round(progress)}%` : '—'}
        </span>
      </div>
      <div className="h-1.5 w-full overflow-hidden rounded-full bg-secondary">
        <div
          className={`h-full rounded-full transition-all duration-500 ${done ? 'bg-success' : active ? 'bg-primary' : 'bg-muted'
            }`}
          style={{ width: `${done ? 100 : active ? Math.round(progress) : 0}%` }}
        />
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function KnowledgeBase({ sessionId }: KnowledgeBaseProps) {
  const {
    documents,
    globalDocuments,
    uploadState,
    uploadDocument,
    deleteDocument,
    resetUploadState,
    loading
  } = useDocuments(sessionId);

  const [dragOver, setDragOver] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Document | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);   // <-- ADDED

  const sessionUploads = documents.filter(d => d.source_type === 'session_upload');
  const uploadedCount = sessionUploads.length;
  const remainingUploads = Math.max(0, MAX_PDFS - uploadedCount);
  const atLimit = uploadedCount >= MAX_PDFS;

  const allDocuments = [...documents, ...globalDocuments];

  const handleFileSelect = (file: File) => {
    setFileError(null);
    if (file.type !== 'application/pdf') {
      setFileError('Only PDF files are supported.');
      return;
    }
    if (file.size > MAX_UPLOAD_SIZE_MB * 1024 * 1024) {
      setFileError(`File exceeds the ${MAX_UPLOAD_SIZE_MB} MB size limit.`);
      return;
    }
    const duplicate = allDocuments.some(
      d => d.filename.toLowerCase() === file.name.toLowerCase()
    );
    if (duplicate) {
      setFileError(`"${file.name}" is already indexed in this session.`);
      return;
    }
    setSelectedFile(file);
  };

  const handleUpload = async () => {
    if (!selectedFile) return;
    const result = await uploadDocument(selectedFile);
    if (result) setSelectedFile(null);
  };

  const handleDragOver = (e: React.DragEvent) => { e.preventDefault(); setDragOver(true); };
  const handleDragLeave = () => setDragOver(false);
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault(); setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFileSelect(file);
  };
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleFileSelect(file);
    e.target.value = '';
  };

  const handleDelete = (doc: Document) => {
    if (doc.source_type === 'seed') return;
    setDeleteTarget(doc);
  };

  // MODIFIED confirmDelete to show loader while deleting
  const confirmDelete = async () => {
    if (!deleteTarget) return;
    setIsDeleting(true);
    try {
      await deleteDocument(deleteTarget.document_id, deleteTarget.filename);
    } finally {
      setIsDeleting(false);
      setDeleteTarget(null);
    }
  };

  const isUploading = uploadState.status === 'uploading';
  const isIndexing = uploadState.status === 'indexing' || uploadState.status === 'upload_complete';
  const isComplete = uploadState.status === 'done';
  const isActive = isUploading || isIndexing || isComplete;

  const phase1Done = uploadState.status === 'upload_complete' || uploadState.status === 'indexing' || isComplete;
  const phase1Progress = phase1Done ? 100 : isUploading ? 60 : 0;

  const phase2Progress = (() => {
    if (isComplete) return 100;
    if (uploadState.status === 'indexing')
      return Math.round((uploadState.processedChunks / uploadState.totalChunks) * 100);
    return 0;
  })();
  const phase2Done = isComplete;
  const phase2Active = uploadState.status === 'indexing' || isComplete;

  const phase2Label = (() => {
    if (uploadState.status === 'indexing')
      return `Chunking & indexing — set ${uploadState.setIndex}/${uploadState.totalSets} (${uploadState.processedChunks}/${uploadState.totalChunks} chunks)`;
    if (isComplete) return 'Indexing complete';
    return 'Chunking & indexing';
  })();

  return (
    <div className="flex h-screen w-full flex-col overflow-hidden bg-background">
      <header className="flex h-14 flex-shrink-0 items-center justify-between border-b bg-background px-6">
        <div className="flex items-center gap-3">
          <Link
            to="/assistant"
            className="flex h-8 w-8 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <h1 className="text-sm font-semibold">Knowledge Base</h1>
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 overflow-y-auto p-6">
        <div className="mx-auto max-w-3xl space-y-6">

          {/* Upload Section */}
          {!atLimit && (
            <section className="rounded-xl border bg-card p-6 space-y-4">
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2 text-muted-foreground">
                  <Files className="h-4 w-4" />
                  <span>Files uploaded: <span className="font-semibold text-foreground">{uploadedCount}</span></span>
                </div>
                <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${remainingUploads === 1
                    ? 'bg-amber-500/10 text-amber-600'
                    : 'bg-primary/10 text-primary'
                  }`}>
                  {remainingUploads} upload{remainingUploads !== 1 ? 's' : ''} remaining
                </span>
              </div>

              {/* Drop zone */}
              <div
                className={`rounded-xl border-2 border-dashed p-8 text-center transition-colors ${dragOver ? 'border-primary bg-primary/5' : 'border-muted-foreground/20'
                  }`}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
              >
                <Upload className="mx-auto mb-4 h-8 w-8 text-muted-foreground" />
                <p className="mb-4 text-sm text-muted-foreground">
                  Drag and drop a PDF here, or click to browse
                </p>
                <input type="file" accept=".pdf" onChange={handleInputChange} className="hidden" id="file-upload" />
                <label htmlFor="file-upload" className="cursor-pointer">
                  <span className="inline-flex h-10 items-center justify-center rounded-lg border border-input bg-background px-4 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground">
                    Browse Files
                  </span>
                </label>
                <p className="mt-2 text-xs text-muted-foreground">
                  PDF only · Max {MAX_UPLOAD_SIZE_MB} MB · {MAX_PDFS} files per session
                </p>
              </div>

              {/* Validation error */}
              {fileError && (
                <div className="flex items-center gap-2 rounded-lg bg-destructive/10 px-4 py-3 text-sm text-destructive">
                  <AlertCircle className="h-4 w-4 flex-shrink-0" />
                  {fileError}
                  <button
                    className="ml-auto text-xs underline opacity-70 hover:opacity-100"
                    onClick={() => setFileError(null)}
                  >
                    Dismiss
                  </button>
                </div>
              )}

              {/* Selected file row */}
              {selectedFile && (
                <div className="rounded-lg bg-secondary/50 p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <FileIcon className="h-4 w-4" />
                      <span className="text-sm">{selectedFile.name}</span>
                      <span className="text-xs text-muted-foreground">({formatBytes(selectedFile.size)})</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Button size="sm" onClick={handleUpload} disabled={uploadState.status !== 'idle'}>Upload</Button>
                      <Button variant="outline" size="sm" onClick={() => setSelectedFile(null)}>Cancel</Button>
                    </div>
                  </div>
                </div>
              )}

              {isActive && (
                <div className="space-y-3 px-1">
                  <ProgressBar
                    label="Uploading file"
                    progress={phase1Progress}
                    active={isUploading}
                    done={phase1Done}
                  />
                  <ProgressBar
                    label={phase2Label}
                    progress={phase2Progress}
                    active={phase2Active && !phase2Done}
                    done={phase2Done}
                  />
                  <div className="flex items-center gap-2 text-xs">
                    {isComplete ? (
                      <><CheckCircle className="h-3.5 w-3.5 text-success" /><span className="text-success">Successfully indexed</span></>
                    ) : (
                      <><Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                        <span className="text-muted-foreground">
                          {isUploading && 'Uploading...'}
                          {uploadState.status === 'upload_complete' && 'Starting indexing...'}
                          {uploadState.status === 'indexing' && `Indexing set ${uploadState.setIndex} of ${uploadState.totalSets}${uploadState.totalSets > 1 && uploadState.setIndex < uploadState.totalSets
                              ? ' (waiting for rate limit after this set)'
                              : ''
                            }`}
                        </span></>
                    )}
                  </div>
                </div>
              )}

              {uploadState.status === 'error' && (
                <div className="flex items-center gap-2 text-sm text-destructive">
                  <AlertCircle className="h-4 w-4" />
                  {uploadState.error}
                  <Button variant="outline" size="sm" onClick={resetUploadState}>Retry</Button>
                </div>
              )}
            </section>
          )}

          {atLimit && (
            <div className="flex items-center gap-3 rounded-xl border border-amber-200 bg-amber-50 px-5 py-4 dark:border-amber-800 dark:bg-amber-950/30">
              <AlertCircle className="h-5 w-5 flex-shrink-0 text-amber-500" />
              <div className="text-sm">
                <span className="font-medium text-amber-700 dark:text-amber-400">Upload limit reached</span>
                <span className="ml-1 text-amber-600 dark:text-amber-500">
                  — {MAX_PDFS}/{MAX_PDFS} files used. Delete an existing upload to add more.
                </span>
              </div>
              <div className="ml-auto flex items-center gap-1.5 text-xs font-medium text-amber-600 dark:text-amber-500">
                <Files className="h-3.5 w-3.5" />
                {uploadedCount}/{MAX_PDFS}
              </div>
            </div>
          )}

          {/* Documents List */}
          {allDocuments.length === 0 ? (
            <div className="rounded-xl border bg-card py-16 text-center">
              <Upload className="mx-auto mb-4 h-12 w-12 text-muted-foreground/40" />
              <h3 className="mb-2 text-lg font-medium">Empty Knowledge Base</h3>
              <p className="mb-4 text-sm text-muted-foreground">Upload PDF documents to start asking questions</p>
              <p className="text-xs text-muted-foreground">Or add seed documents to the seed_documents/ folder</p>
            </div>
          ) : (
            <section className="space-y-3">
              <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Indexed Documents</h2>
              <div className="grid gap-2">
                {allDocuments.map(doc => (
                  <div
                    key={doc.document_id}
                    className="flex items-start justify-between rounded-xl border bg-card p-4 transition-colors hover:bg-accent/30"
                  >
                    <div className="flex items-start gap-3">
                      <FileIcon className="mt-0.5 h-5 w-5 text-muted-foreground" />
                      <div>
                        <h3 className="text-sm font-medium">{doc.filename}</h3>
                        <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                          {doc.status === 'indexing' ? (
                            <span className="flex items-center gap-1">
                              <Loader2 className="h-3 w-3 animate-spin" />
                              Indexing...
                            </span>
                          ) : (
                            <span>{doc.chunk_count} chunks</span>
                          )}
                          <span>{doc.page_count} pages</span>
                          {doc.source_type === 'seed' && (
                            <span className="rounded bg-primary/10 px-2 py-0.5 text-[10px] font-medium text-primary uppercase">Seed</span>
                          )}
                          {doc.source_type === 'session_upload' && (
                            <span className="rounded bg-blue-500/10 px-2 py-0.5 text-[10px] font-medium text-blue-600 uppercase">Session</span>
                          )}
                          {doc.status === 'indexing' && (
                            <span className="rounded bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-600 uppercase">Indexing</span>
                          )}
                          {doc.upload_timestamp && doc.status !== 'indexing' && (
                            <span>Uploaded {formatTimestamp(doc.upload_timestamp)}</span>
                          )}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {doc.source_type !== 'seed' && doc.status !== 'indexing' && (
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDelete(doc)}
                          className="h-8 w-8 text-destructive hover:text-destructive"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </section>
          )}
        </div>
      </main>

      {/* Delete confirmation modal — updated buttons */}
      {deleteTarget && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
          onClick={() => { if (!isDeleting) setDeleteTarget(null); }}
        >
          <div
            className="mx-4 w-full max-w-sm rounded-xl border bg-card p-6 shadow-lg space-y-4"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-start gap-3">
              <div className="flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-full bg-destructive/10">
                <Trash2 className="h-4 w-4 text-destructive" />
              </div>
              <div>
                <h2 className="text-sm font-semibold">Delete document?</h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  <span className="font-medium text-foreground">"{deleteTarget.filename}"</span> will be permanently removed from your session.
                </p>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setDeleteTarget(null)}
                disabled={isDeleting}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                onClick={confirmDelete}
                disabled={isDeleting}
              >
                {isDeleting ? (
                  <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                ) : null}
                {isDeleting ? 'Deleting...' : 'Delete'}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
