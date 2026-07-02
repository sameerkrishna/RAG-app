export interface SearchResult {
  chunkId: string;
  documentId: string;
  filename: string;
  pageNumber: number;
  excerpt: string;
  sourceType: 'session' | 'global' | 'web';
}

export interface Citation {
  id: string;
  index: number;
  documentId: string;
  filename: string;
  pageNumber?: number;
  section?: string;
  excerpt: string;
  score: number;
  sourceType: string;
  chunkId: string;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  citations?: Citation[];
  coverage?: CoverageInfo;
  sources?: SearchResult[];
  isRefusal?: boolean;
  isStreaming?: boolean;
}

export interface CoverageInfo {
  confidence: number;
  topScore: number;
  level?: 'high' | 'medium' | 'low';
  score?: number;
  reason?: string;
}

export interface Document {
  document_id: string;
  filename: string;
  chunk_count: number;
  page_count: number;
  upload_timestamp?: string;
  source_type: 'seed' | 'session_upload';
  first_chunk_text?: string;
  fileSize?: number;
  status?: 'indexing' | 'ready';  // present only for session uploads in progress
}

export type UploadState =
  | { status: 'idle' }
  | {
      status: 'uploading';
      uploadProgress: number;
      uploadBytesLoaded?: number;
      uploadBytesTotal?: number;
      uploadLengthComputable?: boolean;
    }
  | {
      status: 'upload_complete';
      documentId: string;
      totalChunks: number;
      totalSets: number;
      uploadProgress: number;
      uploadLengthComputable?: boolean;
    }
  | {
      status: 'indexing';
      processedChunks: number;
      totalChunks: number;
      setIndex: number;
      totalSets: number;
      indexingProgress: number;
      uploadProgress?: number;
      uploadLengthComputable?: boolean;
      documentId?: string;
    }
  | {
      status: 'done';
      documentId: string;
      totalChunks?: number;
      uploadProgress?: number;
      indexingProgress?: number;
    }
  | {
      status: 'error';
      error: string;
      uploadProgress?: number;
      indexingProgress?: number;
    };

export interface DocumentRecord {
  id: string;
  filename: string;
  createdAt?: string;
  updatedAt?: string;
  size?: number;
  mimeType?: string;
  chunkCount?: number;
}

export interface DocumentsResponse {
  documents: DocumentRecord[];
}

export interface DeleteDialogState {
  open: boolean;
  documentId: string | null;
  filename: string;
}

export interface UploadProgressSnapshot {
  uploadProgress: number;
  uploadBytesLoaded?: number;
  uploadBytesTotal?: number;
  uploadLengthComputable?: boolean;
}

export interface IndexingProgressSnapshot {
  indexingProgress: number;
  processedChunks: number;
  totalChunks: number;
  setIndex: number;
  totalSets: number;
}
export interface WebSearchSource {
  uri: string;
  title: string;
}
