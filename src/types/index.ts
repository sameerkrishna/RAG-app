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
  level: 'high' | 'medium' | 'low';
  score: number;
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
}

export type UploadState =
  | { status: 'idle' }
  | { status: 'uploading' }
  | { status: 'processing'; stage: string }
  | { status: 'embedding'; progress: number; total: number }
  | { status: 'complete' }
  | { status: 'error'; error: string; code: string };

export interface WebSearchSource {
  uri: string;
  title: string;
}
