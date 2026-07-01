export interface SearchResult {
  chunkId: string;
  documentId: string;
  filename: string;
  pageNumber: number;
  excerpt: string;
  sourceType: 'session' | 'global' | 'web';
  score?: number;
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
  id: string;           // memory turn ID (turn_xxx)
  answerKey?: string;   // Supabase UUID — used for feedback PATCH
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
  status?: 'indexing' | 'ready';
}

export type UploadState =
  | { status: 'idle' }
  | { status: 'uploading' }
  | { status: 'upload_complete'; documentId: string; totalChunks: number; totalSets: number }
  | {
      status: 'indexing';
      processedChunks: number;
      totalChunks: number;
      setIndex: number;
      totalSets: number;
    }
  | { status: 'complete' }
  | { status: 'error'; error: string; code: string };

export interface WebSearchSource {
  uri: string;
  title: string;
}
