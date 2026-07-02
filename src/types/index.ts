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
    }
  | {
      status: 'indexing';
      processedChunks: number;
      totalChunks: number;
      setIndex: number;
      totalSets: number;
      indexingProgress: number;
      uploadProgress?: number;
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