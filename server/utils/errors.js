export class AppError extends Error {
  constructor(message, code, statusCode = 500) {
    super(message);
    this.code = code;
    this.statusCode = statusCode;
    this.isOperational = true;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class ValidationError extends AppError {
  constructor(message, code = 'VALIDATION_ERROR') {
    super(message, code, 400);
  }
}

export class UploadLimitError extends AppError {
  constructor(message, code = 'UPLOAD_LIMIT_EXCEEDED') {
    super(message, code, 400);
  }
}

export class FileTooLargeError extends AppError {
  constructor(maxSizeMB) {
    super(`File exceeds maximum size of ${maxSizeMB}MB`, 'FILE_TOO_LARGE', 413);
  }
}

export class InvalidFileTypeError extends AppError {
  constructor() {
    super('Only PDF files are allowed', 'INVALID_FILE_TYPE', 415);
  }
}

export class TooManyPDFsError extends AppError {
  constructor(max) {
    super(`Maximum ${max} PDFs allowed per session`, 'TOO_MANY_PDFS', 400);
  }
}

export class DuplicateFileError extends AppError {
  constructor(filename) {
    super(`File "${filename}" already exists in this session`, 'DUPLICATE_FILE', 409);
  }
}

export class CorruptedPDFError extends AppError {
  constructor() {
    super('Failed to parse PDF file. It may be corrupted.', 'CORRUPTED_PDF', 422);
  }
}

export class RateLimitError extends AppError {
  constructor(retryAfter = 60) {
    super('Rate limit exceeded. Please try again later.', 'RATE_LIMIT_EXCEEDED', 429);
    this.retryAfter = retryAfter;
  }
}

export class LLMUnavailableError extends AppError {
  constructor() {
    super('AI service is temporarily unavailable. Please try again.', 'LLM_UNAVAILABLE', 503);
  }
}

export class EmbeddingError extends AppError {
  constructor(message = 'Failed to generate embeddings') {
    super(message, 'EMBEDDING_ERROR', 503);
  }
}

export class RetrievalUnavailableError extends AppError {
  constructor() {
    super('Document retrieval is temporarily unavailable', 'RETRIEVAL_UNAVAILABLE', 503);
  }
}

export class WebSearchUnavailableError extends AppError {
  constructor() {
    super('Web search is temporarily unavailable', 'WEB_SEARCH_UNAVAILABLE', 503);
  }
}

export class CoverageTooLowError extends AppError {
  constructor() {
    super('Insufficient information in knowledge base', 'COVERAGE_TOO_LOW', 200);
  }
}

export function isRetryableError(error) {
  const retryableCodes = ['RATE_LIMIT_EXCEEDED', 'EMBEDDING_ERROR', 'LLM_UNAVAILABLE'];
  return retryableCodes.includes(error.code);
}

export function is429Error(error) {
  return error?.code === 429 ||
         error?.status === 429 ||
         error?.message?.includes('429') ||
         error?.message?.includes('RESOURCE_EXHAUSTED') ||
         error?.message?.includes('Too Many Requests');
}
