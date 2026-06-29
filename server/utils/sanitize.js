import path from 'path';
import { ValidationError } from './errors.js';

const DANGEROUS_PATTERNS = /[<>:"|?*\x00-\x1f]/g;
const PATH_TRAVERSAL = /\.\./g;

export function sanitizeFilename(filename) {
  if (!filename || typeof filename !== 'string') {
    throw new ValidationError('Invalid filename');
  }

  // Remove path components and get basename
  const basename = path.basename(filename);

  // Remove dangerous characters
  let sanitized = basename.replace(DANGEROUS_PATTERNS, '_');

  // Remove path traversal attempts
  sanitized = sanitized.replace(PATH_TRAVERSAL, '');

  // Trim whitespace and limit length
  sanitized = sanitized.trim().slice(0, 255);

  if (!sanitized) {
    throw new ValidationError('Invalid filename after sanitization');
  }

  return sanitized;
}

export function validatePDFFile(file) {
  if (!file) {
    throw new ValidationError('No file provided');
  }

  // Check MIME type
  const validMimeTypes = ['application/pdf'];
  if (!validMimeTypes.includes(file.mimetype)) {
    throw new ValidationError('Only PDF files are accepted');
  }

  // Check extension
  const ext = path.extname(file.originalname || '').toLowerCase();
  if (ext !== '.pdf') {
    throw new ValidationError('File must have .pdf extension');
  }

  return true;
}

export function validateFileSize(sizeBytes, maxSizeMB) {
  const maxBytes = maxSizeMB * 1024 * 1024;
  if (sizeBytes > maxBytes) {
    throw new ValidationError(`File exceeds maximum size of ${maxSizeMB}MB`);
  }
  return true;
}

export function sanitizeInput(input, maxLength = 10000) {
  if (!input || typeof input !== 'string') {
    return '';
  }
  return input.trim().slice(0, maxLength);
}

export function validateDocumentId(id) {
  if (!id || typeof id !== 'string') {
    throw new ValidationError('Invalid document ID');
  }
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!uuidRegex.test(id)) {
    throw new ValidationError('Invalid document ID format');
  }
  return true;
}

export function extractTextFromPDFBuffer(buffer) {
  // This will be used with pdf-parse
  return buffer;
}
