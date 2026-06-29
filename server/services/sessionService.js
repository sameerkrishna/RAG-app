import { v4 as uuidv4 } from 'uuid';
import { getGlobalCollection, getSessionCollection, listDocuments } from './chromaService.js';

const DEFAULT_TIMEOUT_MINUTES = 60;
const sessions = new Map();
const MAX_PDFS_PER_SESSION = parseInt(process.env.MAX_PDFS_PER_SESSION) || 3;
const MAX_UPLOAD_SIZE_MB = parseInt(process.env.MAX_UPLOAD_SIZE_MB) || 5;

export function createSession() {
  const sessionId = uuidv4();
  const session = {
    id: sessionId,
    createdAt: new Date(),
    lastAccessed: new Date(),
    documents: [],
    timeoutMinutes: DEFAULT_TIMEOUT_MINUTES
  };

  sessions.set(sessionId, session);
  return session;
}

export function getSession(sessionId) {
  const session = sessions.get(sessionId);

  if (!session) {
    return null;
  }

  if (isSessionExpired(session)) {
    deleteSession(sessionId);
    return null;
  }

  session.lastAccessed = new Date();
  return session;
}

export function getOrCreateSession(sessionId) {
  if (sessionId) {
    const existing = getSession(sessionId);
    if (existing) {
      return existing;
    }
  }

  return createSession();
}

export function isSessionExpired(session) {
  const now = Date.now();
  const lastAccessed = new Date(session.lastAccessed).getTime();
  const timeoutMs = session.timeoutMinutes * 60 * 1000;
  return (now - lastAccessed) > timeoutMs;
}

export function deleteSession(sessionId) {
  sessions.delete(sessionId);
}

export function addDocumentToSession(sessionId, documentInfo) {
  const session = getSession(sessionId);
  if (!session) {
    return false;
  }

  session.documents.push({
    id: documentInfo.id,
    filename: documentInfo.filename,
    fileSize: documentInfo.fileSize,
    pageCount: documentInfo.pageCount,
    uploadTimestamp: new Date(),
    chunkCount: documentInfo.chunkCount,
    sourceType: 'session_upload'
  });

  session.lastAccessed = new Date();
  return true;
}

export function canAcceptUpload(sessionId) {
  const session = getSession(sessionId);
  if (!session) {
    return { canUpload: false, reason: 'Session not found' };
  }

  if (session.documents.length >= MAX_PDFS_PER_SESSION) {
    return { canUpload: false, reason: `Maximum ${MAX_PDFS_PER_SESSION} PDFs per session` };
  }

  return { canUpload: true };
}

export function validateUpload(sessionId, file, filename) {
  const session = getSession(sessionId);
  const errors = [];

  // Check file size
  if (file.size > MAX_UPLOAD_SIZE_MB * 1024 * 1024) {
    errors.push(`File exceeds ${MAX_UPLOAD_SIZE_MB}MB limit`);
  }

  // Check max PDFs
  if (session && session.documents.length >= MAX_PDFS_PER_SESSION) {
    errors.push(`Maximum ${MAX_PDFS_PER_SESSION} PDFs per session`);
  }

  // Check for duplicate filename
  if (session && session.documents.some(d => d.filename === filename)) {
    errors.push(`File "${filename}" already exists in this session`);
  }

  return {
    isValid: errors.length === 0,
    errors,
    isLargeFile: file.size > (MAX_UPLOAD_SIZE_MB * 1024 * 1024 * 0.6) // 60% of max
  };
}

export function removeDocumentFromSession(sessionId, documentId) {
  const session = getSession(sessionId);
  if (!session) {
    return false;
  }

  const idx = session.documents.findIndex(d => d.id === documentId);
  if (idx >= 0) {
    session.documents.splice(idx, 1);
    session.lastAccessed = new Date();
    return true;
  }

  return false;
}

export function getSessionDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) {
    return [];
  }
  return session.documents;
}

export async function getAllDocuments(sessionId) {
  const sessionDocs = getSessionDocuments(sessionId);
  const globalCollection = await getGlobalCollection();
  const globalDocs = await listDocuments(globalCollection);

  return {
    sessionDocuments: sessionDocs,
    globalDocuments: globalDocs
  };
}

export function getSessionStats(sessionId) {
  const session = getSession(sessionId);
  if (!session) {
    return null;
  }

  return {
    id: session.id,
    documentCount: session.documents.length,
    createdAt: session.createdAt,
    lastAccessed: session.lastAccessed,
    totalSize: session.documents.reduce((sum, d) => sum + (d.fileSize || 0), 0),
    totalChunks: session.documents.reduce((sum, d) => sum + (d.chunkCount || 0), 0)
  };
}

export function listActiveSessions() {
  return Array.from(sessions.values()).filter(s => !isSessionExpired(s));
}

export function cleanupExpiredSessions() {
  let cleaned = 0;
  for (const [id, session] of sessions) {
    if (isSessionExpired(session)) {
      sessions.delete(id);
      cleaned++;
    }
  }
  return cleaned;
}
