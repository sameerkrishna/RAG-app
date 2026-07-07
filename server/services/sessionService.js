import { v4 as uuidv4 } from 'uuid';
import {
  getGlobalCollection,
  getCollection,
  listDocuments
} from './chromaService.js';

const DEFAULT_TIMEOUT_MINUTES = 60;
const sessions = new Map();
const MAX_PDFS_PER_SESSION = parseInt(process.env.MAX_PDFS_PER_SESSION) || 3;
const MAX_UPLOAD_SIZE_MB = parseInt(process.env.MAX_UPLOAD_SIZE_MB) || 5;

const seededSessions = new Set();

// ─── Global documents cache (populated once on first session init) ──────────
let globalDocumentsCache = [];
let globalDataInitialized = false;

export function getGlobalDocumentsCache() {
  return globalDocumentsCache;
}

export function createSession(sessionId) {
  const id = sessionId || uuidv4();
  const session = {
    id,
    createdAt: new Date(),
    lastAccessed: new Date(),
    documents: [],
    deletedDocumentIds: new Set(),
    timeoutMinutes: DEFAULT_TIMEOUT_MINUTES
  };
  sessions.set(id, session);
  return session;
}

export function getSession(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return null;
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
    if (existing) return existing;
    return createSession(sessionId);
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
  seededSessions.delete(sessionId);
}

// ─── Check if session is seeded ────────────────────────────────────────────
export function isSessionSeeded(sessionId) {
  return seededSessions.has(sessionId);
}

// ─── Notify SSE listeners ──────────────────────────────────────────────────
function notifySeedingComplete(sessionId) {
  if (global.seedingListeners && global.seedingListeners.has(`seeding:${sessionId}`)) {
    const eventKey = `seeding:${sessionId}`;
    const listeners = global.seedingListeners.get(eventKey) || [];
    listeners.forEach((response) => {
      try {
        response.write(`event: seeding_complete\ndata: ${JSON.stringify({ sessionId, seeded: true })}\n\n`);
        response.end();
      } catch (err) {
        console.error(`[notify] Failed to notify listener:`, err.message);
      }
    });
    global.seedingListeners.delete(eventKey);
    console.log(`[notify] Notified ${listeners.length} SSE listeners for session ${sessionId}`);
  }
}

/**
 * On session start:
 * - Reconstruct in-memory session doc list from the single collection
 *   by filtering on session_id metadata.
 * - No vector copying is performed — global docs are served from cache.
 */
export async function initSessionWithGlobalDocs(sessionId) {
  console.log(`🔑 Session init: ${sessionId}`);
  if (seededSessions.has(sessionId)) {
    console.log(`[session] Already seeded ${sessionId}, skipping`);
    notifySeedingComplete(sessionId);
    return;
  }

  try {
    const collection = await getGlobalCollection();

    // ── Lazy one-time global cache init (runs on first session init) ──
    if (!globalDataInitialized) {
      try {
        const globalDocs = await listDocuments(collection, { session_id: 'global' });
        globalDocumentsCache = globalDocs.map(doc => ({
          id: doc.document_id,
          filename: doc.filename,
          fileSize: null,
          pageCount: doc.page_count || null,
          chunkCount: doc.chunk_count,
          sourceType: 'global',
          uploadTimestamp: doc.upload_timestamp
        }));
        globalDataInitialized = true;
        console.log(`✅ Global documents cache loaded: ${globalDocumentsCache.length} document(s)`);
      } catch (err) {
        console.error('❌ Failed to initialize global data:', err.message);
      }
    }
    const session = getSession(sessionId);

    // Reconstruct session-specific docs (user uploads) from the collection
    if (session && session.documents.length === 0) {
      const docs = await listDocuments(collection, { session_id: sessionId });
      docs.forEach(doc => {
        if (!session.documents.find(d => d.id === doc.document_id)) {
          session.documents.push({
            id: doc.document_id,
            filename: doc.filename,
            fileSize: null,
            pageCount: doc.page_count || null,
            chunkCount: doc.chunk_count,
            sourceType: 'session_upload',
            uploadTimestamp: doc.upload_timestamp,
            status: 'ready'
          });
        }
      });
      if (docs.length > 0) {
        console.log(`♻️  Reconstructed ${docs.length} session document(s) for ${sessionId}`);
      }
    }
    seededSessions.add(sessionId);
    console.log(`✅ Session ${sessionId} ready (no vector copying needed)`);
    notifySeedingComplete(sessionId);

  } catch (error) {
    console.error(`❌ Failed to init session ${sessionId}:`, error.message);
    // Still notify listeners so they don't hang forever
    notifySeedingComplete(sessionId);
  }
}

// ─── Document management ────────────────────────────────────────────────────
export function addDocumentToSession(sessionId, documentInfo) {
  const session = getSession(sessionId);
  if (!session) return false;

  const existing = session.documents.find(d => d.id === documentInfo.id);

  if (existing) {
    if (documentInfo.chunkCount !== undefined) existing.chunkCount = documentInfo.chunkCount;
    if (documentInfo.pageCount !== undefined) existing.pageCount = documentInfo.pageCount;
    if (documentInfo.fileSize !== undefined) existing.fileSize = documentInfo.fileSize;
    if (documentInfo.status !== undefined) existing.status = documentInfo.status;
    if (documentInfo.filename !== undefined) existing.filename = documentInfo.filename;
    session.lastAccessed = new Date();
    console.log(`[session] Updated doc ${documentInfo.id} — status=${existing.status}, chunks=${existing.chunkCount}`);
    return true;
  }

  session.documents.push({
    id: documentInfo.id,
    filename: documentInfo.filename,
    fileSize: documentInfo.fileSize,
    pageCount: documentInfo.pageCount,
    uploadTimestamp: new Date(),
    chunkCount: documentInfo.chunkCount ?? 0,
    sourceType: 'session_upload',
    status: documentInfo.status ?? 'indexing'
  });
  session.lastAccessed = new Date();
  console.log(`[session] Added doc ${documentInfo.id} — status=${documentInfo.status ?? 'indexing'}`);
  return true;
}

export function canAcceptUpload(sessionId) {
  const session = getSession(sessionId);
  if (!session) return { canUpload: false, reason: 'Session not found' };
  const uploadedCount = session.documents.filter(d => d.sourceType === 'session_upload').length;
  if (uploadedCount >= MAX_PDFS_PER_SESSION) {
    return { canUpload: false, reason: `Maximum ${MAX_PDFS_PER_SESSION} PDFs per session` };
  }
  return { canUpload: true };
}

export function validateUpload(sessionId, file, filename) {
  const session = getSession(sessionId);
  const errors = [];

  if (file.size > MAX_UPLOAD_SIZE_MB * 1024 * 1024) {
    errors.push(`File exceeds ${MAX_UPLOAD_SIZE_MB}MB limit`);
  }

  const uploadedCount = session
    ? session.documents.filter(d => d.sourceType === 'session_upload').length
    : 0;

  if (uploadedCount >= MAX_PDFS_PER_SESSION) {
    errors.push(`Maximum ${MAX_PDFS_PER_SESSION} PDFs per session`);
  }

  if (session && session.documents.some(d => d.filename === filename)) {
    errors.push(`File "${filename}" already exists in this session`);
  }

  return {
    isValid: errors.length === 0,
    errors,
    isLargeFile: file.size > (MAX_UPLOAD_SIZE_MB * 1024 * 1024 * 0.6)
  };
}

export function removeDocumentFromSession(sessionId, documentId) {
  const session = getSession(sessionId);
  if (!session) return false;
  const idx = session.documents.findIndex(d => d.id === documentId);
  if (idx >= 0) {
    session.documents.splice(idx, 1);
    session.deletedDocumentIds.add(documentId);
    session.lastAccessed = new Date();
    console.log(`[session] Removed doc ${documentId}, added to deletedDocumentIds`);
    return true;
  }
  return false;
}

export function getDeletedDocumentIds(sessionId) {
  const session = getSession(sessionId);
  return session?.deletedDocumentIds ?? new Set();
}

export function getSessionDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) return [];
  return session.documents;
}

export function getAllDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) return { sessionDocuments: [], globalDocuments: [] };

  const normalize = (doc) => ({
    document_id: doc.id,
    filename: doc.filename,
    chunk_count: doc.chunkCount ?? 0,
    page_count: doc.pageCount ?? 0,
    upload_timestamp: doc.uploadTimestamp || null,
    source_type: doc.sourceType === 'session_upload' ? 'session_upload' : 'seed',
    fileSize: doc.fileSize || null,
    status: doc.status ?? null
  });

  return {
    sessionDocuments: session.documents
      .filter(d => d.sourceType === 'session_upload')
      .map(normalize),
    globalDocuments: globalDocumentsCache
      .map(normalize)
  };
}

export function getSessionStats(sessionId) {
  const session = getSession(sessionId);
  if (!session) return null;
  return {
    id: session.id,
    documentCount: session.documents.length + globalDocumentsCache.length,
    createdAt: session.createdAt,
    lastAccessed: session.lastAccessed,
    totalSize: session.documents.reduce((sum, d) => sum + (d.fileSize || 0), 0),
    totalChunks: session.documents.reduce((sum, d) => sum + (d.chunkCount || 0), 0)
      + globalDocumentsCache.reduce((sum, d) => sum + (d.chunkCount || 0), 0)
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
      seededSessions.delete(id);
      cleaned++;
    }
  }
  return cleaned;
}