import { v4 as uuidv4 } from 'uuid';
import {
  getGlobalCollection,
  getSessionCollection,
  listDocuments,
  addVectors
} from './chromaService.js';

const DEFAULT_TIMEOUT_MINUTES = 60;
const sessions = new Map();
const MAX_PDFS_PER_SESSION = parseInt(process.env.MAX_PDFS_PER_SESSION) || 3;
const MAX_UPLOAD_SIZE_MB = parseInt(process.env.MAX_UPLOAD_SIZE_MB) || 5;

// Track which sessions have been seeded from global collection
const seededSessions = new Set();

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

/**
 * On session start: copy all global collection vectors into the session collection.
 * This only runs ONCE per sessionId — skipped on subsequent calls.
 */
export async function initSessionWithGlobalDocs(sessionId) {
  if (seededSessions.has(sessionId)) return; // already done

  try {
    console.log(`🌱 Seeding session ${sessionId} from global collection...`);

    const globalCollection = await getGlobalCollection();
    const sessionCollection = await getSessionCollection(sessionId);

    // Fetch ALL vectors from global (documents + embeddings + metadatas + ids)
    const allGlobal = await globalCollection.get({
      include: ['embeddings', 'documents', 'metadatas']
    });

    if (!allGlobal.ids || allGlobal.ids.length === 0) {
      console.log('⚠️  Global collection is empty — nothing to seed.');
      seededSessions.add(sessionId);
      return;
    }

    // Check which ids already exist in session (avoid duplicate inserts on reconnect)
    const existing = await sessionCollection.get({ ids: allGlobal.ids });
    const existingIds = new Set(existing.ids || []);
    const newIds = allGlobal.ids.filter(id => !existingIds.has(id));

    if (newIds.length === 0) {
      console.log(`✅ Session ${sessionId} already has all global vectors.`);
      seededSessions.add(sessionId);
      return;
    }

    const newIndices = allGlobal.ids
      .map((id, i) => i)
      .filter(i => newIds.includes(allGlobal.ids[i]));

    await sessionCollection.add({
      ids: newIndices.map(i => allGlobal.ids[i]),
      embeddings: newIndices.map(i => allGlobal.embeddings[i]),
      documents: newIndices.map(i => allGlobal.documents[i]),
      metadatas: newIndices.map(i => ({
        ...allGlobal.metadatas[i],
        source_type: 'global'  // preserve so UI can still show Seed badge
      }))
    });

    console.log(`✅ Seeded ${newIds.length} vectors into session ${sessionId}`);
    seededSessions.add(sessionId);

    // Register global docs in session document list for listing in UI
    const session = getSession(sessionId);
    if (session) {
      const docsMap = new Map();
      allGlobal.metadatas.forEach(meta => {
        if (!docsMap.has(meta.document_id)) {
          docsMap.set(meta.document_id, {
            id: meta.document_id,
            filename: meta.filename,
            fileSize: null,
            pageCount: meta.total_pages || null,
            chunkCount: 0,
            sourceType: 'global',
            uploadTimestamp: meta.upload_timestamp
          });
        }
        docsMap.get(meta.document_id).chunkCount++;
      });

      for (const doc of docsMap.values()) {
        if (!session.documents.some(d => d.id === doc.id)) {
          session.documents.push(doc);
        }
      }
    }

  } catch (error) {
    console.error(`❌ Failed to seed session ${sessionId}:`, error.message);
    // Non-fatal — session can still work without global docs
  }
}

export function addDocumentToSession(sessionId, documentInfo) {
  const session = getSession(sessionId);
  if (!session) return false;
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
  if (!session) return { canUpload: false, reason: 'Session not found' };

  // Count only session-uploaded docs (not global seeds)
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
    session.lastAccessed = new Date();
    return true;
  }
  return false;
}

export function getSessionDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) return [];
  return session.documents;
}

export async function getAllDocuments(sessionId) {
  const sessionDocs = getSessionDocuments(sessionId);
  return {
    // Split for UI — global seeded docs vs user-uploaded docs
    sessionDocuments: sessionDocs.filter(d => d.sourceType === 'session_upload'),
    globalDocuments: sessionDocs.filter(d => d.sourceType === 'global')
  };
}

export function getSessionStats(sessionId) {
  const session = getSession(sessionId);
  if (!session) return null;
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
      seededSessions.delete(id);
      cleaned++;
    }
  }
  return cleaned;
}