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

const seededSessions = new Set();

export function createSession(sessionId) {
  const id = sessionId || uuidv4();
  const session = {
    id,
    createdAt: new Date(),
    lastAccessed: new Date(),
    documents: [],
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

/**
 * On session start:
 * - If collection is NEW → seed from global (paginated, 300/batch)
 * - If collection EXISTS → skip seed, reconstruct in-memory doc list from Chroma
 */
export async function initSessionWithGlobalDocs(sessionId) {
  console.log(`🔑 Session init: ${sessionId}`);
  if (seededSessions.has(sessionId)) {
    console.log(`[session] Already seeded ${sessionId}, skipping`);
    return;
  }

  try {
    const globalCollection = await getGlobalCollection();
    const { collection: sessionCollection, isNew } = await getSessionCollection(sessionId);

    if (!isNew) {
      console.log(`♻️  Session exists, reconstructing document list from Chroma...`);
      const session = getSession(sessionId);
      if (session && session.documents.length === 0) {
        const docs = await listDocuments(sessionCollection);
        docs.forEach(doc => {
          session.documents.push({
            id: doc.document_id,
            filename: doc.filename,
            fileSize: null,
            pageCount: doc.page_count || null,
            chunkCount: doc.chunk_count,
            sourceType: doc.source_type,
            uploadTimestamp: doc.upload_timestamp
          });
        });
        console.log(`✅ Reconstructed ${docs.length} document(s) for session ${sessionId}`);
      }
      seededSessions.add(sessionId);
      return;
    }

    console.log(`🌱 New session — seeding from global collection...`);

    const BATCH_SIZE = 300;
    let offset = 0;
    const allIds = [], allEmbeddings = [], allDocuments = [], allMetadatas = [];

    while (true) {
      const batch = await globalCollection.get({
        include: ['embeddings', 'documents', 'metadatas'],
        limit: BATCH_SIZE,
        offset
      });
      if (!batch.ids || batch.ids.length === 0) break;
      allIds.push(...batch.ids);
      allEmbeddings.push(...batch.embeddings);
      allDocuments.push(...batch.documents);
      allMetadatas.push(...batch.metadatas);
      if (batch.ids.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    if (allIds.length === 0) {
      console.log('⚠️  Global collection is empty — nothing to seed.');
      seededSessions.add(sessionId);
      return;
    }

    for (let i = 0; i < allIds.length; i += BATCH_SIZE) {
      await sessionCollection.add({
        ids: allIds.slice(i, i + BATCH_SIZE),
        embeddings: allEmbeddings.slice(i, i + BATCH_SIZE),
        documents: allDocuments.slice(i, i + BATCH_SIZE),
        metadatas: allMetadatas.slice(i, i + BATCH_SIZE).map(m => ({ ...m, source_type: 'global' }))
      });
      console.log(`  📦 Added batch ${Math.floor(i / BATCH_SIZE) + 1}: records ${i + 1}–${Math.min(i + BATCH_SIZE, allIds.length)}`);
    }

    console.log(`✅ Seeded ${allIds.length} vectors into session ${sessionId}`);
    seededSessions.add(sessionId);

    const session = getSession(sessionId);
    if (session) {
      const docsMap = new Map();
      allMetadatas.forEach(meta => {
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
  }
}

/**
 * Upsert a document into the session.
 * If a doc with the same id already exists, update it in place (no duplicate).
 * Supports partial updates — only provided fields overwrite existing values.
 */
export function addDocumentToSession(sessionId, documentInfo) {
  const session = getSession(sessionId);
  if (!session) return false;

  const existing = session.documents.find(d => d.id === documentInfo.id);

  if (existing) {
    // Upsert — update fields that were provided
    if (documentInfo.chunkCount  !== undefined) existing.chunkCount  = documentInfo.chunkCount;
    if (documentInfo.pageCount   !== undefined) existing.pageCount   = documentInfo.pageCount;
    if (documentInfo.fileSize    !== undefined) existing.fileSize    = documentInfo.fileSize;
    if (documentInfo.status      !== undefined) existing.status      = documentInfo.status;
    if (documentInfo.filename    !== undefined) existing.filename    = documentInfo.filename;
    session.lastAccessed = new Date();
    console.log(`[session] Updated doc ${documentInfo.id} — status=${existing.status}, chunks=${existing.chunkCount}`);
    return true;
  }

  // New doc — push
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
    status: doc.status ?? null     // pass through for frontend 'indexing' tag
  });

  return {
    sessionDocuments: session.documents
      .filter(d => d.sourceType === 'session_upload')
      .map(normalize),
    globalDocuments: session.documents
      .filter(d => d.sourceType === 'global')
      .map(normalize)
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
