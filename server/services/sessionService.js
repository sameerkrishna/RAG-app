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
 * On session start: seed the session collection from global.
 * - getSessionCollection() handles getCollection vs createCollection:
 *     new UUID  → collection not found on Chroma → createCollection → needs seeding
 *     server restart, same tab → collection found → reuse → skip seeding (count check)
 * - Both get and add are batched at 300 to respect Chroma Cloud free tier quotas.
 */
export async function initSessionWithGlobalDocs(sessionId) {
  console.log("In the init sessionwith gobaldocs function");
  if (seededSessions.has(sessionId)) return;

  try {
    console.log(`🌱 Seeding session ${sessionId} from global collection...`);

    const globalCollection = await getGlobalCollection();
    const sessionCollection = await getSessionCollection(sessionId);

    // Paginate global fetch — Chroma Cloud hard cap is 300/call
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

    // Skip if session collection already fully seeded (server restart, same tab)
    const existingCount = await sessionCollection.count();
    if (existingCount >= allIds.length) {
      console.log(`✅ Session ${sessionId} already fully seeded (${existingCount} vectors). Skipping.`);
      seededSessions.add(sessionId);
      return;
    }

    // Add in batches of 300 — Chroma Cloud also caps add() at 300 records/call
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

    // Register global docs in session document list for UI
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
