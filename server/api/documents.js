import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { v4 as uuidv4 } from 'uuid';
import { createHash } from 'crypto';
import pdf from 'pdf-parse';
import { fileURLToPath } from 'url';
import { sanitizeFilename } from '../utils/sanitize.js';
import {
  CorruptedPDFError,
  InvalidFileTypeError,
} from '../utils/errors.js';
import { getCollection, addVectors, deleteDocumentVectors } from '../services/chromaService.js';
import { chunkText, cleanText } from '../utils/chunker.js';
import { embedSingleBatchGroup } from '../services/embeddingService.js';
import {
  getOrCreateSession,
  addDocumentToSession,
  removeDocumentFromSession,
  getAllDocuments,
  initSessionWithGlobalDocs,
  isSessionSeeded
} from '../services/sessionService.js';
import { clearMemory } from '../services/memoryService.js';
import { uploadPdfToStorage, deletePdfFromStorage, getPdfUrlFromStorage, getPdfStreamFromStorage } from '../services/blobService.js';

const router = Router();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const uploadDir = '/tmp/uploads';
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Seed documents directory - works in both dev and serverless
// In dev: server/api/../../seed_documents
// In serverless: netlify/functions/../../seed_documents (copied to dist)
let seedDir = path.resolve(__dirname, '../../seed_documents');
if (!fs.existsSync(seedDir)) {
  // Try alternative path for serverless deployment
  seedDir = path.resolve(process.cwd(), 'seed_documents');
}
if (!fs.existsSync(seedDir)) {
  // Try dist folder for deployed static files
  seedDir = path.resolve(process.cwd(), 'dist/seed_documents');
}

// ─── SSE event helper ──────────────────────────────────────────────────────
function sseEvent(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

const storage = multer.memoryStorage();

const upload = multer({
  storage,
  limits: { fileSize: parseInt(process.env.MAX_UPLOAD_SIZE_MB || '5') * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'application/pdf' && path.extname(file.originalname).toLowerCase() === '.pdf') {
      cb(null, true);
    } else {
      cb(new InvalidFileTypeError());
    }
  }
});

function contentDisposition(displayName) {
  const encoded = encodeURIComponent(displayName)
    .replace(/'/g, '%27')
    .replace(/\(/g, '%28')
    .replace(/\)/g, '%29');
  return `inline; filename="document.pdf"; filename*=UTF-8''${encoded}`;
}

/**
 * Join pdf.js text-content items into a single string using each item's
 * x-position (transform[4]) and width to decide whether a space belongs
 * between two items, instead of always joining with a single space.
 *
 * This avoids two common artifacts from naive `.join(' ')`:
 *  - words split across adjacent text runs getting a phantom space
 *    inserted in the middle (e.g. "Sav ings")
 *  - adjacent words with no space in the PDF's internal runs getting
 *    glued together (e.g. "the report" -> "thereport")
 *
 * Empty-string items are pdf.js's signal for a line break, which we
 * convert to a newline so paragraph structure isn't lost.
 */
function joinTextItems(items) {
  let out = '';
  let prevItem = null;

  for (const item of items) {
    const str = item.str;
    if (str === undefined) { prevItem = item; continue; }

    if (str === '') {
      // pdf.js emits empty items to signal line breaks
      if (!/\n$/.test(out)) out += '\n';
      prevItem = null;
      continue;
    }

    if (prevItem && prevItem.str) {
      const prevEnd = prevItem.transform[4] + (prevItem.width || 0);
      const curStart = item.transform[4];
      const gap = curStart - prevEnd;
      const fontH = Math.abs(item.transform[3]) || 10;
      const spaceThreshold = fontH * 0.25;

      const alreadySpaced = /\s$/.test(out) || /^\s/.test(str);
      if (!alreadySpaced && gap > spaceThreshold) {
        out += ' ';
      }
      // else: items are touching/overlapping -> same word, no space inserted
    }

    out += str;
    prevItem = item;
  }

  return out;
}

async function parsePDFWithBoundaryMap(buffer) {
  try {
    const pages = [];
    await pdf(buffer, {
      pagerender: (pageData) => {
        return pageData.getTextContent().then(tc => {
          const pageText = joinTextItems(tc.items);
          pages.push(pageText);
          return pageText;
        });
      }
    });

    if (pages.length === 0 || pages.every(p => !p.trim())) {
      const full = await pdf(buffer);
      pages.push(full.text);
    }

    const totalPages = pages.length;
    const cleanedPages = pages.map(p => cleanText(p));
    const pageMap = [];
    let charPos = 0;

    for (let i = 0; i < cleanedPages.length; i++) {
      pageMap.push({ page: i + 1, start: charPos, end: charPos + cleanedPages[i].length });
      charPos += cleanedPages[i].length + 1;
    }

    const fullText = cleanedPages.join('\n');
    return { fullText, pageMap, totalPages };
  } catch (error) {
    console.error('PDF parsing error:', error);
    throw new CorruptedPDFError();
  }
}

/**
 * Given a chunk's [charStart, charEnd) range, find which page(s) it
 * overlaps. Returns the majority page (most overlapping chars, used
 * for `page_number` for backward compatibility) plus the true start/end
 * pages so chunks spanning a page break aren't silently mislabeled with
 * just the first page.
 */
function getPageRange(charStart, charEnd, pageMap) {
  let startPage = null;
  let endPage = null;
  let bestPage = null;
  let maxOverlap = -1;

  for (const entry of pageMap) {
    const overlapStart = Math.max(charStart, entry.start);
    const overlapEnd = Math.min(charEnd, entry.end);
    const overlap = overlapEnd - overlapStart;
    if (overlap <= 0) continue;

    if (startPage === null) startPage = entry.page;
    endPage = entry.page;

    if (overlap > maxOverlap) {
      maxOverlap = overlap;
      bestPage = entry.page;
    }
  }

  if (startPage === null) {
    const lastPage = pageMap[pageMap.length - 1]?.page || 1;
    return { page: lastPage, pageStart: lastPage, pageEnd: lastPage };
  }

  return { page: bestPage, pageStart: startPage, pageEnd: endPage };
}

// ─── Upload handler ──────────────────────────────────────────────────────────
export async function handleUpload(req, res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const BATCH_SIZE = parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 10;
  const PARALLEL_CALLS = parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 10;
  const GROUP_WAIT_MS = parseInt(process.env.EMBEDDING_GROUP_WAIT_MS) || 1;

  try {
    const file = req.file;
    if (!file) throw new InvalidFileTypeError();

    const sessionId = req.headers['x-session-id'] || req.body.sessionId || uuidv4();
    const session = getOrCreateSession(sessionId);
    const maxPDFs = parseInt(process.env.MAX_PDFS_PER_SESSION || '3');
    const cleanFilename = sanitizeFilename(file.originalname);

    const uploadedCount = session.documents.filter(d => d.sourceType === 'session_upload').length;
    if (uploadedCount >= maxPDFs) {
      sseEvent(res, 'error', { message: `Maximum ${maxPDFs} uploads reached`, code: 'TOO_MANY_PDFS' });
      return res.end();
    }

    if (session.documents.some(d => d.filename === cleanFilename)) {
      sseEvent(res, 'error', { message: `"${cleanFilename}" already uploaded`, code: 'DUPLICATE_FILE' });
      return res.end();
    }

    console.log(`[upload] [${sessionId}] Phase 1 — parsing ${cleanFilename} (${file.size} bytes)`);
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.buffer);

    if (!fullText || fullText.trim().length < 50) {
      sseEvent(res, 'error', { message: 'No extractable text — PDF may be scanned or image-only', code: 'EMPTY_PDF' });
      return res.end();
    }

    const documentId = uuidv4();

    let blobUrl = null;
    try {
      const blob = await uploadPdfToStorage(sessionId, documentId, cleanFilename, file.buffer);
      // Save downloadUrl (without forced download) if available for private blobs, otherwise fallback to url
      if (blob.downloadUrl) {
        blobUrl = blob.downloadUrl.replace('?download=1&', '?').replace('?download=1', '');
      } else {
        blobUrl = blob.url;
      }
    } catch (err) {
      sseEvent(res, 'error', { message: 'Failed to upload PDF to cloud storage', code: 'UPLOAD_ERROR' });
      return res.end();
    }

    const rawChunks = chunkText(fullText);

    if (rawChunks.length === 0) {
      sseEvent(res, 'error', { message: 'No content could be extracted from PDF', code: 'EMPTY_PDF' });
      return res.end();
    }

    const chunks = rawChunks.map((chunk, idx) => {
      const { page, pageStart, pageEnd } = getPageRange(chunk.charStart, chunk.charEnd, pageMap);
      return {
        text: chunk.text,
        metadata: {
          document_id: documentId,
          filename: cleanFilename,
          chunk_id: createHash('md5').update(`${cleanFilename}::${chunk.text}`).digest('hex').slice(0, 16),
          chunk_index: idx,
          total_chunks: rawChunks.length,
          page_number: page,       // majority page — kept for backward compatibility
          page_start: pageStart,   // new: first page this chunk overlaps
          page_end: pageEnd,       // new: last page this chunk overlaps
          total_pages: totalPages,
          source_type: 'session_upload',
          session_id: sessionId,
          upload_timestamp: new Date().toISOString(),
          char_start: chunk.charStart,
          char_end: chunk.charEnd,
          token_count: chunk.tokenCount
        }
      };
    });

    const totalChunks = chunks.length;
    const totalBatches = Math.ceil(totalChunks / BATCH_SIZE);
    const totalSets = Math.ceil(totalBatches / PARALLEL_CALLS);

    console.log(`[upload] [${sessionId}] ${totalChunks} chunks → ${totalBatches} API calls → ${totalSets} sets of ${PARALLEL_CALLS} parallel`);

    sseEvent(res, 'upload_complete', {
      documentId, filename: cleanFilename, fileSize: file.size,
      pageCount: totalPages, totalChunks, totalBatches, totalSets
    });

    addDocumentToSession(sessionId, {
      id: documentId, filename: cleanFilename, fileSize: file.size,
      pageCount: totalPages, chunkCount: 0, status: 'indexing', url: blobUrl
    });

    console.log(`[upload] [${sessionId}] Phase 1 done — ${cleanFilename} added to session as indexing`);

    const { collection } = await getCollection();
    let processedChunks = 0;
    const allEmbeddings = [];

    const batches = [];
    for (let i = 0; i < chunks.length; i += BATCH_SIZE) batches.push(chunks.slice(i, i + BATCH_SIZE));

    const sets = [];
    for (let i = 0; i < batches.length; i += PARALLEL_CALLS) sets.push(batches.slice(i, i + PARALLEL_CALLS));

    console.log(`[upload] [${sessionId}] Phase 2 start — ${sets.length} sets`);

    for (let setIdx = 0; setIdx < sets.length; setIdx++) {
      const isLastSet = setIdx === sets.length - 1;
      const currentSet = sets[setIdx];
      const setChunkCount = currentSet.reduce((acc, b) => acc + b.length, 0);

      console.log(`[upload] [${sessionId}] Set ${setIdx + 1}/${sets.length} — embedding ${currentSet.length} batch call(s) (${setChunkCount} chunks) in parallel`);

      const embedResults = await Promise.allSettled(
        currentSet.map(batch => embedSingleBatchGroup(batch.map(c => c.text)))
      );

      const setEmbeddings = [];
      embedResults.forEach((result, batchIdx) => {
        const batch = currentSet[batchIdx];
        if (result.status === 'fulfilled') {
          result.value.forEach((vector, chunkIdx) => {
            setEmbeddings.push({
              id: batch[chunkIdx].metadata.chunk_id,
              embedding: vector,
              metadata: batch[chunkIdx].metadata,
              text: batch[chunkIdx].text
            });
          });
          console.log(`[upload] [${sessionId}]   Batch ${setIdx * PARALLEL_CALLS + batchIdx + 1} embedded OK (${batch.length} chunks)`);
        } else {
          console.error(`[upload] [${sessionId}]   Batch ${setIdx * PARALLEL_CALLS + batchIdx + 1} FAILED:`, result.reason?.message);
        }
      });

      processedChunks += setEmbeddings.length;
      allEmbeddings.push(...setEmbeddings);

      console.log(`[upload] [${sessionId}] Set ${setIdx + 1} embedded — ${processedChunks}/${totalChunks} chunks so far`);

      if (!isLastSet) {
        console.log(`[upload] [${sessionId}] Starting ${GROUP_WAIT_MS / 1000}s timer + Chroma write concurrently for set ${setIdx + 1}`);
        const timer = new Promise(r => setTimeout(r, GROUP_WAIT_MS));
        const chromaWrite = addVectors(
          collection,
          setEmbeddings.map(e => ({ text: e.text, metadata: e.metadata })),
          setEmbeddings.map(e => e.embedding),
          setEmbeddings.map(e => e.id)
        ).then(() => console.log(`[upload] [${sessionId}] Chroma write done for set ${setIdx + 1} (${setEmbeddings.length} vectors)`))
          .catch(err => console.error(`[upload] [${sessionId}] Chroma write FAILED for set ${setIdx + 1}:`, err.message));

        sseEvent(res, 'embedding_progress', {
          processedChunks, totalChunks,
          setIndex: setIdx + 1, totalSets,
          waitingMs: GROUP_WAIT_MS, chromaWriteComplete: false
        });

        await Promise.all([timer, chromaWrite]);
        console.log(`[upload] [${sessionId}] Timer + Chroma both done for set ${setIdx + 1}, proceeding to set ${setIdx + 2}`);

      } else {
        console.log(`[upload] [${sessionId}] Last set ${setIdx + 1} — awaiting Chroma write directly`);
        await addVectors(
          collection,
          setEmbeddings.map(e => ({ text: e.text, metadata: e.metadata })),
          setEmbeddings.map(e => e.embedding),
          setEmbeddings.map(e => e.id)
        );
        console.log(`[upload] [${sessionId}] Chroma write complete for last set (${setEmbeddings.length} vectors)`);

        sseEvent(res, 'embedding_progress', {
          processedChunks, totalChunks,
          setIndex: setIdx + 1, totalSets,
          waitingMs: 0, chromaWriteComplete: true
        });
      }
    }

    addDocumentToSession(sessionId, {
      id: documentId, filename: cleanFilename, fileSize: file.size,
      pageCount: totalPages, chunkCount: allEmbeddings.length, status: 'ready'
    });

    console.log(`[upload] [${sessionId}] ✅ Done — ${allEmbeddings.length} vectors in Chroma for ${cleanFilename}`);

    sseEvent(res, 'done', {
      document: {
        id: documentId, filename: cleanFilename, fileSize: file.size,
        pageCount: totalPages, chunkCount: allEmbeddings.length,
        uploadTimestamp: new Date().toISOString()
      },
      sessionId
    });

    res.end();

  } catch (error) {
    console.error('[upload] Unhandled error:', error);
    sseEvent(res, 'error', { message: error.message || 'Upload failed', code: error.code || 'UPLOAD_ERROR' });
    res.end();
  }
}

// ─── SSE: Seeding status stream ─────────────────────────────────────────────
export async function seedingStatusHandler(req, res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  if (!sessionId) {
    sseEvent(res, 'error', { message: 'Missing session ID', code: 'MISSING_SESSION' });
    res.end();
    return;
  }

  console.log(`[seeding-status] Client connected for session ${sessionId}`);

  // Check if session is already seeded
  const seeded = isSessionSeeded(sessionId);
  if (seeded) {
    console.log(`[seeding-status] Session ${sessionId} already seeded – returning immediately`);
    sseEvent(res, 'seeding_complete', { sessionId, seeded: true });
    res.end();
    return;
  }

  // Create a listener for this session
  const eventKey = `seeding:${sessionId}`;

  // Store the listener so we can emit when seeding completes
  if (!global.seedingListeners) {
    global.seedingListeners = new Map();
  }
  if (!global.seedingListeners.has(eventKey)) {
    global.seedingListeners.set(eventKey, []);
  }
  global.seedingListeners.get(eventKey).push(res);

  // Clean up listener on client disconnect
  req.on('close', () => {
    const listeners = global.seedingListeners.get(eventKey) || [];
    const idx = listeners.indexOf(res);
    if (idx >= 0) {
      listeners.splice(idx, 1);
      console.log(`[seeding-status] Client disconnected for ${sessionId}`);
    }
    if (listeners.length === 0) {
      global.seedingListeners.delete(eventKey);
    }
  });

  // Start seeding in the background (if not already running)
  try {
    console.log(`[seeding-status] Triggering seeding for ${sessionId}...`);
    await initSessionWithGlobalDocs(sessionId);
    // The seeding function will notify listeners when complete
  } catch (err) {
    console.error(`[seeding-status] Seeding failed for ${sessionId}:`, err.message);
    const listeners = global.seedingListeners.get(eventKey) || [];
    listeners.forEach((response) => {
      sseEvent(response, 'error', { message: err.message, code: 'SEED_FAILED' });
      response.end();
    });
    global.seedingListeners.delete(eventKey);
  }
}

// ─── List documents handler ──────────────────────────────────────────────────
export async function listDocumentsHandler(req, res) {
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;
  try {
    getOrCreateSession(sessionId);
    const documents = getAllDocuments(sessionId);
    res.json(documents);
  } catch (error) {
    console.error('List documents error:', error);
    res.status(500).json({ error: 'Failed to list documents', code: 'LIST_ERROR' });
  }
}

// ─── Delete document ─────────────────────────────────────────────────────────
export async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const filename = req.query.filename;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  try {
    let blobUrl = null;
    if (sessionId) {
      const session = getOrCreateSession(sessionId);
      const doc = session.documents.find(d => d.id === documentId);
      blobUrl = doc?.url;

      try {
        const { collection } = await getCollection();
        if (collection) {
          await deleteDocumentVectors(collection, documentId);
        }
      } catch (chromaErr) {
        console.warn(`[delete] Chroma delete failed for ${documentId}:`, chromaErr.message);
      }

      removeDocumentFromSession(sessionId, documentId);

      clearMemory(sessionId);
      console.log(`[delete] Cleared memory for session ${sessionId}`);
    }

    if (blobUrl) {
      try {
        await deletePdfFromStorage(sessionId, documentId, filename, blobUrl);
        console.log(`[delete] Removed file from Vercel Blob using known URL: ${filename}`);
      } catch (err) {
        console.warn(`[delete] Failed to remove from Vercel Blob: ${err.message}`);
      }
    } else if (filename) {
      // Fallback for older uploads that didn't save the URL
      try {
        await deletePdfFromStorage(sessionId, documentId, filename);
        console.log(`[delete] Removed file from Vercel Blob (fallback search): ${filename}`);
      } catch (err) {
        console.warn(`[delete] Failed to remove from Vercel Blob: ${err.message}`);
      }
    }

    res.json({ success: true, documentId });
  } catch (error) {
    console.error('Delete document error:', error);
    res.status(500).json({ error: 'Failed to delete document', code: 'DELETE_ERROR' });
  }
}

// ─── Get document file ──────────────────────────────────────────────────────
export async function getDocumentFile(req, res) {
  const filename = req.query.filename;
  const { documentId } = req.params;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  try {
    if (filename) {
      // 1. Try seed documents
      const seedPath = path.join(seedDir, filename);
      if (fs.existsSync(seedPath)) {
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', contentDisposition(filename));
        return fs.createReadStream(seedPath).pipe(res);
      }

      if (fs.existsSync(seedDir)) {
        const allPdfs = fs.readdirSync(seedDir).filter(f => f.endsWith('.pdf'));
        const match = allPdfs.find(f => f.includes(path.parse(filename).name));
        if (match) {
          const matchPath = path.join(seedDir, match);
          res.setHeader('Content-Type', 'application/pdf');
          res.setHeader('Content-Disposition', contentDisposition(match));
          return fs.createReadStream(matchPath).pipe(res);
        }
      }

      // 2. Try Vercel Blob Storage for session uploads
      if (sessionId && documentId) {
        try {
          const session = getOrCreateSession(sessionId);
          const doc = session.documents.find(d => d.id === documentId);

          let url = doc?.url;
          if (!url) {
            // Fallback for older uploads that didn't save the URL
            url = await getPdfUrlFromStorage(sessionId, documentId, filename);
          }

          // Use the official @vercel/blob SDK get() method for proxying private blobs
          const result = await getPdfStreamFromStorage(url);
          
          res.setHeader('Cache-Control', 'private, no-cache');
          res.setHeader('Content-Type', result.blob.contentType || 'application/pdf');
          res.setHeader('X-Content-Type-Options', 'nosniff');
          res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(filename)}"`);
          
          // Pipe the ReadableStream to Node.js Express response
          const { Readable } = require('stream');
          return Readable.fromWeb(result.stream).pipe(res);
        } catch (err) {
          console.warn(`[getDocumentFile] Blob redirect failed for ${filename}:`, err.message);
        }
      }
    }

    return res.status(404).json({ error: 'Document file not found', code: 'FILE_NOT_FOUND' });
  } catch (error) {
    console.error('Get document file error:', error);
    res.status(500).json({ error: 'Failed to retrieve document', code: 'RETRIEVE_ERROR' });
  }
}

// ─── Routes ──────────────────────────────────────────────────────────────────
router.post('/upload', upload.single('file'), handleUpload);
router.get('/', listDocumentsHandler);
router.get('/seeding-status', seedingStatusHandler);
router.delete('/:documentId', deleteDocument);
router.get('/:documentId/file', getDocumentFile);

export default router;