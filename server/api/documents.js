import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { v4 as uuidv4 } from 'uuid';
import { createHash } from 'crypto';
import pdf from 'pdf-parse';
import { fileURLToPath } from 'url';;
import { sanitizeFilename, validatePDFFile, validateFileSize } from '../utils/sanitize.js';
import {
  CorruptedPDFError,
  InvalidFileTypeError,
  FileTooLargeError,
  TooManyPDFsError,
  DuplicateFileError
} from '../utils/errors.js';
import { getSessionCollection, addVectors, deleteDocumentVectors } from '../services/chromaService.js';
import { chunkText, cleanText } from '../utils/chunker.js';
import { embedSingleBatchGroup } from '../services/embeddingService.js';
import {
  getOrCreateSession,
  canAcceptUpload,
  addDocumentToSession,
  removeDocumentFromSession,
  getAllDocuments
} from '../services/sessionService.js';
import { invalidateSessionCollectionCache } from '../services/retrievalService.js';
import { clearMemory } from '../services/memoryService.js';

const router = Router();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const uploadDir = '/tmp/uploads';
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const seedDir = path.resolve(__dirname, '../../seed_documents');

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => cb(null, sanitizeFilename(file.originalname))
});

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

async function parsePDFWithBoundaryMap(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);

    const pages = [];
    await pdf(buffer, {
      pagerender: (pageData) => {
        return pageData.getTextContent().then(tc => {
          const pageText = tc.items.map(i => i.str).join(' ');
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

function getPageNumber(charStart, pageMap) {
  for (const entry of pageMap) {
    if (charStart >= entry.start && charStart < entry.end) return entry.page;
  }
  return pageMap[pageMap.length - 1]?.page || 1;
}

function sseEvent(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

export async function handleUpload(req, res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const BATCH_SIZE     = parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 10;
  const PARALLEL_CALLS = parseInt(process.env.EMBEDDING_PARALLEL_CALLS)  || 10;
  const GROUP_WAIT_MS  = parseInt(process.env.EMBEDDING_GROUP_WAIT_MS)   || 10100;

  try {
    const file = req.file;
    if (!file) throw new InvalidFileTypeError();

    const sessionId     = req.headers['x-session-id'] || req.body.sessionId || uuidv4();
    const session       = getOrCreateSession(sessionId);
    const maxPDFs       = parseInt(process.env.MAX_PDFS_PER_SESSION || '3');
    const cleanFilename = sanitizeFilename(file.originalname);

    const uploadedCount = session.documents.filter(d => d.sourceType === 'session_upload').length;
    if (uploadedCount >= maxPDFs) {
      fs.unlinkSync(file.path);
      sseEvent(res, 'error', { message: `Maximum ${maxPDFs} uploads reached`, code: 'TOO_MANY_PDFS' });
      return res.end();
    }

    if (session.documents.some(d => d.filename === cleanFilename)) {
      fs.unlinkSync(file.path);
      sseEvent(res, 'error', { message: `"${cleanFilename}" already uploaded`, code: 'DUPLICATE_FILE' });
      return res.end();
    }

    console.log(`[upload] [${sessionId}] Phase 1 — parsing ${cleanFilename} (${file.size} bytes)`);
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.path);

    if (!fullText || fullText.trim().length < 50) {
      fs.unlinkSync(file.path);
      sseEvent(res, 'error', { message: 'No extractable text — PDF may be scanned or image-only', code: 'EMPTY_PDF' });
      return res.end();
    }

    const documentId = uuidv4();
    // Use chunker defaults (TARGET=600, MAX=750, OVERLAP=100) — do NOT pass overrides
    const rawChunks  = chunkText(fullText);

    if (rawChunks.length === 0) {
      fs.unlinkSync(file.path);
      sseEvent(res, 'error', { message: 'No content could be extracted from PDF', code: 'EMPTY_PDF' });
      return res.end();
    }

    const chunks = rawChunks.map((chunk, idx) => ({
      text: chunk.text,
      metadata: {
        document_id:      documentId,
        filename:         cleanFilename,
        chunk_id:         createHash('md5').update(`${cleanFilename}::${chunk.text}`).digest('hex').slice(0, 16),
        chunk_index:      idx,
        total_chunks:     rawChunks.length,
        page_number:      getPageNumber(chunk.charStart, pageMap),
        total_pages:      totalPages,
        source_type:      'session_upload',
        upload_timestamp: new Date().toISOString(),
        char_start:       chunk.charStart,
        char_end:         chunk.charEnd,
        token_count:      chunk.tokenCount
      }
    }));

    const totalChunks  = chunks.length;
    const totalBatches = Math.ceil(totalChunks / BATCH_SIZE);
    const totalSets    = Math.ceil(totalBatches / PARALLEL_CALLS);

    console.log(`[upload] [${sessionId}] ${totalChunks} chunks → ${totalBatches} API calls → ${totalSets} sets of ${PARALLEL_CALLS} parallel`);

    sseEvent(res, 'upload_complete', {
      documentId, filename: cleanFilename, fileSize: file.size,
      pageCount: totalPages, totalChunks, totalBatches, totalSets
    });

    addDocumentToSession(sessionId, {
      id: documentId, filename: cleanFilename, fileSize: file.size,
      pageCount: totalPages, chunkCount: 0, status: 'indexing'
    });

    console.log(`[upload] [${sessionId}] Phase 1 done — ${cleanFilename} added to session as indexing`);

    const { collection } = await getSessionCollection(sessionId);
    let processedChunks  = 0;
    const allEmbeddings  = [];

    const batches = [];
    for (let i = 0; i < chunks.length; i += BATCH_SIZE) batches.push(chunks.slice(i, i + BATCH_SIZE));

    const sets = [];
    for (let i = 0; i < batches.length; i += PARALLEL_CALLS) sets.push(batches.slice(i, i + PARALLEL_CALLS));

    console.log(`[upload] [${sessionId}] Phase 2 start — ${sets.length} sets`);

    for (let setIdx = 0; setIdx < sets.length; setIdx++) {
      const isLastSet     = setIdx === sets.length - 1;
      const currentSet    = sets[setIdx];
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
              id:        batch[chunkIdx].metadata.chunk_id,
              embedding: vector,
              metadata:  batch[chunkIdx].metadata,
              text:      batch[chunkIdx].text
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

    invalidateSessionCollectionCache(sessionId);
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
    if (req.file && fs.existsSync(req.file.path)) {
      try { fs.unlinkSync(req.file.path); } catch {}
    }
    console.error('[upload] Unhandled error:', error);
    sseEvent(res, 'error', { message: error.message || 'Upload failed', code: error.code || 'UPLOAD_ERROR' });
    res.end();
  }
}

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

export async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const filename = req.query.filename;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  try {
    if (sessionId) {
      try {
        const { collection } = await getSessionCollection(sessionId);
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

    if (filename) {
      const filePath = path.join(uploadDir, filename);
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        console.log(`[delete] Removed file: ${filePath}`);
      } else {
        console.warn(`[delete] File not found on disk: ${filePath}`);
      }
    }

    res.json({ success: true, documentId });
  } catch (error) {
    console.error('Delete document error:', error);
    res.status(500).json({ error: 'Failed to delete document', code: 'DELETE_ERROR' });
  }
}

export async function getDocumentFile(req, res) {
  const filename = req.query.filename;

  try {
    if (filename) {
      const uploadPath = path.join(uploadDir, filename);
      if (fs.existsSync(uploadPath)) {
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', contentDisposition(filename));
        return fs.createReadStream(uploadPath).pipe(res);
      }

      const seedPath = path.join(seedDir, filename);
      if (fs.existsSync(seedPath)) {
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', contentDisposition(filename));
        return fs.createReadStream(seedPath).pipe(res);
      }

      if (fs.existsSync(seedDir)) {
        const allPdfs = fs.readdirSync(seedDir).filter(f => f.endsWith('.pdf'));
        const match   = allPdfs.find(f => f.includes(path.parse(filename).name));
        if (match) {
          const matchPath = path.join(seedDir, match);
          res.setHeader('Content-Type', 'application/pdf');
          res.setHeader('Content-Disposition', contentDisposition(match));
          return fs.createReadStream(matchPath).pipe(res);
        }
      }
    }

    return res.status(404).json({ error: 'Document file not found', code: 'FILE_NOT_FOUND' });
  } catch (error) {
    console.error('Get document file error:', error);
    res.status(500).json({ error: 'Failed to retrieve document', code: 'RETRIEVE_ERROR' });
  }
}

router.post('/upload', upload.single('file'), handleUpload);
router.get('/', listDocumentsHandler);
router.delete('/:documentId', deleteDocument);
router.get('/:documentId/file', getDocumentFile);

export default router;
