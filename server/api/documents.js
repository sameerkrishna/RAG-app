import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { v4 as uuidv4 } from 'uuid';
import { createHash } from 'crypto';
import pdf from 'pdf-parse';
import { sanitizeFilename, validatePDFFile, validateFileSize } from '../utils/sanitize.js';
import {
  CorruptedPDFError,
  InvalidFileTypeError,
  FileTooLargeError,
  TooManyPDFsError,
  DuplicateFileError
} from '../utils/errors.js';
import { getGlobalCollection, getSessionCollection, addVectors, deleteDocumentVectors } from '../services/chromaService.js';
import { chunkText, cleanText } from '../utils/chunker.js';
import { generateEmbeddings } from '../services/embeddingService.js';
import { getOrCreateSession, canAcceptUpload, addDocumentToSession, removeDocumentFromSession, getAllDocuments } from '../services/sessionService.js';

const router = Router();

const uploadDir = '/tmp/uploads';
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => cb(null, `${uuidv4()}${path.extname(file.originalname)}`)
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

// Parse PDF with per-page boundary map for accurate page numbers
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

    // Fallback if pagerender yields nothing
    if (pages.length === 0 || pages.every(p => !p.trim())) {
      const full = await pdf(buffer);
      pages.push(full.text);
    }

    const totalPages = pages.length;
    const cleanedPages = pages.map(p => cleanText(p));

    // Build page boundary map
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

export async function handleUpload(req, res) {
  try {
    const file = req.file;
    if (!file) throw new InvalidFileTypeError();

    const sessionId = req.headers['x-session-id'] || req.body.sessionId || uuidv4();
    const session = getOrCreateSession(sessionId);
    const maxPDFs = parseInt(process.env.MAX_PDFS_PER_SESSION || '3');
    const cleanFilename = sanitizeFilename(file.originalname);

    if (session.documents.length >= maxPDFs) {
      fs.unlinkSync(file.path);
      throw new TooManyPDFsError(maxPDFs);
    }

    if (session.documents.some(d => d.filename === cleanFilename)) {
      fs.unlinkSync(file.path);
      throw new DuplicateFileError(cleanFilename);
    }

    // Parse with boundary map
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.path);

    if (!fullText || fullText.trim().length < 50) {
      fs.unlinkSync(file.path);
      return res.status(422).json({
        error: 'No extractable text found — PDF may be scanned or image-only',
        code: 'EMPTY_PDF'
      });
    }

    const documentId = path.parse(file.filename).name;

    // Chunk full document at 1000 tokens / 200 overlap
    const rawChunks = chunkText(fullText, {
      chunkSizeTokens: 1000,
      overlapTokens: 200
    });

    if (rawChunks.length === 0) {
      fs.unlinkSync(file.path);
      return res.status(422).json({
        error: 'No content could be extracted from PDF',
        code: 'EMPTY_PDF'
      });
    }

    // Build chunks with accurate page numbers from boundary map
    const chunks = rawChunks.map((chunk, idx) => ({
      text: chunk.text,
      metadata: {
        document_id: documentId,
        filename: cleanFilename,
        chunk_id: createHash('md5').update(`${cleanFilename}::${chunk.text}`).digest('hex').slice(0, 16),
        chunk_index: idx,
        total_chunks: rawChunks.length,
        page_number: getPageNumber(chunk.charStart, pageMap),
        total_pages: totalPages,
        source_type: 'session_upload',
        upload_timestamp: new Date().toISOString(),
        char_start: chunk.charStart,
        char_end: chunk.charEnd,
        token_count: chunk.tokenCount
      }
    }));

    const collection = await getSessionCollection(sessionId);

    // Use same batching strategy as seed script
    // (7 chunks per batchEmbedContents, 4 parallel calls, 61s wait between groups)
    const embeddings = await generateEmbeddings(
      chunks,
      'RETRIEVAL_DOCUMENT',
      ({ current_batch, total_batches }) => {
        // Emit SSE progress if available
        if (req.app.locals.progressCallbacks) {
          req.app.locals.progressCallbacks.emit(`progress_${sessionId}`, {
            documentId,
            current_batch,
            total_batches,
            stage: 'embedding'
          });
        }
      }
    );

    if (embeddings.length === 0) {
      fs.unlinkSync(file.path);
      return res.status(503).json({
        error: 'Failed to generate embeddings',
        code: 'EMBEDDING_FAILED'
      });
    }

    await addVectors(
      collection,
      embeddings.map(e => ({ text: e.text, metadata: e.metadata })),
      embeddings.map(e => e.embedding),
      embeddings.map(e => e.id)
    );

    addDocumentToSession(sessionId, {
      id: documentId,
      filename: cleanFilename,
      fileSize: file.size,
      pageCount: totalPages,
      chunkCount: embeddings.length
    });

    res.status(201).json({
      success: true,
      document: {
        id: documentId,
        filename: cleanFilename,
        fileSize: file.size,
        pageCount: totalPages,
        chunkCount: embeddings.length,
        uploadTimestamp: new Date().toISOString()
      },
      sessionId
    });

  } catch (error) {
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    console.error('Upload error:', error);
    res.status(error.statusCode || 500).json({
      error: error.message,
      code: error.code || 'UPLOAD_ERROR'
    });
  }
}

export async function listDocumentsHandler(req, res) {
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;
  try {
    const documents = await getAllDocuments(sessionId);
    res.json(documents);
  } catch (error) {
    console.error('List documents error:', error);
    res.status(500).json({ error: 'Failed to list documents', code: 'LIST_ERROR' });
  }
}

export async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  try {
    if (sessionId) {
      const collection = await getSessionCollection(sessionId);
      if (collection) {
        const count = await deleteDocumentVectors(collection, documentId);
        if (count > 0) removeDocumentFromSession(sessionId, documentId);
      }
    }

    try {
      const globalCollection = await getGlobalCollection();
      await deleteDocumentVectors(globalCollection, documentId);
    } catch (e) { /* not in global */ }

    res.json({ success: true, documentId });
  } catch (error) {
    console.error('Delete document error:', error);
    res.status(500).json({ error: 'Failed to delete document', code: 'DELETE_ERROR' });
  }
}

export async function getDocumentFile(req, res) {
  const { documentId } = req.params;
  try {
    const filePath = path.join(uploadDir, `${documentId}.pdf`);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'Document file not found', code: 'FILE_NOT_FOUND' });
    }
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${path.basename(filePath)}"`);
    fs.createReadStream(filePath).pipe(res);
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