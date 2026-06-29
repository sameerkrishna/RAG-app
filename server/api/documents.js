import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { v4 as uuidv4 } from 'uuid';
import pdf from 'pdf-parse';
import { sanitizeFilename, validatePDFFile, validateFileSize } from '../utils/sanitize.js';
import {
  CorruptedPDFError,
  InvalidFileTypeError,
  FileTooLargeError,
  TooManyPDFsError,
  DuplicateFileError
} from '../utils/errors.js';
import { getGlobalCollection, getSessionCollection, addVectors, deleteDocumentVectors, listDocuments } from '../services/chromaService.js';
import { chunkPDFContent } from '../utils/chunker.js';
import { generateEmbeddings } from '../services/embeddingService.js';
import { getOrCreateSession, canAcceptUpload, addDocumentToSession, removeDocumentFromSession, getAllDocuments } from '../services/sessionService.js';

const router = Router();

const uploadDir = '/tmp/uploads';
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const id = uuidv4();
    const ext = path.extname(file.originalname);
    cb(null, `${id}${ext}`);
  }
});

const upload = multer({
  storage,
  limits: {
    fileSize: parseInt(process.env.MAX_UPLOAD_SIZE_MB || '5') * 1024 * 1024
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'application/pdf' && path.extname(file.originalname).toLowerCase() === '.pdf') {
      cb(null, true);
    } else {
      cb(new InvalidFileTypeError());
    }
  }
});

async function parsePDF(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);
    const data = await pdf(buffer);
    return {
      text: data.text,
      pageCount: data.numpages,
      info: data.info
    };
  } catch (error) {
    console.error('PDF parsing error:', error);
    throw new CorruptedPDFError();
  }
}

export async function handleUpload(req, res) {
  try {
    const file = req.file;
    if (!file) {
      throw new InvalidFileTypeError();
    }

    const sessionId = req.headers['x-session-id'] || req.body.sessionId || uuidv4();
    const session = getOrCreateSession(sessionId);
    const maxUploadsMB = parseInt(process.env.MAX_UPLOAD_SIZE_MB || '5');
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

    const pdfData = await parsePDF(file.path);
    const documentId = path.parse(file.filename).name;
    const documentPath = file.path;

    const chunks = chunkPDFContent({
      text: pdfData.text,
      filename: cleanFilename,
      documentId,
      pageNumber: 1
    });

    if (chunks.length === 0) {
      fs.unlinkSync(file.path);
      return res.status(422).json({
        error: 'No content could be extracted from PDF',
        code: 'EMPTY_PDF'
      });
    }

    const collection = await getSessionCollection(sessionId);

    const embeddings = [];
    const progressCallback = (processed, total) => {
      if (req.app.locals.progressCallbacks) {
        req.app.locals.progressCallbacks.emit(`progress_${sessionId}`, {
          documentId,
          processed,
          total,
          stage: 'embedding'
        });
      }
    };

    for (let i = 0; i < chunks.length; i++) {
      try {
        const embedding = await generateEmbeddings([chunks[i]]);
        if (embedding && embedding.length > 0) {
          embeddings.push({
            id: uuidv4(),
            embedding: embedding[0].embedding,
            text: chunks[i].text,
            metadata: chunks[i].metadata
          });
        }
      } catch (error) {
        console.error(`Failed to embed chunk ${i}:`, error);
      }
    }

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
      pageCount: pdfData.pageCount,
      chunkCount: embeddings.length
    });

    res.status(201).json({
      success: true,
      document: {
        id: documentId,
        filename: cleanFilename,
        fileSize: file.size,
        pageCount: pdfData.pageCount,
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

    const statusCode = error.statusCode || 500;
    res.status(statusCode).json({
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
    res.status(500).json({
      error: 'Failed to list documents',
      code: 'LIST_ERROR'
    });
  }
}

export async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  try {
    let collection;
    let deletedFromSession = false;

    if (sessionId) {
      collection = await getSessionCollection(sessionId);
      if (collection) {
        const count = await deleteDocumentVectors(collection, documentId);
        if (count > 0) {
          removeDocumentFromSession(sessionId, documentId);
          deletedFromSession = true;
        }
      }
    }

    // Delete from global collection
    try {
      const collection = await getGlobalCollection();
      await deleteDocumentVectors(collection, documentId);
    } catch (e) {
      // Not in global
    }

    res.json({
      success: true,
      documentId,
      deletedFrom: deletedFromSession ? 'session' : 'unknown'
    });
  } catch (error) {
    console.error('Delete document error:', error);
    res.status(500).json({
      error: 'Failed to delete document',
      code: 'DELETE_ERROR'
    });
  }
}

export async function getDocumentFile(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  try {
    const filePath = path.join(uploadDir, `${documentId}.pdf`);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({
        error: 'Document file not found',
        code: 'FILE_NOT_FOUND'
      });
    }

    res.setHeader('Content-Type', 'application/pdf');
    const filename = path.basename(filePath);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);

    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
  } catch (error) {
    console.error('Get document file error:', error);
    res.status(500).json({
      error: 'Failed to retrieve document',
      code: 'RETRIEVE_ERROR'
    });
  }
}

router.post('/upload', upload.single('file'), handleUpload);
router.get('/', listDocumentsHandler);
router.delete('/:documentId', deleteDocument);
router.get('/:documentId/file', getDocumentFile);

export default router;
