var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// server/services/chromaService.js
import { CloudClient } from "file:///home/project/node_modules/chromadb/dist/chromadb.mjs";
import { v4 as uuidv4 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
function getCloudClient() {
  if (!cloudClient) {
    const apiKey = process.env.CHROMA_API_KEY;
    const tenant = process.env.CHROMA_TENANT || "default_tenant";
    const database = process.env.CHROMA_DATABASE || "default_database";
    const host = process.env.CHROMA_HOST || void 0;
    console.log("---- CHROMA CONNECTIVITY DEBUG ----");
    console.log("Host:      ", host || "api.trychroma.com (default)");
    console.log("Tenant:    ", tenant);
    console.log("DB Name:   ", database);
    console.log("API Key:   ", apiKey ? "LOADED (VALID)" : "MISSING (UNDEFINED)");
    console.log("-----------------------------------");
    if (!apiKey) {
      throw new Error(
        "CRITICAL ERROR: CHROMA_API_KEY is undefined. Ensure your environment variables are correctly loaded before executing this file."
      );
    }
    const clientOptions = { apiKey, tenant, database };
    if (host) clientOptions.host = host;
    cloudClient = new CloudClient(clientOptions);
  }
  return cloudClient;
}
async function getGlobalCollection() {
  if (!globalCollection) {
    const client = getCloudClient();
    const collectionName = process.env.CHROMA_GLOBAL_COLLECTION || "dev";
    try {
      globalCollection = await client.getOrCreateCollection({
        name: collectionName,
        metadata: {
          description: "Permanent seed documents for RAG",
          type: "global_knowledge"
        },
        embeddingFunction: null
      });
      console.log(`\u2705 Global collection ready: ${collectionName}`);
    } catch (error) {
      console.error("Failed to connect to global collection:", error);
      throw error;
    }
  }
  return globalCollection;
}
async function getSessionCollection(sessionId) {
  if (sessionCollections.has(sessionId)) {
    return sessionCollections.get(sessionId);
  }
  const client = getCloudClient();
  const collectionName = `session_${sessionId}`;
  try {
    const collection = await client.getOrCreateCollection({
      name: collectionName,
      metadata: {
        type: "session_upload",
        session_id: sessionId,
        created: (/* @__PURE__ */ new Date()).toISOString()
      },
      embeddingFunction: null
    });
    sessionCollections.set(sessionId, collection);
    console.log(`\u2705 Session collection ready: ${collectionName}`);
    return collection;
  } catch (error) {
    console.error(`Failed to create session collection ${collectionName}:`, error);
    throw error;
  }
}
async function addVectors(collection, vectors, embeddings, ids) {
  try {
    await collection.add({
      ids,
      embeddings,
      documents: vectors.map((v) => v.text),
      metadatas: vectors.map((v) => v.metadata)
    });
    return true;
  } catch (error) {
    console.error("Failed to add vectors:", error);
    throw error;
  }
}
async function queryCollection(collection, queryEmbedding, topK = 5) {
  try {
    const results = await collection.query({
      queryEmbeddings: [queryEmbedding],
      nResults: topK,
      include: ["documents", "metadatas", "distances"]
    });
    if (!results.ids || results.ids.length === 0 || results.ids[0].length === 0) {
      return [];
    }
    return results.ids[0].map((id, idx) => ({
      id,
      text: results.documents[0][idx],
      metadata: results.metadatas[0][idx],
      distance: results.distances[0][idx],
      score: 1 - results.distances[0][idx]
    }));
  } catch (error) {
    console.error("Failed to query collection:", error);
    throw error;
  }
}
async function deleteDocumentVectors(collection, documentId) {
  try {
    const existing = await collection.get({
      where: { document_id: documentId }
    });
    if (existing.ids && existing.ids.length > 0) {
      await collection.delete({ ids: existing.ids });
      return existing.ids.length;
    }
    return 0;
  } catch (error) {
    console.error("Failed to delete document vectors:", error);
    throw error;
  }
}
async function listDocuments(collection) {
  try {
    const allItems = await collection.get({
      include: ["metadatas", "documents"]
    });
    const documentsMap = /* @__PURE__ */ new Map();
    if (allItems.ids) {
      allItems.ids.forEach((id, idx) => {
        const meta = allItems.metadatas[idx];
        const docId = meta.document_id;
        if (!documentsMap.has(docId)) {
          documentsMap.set(docId, {
            document_id: docId,
            filename: meta.filename,
            chunk_count: 0,
            page_count: meta.page_number || 1,
            upload_timestamp: meta.upload_timestamp,
            source_type: meta.source_type,
            first_chunk_text: allItems.documents[idx]
          });
        }
        const doc = documentsMap.get(docId);
        doc.chunk_count++;
        doc.page_count = Math.max(doc.page_count, meta.page_number || 1);
      });
    }
    return Array.from(documentsMap.values());
  } catch (error) {
    console.error("Failed to list documents:", error);
    return [];
  }
}
async function healthCheck() {
  try {
    const client = getCloudClient();
    const heartbeat = await client.heartbeat();
    return {
      status: "healthy",
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      heartbeat
    };
  } catch (error) {
    return {
      status: "unhealthy",
      error: error.message,
      timestamp: (/* @__PURE__ */ new Date()).toISOString()
    };
  }
}
var cloudClient, globalCollection, sessionCollections;
var init_chromaService = __esm({
  "server/services/chromaService.js"() {
    "use strict";
    cloudClient = null;
    globalCollection = null;
    sessionCollections = /* @__PURE__ */ new Map();
  }
});

// server/utils/errors.js
function is429Error(error) {
  return error?.code === 429 || error?.status === 429 || error?.message?.includes("429") || error?.message?.includes("RESOURCE_EXHAUSTED") || error?.message?.includes("Too Many Requests");
}
var AppError, ValidationError, InvalidFileTypeError, TooManyPDFsError, DuplicateFileError, CorruptedPDFError, LLMUnavailableError, EmbeddingError, WebSearchUnavailableError;
var init_errors = __esm({
  "server/utils/errors.js"() {
    "use strict";
    AppError = class extends Error {
      constructor(message, code, statusCode = 500) {
        super(message);
        this.code = code;
        this.statusCode = statusCode;
        this.isOperational = true;
        Error.captureStackTrace(this, this.constructor);
      }
    };
    ValidationError = class extends AppError {
      constructor(message, code = "VALIDATION_ERROR") {
        super(message, code, 400);
      }
    };
    InvalidFileTypeError = class extends AppError {
      constructor() {
        super("Only PDF files are allowed", "INVALID_FILE_TYPE", 415);
      }
    };
    TooManyPDFsError = class extends AppError {
      constructor(max) {
        super(`Maximum ${max} PDFs allowed per session`, "TOO_MANY_PDFS", 400);
      }
    };
    DuplicateFileError = class extends AppError {
      constructor(filename) {
        super(`File "${filename}" already exists in this session`, "DUPLICATE_FILE", 409);
      }
    };
    CorruptedPDFError = class extends AppError {
      constructor() {
        super("Failed to parse PDF file. It may be corrupted.", "CORRUPTED_PDF", 422);
      }
    };
    LLMUnavailableError = class extends AppError {
      constructor() {
        super("AI service is temporarily unavailable. Please try again.", "LLM_UNAVAILABLE", 503);
      }
    };
    EmbeddingError = class extends AppError {
      constructor(message = "Failed to generate embeddings") {
        super(message, "EMBEDDING_ERROR", 503);
      }
    };
    WebSearchUnavailableError = class extends AppError {
      constructor() {
        super("Web search is temporarily unavailable", "WEB_SEARCH_UNAVAILABLE", 503);
      }
    };
  }
});

// server/services/embeddingService.js
import { GoogleGenerativeAI } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getEmbeddingModel() {
  if (!embeddingModel) {
    genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
    embeddingModel = genAI.getGenerativeModel({
      model: process.env.GEMINI_EMBEDDING_MODEL || "text-embedding-004"
    });
  }
  return embeddingModel;
}
async function embedSingleChunk(text, taskType = "RETRIEVAL_DOCUMENT", attempt = 1) {
  const maxAttempts = 5;
  try {
    const model2 = getEmbeddingModel();
    const result = await model2.embedContent({
      content: { parts: [{ text }] },
      taskType
    });
    if (!result?.embedding?.values) {
      throw new EmbeddingError("No embedding returned from API");
    }
    return result.embedding.values;
  } catch (error) {
    const is429 = is429Error(error) || error?.status === 429 || error?.message?.includes("RESOURCE_EXHAUSTED");
    if (is429 && attempt < maxAttempts) {
      const retryDelay = error.retryAfter || GROUP_WAIT_MS;
      console.log(`Rate limited on embed, waiting ${retryDelay / 1e3}s (attempt ${attempt}/${maxAttempts})`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      return embedSingleChunk(text, taskType, attempt + 1);
    }
    throw new EmbeddingError(error.message || "Embedding generation failed");
  }
}
async function generateEmbeddings(chunks, taskType = "RETRIEVAL_DOCUMENT", onProgress) {
  if (!chunks || chunks.length === 0) return [];
  const parallelCalls = PARALLEL_CALLS();
  const embeddings = [];
  const totalGroups = Math.ceil(chunks.length / parallelCalls);
  for (let i = 0; i < chunks.length; i += parallelCalls) {
    const group = chunks.slice(i, i + parallelCalls);
    const groupNum = Math.floor(i / parallelCalls) + 1;
    console.log(`  Embedding group ${groupNum}/${totalGroups} (${group.length} chunks in parallel)...`);
    const results = await Promise.allSettled(
      group.map((chunk) => embedSingleChunk(chunk.text, taskType))
    );
    const failedChunks = [];
    results.forEach((result, idx) => {
      if (result.status === "fulfilled") {
        embeddings.push({
          id: group[idx].metadata?.chunk_id || `chunk_${i + idx}`,
          embedding: result.value,
          metadata: group[idx].metadata,
          text: group[idx].text
        });
      } else {
        console.warn(`  Chunk ${i + idx} failed in group, will retry:`, result.reason?.message);
        failedChunks.push({ chunk: group[idx], idx: i + idx });
      }
    });
    if (onProgress) {
      onProgress({ current_batch: groupNum, total_batches: totalGroups });
    }
    const isLastGroup = i + parallelCalls >= chunks.length;
    if (!isLastGroup || failedChunks.length > 0) {
      console.log(`  Waiting ${GROUP_WAIT_MS / 1e3}s before next group...`);
      await new Promise((resolve) => setTimeout(resolve, GROUP_WAIT_MS));
    }
    for (const { chunk, idx } of failedChunks) {
      try {
        const embedding = await embedSingleChunk(chunk.text, taskType);
        embeddings.push({
          id: chunk.metadata?.chunk_id || `chunk_${idx}`,
          embedding,
          metadata: chunk.metadata,
          text: chunk.text
        });
        console.log(`  \u2705 Retry succeeded for chunk ${idx}`);
      } catch (err) {
        console.error(`  \u274C Retry failed for chunk ${idx}:`, err.message);
      }
    }
  }
  return embeddings;
}
async function embedQuery(query) {
  return embedSingleChunk(query, "RETRIEVAL_QUERY");
}
function getRateLimitState() {
  return {
    maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 3e4,
    parallelCalls: PARALLEL_CALLS(),
    maxChunksPerCall: BATCH_SIZE()
  };
}
var genAI, embeddingModel, BATCH_SIZE, PARALLEL_CALLS, GROUP_WAIT_MS;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    genAI = null;
    embeddingModel = null;
    BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
    PARALLEL_CALLS = () => parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
    GROUP_WAIT_MS = 61e3;
  }
});

// server/api/health.js
import { Router } from "file:///home/project/node_modules/express/index.js";
async function health(req, res) {
  const healthStatus = {
    status: "ok",
    timestamp: (/* @__PURE__ */ new Date()).toISOString(),
    services: {}
  };
  try {
    const chromaHealth = await healthCheck();
    healthStatus.services.chromadb = chromaHealth;
  } catch (error) {
    healthStatus.services.chromadb = {
      status: "error",
      error: error.message
    };
  }
  healthStatus.services.gemini = {
    status: process.env.GEMINI_API_KEY ? "configured" : "not_configured"
  };
  healthStatus.rateLimit = getRateLimitState();
  const hasErrors = Object.values(healthStatus.services).some(
    (s) => s.status === "error" || s.status === "unhealthy"
  );
  if (hasErrors) {
    healthStatus.status = "degraded";
  }
  res.json(healthStatus);
}
var router, health_default;
var init_health = __esm({
  "server/api/health.js"() {
    "use strict";
    init_chromaService();
    init_embeddingService();
    router = Router();
    router.get("/", health);
    health_default = router;
  }
});

// server/utils/sanitize.js
import path from "path";
function sanitizeFilename(filename) {
  if (!filename || typeof filename !== "string") {
    throw new ValidationError("Invalid filename");
  }
  const basename = path.basename(filename);
  let sanitized = basename.replace(DANGEROUS_PATTERNS, "_");
  sanitized = sanitized.replace(PATH_TRAVERSAL, "");
  sanitized = sanitized.trim().slice(0, 255);
  if (!sanitized) {
    throw new ValidationError("Invalid filename after sanitization");
  }
  return sanitized;
}
var DANGEROUS_PATTERNS, PATH_TRAVERSAL;
var init_sanitize = __esm({
  "server/utils/sanitize.js"() {
    "use strict";
    init_errors();
    DANGEROUS_PATTERNS = /[<>:"|?*\x00-\x1f]/g;
    PATH_TRAVERSAL = /\.\./g;
  }
});

// server/utils/chunker.js
function estimateTokens(text) {
  if (!text || typeof text !== "string") return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}
function chunkText(text, options = {}) {
  const chunkSizeTokens = options.chunkSizeTokens || DEFAULT_CHUNK_SIZE_TOKENS;
  const overlapTokens = options.overlapTokens || DEFAULT_OVERLAP_TOKENS;
  if (!text || typeof text !== "string") {
    return [];
  }
  const chunkSizeChars = chunkSizeTokens * CHARS_PER_TOKEN;
  const overlapChars = overlapTokens * CHARS_PER_TOKEN;
  const chunks = [];
  let start = 0;
  let chunkIndex = 0;
  while (start < text.length) {
    let end = start + chunkSizeChars;
    if (end < text.length) {
      const breakPoints = [". ", ".\n", "! ", "? ", "\n\n", "\n", " "];
      let bestBreak = -1;
      const searchStart = end - Math.floor(chunkSizeChars * 0.2);
      for (const breakpoint of breakPoints) {
        const idx = text.lastIndexOf(breakpoint, end);
        if (idx > searchStart && idx > start) {
          bestBreak = idx + breakpoint.length;
          break;
        }
      }
      if (bestBreak > start) {
        end = bestBreak;
      }
    }
    const chunkText2 = text.slice(start, end).trim();
    if (chunkText2.length > 0) {
      chunks.push({
        text: chunkText2,
        tokenCount: estimateTokens(chunkText2),
        charStart: start,
        charEnd: end,
        chunkIndex: chunkIndex++
      });
    }
    start = end - overlapChars;
    if (start <= chunks[chunks.length - 1]?.charStart) {
      start = end;
    }
    if (chunkIndex > 1e4) {
      console.warn("Chunk limit reached, stopping");
      break;
    }
  }
  return chunks;
}
function chunkPDFContent(pdfData, options = {}) {
  const { filename, documentId, pageNumber, text } = pdfData;
  const textChunks = chunkText(text, options);
  return textChunks.map((chunk) => ({
    text: chunk.text,
    metadata: {
      document_id: documentId,
      filename,
      chunk_id: `${documentId}_${chunk.chunkIndex}`,
      chunk_index: chunk.chunkIndex,
      page_number: pageNumber || 1,
      section_title: extractSectionTitle(chunk.text),
      source_type: "pdf",
      upload_timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      token_start: chunk.charStart,
      token_end: chunk.charEnd,
      token_count: chunk.tokenCount
    }
  }));
}
function extractSectionTitle(text) {
  const lines = text.split("\n").filter((l) => l.trim());
  if (lines.length > 0) {
    const firstLine = lines[0].trim();
    if (firstLine.length < 100 && !firstLine.endsWith(".")) {
      return firstLine.slice(0, 50);
    }
  }
  return null;
}
var CHARS_PER_TOKEN, DEFAULT_CHUNK_SIZE_TOKENS, DEFAULT_OVERLAP_TOKENS;
var init_chunker = __esm({
  "server/utils/chunker.js"() {
    "use strict";
    CHARS_PER_TOKEN = 4;
    DEFAULT_CHUNK_SIZE_TOKENS = 1e3;
    DEFAULT_OVERLAP_TOKENS = 200;
  }
});

// server/services/sessionService.js
import { v4 as uuidv42 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
function createSession() {
  const sessionId = uuidv42();
  const session = {
    id: sessionId,
    createdAt: /* @__PURE__ */ new Date(),
    lastAccessed: /* @__PURE__ */ new Date(),
    documents: [],
    timeoutMinutes: DEFAULT_TIMEOUT_MINUTES
  };
  sessions.set(sessionId, session);
  return session;
}
function getSession(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) {
    return null;
  }
  if (isSessionExpired(session)) {
    deleteSession(sessionId);
    return null;
  }
  session.lastAccessed = /* @__PURE__ */ new Date();
  return session;
}
function getOrCreateSession(sessionId) {
  if (sessionId) {
    const existing = getSession(sessionId);
    if (existing) {
      return existing;
    }
  }
  return createSession();
}
function isSessionExpired(session) {
  const now = Date.now();
  const lastAccessed = new Date(session.lastAccessed).getTime();
  const timeoutMs = session.timeoutMinutes * 60 * 1e3;
  return now - lastAccessed > timeoutMs;
}
function deleteSession(sessionId) {
  sessions.delete(sessionId);
}
function addDocumentToSession(sessionId, documentInfo) {
  const session = getSession(sessionId);
  if (!session) {
    return false;
  }
  session.documents.push({
    id: documentInfo.id,
    filename: documentInfo.filename,
    fileSize: documentInfo.fileSize,
    pageCount: documentInfo.pageCount,
    uploadTimestamp: /* @__PURE__ */ new Date(),
    chunkCount: documentInfo.chunkCount,
    sourceType: "session_upload"
  });
  session.lastAccessed = /* @__PURE__ */ new Date();
  return true;
}
function removeDocumentFromSession(sessionId, documentId) {
  const session = getSession(sessionId);
  if (!session) {
    return false;
  }
  const idx = session.documents.findIndex((d) => d.id === documentId);
  if (idx >= 0) {
    session.documents.splice(idx, 1);
    session.lastAccessed = /* @__PURE__ */ new Date();
    return true;
  }
  return false;
}
function getSessionDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) {
    return [];
  }
  return session.documents;
}
async function getAllDocuments(sessionId) {
  const sessionDocs = getSessionDocuments(sessionId);
  const globalCollection2 = await getGlobalCollection();
  const globalDocs = await listDocuments(globalCollection2);
  return {
    sessionDocuments: sessionDocs,
    globalDocuments: globalDocs
  };
}
var DEFAULT_TIMEOUT_MINUTES, sessions, MAX_PDFS_PER_SESSION, MAX_UPLOAD_SIZE_MB;
var init_sessionService = __esm({
  "server/services/sessionService.js"() {
    "use strict";
    init_chromaService();
    DEFAULT_TIMEOUT_MINUTES = 60;
    sessions = /* @__PURE__ */ new Map();
    MAX_PDFS_PER_SESSION = parseInt(process.env.MAX_PDFS_PER_SESSION) || 3;
    MAX_UPLOAD_SIZE_MB = parseInt(process.env.MAX_UPLOAD_SIZE_MB) || 5;
  }
});

// server/api/documents.js
import { Router as Router2 } from "file:///home/project/node_modules/express/index.js";
import multer from "file:///home/project/node_modules/multer/index.js";
import path2 from "path";
import fs from "fs";
import { v4 as uuidv43 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
import pdf from "file:///home/project/node_modules/pdf-parse/index.js";
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
    console.error("PDF parsing error:", error);
    throw new CorruptedPDFError();
  }
}
async function handleUpload(req, res) {
  try {
    const file = req.file;
    if (!file) {
      throw new InvalidFileTypeError();
    }
    const sessionId = req.headers["x-session-id"] || req.body.sessionId || uuidv43();
    const session = getOrCreateSession(sessionId);
    const maxUploadsMB = parseInt(process.env.MAX_UPLOAD_SIZE_MB || "5");
    const maxPDFs = parseInt(process.env.MAX_PDFS_PER_SESSION || "3");
    const cleanFilename = sanitizeFilename(file.originalname);
    if (session.documents.length >= maxPDFs) {
      fs.unlinkSync(file.path);
      throw new TooManyPDFsError(maxPDFs);
    }
    if (session.documents.some((d) => d.filename === cleanFilename)) {
      fs.unlinkSync(file.path);
      throw new DuplicateFileError(cleanFilename);
    }
    const pdfData = await parsePDF(file.path);
    const documentId = path2.parse(file.filename).name;
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
        error: "No content could be extracted from PDF",
        code: "EMPTY_PDF"
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
          stage: "embedding"
        });
      }
    };
    for (let i = 0; i < chunks.length; i++) {
      try {
        const embedding = await generateEmbeddings([chunks[i]]);
        if (embedding && embedding.length > 0) {
          embeddings.push({
            id: uuidv43(),
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
        error: "Failed to generate embeddings",
        code: "EMBEDDING_FAILED"
      });
    }
    await addVectors(
      collection,
      embeddings.map((e) => ({ text: e.text, metadata: e.metadata })),
      embeddings.map((e) => e.embedding),
      embeddings.map((e) => e.id)
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
        uploadTimestamp: (/* @__PURE__ */ new Date()).toISOString()
      },
      sessionId
    });
  } catch (error) {
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    console.error("Upload error:", error);
    const statusCode = error.statusCode || 500;
    res.status(statusCode).json({
      error: error.message,
      code: error.code || "UPLOAD_ERROR"
    });
  }
}
async function listDocumentsHandler(req, res) {
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  try {
    const documents = await getAllDocuments(sessionId);
    res.json(documents);
  } catch (error) {
    console.error("List documents error:", error);
    res.status(500).json({
      error: "Failed to list documents",
      code: "LIST_ERROR"
    });
  }
}
async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
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
    try {
      const collection2 = await getGlobalCollection();
      await deleteDocumentVectors(collection2, documentId);
    } catch (e) {
    }
    res.json({
      success: true,
      documentId,
      deletedFrom: deletedFromSession ? "session" : "unknown"
    });
  } catch (error) {
    console.error("Delete document error:", error);
    res.status(500).json({
      error: "Failed to delete document",
      code: "DELETE_ERROR"
    });
  }
}
async function getDocumentFile(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  try {
    const filePath = path2.join(uploadDir, `${documentId}.pdf`);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({
        error: "Document file not found",
        code: "FILE_NOT_FOUND"
      });
    }
    res.setHeader("Content-Type", "application/pdf");
    const filename = path2.basename(filePath);
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
  } catch (error) {
    console.error("Get document file error:", error);
    res.status(500).json({
      error: "Failed to retrieve document",
      code: "RETRIEVE_ERROR"
    });
  }
}
var router2, uploadDir, storage, upload, documents_default;
var init_documents = __esm({
  "server/api/documents.js"() {
    "use strict";
    init_sanitize();
    init_errors();
    init_chromaService();
    init_chunker();
    init_embeddingService();
    init_sessionService();
    router2 = Router2();
    uploadDir = "/tmp/uploads";
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    storage = multer.diskStorage({
      destination: (req, file, cb) => {
        cb(null, uploadDir);
      },
      filename: (req, file, cb) => {
        const id = uuidv43();
        const ext = path2.extname(file.originalname);
        cb(null, `${id}${ext}`);
      }
    });
    upload = multer({
      storage,
      limits: {
        fileSize: parseInt(process.env.MAX_UPLOAD_SIZE_MB || "5") * 1024 * 1024
      },
      fileFilter: (req, file, cb) => {
        if (file.mimetype === "application/pdf" && path2.extname(file.originalname).toLowerCase() === ".pdf") {
          cb(null, true);
        } else {
          cb(new InvalidFileTypeError());
        }
      }
    });
    router2.post("/upload", upload.single("file"), handleUpload);
    router2.get("/", listDocumentsHandler);
    router2.delete("/:documentId", deleteDocument);
    router2.get("/:documentId/file", getDocumentFile);
    documents_default = router2;
  }
});

// server/services/retrievalService.js
import { v4 as uuidv44 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
async function getOrCacheGlobalCollection() {
  if (!cachedGlobalCollection) {
    cachedGlobalCollection = await getGlobalCollection();
  }
  return cachedGlobalCollection;
}
async function getOrCacheSessionCollection(sessionId) {
  if (cachedSessionCollections.has(sessionId)) {
    return cachedSessionCollections.get(sessionId);
  }
  try {
    const collection = await getSessionCollection(sessionId);
    if (collection) {
      cachedSessionCollections.set(sessionId, collection);
    }
    return collection;
  } catch {
    return null;
  }
}
function calculateCoverage(results, topK = TOP_K) {
  if (!results || results.length === 0) {
    return { level: "low", score: 0, reason: "No results found" };
  }
  const topResults = results.slice(0, topK);
  const scores = topResults.map((r) => r.score);
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  let level;
  let reason;
  if (avgScore >= COVERAGE_HIGH_THRESHOLD) {
    level = "high";
    reason = "High confidence in retrieved context";
  } else if (avgScore >= COVERAGE_MEDIUM_THRESHOLD) {
    level = "medium";
    reason = "Moderate confidence in retrieved context";
  } else {
    level = "low";
    reason = "Insufficient relevant information found";
  }
  return {
    level,
    score: avgScore,
    topScore: Math.max(...scores),
    bottomScore: Math.min(...scores),
    reason
  };
}
async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || TOP_K;
  const includeGlobal = options.includeGlobal !== false;
  try {
    const [queryEmbedding, globalCollection2, sessionCollection] = await Promise.all([
      embedQuery(query),
      includeGlobal ? getOrCacheGlobalCollection() : Promise.resolve(null),
      sessionId ? getOrCacheSessionCollection(sessionId) : Promise.resolve(null)
    ]);
    const queryPromises = [];
    if (globalCollection2) {
      queryPromises.push(
        queryCollection(globalCollection2, queryEmbedding, topK).then((results) => ({ type: "global", results })).catch(() => ({ type: "global", results: [] }))
      );
    }
    if (sessionCollection) {
      queryPromises.push(
        queryCollection(sessionCollection, queryEmbedding, topK).then((results) => ({ type: "session", results })).catch(() => ({ type: "session", results: [] }))
      );
    }
    const queryResults = await Promise.all(queryPromises);
    const allResults = [];
    for (const { type, results: typeResults } of queryResults) {
      for (const result of typeResults) {
        allResults.push({ ...result, source_type: type });
      }
    }
    allResults.sort((a, b) => b.score - a.score);
    const topResults = allResults.slice(0, topK);
    const coverage = calculateCoverage(topResults, topK);
    return { results: topResults, coverage, queryEmbedding };
  } catch (error) {
    console.error("Retrieval error:", error);
    throw error;
  }
}
function generateCitations(results) {
  if (!results || results.length === 0) {
    return [];
  }
  return results.map((result, idx) => ({
    id: uuidv44(),
    index: idx + 1,
    documentId: result.metadata.document_id,
    filename: result.metadata.filename,
    pageNumber: result.metadata.page_number,
    section: result.metadata.section_title,
    excerpt: result.text.slice(0, 200) + (result.text.length > 200 ? "..." : ""),
    score: result.score,
    sourceType: result.source_type,
    chunkId: result.id
  }));
}
function shouldShowRefusal(coverage) {
  return coverage.level === "low" && coverage.score > 0;
}
var TOP_K, COVERAGE_HIGH_THRESHOLD, COVERAGE_MEDIUM_THRESHOLD, cachedGlobalCollection, cachedSessionCollections;
var init_retrievalService = __esm({
  "server/services/retrievalService.js"() {
    "use strict";
    init_chromaService();
    init_embeddingService();
    TOP_K = parseInt(process.env.TOP_K) || 5;
    COVERAGE_HIGH_THRESHOLD = parseFloat(process.env.COVERAGE_HIGH_THRESHOLD) || 0.75;
    COVERAGE_MEDIUM_THRESHOLD = parseFloat(process.env.COVERAGE_MEDIUM_THRESHOLD) || 0.55;
    cachedGlobalCollection = null;
    cachedSessionCollections = /* @__PURE__ */ new Map();
  }
});

// server/services/memoryService.js
function initializeMemory(sessionId) {
  if (!memoryMap.has(sessionId)) {
    memoryMap.set(sessionId, {
      turns: [],
      createdAt: /* @__PURE__ */ new Date()
    });
  }
  return memoryMap.get(sessionId);
}
function addTurn(sessionId, role, content, metadata = {}) {
  const memory = memoryMap.get(sessionId) || initializeMemory(sessionId);
  const maxTurns = parseInt(process.env.MEMORY_WINDOW_TURNS) || DEFAULT_MEMORY_WINDOW;
  const turn = {
    id: `turn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    role,
    content,
    timestamp: /* @__PURE__ */ new Date(),
    ...metadata
  };
  memory.turns.push(turn);
  if (memory.turns.length > maxTurns) {
    memory.turns = memory.turns.slice(-maxTurns);
  }
  return turn;
}
function getMemory(sessionId) {
  return memoryMap.get(sessionId) || initializeMemory(sessionId);
}
function getRecentTurns(sessionId, maxTurns = null) {
  const memory = getMemory(sessionId);
  const limit = maxTurns || parseInt(process.env.MEMORY_WINDOW_TURNS) || DEFAULT_MEMORY_WINDOW;
  return memory.turns.slice(-limit);
}
function addTurnWithCitations(sessionId, role, content, citations = [], coverage = null) {
  return addTurn(sessionId, role, content, {
    citations,
    coverage,
    hasCitations: citations.length > 0
  });
}
var memoryMap, DEFAULT_MEMORY_WINDOW;
var init_memoryService = __esm({
  "server/services/memoryService.js"() {
    "use strict";
    memoryMap = /* @__PURE__ */ new Map();
    DEFAULT_MEMORY_WINDOW = parseInt(process.env.MEMORY_WINDOW_TURNS) || 10;
  }
});

// server/services/promptService.js
function getRefusalResponse() {
  return REFUSAL_MESSAGE;
}
var REFUSAL_MESSAGE;
var init_promptService = __esm({
  "server/services/promptService.js"() {
    "use strict";
    init_memoryService();
    init_retrievalService();
    REFUSAL_MESSAGE = "I don't have enough information in the knowledge base to answer that confidently. Try uploading relevant documents, or ask me a general question.";
  }
});

// server/services/geminiService.js
import { GoogleGenerativeAI as GoogleGenerativeAI2 } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getGenAI() {
  if (!genAI2) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("GEMINI_API_KEY is undefined");
    genAI2 = new GoogleGenerativeAI2(apiKey);
  }
  return genAI2;
}
function getPrimaryModel() {
  if (!primaryModel) {
    primaryModel = getGenAI().getGenerativeModel({ model: PRIMARY_MODEL });
  }
  return primaryModel;
}
function getFallbackModel() {
  if (!fallbackModel) {
    fallbackModel = getGenAI().getGenerativeModel({ model: FALLBACK_MODEL });
  }
  return fallbackModel;
}
async function* streamResponse(prompt) {
  let model2 = getPrimaryModel();
  let retries = 0;
  const maxRetries = 2;
  while (retries < maxRetries) {
    try {
      const controller = new AbortController();
      const result = await model2.generateContentStream({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      });
      let firstToken = true;
      const firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);
      for await (const chunk of result.stream) {
        if (controller.signal.aborted) {
          clearTimeout(firstTokenTimeout);
          throw new Error("First token timeout \u2014 no response from model");
        }
        const text = chunk.text();
        if (text) {
          if (firstToken) {
            firstToken = false;
            clearTimeout(firstTokenTimeout);
          }
          yield { type: "token", text };
        }
      }
      clearTimeout(firstTokenTimeout);
      return { success: true };
    } catch (error) {
      retries++;
      console.error(`Model attempt ${retries} failed:`, error.message);
      if (retries >= maxRetries) {
        yield { type: "error", error: error.message };
        throw new LLMUnavailableError();
      }
      model2 = getFallbackModel();
    }
  }
}
function getRefusalText() {
  return getRefusalResponse();
}
var genAI2, PRIMARY_MODEL, FALLBACK_MODEL, FIRST_TOKEN_TIMEOUT, REQUEST_TIMEOUT, primaryModel, fallbackModel;
var init_geminiService = __esm({
  "server/services/geminiService.js"() {
    "use strict";
    init_promptService();
    init_errors();
    genAI2 = null;
    PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || "gemini-3.1-flash-lite";
    FALLBACK_MODEL = process.env.GEMINI_MODEL_FALLBACK || "gemini-2.5-flash";
    FIRST_TOKEN_TIMEOUT = parseInt(process.env.LLM_FIRST_TOKEN_TIMEOUT_SECONDS) * 1e3 || 12e3;
    REQUEST_TIMEOUT = parseInt(process.env.LLM_REQUEST_TIMEOUT_SECONDS) * 1e3 || 45e3;
    primaryModel = null;
    fallbackModel = null;
  }
});

// server/api/chat.js
import { Router as Router3 } from "file:///home/project/node_modules/express/index.js";
import { v4 as uuidv45 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({
      error: "Query is required",
      code: "MISSING_QUERY"
    });
  }
  const sessionId = providedSessionId || uuidv45();
  const answerId = uuidv45();
  getOrCreateSession(sessionId);
  const userTurn = addTurnWithCitations(sessionId, "user", query.trim());
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("x-session-id", sessionId);
  res.setHeader("x-answer-id", answerId);
  const sendEvent = (event, data) => {
    res.write(`event: ${event}
`);
    res.write(`data: ${JSON.stringify(data)}

`);
    if (typeof res.flush === "function") res.flush();
  };
  try {
    sendEvent("status", { stage: "retrieving", message: "Searching knowledge base..." });
    const { results, coverage } = await retrieveForQuery(query, sessionId, { topK: 5 });
    sendEvent("retrieval", {
      results: results.length,
      coverage: coverage.level,
      coverageScore: coverage.score
    });
    if (shouldShowRefusal(coverage)) {
      const citations2 = generateCitations(results);
      addTurnWithCitations(sessionId, "assistant", getRefusalText(), citations2, coverage);
      sendEvent("complete", {
        answerId,
        response: getRefusalText(),
        citations: citations2,
        coverage,
        action: "refusal"
      });
      res.end();
      return;
    }
    sendEvent("status", { stage: "generating", message: "Generating response..." });
    const memoryContext = getRecentTurns(sessionId, 5).map((t) => `${t.role === "user" ? "User" : "Assistant"}: ${t.content}`).join("\n\n");
    let prompt;
    if (results.length > 0) {
      const contextText = results.map(
        (r, i) => `[${i + 1}] ${r.metadata.filename || "Source"}: ${r.text}`
      ).join("\n\n");
      prompt = `You are a helpful AI Knowledge Assistant. Answer based on the provided context documents.

CONTEXT:
${contextText}

${memoryContext ? `CONVERSATION HISTORY:
${memoryContext}

` : ""}CURRENT QUESTION: ${query}

Answer concisely and cite sources using [1], [2] etc. referring to the context numbers above.`;
    } else {
      prompt = `You are a Knowledge Assistant that answers questions strictly based on uploaded documents.

${memoryContext ? `CONVERSATION HISTORY:
${memoryContext}

` : ""}CURRENT QUESTION: ${query}

RULES:
- For greetings or small talk (e.g. "hi", "hello", "how are you"), respond briefly and warmly.
- For ANY factual, technical, or knowledge-based question, do NOT attempt to answer it. Instead, tell the user that no documents have been uploaded yet and invite them to upload relevant documents so you can provide a grounded answer.
- Never write code, explain general concepts, or answer from your own training knowledge.`;
    }
    let fullResponse = "";
    for await (const chunk of streamResponse(prompt)) {
      if (chunk.type === "token") {
        fullResponse += chunk.text;
        sendEvent("token", { text: chunk.text });
        await new Promise((resolve) => setTimeout(resolve, 90));
      } else if (chunk.type === "error") {
        sendEvent("error", { message: chunk.error, code: "LLM_ERROR" });
      } else if (chunk.type === "complete") {
        fullResponse = chunk.response;
      }
    }
    const citations = generateCitations(results);
    addTurnWithCitations(sessionId, "assistant", fullResponse, citations, coverage);
    sendEvent("complete", {
      answerId,
      response: fullResponse,
      citations,
      coverage,
      sources: results.map((r) => ({
        chunkId: r.id,
        documentId: r.metadata.document_id,
        filename: r.metadata.filename,
        pageNumber: r.metadata.page_number,
        excerpt: r.text.slice(0, 200),
        sourceType: r.source_type
      }))
    });
    res.end();
  } catch (error) {
    console.error("Chat stream error:", error);
    sendEvent("error", {
      message: error.message || "An error occurred",
      code: error.code || "CHAT_ERROR"
    });
    res.end();
  }
}
async function getSources(req, res) {
  const { answerId } = req.params;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  const recentTurns = getRecentTurns(sessionId, 10);
  for (const turn of recentTurns) {
    if (turn.id === answerId || turn.citations?.length > 0) {
      return res.json({
        sources: turn.citations || []
      });
    }
  }
  res.status(404).json({
    error: "Sources not found for this answer",
    code: "SOURCES_NOT_FOUND"
  });
}
var router3, chat_default;
var init_chat = __esm({
  "server/api/chat.js"() {
    "use strict";
    init_retrievalService();
    init_geminiService();
    init_memoryService();
    init_sessionService();
    router3 = Router3();
    router3.post("/", handleChatStream);
    router3.get("/sources/:answerId", getSources);
    chat_default = router3;
  }
});

// server/api/feedback.js
import { Router as Router4 } from "file:///home/project/node_modules/express/index.js";
import { v4 as uuidv46 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
async function submitFeedback(req, res) {
  const { answerId, sessionId, type, comment, rating } = req.body;
  if (!answerId || !type) {
    return res.status(400).json({
      error: "answerId and type are required",
      code: "MISSING_FIELDS"
    });
  }
  const validTypes = ["positive", "negative", "helpful", "not_helpful", "report_issue"];
  if (!validTypes.includes(type)) {
    return res.status(400).json({
      error: "Invalid feedback type",
      code: "INVALID_TYPE",
      validTypes
    });
  }
  try {
    const feedback = {
      id: uuidv46(),
      answerId,
      sessionId: sessionId || "unknown",
      type,
      rating: rating || null,
      comment: comment || null,
      createdAt: (/* @__PURE__ */ new Date()).toISOString(),
      userAgent: req.headers["user-agent"] || null,
      ip: req.ip || null
    };
    feedbackStore.set(feedback.id, feedback);
    res.status(201).json({
      success: true,
      feedbackId: feedback.id,
      message: "Thank you for your feedback"
    });
  } catch (error) {
    console.error("Feedback submission error:", error);
    res.status(500).json({
      error: "Failed to submit feedback",
      code: "FEEDBACK_ERROR"
    });
  }
}
async function getFeedbackStats(req, res) {
  const { answerId } = req.params;
  try {
    const allFeedback = Array.from(feedbackStore.values());
    const answerFeedback = allFeedback.filter((f) => f.answerId === answerId);
    const stats = {
      total: answerFeedback.length,
      positive: answerFeedback.filter((f) => f.type === "positive" || f.type === "helpful").length,
      negative: answerFeedback.filter((f) => f.type === "negative" || f.type === "not_helpful").length,
      averageRating: answerFeedback.filter((f) => f.rating).reduce((sum, f, _, arr) => sum + f.rating / arr.length, 0) || null
    };
    res.json(stats);
  } catch (error) {
    res.status(500).json({
      error: "Failed to get feedback stats",
      code: "STATS_ERROR"
    });
  }
}
async function listFeedback(req, res) {
  const { sessionId } = req.query;
  try {
    let feedback = Array.from(feedbackStore.values());
    if (sessionId) {
      feedback = feedback.filter((f) => f.sessionId === sessionId);
    }
    res.json({
      total: feedback.length,
      feedback: feedback.slice(-50)
      // Last 50 entries
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to list feedback",
      code: "LIST_ERROR"
    });
  }
}
var router4, feedbackStore, feedback_default;
var init_feedback = __esm({
  "server/api/feedback.js"() {
    "use strict";
    router4 = Router4();
    feedbackStore = /* @__PURE__ */ new Map();
    router4.post("/", submitFeedback);
    router4.get("/stats/:answerId", getFeedbackStats);
    router4.get("/list", listFeedback);
    feedback_default = router4;
  }
});

// server/services/webSearchService.js
import { GoogleGenerativeAI as GoogleGenerativeAI3 } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getModel() {
  if (!model) {
    model = genAI3.getGenerativeModel({ model: PRIMARY_MODEL2 });
  }
  return model;
}
async function performWebSearch(query) {
  try {
    const model2 = getModel();
    const result = await model2.generateContent({
      contents: [{
        role: "user",
        parts: [{ text: query }]
      }],
      generationConfig: {
        temperature: 0.7,
        maxOutputTokens: 2048
      },
      tools: [{ googleSearch: {} }]
    });
    const response = result.response;
    const text = response.text();
    const groundingMetadata = response.candidates?.[0]?.groundingMetadata;
    const webSearchQueries = [];
    const webSources = [];
    if (groundingMetadata?.groundingChunks) {
      for (const chunk of groundingMetadata.groundingChunks) {
        if (chunk.web) {
          webSources.push({
            uri: chunk.web.uri,
            title: chunk.web.title
          });
        }
      }
    }
    if (groundingMetadata?.webSearchQueries) {
      webSearchQueries.push(...groundingMetadata.webSearchQueries);
    }
    return {
      text,
      sources: webSources,
      queries: webSearchQueries,
      rawMetadata: groundingMetadata
    };
  } catch (error) {
    console.error("Web search error:", error);
    throw new WebSearchUnavailableError();
  }
}
async function* streamWebSearch(query) {
  try {
    const model2 = getModel();
    const result = await model2.generateContentStream({
      contents: [{
        role: "user",
        parts: [{ text: query }]
      }],
      generationConfig: {
        temperature: 0.7,
        maxOutputTokens: 2048
      },
      tools: [{ googleSearch: {} }]
    });
    let fullResponse = "";
    for await (const chunk of result.stream) {
      const text = chunk.text();
      if (text) {
        fullResponse += text;
        yield { type: "token", text };
      }
    }
    const response = await result.response;
    const groundingMetadata = response?.candidates?.[0]?.groundingMetadata;
    const sources = [];
    if (groundingMetadata?.groundingChunks) {
      for (const item of groundingMetadata.groundingChunks) {
        if (item.web) {
          sources.push({
            uri: item.web.uri,
            title: item.web.title
          });
        }
      }
    }
    yield {
      type: "complete",
      response: fullResponse,
      sources
    };
  } catch (error) {
    console.error("Web search streaming error:", error);
    yield { type: "error", error: error.message };
    throw new WebSearchUnavailableError();
  }
}
var genAI3, PRIMARY_MODEL2, model;
var init_webSearchService = __esm({
  "server/services/webSearchService.js"() {
    "use strict";
    init_errors();
    genAI3 = new GoogleGenerativeAI3(process.env.GEMINI_API_KEY);
    PRIMARY_MODEL2 = process.env.GEMINI_MODEL_PRIMARY || "gemini-3.1-flash-lite";
    model = null;
  }
});

// server/api/search.js
import { Router as Router5 } from "file:///home/project/node_modules/express/index.js";
async function handleWebSearch(req, res) {
  const { query } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({
      error: "Query is required",
      code: "MISSING_QUERY"
    });
  }
  try {
    const result = await performWebSearch(query.trim());
    res.json({
      success: true,
      answer: result.text,
      sources: result.sources,
      queries: result.queries,
      metadata: {
        performedAt: (/* @__PURE__ */ new Date()).toISOString(),
        searchType: "google_search_grounding"
      }
    });
  } catch (error) {
    console.error("Web search error:", error);
    res.status(error.statusCode || 503).json({
      error: error.message || "Web search unavailable",
      code: error.code || "WEB_SEARCH_ERROR"
    });
  }
}
async function handleWebSearchStream(req, res) {
  const { query } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({
      error: "Query is required",
      code: "MISSING_QUERY"
    });
  }
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  const sendEvent = (event, data) => {
    res.write(`event: ${event}
`);
    res.write(`data: ${JSON.stringify(data)}

`);
  };
  try {
    sendEvent("status", { stage: "searching", message: "Searching the web..." });
    let fullResponse = "";
    let sources = [];
    for await (const chunk of streamWebSearch(query.trim())) {
      if (chunk.type === "token") {
        fullResponse += chunk.text;
        sendEvent("token", { text: chunk.text });
      } else if (chunk.type === "error") {
        sendEvent("error", { message: chunk.error, code: "WEB_SEARCH_ERROR" });
      } else if (chunk.type === "complete") {
        fullResponse = chunk.response;
        sources = chunk.sources || [];
      }
    }
    sendEvent("complete", {
      response: fullResponse,
      sources,
      searchType: "google_search_grounding"
    });
    res.end();
  } catch (error) {
    console.error("Web search stream error:", error);
    sendEvent("error", {
      message: error.message || "Web search failed",
      code: error.code || "WEB_SEARCH_ERROR"
    });
    res.end();
  }
}
var router5, search_default;
var init_search = __esm({
  "server/api/search.js"() {
    "use strict";
    init_webSearchService();
    router5 = Router5();
    router5.post("/", handleWebSearch);
    router5.post("/stream", handleWebSearchStream);
    search_default = router5;
  }
});

// server/app.js
var app_exports = {};
__export(app_exports, {
  default: () => app_default
});
import express from "file:///home/project/node_modules/express/index.js";
import cors from "file:///home/project/node_modules/cors/lib/index.js";
import dotenv from "file:///home/project/node_modules/dotenv/lib/main.js";
import { EventEmitter } from "events";
var app, app_default;
var init_app = __esm({
  "server/app.js"() {
    "use strict";
    init_health();
    init_documents();
    init_chat();
    init_feedback();
    init_search();
    dotenv.config();
    app = express();
    app.locals.progressCallbacks = new EventEmitter();
    app.use(cors({
      origin: [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173"
      ],
      credentials: true
    }));
    app.use(express.json({ limit: "10mb" }));
    app.use(express.urlencoded({ extended: true, limit: "10mb" }));
    app.use((req, res, next) => {
      console.log(`${req.method} ${req.originalUrl}`);
      next();
    });
    app.get("/ping", (req, res) => {
      console.log("\u2705 PING ROUTE EXECUTED");
      res.json({
        success: true,
        message: "Express backend is alive"
      });
    });
    console.log("Mounting routers...");
    app.use("/health", health_default);
    app.use("/documents", documents_default);
    app.use("/chat", chat_default);
    app.use("/feedback", feedback_default);
    app.use("/search", search_default);
    console.log("\u2705 Routers mounted");
    app.use((err, req, res, next) => {
      console.error("ERROR MIDDLEWARE");
      console.error(err);
      res.status(500).json({
        error: err.message,
        stack: err.stack
      });
    });
    app.use((req, res) => {
      res.status(404).json({
        error: "Endpoint not found",
        code: "NOT_FOUND"
      });
    });
    app_default = app;
  }
});

// vite.config.js
import { defineConfig } from "file:///home/project/node_modules/vite/dist/node/index.js";
import react from "file:///home/project/node_modules/@vitejs/plugin-react/dist/index.js";
import path3 from "path";
import { fileURLToPath } from "url";
var __vite_injected_original_import_meta_url = "file:///home/project/vite.config.js";
var __awaiter = function(thisArg, _arguments, P, generator) {
  function adopt(value) {
    return value instanceof P ? value : new P(function(resolve) {
      resolve(value);
    });
  }
  return new (P || (P = Promise))(function(resolve, reject) {
    function fulfilled(value) {
      try {
        step(generator.next(value));
      } catch (e) {
        reject(e);
      }
    }
    function rejected(value) {
      try {
        step(generator["throw"](value));
      } catch (e) {
        reject(e);
      }
    }
    function step(result) {
      result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected);
    }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = function(thisArg, body) {
  var _ = { label: 0, sent: function() {
    if (t[0] & 1) throw t[1];
    return t[1];
  }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
  return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() {
    return this;
  }), g;
  function verb(n) {
    return function(v) {
      return step([n, v]);
    };
  }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (g && (g = 0, op[0] && (_ = 0)), _) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0:
        case 1:
          t = op;
          break;
        case 4:
          _.label++;
          return { value: op[1], done: false };
        case 5:
          _.label++;
          y = op[1];
          op = [0];
          continue;
        case 7:
          op = _.ops.pop();
          _.trys.pop();
          continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) {
            _ = 0;
            continue;
          }
          if (op[0] === 3 && (!t || op[1] > t[0] && op[1] < t[3])) {
            _.label = op[1];
            break;
          }
          if (op[0] === 6 && _.label < t[1]) {
            _.label = t[1];
            t = op;
            break;
          }
          if (t && _.label < t[2]) {
            _.label = t[2];
            _.ops.push(op);
            break;
          }
          if (t[2]) _.ops.pop();
          _.trys.pop();
          continue;
      }
      op = body.call(thisArg, _);
    } catch (e) {
      op = [6, e];
      y = 0;
    } finally {
      f = t = 0;
    }
    if (op[0] & 5) throw op[1];
    return { value: op[0] ? op[1] : void 0, done: true };
  }
};
var __dirname = path3.dirname(fileURLToPath(__vite_injected_original_import_meta_url));
function expressPlugin() {
  var app2;
  return {
    name: "express-plugin",
    configureServer: function(server) {
      return __awaiter(this, void 0, void 0, function() {
        var expressApp;
        return __generator(this, function(_a) {
          switch (_a.label) {
            case 0:
              return [4, Promise.resolve().then(() => (init_app(), app_exports))];
            case 1:
              expressApp = _a.sent().default;
              app2 = expressApp;
              server.middlewares.use("/api", function(req, res, next) {
                app2(req, res, next);
              });
              return [
                2
                /*return*/
              ];
          }
        });
      });
    }
  };
}
var vite_config_default = defineConfig({
  plugins: [react(), expressPlugin()],
  resolve: {
    alias: {
      "@": path3.resolve(__dirname, "./src")
    }
  },
  server: {
    port: 5173
  }
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcblxuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuLy8gQWxpYXMgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbmZ1bmN0aW9uIGdldENsaWVudCgpIHtcbiAgcmV0dXJuIGdldENsb3VkQ2xpZW50KCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IEdsb2JhbCBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY29ubmVjdCB0byBnbG9iYWwgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGdsb2JhbENvbGxlY3Rpb247XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBzZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBgc2Vzc2lvbl8ke3Nlc3Npb25JZH1gO1xuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gY3JlYXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiBkZWxldGVkOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBkZWxldGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgaWRzLFxuICAgICAgZW1iZWRkaW5ncyxcbiAgICAgIGRvY3VtZW50czogdmVjdG9ycy5tYXAodiA9PiB2LnRleHQpLFxuICAgICAgbWV0YWRhdGFzOiB2ZWN0b3JzLm1hcCh2ID0+IHYubWV0YWRhdGEpXG4gICAgfSk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9XG4gICAgfSk7XG4gICAgaWYgKGV4aXN0aW5nLmlkcyAmJiBleGlzdGluZy5pZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGV4aXN0aW5nLmlkcyB9KTtcbiAgICAgIHJldHVybiBleGlzdGluZy5pZHMubGVuZ3RoO1xuICAgIH1cbiAgICByZXR1cm4gMDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudENvdW50KGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICByZXR1cm4gYXdhaXQgY29sbGVjdGlvbi5jb3VudCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBnZXQgZG9jdW1lbnQgY291bnQ6JywgZXJyb3IpO1xuICAgIHJldHVybiAwO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxJdGVtcyA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddXG4gICAgfSk7XG5cbiAgICBjb25zdCBkb2N1bWVudHNNYXAgPSBuZXcgTWFwKCk7XG5cbiAgICBpZiAoYWxsSXRlbXMuaWRzKSB7XG4gICAgICBhbGxJdGVtcy5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYWxsSXRlbXMubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYWxsSXRlbXMuZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBjbGVhbnVwU2Vzc2lvbkNvbGxlY3Rpb25zKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbnMgPSBhd2FpdCBjbGllbnQubGlzdENvbGxlY3Rpb25zKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uQ29sbGVjdGlvbk5hbWVzID0gY29sbGVjdGlvbnMuZmlsdGVyKGMgPT4gYy5zdGFydHNXaXRoKCdzZXNzaW9uXycpKTtcblxuICAgIGlmIChzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjcwNSBObyBzdGFsZSBzZXNzaW9uIGNvbGxlY3Rpb25zIGZvdW5kLicpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBcdUQ4M0VcdURERjkgQ2xlYW5pbmcgdXAgJHtzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLmxlbmd0aH0gc3RhbGUgc2Vzc2lvbiBjb2xsZWN0aW9uKHMpLi4uYCk7XG5cbiAgICBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLm1hcChhc3luYyBjb2xsZWN0aW9uTmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lOiBjb2xsZWN0aW9uTmFtZSB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgICBcdTI3MDUgRGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGAgIFx1MjZBMFx1RkUwRiBDb3VsZCBub3QgZGVsZXRlICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY2xlYW51cCBjb21wbGV0ZS4nKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLndhcm4oJ1x1MjZBMFx1RkUwRiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdXZWIgc2VhcmNoIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1dFQl9TRUFSQ0hfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG4vLyBcdTI3MDUgTGF6eSBpbml0IFx1MjAxNCBhdm9pZHMgZG90ZW52IHRpbWluZyBidWdcbmxldCBnZW5BSSA9IG51bGw7XG5sZXQgZW1iZWRkaW5nTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRFbWJlZGRpbmdNb2RlbCgpIHtcbiAgaWYgKCFlbWJlZGRpbmdNb2RlbCkge1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSk7XG4gICAgZW1iZWRkaW5nTW9kZWwgPSBnZW5BSS5nZXRHZW5lcmF0aXZlTW9kZWwoe1xuICAgICAgbW9kZWw6IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ3RleHQtZW1iZWRkaW5nLTAwNCdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZW1iZWRkaW5nTW9kZWw7XG59XG5cbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0O1xuY29uc3QgR1JPVVBfV0FJVF9NUyA9IDYxMDAwOyAvLyA2MXMgdG8gc2FmZWx5IGNsZWFyIHRoZSAxLW1pbnV0ZSByYXRlIGxpbWl0IHdpbmRvd1xuXG5mdW5jdGlvbiBlc3RpbWF0ZVRva2Vucyh0ZXh0KSB7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyA0KTtcbn1cblxuLy8gXHUyNzA1IEVtYmVkIGEgc2luZ2xlIGNodW5rIFx1MjAxNCBvbmUgQVBJIGNhbGwsIG9uZSBlbWJlZGRpbmcgcmV0dXJuZWRcbmFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlQ2h1bmsodGV4dCwgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbWF4QXR0ZW1wdHMgPSA1O1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0RW1iZWRkaW5nTW9kZWwoKTtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5lbWJlZENvbnRlbnQoe1xuICAgICAgY29udGVudDogeyBwYXJ0czogW3sgdGV4dCB9XSB9LFxuICAgICAgdGFza1R5cGVcbiAgICB9KTtcbiAgICBpZiAoIXJlc3VsdD8uZW1iZWRkaW5nPy52YWx1ZXMpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcignTm8gZW1iZWRkaW5nIHJldHVybmVkIGZyb20gQVBJJyk7XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQuZW1iZWRkaW5nLnZhbHVlcztcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zdCBpczQyOSA9IGlzNDI5RXJyb3IoZXJyb3IpIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJyk7XG5cbiAgICBpZiAoaXM0MjkgJiYgYXR0ZW1wdCA8IG1heEF0dGVtcHRzKSB7XG4gICAgICBjb25zdCByZXRyeURlbGF5ID0gZXJyb3IucmV0cnlBZnRlciB8fCBHUk9VUF9XQUlUX01TO1xuICAgICAgY29uc29sZS5sb2coYFJhdGUgbGltaXRlZCBvbiBlbWJlZCwgd2FpdGluZyAke3JldHJ5RGVsYXkgLyAxMDAwfXMgKGF0dGVtcHQgJHthdHRlbXB0fS8ke21heEF0dGVtcHRzfSlgKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCByZXRyeURlbGF5KSk7XG4gICAgICByZXR1cm4gZW1iZWRTaW5nbGVDaHVuayh0ZXh0LCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnRW1iZWRkaW5nIGdlbmVyYXRpb24gZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gXHUyNzA1IEdlbmVyYXRlIGVtYmVkZGluZ3MgZm9yIGFuIGFycmF5IG9mIGNodW5rc1xuLy8gU3RyYXRlZ3k6IDQgcGFyYWxsZWwgY2FsbHMgcGVyIGdyb3VwLCA2MHMgd2FpdCBiZXR3ZWVuIGdyb3Vwc1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuICBjb25zdCBlbWJlZGRpbmdzID0gW107XG5cbiAgLy8gRWFjaCBjaHVuayA9IG9uZSBBUEkgY2FsbCAoR2VtaW5pIGVtYmVkZGluZyBpcyBwZXItdGV4dCwgbm90IHBlci1iYXRjaClcbiAgLy8gR3JvdXAgaW50byBwYXJhbGxlbCBzZXRzIG9mIFBBUkFMTEVMX0NBTExTXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGNodW5rcy5sZW5ndGggLyBwYXJhbGxlbENhbGxzKTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IGdyb3VwID0gY2h1bmtzLnNsaWNlKGksIGkgKyBwYXJhbGxlbENhbGxzKTtcbiAgICBjb25zdCBncm91cE51bSA9IE1hdGguZmxvb3IoaSAvIHBhcmFsbGVsQ2FsbHMpICsgMTtcblxuICAgIGNvbnNvbGUubG9nKGAgIEVtYmVkZGluZyBncm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSAoJHtncm91cC5sZW5ndGh9IGNodW5rcyBpbiBwYXJhbGxlbCkuLi5gKTtcblxuICAgIC8vIFx1MjcwNSBTZW5kIGdyb3VwIGluIHBhcmFsbGVsIHVzaW5nIFByb21pc2UuYWxsU2V0dGxlZFxuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBncm91cC5tYXAoY2h1bmsgPT4gZW1iZWRTaW5nbGVDaHVuayhjaHVuay50ZXh0LCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIC8vIENvbGxlY3QgcmVzdWx0cywgcmV0cnkgZmFpbGVkIG9uZXMgaW5kaXZpZHVhbGx5XG4gICAgY29uc3QgZmFpbGVkQ2h1bmtzID0gW107XG4gICAgcmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGlkeCkgPT4ge1xuICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgIGVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgaWQ6IGdyb3VwW2lkeF0ubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua18ke2kgKyBpZHh9YCxcbiAgICAgICAgICBlbWJlZGRpbmc6IHJlc3VsdC52YWx1ZSxcbiAgICAgICAgICBtZXRhZGF0YTogZ3JvdXBbaWR4XS5tZXRhZGF0YSxcbiAgICAgICAgICB0ZXh0OiBncm91cFtpZHhdLnRleHRcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oYCAgQ2h1bmsgJHtpICsgaWR4fSBmYWlsZWQgaW4gZ3JvdXAsIHdpbGwgcmV0cnk6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZENodW5rcy5wdXNoKHsgY2h1bms6IGdyb3VwW2lkeF0sIGlkeDogaSArIGlkeCB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIC8vIFx1MjcwNSBXYWl0IGJldHdlZW4gZ3JvdXBzIHRvIHJlc3BlY3QgcmF0ZSBsaW1pdHMgKHNraXAgd2FpdCBhZnRlciBsYXN0IGdyb3VwKVxuICAgIGNvbnN0IGlzTGFzdEdyb3VwID0gaSArIHBhcmFsbGVsQ2FsbHMgPj0gY2h1bmtzLmxlbmd0aDtcbiAgICBpZiAoIWlzTGFzdEdyb3VwIHx8IGZhaWxlZENodW5rcy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgbmV4dCBncm91cC4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIEdST1VQX1dBSVRfTVMpKTtcbiAgICB9XG5cbiAgICAvLyBSZXRyeSBmYWlsZWQgY2h1bmtzIGluZGl2aWR1YWxseSBhZnRlciB0aGUgd2FpdFxuICAgIGZvciAoY29uc3QgeyBjaHVuaywgaWR4IH0gb2YgZmFpbGVkQ2h1bmtzKSB7XG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCBlbWJlZGRpbmcgPSBhd2FpdCBlbWJlZFNpbmdsZUNodW5rKGNodW5rLnRleHQsIHRhc2tUeXBlKTtcbiAgICAgICAgZW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua18ke2lkeH1gLFxuICAgICAgICAgIGVtYmVkZGluZyxcbiAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICB9KTtcbiAgICAgICAgY29uc29sZS5sb2coYCAgXHUyNzA1IFJldHJ5IHN1Y2NlZWRlZCBmb3IgY2h1bmsgJHtpZHh9YCk7XG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcihgICBcdTI3NEMgUmV0cnkgZmFpbGVkIGZvciBjaHVuayAke2lkeH06YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG4vLyBcdTI3MDUgU2luZ2xlIHF1ZXJ5IGVtYmVkZGluZyBcdTIwMTQgUkVUUklFVkFMX1FVRVJZIHRhc2sgdHlwZVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgcmV0dXJuIGVtYmVkU2luZ2xlQ2h1bmsocXVlcnksICdSRVRSSUVWQUxfUVVFUlknKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlKHRleHQpIHtcbiAgcmV0dXJuIGVtYmVkU2luZ2xlQ2h1bmsodGV4dCwgJ1JFVFJJRVZBTF9ET0NVTUVOVCcpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmF0ZUxpbWl0U3RhdGUoKSB7XG4gIHJldHVybiB7XG4gICAgbWF4VG9rZW5zUGVyTWludXRlOiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUkFURV9MSU1JVF9UT0tFTlNfUEVSX01JTlVURSkgfHwgMzAwMDAsXG4gICAgcGFyYWxsZWxDYWxsczogUEFSQUxMRUxfQ0FMTFMoKSxcbiAgICBtYXhDaHVua3NQZXJDYWxsOiBCQVRDSF9TSVpFKCksXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldFJhdGVMaW1pdFN0YXRlIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBDaGVjayBHZW1pbmkgKHZpYSBBUEkga2V5IHByZXNlbmNlKVxuICBoZWFsdGhTdGF0dXMuc2VydmljZXMuZ2VtaW5pID0ge1xuICAgIHN0YXR1czogcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkgPyAnY29uZmlndXJlZCcgOiAnbm90X2NvbmZpZ3VyZWQnXG4gIH07XG5cbiAgLy8gR2V0IHJhdGUgbGltaXQgc3RhdGVcbiAgaGVhbHRoU3RhdHVzLnJhdGVMaW1pdCA9IGdldFJhdGVMaW1pdFN0YXRlKCk7XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjsvLyBUb2tlbiBlc3RpbWF0aW9uOiB+NCBjaGFyYWN0ZXJzIHBlciB0b2tlbiBmb3IgRW5nbGlzaCB0ZXh0XG5jb25zdCBDSEFSU19QRVJfVE9LRU4gPSA0O1xuY29uc3QgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUyA9IDEwMDA7XG5jb25zdCBERUZBVUxUX09WRVJMQVBfVE9LRU5TID0gMjAwO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1RleHQodGV4dCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IGNodW5rU2l6ZVRva2VucyA9IG9wdGlvbnMuY2h1bmtTaXplVG9rZW5zIHx8IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUb2tlbnMgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgfHwgREVGQVVMVF9PVkVSTEFQX1RPS0VOUztcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2h1bmtTaXplQ2hhcnMgPSBjaHVua1NpemVUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG5cbiAgY29uc3QgY2h1bmtzID0gW107XG4gIGxldCBzdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcblxuICB3aGlsZSAoc3RhcnQgPCB0ZXh0Lmxlbmd0aCkge1xuICAgIGxldCBlbmQgPSBzdGFydCArIGNodW5rU2l6ZUNoYXJzO1xuXG4gICAgLy8gVHJ5IHRvIGZpbmQgYSBnb29kIGJyZWFrIHBvaW50XG4gICAgaWYgKGVuZCA8IHRleHQubGVuZ3RoKSB7XG4gICAgICBjb25zdCBicmVha1BvaW50cyA9IFsnLiAnLCAnLlxcbicsICchICcsICc/ICcsICdcXG5cXG4nLCAnXFxuJywgJyAnXTtcbiAgICAgIGxldCBiZXN0QnJlYWsgPSAtMTtcblxuICAgICAgLy8gTG9vayBmb3IgYnJlYWsgcG9pbnRzIGluIHRoZSBsYXN0IDIwJSBvZiB0aGUgY2h1bmtcbiAgICAgIGNvbnN0IHNlYXJjaFN0YXJ0ID0gZW5kIC0gTWF0aC5mbG9vcihjaHVua1NpemVDaGFycyAqIDAuMik7XG5cbiAgICAgIGZvciAoY29uc3QgYnJlYWtwb2ludCBvZiBicmVha1BvaW50cykge1xuICAgICAgICBjb25zdCBpZHggPSB0ZXh0Lmxhc3RJbmRleE9mKGJyZWFrcG9pbnQsIGVuZCk7XG4gICAgICAgIGlmIChpZHggPiBzZWFyY2hTdGFydCAmJiBpZHggPiBzdGFydCkge1xuICAgICAgICAgIGJlc3RCcmVhayA9IGlkeCArIGJyZWFrcG9pbnQubGVuZ3RoO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGlmIChiZXN0QnJlYWsgPiBzdGFydCkge1xuICAgICAgICBlbmQgPSBiZXN0QnJlYWs7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgY2h1bmtUZXh0ID0gdGV4dC5zbGljZShzdGFydCwgZW5kKS50cmltKCk7XG4gICAgaWYgKGNodW5rVGV4dC5sZW5ndGggPiAwKSB7XG4gICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgIHRleHQ6IGNodW5rVGV4dCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY2h1bmtUZXh0KSxcbiAgICAgICAgY2hhclN0YXJ0OiBzdGFydCxcbiAgICAgICAgY2hhckVuZDogZW5kLFxuICAgICAgICBjaHVua0luZGV4OiBjaHVua0luZGV4KytcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIC8vIE1vdmUgdG8gbmV4dCBjaHVuayB3aXRoIG92ZXJsYXBcbiAgICBzdGFydCA9IGVuZCAtIG92ZXJsYXBDaGFycztcbiAgICBpZiAoc3RhcnQgPD0gY2h1bmtzW2NodW5rcy5sZW5ndGggLSAxXT8uY2hhclN0YXJ0KSB7XG4gICAgICBzdGFydCA9IGVuZDtcbiAgICB9XG5cbiAgICAvLyBTYWZldHkgY2hlY2sgdG8gcHJldmVudCBpbmZpbml0ZSBsb29wc1xuICAgIGlmIChjaHVua0luZGV4ID4gMTAwMDApIHtcbiAgICAgIGNvbnNvbGUud2FybignQ2h1bmsgbGltaXQgcmVhY2hlZCwgc3RvcHBpbmcnKTtcbiAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQgfSA9IHBkZkRhdGE7XG5cbiAgY29uc3QgdGV4dENodW5rcyA9IGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zKTtcblxuICByZXR1cm4gdGV4dENodW5rcy5tYXAoY2h1bmsgPT4gKHtcbiAgICB0ZXh0OiBjaHVuay50ZXh0LFxuICAgIG1ldGFkYXRhOiB7XG4gICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgIGZpbGVuYW1lOiBmaWxlbmFtZSxcbiAgICAgIGNodW5rX2lkOiBgJHtkb2N1bWVudElkfV8ke2NodW5rLmNodW5rSW5kZXh9YCxcbiAgICAgIGNodW5rX2luZGV4OiBjaHVuay5jaHVua0luZGV4LFxuICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgIHNlY3Rpb25fdGl0bGU6IGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICBzb3VyY2VfdHlwZTogJ3BkZicsXG4gICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB0b2tlbl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgdG9rZW5fZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICB9XG4gIH0pKTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIC8vIFRyeSB0byBleHRyYWN0IGEgcG90ZW50aWFsIHNlY3Rpb24gdGl0bGUgZnJvbSB0aGUgYmVnaW5uaW5nIG9mIHRoZSBjaHVua1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBtZXJnZUNodW5rcyhjaHVua3MsIG1heFRva2VucyA9IDcwMDApIHtcbiAgLy8gTWVyZ2Ugc21hbGwgY2h1bmtzIHRvIHJlZHVjZSBBUEkgY2FsbHNcbiAgY29uc3QgbWVyZ2VkID0gW107XG4gIGxldCBjdXJyZW50ID0geyB0ZXh0czogW10sIHRvdGFsVG9rZW5zOiAwLCBtZXRhZGF0YTogW10gfTtcblxuICBmb3IgKGNvbnN0IGNodW5rIG9mIGNodW5rcykge1xuICAgIGlmIChjdXJyZW50LnRvdGFsVG9rZW5zICsgY2h1bmsudG9rZW5Db3VudCA8PSBtYXhUb2tlbnMpIHtcbiAgICAgIGN1cnJlbnQudGV4dHMucHVzaChjaHVuay50ZXh0KTtcbiAgICAgIGN1cnJlbnQubWV0YWRhdGEucHVzaChjaHVuay5tZXRhZGF0YSk7XG4gICAgICBjdXJyZW50LnRvdGFsVG9rZW5zICs9IGNodW5rLnRva2VuQ291bnQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChjdXJyZW50LnRleHRzLmxlbmd0aCA+IDApIHtcbiAgICAgICAgbWVyZ2VkLnB1c2goeyB0ZXh0czogY3VycmVudC50ZXh0cywgbWV0YWRhdGE6IGN1cnJlbnQubWV0YWRhdGEgfSk7XG4gICAgICB9XG4gICAgICBjdXJyZW50ID0geyB0ZXh0czogW2NodW5rLnRleHRdLCBtZXRhZGF0YTogW2NodW5rLm1ldGFkYXRhXSwgdG90YWxUb2tlbnM6IGNodW5rLnRva2VuQ291bnQgfTtcbiAgICB9XG4gIH1cblxuICBpZiAoY3VycmVudC50ZXh0cy5sZW5ndGggPiAwKSB7XG4gICAgbWVyZ2VkLnB1c2goeyB0ZXh0czogY3VycmVudC50ZXh0cywgbWV0YWRhdGE6IGN1cnJlbnQubWV0YWRhdGEgfSk7XG4gIH1cblxuICByZXR1cm4gbWVyZ2VkO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IGdldEdsb2JhbENvbGxlY3Rpb24sIGdldFNlc3Npb25Db2xsZWN0aW9uLCBsaXN0RG9jdW1lbnRzIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcblxuY29uc3QgREVGQVVMVF9USU1FT1VUX01JTlVURVMgPSA2MDtcbmNvbnN0IHNlc3Npb25zID0gbmV3IE1hcCgpO1xuY29uc3QgTUFYX1BERlNfUEVSX1NFU1NJT04gPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTikgfHwgMztcbmNvbnN0IE1BWF9VUExPQURfU0laRV9NQiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQikgfHwgNTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkOiBzZXNzaW9uSWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuXG4gIHNlc3Npb25zLnNldChzZXNzaW9uSWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcblxuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykge1xuICAgICAgcmV0dXJuIGV4aXN0aW5nO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SW5mbykge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICBpZDogZG9jdW1lbnRJbmZvLmlkLFxuICAgIGZpbGVuYW1lOiBkb2N1bWVudEluZm8uZmlsZW5hbWUsXG4gICAgZmlsZVNpemU6IGRvY3VtZW50SW5mby5maWxlU2l6ZSxcbiAgICBwYWdlQ291bnQ6IGRvY3VtZW50SW5mby5wYWdlQ291bnQsXG4gICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIGNodW5rQ291bnQ6IGRvY3VtZW50SW5mby5jaHVua0NvdW50LFxuICAgIHNvdXJjZVR5cGU6ICdzZXNzaW9uX3VwbG9hZCdcbiAgfSk7XG5cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiAnU2Vzc2lvbiBub3QgZm91bmQnIH07XG4gIH1cblxuICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiBgTWF4aW11bSAke01BWF9QREZTX1BFUl9TRVNTSU9OfSBQREZzIHBlciBzZXNzaW9uYCB9O1xuICB9XG5cbiAgcmV0dXJuIHsgY2FuVXBsb2FkOiB0cnVlIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVVwbG9hZChzZXNzaW9uSWQsIGZpbGUsIGZpbGVuYW1lKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGNvbnN0IGVycm9ycyA9IFtdO1xuXG4gIC8vIENoZWNrIGZpbGUgc2l6ZVxuICBpZiAoZmlsZS5zaXplID4gTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBleGNlZWRzICR7TUFYX1VQTE9BRF9TSVpFX01CfU1CIGxpbWl0YCk7XG4gIH1cblxuICAvLyBDaGVjayBtYXggUERGc1xuICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICBlcnJvcnMucHVzaChgTWF4aW11bSAke01BWF9QREZTX1BFUl9TRVNTSU9OfSBQREZzIHBlciBzZXNzaW9uYCk7XG4gIH1cblxuICAvLyBDaGVjayBmb3IgZHVwbGljYXRlIGZpbGVuYW1lXG4gIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBmaWxlbmFtZSkpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCk7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGlzVmFsaWQ6IGVycm9ycy5sZW5ndGggPT09IDAsXG4gICAgZXJyb3JzLFxuICAgIGlzTGFyZ2VGaWxlOiBmaWxlLnNpemUgPiAoTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQgKiAwLjYpIC8vIDYwJSBvZiBtYXhcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuXG4gIGNvbnN0IGlkeCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmRJbmRleChkID0+IGQuaWQgPT09IGRvY3VtZW50SWQpO1xuICBpZiAoaWR4ID49IDApIHtcbiAgICBzZXNzaW9uLmRvY3VtZW50cy5zcGxpY2UoaWR4LCAxKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gW107XG4gIH1cbiAgcmV0dXJuIHNlc3Npb24uZG9jdW1lbnRzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uRG9jcyA9IGdldFNlc3Npb25Eb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgY29uc3QgZ2xvYmFsRG9jcyA9IGF3YWl0IGxpc3REb2N1bWVudHMoZ2xvYmFsQ29sbGVjdGlvbik7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uRG9jcyxcbiAgICBnbG9iYWxEb2N1bWVudHM6IGdsb2JhbERvY3NcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25TdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBjbGVhbmVkKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBjbGVhbmVkO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IG11bHRlciBmcm9tICdtdWx0ZXInO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG5pbXBvcnQgeyBzYW5pdGl6ZUZpbGVuYW1lLCB2YWxpZGF0ZVBERkZpbGUsIHZhbGlkYXRlRmlsZVNpemUgfSBmcm9tICcuLi91dGlscy9zYW5pdGl6ZS5qcyc7XG5pbXBvcnQge1xuICBDb3JydXB0ZWRQREZFcnJvcixcbiAgSW52YWxpZEZpbGVUeXBlRXJyb3IsXG4gIEZpbGVUb29MYXJnZUVycm9yLFxuICBUb29NYW55UERGc0Vycm9yLFxuICBEdXBsaWNhdGVGaWxlRXJyb3Jcbn0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcbmltcG9ydCB7IGdldEdsb2JhbENvbGxlY3Rpb24sIGdldFNlc3Npb25Db2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMsIGxpc3REb2N1bWVudHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNodW5rUERGQ29udGVudCB9IGZyb20gJy4uL3V0aWxzL2NodW5rZXIuanMnO1xuaW1wb3J0IHsgZ2VuZXJhdGVFbWJlZGRpbmdzIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGNhbkFjY2VwdFVwbG9hZCwgYWRkRG9jdW1lbnRUb1Nlc3Npb24sIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sIGdldEFsbERvY3VtZW50cyB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmNvbnN0IHVwbG9hZERpciA9ICcvdG1wL3VwbG9hZHMnO1xuaWYgKCFmcy5leGlzdHNTeW5jKHVwbG9hZERpcikpIHtcbiAgZnMubWtkaXJTeW5jKHVwbG9hZERpciwgeyByZWN1cnNpdmU6IHRydWUgfSk7XG59XG5cbmNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBjYihudWxsLCB1cGxvYWREaXIpO1xuICB9LFxuICBmaWxlbmFtZTogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBjb25zdCBpZCA9IHV1aWR2NCgpO1xuICAgIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSk7XG4gICAgY2IobnVsbCwgYCR7aWR9JHtleHR9YCk7XG4gIH1cbn0pO1xuXG5jb25zdCB1cGxvYWQgPSBtdWx0ZXIoe1xuICBzdG9yYWdlLFxuICBsaW1pdHM6IHtcbiAgICBmaWxlU2l6ZTogcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CIHx8ICc1JykgKiAxMDI0ICogMTAyNFxuICB9LFxuICBmaWxlRmlsdGVyOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgIGlmIChmaWxlLm1pbWV0eXBlID09PSAnYXBwbGljYXRpb24vcGRmJyAmJiBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpLnRvTG93ZXJDYXNlKCkgPT09ICcucGRmJykge1xuICAgICAgY2IobnVsbCwgdHJ1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNiKG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpKTtcbiAgICB9XG4gIH1cbn0pO1xuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERihmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG4gICAgY29uc3QgZGF0YSA9IGF3YWl0IHBkZihidWZmZXIpO1xuICAgIHJldHVybiB7XG4gICAgICB0ZXh0OiBkYXRhLnRleHQsXG4gICAgICBwYWdlQ291bnQ6IGRhdGEubnVtcGFnZXMsXG4gICAgICBpbmZvOiBkYXRhLmluZm9cbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlVXBsb2FkKHJlcSwgcmVzKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgIGlmICghZmlsZSkge1xuICAgICAgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG4gICAgfVxuXG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgbWF4VXBsb2Fkc01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CIHx8ICc1Jyk7XG4gICAgY29uc3QgbWF4UERGcyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG5cbiAgICBjb25zdCBjbGVhbkZpbGVuYW1lID0gc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSk7XG5cbiAgICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBUb29NYW55UERGc0Vycm9yKG1heFBERnMpO1xuICAgIH1cblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBEdXBsaWNhdGVGaWxlRXJyb3IoY2xlYW5GaWxlbmFtZSk7XG4gICAgfVxuXG4gICAgY29uc3QgcGRmRGF0YSA9IGF3YWl0IHBhcnNlUERGKGZpbGUucGF0aCk7XG4gICAgY29uc3QgZG9jdW1lbnRJZCA9IHBhdGgucGFyc2UoZmlsZS5maWxlbmFtZSkubmFtZTtcbiAgICBjb25zdCBkb2N1bWVudFBhdGggPSBmaWxlLnBhdGg7XG5cbiAgICBjb25zdCBjaHVua3MgPSBjaHVua1BERkNvbnRlbnQoe1xuICAgICAgdGV4dDogcGRmRGF0YS50ZXh0LFxuICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICBkb2N1bWVudElkLFxuICAgICAgcGFnZU51bWJlcjogMVxuICAgIH0pO1xuXG4gICAgaWYgKGNodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQyMikuanNvbih7XG4gICAgICAgIGVycm9yOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLFxuICAgICAgICBjb2RlOiAnRU1QVFlfUERGJ1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG5cbiAgICBjb25zdCBlbWJlZGRpbmdzID0gW107XG4gICAgY29uc3QgcHJvZ3Jlc3NDYWxsYmFjayA9IChwcm9jZXNzZWQsIHRvdGFsKSA9PiB7XG4gICAgICBpZiAocmVxLmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MpIHtcbiAgICAgICAgcmVxLmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MuZW1pdChgcHJvZ3Jlc3NfJHtzZXNzaW9uSWR9YCwge1xuICAgICAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgICAgcHJvY2Vzc2VkLFxuICAgICAgICAgIHRvdGFsLFxuICAgICAgICAgIHN0YWdlOiAnZW1iZWRkaW5nJ1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9O1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpKyspIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGVtYmVkZGluZyA9IGF3YWl0IGdlbmVyYXRlRW1iZWRkaW5ncyhbY2h1bmtzW2ldXSk7XG4gICAgICAgIGlmIChlbWJlZGRpbmcgJiYgZW1iZWRkaW5nLmxlbmd0aCA+IDApIHtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IHV1aWR2NCgpLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiBlbWJlZGRpbmdbMF0uZW1iZWRkaW5nLFxuICAgICAgICAgICAgdGV4dDogY2h1bmtzW2ldLnRleHQsXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmtzW2ldLm1ldGFkYXRhXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBlbWJlZCBjaHVuayAke2l9OmAsIGVycm9yKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoZW1iZWRkaW5ncy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDUwMykuanNvbih7XG4gICAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnLFxuICAgICAgICBjb2RlOiAnRU1CRURESU5HX0ZBSUxFRCdcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGF3YWl0IGFkZFZlY3RvcnMoXG4gICAgICBjb2xsZWN0aW9uLFxuICAgICAgZW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gZS5lbWJlZGRpbmcpLFxuICAgICAgZW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICk7XG5cbiAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgIGlkOiBkb2N1bWVudElkLFxuICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiBwZGZEYXRhLnBhZ2VDb3VudCxcbiAgICAgIGNodW5rQ291bnQ6IGVtYmVkZGluZ3MubGVuZ3RoXG4gICAgfSk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZG9jdW1lbnQ6IHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgICBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHBkZkRhdGEucGFnZUNvdW50LFxuICAgICAgICBjaHVua0NvdW50OiBlbWJlZGRpbmdzLmxlbmd0aCxcbiAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBzZXNzaW9uSWRcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGlmIChyZXEuZmlsZSAmJiBmcy5leGlzdHNTeW5jKHJlcS5maWxlLnBhdGgpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKHJlcS5maWxlLnBhdGgpO1xuICAgIH1cbiAgICBjb25zb2xlLmVycm9yKCdVcGxvYWQgZXJyb3I6JywgZXJyb3IpO1xuXG4gICAgY29uc3Qgc3RhdHVzQ29kZSA9IGVycm9yLnN0YXR1c0NvZGUgfHwgNTAwO1xuICAgIHJlcy5zdGF0dXMoc3RhdHVzQ29kZSkuanNvbih7XG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1VQTE9BRF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdERvY3VtZW50c0hhbmRsZXIocmVxLCByZXMpIHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBkb2N1bWVudHMgPSBhd2FpdCBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0xpc3QgZG9jdW1lbnRzIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50cycsXG4gICAgICBjb2RlOiAnTElTVF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnQocmVxLCByZXMpIHtcbiAgY29uc3QgeyBkb2N1bWVudElkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGxldCBjb2xsZWN0aW9uO1xuICAgIGxldCBkZWxldGVkRnJvbVNlc3Npb24gPSBmYWxzZTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgY29uc3QgY291bnQgPSBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgICAgIGlmIChjb3VudCA+IDApIHtcbiAgICAgICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG4gICAgICAgICAgZGVsZXRlZEZyb21TZXNzaW9uID0gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIC8vIERlbGV0ZSBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gICAgICBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgLy8gTm90IGluIGdsb2JhbFxuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBkb2N1bWVudElkLFxuICAgICAgZGVsZXRlZEZyb206IGRlbGV0ZWRGcm9tU2Vzc2lvbiA/ICdzZXNzaW9uJyA6ICd1bmtub3duJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50JyxcbiAgICAgIGNvZGU6ICdERUxFVEVfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZVBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBgJHtkb2N1bWVudElkfS5wZGZgKTtcblxuICAgIGlmICghZnMuZXhpc3RzU3luYyhmaWxlUGF0aCkpIHtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7XG4gICAgICAgIGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLFxuICAgICAgICBjb2RlOiAnRklMRV9OT1RfRk9VTkQnXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgY29uc3QgZmlsZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVQYXRoKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgYGF0dGFjaG1lbnQ7IGZpbGVuYW1lPVwiJHtmaWxlbmFtZX1cImApO1xuXG4gICAgY29uc3Qgc3RyZWFtID0gZnMuY3JlYXRlUmVhZFN0cmVhbShmaWxlUGF0aCk7XG4gICAgc3RyZWFtLnBpcGUocmVzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gcmV0cmlldmUgZG9jdW1lbnQnLFxuICAgICAgY29kZTogJ1JFVFJJRVZFX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvdXBsb2FkJywgdXBsb2FkLnNpbmdsZSgnZmlsZScpLCBoYW5kbGVVcGxvYWQpO1xucm91dGVyLmdldCgnLycsIGxpc3REb2N1bWVudHNIYW5kbGVyKTtcbnJvdXRlci5kZWxldGUoJy86ZG9jdW1lbnRJZCcsIGRlbGV0ZURvY3VtZW50KTtcbnJvdXRlci5nZXQoJy86ZG9jdW1lbnRJZC9maWxlJywgZ2V0RG9jdW1lbnRGaWxlKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZ2V0R2xvYmFsQ29sbGVjdGlvbiwgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIHF1ZXJ5Q29sbGVjdGlvbiB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBlbWJlZFF1ZXJ5IH0gZnJvbSAnLi9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBUT1BfSyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LlRPUF9LKSB8fCA1O1xuY29uc3QgQ09WRVJBR0VfSElHSF9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LkNPVkVSQUdFX0hJR0hfVEhSRVNIT0xEKSB8fCAwLjc1O1xuY29uc3QgQ09WRVJBR0VfTUVESVVNX1RIUkVTSE9MRCA9IHBhcnNlRmxvYXQocHJvY2Vzcy5lbnYuQ09WRVJBR0VfTUVESVVNX1RIUkVTSE9MRCkgfHwgMC41NTtcblxuLy8gXHUyNzA1IENhY2hlIHJlc29sdmVkIGNvbGxlY3Rpb24gb2JqZWN0cyBcdTIwMTQgbmV2ZXIgaGl0IENocm9tYSBtb3JlIHRoYW4gb25jZSBwZXIgc2Vzc2lvblxubGV0IGNhY2hlZEdsb2JhbENvbGxlY3Rpb24gPSBudWxsO1xuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlR2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFjYWNoZWRHbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY2FjaGVkR2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgfVxuICByZXR1cm4gY2FjaGVkR2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuYXN5bmMgZnVuY3Rpb24gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgfVxuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgfVxuICAgIHJldHVybiBjb2xsZWN0aW9uO1xuICB9IGNhdGNoIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxufVxuXG5mdW5jdGlvbiBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLID0gVE9QX0spIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHsgbGV2ZWw6ICdsb3cnLCBzY29yZTogMCwgcmVhc29uOiAnTm8gcmVzdWx0cyBmb3VuZCcgfTtcbiAgfVxuXG4gIGNvbnN0IHRvcFJlc3VsdHMgPSByZXN1bHRzLnNsaWNlKDAsIHRvcEspO1xuICBjb25zdCBzY29yZXMgPSB0b3BSZXN1bHRzLm1hcChyID0+IHIuc2NvcmUpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG5cbiAgbGV0IGxldmVsO1xuICBsZXQgcmVhc29uO1xuXG4gIGlmIChhdmdTY29yZSA+PSBDT1ZFUkFHRV9ISUdIX1RIUkVTSE9MRCkge1xuICAgIGxldmVsID0gJ2hpZ2gnO1xuICAgIHJlYXNvbiA9ICdIaWdoIGNvbmZpZGVuY2UgaW4gcmV0cmlldmVkIGNvbnRleHQnO1xuICB9IGVsc2UgaWYgKGF2Z1Njb3JlID49IENPVkVSQUdFX01FRElVTV9USFJFU0hPTEQpIHtcbiAgICBsZXZlbCA9ICdtZWRpdW0nO1xuICAgIHJlYXNvbiA9ICdNb2RlcmF0ZSBjb25maWRlbmNlIGluIHJldHJpZXZlZCBjb250ZXh0JztcbiAgfSBlbHNlIHtcbiAgICBsZXZlbCA9ICdsb3cnO1xuICAgIHJlYXNvbiA9ICdJbnN1ZmZpY2llbnQgcmVsZXZhbnQgaW5mb3JtYXRpb24gZm91bmQnO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBsZXZlbCxcbiAgICBzY29yZTogYXZnU2NvcmUsXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcyksXG4gICAgYm90dG9tU2NvcmU6IE1hdGgubWluKC4uLnNjb3JlcyksXG4gICAgcmVhc29uXG4gIH07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiByZXRyaWV2ZUZvclF1ZXJ5KHF1ZXJ5LCBzZXNzaW9uSWQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0b3BLID0gb3B0aW9ucy50b3BLIHx8IFRPUF9LO1xuICBjb25zdCBpbmNsdWRlR2xvYmFsID0gb3B0aW9ucy5pbmNsdWRlR2xvYmFsICE9PSBmYWxzZTtcblxuICB0cnkge1xuICAgIC8vIFx1MjcwNSBSdW4gZW1iZWRkaW5nICsgYm90aCBjb2xsZWN0aW9uIGZldGNoZXMgaW4gcGFyYWxsZWxcbiAgICAvLyBDb2xsZWN0aW9ucyBhcmUgc2VydmVkIGZyb20gY2FjaGUgYWZ0ZXIgdGhlIGZpcnN0IGNhbGwgXHUyMDE0IHplcm8gQ2hyb21hIHJvdW5kLXRyaXBzXG4gICAgY29uc3QgW3F1ZXJ5RW1iZWRkaW5nLCBnbG9iYWxDb2xsZWN0aW9uLCBzZXNzaW9uQ29sbGVjdGlvbl0gPSBhd2FpdCBQcm9taXNlLmFsbChbXG4gICAgICBlbWJlZFF1ZXJ5KHF1ZXJ5KSxcbiAgICAgIGluY2x1ZGVHbG9iYWwgPyBnZXRPckNhY2hlR2xvYmFsQ29sbGVjdGlvbigpIDogUHJvbWlzZS5yZXNvbHZlKG51bGwpLFxuICAgICAgc2Vzc2lvbklkID8gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkgOiBQcm9taXNlLnJlc29sdmUobnVsbClcbiAgICBdKTtcblxuICAgIC8vIFx1MjcwNSBRdWVyeSBib3RoIGNvbGxlY3Rpb25zIGluIHBhcmFsbGVsXG4gICAgY29uc3QgcXVlcnlQcm9taXNlcyA9IFtdO1xuXG4gICAgaWYgKGdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICAgIHF1ZXJ5UHJvbWlzZXMucHVzaChcbiAgICAgICAgcXVlcnlDb2xsZWN0aW9uKGdsb2JhbENvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLKVxuICAgICAgICAgIC50aGVuKHJlc3VsdHMgPT4gKHsgdHlwZTogJ2dsb2JhbCcsIHJlc3VsdHMgfSkpXG4gICAgICAgICAgLmNhdGNoKCgpID0+ICh7IHR5cGU6ICdnbG9iYWwnLCByZXN1bHRzOiBbXSB9KSlcbiAgICAgICk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb25Db2xsZWN0aW9uKSB7XG4gICAgICBxdWVyeVByb21pc2VzLnB1c2goXG4gICAgICAgIHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAgICAgLnRoZW4ocmVzdWx0cyA9PiAoeyB0eXBlOiAnc2Vzc2lvbicsIHJlc3VsdHMgfSkpXG4gICAgICAgICAgLmNhdGNoKCgpID0+ICh7IHR5cGU6ICdzZXNzaW9uJywgcmVzdWx0czogW10gfSkpXG4gICAgICApO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5UmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKHF1ZXJ5UHJvbWlzZXMpO1xuXG4gICAgY29uc3QgYWxsUmVzdWx0cyA9IFtdO1xuICAgIGZvciAoY29uc3QgeyB0eXBlLCByZXN1bHRzOiB0eXBlUmVzdWx0cyB9IG9mIHF1ZXJ5UmVzdWx0cykge1xuICAgICAgZm9yIChjb25zdCByZXN1bHQgb2YgdHlwZVJlc3VsdHMpIHtcbiAgICAgICAgYWxsUmVzdWx0cy5wdXNoKHsgLi4ucmVzdWx0LCBzb3VyY2VfdHlwZTogdHlwZSB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBhbGxSZXN1bHRzLnNvcnQoKGEsIGIpID0+IGIuc2NvcmUgLSBhLnNjb3JlKTtcbiAgICBjb25zdCB0b3BSZXN1bHRzID0gYWxsUmVzdWx0cy5zbGljZSgwLCB0b3BLKTtcbiAgICBjb25zdCBjb3ZlcmFnZSA9IGNhbGN1bGF0ZUNvdmVyYWdlKHRvcFJlc3VsdHMsIHRvcEspO1xuXG4gICAgcmV0dXJuIHsgcmVzdWx0czogdG9wUmVzdWx0cywgY292ZXJhZ2UsIHF1ZXJ5RW1iZWRkaW5nIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8vIFx1MjcwNSBDYWxsIHRoaXMgYWZ0ZXIgYSB1c2VyIHVwbG9hZHMgYSBkb2N1bWVudCB0byBhIHNlc3Npb25cbi8vIHNvIHRoZSBuZXh0IHF1ZXJ5IGZldGNoZXMgdGhlIHVwZGF0ZWQgY29sbGVjdGlvbiBmcmVzaFxuZXhwb3J0IGZ1bmN0aW9uIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCkge1xuICBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMsIG1heFRva2VucyA9IDcwMDApIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG5cbiAgbGV0IHRvdGFsVG9rZW5zID0gMDtcbiAgY29uc3QgbWF4VG9rZW5zUGVyQ2hhciA9IDQ7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIG1heFRva2Vuc1BlckNoYXI7XG5cbiAgICBpZiAodG90YWxUb2tlbnMgKyB0b2tlbkVzdGltYXRlID4gbWF4VG9rZW5zKSB7XG4gICAgICBicmVhaztcbiAgICB9XG5cbiAgICB0b3RhbFRva2VucyArPSB0b2tlbkVzdGltYXRlO1xuXG4gICAgY29uc3Qgc291cmNlTGFiZWwgPSByZXN1bHQuc291cmNlX3R5cGUgPT09ICdnbG9iYWwnID8gJ1tTZWVkIERvY3VtZW50XScgOiAnW1Nlc3Npb24gVXBsb2FkXSc7XG4gICAgY29uc3QgY2l0YXRpb24gPSBgWyR7aSArIDF9XSAke3NvdXJjZUxhYmVsfSAke3Jlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSB8fCAnVW5rbm93bid9YDtcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuXG4gICAgY29udGV4dFBhcnRzLnB1c2goYCR7Y2l0YXRpb259JHtwYWdlfTpcXG4ke3Jlc3VsdC50ZXh0fWApO1xuICB9XG5cbiAgcmV0dXJuIGNvbnRleHRQYXJ0cy5qb2luKCdcXG5cXG4tLS1cXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS5sZXZlbCA9PT0gJ2xvdycgJiYgY292ZXJhZ2Uuc2NvcmUgPiAwO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9OyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IG1lbW9yeU1hcCA9IG5ldyBNYXAoKTtcbmNvbnN0IERFRkFVTFRfTUVNT1JZX1dJTkRPVyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IDEwO1xuXG5leHBvcnQgZnVuY3Rpb24gaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpIHtcbiAgaWYgKCFtZW1vcnlNYXAuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBtZW1vcnlNYXAuc2V0KHNlc3Npb25JZCwge1xuICAgICAgdHVybnM6IFtdLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBtZXRhZGF0YSA9IHt9KSB7XG4gIGNvbnN0IG1lbW9yeSA9IG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IG1heFR1cm5zID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuXG4gIGNvbnN0IHR1cm4gPSB7XG4gICAgaWQ6IGB0dXJuXyR7RGF0ZS5ub3coKX1fJHtNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zdWJzdHIoMiwgOSl9YCxcbiAgICByb2xlLFxuICAgIGNvbnRlbnQsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIC4uLm1ldGFkYXRhXG4gIH07XG5cbiAgbWVtb3J5LnR1cm5zLnB1c2godHVybik7XG5cbiAgLy8gS2VlcCBvbmx5IHRoZSBsYXN0IE4gdHVybnNcbiAgaWYgKG1lbW9yeS50dXJucy5sZW5ndGggPiBtYXhUdXJucykge1xuICAgIG1lbW9yeS50dXJucyA9IG1lbW9yeS50dXJucy5zbGljZSgtbWF4VHVybnMpO1xuICB9XG5cbiAgcmV0dXJuIHR1cm47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCBtYXhUdXJucyA9IG51bGwpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IGxpbWl0ID0gbWF4VHVybnMgfHwgcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuXG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiAnJztcbiAgfVxuXG4gIGNvbnN0IGZvcm1hdHRlZCA9IHR1cm5zLm1hcCh0ID0+IHtcbiAgICBjb25zdCBwcmVmaXggPSB0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyOicgOiAnQXNzaXN0YW50Oic7XG4gICAgcmV0dXJuIGAke3ByZWZpeH0gJHt0LmNvbnRlbnR9YDtcbiAgfSkuam9pbignXFxuXFxuJyk7XG5cbiAgcmV0dXJuIGZvcm1hdHRlZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFyTWVtb3J5KHNlc3Npb25JZCkge1xuICBtZW1vcnlNYXAuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnlTdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIHJldHVybiB7XG4gICAgdHVybkNvdW50OiBtZW1vcnkudHVybnMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogbWVtb3J5LmNyZWF0ZWRBdCxcbiAgICBsYXN0VHVybkF0OiBtZW1vcnkudHVybnMubGVuZ3RoID4gMCA/IG1lbW9yeS50dXJuc1ttZW1vcnkudHVybnMubGVuZ3RoIC0gMV0udGltZXN0YW1wIDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBjaXRhdGlvbnMgPSBbXSwgY292ZXJhZ2UgPSBudWxsKSB7XG4gIHJldHVybiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwge1xuICAgIGNpdGF0aW9ucyxcbiAgICBjb3ZlcmFnZSxcbiAgICBoYXNDaXRhdGlvbnM6IGNpdGF0aW9ucy5sZW5ndGggPiAwXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdFVzZXJNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ3VzZXInKSB7XG4gICAgICByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RBc3Npc3RhbnRNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHtcbiAgICAgIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcHJvbXB0U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZm9ybWF0TWVtb3J5Rm9yUHJvbXB0IH0gZnJvbSAnLi9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGZvcm1hdENvbnRleHRGb3JQcm9tcHQsIGNhbGN1bGF0ZUNvdmVyYWdlIH0gZnJvbSAnLi9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcblxuY29uc3QgU1lTVEVNX0lOU1RSVUNUSU9OID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudCB0aGF0IGFuc3dlcnMgcXVlc3Rpb25zIGJhc2VkIG9uIGluZGV4ZWQgZG9jdW1lbnRzIHdoZW4gYXZhaWxhYmxlLlxuXG5SVUxFUzpcbjEuIFdoZW4gY29udGV4dCBpcyBwcm92aWRlZCwgYW5zd2VyIGJhc2VkIG9uIGl0IGFuZCBjaXRlIHNvdXJjZXMgdXNpbmcgWzFdLCBbMl0sIGV0Yy5cbjIuIEZvciBnZW5lcmFsIGNvbnZlcnNhdGlvbiAoZ3JlZXRpbmdzLCBjbGFyaWZ5aW5nIHF1ZXN0aW9ucywgc21hbGwgdGFsayksIHJlc3BvbmQgbmF0dXJhbGx5IGFuZCBoZWxwZnVsbHkgd2l0aG91dCByZXF1aXJpbmcgY29udGV4dC5cbjMuIElmIGEgZmFjdHVhbCBxdWVzdGlvbiBpcyBhc2tlZCBidXQgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5IGFuZCBzdWdnZXN0IHVwbG9hZGluZyByZWxldmFudCBkb2N1bWVudHMuXG40LiBCZSBjb25jaXNlIGJ1dCB0aG9yb3VnaC4gVXNlIGJ1bGxldCBwb2ludHMgb3IgbnVtYmVyZWQgbGlzdHMgZm9yIGNvbXBsZXggYW5zd2Vycy5cbjUuIE1haW50YWluIGNvbnZlcnNhdGlvbiBjb250aW51aXR5IGJ1dCBkb24ndCByZXBlYXQgaW5mb3JtYXRpb24gdW5uZWNlc3NhcmlseS5cbjYuIEZvcm1hdCByZXNwb25zZXMgaW4gY2xlYXIsIHJlYWRhYmxlIG1hcmtkb3duLmA7XG5cbmNvbnN0IFJFRlVTQUxfTUVTU0FHRSA9IFwiSSBkb24ndCBoYXZlIGVub3VnaCBpbmZvcm1hdGlvbiBpbiB0aGUga25vd2xlZGdlIGJhc2UgdG8gYW5zd2VyIHRoYXQgY29uZmlkZW50bHkuIFRyeSB1cGxvYWRpbmcgcmVsZXZhbnQgZG9jdW1lbnRzLCBvciBhc2sgbWUgYSBnZW5lcmFsIHF1ZXN0aW9uLlwiO1xuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkUHJvbXB0KHsgcXVlcnksIGNvbnRleHQsIG1lbW9yeUNvbnRleHQsIGNvdmVyYWdlIH0pIHtcbiAgY29uc3QgcGFydHMgPSBbXTtcblxuICAvLyBTeXN0ZW0gaW5zdHJ1Y3Rpb25cbiAgcGFydHMucHVzaChTWVNURU1fSU5TVFJVQ1RJT04pO1xuXG4gIC8vIFBhc3QgY29udmVyc2F0aW9uIGlmIGF2YWlsYWJsZVxuICBpZiAobWVtb3J5Q29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChtZW1vcnlDb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgfVxuXG4gIC8vIFJldHJpZXZlZCBjb250ZXh0XG4gIGlmIChjb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFJFTEVWQU5UIENPTlRFWFQgRlJPTSBLTk9XTEVER0UgQkFTRSAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKGNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgQ09OVEVYVCAtLS1cXG4nKTtcbiAgfVxuXG4gIC8vIEN1cnJlbnQgcXVlc3Rpb25cbiAgcGFydHMucHVzaCgnXFxuXFxuLS0tIENVUlJFTlQgUVVFU1RJT04gLS0tXFxuJyk7XG4gIHBhcnRzLnB1c2gocXVlcnkpO1xuICBwYXJ0cy5wdXNoKCdcXG5cXG5SZW1lbWJlcjogQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIFVzZSBbMV0sIFsyXSwgZXRjLiBmb3IgY2l0YXRpb25zLiBJZiB0aGUgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5LicpO1xuXG4gIHJldHVybiBwYXJ0cy5qb2luKCcnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkU3RyZWFtaW5nUHJvbXB0KHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpO1xuICBjb25zdCBjb250ZXh0U3RyaW5nID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXRyaWV2ZWRSZXN1bHRzKTtcblxuICByZXR1cm4gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRTdHJpbmcsXG4gICAgbWVtb3J5Q29udGV4dCxcbiAgICBjb3ZlcmFnZTogY2FsY3VsYXRlQ292ZXJhZ2UocmV0cmlldmVkUmVzdWx0cylcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsUmVzcG9uc2UoKSB7XG4gIHJldHVybiBSRUZVU0FMX01FU1NBR0U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTeXN0ZW1JbnN0cnVjdGlvbigpIHtcbiAgcmV0dXJuIFNZU1RFTV9JTlNUUlVDVElPTjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkV2ViU2VhcmNoUHJvbXB0KHF1ZXJ5LCBncm91bmRpbmdNZXRhZGF0YSkge1xuICByZXR1cm4gYEJhc2VkIG9uIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBmb2xsb3dpbmcgcXVlc3Rpb246ICR7cXVlcnl9XG5cbkd1aWRlbGluZXM6XG4tIFVzZSBpbmZvcm1hdGlvbiBmcm9tIHRoZSB3ZWIgc2VhcmNoXG4tIFByb3ZpZGUgc291cmNlcy9VUkxzIHdoZXJlIGFwcGxpY2FibGVcbi0gQmUgY29uY2lzZSBhbmQgaW5mb3JtYXRpdmVcbi0gSWYgbXVsdGlwbGUgc291cmNlcyBhZ3JlZSBvciBjb250cmFkaWN0LCBtZW50aW9uIHRoYXRgO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0R2VuZXJhdGlvbkNvbmZpZyhjdXN0b21Db25maWcgPSB7fSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgdG9wUDogMC45NSxcbiAgICB0b3BLOiA0MCxcbiAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDgsXG4gICAgLi4uY3VzdG9tQ29uZmlnXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0U291cmNlc0Zyb21SZXNwb25zZShyZXNwb25zZSkge1xuICAvLyBFeHRyYWN0IGNpdGF0aW9uIHBhdHRlcm5zIGxpa2UgWzFdLCBbMl0sIGV0Yy5cbiAgY29uc3QgY2l0YXRpb25QYXR0ZXJuID0gL1xcWyhcXGQrKVxcXS9nO1xuICBjb25zdCBjaXRhdGlvbnMgPSBuZXcgU2V0KCk7XG4gIGxldCBtYXRjaDtcblxuICB3aGlsZSAoKG1hdGNoID0gY2l0YXRpb25QYXR0ZXJuLmV4ZWMocmVzcG9uc2UpKSAhPT0gbnVsbCkge1xuICAgIGNpdGF0aW9ucy5hZGQocGFyc2VJbnQobWF0Y2hbMV0pKTtcbiAgfVxuXG4gIHJldHVybiBBcnJheS5mcm9tKGNpdGF0aW9ucykuc29ydCgoYSwgYikgPT4gYSAtIGIpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuZXJhdGl2ZUFJIH0gZnJvbSAnQGdvb2dsZS9nZW5lcmF0aXZlLWFpJztcbmltcG9ydCB7IGJ1aWxkUHJvbXB0LCBnZXRSZWZ1c2FsUmVzcG9uc2UgfSBmcm9tICcuL3Byb21wdFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgTExNVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbi8vIFx1MjcwNSBMYXp5IFx1MjAxNCByZWFkIGluc2lkZSB0aGUgZnVuY3Rpb24sIG5vdCBhdCBtb2R1bGUgdG9wIGxldmVsXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGNvbnN0IGFwaUtleSA9IHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZO1xuICAgIGlmICghYXBpS2V5KSB0aHJvdyBuZXcgRXJyb3IoJ0dFTUlOSV9BUElfS0VZIGlzIHVuZGVmaW5lZCcpO1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShhcGlLZXkpO1xuICB9XG4gIHJldHVybiBnZW5BSTtcbn1cblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuY29uc3QgRkFMTEJBQ0tfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfRkFMTEJBQ0sgfHwgJ2dlbWluaS0yLjUtZmxhc2gnO1xuY29uc3QgRklSU1RfVE9LRU5fVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9GSVJTVF9UT0tFTl9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCAxMjAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9SRVFVRVNUX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDQ1MDAwO1xuXG5sZXQgcHJpbWFyeU1vZGVsID0gbnVsbDtcbmxldCBmYWxsYmFja01vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsKCkge1xuICBpZiAoIXByaW1hcnlNb2RlbCkge1xuICAgIHByaW1hcnlNb2RlbCA9IGdldEdlbkFJKCkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIHByaW1hcnlNb2RlbDtcbn1cblxuZnVuY3Rpb24gZ2V0RmFsbGJhY2tNb2RlbCgpIHtcbiAgaWYgKCFmYWxsYmFja01vZGVsKSB7XG4gICAgZmFsbGJhY2tNb2RlbCA9IGdldEdlbkFJKCkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IEZBTExCQUNLX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBmYWxsYmFja01vZGVsO1xufVxuXG5hc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVdpdGhNb2RlbChtb2RlbCwgcHJvbXB0LCBzaWduYWwpIHtcbiAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfVxuICB9LCB7IHNpZ25hbCB9KTtcblxuICByZXR1cm4gcmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlUmVzcG9uc2UocHJvbXB0KSB7XG4gIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG4gIGNvbnN0IHRpbWVvdXRJZCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBSRVFVRVNUX1RJTUVPVVQpO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZ2V0UHJpbWFyeU1vZGVsKCkuZ2VuZXJhdGVDb250ZW50KHtcbiAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG4gICAgcmV0dXJuIHJlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG4gIH0gY2F0Y2ggKHByaW1hcnlFcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1ByaW1hcnkgbW9kZWwgZmFpbGVkOicsIHByaW1hcnlFcnJvci5tZXNzYWdlKTtcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBmYWxsYmFja1Jlc3VsdCA9IGF3YWl0IGdldEZhbGxiYWNrTW9kZWwoKS5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG4gICAgICByZXR1cm4gZmFsbGJhY2tSZXN1bHQucmVzcG9uc2UudGV4dCgpO1xuICAgIH0gY2F0Y2ggKGZhbGxiYWNrRXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhbGxiYWNrIG1vZGVsIGFsc28gZmFpbGVkOicsIGZhbGxiYWNrRXJyb3IubWVzc2FnZSk7XG4gICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVJlc3BvbnNlKHByb21wdCkge1xuICBsZXQgbW9kZWwgPSBnZXRQcmltYXJ5TW9kZWwoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICB0cnkge1xuICAgICAgLy8gXHUyNzA1IEZJWDogQ3JlYXRlIEFib3J0Q29udHJvbGxlciBwZXIgYXR0ZW1wdCBmb3IgdGltZW91dCBzaWduYWxsaW5nXG4gICAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgbGV0IGZpcnN0VG9rZW4gPSB0cnVlO1xuXG4gICAgICAvLyBcdTI3MDUgRklYOiBVc2UgY29udHJvbGxlci5hYm9ydCgpIGluc3RlYWQgb2YgdGhyb3cgaW5zaWRlIHNldFRpbWVvdXRcbiAgICAgIC8vICh0aHJvdyBpbnNpZGUgc2V0VGltZW91dCBpcyB1bmNhdWdodCBhbmQgc2lsZW50bHkga2lsbHMgdGhlIHN0cmVhbSlcbiAgICAgIGNvbnN0IGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgICAgLy8gXHUyNzA1IEZJWDogQ2hlY2sgYWJvcnQgc2lnbmFsIG9uIGVhY2ggaXRlcmF0aW9uXG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ0ZpcnN0IHRva2VuIHRpbWVvdXQgXHUyMDE0IG5vIHJlc3BvbnNlIGZyb20gbW9kZWwnKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHRleHQgPSBjaHVuay50ZXh0KCk7XG4gICAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgICAgaWYgKGZpcnN0VG9rZW4pIHtcbiAgICAgICAgICAgIGZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7IC8vIGdvdCBmaXJzdCB0b2tlbiwgY2FuY2VsIHRpbWVvdXRcbiAgICAgICAgICB9XG4gICAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgLy8gU3RyZWFtIGNvbXBsZXRlZCBuYXR1cmFsbHkgXHUyMDE0IGNsZWFuIHVwIHRpbWVvdXRcbiAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICByZXR1cm4geyBzdWNjZXNzOiB0cnVlIH07XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgcmV0cmllcysrO1xuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIC8vIFN3aXRjaCB0byBmYWxsYmFjayBtb2RlbCBvbiByZXRyeVxuICAgICAgbW9kZWwgPSBnZXRGYWxsYmFja01vZGVsKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtQ2hhdFJlc3BvbnNlKHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IG1lbW9yeVNlcnZpY2UgPyBtZW1vcnlTZXJ2aWNlLmZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIDogJyc7XG4gIGNvbnN0IGNvbnRleHRMaXN0ID0gcmV0cmlldmVkUmVzdWx0cyB8fCBbXTtcbiAgY29uc3QgY29udGV4dFRleHQgPSBjb250ZXh0TGlzdC5tYXAoKHIsIGkpID0+XG4gICAgYFske2kgKyAxfV0gJHtyLm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ306ICR7ci50ZXh0fWBcbiAgKS5qb2luKCdcXG5cXG4nKTtcblxuICBjb25zdCBwcm9tcHQgPSBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFRleHQsXG4gICAgbWVtb3J5Q29udGV4dCxcbiAgICBjb3ZlcmFnZTogeyBsZXZlbDogJ2hpZ2gnIH1cbiAgfSk7XG5cbiAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gIHRyeSB7XG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgeWllbGQgY2h1bms7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgeWllbGQgY2h1bms7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICB5aWVsZCB7IHR5cGU6ICdjb21wbGV0ZScsIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxUZXh0KCkge1xuICByZXR1cm4gZ2V0UmVmdXNhbFJlc3BvbnNlKCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVdlYlNlYXJjaFJlc3BvbnNlKHF1ZXJ5LCBncm91bmRpbmdDb250ZW50KSB7XG4gIGNvbnN0IG1vZGVsID0gZ2V0UHJpbWFyeU1vZGVsKCk7XG5cbiAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICBjb250ZW50czogW3tcbiAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgIHBhcnRzOiBbeyB0ZXh0OiBgQmFzZWQgb24gdGhlc2Ugd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIHF1ZXN0aW9uOiBcIiR7cXVlcnl9XCJcXG5cXG4ke2dyb3VuZGluZ0NvbnRlbnR9YCB9XVxuICAgIH1dLFxuICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfSxcbiAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICB9KTtcblxuICBjb25zdCByZXNwb25zZSA9IHJlc3VsdC5yZXNwb25zZTtcbiAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZS5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gIHJldHVybiB7XG4gICAgdGV4dCxcbiAgICBncm91bmRpbmdNZXRhZGF0YSxcbiAgICBncm91bmRpbmdDaHVua3M6IGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MgfHwgW11cbiAgfTtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgcmV0cmlldmVGb3JRdWVyeSwgZ2VuZXJhdGVDaXRhdGlvbnMsIHNob3VsZFNob3dSZWZ1c2FsIH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5pbXBvcnQgeyBzdHJlYW1SZXNwb25zZSwgZ2V0UmVmdXNhbFRleHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBnZXRSZWNlbnRUdXJucywgZ2V0TGFzdFVzZXJNZXNzYWdlIH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24gfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlQ2hhdFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5LCBzZXNzaW9uSWQ6IHByb3ZpZGVkU2Vzc2lvbklkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX1FVRVJZJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcHJvdmlkZWRTZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkID0gdXVpZHY0KCk7XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGNvbnN0IHVzZXJUdXJuID0gYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIC8vIFx1MjcwNSBGb3JjZSBmbHVzaCB0aHJvdWdoIFZpdGUncyBtaWRkbGV3YXJlIGJ1ZmZlciBpbW1lZGlhdGVseVxuICBpZiAodHlwZW9mIHJlcy5mbHVzaCA9PT0gJ2Z1bmN0aW9uJykgcmVzLmZsdXNoKCk7XG59O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgY292ZXJhZ2U6IGNvdmVyYWdlLmxldmVsLFxuICAgICAgY292ZXJhZ2VTY29yZTogY292ZXJhZ2Uuc2NvcmVcbiAgICB9KTtcblxuICAgIGlmIChzaG91bGRTaG93UmVmdXNhbChjb3ZlcmFnZSkpIHtcbiAgICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuICAgICAgYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCAnYXNzaXN0YW50JywgZ2V0UmVmdXNhbFRleHQoKSwgY2l0YXRpb25zLCBjb3ZlcmFnZSk7XG4gICAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgICBhbnN3ZXJJZCxcbiAgICAgICAgcmVzcG9uc2U6IGdldFJlZnVzYWxUZXh0KCksXG4gICAgICAgIGNpdGF0aW9ucyxcbiAgICAgICAgY292ZXJhZ2UsXG4gICAgICAgIGFjdGlvbjogJ3JlZnVzYWwnXG4gICAgICB9KTtcbiAgICAgIHJlcy5lbmQoKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdnZW5lcmF0aW5nJywgbWVzc2FnZTogJ0dlbmVyYXRpbmcgcmVzcG9uc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgNSlcbiAgICAgIC5tYXAodCA9PiBgJHt0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyJyA6ICdBc3Npc3RhbnQnfTogJHt0LmNvbnRlbnR9YClcbiAgICAgIC5qb2luKCdcXG5cXG4nKTtcblxuICAgIC8vIFx1MjcwNSBGSVg6IEJ1aWxkIHByb21wdCBiYXNlZCBvbiB3aGV0aGVyIGNvbnRleHQgZXhpc3RzXG4gICAgbGV0IHByb21wdDtcblxuICAgIGlmIChyZXN1bHRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IGNvbnRleHRUZXh0ID0gcmVzdWx0cy5tYXAoKHIsIGkpID0+XG4gICAgICAgIGBbJHtpICsgMX1dICR7ci5tZXRhZGF0YS5maWxlbmFtZSB8fCAnU291cmNlJ306ICR7ci50ZXh0fWBcbiAgICAgICkuam9pbignXFxuXFxuJyk7XG5cbiAgICAgIHByb21wdCA9IGBZb3UgYXJlIGEgaGVscGZ1bCBBSSBLbm93bGVkZ2UgQXNzaXN0YW50LiBBbnN3ZXIgYmFzZWQgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQgZG9jdW1lbnRzLlxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dH1cblxuJHttZW1vcnlDb250ZXh0ID8gYENPTlZFUlNBVElPTiBISVNUT1JZOlxcbiR7bWVtb3J5Q29udGV4dH1cXG5cXG5gIDogJyd9Q1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1cblxuQW5zd2VyIGNvbmNpc2VseSBhbmQgY2l0ZSBzb3VyY2VzIHVzaW5nIFsxXSwgWzJdIGV0Yy4gcmVmZXJyaW5nIHRvIHRoZSBjb250ZXh0IG51bWJlcnMgYWJvdmUuYDtcblxuICAgIH0gZWxzZSB7XG4gICAgICAgLy8gXHUyNzA1IE5vIGNvbnRleHQgXHUyMDE0IGdyZWV0IG5hdHVyYWxseSBidXQgZG9uJ3QgYW5zd2VyIGtub3dsZWRnZSBxdWVzdGlvbnNcbiAgcHJvbXB0ID0gYFlvdSBhcmUgYSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgc3RyaWN0bHkgYmFzZWQgb24gdXBsb2FkZWQgZG9jdW1lbnRzLlxuXG4ke21lbW9yeUNvbnRleHQgPyBgQ09OVkVSU0FUSU9OIEhJU1RPUlk6XFxuJHttZW1vcnlDb250ZXh0fVxcblxcbmAgOiAnJ31DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fVxuXG5SVUxFUzpcbi0gRm9yIGdyZWV0aW5ncyBvciBzbWFsbCB0YWxrIChlLmcuIFwiaGlcIiwgXCJoZWxsb1wiLCBcImhvdyBhcmUgeW91XCIpLCByZXNwb25kIGJyaWVmbHkgYW5kIHdhcm1seS5cbi0gRm9yIEFOWSBmYWN0dWFsLCB0ZWNobmljYWwsIG9yIGtub3dsZWRnZS1iYXNlZCBxdWVzdGlvbiwgZG8gTk9UIGF0dGVtcHQgdG8gYW5zd2VyIGl0LiBJbnN0ZWFkLCB0ZWxsIHRoZSB1c2VyIHRoYXQgbm8gZG9jdW1lbnRzIGhhdmUgYmVlbiB1cGxvYWRlZCB5ZXQgYW5kIGludml0ZSB0aGVtIHRvIHVwbG9hZCByZWxldmFudCBkb2N1bWVudHMgc28geW91IGNhbiBwcm92aWRlIGEgZ3JvdW5kZWQgYW5zd2VyLlxuLSBOZXZlciB3cml0ZSBjb2RlLCBleHBsYWluIGdlbmVyYWwgY29uY2VwdHMsIG9yIGFuc3dlciBmcm9tIHlvdXIgb3duIHRyYWluaW5nIGtub3dsZWRnZS5gO1xufVxuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgICAgLy8gXHUyNzA1IEFkZCBhIHNtYWxsIGRlbGF5IGJldHdlZW4gdG9rZW5zIGZvciBuYXR1cmFsIHN0cmVhbWluZyBmZWVsXG4gICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIDkwKSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ2Fzc2lzdGFudCcsIGZ1bGxSZXNwb25zZSwgY2l0YXRpb25zLCBjb3ZlcmFnZSk7XG5cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgYW5zd2VySWQsXG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBzb3VyY2VzOiByZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICAgIGZpbGVuYW1lOiByLm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgICBleGNlcnB0OiByLnRleHQuc2xpY2UoMCwgMjAwKSxcbiAgICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgICAgfSkpXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHtcbiAgICAgIG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ0NIQVRfRVJST1InXG4gICAgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAxMCk7XG5cbiAgZm9yIChjb25zdCB0dXJuIG9mIHJlY2VudFR1cm5zKSB7XG4gICAgaWYgKHR1cm4uaWQgPT09IGFuc3dlcklkIHx8IHR1cm4uY2l0YXRpb25zPy5sZW5ndGggPiAwKSB7XG4gICAgICByZXR1cm4gcmVzLmpzb24oe1xuICAgICAgICBzb3VyY2VzOiB0dXJuLmNpdGF0aW9ucyB8fCBbXVxuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oe1xuICAgIGVycm9yOiAnU291cmNlcyBub3QgZm91bmQgZm9yIHRoaXMgYW5zd2VyJyxcbiAgICBjb2RlOiAnU09VUkNFU19OT1RfRk9VTkQnXG4gIH0pO1xufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZUNoYXRTdHJlYW0pO1xucm91dGVyLmdldCgnL3NvdXJjZXMvOmFuc3dlcklkJywgZ2V0U291cmNlcyk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjsiLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuY29uc3QgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZKTtcblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuXG5sZXQgbW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRNb2RlbCgpIHtcbiAgaWYgKCFtb2RlbCkge1xuICAgIG1vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIG1vZGVsO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcGVyZm9ybVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICAvLyBFeHRyYWN0IHNlYXJjaCBxdWVyaWVzIGFuZCBzb3VyY2VzXG4gICAgY29uc3Qgd2ViU2VhcmNoUXVlcmllcyA9IFtdO1xuICAgIGNvbnN0IHdlYlNvdXJjZXMgPSBbXTtcblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGNodW5rIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoY2h1bmsud2ViKSB7XG4gICAgICAgICAgd2ViU291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogY2h1bmsud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBjaHVuay53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8ud2ViU2VhcmNoUXVlcmllcykge1xuICAgICAgd2ViU2VhcmNoUXVlcmllcy5wdXNoKC4uLmdyb3VuZGluZ01ldGFkYXRhLndlYlNlYXJjaFF1ZXJpZXMpO1xuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICB0ZXh0LFxuICAgICAgc291cmNlczogd2ViU291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHdlYlNlYXJjaFF1ZXJpZXMsXG4gICAgICByYXdNZXRhZGF0YTogZ3JvdW5kaW5nTWV0YWRhdGFcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSB0ZXh0O1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlPy5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgY29uc3Qgc291cmNlcyA9IFtdO1xuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChpdGVtLndlYikge1xuICAgICAgICAgIHNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGl0ZW0ud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBpdGVtLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQge1xuICAgICAgdHlwZTogJ2NvbXBsZXRlJyxcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbWluZyBlcnJvcjonLCBlcnJvcik7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdFdlYlNlYXJjaFJlc3BvbnNlKHJlc3VsdCkge1xuICByZXR1cm4ge1xuICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMubWFwKHMgPT4gKHtcbiAgICAgIHVyaTogcy51cmksXG4gICAgICB0aXRsZTogcy50aXRsZSxcbiAgICAgIHR5cGU6ICd3ZWInXG4gICAgfSkpLFxuICAgIHF1ZXJpZXNVc2VkOiByZXN1bHQucXVlcmllcyxcbiAgICBtZXRhZGF0YToge1xuICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9XG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBwZXJmb3JtV2ViU2VhcmNoLCBzdHJlYW1XZWJTZWFyY2ggfSBmcm9tICcuLi9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2gocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgcGVyZm9ybVdlYlNlYXJjaChxdWVyeS50cmltKCkpO1xuXG4gICAgcmVzLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHJlc3VsdC5xdWVyaWVzLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgICAgfVxuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDMpLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggdW5hdmFpbGFibGUnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoU3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICAvLyBTZXQgdXAgU1NFXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnc2VhcmNoaW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyB0aGUgd2ViLi4uJyB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcbiAgICBsZXQgc291cmNlcyA9IFtdO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1XZWJTZWFyY2gocXVlcnkudHJpbSgpKSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdXRUJfU0VBUkNIX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgICAgc291cmNlcyA9IGNodW5rLnNvdXJjZXMgfHwgW107XG4gICAgICB9XG4gICAgfVxuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywge1xuICAgICAgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCBmYWlsZWQnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVXZWJTZWFyY2gpO1xucm91dGVyLnBvc3QoJy9zdHJlYW0nLCBoYW5kbGVXZWJTZWFyY2hTdHJlYW0pO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHNlYXJjaFJvdXRlciBmcm9tICcuL2FwaS9zZWFyY2guanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcblxuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBST1VURVJTXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zb2xlLmxvZygnTW91bnRpbmcgcm91dGVycy4uLicpO1xuXG5hcHAudXNlKCcvaGVhbHRoJywgaGVhbHRoUm91dGVyKTtcbmFwcC51c2UoJy9kb2N1bWVudHMnLCBkb2N1bWVudHNSb3V0ZXIpO1xuYXBwLnVzZSgnL2NoYXQnLCBjaGF0Um91dGVyKTtcbmFwcC51c2UoJy9mZWVkYmFjaycsIGZlZWRiYWNrUm91dGVyKTtcbmFwcC51c2UoJy9zZWFyY2gnLCBzZWFyY2hSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuXG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxtQkFBbUI7QUFDelMsU0FBUyxNQUFNLGNBQWM7QUFNN0IsU0FBUyxpQkFBaUI7QUFDeEIsTUFBSSxDQUFDLGFBQWE7QUFDaEIsVUFBTSxTQUFTLFFBQVEsSUFBSTtBQUMzQixVQUFNLFNBQVMsUUFBUSxJQUFJLGlCQUFpQjtBQUM1QyxVQUFNLFdBQVcsUUFBUSxJQUFJLG1CQUFtQjtBQUNoRCxVQUFNLE9BQU8sUUFBUSxJQUFJLGVBQWU7QUFFeEMsWUFBUSxJQUFJLHFDQUFxQztBQUNqRCxZQUFRLElBQUksZUFBZSxRQUFRLDZCQUE2QjtBQUNoRSxZQUFRLElBQUksZUFBZSxNQUFNO0FBQ2pDLFlBQVEsSUFBSSxlQUFlLFFBQVE7QUFDbkMsWUFBUSxJQUFJLGVBQWUsU0FBUyxtQkFBbUIscUJBQXFCO0FBQzVFLFlBQVEsSUFBSSxxQ0FBcUM7QUFFakQsUUFBSSxDQUFDLFFBQVE7QUFDWCxZQUFNLElBQUk7QUFBQSxRQUNSO0FBQUEsTUFFRjtBQUFBLElBQ0Y7QUFFQSxVQUFNLGdCQUFnQixFQUFFLFFBQVEsUUFBUSxTQUFTO0FBQ2pELFFBQUksS0FBTSxlQUFjLE9BQU87QUFFL0Isa0JBQWMsSUFBSSxZQUFZLGFBQWE7QUFBQSxFQUM3QztBQUNBLFNBQU87QUFDVDtBQU9BLGVBQXNCLHNCQUFzQjtBQUMxQyxNQUFJLENBQUMsa0JBQWtCO0FBQ3JCLFVBQU0sU0FBUyxlQUFlO0FBQzlCLFVBQU0saUJBQWlCLFFBQVEsSUFBSSw0QkFBNEI7QUFDL0QsUUFBSTtBQUNGLHlCQUFtQixNQUFNLE9BQU8sc0JBQXNCO0FBQUEsUUFDcEQsTUFBTTtBQUFBLFFBQ04sVUFBVTtBQUFBLFVBQ1IsYUFBYTtBQUFBLFVBQ2IsTUFBTTtBQUFBLFFBQ1I7QUFBQSxRQUNBLG1CQUFtQjtBQUFBLE1BQ3JCLENBQUM7QUFDRCxj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LEtBQUksS0FBSyxLQUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0QsTUFBSyxRQUFRLFdBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJtb2RlbCIsICJjaHVua1RleHQiLCAidXVpZHY0IiwgImdsb2JhbENvbGxlY3Rpb24iLCAiUm91dGVyIiwgInBhdGgiLCAidXVpZHY0IiwgImNvbGxlY3Rpb24iLCAicm91dGVyIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgIkdvb2dsZUdlbmVyYXRpdmVBSSIsICJnZW5BSSIsICJtb2RlbCIsICJSb3V0ZXIiLCAidXVpZHY0IiwgImNpdGF0aW9ucyIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuZXJhdGl2ZUFJIiwgImdlbkFJIiwgIlBSSU1BUllfTU9ERUwiLCAibW9kZWwiLCAiUm91dGVyIiwgInJvdXRlciIsICJwYXRoIiwgInBhdGgiLCAiYXBwIl0KfQo=
