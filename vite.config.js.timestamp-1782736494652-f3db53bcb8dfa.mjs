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
function getClient() {
  if (!client) {
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
    client = new CloudClient(clientOptions);
  }
  return client;
}
async function getGlobalCollection() {
  if (!globalCollection) {
    const client2 = getClient();
    const collectionName = process.env.CHROMA_GLOBAL_COLLECTION || "dev";
    try {
      globalCollection = await client2.getOrCreateCollection({
        name: collectionName,
        metadata: {
          description: "Permanent seed documents for RAG",
          type: "global_knowledge"
        }
      });
    } catch (error) {
      console.error("Failed to create global collection:", error);
      throw error;
    }
  }
  console.log("created global db");
  return globalCollection;
}
async function createSessionCollection(sessionId) {
  const client2 = getClient();
  const collectionName = `session_${sessionId}`;
  try {
    const collection = await client2.getOrCreateCollection({
      name: collectionName,
      metadata: {
        type: "session_upload",
        session_id: sessionId,
        created: (/* @__PURE__ */ new Date()).toISOString()
      }
    });
    sessionCollections.set(sessionId, collection);
    console.log("created session db");
    return collection;
  } catch (error) {
    console.error(`Failed to create session collection ${collectionName}:`, error);
    throw error;
  }
}
async function getSessionCollection(sessionId) {
  if (sessionCollections.has(sessionId)) {
    return sessionCollections.get(sessionId);
  }
  return createSessionCollection(sessionId);
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
      await collection.delete({
        ids: existing.ids
      });
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
    const client2 = getClient();
    const heartbeat = await client2.heartbeat();
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
var client, globalCollection, sessionCollections;
var init_chromaService = __esm({
  "server/services/chromaService.js"() {
    "use strict";
    client = null;
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
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
      throw new EmbeddingError(
        "GEMINI_API_KEY is undefined at embedding call time \u2014 check env load order"
      );
    }
    const genAI3 = new GoogleGenerativeAI(apiKey);
    embeddingModel = genAI3.getGenerativeModel({
      model: process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-2"
    });
  }
  return embeddingModel;
}
function estimateTokens(text) {
  return Math.ceil(text.length / 4);
}
async function waitForRateLimit(tokens = 0) {
  const now = Date.now();
  const windowElapsed = now - rateLimitState.windowStart;
  if (windowElapsed >= 6e4) {
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = now;
  }
  const remainingTokens = rateLimitState.maxTokensPerMinute - rateLimitState.tokenCount;
  if (remainingTokens <= 0) {
    const waitTime = 6e4 - (Date.now() - rateLimitState.windowStart);
    console.log(`Rate limit reached, waiting ${Math.ceil(waitTime / 1e3)}s`);
    await new Promise((resolve) => setTimeout(resolve, waitTime));
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = Date.now();
  }
  rateLimitState.tokenCount += tokens;
}
async function embedWithRetry(text, attempt = 1, maxAttempts = 5) {
  const baseRetryDelay = 6e4;
  const invalidKeyRetryDelay = 2e3;
  try {
    const result = await getEmbeddingModel().embedContent(text);
    if (result.embedding) {
      return result.embedding.values;
    }
    throw new EmbeddingError("No embedding returned from API");
  } catch (error) {
    const isSpuriousInvalidKey = error?.status === 400 && error?.message?.includes("API_KEY_INVALID");
    if (isSpuriousInvalidKey) {
      if (attempt >= maxAttempts) {
        throw new EmbeddingError("API key validation failed after retries \u2014 check GEMINI_API_KEY");
      }
      console.warn(`Spurious API_KEY_INVALID (attempt ${attempt}/${maxAttempts}), retrying in ${invalidKeyRetryDelay / 1e3}s...`);
      await new Promise((resolve) => setTimeout(resolve, invalidKeyRetryDelay));
      return embedWithRetry(text, attempt + 1, maxAttempts);
    }
    if (is429Error(error) || error?.status === 429 || error?.message?.includes("RESOURCE_EXHAUSTED")) {
      if (attempt >= maxAttempts) {
        throw new EmbeddingError("Max retry attempts reached for rate limiting");
      }
      const retryDelay = error.retryAfter || baseRetryDelay;
      console.log(`Rate limited, waiting ${retryDelay / 1e3}s before retry ${attempt}/${maxAttempts}`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      return embedWithRetry(text, attempt + 1, maxAttempts);
    }
    throw new EmbeddingError(error.message || "Embedding generation failed");
  }
}
async function generateEmbeddings(chunks) {
  if (!chunks || chunks.length === 0) {
    return [];
  }
  const embeddings = [];
  const maxChunksPerCall = rateLimitState.maxChunksPerCall;
  const maxParallelCalls = rateLimitState.parallelCalls;
  const groups = [];
  for (let i = 0; i < chunks.length; i += maxChunksPerCall) {
    groups.push(chunks.slice(i, i + maxChunksPerCall));
  }
  for (let i = 0; i < groups.length; i += maxParallelCalls) {
    const batch = groups.slice(i, i + maxParallelCalls);
    if (i > 0) {
      console.log("Waiting 1 minute before next embedding batch...");
      await new Promise((resolve) => setTimeout(resolve, 6e4));
    }
    const batchPromises = batch.flatMap(
      (group) => group.map(async (chunk) => {
        const tokens = estimateTokens(chunk.text);
        await waitForRateLimit(tokens);
        try {
          const embedding = await embedWithRetry(chunk.text);
          return {
            id: chunk.metadata.chunk_id,
            embedding,
            metadata: chunk.metadata,
            text: chunk.text
          };
        } catch (error) {
          console.error(`Failed to embed chunk ${chunk.metadata.chunk_id}:`, error);
          return null;
        }
      })
    );
    const results = await Promise.all(batchPromises);
    for (const result of results) {
      if (result) embeddings.push(result);
    }
  }
  return embeddings;
}
async function embedQuery(query) {
  const tokens = estimateTokens(query);
  await waitForRateLimit(tokens);
  return embedWithRetry(query);
}
function getRateLimitState() {
  return { ...rateLimitState };
}
var embeddingModel, rateLimitState;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    embeddingModel = null;
    rateLimitState = {
      tokenCount: 0,
      windowStart: Date.now(),
      maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 3e4,
      parallelCalls: parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4,
      maxChunksPerCall: parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7,
      lastCallGroupTime: null
    };
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
function estimateTokens2(text) {
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
        tokenCount: estimateTokens2(chunkText2),
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
  if (!genAI) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("GEMINI_API_KEY is undefined");
    genAI = new GoogleGenerativeAI2(apiKey);
  }
  return genAI;
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
var genAI, PRIMARY_MODEL, FALLBACK_MODEL, FIRST_TOKEN_TIMEOUT, REQUEST_TIMEOUT, primaryModel, fallbackModel;
var init_geminiService = __esm({
  "server/services/geminiService.js"() {
    "use strict";
    init_promptService();
    init_errors();
    genAI = null;
    PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || "gemini-2.0-flash-lite";
    FALLBACK_MODEL = process.env.GEMINI_MODEL_FALLBACK || "gemini-2.0-flash";
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
    model = genAI2.getGenerativeModel({ model: PRIMARY_MODEL2 });
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
var genAI2, PRIMARY_MODEL2, model;
var init_webSearchService = __esm({
  "server/services/webSearchService.js"() {
    "use strict";
    init_errors();
    genAI2 = new GoogleGenerativeAI3(process.env.GEMINI_API_KEY);
    PRIMARY_MODEL2 = process.env.GEMINI_MODEL_PRIMARY || "gemini-2.0-flash-lite";
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tIFwiY2hyb21hZGJcIjtcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG4vLyBcdTI3MDUgTm8gdG9wLWxldmVsIHByb2Nlc3MuZW52IHJlYWRzIFx1MjAxNCBldmVyeXRoaW5nIGlzIGxhenkgaW5zaWRlIGdldENsaWVudCgpXG5sZXQgY2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xpZW50KCkge1xuICBpZiAoIWNsaWVudCkge1xuICAgIC8vIFx1MjcwNSBSZWFkIGVudiBoZXJlIFx1MjAxNCBkb3RlbnYgaXMgZ3VhcmFudGVlZCB0byBoYXZlIGxvYWRlZCBieSByZXF1ZXN0IHRpbWVcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcblxuICAgIGNsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xpZW50O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0R2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFnbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBwcm9jZXNzLmVudi5DSFJPTUFfR0xPQkFMX0NPTExFQ1RJT04gfHwgJ2Rldic7XG5cbiAgICB0cnkge1xuICAgICAgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRPckNyZWF0ZUNvbGxlY3Rpb24oe1xuICAgICAgICBuYW1lOiBjb2xsZWN0aW9uTmFtZSxcbiAgICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgICBkZXNjcmlwdGlvbjogJ1Blcm1hbmVudCBzZWVkIGRvY3VtZW50cyBmb3IgUkFHJyxcbiAgICAgICAgICB0eXBlOiAnZ2xvYmFsX2tub3dsZWRnZSdcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjcmVhdGUgZ2xvYmFsIGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG4gIGNvbnNvbGUubG9nKFwiY3JlYXRlZCBnbG9iYWwgZGJcIik7XG4gIHJldHVybiBnbG9iYWxDb2xsZWN0aW9uO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY3JlYXRlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGNsaWVudCA9IGdldENsaWVudCgpO1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICBuYW1lOiBjb2xsZWN0aW9uTmFtZSxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIHR5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgICAgIHNlc3Npb25faWQ6IHNlc3Npb25JZCxcbiAgICAgICAgY3JlYXRlZDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBzZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgY29uc29sZS5sb2coXCJjcmVhdGVkIHNlc3Npb24gZGJcIik7XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGNyZWF0ZSBzZXNzaW9uIGNvbGxlY3Rpb24gJHtjb2xsZWN0aW9uTmFtZX06YCwgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBzZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgcmV0dXJuIGNyZWF0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgY29uc3QgY2xpZW50ID0gZ2V0Q2xpZW50KCk7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcblxuICB0cnkge1xuICAgIGF3YWl0IGNsaWVudC5kZWxldGVDb2xsZWN0aW9uKHsgbmFtZTogY29sbGVjdGlvbk5hbWUgfSk7XG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBkZWxldGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgaWRzLFxuICAgICAgZW1iZWRkaW5ncyxcbiAgICAgIGRvY3VtZW50czogdmVjdG9ycy5tYXAodiA9PiB2LnRleHQpLFxuICAgICAgbWV0YWRhdGFzOiB2ZWN0b3JzLm1hcCh2ID0+IHYubWV0YWRhdGEpXG4gICAgfSk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9XG4gICAgfSk7XG5cbiAgICBpZiAoZXhpc3RpbmcuaWRzICYmIGV4aXN0aW5nLmlkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7XG4gICAgICAgIGlkczogZXhpc3RpbmcuaWRzXG4gICAgICB9KTtcbiAgICAgIHJldHVybiBleGlzdGluZy5pZHMubGVuZ3RoO1xuICAgIH1cbiAgICByZXR1cm4gMDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudENvdW50KGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjb3VudCA9IGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgICByZXR1cm4gY291bnQ7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbEl0ZW1zID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ11cbiAgICB9KTtcblxuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcblxuICAgIGlmIChhbGxJdGVtcy5pZHMpIHtcbiAgICAgIGFsbEl0ZW1zLmlkcy5mb3JFYWNoKChpZCwgaWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IG1ldGEgPSBhbGxJdGVtcy5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogZG9jSWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2NvdW50OiAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6IG1ldGEuc291cmNlX3R5cGUsXG4gICAgICAgICAgICBmaXJzdF9jaHVua190ZXh0OiBhbGxJdGVtcy5kb2N1bWVudHNbaWR4XVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgZG9jID0gZG9jdW1lbnRzTWFwLmdldChkb2NJZCk7XG4gICAgICAgIGRvYy5jaHVua19jb3VudCsrO1xuICAgICAgICBkb2MucGFnZV9jb3VudCA9IE1hdGgubWF4KGRvYy5wYWdlX2NvdW50LCBtZXRhLnBhZ2VfbnVtYmVyIHx8IDEpO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIEFycmF5LmZyb20oZG9jdW1lbnRzTWFwLnZhbHVlcygpKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHM6JywgZXJyb3IpO1xuICAgIHJldHVybiBbXTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoQ2hlY2soKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdXZWIgc2VhcmNoIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1dFQl9TRUFSQ0hfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG4vLyBcdTI3MDUgRklYOiBMYXp5IGluaXQgXHUyMDE0IGRlZmVyIG1vZGVsIGNvbnN0cnVjdGlvbiB1bnRpbCBmaXJzdCBjYWxsIHNvXG4vLyBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSBpcyBndWFyYW50ZWVkIHRvIGJlIGxvYWRlZCBieSB0aGVuXG5sZXQgZW1iZWRkaW5nTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRFbWJlZGRpbmdNb2RlbCgpIHtcbiAgaWYgKCFlbWJlZGRpbmdNb2RlbCkge1xuICAgIGNvbnN0IGFwaUtleSA9IHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZO1xuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoXG4gICAgICAgICdHRU1JTklfQVBJX0tFWSBpcyB1bmRlZmluZWQgYXQgZW1iZWRkaW5nIGNhbGwgdGltZSBcdTIwMTQgY2hlY2sgZW52IGxvYWQgb3JkZXInXG4gICAgICApO1xuICAgIH1cbiAgICBjb25zdCBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkoYXBpS2V5KTtcbiAgICBlbWJlZGRpbmdNb2RlbCA9IGdlbkFJLmdldEdlbmVyYXRpdmVNb2RlbCh7XG4gICAgICBtb2RlbDogcHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19NT0RFTCB8fCAnZ2VtaW5pLWVtYmVkZGluZy0yJ1xuICAgIH0pO1xuICB9XG4gIHJldHVybiBlbWJlZGRpbmdNb2RlbDtcbn1cblxuY29uc3QgcmF0ZUxpbWl0U3RhdGUgPSB7XG4gIHRva2VuQ291bnQ6IDAsXG4gIHdpbmRvd1N0YXJ0OiBEYXRlLm5vdygpLFxuICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgcGFyYWxsZWxDYWxsczogcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0LFxuICBtYXhDaHVua3NQZXJDYWxsOiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNyxcbiAgbGFzdENhbGxHcm91cFRpbWU6IG51bGxcbn07XG5cbmZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIDQpO1xufVxuXG4vLyBcdTI3MDUgRklYIDM6IEFjY2VwdCB0b2tlbnMgcGFyYW0gc28gY2FsbGVycyBjYW4gYWNjdXJhdGVseSB0cmFjayB1c2FnZVxuYXN5bmMgZnVuY3Rpb24gd2FpdEZvclJhdGVMaW1pdCh0b2tlbnMgPSAwKSB7XG4gIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gIGNvbnN0IHdpbmRvd0VsYXBzZWQgPSBub3cgLSByYXRlTGltaXRTdGF0ZS53aW5kb3dTdGFydDtcblxuICBpZiAod2luZG93RWxhcHNlZCA+PSA2MDAwMCkge1xuICAgIHJhdGVMaW1pdFN0YXRlLnRva2VuQ291bnQgPSAwO1xuICAgIHJhdGVMaW1pdFN0YXRlLndpbmRvd1N0YXJ0ID0gbm93O1xuICB9XG5cbiAgY29uc3QgcmVtYWluaW5nVG9rZW5zID0gcmF0ZUxpbWl0U3RhdGUubWF4VG9rZW5zUGVyTWludXRlIC0gcmF0ZUxpbWl0U3RhdGUudG9rZW5Db3VudDtcbiAgaWYgKHJlbWFpbmluZ1Rva2VucyA8PSAwKSB7XG4gICAgY29uc3Qgd2FpdFRpbWUgPSA2MDAwMCAtIChEYXRlLm5vdygpIC0gcmF0ZUxpbWl0U3RhdGUud2luZG93U3RhcnQpO1xuICAgIGNvbnNvbGUubG9nKGBSYXRlIGxpbWl0IHJlYWNoZWQsIHdhaXRpbmcgJHtNYXRoLmNlaWwod2FpdFRpbWUgLyAxMDAwKX1zYCk7XG4gICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHdhaXRUaW1lKSk7XG4gICAgcmF0ZUxpbWl0U3RhdGUudG9rZW5Db3VudCA9IDA7XG4gICAgcmF0ZUxpbWl0U3RhdGUud2luZG93U3RhcnQgPSBEYXRlLm5vdygpO1xuICB9XG5cbiAgLy8gXHUyNzA1IEZJWCAzOiBJbmNyZW1lbnQgdG9rZW4gY291bnQgZm9yIHRoaXMgY2FsbFxuICByYXRlTGltaXRTdGF0ZS50b2tlbkNvdW50ICs9IHRva2Vucztcbn1cblxuYXN5bmMgZnVuY3Rpb24gZW1iZWRXaXRoUmV0cnkodGV4dCwgYXR0ZW1wdCA9IDEsIG1heEF0dGVtcHRzID0gNSkge1xuICBjb25zdCBiYXNlUmV0cnlEZWxheSA9IDYwMDAwO1xuICBjb25zdCBpbnZhbGlkS2V5UmV0cnlEZWxheSA9IDIwMDA7XG5cbiAgdHJ5IHtcbiAgICAvLyBcdTI3MDUgRklYOiBVc2UgbGF6eSBnZXR0ZXIgaW5zdGVhZCBvZiBtb2R1bGUtbGV2ZWwgY29uc3RhbnRcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRFbWJlZGRpbmdNb2RlbCgpLmVtYmVkQ29udGVudCh0ZXh0KTtcblxuICAgIGlmIChyZXN1bHQuZW1iZWRkaW5nKSB7XG4gICAgICByZXR1cm4gcmVzdWx0LmVtYmVkZGluZy52YWx1ZXM7XG4gICAgfVxuXG4gICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdObyBlbWJlZGRpbmcgcmV0dXJuZWQgZnJvbSBBUEknKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAvLyBcdTI3MDUgRklYIDQ6IFJldHJ5IG9uIGludGVybWl0dGVudCBzcHVyaW91cyBBUElfS0VZX0lOVkFMSUQgXHUyMDE0IHZhbGlkIGtleXNcbiAgICAvLyBvY2Nhc2lvbmFsbHkgZ2V0IGEgNDAwIGZyb20gR29vZ2xlJ3MgZ2F0ZXdheSBvbiBjb2xkL2ZpcnN0IHJlcXVlc3RzXG4gICAgY29uc3QgaXNTcHVyaW91c0ludmFsaWRLZXkgPVxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDAwICYmXG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ0FQSV9LRVlfSU5WQUxJRCcpO1xuXG4gICAgaWYgKGlzU3B1cmlvdXNJbnZhbGlkS2V5KSB7XG4gICAgICBpZiAoYXR0ZW1wdCA+PSBtYXhBdHRlbXB0cykge1xuICAgICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoJ0FQSSBrZXkgdmFsaWRhdGlvbiBmYWlsZWQgYWZ0ZXIgcmV0cmllcyBcdTIwMTQgY2hlY2sgR0VNSU5JX0FQSV9LRVknKTtcbiAgICAgIH1cbiAgICAgIGNvbnNvbGUud2FybihgU3B1cmlvdXMgQVBJX0tFWV9JTlZBTElEIChhdHRlbXB0ICR7YXR0ZW1wdH0vJHttYXhBdHRlbXB0c30pLCByZXRyeWluZyBpbiAke2ludmFsaWRLZXlSZXRyeURlbGF5IC8gMTAwMH1zLi4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgaW52YWxpZEtleVJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZFdpdGhSZXRyeSh0ZXh0LCBhdHRlbXB0ICsgMSwgbWF4QXR0ZW1wdHMpO1xuICAgIH1cblxuICAgIGlmIChpczQyOUVycm9yKGVycm9yKSB8fCBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHwgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSkge1xuICAgICAgaWYgKGF0dGVtcHQgPj0gbWF4QXR0ZW1wdHMpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdNYXggcmV0cnkgYXR0ZW1wdHMgcmVhY2hlZCBmb3IgcmF0ZSBsaW1pdGluZycpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCByZXRyeURlbGF5ID0gZXJyb3IucmV0cnlBZnRlciB8fCBiYXNlUmV0cnlEZWxheTtcbiAgICAgIGNvbnNvbGUubG9nKGBSYXRlIGxpbWl0ZWQsIHdhaXRpbmcgJHtyZXRyeURlbGF5IC8gMTAwMH1zIGJlZm9yZSByZXRyeSAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9YCk7XG5cbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCByZXRyeURlbGF5KSk7XG4gICAgICByZXR1cm4gZW1iZWRXaXRoUmV0cnkodGV4dCwgYXR0ZW1wdCArIDEsIG1heEF0dGVtcHRzKTtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnRW1iZWRkaW5nIGdlbmVyYXRpb24gZmFpbGVkJyk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBbXTtcbiAgY29uc3QgbWF4Q2h1bmtzUGVyQ2FsbCA9IHJhdGVMaW1pdFN0YXRlLm1heENodW5rc1BlckNhbGw7XG4gIGNvbnN0IG1heFBhcmFsbGVsQ2FsbHMgPSByYXRlTGltaXRTdGF0ZS5wYXJhbGxlbENhbGxzO1xuXG4gIGNvbnN0IGdyb3VwcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gbWF4Q2h1bmtzUGVyQ2FsbCkge1xuICAgIGdyb3Vwcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgbWF4Q2h1bmtzUGVyQ2FsbCkpO1xuICB9XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBncm91cHMubGVuZ3RoOyBpICs9IG1heFBhcmFsbGVsQ2FsbHMpIHtcbiAgICBjb25zdCBiYXRjaCA9IGdyb3Vwcy5zbGljZShpLCBpICsgbWF4UGFyYWxsZWxDYWxscyk7XG5cbiAgICBpZiAoaSA+IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdXYWl0aW5nIDEgbWludXRlIGJlZm9yZSBuZXh0IGVtYmVkZGluZyBiYXRjaC4uLicpO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIDYwMDAwKSk7XG4gICAgfVxuXG4gICAgLy8gXHUyNzA1IEZJWCAyOiBSZW1vdmVkIGVtYmVkQmF0Y2goKSBcdTIwMTQgaXQgd2FzIGNhbGxlZCBhbmQgaXRzIHJlc3VsdCBkaXNjYXJkZWQsXG4gICAgLy8gdGhlbiBldmVyeSBjaHVuayB3YXMgcmUtZW1iZWRkZWQgaW5kaXZpZHVhbGx5IGFueXdheS4gRW1iZWQgZGlyZWN0bHkuXG4gICAgY29uc3QgYmF0Y2hQcm9taXNlcyA9IGJhdGNoLmZsYXRNYXAoZ3JvdXAgPT5cbiAgICAgIGdyb3VwLm1hcChhc3luYyAoY2h1bmspID0+IHtcbiAgICAgICAgY29uc3QgdG9rZW5zID0gZXN0aW1hdGVUb2tlbnMoY2h1bmsudGV4dCk7XG4gICAgICAgIGF3YWl0IHdhaXRGb3JSYXRlTGltaXQodG9rZW5zKTtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCBlbWJlZGRpbmcgPSBhd2FpdCBlbWJlZFdpdGhSZXRyeShjaHVuay50ZXh0KTtcbiAgICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhLmNodW5rX2lkLFxuICAgICAgICAgICAgZW1iZWRkaW5nLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH07XG4gICAgICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGVtYmVkIGNodW5rICR7Y2h1bmsubWV0YWRhdGEuY2h1bmtfaWR9OmAsIGVycm9yKTtcbiAgICAgICAgICByZXR1cm4gbnVsbDtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKGJhdGNoUHJvbWlzZXMpO1xuICAgIGZvciAoY29uc3QgcmVzdWx0IG9mIHJlc3VsdHMpIHtcbiAgICAgIGlmIChyZXN1bHQpIGVtYmVkZGluZ3MucHVzaChyZXN1bHQpO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICAvLyBcdTI3MDUgRklYIDM6IFRyYWNrIHRva2VucyBzbyByYXRlIGxpbWl0IHN0YXRlIHN0YXlzIGFjY3VyYXRlXG4gIGNvbnN0IHRva2VucyA9IGVzdGltYXRlVG9rZW5zKHF1ZXJ5KTtcbiAgYXdhaXQgd2FpdEZvclJhdGVMaW1pdCh0b2tlbnMpO1xuICByZXR1cm4gZW1iZWRXaXRoUmV0cnkocXVlcnkpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICAvLyBcdTI3MDUgRklYIDM6IFRyYWNrIHRva2VucyBzbyByYXRlIGxpbWl0IHN0YXRlIHN0YXlzIGFjY3VyYXRlXG4gIGNvbnN0IHRva2VucyA9IGVzdGltYXRlVG9rZW5zKHRleHQpO1xuICBhd2FpdCB3YWl0Rm9yUmF0ZUxpbWl0KHRva2Vucyk7XG4gIHJldHVybiBlbWJlZFdpdGhSZXRyeSh0ZXh0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJhdGVMaW1pdFN0YXRlKCkge1xuICByZXR1cm4geyAuLi5yYXRlTGltaXRTdGF0ZSB9O1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IGhlYWx0aENoZWNrIGFzIGNocm9tYUhlYWx0aENoZWNrIH0gZnJvbSAnLi4vc2VydmljZXMvY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRSYXRlTGltaXRTdGF0ZSB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gQ2hlY2sgR2VtaW5pICh2aWEgQVBJIGtleSBwcmVzZW5jZSlcbiAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmdlbWluaSA9IHtcbiAgICBzdGF0dXM6IHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZID8gJ2NvbmZpZ3VyZWQnIDogJ25vdF9jb25maWd1cmVkJ1xuICB9O1xuXG4gIC8vIEdldCByYXRlIGxpbWl0IHN0YXRlXG4gIGhlYWx0aFN0YXR1cy5yYXRlTGltaXQgPSBnZXRSYXRlTGltaXRTdGF0ZSgpO1xuXG4gIC8vIE92ZXJhbGwgc3RhdHVzXG4gIGNvbnN0IGhhc0Vycm9ycyA9IE9iamVjdC52YWx1ZXMoaGVhbHRoU3RhdHVzLnNlcnZpY2VzKS5zb21lKFxuICAgIHMgPT4gcy5zdGF0dXMgPT09ICdlcnJvcicgfHwgcy5zdGF0dXMgPT09ICd1bmhlYWx0aHknXG4gICk7XG5cbiAgaWYgKGhhc0Vycm9ycykge1xuICAgIGhlYWx0aFN0YXR1cy5zdGF0dXMgPSAnZGVncmFkZWQnO1xuICB9XG5cbiAgcmVzLmpzb24oaGVhbHRoU3RhdHVzKTtcbn1cblxucm91dGVyLmdldCgnLycsIGhlYWx0aCk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Ly8gVG9rZW4gZXN0aW1hdGlvbjogfjQgY2hhcmFjdGVycyBwZXIgdG9rZW4gZm9yIEVuZ2xpc2ggdGV4dFxuY29uc3QgQ0hBUlNfUEVSX1RPS0VOID0gNDtcbmNvbnN0IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlMgPSAxMDAwO1xuY29uc3QgREVGQVVMVF9PVkVSTEFQX1RPS0VOUyA9IDIwMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCBjaHVua1NpemVUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBERUZBVUxUX0NIVU5LX1NJWkVfVE9LRU5TO1xuICBjb25zdCBvdmVybGFwVG9rZW5zID0gb3B0aW9ucy5vdmVybGFwVG9rZW5zIHx8IERFRkFVTFRfT1ZFUkxBUF9UT0tFTlM7XG5cbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGNodW5rU2l6ZUNoYXJzID0gY2h1bmtTaXplVG9rZW5zICogQ0hBUlNfUEVSX1RPS0VOO1xuICBjb25zdCBvdmVybGFwQ2hhcnMgPSBvdmVybGFwVG9rZW5zICogQ0hBUlNfUEVSX1RPS0VOO1xuXG4gIGNvbnN0IGNodW5rcyA9IFtdO1xuICBsZXQgc3RhcnQgPSAwO1xuICBsZXQgY2h1bmtJbmRleCA9IDA7XG5cbiAgd2hpbGUgKHN0YXJ0IDwgdGV4dC5sZW5ndGgpIHtcbiAgICBsZXQgZW5kID0gc3RhcnQgKyBjaHVua1NpemVDaGFycztcblxuICAgIC8vIFRyeSB0byBmaW5kIGEgZ29vZCBicmVhayBwb2ludFxuICAgIGlmIChlbmQgPCB0ZXh0Lmxlbmd0aCkge1xuICAgICAgY29uc3QgYnJlYWtQb2ludHMgPSBbJy4gJywgJy5cXG4nLCAnISAnLCAnPyAnLCAnXFxuXFxuJywgJ1xcbicsICcgJ107XG4gICAgICBsZXQgYmVzdEJyZWFrID0gLTE7XG5cbiAgICAgIC8vIExvb2sgZm9yIGJyZWFrIHBvaW50cyBpbiB0aGUgbGFzdCAyMCUgb2YgdGhlIGNodW5rXG4gICAgICBjb25zdCBzZWFyY2hTdGFydCA9IGVuZCAtIE1hdGguZmxvb3IoY2h1bmtTaXplQ2hhcnMgKiAwLjIpO1xuXG4gICAgICBmb3IgKGNvbnN0IGJyZWFrcG9pbnQgb2YgYnJlYWtQb2ludHMpIHtcbiAgICAgICAgY29uc3QgaWR4ID0gdGV4dC5sYXN0SW5kZXhPZihicmVha3BvaW50LCBlbmQpO1xuICAgICAgICBpZiAoaWR4ID4gc2VhcmNoU3RhcnQgJiYgaWR4ID4gc3RhcnQpIHtcbiAgICAgICAgICBiZXN0QnJlYWsgPSBpZHggKyBicmVha3BvaW50Lmxlbmd0aDtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBpZiAoYmVzdEJyZWFrID4gc3RhcnQpIHtcbiAgICAgICAgZW5kID0gYmVzdEJyZWFrO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGNodW5rVGV4dCA9IHRleHQuc2xpY2Uoc3RhcnQsIGVuZCkudHJpbSgpO1xuICAgIGlmIChjaHVua1RleHQubGVuZ3RoID4gMCkge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiBjaHVua1RleHQsXG4gICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKGNodW5rVGV4dCksXG4gICAgICAgIGNoYXJTdGFydDogc3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGVuZCxcbiAgICAgICAgY2h1bmtJbmRleDogY2h1bmtJbmRleCsrXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICAvLyBNb3ZlIHRvIG5leHQgY2h1bmsgd2l0aCBvdmVybGFwXG4gICAgc3RhcnQgPSBlbmQgLSBvdmVybGFwQ2hhcnM7XG4gICAgaWYgKHN0YXJ0IDw9IGNodW5rc1tjaHVua3MubGVuZ3RoIC0gMV0/LmNoYXJTdGFydCkge1xuICAgICAgc3RhcnQgPSBlbmQ7XG4gICAgfVxuXG4gICAgLy8gU2FmZXR5IGNoZWNrIHRvIHByZXZlbnQgaW5maW5pdGUgbG9vcHNcbiAgICBpZiAoY2h1bmtJbmRleCA+IDEwMDAwKSB7XG4gICAgICBjb25zb2xlLndhcm4oJ0NodW5rIGxpbWl0IHJlYWNoZWQsIHN0b3BwaW5nJyk7XG4gICAgICBicmVhaztcbiAgICB9XG4gIH1cblxuICByZXR1cm4gY2h1bmtzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtQREZDb250ZW50KHBkZkRhdGEsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB7IGZpbGVuYW1lLCBkb2N1bWVudElkLCBwYWdlTnVtYmVyLCB0ZXh0IH0gPSBwZGZEYXRhO1xuXG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQodGV4dCwgb3B0aW9ucyk7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+ICh7XG4gICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICBtZXRhZGF0YToge1xuICAgICAgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQsXG4gICAgICBmaWxlbmFtZTogZmlsZW5hbWUsXG4gICAgICBjaHVua19pZDogYCR7ZG9jdW1lbnRJZH1fJHtjaHVuay5jaHVua0luZGV4fWAsXG4gICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgIHBhZ2VfbnVtYmVyOiBwYWdlTnVtYmVyIHx8IDEsXG4gICAgICBzZWN0aW9uX3RpdGxlOiBleHRyYWN0U2VjdGlvblRpdGxlKGNodW5rLnRleHQpLFxuICAgICAgc291cmNlX3R5cGU6ICdwZGYnLFxuICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgdG9rZW5fc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgIHRva2VuX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgIHRva2VuX2NvdW50OiBjaHVuay50b2tlbkNvdW50XG4gICAgfVxuICB9KSk7XG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RTZWN0aW9uVGl0bGUodGV4dCkge1xuICAvLyBUcnkgdG8gZXh0cmFjdCBhIHBvdGVudGlhbCBzZWN0aW9uIHRpdGxlIGZyb20gdGhlIGJlZ2lubmluZyBvZiB0aGUgY2h1bmtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbWVyZ2VDaHVua3MoY2h1bmtzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIC8vIE1lcmdlIHNtYWxsIGNodW5rcyB0byByZWR1Y2UgQVBJIGNhbGxzXG4gIGNvbnN0IG1lcmdlZCA9IFtdO1xuICBsZXQgY3VycmVudCA9IHsgdGV4dHM6IFtdLCB0b3RhbFRva2VuczogMCwgbWV0YWRhdGE6IFtdIH07XG5cbiAgZm9yIChjb25zdCBjaHVuayBvZiBjaHVua3MpIHtcbiAgICBpZiAoY3VycmVudC50b3RhbFRva2VucyArIGNodW5rLnRva2VuQ291bnQgPD0gbWF4VG9rZW5zKSB7XG4gICAgICBjdXJyZW50LnRleHRzLnB1c2goY2h1bmsudGV4dCk7XG4gICAgICBjdXJyZW50Lm1ldGFkYXRhLnB1c2goY2h1bmsubWV0YWRhdGEpO1xuICAgICAgY3VycmVudC50b3RhbFRva2VucyArPSBjaHVuay50b2tlbkNvdW50O1xuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoY3VycmVudC50ZXh0cy5sZW5ndGggPiAwKSB7XG4gICAgICAgIG1lcmdlZC5wdXNoKHsgdGV4dHM6IGN1cnJlbnQudGV4dHMsIG1ldGFkYXRhOiBjdXJyZW50Lm1ldGFkYXRhIH0pO1xuICAgICAgfVxuICAgICAgY3VycmVudCA9IHsgdGV4dHM6IFtjaHVuay50ZXh0XSwgbWV0YWRhdGE6IFtjaHVuay5tZXRhZGF0YV0sIHRvdGFsVG9rZW5zOiBjaHVuay50b2tlbkNvdW50IH07XG4gICAgfVxuICB9XG5cbiAgaWYgKGN1cnJlbnQudGV4dHMubGVuZ3RoID4gMCkge1xuICAgIG1lcmdlZC5wdXNoKHsgdGV4dHM6IGN1cnJlbnQudGV4dHMsIG1ldGFkYXRhOiBjdXJyZW50Lm1ldGFkYXRhIH0pO1xuICB9XG5cbiAgcmV0dXJuIG1lcmdlZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBnZXRHbG9iYWxDb2xsZWN0aW9uLCBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgY3JlYXRlU2Vzc2lvbkNvbGxlY3Rpb24sIGxpc3REb2N1bWVudHMgfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlU2Vzc2lvbigpIHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gdXVpZHY0KCk7XG4gIGNvbnN0IHNlc3Npb24gPSB7XG4gICAgaWQ6IHNlc3Npb25JZCxcbiAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCksXG4gICAgbGFzdEFjY2Vzc2VkOiBuZXcgRGF0ZSgpLFxuICAgIGRvY3VtZW50czogW10sXG4gICAgdGltZW91dE1pbnV0ZXM6IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTXG4gIH07XG5cbiAgc2Vzc2lvbnMuc2V0KHNlc3Npb25JZCwgc2Vzc2lvbik7XG4gIHJldHVybiBzZXNzaW9uO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IHNlc3Npb25zLmdldChzZXNzaW9uSWQpO1xuXG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIHJldHVybiBzZXNzaW9uO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGV4aXN0aW5nKSB7XG4gICAgICByZXR1cm4gZXhpc3Rpbmc7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikge1xuICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICBjb25zdCBsYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZShzZXNzaW9uLmxhc3RBY2Nlc3NlZCkuZ2V0VGltZSgpO1xuICBjb25zdCB0aW1lb3V0TXMgPSBzZXNzaW9uLnRpbWVvdXRNaW51dGVzICogNjAgKiAxMDAwO1xuICByZXR1cm4gKG5vdyAtIGxhc3RBY2Nlc3NlZCkgPiB0aW1lb3V0TXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBzZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJbmZvKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQsXG4gICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJ1xuICB9KTtcblxuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2FuQWNjZXB0VXBsb2FkKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgfVxuXG4gIGlmIChzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246IGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gIH07XG4gIH1cblxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgLy8gQ2hlY2sgZmlsZSBzaXplXG4gIGlmIChmaWxlLnNpemUgPiBNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIGV4Y2VlZHMgJHtNQVhfVVBMT0FEX1NJWkVfTUJ9TUIgbGltaXRgKTtcbiAgfVxuXG4gIC8vIENoZWNrIG1heCBQREZzXG4gIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIGVycm9ycy5wdXNoKGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gKTtcbiAgfVxuXG4gIC8vIENoZWNrIGZvciBkdXBsaWNhdGUgZmlsZW5hbWVcbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNikgLy8gNjAlIG9mIG1heFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgY29uc3QgaWR4ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZEluZGV4KGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJZCk7XG4gIGlmIChpZHggPj0gMCkge1xuICAgIHNlc3Npb24uZG9jdW1lbnRzLnNwbGljZShpZHgsIDEpO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25Eb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBbXTtcbiAgfVxuICByZXR1cm4gc2Vzc2lvbi5kb2N1bWVudHM7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb25Eb2NzID0gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpO1xuICBjb25zdCBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICBjb25zdCBnbG9iYWxEb2NzID0gYXdhaXQgbGlzdERvY3VtZW50cyhnbG9iYWxDb2xsZWN0aW9uKTtcblxuICByZXR1cm4ge1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb25Eb2NzLFxuICAgIGdsb2JhbERvY3VtZW50czogZ2xvYmFsRG9jc1xuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaWQ6IHNlc3Npb24uaWQsXG4gICAgZG9jdW1lbnRDb3VudDogc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogc2Vzc2lvbi5jcmVhdGVkQXQsXG4gICAgbGFzdEFjY2Vzc2VkOiBzZXNzaW9uLmxhc3RBY2Nlc3NlZCxcbiAgICB0b3RhbFNpemU6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5maWxlU2l6ZSB8fCAwKSwgMCksXG4gICAgdG90YWxDaHVua3M6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdEFjdGl2ZVNlc3Npb25zKCkge1xuICByZXR1cm4gQXJyYXkuZnJvbShzZXNzaW9ucy52YWx1ZXMoKSkuZmlsdGVyKHMgPT4gIWlzU2Vzc2lvbkV4cGlyZWQocykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW51cEV4cGlyZWRTZXNzaW9ucygpIHtcbiAgbGV0IGNsZWFuZWQgPSAwO1xuICBmb3IgKGNvbnN0IFtpZCwgc2Vzc2lvbl0gb2Ygc2Vzc2lvbnMpIHtcbiAgICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgICAgc2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIGNsZWFuZWQrKztcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGNsZWFuZWQ7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgbXVsdGVyIGZyb20gJ211bHRlcic7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCBmcyBmcm9tICdmcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCBwZGYgZnJvbSAncGRmLXBhcnNlJztcbmltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUsIHZhbGlkYXRlUERGRmlsZSwgdmFsaWRhdGVGaWxlU2l6ZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbmltcG9ydCB7XG4gIENvcnJ1cHRlZFBERkVycm9yLFxuICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgRmlsZVRvb0xhcmdlRXJyb3IsXG4gIFRvb01hbnlQREZzRXJyb3IsXG4gIER1cGxpY2F0ZUZpbGVFcnJvclxufSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuaW1wb3J0IHsgZ2V0R2xvYmFsQ29sbGVjdGlvbiwgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycywgbGlzdERvY3VtZW50cyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2h1bmtQREZDb250ZW50IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG5pbXBvcnQgeyBnZW5lcmF0ZUVtYmVkZGluZ3MgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgY2FuQWNjZXB0VXBsb2FkLCBhZGREb2N1bWVudFRvU2Vzc2lvbiwgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbiwgZ2V0QWxsRG9jdW1lbnRzIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG5pZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbn1cblxuY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gIGRlc3RpbmF0aW9uOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgIGNiKG51bGwsIHVwbG9hZERpcik7XG4gIH0sXG4gIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgIGNvbnN0IGlkID0gdXVpZHY0KCk7XG4gICAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcbiAgICBjYihudWxsLCBgJHtpZH0ke2V4dH1gKTtcbiAgfVxufSk7XG5cbmNvbnN0IHVwbG9hZCA9IG11bHRlcih7XG4gIHN0b3JhZ2UsXG4gIGxpbWl0czoge1xuICAgIGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0XG4gIH0sXG4gIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICBjYihudWxsLCB0cnVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgIH1cbiAgfVxufSk7XG5cbmFzeW5jIGZ1bmN0aW9uIHBhcnNlUERGKGZpbGVQYXRoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYnVmZmVyID0gZnMucmVhZEZpbGVTeW5jKGZpbGVQYXRoKTtcbiAgICBjb25zdCBkYXRhID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGRhdGEudGV4dCxcbiAgICAgIHBhZ2VDb3VudDogZGF0YS5udW1wYWdlcyxcbiAgICAgIGluZm86IGRhdGEuaW5mb1xuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUERGIHBhcnNpbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBDb3JydXB0ZWRQREZFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBmaWxlID0gcmVxLmZpbGU7XG4gICAgaWYgKCFmaWxlKSB7XG4gICAgICB0aHJvdyBuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKTtcbiAgICB9XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLmJvZHkuc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICAgIGNvbnN0IHNlc3Npb24gPSBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBtYXhVcGxvYWRzTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKTtcbiAgICBjb25zdCBtYXhQREZzID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04gfHwgJzMnKTtcblxuICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPj0gbWF4UERGcykge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgdGhyb3cgbmV3IFRvb01hbnlQREZzRXJyb3IobWF4UERGcyk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgdGhyb3cgbmV3IER1cGxpY2F0ZUZpbGVFcnJvcihjbGVhbkZpbGVuYW1lKTtcbiAgICB9XG5cbiAgICBjb25zdCBwZGZEYXRhID0gYXdhaXQgcGFyc2VQREYoZmlsZS5wYXRoKTtcbiAgICBjb25zdCBkb2N1bWVudElkID0gcGF0aC5wYXJzZShmaWxlLmZpbGVuYW1lKS5uYW1lO1xuICAgIGNvbnN0IGRvY3VtZW50UGF0aCA9IGZpbGUucGF0aDtcblxuICAgIGNvbnN0IGNodW5rcyA9IGNodW5rUERGQ29udGVudCh7XG4gICAgICB0ZXh0OiBwZGZEYXRhLnRleHQsXG4gICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgIGRvY3VtZW50SWQsXG4gICAgICBwYWdlTnVtYmVyOiAxXG4gICAgfSk7XG5cbiAgICBpZiAoY2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNDIyKS5qc29uKHtcbiAgICAgICAgZXJyb3I6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsXG4gICAgICAgIGNvZGU6ICdFTVBUWV9QREYnXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGVtYmVkZGluZ3MgPSBbXTtcbiAgICBjb25zdCBwcm9ncmVzc0NhbGxiYWNrID0gKHByb2Nlc3NlZCwgdG90YWwpID0+IHtcbiAgICAgIGlmIChyZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcykge1xuICAgICAgICByZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcy5lbWl0KGBwcm9ncmVzc18ke3Nlc3Npb25JZH1gLCB7XG4gICAgICAgICAgZG9jdW1lbnRJZCxcbiAgICAgICAgICBwcm9jZXNzZWQsXG4gICAgICAgICAgdG90YWwsXG4gICAgICAgICAgc3RhZ2U6ICdlbWJlZGRpbmcnXG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH07XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkrKykge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgZW1iZWRkaW5nID0gYXdhaXQgZ2VuZXJhdGVFbWJlZGRpbmdzKFtjaHVua3NbaV1dKTtcbiAgICAgICAgaWYgKGVtYmVkZGluZyAmJiBlbWJlZGRpbmcubGVuZ3RoID4gMCkge1xuICAgICAgICAgIGVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICBpZDogdXVpZHY0KCksXG4gICAgICAgICAgICBlbWJlZGRpbmc6IGVtYmVkZGluZ1swXS5lbWJlZGRpbmcsXG4gICAgICAgICAgICB0ZXh0OiBjaHVua3NbaV0udGV4dCxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVua3NbaV0ubWV0YWRhdGFcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGVtYmVkIGNodW5rICR7aX06YCwgZXJyb3IpO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNTAzKS5qc29uKHtcbiAgICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZ2VuZXJhdGUgZW1iZWRkaW5ncycsXG4gICAgICAgIGNvZGU6ICdFTUJFRERJTkdfRkFJTEVEJ1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgIGNvbGxlY3Rpb24sXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgZW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgKTtcblxuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgaWQ6IGRvY3VtZW50SWQsXG4gICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHBkZkRhdGEucGFnZUNvdW50LFxuICAgICAgY2h1bmtDb3VudDogZW1iZWRkaW5ncy5sZW5ndGhcbiAgICB9KTtcblxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBkb2N1bWVudDoge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogcGRmRGF0YS5wYWdlQ291bnQsXG4gICAgICAgIGNodW5rQ291bnQ6IGVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIHNlc3Npb25JZFxuICAgIH0pO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaWYgKHJlcS5maWxlICYmIGZzLmV4aXN0c1N5bmMocmVxLmZpbGUucGF0aCkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMocmVxLmZpbGUucGF0aCk7XG4gICAgfVxuICAgIGNvbnNvbGUuZXJyb3IoJ1VwbG9hZCBlcnJvcjonLCBlcnJvcik7XG5cbiAgICBjb25zdCBzdGF0dXNDb2RlID0gZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDA7XG4gICAgcmVzLnN0YXR1cyhzdGF0dXNDb2RlKS5qc29uKHtcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGF3YWl0IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgbGV0IGNvbGxlY3Rpb247XG4gICAgbGV0IGRlbGV0ZWRGcm9tU2Vzc2lvbiA9IGZhbHNlO1xuXG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICBjb25zdCBjb3VudCA9IGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgaWYgKGNvdW50ID4gMCkge1xuICAgICAgICAgIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKTtcbiAgICAgICAgICBkZWxldGVkRnJvbVNlc3Npb24gPSB0cnVlO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gRGVsZXRlIGZyb20gZ2xvYmFsIGNvbGxlY3Rpb25cbiAgICB0cnkge1xuICAgICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICAvLyBOb3QgaW4gZ2xvYmFsXG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGRvY3VtZW50SWQsXG4gICAgICBkZWxldGVkRnJvbTogZGVsZXRlZEZyb21TZXNzaW9uID8gJ3Nlc3Npb24nIDogJ3Vua25vd24nXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRGVsZXRlIGRvY3VtZW50IGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQnLFxuICAgICAgY29kZTogJ0RFTEVURV9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRGaWxlKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmaWxlUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGAke2RvY3VtZW50SWR9LnBkZmApO1xuXG4gICAgaWYgKCFmcy5leGlzdHNTeW5jKGZpbGVQYXRoKSkge1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICAgICAgZXJyb3I6ICdEb2N1bWVudCBmaWxlIG5vdCBmb3VuZCcsXG4gICAgICAgIGNvZGU6ICdGSUxFX05PVF9GT1VORCdcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICBjb25zdCBmaWxlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZVBhdGgpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBgYXR0YWNobWVudDsgZmlsZW5hbWU9XCIke2ZpbGVuYW1lfVwiYCk7XG5cbiAgICBjb25zdCBzdHJlYW0gPSBmcy5jcmVhdGVSZWFkU3RyZWFtKGZpbGVQYXRoKTtcbiAgICBzdHJlYW0ucGlwZShyZXMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsXG4gICAgICBjb2RlOiAnUkVUUklFVkVfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG5yb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xucm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xucm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtpbXBvcnQgeyBnZXRHbG9iYWxDb2xsZWN0aW9uLCBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDU7XG5jb25zdCBDT1ZFUkFHRV9ISUdIX1RIUkVTSE9MRCA9IHBhcnNlRmxvYXQocHJvY2Vzcy5lbnYuQ09WRVJBR0VfSElHSF9USFJFU0hPTEQpIHx8IDAuNzU7XG5jb25zdCBDT1ZFUkFHRV9NRURJVU1fVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5DT1ZFUkFHRV9NRURJVU1fVEhSRVNIT0xEKSB8fCAwLjU1O1xuXG4vLyBcdTI3MDUgQ2FjaGUgcmVzb2x2ZWQgY29sbGVjdGlvbiBvYmplY3RzIFx1MjAxNCBuZXZlciBoaXQgQ2hyb21hIG1vcmUgdGhhbiBvbmNlIHBlciBzZXNzaW9uXG5sZXQgY2FjaGVkR2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5jb25zdCBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMgPSBuZXcgTWFwKCk7XG5cbmFzeW5jIGZ1bmN0aW9uIGdldE9yQ2FjaGVHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWNhY2hlZEdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjYWNoZWRHbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICB9XG4gIHJldHVybiBjYWNoZWRHbG9iYWxDb2xsZWN0aW9uO1xufVxuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICB9XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4geyBsZXZlbDogJ2xvdycsIHNjb3JlOiAwLCByZWFzb246ICdObyByZXN1bHRzIGZvdW5kJyB9O1xuICB9XG5cbiAgY29uc3QgdG9wUmVzdWx0cyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSyk7XG4gIGNvbnN0IHNjb3JlcyA9IHRvcFJlc3VsdHMubWFwKHIgPT4gci5zY29yZSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcblxuICBsZXQgbGV2ZWw7XG4gIGxldCByZWFzb247XG5cbiAgaWYgKGF2Z1Njb3JlID49IENPVkVSQUdFX0hJR0hfVEhSRVNIT0xEKSB7XG4gICAgbGV2ZWwgPSAnaGlnaCc7XG4gICAgcmVhc29uID0gJ0hpZ2ggY29uZmlkZW5jZSBpbiByZXRyaWV2ZWQgY29udGV4dCc7XG4gIH0gZWxzZSBpZiAoYXZnU2NvcmUgPj0gQ09WRVJBR0VfTUVESVVNX1RIUkVTSE9MRCkge1xuICAgIGxldmVsID0gJ21lZGl1bSc7XG4gICAgcmVhc29uID0gJ01vZGVyYXRlIGNvbmZpZGVuY2UgaW4gcmV0cmlldmVkIGNvbnRleHQnO1xuICB9IGVsc2Uge1xuICAgIGxldmVsID0gJ2xvdyc7XG4gICAgcmVhc29uID0gJ0luc3VmZmljaWVudCByZWxldmFudCBpbmZvcm1hdGlvbiBmb3VuZCc7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGxldmVsLFxuICAgIHNjb3JlOiBhdmdTY29yZSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKSxcbiAgICBib3R0b21TY29yZTogTWF0aC5taW4oLi4uc2NvcmVzKSxcbiAgICByZWFzb25cbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG4gIGNvbnN0IGluY2x1ZGVHbG9iYWwgPSBvcHRpb25zLmluY2x1ZGVHbG9iYWwgIT09IGZhbHNlO1xuXG4gIHRyeSB7XG4gICAgLy8gXHUyNzA1IFJ1biBlbWJlZGRpbmcgKyBib3RoIGNvbGxlY3Rpb24gZmV0Y2hlcyBpbiBwYXJhbGxlbFxuICAgIC8vIENvbGxlY3Rpb25zIGFyZSBzZXJ2ZWQgZnJvbSBjYWNoZSBhZnRlciB0aGUgZmlyc3QgY2FsbCBcdTIwMTQgemVybyBDaHJvbWEgcm91bmQtdHJpcHNcbiAgICBjb25zdCBbcXVlcnlFbWJlZGRpbmcsIGdsb2JhbENvbGxlY3Rpb24sIHNlc3Npb25Db2xsZWN0aW9uXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIGVtYmVkUXVlcnkocXVlcnkpLFxuICAgICAgaW5jbHVkZUdsb2JhbCA/IGdldE9yQ2FjaGVHbG9iYWxDb2xsZWN0aW9uKCkgOiBQcm9taXNlLnJlc29sdmUobnVsbCksXG4gICAgICBzZXNzaW9uSWQgPyBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSA6IFByb21pc2UucmVzb2x2ZShudWxsKVxuICAgIF0pO1xuXG4gICAgLy8gXHUyNzA1IFF1ZXJ5IGJvdGggY29sbGVjdGlvbnMgaW4gcGFyYWxsZWxcbiAgICBjb25zdCBxdWVyeVByb21pc2VzID0gW107XG5cbiAgICBpZiAoZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgICAgcXVlcnlQcm9taXNlcy5wdXNoKFxuICAgICAgICBxdWVyeUNvbGxlY3Rpb24oZ2xvYmFsQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAgICAgLnRoZW4ocmVzdWx0cyA9PiAoeyB0eXBlOiAnZ2xvYmFsJywgcmVzdWx0cyB9KSlcbiAgICAgICAgICAuY2F0Y2goKCkgPT4gKHsgdHlwZTogJ2dsb2JhbCcsIHJlc3VsdHM6IFtdIH0pKVxuICAgICAgKTtcbiAgICB9XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb24pIHtcbiAgICAgIHF1ZXJ5UHJvbWlzZXMucHVzaChcbiAgICAgICAgcXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgICAgICAudGhlbihyZXN1bHRzID0+ICh7IHR5cGU6ICdzZXNzaW9uJywgcmVzdWx0cyB9KSlcbiAgICAgICAgICAuY2F0Y2goKCkgPT4gKHsgdHlwZTogJ3Nlc3Npb24nLCByZXN1bHRzOiBbXSB9KSlcbiAgICAgICk7XG4gICAgfVxuXG4gICAgY29uc3QgcXVlcnlSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGwocXVlcnlQcm9taXNlcyk7XG5cbiAgICBjb25zdCBhbGxSZXN1bHRzID0gW107XG4gICAgZm9yIChjb25zdCB7IHR5cGUsIHJlc3VsdHM6IHR5cGVSZXN1bHRzIH0gb2YgcXVlcnlSZXN1bHRzKSB7XG4gICAgICBmb3IgKGNvbnN0IHJlc3VsdCBvZiB0eXBlUmVzdWx0cykge1xuICAgICAgICBhbGxSZXN1bHRzLnB1c2goeyAuLi5yZXN1bHQsIHNvdXJjZV90eXBlOiB0eXBlIH0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGFsbFJlc3VsdHMuc29ydCgoYSwgYikgPT4gYi5zY29yZSAtIGEuc2NvcmUpO1xuICAgIGNvbnN0IHRvcFJlc3VsdHMgPSBhbGxSZXN1bHRzLnNsaWNlKDAsIHRvcEspO1xuICAgIGNvbnN0IGNvdmVyYWdlID0gY2FsY3VsYXRlQ292ZXJhZ2UodG9wUmVzdWx0cywgdG9wSyk7XG5cbiAgICByZXR1cm4geyByZXN1bHRzOiB0b3BSZXN1bHRzLCBjb3ZlcmFnZSwgcXVlcnlFbWJlZGRpbmcgfTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1JldHJpZXZhbCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLy8gXHUyNzA1IENhbGwgdGhpcyBhZnRlciBhIHVzZXIgdXBsb2FkcyBhIGRvY3VtZW50IHRvIGEgc2Vzc2lvblxuLy8gc28gdGhlIG5leHQgcXVlcnkgZmV0Y2hlcyB0aGUgdXBkYXRlZCBjb2xsZWN0aW9uIGZyZXNoXG5leHBvcnQgZnVuY3Rpb24gaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUoc2Vzc2lvbklkKSB7XG4gIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cywgbWF4VG9rZW5zID0gNzAwMCkge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cblxuICBsZXQgdG90YWxUb2tlbnMgPSAwO1xuICBjb25zdCBtYXhUb2tlbnNQZXJDaGFyID0gNDtcbiAgY29uc3QgY29udGV4dFBhcnRzID0gW107XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCByZXN1bHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gcmVzdWx0c1tpXTtcbiAgICBjb25zdCB0b2tlbkVzdGltYXRlID0gcmVzdWx0LnRleHQubGVuZ3RoIC8gbWF4VG9rZW5zUGVyQ2hhcjtcblxuICAgIGlmICh0b3RhbFRva2VucyArIHRva2VuRXN0aW1hdGUgPiBtYXhUb2tlbnMpIHtcbiAgICAgIGJyZWFrO1xuICAgIH1cblxuICAgIHRvdGFsVG9rZW5zICs9IHRva2VuRXN0aW1hdGU7XG5cbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBjaXRhdGlvbiA9IGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ31gO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG5cbiAgICBjb250ZXh0UGFydHMucHVzaChgJHtjaXRhdGlvbn0ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gW107XG4gIH1cblxuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQuc2xpY2UoMCwgMjAwKSArIChyZXN1bHQudGV4dC5sZW5ndGggPiAyMDAgPyAnLi4uJyA6ICcnKSxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hvdWxkU2hvd1JlZnVzYWwoY292ZXJhZ2UpIHtcbiAgcmV0dXJuIGNvdmVyYWdlLmxldmVsID09PSAnbG93JyAmJiBjb3ZlcmFnZS5zY29yZSA+IDA7XG59XG5cbmV4cG9ydCB7IGNhbGN1bGF0ZUNvdmVyYWdlIH07IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICAvLyBLZWVwIG9ubHkgdGhlIGxhc3QgTiB0dXJuc1xuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgcmV0dXJuIG1lbW9yeS50dXJucy5zbGljZSgtbGltaXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q29udmVyc2F0aW9uQ29udGV4dChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICByZXR1cm4gdHVybnMubWFwKHQgPT4gKHtcbiAgICByb2xlOiB0LnJvbGUsXG4gICAgY29udGVudDogdC5jb250ZW50XG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICBpZiAodHVybnMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG5cbiAgY29uc3QgZm9ybWF0dGVkID0gdHVybnMubWFwKHQgPT4ge1xuICAgIGNvbnN0IHByZWZpeCA9IHQucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXI6JyA6ICdBc3Npc3RhbnQ6JztcbiAgICByZXR1cm4gYCR7cHJlZml4fSAke3QuY29udGVudH1gO1xuICB9KS5qb2luKCdcXG5cXG4nKTtcblxuICByZXR1cm4gZm9ybWF0dGVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwpIHtcbiAgcmV0dXJuIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCB7XG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtpbXBvcnQgeyBmb3JtYXRNZW1vcnlGb3JQcm9tcHQgfSBmcm9tICcuL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZm9ybWF0Q29udGV4dEZvclByb21wdCwgY2FsY3VsYXRlQ292ZXJhZ2UgfSBmcm9tICcuL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCBTWVNURU1fSU5TVFJVQ1RJT04gPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgYmFzZWQgb24gaW5kZXhlZCBkb2N1bWVudHMgd2hlbiBhdmFpbGFibGUuXG5cblJVTEVTOlxuMS4gV2hlbiBjb250ZXh0IGlzIHByb3ZpZGVkLCBhbnN3ZXIgYmFzZWQgb24gaXQgYW5kIGNpdGUgc291cmNlcyB1c2luZyBbMV0sIFsyXSwgZXRjLlxuMi4gRm9yIGdlbmVyYWwgY29udmVyc2F0aW9uIChncmVldGluZ3MsIGNsYXJpZnlpbmcgcXVlc3Rpb25zLCBzbWFsbCB0YWxrKSwgcmVzcG9uZCBuYXR1cmFsbHkgYW5kIGhlbHBmdWxseSB3aXRob3V0IHJlcXVpcmluZyBjb250ZXh0LlxuMy4gSWYgYSBmYWN0dWFsIHF1ZXN0aW9uIGlzIGFza2VkIGJ1dCBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkgYW5kIHN1Z2dlc3QgdXBsb2FkaW5nIHJlbGV2YW50IGRvY3VtZW50cy5cbjQuIEJlIGNvbmNpc2UgYnV0IHRob3JvdWdoLiBVc2UgYnVsbGV0IHBvaW50cyBvciBudW1iZXJlZCBsaXN0cyBmb3IgY29tcGxleCBhbnN3ZXJzLlxuNS4gTWFpbnRhaW4gY29udmVyc2F0aW9uIGNvbnRpbnVpdHkgYnV0IGRvbid0IHJlcGVhdCBpbmZvcm1hdGlvbiB1bm5lY2Vzc2FyaWx5LlxuNi4gRm9ybWF0IHJlc3BvbnNlcyBpbiBjbGVhciwgcmVhZGFibGUgbWFya2Rvd24uYDtcblxuY29uc3QgUkVGVVNBTF9NRVNTQUdFID0gXCJJIGRvbid0IGhhdmUgZW5vdWdoIGluZm9ybWF0aW9uIGluIHRoZSBrbm93bGVkZ2UgYmFzZSB0byBhbnN3ZXIgdGhhdCBjb25maWRlbnRseS4gVHJ5IHVwbG9hZGluZyByZWxldmFudCBkb2N1bWVudHMsIG9yIGFzayBtZSBhIGdlbmVyYWwgcXVlc3Rpb24uXCI7XG5leHBvcnQgZnVuY3Rpb24gYnVpbGRQcm9tcHQoeyBxdWVyeSwgY29udGV4dCwgbWVtb3J5Q29udGV4dCwgY292ZXJhZ2UgfSkge1xuICBjb25zdCBwYXJ0cyA9IFtdO1xuXG4gIC8vIFN5c3RlbSBpbnN0cnVjdGlvblxuICBwYXJ0cy5wdXNoKFNZU1RFTV9JTlNUUlVDVElPTik7XG5cbiAgLy8gUGFzdCBjb252ZXJzYXRpb24gaWYgYXZhaWxhYmxlXG4gIGlmIChtZW1vcnlDb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKG1lbW9yeUNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgUFJFVklPVVMgQ09OVkVSU0FUSU9OIC0tLVxcbicpO1xuICB9XG5cbiAgLy8gUmV0cmlldmVkIGNvbnRleHRcbiAgaWYgKGNvbnRleHQpIHtcbiAgICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gUkVMRVZBTlQgQ09OVEVYVCBGUk9NIEtOT1dMRURHRSBCQVNFIC0tLVxcbicpO1xuICAgIHBhcnRzLnB1c2goY29udGV4dCk7XG4gICAgcGFydHMucHVzaCgnXFxuLS0tIEVORCBDT05URVhUIC0tLVxcbicpO1xuICB9XG5cbiAgLy8gQ3VycmVudCBxdWVzdGlvblxuICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gQ1VSUkVOVCBRVUVTVElPTiAtLS1cXG4nKTtcbiAgcGFydHMucHVzaChxdWVyeSk7XG4gIHBhcnRzLnB1c2goJ1xcblxcblJlbWVtYmVyOiBBbnN3ZXIgYmFzZWQgT05MWSBvbiB0aGUgcHJvdmlkZWQgY29udGV4dC4gVXNlIFsxXSwgWzJdLCBldGMuIGZvciBjaXRhdGlvbnMuIElmIHRoZSBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkuJyk7XG5cbiAgcmV0dXJuIHBhcnRzLmpvaW4oJycpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRTdHJlYW1pbmdQcm9tcHQocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCk7XG4gIGNvbnN0IGNvbnRleHRTdHJpbmcgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJldHJpZXZlZFJlc3VsdHMpO1xuXG4gIHJldHVybiBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFN0cmluZyxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiBjYWxjdWxhdGVDb3ZlcmFnZShyZXRyaWV2ZWRSZXN1bHRzKVxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxSZXNwb25zZSgpIHtcbiAgcmV0dXJuIFJFRlVTQUxfTUVTU0FHRTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFN5c3RlbUluc3RydWN0aW9uKCkge1xuICByZXR1cm4gU1lTVEVNX0lOU1RSVUNUSU9OO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRXZWJTZWFyY2hQcm9tcHQocXVlcnksIGdyb3VuZGluZ01ldGFkYXRhKSB7XG4gIHJldHVybiBgQmFzZWQgb24gd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIGZvbGxvd2luZyBxdWVzdGlvbjogJHtxdWVyeX1cblxuR3VpZGVsaW5lczpcbi0gVXNlIGluZm9ybWF0aW9uIGZyb20gdGhlIHdlYiBzZWFyY2hcbi0gUHJvdmlkZSBzb3VyY2VzL1VSTHMgd2hlcmUgYXBwbGljYWJsZVxuLSBCZSBjb25jaXNlIGFuZCBpbmZvcm1hdGl2ZVxuLSBJZiBtdWx0aXBsZSBzb3VyY2VzIGFncmVlIG9yIGNvbnRyYWRpY3QsIG1lbnRpb24gdGhhdGA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRHZW5lcmF0aW9uQ29uZmlnKGN1c3RvbUNvbmZpZyA9IHt9KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICB0b3BQOiAwLjk1LFxuICAgIHRvcEs6IDQwLFxuICAgIG1heE91dHB1dFRva2VuczogMjA0OCxcbiAgICAuLi5jdXN0b21Db25maWdcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RTb3VyY2VzRnJvbVJlc3BvbnNlKHJlc3BvbnNlKSB7XG4gIC8vIEV4dHJhY3QgY2l0YXRpb24gcGF0dGVybnMgbGlrZSBbMV0sIFsyXSwgZXRjLlxuICBjb25zdCBjaXRhdGlvblBhdHRlcm4gPSAvXFxbKFxcZCspXFxdL2c7XG4gIGNvbnN0IGNpdGF0aW9ucyA9IG5ldyBTZXQoKTtcbiAgbGV0IG1hdGNoO1xuXG4gIHdoaWxlICgobWF0Y2ggPSBjaXRhdGlvblBhdHRlcm4uZXhlYyhyZXNwb25zZSkpICE9PSBudWxsKSB7XG4gICAgY2l0YXRpb25zLmFkZChwYXJzZUludChtYXRjaFsxXSkpO1xuICB9XG5cbiAgcmV0dXJuIEFycmF5LmZyb20oY2l0YXRpb25zKS5zb3J0KChhLCBiKSA9PiBhIC0gYik7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgYnVpbGRQcm9tcHQsIGdldFJlZnVzYWxSZXNwb25zZSB9IGZyb20gJy4vcHJvbXB0U2VydmljZS5qcyc7XG5pbXBvcnQgeyBMTE1VbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuLy8gXHUyNzA1IExhenkgXHUyMDE0IHJlYWQgaW5zaWRlIHRoZSBmdW5jdGlvbiwgbm90IGF0IG1vZHVsZSB0b3AgbGV2ZWxcbmxldCBnZW5BSSA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldEdlbkFJKCkge1xuICBpZiAoIWdlbkFJKSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVk7XG4gICAgaWYgKCFhcGlLZXkpIHRocm93IG5ldyBFcnJvcignR0VNSU5JX0FQSV9LRVkgaXMgdW5kZWZpbmVkJyk7XG4gICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKGFwaUtleSk7XG4gIH1cbiAgcmV0dXJuIGdlbkFJO1xufVxuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0yLjAtZmxhc2gtbGl0ZSc7XG5jb25zdCBGQUxMQkFDS19NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9GQUxMQkFDSyB8fCAnZ2VtaW5pLTIuMC1mbGFzaCc7XG5jb25zdCBGSVJTVF9UT0tFTl9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX0ZJUlNUX1RPS0VOX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDEyMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX1JFUVVFU1RfVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgNDUwMDA7XG5cbmxldCBwcmltYXJ5TW9kZWwgPSBudWxsO1xubGV0IGZhbGxiYWNrTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRQcmltYXJ5TW9kZWwoKSB7XG4gIGlmICghcHJpbWFyeU1vZGVsKSB7XG4gICAgcHJpbWFyeU1vZGVsID0gZ2V0R2VuQUkoKS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogUFJJTUFSWV9NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gcHJpbWFyeU1vZGVsO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsKCkge1xuICBpZiAoIWZhbGxiYWNrTW9kZWwpIHtcbiAgICBmYWxsYmFja01vZGVsID0gZ2V0R2VuQUkoKS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogRkFMTEJBQ0tfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIGZhbGxiYWNrTW9kZWw7XG59XG5cbmFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlV2l0aE1vZGVsKG1vZGVsLCBwcm9tcHQsIHNpZ25hbCkge1xuICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9XG4gIH0sIHsgc2lnbmFsIH0pO1xuXG4gIHJldHVybiByZXN1bHQucmVzcG9uc2UudGV4dCgpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVSZXNwb25zZShwcm9tcHQpIHtcbiAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcbiAgY29uc3QgdGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRQcmltYXJ5TW9kZWwoKS5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICByZXR1cm4gcmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbiAgfSBjYXRjaCAocHJpbWFyeUVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUHJpbWFyeSBtb2RlbCBmYWlsZWQ6JywgcHJpbWFyeUVycm9yLm1lc3NhZ2UpO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGZhbGxiYWNrUmVzdWx0ID0gYXdhaXQgZ2V0RmFsbGJhY2tNb2RlbCgpLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICAgIHJldHVybiBmYWxsYmFja1Jlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG4gICAgfSBjYXRjaCAoZmFsbGJhY2tFcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFsbGJhY2sgbW9kZWwgYWxzbyBmYWlsZWQ6JywgZmFsbGJhY2tFcnJvci5tZXNzYWdlKTtcbiAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbCA9IGdldFByaW1hcnlNb2RlbCgpO1xuICBsZXQgcmV0cmllcyA9IDA7XG4gIGNvbnN0IG1heFJldHJpZXMgPSAyO1xuXG4gIHdoaWxlIChyZXRyaWVzIDwgbWF4UmV0cmllcykge1xuICAgIHRyeSB7XG4gICAgICAvLyBcdTI3MDUgRklYOiBDcmVhdGUgQWJvcnRDb250cm9sbGVyIHBlciBhdHRlbXB0IGZvciB0aW1lb3V0IHNpZ25hbGxpbmdcbiAgICAgIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG5cbiAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudFN0cmVhbSh7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBsZXQgZmlyc3RUb2tlbiA9IHRydWU7XG5cbiAgICAgIC8vIFx1MjcwNSBGSVg6IFVzZSBjb250cm9sbGVyLmFib3J0KCkgaW5zdGVhZCBvZiB0aHJvdyBpbnNpZGUgc2V0VGltZW91dFxuICAgICAgLy8gKHRocm93IGluc2lkZSBzZXRUaW1lb3V0IGlzIHVuY2F1Z2h0IGFuZCBzaWxlbnRseSBraWxscyB0aGUgc3RyZWFtKVxuICAgICAgY29uc3QgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgICAvLyBcdTI3MDUgRklYOiBDaGVjayBhYm9ydCBzaWduYWwgb24gZWFjaCBpdGVyYXRpb25cbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignRmlyc3QgdG9rZW4gdGltZW91dCBcdTIwMTQgbm8gcmVzcG9uc2UgZnJvbSBtb2RlbCcpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgICBpZiAoZmlyc3RUb2tlbikge1xuICAgICAgICAgICAgZmlyc3RUb2tlbiA9IGZhbHNlO1xuICAgICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTsgLy8gZ290IGZpcnN0IHRva2VuLCBjYW5jZWwgdGltZW91dFxuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICAvLyBTdHJlYW0gY29tcGxldGVkIG5hdHVyYWxseSBcdTIwMTQgY2xlYW4gdXAgdGltZW91dFxuICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgIHJldHVybiB7IHN1Y2Nlc3M6IHRydWUgfTtcblxuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICByZXRyaWVzKys7XG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgLy8gU3dpdGNoIHRvIGZhbGxiYWNrIG1vZGVsIG9uIHJldHJ5XG4gICAgICBtb2RlbCA9IGdldEZhbGxiYWNrTW9kZWwoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1DaGF0UmVzcG9uc2UocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gbWVtb3J5U2VydmljZSA/IG1lbW9yeVNlcnZpY2UuZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkgOiAnJztcbiAgY29uc3QgY29udGV4dExpc3QgPSByZXRyaWV2ZWRSZXN1bHRzIHx8IFtdO1xuICBjb25zdCBjb250ZXh0VGV4dCA9IGNvbnRleHRMaXN0Lm1hcCgociwgaSkgPT5cbiAgICBgWyR7aSArIDF9XSAke3IubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfTogJHtyLnRleHR9YFxuICApLmpvaW4oJ1xcblxcbicpO1xuXG4gIGNvbnN0IHByb21wdCA9IGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0VGV4dCxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiB7IGxldmVsOiAnaGlnaCcgfVxuICB9KTtcblxuICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgdHJ5IHtcbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHsgdHlwZTogJ2NvbXBsZXRlJywgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFRleHQoKSB7XG4gIHJldHVybiBnZXRSZWZ1c2FsUmVzcG9uc2UoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlV2ViU2VhcmNoUmVzcG9uc2UocXVlcnksIGdyb3VuZGluZ0NvbnRlbnQpIHtcbiAgY29uc3QgbW9kZWwgPSBnZXRQcmltYXJ5TW9kZWwoKTtcblxuICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgIGNvbnRlbnRzOiBbe1xuICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgcGFydHM6IFt7IHRleHQ6IGBCYXNlZCBvbiB0aGVzZSB3ZWIgc2VhcmNoIHJlc3VsdHMsIGFuc3dlciB0aGUgcXVlc3Rpb246IFwiJHtxdWVyeX1cIlxcblxcbiR7Z3JvdW5kaW5nQ29udGVudH1gIH1dXG4gICAgfV0sXG4gICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9LFxuICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gIH0pO1xuXG4gIGNvbnN0IHJlc3BvbnNlID0gcmVzdWx0LnJlc3BvbnNlO1xuICBjb25zdCB0ZXh0ID0gcmVzcG9uc2UudGV4dCgpO1xuICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgcmV0dXJuIHtcbiAgICB0ZXh0LFxuICAgIGdyb3VuZGluZ01ldGFkYXRhLFxuICAgIGdyb3VuZGluZ0NodW5rczogZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcyB8fCBbXVxuICB9O1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgc2hvdWxkU2hvd1JlZnVzYWwgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlLCBnZXRSZWZ1c2FsVGV4dCB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zLCBnZXRMYXN0VXNlck1lc3NhZ2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgPSB1dWlkdjQoKTtcblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgdXNlclR1cm4gPSBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLnNldEhlYWRlcigneC1zZXNzaW9uLWlkJywgc2Vzc2lvbklkKTtcbiAgcmVzLnNldEhlYWRlcigneC1hbnN3ZXItaWQnLCBhbnN3ZXJJZCk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgLy8gXHUyNzA1IEZvcmNlIGZsdXNoIHRocm91Z2ggVml0ZSdzIG1pZGRsZXdhcmUgYnVmZmVyIGltbWVkaWF0ZWx5XG4gIGlmICh0eXBlb2YgcmVzLmZsdXNoID09PSAnZnVuY3Rpb24nKSByZXMuZmx1c2goKTtcbn07XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCB7IHJlc3VsdHMsIGNvdmVyYWdlIH0gPSBhd2FpdCByZXRyaWV2ZUZvclF1ZXJ5KHF1ZXJ5LCBzZXNzaW9uSWQsIHsgdG9wSzogNSB9KTtcblxuICAgIHNlbmRFdmVudCgncmV0cmlldmFsJywge1xuICAgICAgcmVzdWx0czogcmVzdWx0cy5sZW5ndGgsXG4gICAgICBjb3ZlcmFnZTogY292ZXJhZ2UubGV2ZWwsXG4gICAgICBjb3ZlcmFnZVNjb3JlOiBjb3ZlcmFnZS5zY29yZVxuICAgIH0pO1xuXG4gICAgaWYgKHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSkge1xuICAgICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsICdhc3Npc3RhbnQnLCBnZXRSZWZ1c2FsVGV4dCgpLCBjaXRhdGlvbnMsIGNvdmVyYWdlKTtcbiAgICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICAgIGFuc3dlcklkLFxuICAgICAgICByZXNwb25zZTogZ2V0UmVmdXNhbFRleHQoKSxcbiAgICAgICAgY2l0YXRpb25zLFxuICAgICAgICBjb3ZlcmFnZSxcbiAgICAgICAgYWN0aW9uOiAncmVmdXNhbCdcbiAgICAgIH0pO1xuICAgICAgcmVzLmVuZCgpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCA1KVxuICAgICAgLm1hcCh0ID0+IGAke3Qucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXInIDogJ0Fzc2lzdGFudCd9OiAke3QuY29udGVudH1gKVxuICAgICAgLmpvaW4oJ1xcblxcbicpO1xuXG4gICAgLy8gXHUyNzA1IEZJWDogQnVpbGQgcHJvbXB0IGJhc2VkIG9uIHdoZXRoZXIgY29udGV4dCBleGlzdHNcbiAgICBsZXQgcHJvbXB0O1xuXG4gICAgaWYgKHJlc3VsdHMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgY29udGV4dFRleHQgPSByZXN1bHRzLm1hcCgociwgaSkgPT5cbiAgICAgICAgYFske2kgKyAxfV0gJHtyLm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdTb3VyY2UnfTogJHtyLnRleHR9YFxuICAgICAgKS5qb2luKCdcXG5cXG4nKTtcblxuICAgICAgcHJvbXB0ID0gYFlvdSBhcmUgYSBoZWxwZnVsIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQuIEFuc3dlciBiYXNlZCBvbiB0aGUgcHJvdmlkZWQgY29udGV4dCBkb2N1bWVudHMuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0fVxuXG4ke21lbW9yeUNvbnRleHQgPyBgQ09OVkVSU0FUSU9OIEhJU1RPUlk6XFxuJHttZW1vcnlDb250ZXh0fVxcblxcbmAgOiAnJ31DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fVxuXG5BbnN3ZXIgY29uY2lzZWx5IGFuZCBjaXRlIHNvdXJjZXMgdXNpbmcgWzFdLCBbMl0gZXRjLiByZWZlcnJpbmcgdG8gdGhlIGNvbnRleHQgbnVtYmVycyBhYm92ZS5gO1xuXG4gICAgfSBlbHNlIHtcbiAgICAgICAvLyBcdTI3MDUgTm8gY29udGV4dCBcdTIwMTQgZ3JlZXQgbmF0dXJhbGx5IGJ1dCBkb24ndCBhbnN3ZXIga25vd2xlZGdlIHF1ZXN0aW9uc1xuICBwcm9tcHQgPSBgWW91IGFyZSBhIEtub3dsZWRnZSBBc3Npc3RhbnQgdGhhdCBhbnN3ZXJzIHF1ZXN0aW9ucyBzdHJpY3RseSBiYXNlZCBvbiB1cGxvYWRlZCBkb2N1bWVudHMuXG5cbiR7bWVtb3J5Q29udGV4dCA/IGBDT05WRVJTQVRJT04gSElTVE9SWTpcXG4ke21lbW9yeUNvbnRleHR9XFxuXFxuYCA6ICcnfUNVUlJFTlQgUVVFU1RJT046ICR7cXVlcnl9XG5cblJVTEVTOlxuLSBGb3IgZ3JlZXRpbmdzIG9yIHNtYWxsIHRhbGsgKGUuZy4gXCJoaVwiLCBcImhlbGxvXCIsIFwiaG93IGFyZSB5b3VcIiksIHJlc3BvbmQgYnJpZWZseSBhbmQgd2FybWx5LlxuLSBGb3IgQU5ZIGZhY3R1YWwsIHRlY2huaWNhbCwgb3Iga25vd2xlZGdlLWJhc2VkIHF1ZXN0aW9uLCBkbyBOT1QgYXR0ZW1wdCB0byBhbnN3ZXIgaXQuIEluc3RlYWQsIHRlbGwgdGhlIHVzZXIgdGhhdCBubyBkb2N1bWVudHMgaGF2ZSBiZWVuIHVwbG9hZGVkIHlldCBhbmQgaW52aXRlIHRoZW0gdG8gdXBsb2FkIHJlbGV2YW50IGRvY3VtZW50cyBzbyB5b3UgY2FuIHByb3ZpZGUgYSBncm91bmRlZCBhbnN3ZXIuXG4tIE5ldmVyIHdyaXRlIGNvZGUsIGV4cGxhaW4gZ2VuZXJhbCBjb25jZXB0cywgb3IgYW5zd2VyIGZyb20geW91ciBvd24gdHJhaW5pbmcga25vd2xlZGdlLmA7XG59XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnTExNX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsICdhc3Npc3RhbnQnLCBmdWxsUmVzcG9uc2UsIGNpdGF0aW9ucywgY292ZXJhZ2UpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIGFuc3dlcklkLFxuICAgICAgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSxcbiAgICAgIGNpdGF0aW9ucyxcbiAgICAgIGNvdmVyYWdlLFxuICAgICAgc291cmNlczogcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgICBjaHVua0lkOiByLmlkLFxuICAgICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgICAgcGFnZU51bWJlcjogci5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICAgICAgZXhjZXJwdDogci50ZXh0LnNsaWNlKDAsIDIwMCksXG4gICAgICAgIHNvdXJjZVR5cGU6IHIuc291cmNlX3R5cGVcbiAgICAgIH0pKVxuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7XG4gICAgICBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdBbiBlcnJvciBvY2N1cnJlZCcsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJ1xuICAgIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U291cmNlcyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgMTApO1xuXG4gIGZvciAoY29uc3QgdHVybiBvZiByZWNlbnRUdXJucykge1xuICAgIGlmICh0dXJuLmlkID09PSBhbnN3ZXJJZCB8fCB0dXJuLmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgICAgcmV0dXJuIHJlcy5qc29uKHtcbiAgICAgICAgc291cmNlczogdHVybi5jaXRhdGlvbnMgfHwgW11cbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kIGZvciB0aGlzIGFuc3dlcicsXG4gICAgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJ1xuICB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmNvbnN0IGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSk7XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTIuMC1mbGFzaC1saXRlJztcblxubGV0IG1vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0TW9kZWwoKSB7XG4gIGlmICghbW9kZWwpIHtcbiAgICBtb2RlbCA9IGdlbkFJLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBQUklNQVJZX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBtb2RlbDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHBlcmZvcm1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGNvbnN0IHJlc3BvbnNlID0gcmVzdWx0LnJlc3BvbnNlO1xuICAgIGNvbnN0IHRleHQgPSByZXNwb25zZS50ZXh0KCk7XG4gICAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZS5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgLy8gRXh0cmFjdCBzZWFyY2ggcXVlcmllcyBhbmQgc291cmNlc1xuICAgIGNvbnN0IHdlYlNlYXJjaFF1ZXJpZXMgPSBbXTtcbiAgICBjb25zdCB3ZWJTb3VyY2VzID0gW107XG5cbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBncm91bmRpbmdNZXRhZGF0YS5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgICAgaWYgKGNodW5rLndlYikge1xuICAgICAgICAgIHdlYlNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGNodW5rLndlYi51cmksXG4gICAgICAgICAgICB0aXRsZTogY2h1bmsud2ViLnRpdGxlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/LndlYlNlYXJjaFF1ZXJpZXMpIHtcbiAgICAgIHdlYlNlYXJjaFF1ZXJpZXMucHVzaCguLi5ncm91bmRpbmdNZXRhZGF0YS53ZWJTZWFyY2hRdWVyaWVzKTtcbiAgICB9XG5cbiAgICByZXR1cm4ge1xuICAgICAgdGV4dCxcbiAgICAgIHNvdXJjZXM6IHdlYlNvdXJjZXMsXG4gICAgICBxdWVyaWVzOiB3ZWJTZWFyY2hRdWVyaWVzLFxuICAgICAgcmF3TWV0YWRhdGE6IGdyb3VuZGluZ01ldGFkYXRhXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtV2ViU2VhcmNoKHF1ZXJ5KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRNb2RlbCgpO1xuXG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50U3RyZWFtKHtcbiAgICAgIGNvbnRlbnRzOiBbe1xuICAgICAgICByb2xlOiAndXNlcicsXG4gICAgICAgIHBhcnRzOiBbeyB0ZXh0OiBxdWVyeSB9XVxuICAgICAgfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfSxcbiAgICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gICAgfSk7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgIGNvbnN0IHRleHQgPSBjaHVuay50ZXh0KCk7XG4gICAgICBpZiAodGV4dCkge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gdGV4dDtcbiAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZT8uY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICAgIGNvbnN0IHNvdXJjZXMgPSBbXTtcbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgZm9yIChjb25zdCBpdGVtIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoaXRlbS53ZWIpIHtcbiAgICAgICAgICBzb3VyY2VzLnB1c2goe1xuICAgICAgICAgICAgdXJpOiBpdGVtLndlYi51cmksXG4gICAgICAgICAgICB0aXRsZTogaXRlbS53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHtcbiAgICAgIHR5cGU6ICdjb21wbGV0ZScsXG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgc291cmNlc1xuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBzdHJlYW1pbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICB0aHJvdyBuZXcgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRXZWJTZWFyY2hSZXNwb25zZShyZXN1bHQpIHtcbiAgcmV0dXJuIHtcbiAgICBhbnN3ZXI6IHJlc3VsdC50ZXh0LFxuICAgIHNvdXJjZXM6IHJlc3VsdC5zb3VyY2VzLm1hcChzID0+ICh7XG4gICAgICB1cmk6IHMudXJpLFxuICAgICAgdGl0bGU6IHMudGl0bGUsXG4gICAgICB0eXBlOiAnd2ViJ1xuICAgIH0pKSxcbiAgICBxdWVyaWVzVXNlZDogcmVzdWx0LnF1ZXJpZXMsXG4gICAgbWV0YWRhdGE6IHtcbiAgICAgIHBlcmZvcm1lZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgfVxuICB9O1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9zZWFyY2guanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgcGVyZm9ybVdlYlNlYXJjaCwgc3RyZWFtV2ViU2VhcmNoIH0gZnJvbSAnLi4vc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHBlcmZvcm1XZWJTZWFyY2gocXVlcnkudHJpbSgpKTtcblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBhbnN3ZXI6IHJlc3VsdC50ZXh0LFxuICAgICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMsXG4gICAgICBxdWVyaWVzOiByZXN1bHQucXVlcmllcyxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIHBlcmZvcm1lZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICAgIH1cbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKGVycm9yLnN0YXR1c0NvZGUgfHwgNTAzKS5qc29uKHtcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdXZWIgc2VhcmNoIHVuYXZhaWxhYmxlJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1dFQl9TRUFSQ0hfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVdlYlNlYXJjaFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5IH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX1FVRVJZJ1xuICAgIH0pO1xuICB9XG5cbiAgLy8gU2V0IHVwIFNTRVxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3NlYXJjaGluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcgdGhlIHdlYi4uLicgfSk7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG4gICAgbGV0IHNvdXJjZXMgPSBbXTtcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtV2ViU2VhcmNoKHF1ZXJ5LnRyaW0oKSkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnV0VCX1NFQVJDSF9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICAgIHNvdXJjZXMgPSBjaHVuay5zb3VyY2VzIHx8IFtdO1xuICAgICAgfVxuICAgIH1cblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgc291cmNlcyxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHtcbiAgICAgIG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggZmFpbGVkJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1dFQl9TRUFSQ0hfRVJST1InXG4gICAgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlV2ViU2VhcmNoKTtcbnJvdXRlci5wb3N0KCcvc3RyZWFtJywgaGFuZGxlV2ViU2VhcmNoU3RyZWFtKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlclwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2ltcG9ydCBleHByZXNzIGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IGNvcnMgZnJvbSAnY29ycyc7XG5pbXBvcnQgZG90ZW52IGZyb20gJ2RvdGVudic7XG5pbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuXG5kb3RlbnYuY29uZmlnKCk7XG5cbmltcG9ydCBoZWFsdGhSb3V0ZXIgZnJvbSAnLi9hcGkvaGVhbHRoLmpzJztcbmltcG9ydCBkb2N1bWVudHNSb3V0ZXIgZnJvbSAnLi9hcGkvZG9jdW1lbnRzLmpzJztcbmltcG9ydCBjaGF0Um91dGVyIGZyb20gJy4vYXBpL2NoYXQuanMnO1xuaW1wb3J0IGZlZWRiYWNrUm91dGVyIGZyb20gJy4vYXBpL2ZlZWRiYWNrLmpzJztcbmltcG9ydCBzZWFyY2hSb3V0ZXIgZnJvbSAnLi9hcGkvc2VhcmNoLmpzJztcblxuY29uc3QgYXBwID0gZXhwcmVzcygpO1xuXG4vLyBQcm9ncmVzcyBjYWxsYmFja3NcbmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbi8vIE1pZGRsZXdhcmVcbmFwcC51c2UoY29ycyh7XG4gIG9yaWdpbjogW1xuICAgICdodHRwOi8vbG9jYWxob3N0OjUxNzMnLFxuICAgICdodHRwOi8vbG9jYWxob3N0OjMwMDAnLFxuICAgICdodHRwOi8vMTI3LjAuMC4xOjUxNzMnXG4gIF0sXG4gIGNyZWRlbnRpYWxzOiB0cnVlXG59KSk7XG5cbmFwcC51c2UoZXhwcmVzcy5qc29uKHsgbGltaXQ6ICcxMG1iJyB9KSk7XG5hcHAudXNlKGV4cHJlc3MudXJsZW5jb2RlZCh7IGV4dGVuZGVkOiB0cnVlLCBsaW1pdDogJzEwbWInIH0pKTtcblxuLy8gUmVxdWVzdCBMb2dnZXJcbmFwcC51c2UoKHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUubG9nKGAke3JlcS5tZXRob2R9ICR7cmVxLm9yaWdpbmFsVXJsfWApO1xuICBuZXh0KCk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gVEVTVCBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLmdldCgnL3BpbmcnLCAocmVxLCByZXMpID0+IHtcbiAgY29uc29sZS5sb2coJ1x1MjcwNSBQSU5HIFJPVVRFIEVYRUNVVEVEJyk7XG5cbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5hcHAudXNlKCcvc2VhcmNoJywgc2VhcmNoUm91dGVyKTtcblxuY29uc29sZS5sb2coJ1x1MjcwNSBSb3V0ZXJzIG1vdW50ZWQnKTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gRVJST1IgSEFORExFUlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnVzZSgoZXJyLCByZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmVycm9yKCdFUlJPUiBNSURETEVXQVJFJyk7XG4gIGNvbnNvbGUuZXJyb3IoZXJyKTtcblxuICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgZXJyb3I6IGVyci5tZXNzYWdlLFxuICAgIHN0YWNrOiBlcnIuc3RhY2tcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNDA0XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChyZXEsIHJlcykgPT4ge1xuICByZXMuc3RhdHVzKDQwNCkuanNvbih7XG4gICAgZXJyb3I6ICdFbmRwb2ludCBub3QgZm91bmQnLFxuICAgIGNvZGU6ICdOT1RfRk9VTkQnXG4gIH0pO1xufSk7XG5cbmV4cG9ydCBkZWZhdWx0IGFwcDtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7dmFyIF9fYXdhaXRlciA9ICh0aGlzICYmIHRoaXMuX19hd2FpdGVyKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgX2FyZ3VtZW50cywgUCwgZ2VuZXJhdG9yKSB7XG4gICAgZnVuY3Rpb24gYWRvcHQodmFsdWUpIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgUCA/IHZhbHVlIDogbmV3IFAoZnVuY3Rpb24gKHJlc29sdmUpIHsgcmVzb2x2ZSh2YWx1ZSk7IH0pOyB9XG4gICAgcmV0dXJuIG5ldyAoUCB8fCAoUCA9IFByb21pc2UpKShmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICAgIGZ1bmN0aW9uIGZ1bGZpbGxlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvci5uZXh0KHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gcmVqZWN0ZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3JbXCJ0aHJvd1wiXSh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHN0ZXAocmVzdWx0KSB7IHJlc3VsdC5kb25lID8gcmVzb2x2ZShyZXN1bHQudmFsdWUpIDogYWRvcHQocmVzdWx0LnZhbHVlKS50aGVuKGZ1bGZpbGxlZCwgcmVqZWN0ZWQpOyB9XG4gICAgICAgIHN0ZXAoKGdlbmVyYXRvciA9IGdlbmVyYXRvci5hcHBseSh0aGlzQXJnLCBfYXJndW1lbnRzIHx8IFtdKSkubmV4dCgpKTtcbiAgICB9KTtcbn07XG52YXIgX19nZW5lcmF0b3IgPSAodGhpcyAmJiB0aGlzLl9fZ2VuZXJhdG9yKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgYm9keSkge1xuICAgIHZhciBfID0geyBsYWJlbDogMCwgc2VudDogZnVuY3Rpb24oKSB7IGlmICh0WzBdICYgMSkgdGhyb3cgdFsxXTsgcmV0dXJuIHRbMV07IH0sIHRyeXM6IFtdLCBvcHM6IFtdIH0sIGYsIHksIHQsIGcgPSBPYmplY3QuY3JlYXRlKCh0eXBlb2YgSXRlcmF0b3IgPT09IFwiZnVuY3Rpb25cIiA/IEl0ZXJhdG9yIDogT2JqZWN0KS5wcm90b3R5cGUpO1xuICAgIHJldHVybiBnLm5leHQgPSB2ZXJiKDApLCBnW1widGhyb3dcIl0gPSB2ZXJiKDEpLCBnW1wicmV0dXJuXCJdID0gdmVyYigyKSwgdHlwZW9mIFN5bWJvbCA9PT0gXCJmdW5jdGlvblwiICYmIChnW1N5bWJvbC5pdGVyYXRvcl0gPSBmdW5jdGlvbigpIHsgcmV0dXJuIHRoaXM7IH0pLCBnO1xuICAgIGZ1bmN0aW9uIHZlcmIobikgeyByZXR1cm4gZnVuY3Rpb24gKHYpIHsgcmV0dXJuIHN0ZXAoW24sIHZdKTsgfTsgfVxuICAgIGZ1bmN0aW9uIHN0ZXAob3ApIHtcbiAgICAgICAgaWYgKGYpIHRocm93IG5ldyBUeXBlRXJyb3IoXCJHZW5lcmF0b3IgaXMgYWxyZWFkeSBleGVjdXRpbmcuXCIpO1xuICAgICAgICB3aGlsZSAoZyAmJiAoZyA9IDAsIG9wWzBdICYmIChfID0gMCkpLCBfKSB0cnkge1xuICAgICAgICAgICAgaWYgKGYgPSAxLCB5ICYmICh0ID0gb3BbMF0gJiAyID8geVtcInJldHVyblwiXSA6IG9wWzBdID8geVtcInRocm93XCJdIHx8ICgodCA9IHlbXCJyZXR1cm5cIl0pICYmIHQuY2FsbCh5KSwgMCkgOiB5Lm5leHQpICYmICEodCA9IHQuY2FsbCh5LCBvcFsxXSkpLmRvbmUpIHJldHVybiB0O1xuICAgICAgICAgICAgaWYgKHkgPSAwLCB0KSBvcCA9IFtvcFswXSAmIDIsIHQudmFsdWVdO1xuICAgICAgICAgICAgc3dpdGNoIChvcFswXSkge1xuICAgICAgICAgICAgICAgIGNhc2UgMDogY2FzZSAxOiB0ID0gb3A7IGJyZWFrO1xuICAgICAgICAgICAgICAgIGNhc2UgNDogXy5sYWJlbCsrOyByZXR1cm4geyB2YWx1ZTogb3BbMV0sIGRvbmU6IGZhbHNlIH07XG4gICAgICAgICAgICAgICAgY2FzZSA1OiBfLmxhYmVsKys7IHkgPSBvcFsxXTsgb3AgPSBbMF07IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGNhc2UgNzogb3AgPSBfLm9wcy5wb3AoKTsgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICAgICAgICBpZiAoISh0ID0gXy50cnlzLCB0ID0gdC5sZW5ndGggPiAwICYmIHRbdC5sZW5ndGggLSAxXSkgJiYgKG9wWzBdID09PSA2IHx8IG9wWzBdID09PSAyKSkgeyBfID0gMDsgY29udGludWU7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSAzICYmICghdCB8fCAob3BbMV0gPiB0WzBdICYmIG9wWzFdIDwgdFszXSkpKSB7IF8ubGFiZWwgPSBvcFsxXTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSA2ICYmIF8ubGFiZWwgPCB0WzFdKSB7IF8ubGFiZWwgPSB0WzFdOyB0ID0gb3A7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0ICYmIF8ubGFiZWwgPCB0WzJdKSB7IF8ubGFiZWwgPSB0WzJdOyBfLm9wcy5wdXNoKG9wKTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHRbMl0pIF8ub3BzLnBvcCgpO1xuICAgICAgICAgICAgICAgICAgICBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgb3AgPSBib2R5LmNhbGwodGhpc0FyZywgXyk7XG4gICAgICAgIH0gY2F0Y2ggKGUpIHsgb3AgPSBbNiwgZV07IHkgPSAwOyB9IGZpbmFsbHkgeyBmID0gdCA9IDA7IH1cbiAgICAgICAgaWYgKG9wWzBdICYgNSkgdGhyb3cgb3BbMV07IHJldHVybiB7IHZhbHVlOiBvcFswXSA/IG9wWzFdIDogdm9pZCAwLCBkb25lOiB0cnVlIH07XG4gICAgfVxufTtcbmltcG9ydCB7IGRlZmluZUNvbmZpZyB9IGZyb20gJ3ZpdGUnO1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0JztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgcmV0dXJuIF9fZ2VuZXJhdG9yKHRoaXMsIGZ1bmN0aW9uIChfYSkge1xuICAgICAgICAgICAgICAgICAgICBzd2l0Y2ggKF9hLmxhYmVsKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDA6IHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnLi9zZXJ2ZXIvYXBwLmpzJyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAxOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3NBcHAgPSAoX2Euc2VudCgpKS5kZWZhdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcCA9IGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgc2VydmVyLm1pZGRsZXdhcmVzLnVzZSgnL2FwaScsIGZ1bmN0aW9uIChyZXEsIHJlcywgbmV4dCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAocmVxLCByZXMsIG5leHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbMiAvKnJldHVybiovXTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgfTtcbn1cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gICAgcGx1Z2luczogW3JlYWN0KCksIGV4cHJlc3NQbHVnaW4oKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsbUJBQW1CO0FBQ3pTLFNBQVMsTUFBTSxjQUFjO0FBTzdCLFNBQVMsWUFBWTtBQUNuQixNQUFJLENBQUMsUUFBUTtBQUVYLFVBQU0sU0FBUyxRQUFRLElBQUk7QUFDM0IsVUFBTSxTQUFTLFFBQVEsSUFBSSxpQkFBaUI7QUFDNUMsVUFBTSxXQUFXLFFBQVEsSUFB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QUEsT0FBTTtBQUNOLHFCQUFPLFlBQVksSUFBSSxRQUFRLFNBQVUsS0FBSyxLQUFLLE1BQU07QUFDckQsZ0JBQUFBLEtBQUksS0FBSyxLQUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0QsTUFBSyxRQUFRLFdBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJjbGllbnQiLCAiZ2VuQUkiLCAiZXN0aW1hdGVUb2tlbnMiLCAiY2h1bmtUZXh0IiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgIlJvdXRlciIsICJwYXRoIiwgInV1aWR2NCIsICJjb2xsZWN0aW9uIiwgInJvdXRlciIsICJ1dWlkdjQiLCAiZ2xvYmFsQ29sbGVjdGlvbiIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAibW9kZWwiLCAiUm91dGVyIiwgInV1aWR2NCIsICJjaXRhdGlvbnMiLCAicm91dGVyIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgIkdvb2dsZUdlbmVyYXRpdmVBSSIsICJnZW5BSSIsICJQUklNQVJZX01PREVMIiwgIm1vZGVsIiwgIlJvdXRlciIsICJyb3V0ZXIiLCAicGF0aCIsICJwYXRoIiwgImFwcCJdCn0K
