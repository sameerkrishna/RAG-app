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
    if (!chromaConfig.apiKey) {
      throw new Error(
        "CRITICAL ERROR: CHROMA_API_KEY is undefined. Ensure your environment variables are correctly loaded before executing this file."
      );
    }
    const clientOptions = {
      apiKey: chromaConfig.apiKey,
      tenant: chromaConfig.tenant,
      database: chromaConfig.database
    };
    if (chromaConfig.host) {
      clientOptions.host = chromaConfig.host;
    }
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
var chromaConfig, client, globalCollection, sessionCollections;
var init_chromaService = __esm({
  "server/services/chromaService.js"() {
    "use strict";
    chromaConfig = {
      apiKey: process.env.CHROMA_API_KEY,
      tenant: process.env.CHROMA_TENANT || "default_tenant",
      database: process.env.CHROMA_DATABASE || "default_database",
      // Only needed if connecting to a non-default region (e.g., GCP europe-west1)
      host: process.env.CHROMA_HOST || void 0
    };
    console.log("---- CHROMA CONNECTIVITY DEBUG ----");
    console.log("Host:      ", chromaConfig.host || "api.trychroma.com (default)");
    console.log("Tenant:    ", chromaConfig.tenant);
    console.log("DB Name:   ", chromaConfig.database);
    console.log("API Key:   ", chromaConfig.apiKey ? "LOADED (VALID)" : "MISSING (UNDEFINED)");
    console.log("-----------------------------------");
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
function estimateTokens(text) {
  return Math.ceil(text.length / 4);
}
async function waitForRateLimit() {
  const now = Date.now();
  const windowElapsed = now - rateLimitState.windowStart;
  if (windowElapsed >= 6e4) {
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = now;
    return;
  }
  const remainingTokens = rateLimitState.maxTokensPerMinute - rateLimitState.tokenCount;
  if (remainingTokens <= 0) {
    const waitTime = 6e4 - windowElapsed;
    console.log(`Rate limit reached, waiting ${Math.ceil(waitTime / 1e3)}s`);
    await new Promise((resolve) => setTimeout(resolve, waitTime));
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = Date.now();
  }
}
async function embedWithRetry(text, attempt = 1, maxAttempts = 5) {
  const baseRetryDelay = 6e4;
  try {
    const result = await embeddingModel.embedContent(text);
    if (result.embedding) {
      return result.embedding.values;
    }
    throw new EmbeddingError("No embedding returned from API");
  } catch (error) {
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
async function embedBatch(texts) {
  const combinedText = texts.join("\n\n---\n\n");
  const tokens = estimateTokens(combinedText);
  await waitForRateLimit();
  rateLimitState.tokenCount += tokens;
  return embedWithRetry(combinedText);
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
    const batchPromises = batch.map(async (group) => {
      const texts = group.map((c) => c.text);
      try {
        return await embedBatch(texts);
      } catch (error) {
        console.error("Batch embedding failed:", error);
        return null;
      }
    });
    const batchResults = await Promise.all(batchPromises);
    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      const group = batch[j];
      if (result) {
        for (const chunk of group) {
          try {
            const individualEmbedding = await embedWithRetry(chunk.text);
            embeddings.push({
              id: chunk.metadata.chunk_id,
              embedding: individualEmbedding,
              metadata: chunk.metadata,
              text: chunk.text
            });
          } catch (error) {
            console.error(`Failed to embed chunk ${chunk.metadata.chunk_id}:`, error);
          }
        }
      } else {
        for (const chunk of group) {
          try {
            const individualEmbedding = await embedWithRetry(chunk.text);
            embeddings.push({
              id: chunk.metadata.chunk_id,
              embedding: individualEmbedding,
              metadata: chunk.metadata,
              text: chunk.text
            });
          } catch (error) {
            console.error(`Failed to embed chunk ${chunk.metadata.chunk_id}:`, error);
          }
        }
      }
    }
  }
  return embeddings;
}
async function embedQuery(query) {
  await waitForRateLimit();
  return embedWithRetry(query);
}
function getRateLimitState() {
  return { ...rateLimitState };
}
var genAI, embeddingModel, rateLimitState;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
    embeddingModel = genAI.getGenerativeModel({
      model: process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-2"
    });
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
    const queryEmbedding = await embedQuery(query);
    const queries = [];
    if (includeGlobal) {
      const globalCollection2 = await getGlobalCollection();
      queries.push({
        type: "global",
        promise: queryCollection(globalCollection2, queryEmbedding, topK)
      });
    }
    if (sessionId) {
      try {
        const sessionCollection = await getSessionCollection(sessionId);
        if (sessionCollection) {
          queries.push({
            type: "session",
            promise: queryCollection(sessionCollection, queryEmbedding, topK)
          });
        }
      } catch (e) {
      }
    }
    const results = await Promise.all(queries.map(async (q) => ({
      type: q.type,
      results: await q.promise
    })));
    const allResults = [];
    for (const { type, results: typeResults } of results) {
      for (const result of typeResults) {
        allResults.push({
          ...result,
          source_type: type
          // 'global' or 'session'
        });
      }
    }
    allResults.sort((a, b) => b.score - a.score);
    const topResults = allResults.slice(0, topK);
    const coverage = calculateCoverage(topResults, topK);
    return {
      results: topResults,
      coverage,
      queryEmbedding
    };
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
  return coverage.level === "low";
}
var TOP_K, COVERAGE_HIGH_THRESHOLD, COVERAGE_MEDIUM_THRESHOLD;
var init_retrievalService = __esm({
  "server/services/retrievalService.js"() {
    "use strict";
    init_chromaService();
    init_embeddingService();
    TOP_K = parseInt(process.env.TOP_K) || 5;
    COVERAGE_HIGH_THRESHOLD = parseFloat(process.env.COVERAGE_HIGH_THRESHOLD) || 0.75;
    COVERAGE_MEDIUM_THRESHOLD = parseFloat(process.env.COVERAGE_MEDIUM_THRESHOLD) || 0.55;
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
    REFUSAL_MESSAGE = "I couldn't find enough supporting information in the current knowledge base.";
  }
});

// server/services/geminiService.js
import { GoogleGenerativeAI as GoogleGenerativeAI2 } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getPrimaryModel() {
  if (!primaryModel) {
    primaryModel = genAI2.getGenerativeModel({ model: PRIMARY_MODEL });
  }
  return primaryModel;
}
function getFallbackModel() {
  if (!fallbackModel) {
    fallbackModel = genAI2.getGenerativeModel({ model: FALLBACK_MODEL });
  }
  return fallbackModel;
}
async function* streamResponse(prompt) {
  let model2 = getPrimaryModel();
  let retries = 0;
  const maxRetries = 2;
  while (retries < maxRetries) {
    try {
      const result = await model2.generateContentStream({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      });
      let firstToken = true;
      let firstTokenTimeout = setTimeout(() => {
        throw new Error("First token timeout");
      }, FIRST_TOKEN_TIMEOUT);
      for await (const chunk of result.stream) {
        const text = chunk.text();
        if (text) {
          if (firstToken) {
            firstToken = false;
            clearTimeout(firstTokenTimeout);
          }
          yield { type: "token", text };
        }
      }
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
    genAI2 = new GoogleGenerativeAI2(process.env.GEMINI_API_KEY);
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
      sendEvent("status", { stage: "refusal", message: getRefusalText() });
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
    const contextText = results.map(
      (r, i) => `[${i + 1}] ${r.metadata.filename || "Source"}: ${r.text}`
    ).join("\n\n");
    const memoryContext = getRecentTurns(sessionId, 5).map((t) => `${t.role === "user" ? "User" : "Assistant"}: ${t.content}`).join("\n\n");
    const prompt = `You are an AI Knowledge Assistant. Answer based ONLY on the provided context.

CONTEXT:
${contextText}

CONVERSATION HISTORY:
${memoryContext}

CURRENT QUESTION: ${query}

Answer concisely with citations like [1], [2] referring to context numbers.`;
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tIFwiY2hyb21hZGJcIjsgLy8gXHUyNzA1IFVzZSBDbG91ZENsaWVudCwgbm90IENocm9tYUNsaWVudFxuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IGNocm9tYUNvbmZpZyA9IHtcbiAgYXBpS2V5OiBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWSxcbiAgdGVuYW50OiBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCcsXG4gIGRhdGFiYXNlOiBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnLFxuICAvLyBPbmx5IG5lZWRlZCBpZiBjb25uZWN0aW5nIHRvIGEgbm9uLWRlZmF1bHQgcmVnaW9uIChlLmcuLCBHQ1AgZXVyb3BlLXdlc3QxKVxuICBob3N0OiBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQsXG59O1xuXG4vLyAtLS0gUlVOVElNRSBERUJVRyBMT0dTIC0tLVxuY29uc29sZS5sb2coXCItLS0tIENIUk9NQSBDT05ORUNUSVZJVFkgREVCVUcgLS0tLVwiKTtcbmNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgY2hyb21hQ29uZmlnLmhvc3QgfHwgXCJhcGkudHJ5Y2hyb21hLmNvbSAoZGVmYXVsdClcIik7XG5jb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIGNocm9tYUNvbmZpZy50ZW5hbnQpO1xuY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBjaHJvbWFDb25maWcuZGF0YWJhc2UpO1xuY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBjaHJvbWFDb25maWcuYXBpS2V5ID8gXCJMT0FERUQgKFZBTElEKVwiIDogXCJNSVNTSU5HIChVTkRFRklORUQpXCIpO1xuY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxubGV0IGNsaWVudCA9IG51bGw7XG5sZXQgZ2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5jb25zdCBzZXNzaW9uQ29sbGVjdGlvbnMgPSBuZXcgTWFwKCk7XG5cbmZ1bmN0aW9uIGdldENsaWVudCgpIHtcbiAgaWYgKCFjbGllbnQpIHtcbiAgICBpZiAoIWNocm9tYUNvbmZpZy5hcGlLZXkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgXCJDUklUSUNBTCBFUlJPUjogQ0hST01BX0FQSV9LRVkgaXMgdW5kZWZpbmVkLiBcIiArXG4gICAgICAgIFwiRW5zdXJlIHlvdXIgZW52aXJvbm1lbnQgdmFyaWFibGVzIGFyZSBjb3JyZWN0bHkgbG9hZGVkIGJlZm9yZSBleGVjdXRpbmcgdGhpcyBmaWxlLlwiXG4gICAgICApO1xuICAgIH1cblxuICAgIC8vIFx1MjcwNSBDbG91ZENsaWVudCBoYW5kbGVzIGF1dGggaW50ZXJuYWxseSBcdTIwMTQganVzdCBwYXNzIGFwaUtleSwgdGVuYW50LCBhbmQgZGF0YWJhc2VcbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0ge1xuICAgICAgYXBpS2V5OiBjaHJvbWFDb25maWcuYXBpS2V5LFxuICAgICAgdGVuYW50OiBjaHJvbWFDb25maWcudGVuYW50LFxuICAgICAgZGF0YWJhc2U6IGNocm9tYUNvbmZpZy5kYXRhYmFzZSxcbiAgICB9O1xuXG4gICAgLy8gT25seSBhZGQgaG9zdCBpZiBjb25uZWN0aW5nIHRvIGEgbm9uLWRlZmF1bHQgcmVnaW9uXG4gICAgaWYgKGNocm9tYUNvbmZpZy5ob3N0KSB7XG4gICAgICBjbGllbnRPcHRpb25zLmhvc3QgPSBjaHJvbWFDb25maWcuaG9zdDtcbiAgICB9XG5cbiAgICBjbGllbnQgPSBuZXcgQ2xvdWRDbGllbnQoY2xpZW50T3B0aW9ucyk7XG4gIH1cbiAgcmV0dXJuIGNsaWVudDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEdsb2JhbENvbGxlY3Rpb24oKSB7XG4gIGlmICghZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuXG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgICAgZGVzY3JpcHRpb246ICdQZXJtYW5lbnQgc2VlZCBkb2N1bWVudHMgZm9yIFJBRycsXG4gICAgICAgICAgdHlwZTogJ2dsb2JhbF9rbm93bGVkZ2UnXG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY3JlYXRlIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICBjb25zb2xlLmxvZyhcImNyZWF0ZWQgZ2xvYmFsIGRiXCIpO1xuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjbGllbnQgPSBnZXRDbGllbnQoKTtcbiAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBgc2Vzc2lvbl8ke3Nlc3Npb25JZH1gO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRPckNyZWF0ZUNvbGxlY3Rpb24oe1xuICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICB0eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICBzZXNzaW9uX2lkOiBzZXNzaW9uSWQsXG4gICAgICAgIGNyZWF0ZWQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLnNldChzZXNzaW9uSWQsIGNvbGxlY3Rpb24pO1xuICAgIGNvbnNvbGUubG9nKFwiY3JlYXRlZCBzZXNzaW9uIGRiXCIpO1xuICAgIHJldHVybiBjb2xsZWN0aW9uO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBjcmVhdGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gc2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGNsaWVudCA9IGdldENsaWVudCgpO1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG5cbiAgdHJ5IHtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBhZGRWZWN0b3JzKGNvbGxlY3Rpb24sIHZlY3RvcnMsIGVtYmVkZGluZ3MsIGlkcykge1xuICB0cnkge1xuICAgIGF3YWl0IGNvbGxlY3Rpb24uYWRkKHtcbiAgICAgIGlkcyxcbiAgICAgIGVtYmVkZGluZ3MsXG4gICAgICBkb2N1bWVudHM6IHZlY3RvcnMubWFwKHYgPT4gdi50ZXh0KSxcbiAgICAgIG1ldGFkYXRhczogdmVjdG9ycy5tYXAodiA9PiB2Lm1ldGFkYXRhKVxuICAgIH0pO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBhZGQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSyA9IDUpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5xdWVyeSh7XG4gICAgICBxdWVyeUVtYmVkZGluZ3M6IFtxdWVyeUVtYmVkZGluZ10sXG4gICAgICBuUmVzdWx0czogdG9wSyxcbiAgICAgIGluY2x1ZGU6IFsnZG9jdW1lbnRzJywgJ21ldGFkYXRhcycsICdkaXN0YW5jZXMnXVxuICAgIH0pO1xuXG4gICAgaWYgKCFyZXN1bHRzLmlkcyB8fCByZXN1bHRzLmlkcy5sZW5ndGggPT09IDAgfHwgcmVzdWx0cy5pZHNbMF0ubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdHMuaWRzWzBdLm1hcCgoaWQsIGlkeCkgPT4gKHtcbiAgICAgIGlkLFxuICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHNbMF1baWR4XSxcbiAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhc1swXVtpZHhdLFxuICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF0sXG4gICAgICBzY29yZTogMSAtIHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF1cbiAgICB9KSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIHF1ZXJ5IGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgd2hlcmU6IHsgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQgfVxuICAgIH0pO1xuXG4gICAgaWYgKGV4aXN0aW5nLmlkcyAmJiBleGlzdGluZy5pZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoe1xuICAgICAgICBpZHM6IGV4aXN0aW5nLmlkc1xuICAgICAgfSk7XG4gICAgICByZXR1cm4gZXhpc3RpbmcuaWRzLmxlbmd0aDtcbiAgICB9XG4gICAgcmV0dXJuIDA7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY291bnQgPSBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gICAgcmV0dXJuIGNvdW50O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBnZXQgZG9jdW1lbnQgY291bnQ6JywgZXJyb3IpO1xuICAgIHJldHVybiAwO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxJdGVtcyA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddXG4gICAgfSk7XG5cbiAgICBjb25zdCBkb2N1bWVudHNNYXAgPSBuZXcgTWFwKCk7XG5cbiAgICBpZiAoYWxsSXRlbXMuaWRzKSB7XG4gICAgICBhbGxJdGVtcy5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYWxsSXRlbXMubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYWxsSXRlbXMuZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtleHBvcnQgY2xhc3MgQXBwRXJyb3IgZXh0ZW5kcyBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUsIHN0YXR1c0NvZGUgPSA1MDApIHtcbiAgICBzdXBlcihtZXNzYWdlKTtcbiAgICB0aGlzLmNvZGUgPSBjb2RlO1xuICAgIHRoaXMuc3RhdHVzQ29kZSA9IHN0YXR1c0NvZGU7XG4gICAgdGhpcy5pc09wZXJhdGlvbmFsID0gdHJ1ZTtcbiAgICBFcnJvci5jYXB0dXJlU3RhY2tUcmFjZSh0aGlzLCB0aGlzLmNvbnN0cnVjdG9yKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVmFsaWRhdGlvbkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1ZBTElEQVRJT05fRVJST1InKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgY29kZSwgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVXBsb2FkTGltaXRFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdVUExPQURfTElNSVRfRVhDRUVERUQnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgY29kZSwgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRmlsZVRvb0xhcmdlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1heFNpemVNQikge1xuICAgIHN1cGVyKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCwgJ0ZJTEVfVE9PX0xBUkdFJywgNDEzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgSW52YWxpZEZpbGVUeXBlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdPbmx5IFBERiBmaWxlcyBhcmUgYWxsb3dlZCcsICdJTlZBTElEX0ZJTEVfVFlQRScsIDQxNSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFRvb01hbnlQREZzRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1heCkge1xuICAgIHN1cGVyKGBNYXhpbXVtICR7bWF4fSBQREZzIGFsbG93ZWQgcGVyIHNlc3Npb25gLCAnVE9PX01BTllfUERGUycsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIER1cGxpY2F0ZUZpbGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoZmlsZW5hbWUpIHtcbiAgICBzdXBlcihgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCwgJ0RVUExJQ0FURV9GSUxFJywgNDA5KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ29ycnVwdGVkUERGRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdGYWlsZWQgdG8gcGFyc2UgUERGIGZpbGUuIEl0IG1heSBiZSBjb3JydXB0ZWQuJywgJ0NPUlJVUFRFRF9QREYnLCA0MjIpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSYXRlTGltaXRFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IocmV0cnlBZnRlciA9IDYwKSB7XG4gICAgc3VwZXIoJ1JhdGUgbGltaXQgZXhjZWVkZWQuIFBsZWFzZSB0cnkgYWdhaW4gbGF0ZXIuJywgJ1JBVEVfTElNSVRfRVhDRUVERUQnLCA0MjkpO1xuICAgIHRoaXMucmV0cnlBZnRlciA9IHJldHJ5QWZ0ZXI7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIExMTVVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdBSSBzZXJ2aWNlIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlLiBQbGVhc2UgdHJ5IGFnYWluLicsICdMTE1fVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBFbWJlZGRpbmdFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSA9ICdGYWlsZWQgdG8gZ2VuZXJhdGUgZW1iZWRkaW5ncycpIHtcbiAgICBzdXBlcihtZXNzYWdlLCAnRU1CRURESU5HX0VSUk9SJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmV0cmlldmFsVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0RvY3VtZW50IHJldHJpZXZhbCBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZScsICdSRVRSSUVWQUxfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignV2ViIHNlYXJjaCBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZScsICdXRUJfU0VBUkNIX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ292ZXJhZ2VUb29Mb3dFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0luc3VmZmljaWVudCBpbmZvcm1hdGlvbiBpbiBrbm93bGVkZ2UgYmFzZScsICdDT1ZFUkFHRV9UT09fTE9XJywgMjAwKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNSZXRyeWFibGVFcnJvcihlcnJvcikge1xuICBjb25zdCByZXRyeWFibGVDb2RlcyA9IFsnUkFURV9MSU1JVF9FWENFRURFRCcsICdFTUJFRERJTkdfRVJST1InLCAnTExNX1VOQVZBSUxBQkxFJ107XG4gIHJldHVybiByZXRyeWFibGVDb2Rlcy5pbmNsdWRlcyhlcnJvci5jb2RlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzNDI5RXJyb3IoZXJyb3IpIHtcbiAgcmV0dXJuIGVycm9yPy5jb2RlID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCc0MjknKSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdUb28gTWFueSBSZXF1ZXN0cycpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuZXJhdGl2ZUFJIH0gZnJvbSAnQGdvb2dsZS9nZW5lcmF0aXZlLWFpJztcbmltcG9ydCB7IEVtYmVkZGluZ0Vycm9yLCBpczQyOUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuY29uc3QgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZKTtcbmNvbnN0IGVtYmVkZGluZ01vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHtcbiAgbW9kZWw6IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMidcbn0pO1xuLy8gUmF0ZSBsaW1pdGluZyBzdGF0ZVxuY29uc3QgcmF0ZUxpbWl0U3RhdGUgPSB7XG4gIHRva2VuQ291bnQ6IDAsXG4gIHdpbmRvd1N0YXJ0OiBEYXRlLm5vdygpLFxuICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgcGFyYWxsZWxDYWxsczogcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0LFxuICBtYXhDaHVua3NQZXJDYWxsOiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNyxcbiAgbGFzdENhbGxHcm91cFRpbWU6IG51bGxcbn07XG5cbmZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIDQpO1xufVxuXG5hc3luYyBmdW5jdGlvbiB3YWl0Rm9yUmF0ZUxpbWl0KCkge1xuICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICBjb25zdCB3aW5kb3dFbGFwc2VkID0gbm93IC0gcmF0ZUxpbWl0U3RhdGUud2luZG93U3RhcnQ7XG5cbiAgLy8gUmVzZXQgd2luZG93IGlmIGEgbWludXRlIGhhcyBwYXNzZWRcbiAgaWYgKHdpbmRvd0VsYXBzZWQgPj0gNjAwMDApIHtcbiAgICByYXRlTGltaXRTdGF0ZS50b2tlbkNvdW50ID0gMDtcbiAgICByYXRlTGltaXRTdGF0ZS53aW5kb3dTdGFydCA9IG5vdztcbiAgICByZXR1cm47XG4gIH1cblxuICAvLyBDaGVjayBpZiB3ZSdyZSBhdCB0aGUgcmF0ZSBsaW1pdFxuICBjb25zdCByZW1haW5pbmdUb2tlbnMgPSByYXRlTGltaXRTdGF0ZS5tYXhUb2tlbnNQZXJNaW51dGUgLSByYXRlTGltaXRTdGF0ZS50b2tlbkNvdW50O1xuICBpZiAocmVtYWluaW5nVG9rZW5zIDw9IDApIHtcbiAgICBjb25zdCB3YWl0VGltZSA9IDYwMDAwIC0gd2luZG93RWxhcHNlZDtcbiAgICBjb25zb2xlLmxvZyhgUmF0ZSBsaW1pdCByZWFjaGVkLCB3YWl0aW5nICR7TWF0aC5jZWlsKHdhaXRUaW1lIC8gMTAwMCl9c2ApO1xuICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCB3YWl0VGltZSkpO1xuICAgIHJhdGVMaW1pdFN0YXRlLnRva2VuQ291bnQgPSAwO1xuICAgIHJhdGVMaW1pdFN0YXRlLndpbmRvd1N0YXJ0ID0gRGF0ZS5ub3coKTtcbiAgfVxufVxuXG5hc3luYyBmdW5jdGlvbiBlbWJlZFdpdGhSZXRyeSh0ZXh0LCBhdHRlbXB0ID0gMSwgbWF4QXR0ZW1wdHMgPSA1KSB7XG4gIGNvbnN0IGJhc2VSZXRyeURlbGF5ID0gNjAwMDA7IC8vIDYwIHNlY29uZHMgZm9yIDQyOVxuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZW1iZWRkaW5nTW9kZWwuZW1iZWRDb250ZW50KHRleHQpO1xuXG4gICAgaWYgKHJlc3VsdC5lbWJlZGRpbmcpIHtcbiAgICAgIHJldHVybiByZXN1bHQuZW1iZWRkaW5nLnZhbHVlcztcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoJ05vIGVtYmVkZGluZyByZXR1cm5lZCBmcm9tIEFQSScpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIC8vIENoZWNrIGZvciA0MjkgcmF0ZSBsaW1pdFxuICAgIGlmIChpczQyOUVycm9yKGVycm9yKSB8fCBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHwgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSkge1xuICAgICAgaWYgKGF0dGVtcHQgPj0gbWF4QXR0ZW1wdHMpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdNYXggcmV0cnkgYXR0ZW1wdHMgcmVhY2hlZCBmb3IgcmF0ZSBsaW1pdGluZycpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCByZXRyeURlbGF5ID0gZXJyb3IucmV0cnlBZnRlciB8fCBiYXNlUmV0cnlEZWxheTtcbiAgICAgIGNvbnNvbGUubG9nKGBSYXRlIGxpbWl0ZWQsIHdhaXRpbmcgJHtyZXRyeURlbGF5IC8gMTAwMH1zIGJlZm9yZSByZXRyeSAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9YCk7XG5cbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCByZXRyeURlbGF5KSk7XG4gICAgICByZXR1cm4gZW1iZWRXaXRoUmV0cnkodGV4dCwgYXR0ZW1wdCArIDEsIG1heEF0dGVtcHRzKTtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnRW1iZWRkaW5nIGdlbmVyYXRpb24gZmFpbGVkJyk7XG4gIH1cbn1cblxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cykge1xuICAvLyBKb2luIHRleHRzIGZvciBzaW5nbGUgZW1iZWRkaW5nIGNhbGwgKEdlbWluaSBzdXBwb3J0cyB1cCB0byBzcGVjaWZpZWQgdG9rZW4gbGltaXQpXG4gIGNvbnN0IGNvbWJpbmVkVGV4dCA9IHRleHRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xuXG4gIGNvbnN0IHRva2VucyA9IGVzdGltYXRlVG9rZW5zKGNvbWJpbmVkVGV4dCk7XG4gIGF3YWl0IHdhaXRGb3JSYXRlTGltaXQoKTtcblxuICByYXRlTGltaXRTdGF0ZS50b2tlbkNvdW50ICs9IHRva2VucztcblxuICByZXR1cm4gZW1iZWRXaXRoUmV0cnkoY29tYmluZWRUZXh0KTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBbXTtcbiAgY29uc3QgbWF4Q2h1bmtzUGVyQ2FsbCA9IHJhdGVMaW1pdFN0YXRlLm1heENodW5rc1BlckNhbGw7XG4gIGNvbnN0IG1heFBhcmFsbGVsQ2FsbHMgPSByYXRlTGltaXRTdGF0ZS5wYXJhbGxlbENhbGxzO1xuXG4gIC8vIFNwbGl0IGNodW5rcyBpbnRvIGdyb3VwcyBvZiBtYXhDaHVua3NQZXJDYWxsXG4gIGNvbnN0IGdyb3VwcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gbWF4Q2h1bmtzUGVyQ2FsbCkge1xuICAgIGdyb3Vwcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgbWF4Q2h1bmtzUGVyQ2FsbCkpO1xuICB9XG5cbiAgLy8gUHJvY2VzcyBncm91cHMgaW4gYmF0Y2hlcyBvZiBwYXJhbGxlbCBjYWxsc1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGdyb3Vwcy5sZW5ndGg7IGkgKz0gbWF4UGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IGJhdGNoID0gZ3JvdXBzLnNsaWNlKGksIGkgKyBtYXhQYXJhbGxlbENhbGxzKTtcblxuICAgIC8vIFdhaXQgMSBtaW51dGUgYmV0d2VlbiBjYWxsIGdyb3VwcyAobm90IG9uIGZpcnN0IGdyb3VwKVxuICAgIGlmIChpID4gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1dhaXRpbmcgMSBtaW51dGUgYmVmb3JlIG5leHQgZW1iZWRkaW5nIGJhdGNoLi4uJyk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgNjAwMDApKTtcbiAgICB9XG5cbiAgICAvLyBFeGVjdXRlIGJhdGNoIGluIHBhcmFsbGVsXG4gICAgY29uc3QgYmF0Y2hQcm9taXNlcyA9IGJhdGNoLm1hcChhc3luYyAoZ3JvdXApID0+IHtcbiAgICAgIGNvbnN0IHRleHRzID0gZ3JvdXAubWFwKGMgPT4gYy50ZXh0KTtcbiAgICAgIHRyeSB7XG4gICAgICAgIHJldHVybiBhd2FpdCBlbWJlZEJhdGNoKHRleHRzKTtcbiAgICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoJ0JhdGNoIGVtYmVkZGluZyBmYWlsZWQ6JywgZXJyb3IpO1xuICAgICAgICAvLyBSZXR1cm4gbnVsbCBmb3IgZmFpbGVkIGJhdGNoZXMgLSB3ZSdsbCBoYW5kbGUgdGhpcyBiZWxvd1xuICAgICAgICByZXR1cm4gbnVsbDtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGNvbnN0IGJhdGNoUmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKGJhdGNoUHJvbWlzZXMpO1xuXG4gICAgLy8gUHJvY2VzcyByZXN1bHRzXG4gICAgZm9yIChsZXQgaiA9IDA7IGogPCBiYXRjaFJlc3VsdHMubGVuZ3RoOyBqKyspIHtcbiAgICAgIGNvbnN0IHJlc3VsdCA9IGJhdGNoUmVzdWx0c1tqXTtcbiAgICAgIGNvbnN0IGdyb3VwID0gYmF0Y2hbal07XG5cbiAgICAgIGlmIChyZXN1bHQpIHtcbiAgICAgICAgLy8gSWYgZW1iZWRkaW5nIHN1Y2NlZWRlZCwgd2UgbmVlZCB0byBzcGxpdCBiYWNrIGludG8gaW5kaXZpZHVhbCBlbWJlZGRpbmdzXG4gICAgICAgIC8vIFNpbmNlIEdlbWluaSByZXR1cm5zIG9uZSBlbWJlZGRpbmcgZm9yIHRoZSBjb21iaW5lZCB0ZXh0LCB3ZSdsbCBnZW5lcmF0ZSBpbmRpdmlkdWFsIGVtYmVkZGluZ3NcbiAgICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBncm91cCkge1xuICAgICAgICAgIHRyeSB7XG4gICAgICAgICAgICBjb25zdCBpbmRpdmlkdWFsRW1iZWRkaW5nID0gYXdhaXQgZW1iZWRXaXRoUmV0cnkoY2h1bmsudGV4dCk7XG4gICAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGEuY2h1bmtfaWQsXG4gICAgICAgICAgICAgIGVtYmVkZGluZzogaW5kaXZpZHVhbEVtYmVkZGluZyxcbiAgICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGVtYmVkIGNodW5rICR7Y2h1bmsubWV0YWRhdGEuY2h1bmtfaWR9OmAsIGVycm9yKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIFJldHJ5IGZhaWxlZCBncm91cCBpbmRpdmlkdWFsbHlcbiAgICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBncm91cCkge1xuICAgICAgICAgIHRyeSB7XG4gICAgICAgICAgICBjb25zdCBpbmRpdmlkdWFsRW1iZWRkaW5nID0gYXdhaXQgZW1iZWRXaXRoUmV0cnkoY2h1bmsudGV4dCk7XG4gICAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGEuY2h1bmtfaWQsXG4gICAgICAgICAgICAgIGVtYmVkZGluZzogaW5kaXZpZHVhbEVtYmVkZGluZyxcbiAgICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGVtYmVkIGNodW5rICR7Y2h1bmsubWV0YWRhdGEuY2h1bmtfaWR9OmAsIGVycm9yKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICByZXR1cm4gZW1iZWRkaW5ncztcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgYXdhaXQgd2FpdEZvclJhdGVMaW1pdCgpO1xuICByZXR1cm4gZW1iZWRXaXRoUmV0cnkocXVlcnkpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICBhd2FpdCB3YWl0Rm9yUmF0ZUxpbWl0KCk7XG4gIHJldHVybiBlbWJlZFdpdGhSZXRyeSh0ZXh0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJhdGVMaW1pdFN0YXRlKCkge1xuICByZXR1cm4geyAuLi5yYXRlTGltaXRTdGF0ZSB9O1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldFJhdGVMaW1pdFN0YXRlIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBDaGVjayBHZW1pbmkgKHZpYSBBUEkga2V5IHByZXNlbmNlKVxuICBoZWFsdGhTdGF0dXMuc2VydmljZXMuZ2VtaW5pID0ge1xuICAgIHN0YXR1czogcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkgPyAnY29uZmlndXJlZCcgOiAnbm90X2NvbmZpZ3VyZWQnXG4gIH07XG5cbiAgLy8gR2V0IHJhdGUgbGltaXQgc3RhdGVcbiAgaGVhbHRoU3RhdHVzLnJhdGVMaW1pdCA9IGdldFJhdGVMaW1pdFN0YXRlKCk7XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjsvLyBUb2tlbiBlc3RpbWF0aW9uOiB+NCBjaGFyYWN0ZXJzIHBlciB0b2tlbiBmb3IgRW5nbGlzaCB0ZXh0XG5jb25zdCBDSEFSU19QRVJfVE9LRU4gPSA0O1xuY29uc3QgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUyA9IDEwMDA7XG5jb25zdCBERUZBVUxUX09WRVJMQVBfVE9LRU5TID0gMjAwO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1RleHQodGV4dCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IGNodW5rU2l6ZVRva2VucyA9IG9wdGlvbnMuY2h1bmtTaXplVG9rZW5zIHx8IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUb2tlbnMgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgfHwgREVGQVVMVF9PVkVSTEFQX1RPS0VOUztcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2h1bmtTaXplQ2hhcnMgPSBjaHVua1NpemVUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG5cbiAgY29uc3QgY2h1bmtzID0gW107XG4gIGxldCBzdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcblxuICB3aGlsZSAoc3RhcnQgPCB0ZXh0Lmxlbmd0aCkge1xuICAgIGxldCBlbmQgPSBzdGFydCArIGNodW5rU2l6ZUNoYXJzO1xuXG4gICAgLy8gVHJ5IHRvIGZpbmQgYSBnb29kIGJyZWFrIHBvaW50XG4gICAgaWYgKGVuZCA8IHRleHQubGVuZ3RoKSB7XG4gICAgICBjb25zdCBicmVha1BvaW50cyA9IFsnLiAnLCAnLlxcbicsICchICcsICc/ICcsICdcXG5cXG4nLCAnXFxuJywgJyAnXTtcbiAgICAgIGxldCBiZXN0QnJlYWsgPSAtMTtcblxuICAgICAgLy8gTG9vayBmb3IgYnJlYWsgcG9pbnRzIGluIHRoZSBsYXN0IDIwJSBvZiB0aGUgY2h1bmtcbiAgICAgIGNvbnN0IHNlYXJjaFN0YXJ0ID0gZW5kIC0gTWF0aC5mbG9vcihjaHVua1NpemVDaGFycyAqIDAuMik7XG5cbiAgICAgIGZvciAoY29uc3QgYnJlYWtwb2ludCBvZiBicmVha1BvaW50cykge1xuICAgICAgICBjb25zdCBpZHggPSB0ZXh0Lmxhc3RJbmRleE9mKGJyZWFrcG9pbnQsIGVuZCk7XG4gICAgICAgIGlmIChpZHggPiBzZWFyY2hTdGFydCAmJiBpZHggPiBzdGFydCkge1xuICAgICAgICAgIGJlc3RCcmVhayA9IGlkeCArIGJyZWFrcG9pbnQubGVuZ3RoO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGlmIChiZXN0QnJlYWsgPiBzdGFydCkge1xuICAgICAgICBlbmQgPSBiZXN0QnJlYWs7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgY2h1bmtUZXh0ID0gdGV4dC5zbGljZShzdGFydCwgZW5kKS50cmltKCk7XG4gICAgaWYgKGNodW5rVGV4dC5sZW5ndGggPiAwKSB7XG4gICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgIHRleHQ6IGNodW5rVGV4dCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY2h1bmtUZXh0KSxcbiAgICAgICAgY2hhclN0YXJ0OiBzdGFydCxcbiAgICAgICAgY2hhckVuZDogZW5kLFxuICAgICAgICBjaHVua0luZGV4OiBjaHVua0luZGV4KytcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIC8vIE1vdmUgdG8gbmV4dCBjaHVuayB3aXRoIG92ZXJsYXBcbiAgICBzdGFydCA9IGVuZCAtIG92ZXJsYXBDaGFycztcbiAgICBpZiAoc3RhcnQgPD0gY2h1bmtzW2NodW5rcy5sZW5ndGggLSAxXT8uY2hhclN0YXJ0KSB7XG4gICAgICBzdGFydCA9IGVuZDtcbiAgICB9XG5cbiAgICAvLyBTYWZldHkgY2hlY2sgdG8gcHJldmVudCBpbmZpbml0ZSBsb29wc1xuICAgIGlmIChjaHVua0luZGV4ID4gMTAwMDApIHtcbiAgICAgIGNvbnNvbGUud2FybignQ2h1bmsgbGltaXQgcmVhY2hlZCwgc3RvcHBpbmcnKTtcbiAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQgfSA9IHBkZkRhdGE7XG5cbiAgY29uc3QgdGV4dENodW5rcyA9IGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zKTtcblxuICByZXR1cm4gdGV4dENodW5rcy5tYXAoY2h1bmsgPT4gKHtcbiAgICB0ZXh0OiBjaHVuay50ZXh0LFxuICAgIG1ldGFkYXRhOiB7XG4gICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgIGZpbGVuYW1lOiBmaWxlbmFtZSxcbiAgICAgIGNodW5rX2lkOiBgJHtkb2N1bWVudElkfV8ke2NodW5rLmNodW5rSW5kZXh9YCxcbiAgICAgIGNodW5rX2luZGV4OiBjaHVuay5jaHVua0luZGV4LFxuICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgIHNlY3Rpb25fdGl0bGU6IGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICBzb3VyY2VfdHlwZTogJ3BkZicsXG4gICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB0b2tlbl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgdG9rZW5fZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICB9XG4gIH0pKTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIC8vIFRyeSB0byBleHRyYWN0IGEgcG90ZW50aWFsIHNlY3Rpb24gdGl0bGUgZnJvbSB0aGUgYmVnaW5uaW5nIG9mIHRoZSBjaHVua1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBtZXJnZUNodW5rcyhjaHVua3MsIG1heFRva2VucyA9IDcwMDApIHtcbiAgLy8gTWVyZ2Ugc21hbGwgY2h1bmtzIHRvIHJlZHVjZSBBUEkgY2FsbHNcbiAgY29uc3QgbWVyZ2VkID0gW107XG4gIGxldCBjdXJyZW50ID0geyB0ZXh0czogW10sIHRvdGFsVG9rZW5zOiAwLCBtZXRhZGF0YTogW10gfTtcblxuICBmb3IgKGNvbnN0IGNodW5rIG9mIGNodW5rcykge1xuICAgIGlmIChjdXJyZW50LnRvdGFsVG9rZW5zICsgY2h1bmsudG9rZW5Db3VudCA8PSBtYXhUb2tlbnMpIHtcbiAgICAgIGN1cnJlbnQudGV4dHMucHVzaChjaHVuay50ZXh0KTtcbiAgICAgIGN1cnJlbnQubWV0YWRhdGEucHVzaChjaHVuay5tZXRhZGF0YSk7XG4gICAgICBjdXJyZW50LnRvdGFsVG9rZW5zICs9IGNodW5rLnRva2VuQ291bnQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChjdXJyZW50LnRleHRzLmxlbmd0aCA+IDApIHtcbiAgICAgICAgbWVyZ2VkLnB1c2goeyB0ZXh0czogY3VycmVudC50ZXh0cywgbWV0YWRhdGE6IGN1cnJlbnQubWV0YWRhdGEgfSk7XG4gICAgICB9XG4gICAgICBjdXJyZW50ID0geyB0ZXh0czogW2NodW5rLnRleHRdLCBtZXRhZGF0YTogW2NodW5rLm1ldGFkYXRhXSwgdG90YWxUb2tlbnM6IGNodW5rLnRva2VuQ291bnQgfTtcbiAgICB9XG4gIH1cblxuICBpZiAoY3VycmVudC50ZXh0cy5sZW5ndGggPiAwKSB7XG4gICAgbWVyZ2VkLnB1c2goeyB0ZXh0czogY3VycmVudC50ZXh0cywgbWV0YWRhdGE6IGN1cnJlbnQubWV0YWRhdGEgfSk7XG4gIH1cblxuICByZXR1cm4gbWVyZ2VkO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IGdldEdsb2JhbENvbGxlY3Rpb24sIGdldFNlc3Npb25Db2xsZWN0aW9uLCBjcmVhdGVTZXNzaW9uQ29sbGVjdGlvbiwgbGlzdERvY3VtZW50cyB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTID0gNjA7XG5jb25zdCBzZXNzaW9ucyA9IG5ldyBNYXAoKTtcbmNvbnN0IE1BWF9QREZTX1BFUl9TRVNTSU9OID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04pIHx8IDM7XG5jb25zdCBNQVhfVVBMT0FEX1NJWkVfTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIpIHx8IDU7XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVTZXNzaW9uKCkge1xuICBjb25zdCBzZXNzaW9uSWQgPSB1dWlkdjQoKTtcbiAgY29uc3Qgc2Vzc2lvbiA9IHtcbiAgICBpZDogc2Vzc2lvbklkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcblxuICBzZXNzaW9ucy5zZXQoc2Vzc2lvbklkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG5cbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHtcbiAgICAgIHJldHVybiBleGlzdGluZztcbiAgICB9XG4gIH1cblxuICByZXR1cm4gY3JlYXRlU2Vzc2lvbigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSB7XG4gIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gIGNvbnN0IGxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKHNlc3Npb24ubGFzdEFjY2Vzc2VkKS5nZXRUaW1lKCk7XG4gIGNvbnN0IHRpbWVvdXRNcyA9IHNlc3Npb24udGltZW91dE1pbnV0ZXMgKiA2MCAqIDEwMDA7XG4gIHJldHVybiAobm93IC0gbGFzdEFjY2Vzc2VkKSA+IHRpbWVvdXRNcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIHNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgaWQ6IGRvY3VtZW50SW5mby5pZCxcbiAgICBmaWxlbmFtZTogZG9jdW1lbnRJbmZvLmZpbGVuYW1lLFxuICAgIGZpbGVTaXplOiBkb2N1bWVudEluZm8uZmlsZVNpemUsXG4gICAgcGFnZUNvdW50OiBkb2N1bWVudEluZm8ucGFnZUNvdW50LFxuICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICBjaHVua0NvdW50OiBkb2N1bWVudEluZm8uY2h1bmtDb3VudCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnXG4gIH0pO1xuXG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogJ1Nlc3Npb24gbm90IGZvdW5kJyB9O1xuICB9XG5cbiAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuXG4gIHJldHVybiB7IGNhblVwbG9hZDogdHJ1ZSB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVVcGxvYWQoc2Vzc2lvbklkLCBmaWxlLCBmaWxlbmFtZSkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBjb25zdCBlcnJvcnMgPSBbXTtcblxuICAvLyBDaGVjayBmaWxlIHNpemVcbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgLy8gQ2hlY2sgbWF4IFBERnNcbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgLy8gQ2hlY2sgZm9yIGR1cGxpY2F0ZSBmaWxlbmFtZVxuICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gZmlsZW5hbWUpKSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmApO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpc1ZhbGlkOiBlcnJvcnMubGVuZ3RoID09PSAwLFxuICAgIGVycm9ycyxcbiAgICBpc0xhcmdlRmlsZTogZmlsZS5zaXplID4gKE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0ICogMC42KSAvLyA2MCUgb2YgbWF4XG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgcmV0dXJuIGZhbHNlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbkRvY3MgPSBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gIGNvbnN0IGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIGNvbnN0IGdsb2JhbERvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKGdsb2JhbENvbGxlY3Rpb24pO1xuXG4gIHJldHVybiB7XG4gICAgc2Vzc2lvbkRvY3VtZW50czogc2Vzc2lvbkRvY3MsXG4gICAgZ2xvYmFsRG9jdW1lbnRzOiBnbG9iYWxEb2NzXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpZDogc2Vzc2lvbi5pZCxcbiAgICBkb2N1bWVudENvdW50OiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBzZXNzaW9uLmNyZWF0ZWRBdCxcbiAgICBsYXN0QWNjZXNzZWQ6IHNlc3Npb24ubGFzdEFjY2Vzc2VkLFxuICAgIHRvdGFsU2l6ZTogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmZpbGVTaXplIHx8IDApLCAwKSxcbiAgICB0b3RhbENodW5rczogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsaXN0QWN0aXZlU2Vzc2lvbnMoKSB7XG4gIHJldHVybiBBcnJheS5mcm9tKHNlc3Npb25zLnZhbHVlcygpKS5maWx0ZXIocyA9PiAhaXNTZXNzaW9uRXhwaXJlZChzKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhbnVwRXhwaXJlZFNlc3Npb25zKCkge1xuICBsZXQgY2xlYW5lZCA9IDA7XG4gIGZvciAoY29uc3QgW2lkLCBzZXNzaW9uXSBvZiBzZXNzaW9ucykge1xuICAgIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgICBzZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IGZzIGZyb20gJ2ZzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHBkZiBmcm9tICdwZGYtcGFyc2UnO1xuaW1wb3J0IHsgc2FuaXRpemVGaWxlbmFtZSwgdmFsaWRhdGVQREZGaWxlLCB2YWxpZGF0ZUZpbGVTaXplIH0gZnJvbSAnLi4vdXRpbHMvc2FuaXRpemUuanMnO1xuaW1wb3J0IHtcbiAgQ29ycnVwdGVkUERGRXJyb3IsXG4gIEludmFsaWRGaWxlVHlwZUVycm9yLFxuICBGaWxlVG9vTGFyZ2VFcnJvcixcbiAgVG9vTWFueVBERnNFcnJvcixcbiAgRHVwbGljYXRlRmlsZUVycm9yXG59IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5pbXBvcnQgeyBnZXRHbG9iYWxDb2xsZWN0aW9uLCBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgYWRkVmVjdG9ycywgZGVsZXRlRG9jdW1lbnRWZWN0b3JzLCBsaXN0RG9jdW1lbnRzIH0gZnJvbSAnLi4vc2VydmljZXMvY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBjaHVua1BERkNvbnRlbnQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbmltcG9ydCB7IGdlbmVyYXRlRW1iZWRkaW5ncyB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBjYW5BY2NlcHRVcGxvYWQsIGFkZERvY3VtZW50VG9TZXNzaW9uLCByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uLCBnZXRBbGxEb2N1bWVudHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG5jb25zdCBzdG9yYWdlID0gbXVsdGVyLmRpc2tTdG9yYWdlKHtcbiAgZGVzdGluYXRpb246IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgY2IobnVsbCwgdXBsb2FkRGlyKTtcbiAgfSxcbiAgZmlsZW5hbWU6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgY29uc3QgaWQgPSB1dWlkdjQoKTtcbiAgICBjb25zdCBleHQgPSBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuICAgIGNiKG51bGwsIGAke2lkfSR7ZXh0fWApO1xuICB9XG59KTtcblxuY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgc3RvcmFnZSxcbiAgbGltaXRzOiB7XG4gICAgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjRcbiAgfSxcbiAgZmlsZUZpbHRlcjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBpZiAoZmlsZS5taW1ldHlwZSA9PT0gJ2FwcGxpY2F0aW9uL3BkZicgJiYgcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKS50b0xvd2VyQ2FzZSgpID09PSAnLnBkZicpIHtcbiAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjYihuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKSk7XG4gICAgfVxuICB9XG59KTtcblxuYXN5bmMgZnVuY3Rpb24gcGFyc2VQREYoZmlsZVBhdGgpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBidWZmZXIgPSBmcy5yZWFkRmlsZVN5bmMoZmlsZVBhdGgpO1xuICAgIGNvbnN0IGRhdGEgPSBhd2FpdCBwZGYoYnVmZmVyKTtcbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogZGF0YS50ZXh0LFxuICAgICAgcGFnZUNvdW50OiBkYXRhLm51bXBhZ2VzLFxuICAgICAgaW5mbzogZGF0YS5pbmZvXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdQREYgcGFyc2luZyBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgbmV3IENvcnJ1cHRlZFBERkVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICB0cnkge1xuICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICBpZiAoIWZpbGUpIHtcbiAgICAgIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuICAgIH1cblxuICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IG1heFVwbG9hZHNNQiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpO1xuICAgIGNvbnN0IG1heFBERnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTiB8fCAnMycpO1xuXG4gICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA+PSBtYXhQREZzKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICB0aHJvdyBuZXcgVG9vTWFueVBERnNFcnJvcihtYXhQREZzKTtcbiAgICB9XG5cbiAgICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGNsZWFuRmlsZW5hbWUpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICB0aHJvdyBuZXcgRHVwbGljYXRlRmlsZUVycm9yKGNsZWFuRmlsZW5hbWUpO1xuICAgIH1cblxuICAgIGNvbnN0IHBkZkRhdGEgPSBhd2FpdCBwYXJzZVBERihmaWxlLnBhdGgpO1xuICAgIGNvbnN0IGRvY3VtZW50SWQgPSBwYXRoLnBhcnNlKGZpbGUuZmlsZW5hbWUpLm5hbWU7XG4gICAgY29uc3QgZG9jdW1lbnRQYXRoID0gZmlsZS5wYXRoO1xuXG4gICAgY29uc3QgY2h1bmtzID0gY2h1bmtQREZDb250ZW50KHtcbiAgICAgIHRleHQ6IHBkZkRhdGEudGV4dCxcbiAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgZG9jdW1lbnRJZCxcbiAgICAgIHBhZ2VOdW1iZXI6IDFcbiAgICB9KTtcblxuICAgIGlmIChjaHVua3MubGVuZ3RoID09PSAwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MjIpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ05vIGNvbnRlbnQgY291bGQgYmUgZXh0cmFjdGVkIGZyb20gUERGJyxcbiAgICAgICAgY29kZTogJ0VNUFRZX1BERidcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IFtdO1xuICAgIGNvbnN0IHByb2dyZXNzQ2FsbGJhY2sgPSAocHJvY2Vzc2VkLCB0b3RhbCkgPT4ge1xuICAgICAgaWYgKHJlcS5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzKSB7XG4gICAgICAgIHJlcS5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzLmVtaXQoYHByb2dyZXNzXyR7c2Vzc2lvbklkfWAsIHtcbiAgICAgICAgICBkb2N1bWVudElkLFxuICAgICAgICAgIHByb2Nlc3NlZCxcbiAgICAgICAgICB0b3RhbCxcbiAgICAgICAgICBzdGFnZTogJ2VtYmVkZGluZydcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfTtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSsrKSB7XG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCBlbWJlZGRpbmcgPSBhd2FpdCBnZW5lcmF0ZUVtYmVkZGluZ3MoW2NodW5rc1tpXV0pO1xuICAgICAgICBpZiAoZW1iZWRkaW5nICYmIGVtYmVkZGluZy5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgZW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgICAgICAgIGVtYmVkZGluZzogZW1iZWRkaW5nWzBdLmVtYmVkZGluZyxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rc1tpXS50ZXh0LFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rc1tpXS5tZXRhZGF0YVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZW1iZWQgY2h1bmsgJHtpfTpgLCBlcnJvcik7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGVtYmVkZGluZ3MubGVuZ3RoID09PSAwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg1MDMpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJyxcbiAgICAgICAgY29kZTogJ0VNQkVERElOR19GQUlMRUQnXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgY29sbGVjdGlvbixcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICApO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCxcbiAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgIHBhZ2VDb3VudDogcGRmRGF0YS5wYWdlQ291bnQsXG4gICAgICBjaHVua0NvdW50OiBlbWJlZGRpbmdzLmxlbmd0aFxuICAgIH0pO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGRvY3VtZW50OiB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiBwZGZEYXRhLnBhZ2VDb3VudCxcbiAgICAgICAgY2h1bmtDb3VudDogZW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9LFxuICAgICAgc2Vzc2lvbklkXG4gICAgfSk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTtcbiAgICB9XG4gICAgY29uc29sZS5lcnJvcignVXBsb2FkIGVycm9yOicsIGVycm9yKTtcblxuICAgIGNvbnN0IHN0YXR1c0NvZGUgPSBlcnJvci5zdGF0dXNDb2RlIHx8IDUwMDtcbiAgICByZXMuc3RhdHVzKHN0YXR1c0NvZGUpLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdVUExPQURfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZG9jdW1lbnRzID0gYXdhaXQgZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oZG9jdW1lbnRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdMaXN0IGRvY3VtZW50cyBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICBsZXQgY29sbGVjdGlvbjtcbiAgICBsZXQgZGVsZXRlZEZyb21TZXNzaW9uID0gZmFsc2U7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICAgIGNvbnN0IGNvdW50ID0gYXdhaXQgZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpO1xuICAgICAgICBpZiAoY291bnQgPiAwKSB7XG4gICAgICAgICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpO1xuICAgICAgICAgIGRlbGV0ZWRGcm9tU2Vzc2lvbiA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBEZWxldGUgZnJvbSBnbG9iYWwgY29sbGVjdGlvblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgICAgYXdhaXQgZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIC8vIE5vdCBpbiBnbG9iYWxcbiAgICB9XG5cbiAgICByZXMuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZG9jdW1lbnRJZCxcbiAgICAgIGRlbGV0ZWRGcm9tOiBkZWxldGVkRnJvbVNlc3Npb24gPyAnc2Vzc2lvbicgOiAndW5rbm93bidcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdEZWxldGUgZG9jdW1lbnQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsXG4gICAgICBjb2RlOiAnREVMRVRFX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudEZpbGUocmVxLCByZXMpIHtcbiAgY29uc3QgeyBkb2N1bWVudElkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGNvbnN0IGZpbGVQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgYCR7ZG9jdW1lbnRJZH0ucGRmYCk7XG5cbiAgICBpZiAoIWZzLmV4aXN0c1N5bmMoZmlsZVBhdGgpKSB7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJyxcbiAgICAgICAgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJ1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgIGNvbnN0IGZpbGVuYW1lID0gcGF0aC5iYXNlbmFtZShmaWxlUGF0aCk7XG4gICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGBhdHRhY2htZW50OyBmaWxlbmFtZT1cIiR7ZmlsZW5hbWV9XCJgKTtcblxuICAgIGNvbnN0IHN0cmVhbSA9IGZzLmNyZWF0ZVJlYWRTdHJlYW0oZmlsZVBhdGgpO1xuICAgIHN0cmVhbS5waXBlKHJlcyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignR2V0IGRvY3VtZW50IGZpbGUgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JyxcbiAgICAgIGNvZGU6ICdSRVRSSUVWRV9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbnJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG5yb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG5yb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldEdsb2JhbENvbGxlY3Rpb24sIGdldFNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgNTtcbmNvbnN0IENPVkVSQUdFX0hJR0hfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5DT1ZFUkFHRV9ISUdIX1RIUkVTSE9MRCkgfHwgMC43NTtcbmNvbnN0IENPVkVSQUdFX01FRElVTV9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LkNPVkVSQUdFX01FRElVTV9USFJFU0hPTEQpIHx8IDAuNTU7XG5cbmZ1bmN0aW9uIGNvbXB1dGVTaW1pbGFyaXR5U2NvcmUoZGlzdGFuY2UpIHtcbiAgLy8gQ29udmVydCBkaXN0YW5jZSB0byBzaW1pbGFyaXR5IChhc3N1bWluZyBjb3NpbmUgZGlzdGFuY2UpXG4gIHJldHVybiAxIC0gZGlzdGFuY2U7XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4geyBsZXZlbDogJ2xvdycsIHNjb3JlOiAwLCByZWFzb246ICdObyByZXN1bHRzIGZvdW5kJyB9O1xuICB9XG5cbiAgY29uc3QgdG9wUmVzdWx0cyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSyk7XG4gIGNvbnN0IHNjb3JlcyA9IHRvcFJlc3VsdHMubWFwKHIgPT4gci5zY29yZSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcblxuICBsZXQgbGV2ZWw7XG4gIGxldCByZWFzb247XG5cbiAgaWYgKGF2Z1Njb3JlID49IENPVkVSQUdFX0hJR0hfVEhSRVNIT0xEKSB7XG4gICAgbGV2ZWwgPSAnaGlnaCc7XG4gICAgcmVhc29uID0gJ0hpZ2ggY29uZmlkZW5jZSBpbiByZXRyaWV2ZWQgY29udGV4dCc7XG4gIH0gZWxzZSBpZiAoYXZnU2NvcmUgPj0gQ09WRVJBR0VfTUVESVVNX1RIUkVTSE9MRCkge1xuICAgIGxldmVsID0gJ21lZGl1bSc7XG4gICAgcmVhc29uID0gJ01vZGVyYXRlIGNvbmZpZGVuY2UgaW4gcmV0cmlldmVkIGNvbnRleHQnO1xuICB9IGVsc2Uge1xuICAgIGxldmVsID0gJ2xvdyc7XG4gICAgcmVhc29uID0gJ0luc3VmZmljaWVudCByZWxldmFudCBpbmZvcm1hdGlvbiBmb3VuZCc7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGxldmVsLFxuICAgIHNjb3JlOiBhdmdTY29yZSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKSxcbiAgICBib3R0b21TY29yZTogTWF0aC5taW4oLi4uc2NvcmVzKSxcbiAgICByZWFzb25cbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG4gIGNvbnN0IGluY2x1ZGVHbG9iYWwgPSBvcHRpb25zLmluY2x1ZGVHbG9iYWwgIT09IGZhbHNlO1xuXG4gIHRyeSB7XG4gICAgLy8gRW1iZWQgdGhlIHF1ZXJ5XG4gICAgY29uc3QgcXVlcnlFbWJlZGRpbmcgPSBhd2FpdCBlbWJlZFF1ZXJ5KHF1ZXJ5KTtcblxuICAgIC8vIFF1ZXJ5IGJvdGggY29sbGVjdGlvbnMgaW4gcGFyYWxsZWxcbiAgICBjb25zdCBxdWVyaWVzID0gW107XG5cbiAgICAvLyBHbG9iYWwgY29sbGVjdGlvblxuICAgIGlmIChpbmNsdWRlR2xvYmFsKSB7XG4gICAgICBjb25zdCBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgICAgcXVlcmllcy5wdXNoKHtcbiAgICAgICAgdHlwZTogJ2dsb2JhbCcsXG4gICAgICAgIHByb21pc2U6IHF1ZXJ5Q29sbGVjdGlvbihnbG9iYWxDb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIC8vIFNlc3Npb24gY29sbGVjdGlvblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHNlc3Npb25Db2xsZWN0aW9uID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICAgICAgaWYgKHNlc3Npb25Db2xsZWN0aW9uKSB7XG4gICAgICAgICAgcXVlcmllcy5wdXNoKHtcbiAgICAgICAgICAgIHR5cGU6ICdzZXNzaW9uJyxcbiAgICAgICAgICAgIHByb21pc2U6IHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgLy8gU2Vzc2lvbiBjb2xsZWN0aW9uIGRvZXNuJ3QgZXhpc3QgeWV0LCB0aGF0J3Mgb2tcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBXYWl0IGZvciBhbGwgcXVlcmllc1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbChxdWVyaWVzLm1hcChhc3luYyBxID0+ICh7XG4gICAgICB0eXBlOiBxLnR5cGUsXG4gICAgICByZXN1bHRzOiBhd2FpdCBxLnByb21pc2VcbiAgICB9KSkpO1xuXG4gICAgLy8gTWVyZ2UgYW5kIHJlLXJhbmsgcmVzdWx0c1xuICAgIGNvbnN0IGFsbFJlc3VsdHMgPSBbXTtcblxuICAgIGZvciAoY29uc3QgeyB0eXBlLCByZXN1bHRzOiB0eXBlUmVzdWx0cyB9IG9mIHJlc3VsdHMpIHtcbiAgICAgIGZvciAoY29uc3QgcmVzdWx0IG9mIHR5cGVSZXN1bHRzKSB7XG4gICAgICAgIGFsbFJlc3VsdHMucHVzaCh7XG4gICAgICAgICAgLi4ucmVzdWx0LFxuICAgICAgICAgIHNvdXJjZV90eXBlOiB0eXBlIC8vICdnbG9iYWwnIG9yICdzZXNzaW9uJ1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBTb3J0IGJ5IHNjb3JlIGRlc2NlbmRpbmdcbiAgICBhbGxSZXN1bHRzLnNvcnQoKGEsIGIpID0+IGIuc2NvcmUgLSBhLnNjb3JlKTtcblxuICAgIC8vIFRha2UgdG9wIEsgYWZ0ZXIgbWVyZ2luZ1xuICAgIGNvbnN0IHRvcFJlc3VsdHMgPSBhbGxSZXN1bHRzLnNsaWNlKDAsIHRvcEspO1xuXG4gICAgLy8gQ2FsY3VsYXRlIGNvdmVyYWdlXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZSh0b3BSZXN1bHRzLCB0b3BLKTtcblxuICAgIHJldHVybiB7XG4gICAgICByZXN1bHRzOiB0b3BSZXN1bHRzLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUmV0cmlldmFsIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiAnJztcbiAgfVxuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IG1heFRva2Vuc1BlckNoYXIgPSA0O1xuICBjb25zdCBjb250ZXh0UGFydHMgPSBbXTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IHJlc3VsdHMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCByZXN1bHQgPSByZXN1bHRzW2ldO1xuICAgIGNvbnN0IHRva2VuRXN0aW1hdGUgPSByZXN1bHQudGV4dC5sZW5ndGggLyBtYXhUb2tlbnNQZXJDaGFyO1xuXG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2Vucykge1xuICAgICAgYnJlYWs7XG4gICAgfVxuXG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcblxuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnZ2xvYmFsJyA/ICdbU2VlZCBEb2N1bWVudF0nIDogJ1tTZXNzaW9uIFVwbG9hZF0nO1xuICAgIGNvbnN0IGNpdGF0aW9uID0gYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfWA7XG4gICAgY29uc3QgcGFnZSA9IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlciA/IGAgKFBhZ2UgJHtyZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXJ9KWAgOiAnJztcblxuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGAke2NpdGF0aW9ufSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIHJldHVybiByZXN1bHRzLm1hcCgocmVzdWx0LCBpZHgpID0+ICh7XG4gICAgaWQ6IHV1aWR2NCgpLFxuICAgIGluZGV4OiBpZHggKyAxLFxuICAgIGRvY3VtZW50SWQ6IHJlc3VsdC5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICBmaWxlbmFtZTogcmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgIHBhZ2VOdW1iZXI6IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICBzZWN0aW9uOiByZXN1bHQubWV0YWRhdGEuc2VjdGlvbl90aXRsZSxcbiAgICBleGNlcnB0OiByZXN1bHQudGV4dC5zbGljZSgwLCAyMDApICsgKHJlc3VsdC50ZXh0Lmxlbmd0aCA+IDIwMCA/ICcuLi4nIDogJycpLFxuICAgIHNjb3JlOiByZXN1bHQuc2NvcmUsXG4gICAgc291cmNlVHlwZTogcmVzdWx0LnNvdXJjZV90eXBlLFxuICAgIGNodW5rSWQ6IHJlc3VsdC5pZFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzaG91bGRTaG93UmVmdXNhbChjb3ZlcmFnZSkge1xuICByZXR1cm4gY292ZXJhZ2UubGV2ZWwgPT09ICdsb3cnO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICAvLyBLZWVwIG9ubHkgdGhlIGxhc3QgTiB0dXJuc1xuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgcmV0dXJuIG1lbW9yeS50dXJucy5zbGljZSgtbGltaXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q29udmVyc2F0aW9uQ29udGV4dChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICByZXR1cm4gdHVybnMubWFwKHQgPT4gKHtcbiAgICByb2xlOiB0LnJvbGUsXG4gICAgY29udGVudDogdC5jb250ZW50XG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICBpZiAodHVybnMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG5cbiAgY29uc3QgZm9ybWF0dGVkID0gdHVybnMubWFwKHQgPT4ge1xuICAgIGNvbnN0IHByZWZpeCA9IHQucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXI6JyA6ICdBc3Npc3RhbnQ6JztcbiAgICByZXR1cm4gYCR7cHJlZml4fSAke3QuY29udGVudH1gO1xuICB9KS5qb2luKCdcXG5cXG4nKTtcblxuICByZXR1cm4gZm9ybWF0dGVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwpIHtcbiAgcmV0dXJuIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCB7XG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtpbXBvcnQgeyBmb3JtYXRNZW1vcnlGb3JQcm9tcHQgfSBmcm9tICcuL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZm9ybWF0Q29udGV4dEZvclByb21wdCwgY2FsY3VsYXRlQ292ZXJhZ2UgfSBmcm9tICcuL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCBTWVNURU1fSU5TVFJVQ1RJT04gPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgYmFzZWQgc29sZWx5IG9uIHRoZSBwcm92aWRlZCBjb250ZXh0IGZyb20gaW5kZXhlZCBkb2N1bWVudHMuXG5cbkNSSVRJQ0FMIFJVTEVTOlxuMS4gT05MWSBhbnN3ZXIgYmFzZWQgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIE5ldmVyIGhhbGx1Y2luYXRlIG9yIGFkZCBleHRlcm5hbCBrbm93bGVkZ2UuXG4yLiBJZiB0aGUgY29udGV4dCBkb2Vzbid0IGNvbnRhaW4gZW5vdWdoIGluZm9ybWF0aW9uIHRvIGFuc3dlciBmdWxseSwgc3RhdGUgdGhhdCBjbGVhcmx5LlxuMy4gQWx3YXlzIGNpdGUgeW91ciBzb3VyY2VzIHVzaW5nIFsxXSwgWzJdLCBldGMuIGZvcm1hdCByZWZlcnJpbmcgdG8gdGhlIG51bWJlcmVkIGNvbnRleHQgY2h1bmtzLlxuNC4gQmUgY29uY2lzZSBidXQgdGhvcm91Z2guIFVzZSBidWxsZXQgcG9pbnRzIG9yIG51bWJlcmVkIGxpc3RzIGZvciBjb21wbGV4IGFuc3dlcnMuXG41LiBJZiBhc2tlZCBhYm91dCBzb21ldGhpbmcgdW5yZWxhdGVkIHRvIHRoZSBjb250ZXh0LCBwb2xpdGVseSByZWRpcmVjdCB0byByZWxldmFudCB0b3BpY3MuXG42LiBNYWludGFpbiBjb252ZXJzYXRpb24gY29udGludWl0eSBidXQgZG9uJ3QgcmVwZWF0IGluZm9ybWF0aW9uIHVubmVjZXNzYXJpbHkuXG43LiBGb3JtYXQgcmVzcG9uc2VzIGluIGNsZWFyLCByZWFkYWJsZSBtYXJrZG93bi5gO1xuXG5jb25zdCBSRUZVU0FMX01FU1NBR0UgPSAnSSBjb3VsZG5cXCd0IGZpbmQgZW5vdWdoIHN1cHBvcnRpbmcgaW5mb3JtYXRpb24gaW4gdGhlIGN1cnJlbnQga25vd2xlZGdlIGJhc2UuJztcblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkUHJvbXB0KHsgcXVlcnksIGNvbnRleHQsIG1lbW9yeUNvbnRleHQsIGNvdmVyYWdlIH0pIHtcbiAgY29uc3QgcGFydHMgPSBbXTtcblxuICAvLyBTeXN0ZW0gaW5zdHJ1Y3Rpb25cbiAgcGFydHMucHVzaChTWVNURU1fSU5TVFJVQ1RJT04pO1xuXG4gIC8vIFBhc3QgY29udmVyc2F0aW9uIGlmIGF2YWlsYWJsZVxuICBpZiAobWVtb3J5Q29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChtZW1vcnlDb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgfVxuXG4gIC8vIFJldHJpZXZlZCBjb250ZXh0XG4gIGlmIChjb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFJFTEVWQU5UIENPTlRFWFQgRlJPTSBLTk9XTEVER0UgQkFTRSAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKGNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgQ09OVEVYVCAtLS1cXG4nKTtcbiAgfVxuXG4gIC8vIEN1cnJlbnQgcXVlc3Rpb25cbiAgcGFydHMucHVzaCgnXFxuXFxuLS0tIENVUlJFTlQgUVVFU1RJT04gLS0tXFxuJyk7XG4gIHBhcnRzLnB1c2gocXVlcnkpO1xuICBwYXJ0cy5wdXNoKCdcXG5cXG5SZW1lbWJlcjogQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIFVzZSBbMV0sIFsyXSwgZXRjLiBmb3IgY2l0YXRpb25zLiBJZiB0aGUgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5LicpO1xuXG4gIHJldHVybiBwYXJ0cy5qb2luKCcnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkU3RyZWFtaW5nUHJvbXB0KHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpO1xuICBjb25zdCBjb250ZXh0U3RyaW5nID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXRyaWV2ZWRSZXN1bHRzKTtcblxuICByZXR1cm4gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRTdHJpbmcsXG4gICAgbWVtb3J5Q29udGV4dCxcbiAgICBjb3ZlcmFnZTogY2FsY3VsYXRlQ292ZXJhZ2UocmV0cmlldmVkUmVzdWx0cylcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsUmVzcG9uc2UoKSB7XG4gIHJldHVybiBSRUZVU0FMX01FU1NBR0U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTeXN0ZW1JbnN0cnVjdGlvbigpIHtcbiAgcmV0dXJuIFNZU1RFTV9JTlNUUlVDVElPTjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkV2ViU2VhcmNoUHJvbXB0KHF1ZXJ5LCBncm91bmRpbmdNZXRhZGF0YSkge1xuICByZXR1cm4gYEJhc2VkIG9uIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBmb2xsb3dpbmcgcXVlc3Rpb246ICR7cXVlcnl9XG5cbkd1aWRlbGluZXM6XG4tIFVzZSBpbmZvcm1hdGlvbiBmcm9tIHRoZSB3ZWIgc2VhcmNoXG4tIFByb3ZpZGUgc291cmNlcy9VUkxzIHdoZXJlIGFwcGxpY2FibGVcbi0gQmUgY29uY2lzZSBhbmQgaW5mb3JtYXRpdmVcbi0gSWYgbXVsdGlwbGUgc291cmNlcyBhZ3JlZSBvciBjb250cmFkaWN0LCBtZW50aW9uIHRoYXRgO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0R2VuZXJhdGlvbkNvbmZpZyhjdXN0b21Db25maWcgPSB7fSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgdG9wUDogMC45NSxcbiAgICB0b3BLOiA0MCxcbiAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDgsXG4gICAgLi4uY3VzdG9tQ29uZmlnXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0U291cmNlc0Zyb21SZXNwb25zZShyZXNwb25zZSkge1xuICAvLyBFeHRyYWN0IGNpdGF0aW9uIHBhdHRlcm5zIGxpa2UgWzFdLCBbMl0sIGV0Yy5cbiAgY29uc3QgY2l0YXRpb25QYXR0ZXJuID0gL1xcWyhcXGQrKVxcXS9nO1xuICBjb25zdCBjaXRhdGlvbnMgPSBuZXcgU2V0KCk7XG4gIGxldCBtYXRjaDtcblxuICB3aGlsZSAoKG1hdGNoID0gY2l0YXRpb25QYXR0ZXJuLmV4ZWMocmVzcG9uc2UpKSAhPT0gbnVsbCkge1xuICAgIGNpdGF0aW9ucy5hZGQocGFyc2VJbnQobWF0Y2hbMV0pKTtcbiAgfVxuXG4gIHJldHVybiBBcnJheS5mcm9tKGNpdGF0aW9ucykuc29ydCgoYSwgYikgPT4gYSAtIGIpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuZXJhdGl2ZUFJIH0gZnJvbSAnQGdvb2dsZS9nZW5lcmF0aXZlLWFpJztcbmltcG9ydCB7IGJ1aWxkUHJvbXB0LCBnZXRSZWZ1c2FsUmVzcG9uc2UgfSBmcm9tICcuL3Byb21wdFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgTExNVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmNvbnN0IGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSk7XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTIuMC1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi4wLWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxubGV0IHByaW1hcnlNb2RlbCA9IG51bGw7XG5sZXQgZmFsbGJhY2tNb2RlbCA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldFByaW1hcnlNb2RlbCgpIHtcbiAgaWYgKCFwcmltYXJ5TW9kZWwpIHtcbiAgICBwcmltYXJ5TW9kZWwgPSBnZW5BSS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogUFJJTUFSWV9NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gcHJpbWFyeU1vZGVsO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsKCkge1xuICBpZiAoIWZhbGxiYWNrTW9kZWwpIHtcbiAgICBmYWxsYmFja01vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IEZBTExCQUNLX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBmYWxsYmFja01vZGVsO1xufVxuXG5hc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVdpdGhNb2RlbChtb2RlbCwgcHJvbXB0LCBzaWduYWwpIHtcbiAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfVxuICB9LCB7IHNpZ25hbCB9KTtcblxuICByZXR1cm4gcmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlUmVzcG9uc2UocHJvbXB0KSB7XG4gIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG4gIGNvbnN0IHRpbWVvdXRJZCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBSRVFVRVNUX1RJTUVPVVQpO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZ2V0UHJpbWFyeU1vZGVsKCkuZ2VuZXJhdGVDb250ZW50KHtcbiAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG4gICAgcmV0dXJuIHJlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG4gIH0gY2F0Y2ggKHByaW1hcnlFcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1ByaW1hcnkgbW9kZWwgZmFpbGVkOicsIHByaW1hcnlFcnJvci5tZXNzYWdlKTtcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBmYWxsYmFja1Jlc3VsdCA9IGF3YWl0IGdldEZhbGxiYWNrTW9kZWwoKS5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG4gICAgICByZXR1cm4gZmFsbGJhY2tSZXN1bHQucmVzcG9uc2UudGV4dCgpO1xuICAgIH0gY2F0Y2ggKGZhbGxiYWNrRXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhbGxiYWNrIG1vZGVsIGFsc28gZmFpbGVkOicsIGZhbGxiYWNrRXJyb3IubWVzc2FnZSk7XG4gICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVJlc3BvbnNlKHByb21wdCkge1xuICBsZXQgbW9kZWwgPSBnZXRQcmltYXJ5TW9kZWwoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50U3RyZWFtKHtcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGxldCBmaXJzdFRva2VuVGltZW91dCA9IHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgICAvLyBJZiBubyBmaXJzdCB0b2tlbiBpbiB0aW1lLCB3ZSdsbCByZXRyeVxuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ0ZpcnN0IHRva2VuIHRpbWVvdXQnKTtcbiAgICAgIH0sIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgICBpZiAoZmlyc3RUb2tlbikge1xuICAgICAgICAgICAgZmlyc3RUb2tlbiA9IGZhbHNlO1xuICAgICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHsgc3VjY2VzczogdHJ1ZSB9O1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICByZXRyaWVzKys7XG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgLy8gU3dpdGNoIHRvIGZhbGxiYWNrIG1vZGVsXG4gICAgICBtb2RlbCA9IGdldEZhbGxiYWNrTW9kZWwoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1DaGF0UmVzcG9uc2UocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICAvLyBCdWlsZCB0aGUgcHJvbXB0XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBtZW1vcnlTZXJ2aWNlID8gbWVtb3J5U2VydmljZS5mb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSA6ICcnO1xuICBjb25zdCBjb250ZXh0TGlzdCA9IHJldHJpZXZlZFJlc3VsdHMgfHwgW107XG4gIGNvbnN0IGNvbnRleHRUZXh0ID0gY29udGV4dExpc3QubWFwKChyLCBpKSA9PlxuICAgIGBbJHtpICsgMX1dICR7ci5tZXRhZGF0YS5maWxlbmFtZSB8fCAnVW5rbm93bid9OiAke3IudGV4dH1gXG4gICkuam9pbignXFxuXFxuJyk7XG5cbiAgY29uc3QgcHJvbXB0ID0gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRUZXh0LFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IHsgbGV2ZWw6ICdoaWdoJyB9XG4gIH0pO1xuXG4gIC8vIFN0cmVhbSB0aGUgcmVzcG9uc2VcbiAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gIHRyeSB7XG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgeWllbGQgY2h1bms7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgeWllbGQgY2h1bms7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICB5aWVsZCB7IHR5cGU6ICdjb21wbGV0ZScsIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxUZXh0KCkge1xuICByZXR1cm4gZ2V0UmVmdXNhbFJlc3BvbnNlKCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVdlYlNlYXJjaFJlc3BvbnNlKHF1ZXJ5LCBncm91bmRpbmdDb250ZW50KSB7XG4gIGNvbnN0IG1vZGVsID0gZ2V0UHJpbWFyeU1vZGVsKCk7XG5cbiAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICBjb250ZW50czogW3tcbiAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgIHBhcnRzOiBbeyB0ZXh0OiBgQmFzZWQgb24gdGhlc2Ugd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIHF1ZXN0aW9uOiBcIiR7cXVlcnl9XCJcXG5cXG4ke2dyb3VuZGluZ0NvbnRlbnR9YCB9XVxuICAgIH1dLFxuICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfSxcbiAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICB9KTtcblxuICBjb25zdCByZXNwb25zZSA9IHJlc3VsdC5yZXNwb25zZTtcbiAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZS5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gIHJldHVybiB7XG4gICAgdGV4dCxcbiAgICBncm91bmRpbmdNZXRhZGF0YSxcbiAgICBncm91bmRpbmdDaHVua3M6IGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MgfHwgW11cbiAgfTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgc2hvdWxkU2hvd1JlZnVzYWwgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlLCBnZXRSZWZ1c2FsVGV4dCB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zLCBnZXRMYXN0VXNlck1lc3NhZ2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgPSB1dWlkdjQoKTtcblxuICAvLyBFbnN1cmUgc2Vzc2lvbiBleGlzdHNcbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgLy8gU3RvcmUgdXNlciBxdWVzdGlvbiBpbiBtZW1vcnlcbiAgY29uc3QgdXNlclR1cm4gPSBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICAvLyBTZXQgdXAgU1NFXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICB0cnkge1xuICAgIC8vIFNlbmQgaW5pdGlhbCBzdGF0dXNcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICAvLyBSZXRyaWV2ZSByZWxldmFudCBjb250ZXh0XG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICAvLyBTZW5kIHJldHJpZXZhbCByZXN1bHRzXG4gICAgc2VuZEV2ZW50KCdyZXRyaWV2YWwnLCB7XG4gICAgICByZXN1bHRzOiByZXN1bHRzLmxlbmd0aCxcbiAgICAgIGNvdmVyYWdlOiBjb3ZlcmFnZS5sZXZlbCxcbiAgICAgIGNvdmVyYWdlU2NvcmU6IGNvdmVyYWdlLnNjb3JlXG4gICAgfSk7XG5cbiAgICAvLyBDaGVjayBpZiB3ZSBzaG91bGQgcmVmdXNlXG4gICAgaWYgKHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSkge1xuICAgICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmVmdXNhbCcsIG1lc3NhZ2U6IGdldFJlZnVzYWxUZXh0KCkgfSk7XG5cbiAgICAgIC8vIEdlbmVyYXRlIGNpdGF0aW9ucyBhbnl3YXkgZm9yIHRoZSByZWZ1c2FsXG4gICAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcblxuICAgICAgYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCAnYXNzaXN0YW50JywgZ2V0UmVmdXNhbFRleHQoKSwgY2l0YXRpb25zLCBjb3ZlcmFnZSk7XG5cbiAgICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICAgIGFuc3dlcklkLFxuICAgICAgICByZXNwb25zZTogZ2V0UmVmdXNhbFRleHQoKSxcbiAgICAgICAgY2l0YXRpb25zLFxuICAgICAgICBjb3ZlcmFnZSxcbiAgICAgICAgYWN0aW9uOiAncmVmdXNhbCdcbiAgICAgIH0pO1xuXG4gICAgICByZXMuZW5kKCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gQnVpbGQgcHJvbXB0IHdpdGggY29udGV4dFxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IHJlc3VsdHMubWFwKChyLCBpKSA9PlxuICAgICAgYFske2kgKyAxfV0gJHtyLm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdTb3VyY2UnfTogJHtyLnRleHR9YFxuICAgICkuam9pbignXFxuXFxuJyk7XG5cbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCA1KVxuICAgICAgLm1hcCh0ID0+IGAke3Qucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXInIDogJ0Fzc2lzdGFudCd9OiAke3QuY29udGVudH1gKVxuICAgICAgLmpvaW4oJ1xcblxcbicpO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0fVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dH1cblxuQ1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1cblxuQW5zd2VyIGNvbmNpc2VseSB3aXRoIGNpdGF0aW9ucyBsaWtlIFsxXSwgWzJdIHJlZmVycmluZyB0byBjb250ZXh0IG51bWJlcnMuYDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIC8vIFN0cmVhbSByZXNwb25zZVxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIEdlbmVyYXRlIGNpdGF0aW9uc1xuICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuXG4gICAgLy8gU3RvcmUgYXNzaXN0YW50IHJlc3BvbnNlIGluIG1lbW9yeVxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ2Fzc2lzdGFudCcsIGZ1bGxSZXNwb25zZSwgY2l0YXRpb25zLCBjb3ZlcmFnZSk7XG5cbiAgICAvLyBTZW5kIGNvbXBsZXRpb25cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgYW5zd2VySWQsXG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBzb3VyY2VzOiByZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICAgIGZpbGVuYW1lOiByLm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgICBleGNlcnB0OiByLnRleHQuc2xpY2UoMCwgMjAwKSxcbiAgICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgICAgfSkpXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHtcbiAgICAgIG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ0NIQVRfRVJST1InXG4gICAgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIC8vIEdldCBmcm9tIG1lbW9yeVxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgMTApO1xuXG4gIGZvciAoY29uc3QgdHVybiBvZiByZWNlbnRUdXJucykge1xuICAgIGlmICh0dXJuLmlkID09PSBhbnN3ZXJJZCB8fCB0dXJuLmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgICAgcmV0dXJuIHJlcy5qc29uKHtcbiAgICAgICAgc291cmNlczogdHVybi5jaXRhdGlvbnMgfHwgW11cbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kIGZvciB0aGlzIGFuc3dlcicsXG4gICAgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJ1xuICB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuY29uc3QgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZKTtcblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMi4wLWZsYXNoLWxpdGUnO1xuXG5sZXQgbW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRNb2RlbCgpIHtcbiAgaWYgKCFtb2RlbCkge1xuICAgIG1vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIG1vZGVsO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcGVyZm9ybVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICAvLyBFeHRyYWN0IHNlYXJjaCBxdWVyaWVzIGFuZCBzb3VyY2VzXG4gICAgY29uc3Qgd2ViU2VhcmNoUXVlcmllcyA9IFtdO1xuICAgIGNvbnN0IHdlYlNvdXJjZXMgPSBbXTtcblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGNodW5rIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoY2h1bmsud2ViKSB7XG4gICAgICAgICAgd2ViU291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogY2h1bmsud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBjaHVuay53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8ud2ViU2VhcmNoUXVlcmllcykge1xuICAgICAgd2ViU2VhcmNoUXVlcmllcy5wdXNoKC4uLmdyb3VuZGluZ01ldGFkYXRhLndlYlNlYXJjaFF1ZXJpZXMpO1xuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICB0ZXh0LFxuICAgICAgc291cmNlczogd2ViU291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHdlYlNlYXJjaFF1ZXJpZXMsXG4gICAgICByYXdNZXRhZGF0YTogZ3JvdW5kaW5nTWV0YWRhdGFcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSB0ZXh0O1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlPy5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgY29uc3Qgc291cmNlcyA9IFtdO1xuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChpdGVtLndlYikge1xuICAgICAgICAgIHNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGl0ZW0ud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBpdGVtLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQge1xuICAgICAgdHlwZTogJ2NvbXBsZXRlJyxcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbWluZyBlcnJvcjonLCBlcnJvcik7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdFdlYlNlYXJjaFJlc3BvbnNlKHJlc3VsdCkge1xuICByZXR1cm4ge1xuICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMubWFwKHMgPT4gKHtcbiAgICAgIHVyaTogcy51cmksXG4gICAgICB0aXRsZTogcy50aXRsZSxcbiAgICAgIHR5cGU6ICd3ZWInXG4gICAgfSkpLFxuICAgIHF1ZXJpZXNVc2VkOiByZXN1bHQucXVlcmllcyxcbiAgICBtZXRhZGF0YToge1xuICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9XG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBwZXJmb3JtV2ViU2VhcmNoLCBzdHJlYW1XZWJTZWFyY2ggfSBmcm9tICcuLi9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2gocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgcGVyZm9ybVdlYlNlYXJjaChxdWVyeS50cmltKCkpO1xuXG4gICAgcmVzLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHJlc3VsdC5xdWVyaWVzLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgICAgfVxuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDMpLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggdW5hdmFpbGFibGUnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoU3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICAvLyBTZXQgdXAgU1NFXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnc2VhcmNoaW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyB0aGUgd2ViLi4uJyB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcbiAgICBsZXQgc291cmNlcyA9IFtdO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1XZWJTZWFyY2gocXVlcnkudHJpbSgpKSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdXRUJfU0VBUkNIX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgICAgc291cmNlcyA9IGNodW5rLnNvdXJjZXMgfHwgW107XG4gICAgICB9XG4gICAgfVxuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywge1xuICAgICAgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCBmYWlsZWQnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVXZWJTZWFyY2gpO1xucm91dGVyLnBvc3QoJy9zdHJlYW0nLCBoYW5kbGVXZWJTZWFyY2hTdHJlYW0pO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHNlYXJjaFJvdXRlciBmcm9tICcuL2FwaS9zZWFyY2guanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcblxuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBST1VURVJTXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zb2xlLmxvZygnTW91bnRpbmcgcm91dGVycy4uLicpO1xuXG5hcHAudXNlKCcvaGVhbHRoJywgaGVhbHRoUm91dGVyKTtcbmFwcC51c2UoJy9kb2N1bWVudHMnLCBkb2N1bWVudHNSb3V0ZXIpO1xuYXBwLnVzZSgnL2NoYXQnLCBjaGF0Um91dGVyKTtcbmFwcC51c2UoJy9mZWVkYmFjaycsIGZlZWRiYWNrUm91dGVyKTtcbmFwcC51c2UoJy9zZWFyY2gnLCBzZWFyY2hSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuXG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxtQkFBbUI7QUFDelMsU0FBUyxNQUFNLGNBQWM7QUFzQjdCLFNBQVMsWUFBWTtBQUNuQixNQUFJLENBQUMsUUFBUTtBQUNYLFFBQUksQ0FBQyxhQUFhLFFBQVE7QUFDeEIsWUFBTSxJQUFJO0FBQUEsUUFDUjtBQUFBLE1BRUY7QUFBQSxJQUNGO0FBR0EsVUFBTSxnQkFBZ0I7QUFBQSxNQUNwQixRQUFRLGFBQWE7QUFBQSxNQUNyQixRQUFRLGFBQWE7QUFBQSxNQUNyQixVQUFVLGFBQWE7QUFBQSxJQUN6QjtBQUdBLFFBQUksYUFBYSxNQUFNO0FBQ3JCLG9CQUFjLE9BQU8sYUFBYTtBQUFBLElBQ3BDO0FBRUEsYUFBUyxJQUFJ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LFlBQ2QsS0FBSztBQUFHLHFCQUFPLENBQUMsR0FBYSx1REFBeUI7QUFBQSxZQUN0RCxLQUFLO0FBQ0QsMkJBQWMsR0FBRyxLQUFLLEVBQUc7QUFDekIsY0FBQUEsT0FBTTtBQUNOLHFCQUFPLFlBQVksSUFBSSxRQUFRLFNBQVUsS0FBSyxLQUFLLE1BQU07QUFDckQsZ0JBQUFBLEtBQUksS0FBSyxLQUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0QsTUFBSyxRQUFRLFdBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJjbGllbnQiLCAiZXN0aW1hdGVUb2tlbnMiLCAiY2h1bmtUZXh0IiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgIlJvdXRlciIsICJwYXRoIiwgInV1aWR2NCIsICJjb2xsZWN0aW9uIiwgInJvdXRlciIsICJ1dWlkdjQiLCAiZ2xvYmFsQ29sbGVjdGlvbiIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAiZ2VuQUkiLCAibW9kZWwiLCAiUm91dGVyIiwgInV1aWR2NCIsICJjaXRhdGlvbnMiLCAicm91dGVyIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgIkdvb2dsZUdlbmVyYXRpdmVBSSIsICJnZW5BSSIsICJQUklNQVJZX01PREVMIiwgIm1vZGVsIiwgIlJvdXRlciIsICJyb3V0ZXIiLCAicGF0aCIsICJwYXRoIiwgImFwcCJdCn0K
