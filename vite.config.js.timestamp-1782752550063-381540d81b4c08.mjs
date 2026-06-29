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
      model: process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-001"
    });
  }
  return embeddingModel;
}
async function embedBatch(texts, taskType = "RETRIEVAL_DOCUMENT", attempt = 1) {
  const maxAttempts = 5;
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-001";
  try {
    const model2 = getEmbeddingModel();
    const result = await model2.batchEmbedContents({
      requests: texts.map((text) => ({
        model: `models/${modelName}`,
        content: { parts: [{ text }] },
        taskType,
        // Fix 13
        outputDimensionality: OUTPUT_DIMENSIONS()
        // Fix 9
      }))
    });
    if (!result?.embeddings || result.embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${result?.embeddings?.length ?? 0}`);
    }
    return result.embeddings.map((e) => {
      if (!e?.values) throw new EmbeddingError("Missing values in embedding response");
      return e.values;
    });
  } catch (error) {
    const is429 = is429Error(error) || error?.status === 429 || error?.message?.includes("RESOURCE_EXHAUSTED");
    if (is429 && attempt < maxAttempts) {
      const retryDelay = error.retryAfter || GROUP_WAIT_MS;
      console.log(`Rate limited, waiting ${retryDelay / 1e3}s (attempt ${attempt}/${maxAttempts})`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      return embedBatch(texts, taskType, attempt + 1);
    }
    throw new EmbeddingError(error.message || "Batch embedding failed");
  }
}
async function generateEmbeddings(chunks, taskType = "RETRIEVAL_DOCUMENT", onProgress) {
  if (!chunks || chunks.length === 0) return [];
  const batchSize = BATCH_SIZE();
  const parallelCalls = PARALLEL_CALLS();
  const embeddings = [];
  const batches = [];
  for (let i = 0; i < chunks.length; i += batchSize) {
    batches.push(chunks.slice(i, i + batchSize));
  }
  const totalGroups = Math.ceil(batches.length / parallelCalls);
  for (let i = 0; i < batches.length; i += parallelCalls) {
    const parallelBatches = batches.slice(i, i + parallelCalls);
    const groupNum = Math.floor(i / parallelCalls) + 1;
    const chunksCovered = Math.min((i + parallelCalls) * batchSize, chunks.length);
    console.log(`  Embedding group ${groupNum}/${totalGroups} \u2014 ${parallelBatches.length} batch call(s) in parallel (chunks ${i * batchSize + 1}\u2013${chunksCovered})...`);
    const results = await Promise.allSettled(
      parallelBatches.map((batch) => embedBatch(batch.map((c) => c.text), taskType))
    );
    const failedBatches = [];
    results.forEach((result, batchIdx) => {
      const batch = parallelBatches[batchIdx];
      if (result.status === "fulfilled") {
        const vectors = result.value;
        batch.forEach((chunk, chunkIdx) => {
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_${i * batchSize + batchIdx * batchSize + chunkIdx}`,
            embedding: vectors[chunkIdx],
            metadata: chunk.metadata,
            text: chunk.text
          });
        });
      } else {
        console.warn(`  Batch ${i + batchIdx} failed, will retry individually:`, result.reason?.message);
        failedBatches.push({ batch, batchIdx: i + batchIdx });
      }
    });
    if (onProgress) {
      onProgress({ current_batch: groupNum, total_batches: totalGroups });
    }
    const isLastGroup = i + parallelCalls >= batches.length;
    if (!isLastGroup || failedBatches.length > 0) {
      console.log(`  Waiting ${GROUP_WAIT_MS / 1e3}s before next group...`);
      await new Promise((resolve) => setTimeout(resolve, GROUP_WAIT_MS));
    }
    for (const { batch, batchIdx } of failedBatches) {
      for (const chunk of batch) {
        try {
          const vectors = await embedBatch([chunk.text], taskType);
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_retry_${batchIdx}`,
            embedding: vectors[0],
            metadata: chunk.metadata,
            text: chunk.text
          });
          console.log(`  \u2705 Retry succeeded for chunk ${chunk.metadata?.chunk_id}`);
        } catch (err) {
          console.error(`  \u274C Retry failed for chunk ${chunk.metadata?.chunk_id}:`, err.message);
        }
      }
    }
  }
  return embeddings;
}
async function embedQuery(query) {
  const vectors = await embedBatch([query], "RETRIEVAL_QUERY");
  return vectors[0];
}
function getRateLimitState() {
  return {
    maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 3e4,
    parallelCalls: PARALLEL_CALLS(),
    maxChunksPerCall: BATCH_SIZE(),
    outputDimensions: OUTPUT_DIMENSIONS()
    // Fix 9: expose for health check
  };
}
var genAI, embeddingModel, BATCH_SIZE, PARALLEL_CALLS, OUTPUT_DIMENSIONS, GROUP_WAIT_MS;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    genAI = null;
    embeddingModel = null;
    BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
    PARALLEL_CALLS = () => parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
    OUTPUT_DIMENSIONS = () => parseInt(process.env.GEMINI_EMBEDDING_DIMENSIONS) || 3072;
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
function cleanText(text) {
  if (!text || typeof text !== "string") return "";
  return text.replace(/\f/g, "\n").replace(/(\s*\n){3,}/g, "\n\n").replace(/^\s*\d+\s*$/gm, "").replace(/[ \t]{2,}/g, " ").trim();
}
function chunkText(text, options = {}) {
  const chunkSizeTokens = options.chunkSizeTokens || DEFAULT_CHUNK_SIZE_TOKENS;
  const overlapTokens = options.overlapTokens || DEFAULT_OVERLAP_TOKENS;
  if (!text || typeof text !== "string") return [];
  const chunkSizeChars = chunkSizeTokens * CHARS_PER_TOKEN;
  const overlapChars = overlapTokens * CHARS_PER_TOKEN;
  const chunks = [];
  let start = 0;
  let chunkIndex = 0;
  while (start < text.length) {
    let end = start + chunkSizeChars;
    if (end < text.length) {
      const breakPoints = [". ", ".\n", "! ", "? ", "\n\n", "\n", " "];
      const searchStart = end - Math.floor(chunkSizeChars * 0.2);
      for (const breakpoint of breakPoints) {
        const idx = text.lastIndexOf(breakpoint, end);
        if (idx > searchStart && idx > start) {
          end = idx + breakpoint.length;
          break;
        }
      }
    }
    end = Math.min(end, text.length);
    const chunkContent = text.slice(start, end).trim();
    if (chunkContent.length >= MIN_CHUNK_CHARS) {
      chunks.push({
        text: chunkContent,
        tokenCount: estimateTokens(chunkContent),
        charStart: start,
        charEnd: end,
        chunkIndex: chunkIndex++
      });
    }
    const nextStart = end - overlapChars;
    start = nextStart > start ? nextStart : end;
    if (chunkIndex > 1e4) {
      console.warn("Chunk limit reached, stopping");
      break;
    }
  }
  return chunks;
}
var CHARS_PER_TOKEN, DEFAULT_CHUNK_SIZE_TOKENS, DEFAULT_OVERLAP_TOKENS, MIN_CHUNK_CHARS;
var init_chunker = __esm({
  "server/utils/chunker.js"() {
    "use strict";
    CHARS_PER_TOKEN = 4;
    DEFAULT_CHUNK_SIZE_TOKENS = 1e3;
    DEFAULT_OVERLAP_TOKENS = 200;
    MIN_CHUNK_CHARS = 100;
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
import { createHash } from "crypto";
import pdf from "file:///home/project/node_modules/pdf-parse/index.js";
async function parsePDFWithBoundaryMap(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);
    const pages = [];
    await pdf(buffer, {
      pagerender: (pageData) => {
        return pageData.getTextContent().then((tc) => {
          const pageText = tc.items.map((i) => i.str).join(" ");
          pages.push(pageText);
          return pageText;
        });
      }
    });
    if (pages.length === 0 || pages.every((p) => !p.trim())) {
      const full = await pdf(buffer);
      pages.push(full.text);
    }
    const totalPages = pages.length;
    const cleanedPages = pages.map((p) => cleanText(p));
    const pageMap = [];
    let charPos = 0;
    for (let i = 0; i < cleanedPages.length; i++) {
      pageMap.push({ page: i + 1, start: charPos, end: charPos + cleanedPages[i].length });
      charPos += cleanedPages[i].length + 1;
    }
    const fullText = cleanedPages.join("\n");
    return { fullText, pageMap, totalPages };
  } catch (error) {
    console.error("PDF parsing error:", error);
    throw new CorruptedPDFError();
  }
}
function getPageNumber(charStart, pageMap) {
  for (const entry of pageMap) {
    if (charStart >= entry.start && charStart < entry.end) return entry.page;
  }
  return pageMap[pageMap.length - 1]?.page || 1;
}
async function handleUpload(req, res) {
  try {
    const file = req.file;
    if (!file) throw new InvalidFileTypeError();
    const sessionId = req.headers["x-session-id"] || req.body.sessionId || uuidv43();
    const session = getOrCreateSession(sessionId);
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
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.path);
    if (!fullText || fullText.trim().length < 50) {
      fs.unlinkSync(file.path);
      return res.status(422).json({
        error: "No extractable text found \u2014 PDF may be scanned or image-only",
        code: "EMPTY_PDF"
      });
    }
    const documentId = path2.parse(file.filename).name;
    const rawChunks = chunkText(fullText, {
      chunkSizeTokens: 1e3,
      overlapTokens: 200
    });
    if (rawChunks.length === 0) {
      fs.unlinkSync(file.path);
      return res.status(422).json({
        error: "No content could be extracted from PDF",
        code: "EMPTY_PDF"
      });
    }
    const chunks = rawChunks.map((chunk, idx) => ({
      text: chunk.text,
      metadata: {
        document_id: documentId,
        filename: cleanFilename,
        chunk_id: createHash("md5").update(`${cleanFilename}::${chunk.text}`).digest("hex").slice(0, 16),
        chunk_index: idx,
        total_chunks: rawChunks.length,
        page_number: getPageNumber(chunk.charStart, pageMap),
        total_pages: totalPages,
        source_type: "session_upload",
        upload_timestamp: (/* @__PURE__ */ new Date()).toISOString(),
        char_start: chunk.charStart,
        char_end: chunk.charEnd,
        token_count: chunk.tokenCount
      }
    }));
    const collection = await getSessionCollection(sessionId);
    const embeddings = await generateEmbeddings(
      chunks,
      "RETRIEVAL_DOCUMENT",
      ({ current_batch, total_batches }) => {
        if (req.app.locals.progressCallbacks) {
          req.app.locals.progressCallbacks.emit(`progress_${sessionId}`, {
            documentId,
            current_batch,
            total_batches,
            stage: "embedding"
          });
        }
      }
    );
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
        uploadTimestamp: (/* @__PURE__ */ new Date()).toISOString()
      },
      sessionId
    });
  } catch (error) {
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    console.error("Upload error:", error);
    res.status(error.statusCode || 500).json({
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
    res.status(500).json({ error: "Failed to list documents", code: "LIST_ERROR" });
  }
}
async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  try {
    if (sessionId) {
      const collection = await getSessionCollection(sessionId);
      if (collection) {
        const count = await deleteDocumentVectors(collection, documentId);
        if (count > 0) removeDocumentFromSession(sessionId, documentId);
      }
    }
    try {
      const globalCollection2 = await getGlobalCollection();
      await deleteDocumentVectors(globalCollection2, documentId);
    } catch (e) {
    }
    res.json({ success: true, documentId });
  } catch (error) {
    console.error("Delete document error:", error);
    res.status(500).json({ error: "Failed to delete document", code: "DELETE_ERROR" });
  }
}
async function getDocumentFile(req, res) {
  const { documentId } = req.params;
  try {
    const filePath = path2.join(uploadDir, `${documentId}.pdf`);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: "Document file not found", code: "FILE_NOT_FOUND" });
    }
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${path2.basename(filePath)}"`);
    fs.createReadStream(filePath).pipe(res);
  } catch (error) {
    console.error("Get document file error:", error);
    res.status(500).json({ error: "Failed to retrieve document", code: "RETRIEVE_ERROR" });
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
      destination: (req, file, cb) => cb(null, uploadDir),
      filename: (req, file, cb) => cb(null, `${uuidv43()}${path2.extname(file.originalname)}`)
    });
    upload = multer({
      storage,
      limits: { fileSize: parseInt(process.env.MAX_UPLOAD_SIZE_MB || "5") * 1024 * 1024 },
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
    return { confidence: 0, topScore: 0 };
  }
  const scores = results.slice(0, topK).map((r) => r.score);
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  return {
    confidence: Math.round(avgScore * 100),
    // 0.47 → 47
    topScore: Math.max(...scores)
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
function formatContextForPrompt(results, maxTokens = 7e3) {
  if (!results || results.length === 0) return "";
  let totalTokens = 0;
  const contextParts = [];
  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    const tokenEstimate = result.text.length / 4;
    if (totalTokens + tokenEstimate > maxTokens) break;
    totalTokens += tokenEstimate;
    const sourceLabel = result.source_type === "global" ? "[Seed Document]" : "[Session Upload]";
    const page = result.metadata.page_number ? ` (Page ${result.metadata.page_number})` : "";
    contextParts.push(`[${i + 1}] ${sourceLabel} ${result.metadata.filename || "Unknown"}${page}:
${result.text}`);
  }
  return contextParts.join("\n\n---\n\n");
}
function generateCitations(results) {
  if (!results || results.length === 0) return [];
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
  return coverage.topScore < REFUSAL_THRESHOLD;
}
var TOP_K, REFUSAL_THRESHOLD, cachedGlobalCollection, cachedSessionCollections;
var init_retrievalService = __esm({
  "server/services/retrievalService.js"() {
    "use strict";
    init_chromaService();
    init_embeddingService();
    TOP_K = parseInt(process.env.TOP_K) || 5;
    REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;
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
function addTurnWithCitations(sessionId, role, content, citations = [], coverage = null, answerId = null) {
  return addTurn(sessionId, role, content, {
    ...answerId && { id: answerId },
    // override turn id so getSources can match it
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
    return res.status(400).json({ error: "Query is required", code: "MISSING_QUERY" });
  }
  const sessionId = providedSessionId || uuidv45();
  const answerId = uuidv45();
  getOrCreateSession(sessionId);
  addTurnWithCitations(sessionId, "user", query.trim());
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
      confidence: coverage.confidence
      // e.g. 47  → display as "47% confidence"
    });
    const citations = generateCitations(results);
    const sources = results.map((r) => ({
      chunkId: r.id,
      documentId: r.metadata.document_id,
      filename: r.metadata.filename,
      pageNumber: r.metadata.page_number,
      excerpt: r.text.slice(0, 200),
      score: r.score,
      sourceType: r.source_type
    }));
    if (shouldShowRefusal(coverage)) {
      const refusalText = getRefusalText();
      addTurnWithCitations(sessionId, "assistant", refusalText, citations, coverage, answerId);
      sendEvent("complete", {
        answerId,
        response: refusalText,
        citations,
        coverage,
        sources,
        action: "refusal"
      });
      res.end();
      return;
    }
    sendEvent("status", { stage: "generating", message: "Generating response..." });
    const contextText = formatContextForPrompt(results);
    const memoryContext = getRecentTurns(sessionId, 5).map((t) => `${t.role === "user" ? "User" : "Assistant"}: ${t.content}`).join("\n\n");
    const prompt = `You are an AI Knowledge Assistant. Answer based ONLY on the provided context. If the context contains relevant information, use it to answer clearly and cite sources with [1], [2] etc.

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
    addTurnWithCitations(sessionId, "assistant", fullResponse, citations, coverage, answerId);
    sendEvent("complete", { answerId, response: fullResponse, citations, coverage, sources });
    res.end();
  } catch (error) {
    console.error("Chat stream error:", error);
    sendEvent("error", { message: error.message || "An error occurred", code: error.code || "CHAT_ERROR" });
    res.end();
  }
}
async function getSources(req, res) {
  const { answerId } = req.params;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  const recentTurns = getRecentTurns(sessionId, 20);
  const exactMatch = recentTurns.find((t) => t.id === answerId);
  if (exactMatch?.citations?.length > 0) {
    return res.json({ sources: exactMatch.citations });
  }
  const fallback = [...recentTurns].reverse().find(
    (t) => t.role === "assistant" && t.citations?.length > 0
  );
  if (fallback) {
    return res.json({ sources: fallback.citations });
  }
  res.status(404).json({ error: "Sources not found", code: "SOURCES_NOT_FOUND" });
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcblxuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuLy8gQWxpYXMgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbmZ1bmN0aW9uIGdldENsaWVudCgpIHtcbiAgcmV0dXJuIGdldENsb3VkQ2xpZW50KCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IEdsb2JhbCBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY29ubmVjdCB0byBnbG9iYWwgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGdsb2JhbENvbGxlY3Rpb247XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBzZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBgc2Vzc2lvbl8ke3Nlc3Npb25JZH1gO1xuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gY3JlYXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiBkZWxldGVkOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBkZWxldGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgaWRzLFxuICAgICAgZW1iZWRkaW5ncyxcbiAgICAgIGRvY3VtZW50czogdmVjdG9ycy5tYXAodiA9PiB2LnRleHQpLFxuICAgICAgbWV0YWRhdGFzOiB2ZWN0b3JzLm1hcCh2ID0+IHYubWV0YWRhdGEpXG4gICAgfSk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9XG4gICAgfSk7XG4gICAgaWYgKGV4aXN0aW5nLmlkcyAmJiBleGlzdGluZy5pZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGV4aXN0aW5nLmlkcyB9KTtcbiAgICAgIHJldHVybiBleGlzdGluZy5pZHMubGVuZ3RoO1xuICAgIH1cbiAgICByZXR1cm4gMDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudENvdW50KGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICByZXR1cm4gYXdhaXQgY29sbGVjdGlvbi5jb3VudCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBnZXQgZG9jdW1lbnQgY291bnQ6JywgZXJyb3IpO1xuICAgIHJldHVybiAwO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxJdGVtcyA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddXG4gICAgfSk7XG5cbiAgICBjb25zdCBkb2N1bWVudHNNYXAgPSBuZXcgTWFwKCk7XG5cbiAgICBpZiAoYWxsSXRlbXMuaWRzKSB7XG4gICAgICBhbGxJdGVtcy5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYWxsSXRlbXMubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYWxsSXRlbXMuZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBjbGVhbnVwU2Vzc2lvbkNvbGxlY3Rpb25zKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbnMgPSBhd2FpdCBjbGllbnQubGlzdENvbGxlY3Rpb25zKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uQ29sbGVjdGlvbk5hbWVzID0gY29sbGVjdGlvbnMuZmlsdGVyKGMgPT4gYy5zdGFydHNXaXRoKCdzZXNzaW9uXycpKTtcblxuICAgIGlmIChzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjcwNSBObyBzdGFsZSBzZXNzaW9uIGNvbGxlY3Rpb25zIGZvdW5kLicpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBcdUQ4M0VcdURERjkgQ2xlYW5pbmcgdXAgJHtzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLmxlbmd0aH0gc3RhbGUgc2Vzc2lvbiBjb2xsZWN0aW9uKHMpLi4uYCk7XG5cbiAgICBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLm1hcChhc3luYyBjb2xsZWN0aW9uTmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lOiBjb2xsZWN0aW9uTmFtZSB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgICBcdTI3MDUgRGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGAgIFx1MjZBMFx1RkUwRiBDb3VsZCBub3QgZGVsZXRlICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY2xlYW51cCBjb21wbGV0ZS4nKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLndhcm4oJ1x1MjZBMFx1RkUwRiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdXZWIgc2VhcmNoIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1dFQl9TRUFSQ0hfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xubGV0IGVtYmVkZGluZ01vZGVsID0gbnVsbDtcblxuLy8gRml4IDEwOiBMYXp5IGluaXQgXHUyMDE0IGF2b2lkcyBkb3RlbnYgdGltaW5nIGJ1Z1xuZnVuY3Rpb24gZ2V0RW1iZWRkaW5nTW9kZWwoKSB7XG4gIGlmICghZW1iZWRkaW5nTW9kZWwpIHtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkocHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkpO1xuICAgIGVtYmVkZGluZ01vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHtcbiAgICAgIG1vZGVsOiBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZW1iZWRkaW5nTW9kZWw7XG59XG5cbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0O1xuY29uc3QgT1VUUFVUX0RJTUVOU0lPTlMgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7IC8vIEZpeCA5XG5jb25zdCBHUk9VUF9XQUlUX01TID0gNjEwMDA7XG5cbi8vIEZpeCAxMSsxMjogVXNlIGJhdGNoRW1iZWRDb250ZW50cyBcdTIwMTQgb25lIEFQSSBjYWxsIHJldHVybnMgTiB2ZWN0b3JzIChvbmUgcGVyIHRleHQpXG4vLyBGaXggMTM6IHRhc2tUeXBlIHBhc3NlZCBwZXIgcmVxdWVzdFxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbWF4QXR0ZW1wdHMgPSA1O1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldEVtYmVkZGluZ01vZGVsKCk7XG5cbiAgICAvLyBGaXggMTI6IENvcnJlY3QgQVBJIGZvcm1hdCB1c2luZyBiYXRjaEVtYmVkQ29udGVudHNcbiAgICAvLyBGaXggOTogb3V0cHV0RGltZW5zaW9uYWxpdHkgZXhwbGljaXRseSBzZXRcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5iYXRjaEVtYmVkQ29udGVudHMoe1xuICAgICAgcmVxdWVzdHM6IHRleHRzLm1hcCh0ZXh0ID0+ICh7XG4gICAgICAgIG1vZGVsOiBgbW9kZWxzLyR7bW9kZWxOYW1lfWAsXG4gICAgICAgIGNvbnRlbnQ6IHsgcGFydHM6IFt7IHRleHQgfV0gfSxcbiAgICAgICAgdGFza1R5cGUsICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gRml4IDEzXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBPVVRQVVRfRElNRU5TSU9OUygpICAgIC8vIEZpeCA5XG4gICAgICB9KSlcbiAgICB9KTtcblxuICAgIGlmICghcmVzdWx0Py5lbWJlZGRpbmdzIHx8IHJlc3VsdC5lbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtyZXN1bHQ/LmVtYmVkZGluZ3M/Lmxlbmd0aCA/PyAwfWApO1xuICAgIH1cblxuICAgIC8vIFJldHVybnMgYXJyYXkgb2YgdmVjdG9ycyBcdTIwMTQgb25lIHBlciBpbnB1dCB0ZXh0IFx1MjcwNVxuICAgIHJldHVybiByZXN1bHQuZW1iZWRkaW5ncy5tYXAoZSA9PiB7XG4gICAgICBpZiAoIWU/LnZhbHVlcykgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdNaXNzaW5nIHZhbHVlcyBpbiBlbWJlZGRpbmcgcmVzcG9uc2UnKTtcbiAgICAgIHJldHVybiBlLnZhbHVlcztcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzNDI5ID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKTtcblxuICAgIGlmIChpczQyOSAmJiBhdHRlbXB0IDwgbWF4QXR0ZW1wdHMpIHtcbiAgICAgIGNvbnN0IHJldHJ5RGVsYXkgPSBlcnJvci5yZXRyeUFmdGVyIHx8IEdST1VQX1dBSVRfTVM7XG4gICAgICBjb25zb2xlLmxvZyhgUmF0ZSBsaW1pdGVkLCB3YWl0aW5nICR7cmV0cnlEZWxheSAvIDEwMDB9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9KWApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gU3RyYXRlZ3k6XG4vLyAgIC0gU3BsaXQgY2h1bmtzIGludG8gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIChkZWZhdWx0IDcpIFx1MjE5MiAxIEFQSSBjYWxsIHBlciBiYXRjaCBcdTIxOTIgNyB2ZWN0b3JzIGJhY2tcbi8vICAgLSBGaXJlIFBBUkFMTEVMX0NBTExTIChkZWZhdWx0IDQpIGJhdGNoZXMgc2ltdWx0YW5lb3VzbHkgXHUyMTkyIDI4IGNodW5rcyBwZXIgZ3JvdXBcbi8vICAgLSBXYWl0IDYxcyBiZXR3ZWVuIGdyb3VwcyB0byByZXNwZWN0IHJhdGUgbGltaXRzXG4vL1xuLy8gRm9yIDQ4IGNodW5rczogY2VpbCg0OC83KSA9IDcgYmF0Y2hlcyBcdTIxOTIgMiBwYXJhbGxlbCBncm91cHMgKDQrMykgXHUyMTkyIH4yIG1pbnV0ZXMgdG90YWxcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZUVtYmVkZGluZ3MoY2h1bmtzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBvblByb2dyZXNzKSB7XG4gIGlmICghY2h1bmtzIHx8IGNodW5rcy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICBjb25zdCBiYXRjaFNpemUgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuICBjb25zdCBlbWJlZGRpbmdzID0gW107XG5cbiAgLy8gU3BsaXQgY2h1bmtzIGludG8gYmF0Y2hlcyBvZiBiYXRjaFNpemVcbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSk7XG4gIH1cblxuICAvLyBHcm91cCBiYXRjaGVzIGludG8gcGFyYWxsZWwgc2V0cyBvZiBwYXJhbGxlbENhbGxzXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG4gICAgY29uc3QgY2h1bmtzQ292ZXJlZCA9IE1hdGgubWluKChpICsgcGFyYWxsZWxDYWxscykgKiBiYXRjaFNpemUsIGNodW5rcy5sZW5ndGgpO1xuXG4gICAgY29uc29sZS5sb2coYCAgRW1iZWRkaW5nIGdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCAke3BhcmFsbGVsQmF0Y2hlcy5sZW5ndGh9IGJhdGNoIGNhbGwocykgaW4gcGFyYWxsZWwgKGNodW5rcyAke2kgKiBiYXRjaFNpemUgKyAxfVx1MjAxMyR7Y2h1bmtzQ292ZXJlZH0pLi4uYCk7XG5cbiAgICAvLyBGaXJlIHBhcmFsbGVsQ2FsbHMgYmF0Y2ggY2FsbHMgc2ltdWx0YW5lb3VzbHlcbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgcGFyYWxsZWxCYXRjaGVzLm1hcChiYXRjaCA9PiBlbWJlZEJhdGNoKGJhdGNoLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgLy8gQ29sbGVjdCByZXN1bHRzIHBlciBiYXRjaFxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGJhdGNoID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlOyAvLyBhcnJheSBvZiBOIHZlY3RvcnNcbiAgICAgICAgYmF0Y2guZm9yRWFjaCgoY2h1bmssIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgZW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rXyR7aSAqIGJhdGNoU2l6ZSArIGJhdGNoSWR4ICogYmF0Y2hTaXplICsgY2h1bmtJZHh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1tjaHVua0lkeF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGAgIEJhdGNoICR7aSArIGJhdGNoSWR4fSBmYWlsZWQsIHdpbGwgcmV0cnkgaW5kaXZpZHVhbGx5OmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICBmYWlsZWRCYXRjaGVzLnB1c2goeyBiYXRjaCwgYmF0Y2hJZHg6IGkgKyBiYXRjaElkeCB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIC8vIFdhaXQgYmV0d2VlbiBncm91cHMgKHNraXAgd2FpdCBhZnRlciBsYXN0IGdyb3VwIHVubGVzcyB0aGVyZSBhcmUgcmV0cmllcylcbiAgICBjb25zdCBpc0xhc3RHcm91cCA9IGkgKyBwYXJhbGxlbENhbGxzID49IGJhdGNoZXMubGVuZ3RoO1xuICAgIGlmICghaXNMYXN0R3JvdXAgfHwgZmFpbGVkQmF0Y2hlcy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgbmV4dCBncm91cC4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIEdST1VQX1dBSVRfTVMpKTtcbiAgICB9XG5cbiAgICAvLyBSZXRyeSBmYWlsZWQgYmF0Y2hlcyBvbmUgY2h1bmsgYXQgYSB0aW1lXG4gICAgZm9yIChjb25zdCB7IGJhdGNoLCBiYXRjaElkeCB9IG9mIGZhaWxlZEJhdGNoZXMpIHtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgYmF0Y2gpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtiYXRjaElkeH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFx1MjcwNSBSZXRyeSBzdWNjZWVkZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLmVycm9yKGAgIFx1Mjc0QyBSZXRyeSBmYWlsZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkfTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICByZXR1cm4gZW1iZWRkaW5ncztcbn1cblxuLy8gRml4IDEzOiBRdWVyeSB1c2VzIFJFVFJJRVZBTF9RVUVSWSB0YXNrIHR5cGUsIHNpbmdsZSBlbWJlZCB2aWEgYmF0Y2ggb2YgMVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3F1ZXJ5XSwgJ1JFVFJJRVZBTF9RVUVSWScpO1xuICByZXR1cm4gdmVjdG9yc1swXTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlKHRleHQpIHtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3RleHRdLCAnUkVUUklFVkFMX0RPQ1VNRU5UJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmF0ZUxpbWl0U3RhdGUoKSB7XG4gIHJldHVybiB7XG4gICAgbWF4VG9rZW5zUGVyTWludXRlOiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUkFURV9MSU1JVF9UT0tFTlNfUEVSX01JTlVURSkgfHwgMzAwMDAsXG4gICAgcGFyYWxsZWxDYWxsczogUEFSQUxMRUxfQ0FMTFMoKSxcbiAgICBtYXhDaHVua3NQZXJDYWxsOiBCQVRDSF9TSVpFKCksXG4gICAgb3V0cHV0RGltZW5zaW9uczogT1VUUFVUX0RJTUVOU0lPTlMoKSAgLy8gRml4IDk6IGV4cG9zZSBmb3IgaGVhbHRoIGNoZWNrXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldFJhdGVMaW1pdFN0YXRlIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBDaGVjayBHZW1pbmkgKHZpYSBBUEkga2V5IHByZXNlbmNlKVxuICBoZWFsdGhTdGF0dXMuc2VydmljZXMuZ2VtaW5pID0ge1xuICAgIHN0YXR1czogcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkgPyAnY29uZmlndXJlZCcgOiAnbm90X2NvbmZpZ3VyZWQnXG4gIH07XG5cbiAgLy8gR2V0IHJhdGUgbGltaXQgc3RhdGVcbiAgaGVhbHRoU3RhdHVzLnJhdGVMaW1pdCA9IGdldFJhdGVMaW1pdFN0YXRlKCk7XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcblxuY29uc3QgQ0hBUlNfUEVSX1RPS0VOID0gNDtcbmNvbnN0IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlMgPSAxMDAwO1xuY29uc3QgREVGQVVMVF9PVkVSTEFQX1RPS0VOUyA9IDIwMDtcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG4vLyBGaXggNTogQ2xlYW4gcmF3IFBERiB0ZXh0IGJlZm9yZSBjaHVua2luZ1xuZXhwb3J0IGZ1bmN0aW9uIGNsZWFuVGV4dCh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAnJztcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvXFxmL2csICdcXG4nKSAgICAgICAgICAgICAgICAvLyBmb3JtIGZlZWRzIFx1MjE5MiBuZXdsaW5lXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpICAgICAvLyBjb2xsYXBzZSAzKyBibGFuayBsaW5lcyB0byAyXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJykgICAgICAgIC8vIHJlbW92ZSBsb25lIHBhZ2UgbnVtYmVyc1xuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJykgICAgICAgICAgLy8gY29sbGFwc2UgbXVsdGlwbGUgc3BhY2VzL3RhYnNcbiAgICAudHJpbSgpO1xufVxuXG4vLyBGaXggNDogQ29udGVudC1oYXNoIGJhc2VkIGNodW5rIElEIGZvciBkZWR1cGxpY2F0aW9uXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgY2h1bmtTaXplVG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRva2VucyA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBERUZBVUxUX09WRVJMQVBfVE9LRU5TO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICBjb25zdCBjaHVua1NpemVDaGFycyA9IGNodW5rU2l6ZVRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBjb25zdCBjaHVua3MgPSBbXTtcbiAgbGV0IHN0YXJ0ID0gMDtcbiAgbGV0IGNodW5rSW5kZXggPSAwO1xuXG4gIHdoaWxlIChzdGFydCA8IHRleHQubGVuZ3RoKSB7XG4gICAgbGV0IGVuZCA9IHN0YXJ0ICsgY2h1bmtTaXplQ2hhcnM7XG5cbiAgICBpZiAoZW5kIDwgdGV4dC5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IGJyZWFrUG9pbnRzID0gWycuICcsICcuXFxuJywgJyEgJywgJz8gJywgJ1xcblxcbicsICdcXG4nLCAnICddO1xuICAgICAgY29uc3Qgc2VhcmNoU3RhcnQgPSBlbmQgLSBNYXRoLmZsb29yKGNodW5rU2l6ZUNoYXJzICogMC4yKTtcblxuICAgICAgZm9yIChjb25zdCBicmVha3BvaW50IG9mIGJyZWFrUG9pbnRzKSB7XG4gICAgICAgIGNvbnN0IGlkeCA9IHRleHQubGFzdEluZGV4T2YoYnJlYWtwb2ludCwgZW5kKTtcbiAgICAgICAgaWYgKGlkeCA+IHNlYXJjaFN0YXJ0ICYmIGlkeCA+IHN0YXJ0KSB7XG4gICAgICAgICAgZW5kID0gaWR4ICsgYnJlYWtwb2ludC5sZW5ndGg7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBlbmQgPSBNYXRoLm1pbihlbmQsIHRleHQubGVuZ3RoKTtcbiAgICBjb25zdCBjaHVua0NvbnRlbnQgPSB0ZXh0LnNsaWNlKHN0YXJ0LCBlbmQpLnRyaW0oKTtcblxuICAgIC8vIEZpeCA2OiBTa2lwIGNodW5rcyBiZWxvdyBtaW5pbXVtIHNpemVcbiAgICBpZiAoY2h1bmtDb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY2h1bmtDb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2VucyhjaHVua0NvbnRlbnQpLFxuICAgICAgICBjaGFyU3RhcnQ6IHN0YXJ0LFxuICAgICAgICBjaGFyRW5kOiBlbmQsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgLy8gRml4IDE6IENvcnJlY3Qgb3ZlcmxhcCBcdTIwMTQgb25seSBza2lwIG92ZXJsYXAgaWYgaXQgd291bGQgY2F1c2UgaW5maW5pdGUgbG9vcFxuICAgIGNvbnN0IG5leHRTdGFydCA9IGVuZCAtIG92ZXJsYXBDaGFycztcbiAgICBzdGFydCA9IG5leHRTdGFydCA+IHN0YXJ0ID8gbmV4dFN0YXJ0IDogZW5kO1xuXG4gICAgaWYgKGNodW5rSW5kZXggPiAxMDAwMCkge1xuICAgICAgY29uc29sZS53YXJuKCdDaHVuayBsaW1pdCByZWFjaGVkLCBzdG9wcGluZycpO1xuICAgICAgYnJlYWs7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIGNodW5rcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rUERGQ29udGVudChwZGZEYXRhLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgeyBmaWxlbmFtZSwgZG9jdW1lbnRJZCwgcGFnZU51bWJlciwgdGV4dCwgdG90YWxQYWdlcyB9ID0gcGRmRGF0YTtcblxuICAvLyBGaXggMzogRGV0ZWN0IHNjYW5uZWQvZW1wdHkgUERGc1xuICBpZiAoIXRleHQgfHwgdGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICBjb25zb2xlLndhcm4oYFx1MjZBMFx1RkUwRiAgJHtmaWxlbmFtZX0gcGFnZSAke3BhZ2VOdW1iZXJ9OiBleHRyYWN0ZWQgdGV4dCB0b28gc2hvcnQgXHUyMDE0IG1heSBiZSBhIHNjYW5uZWQgcGFnZSwgc2tpcHBpbmdgKTtcbiAgICByZXR1cm4gW107XG4gIH1cblxuICAvLyBGaXggNTogQ2xlYW4gdGV4dCBiZWZvcmUgY2h1bmtpbmdcbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG5cbiAgY29uc3QgdGV4dENodW5rcyA9IGNodW5rVGV4dChjbGVhbmVkVGV4dCwgb3B0aW9ucyk7XG5cbiAgLy8gRml4IDc6IENvbXB1dGUgdG90YWxfY2h1bmtzIGFuZCBhdHRhY2ggdG8gYWxsIG1ldGFkYXRhXG4gIGNvbnN0IHRvdGFsQ2h1bmtzID0gdGV4dENodW5rcy5sZW5ndGg7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+IHtcbiAgICAvLyBGaXggNDogVXNlIGNvbnRlbnQgaGFzaCBhcyBjaHVuayBJRCBmb3IgZGVkdXBsaWNhdGlvblxuICAgIGNvbnN0IGNodW5rSWQgPSBnZW5lcmF0ZUNodW5rSWQoY2h1bmsudGV4dCwgZmlsZW5hbWUpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY2h1bmtJZCxcbiAgICAgICAgY2h1bmtfaW5kZXg6IGNodW5rLmNodW5rSW5kZXgsXG4gICAgICAgIHRvdGFsX2NodW5rczogdG90YWxDaHVua3MsICAgICAgICAgLy8gRml4IDdcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSwgICAgICAvLyBGaXggMjogY2FsbGVyIG11c3QgcGFzcyBjb3JyZWN0IHBhZ2VcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6ICdwZGYnLFxuICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIGNoYXJfc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgY2hhcl9lbmQ6IGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfTtcbiAgfSk7XG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RTZWN0aW9uVGl0bGUodGV4dCkge1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbi8vIEZpeCA4OiBtZXJnZUNodW5rcyByZW1vdmVkIFx1MjAxNCB3YXMgZGVhZCBjb2RlIGZyb20gb2xkIGJyb2tlbiBiYXRjaGluZyBzdHJhdGVneSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBnZXRHbG9iYWxDb2xsZWN0aW9uLCBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgbGlzdERvY3VtZW50cyB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTID0gNjA7XG5jb25zdCBzZXNzaW9ucyA9IG5ldyBNYXAoKTtcbmNvbnN0IE1BWF9QREZTX1BFUl9TRVNTSU9OID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04pIHx8IDM7XG5jb25zdCBNQVhfVVBMT0FEX1NJWkVfTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIpIHx8IDU7XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVTZXNzaW9uKCkge1xuICBjb25zdCBzZXNzaW9uSWQgPSB1dWlkdjQoKTtcbiAgY29uc3Qgc2Vzc2lvbiA9IHtcbiAgICBpZDogc2Vzc2lvbklkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcblxuICBzZXNzaW9ucy5zZXQoc2Vzc2lvbklkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG5cbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHtcbiAgICAgIHJldHVybiBleGlzdGluZztcbiAgICB9XG4gIH1cblxuICByZXR1cm4gY3JlYXRlU2Vzc2lvbigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSB7XG4gIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gIGNvbnN0IGxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKHNlc3Npb24ubGFzdEFjY2Vzc2VkKS5nZXRUaW1lKCk7XG4gIGNvbnN0IHRpbWVvdXRNcyA9IHNlc3Npb24udGltZW91dE1pbnV0ZXMgKiA2MCAqIDEwMDA7XG4gIHJldHVybiAobm93IC0gbGFzdEFjY2Vzc2VkKSA+IHRpbWVvdXRNcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIHNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgaWQ6IGRvY3VtZW50SW5mby5pZCxcbiAgICBmaWxlbmFtZTogZG9jdW1lbnRJbmZvLmZpbGVuYW1lLFxuICAgIGZpbGVTaXplOiBkb2N1bWVudEluZm8uZmlsZVNpemUsXG4gICAgcGFnZUNvdW50OiBkb2N1bWVudEluZm8ucGFnZUNvdW50LFxuICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICBjaHVua0NvdW50OiBkb2N1bWVudEluZm8uY2h1bmtDb3VudCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnXG4gIH0pO1xuXG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogJ1Nlc3Npb24gbm90IGZvdW5kJyB9O1xuICB9XG5cbiAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuXG4gIHJldHVybiB7IGNhblVwbG9hZDogdHJ1ZSB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVVcGxvYWQoc2Vzc2lvbklkLCBmaWxlLCBmaWxlbmFtZSkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBjb25zdCBlcnJvcnMgPSBbXTtcblxuICAvLyBDaGVjayBmaWxlIHNpemVcbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgLy8gQ2hlY2sgbWF4IFBERnNcbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgLy8gQ2hlY2sgZm9yIGR1cGxpY2F0ZSBmaWxlbmFtZVxuICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gZmlsZW5hbWUpKSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmApO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpc1ZhbGlkOiBlcnJvcnMubGVuZ3RoID09PSAwLFxuICAgIGVycm9ycyxcbiAgICBpc0xhcmdlRmlsZTogZmlsZS5zaXplID4gKE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0ICogMC42KSAvLyA2MCUgb2YgbWF4XG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgcmV0dXJuIGZhbHNlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbkRvY3MgPSBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gIGNvbnN0IGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIGNvbnN0IGdsb2JhbERvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKGdsb2JhbENvbGxlY3Rpb24pO1xuXG4gIHJldHVybiB7XG4gICAgc2Vzc2lvbkRvY3VtZW50czogc2Vzc2lvbkRvY3MsXG4gICAgZ2xvYmFsRG9jdW1lbnRzOiBnbG9iYWxEb2NzXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpZDogc2Vzc2lvbi5pZCxcbiAgICBkb2N1bWVudENvdW50OiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBzZXNzaW9uLmNyZWF0ZWRBdCxcbiAgICBsYXN0QWNjZXNzZWQ6IHNlc3Npb24ubGFzdEFjY2Vzc2VkLFxuICAgIHRvdGFsU2l6ZTogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmZpbGVTaXplIHx8IDApLCAwKSxcbiAgICB0b3RhbENodW5rczogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsaXN0QWN0aXZlU2Vzc2lvbnMoKSB7XG4gIHJldHVybiBBcnJheS5mcm9tKHNlc3Npb25zLnZhbHVlcygpKS5maWx0ZXIocyA9PiAhaXNTZXNzaW9uRXhwaXJlZChzKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhbnVwRXhwaXJlZFNlc3Npb25zKCkge1xuICBsZXQgY2xlYW5lZCA9IDA7XG4gIGZvciAoY29uc3QgW2lkLCBzZXNzaW9uXSBvZiBzZXNzaW9ucykge1xuICAgIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgICBzZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IGZzIGZyb20gJ2ZzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5pbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG5pbXBvcnQgeyBzYW5pdGl6ZUZpbGVuYW1lLCB2YWxpZGF0ZVBERkZpbGUsIHZhbGlkYXRlRmlsZVNpemUgfSBmcm9tICcuLi91dGlscy9zYW5pdGl6ZS5qcyc7XG5pbXBvcnQge1xuICBDb3JydXB0ZWRQREZFcnJvcixcbiAgSW52YWxpZEZpbGVUeXBlRXJyb3IsXG4gIEZpbGVUb29MYXJnZUVycm9yLFxuICBUb29NYW55UERGc0Vycm9yLFxuICBEdXBsaWNhdGVGaWxlRXJyb3Jcbn0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcbmltcG9ydCB7IGdldEdsb2JhbENvbGxlY3Rpb24sIGdldFNlc3Npb25Db2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNodW5rVGV4dCwgY2xlYW5UZXh0IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG5pbXBvcnQgeyBnZW5lcmF0ZUVtYmVkZGluZ3MgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgY2FuQWNjZXB0VXBsb2FkLCBhZGREb2N1bWVudFRvU2Vzc2lvbiwgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbiwgZ2V0QWxsRG9jdW1lbnRzIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG5pZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbn1cblxuY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gIGRlc3RpbmF0aW9uOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgdXBsb2FkRGlyKSxcbiAgZmlsZW5hbWU6IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCBgJHt1dWlkdjQoKX0ke3BhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSl9YClcbn0pO1xuXG5jb25zdCB1cGxvYWQgPSBtdWx0ZXIoe1xuICBzdG9yYWdlLFxuICBsaW1pdHM6IHsgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjQgfSxcbiAgZmlsZUZpbHRlcjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBpZiAoZmlsZS5taW1ldHlwZSA9PT0gJ2FwcGxpY2F0aW9uL3BkZicgJiYgcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKS50b0xvd2VyQ2FzZSgpID09PSAnLnBkZicpIHtcbiAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjYihuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKSk7XG4gICAgfVxuICB9XG59KTtcblxuLy8gUGFyc2UgUERGIHdpdGggcGVyLXBhZ2UgYm91bmRhcnkgbWFwIGZvciBhY2N1cmF0ZSBwYWdlIG51bWJlcnNcbmFzeW5jIGZ1bmN0aW9uIHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGVQYXRoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYnVmZmVyID0gZnMucmVhZEZpbGVTeW5jKGZpbGVQYXRoKTtcblxuICAgIGNvbnN0IHBhZ2VzID0gW107XG4gICAgYXdhaXQgcGRmKGJ1ZmZlciwge1xuICAgICAgcGFnZXJlbmRlcjogKHBhZ2VEYXRhKSA9PiB7XG4gICAgICAgIHJldHVybiBwYWdlRGF0YS5nZXRUZXh0Q29udGVudCgpLnRoZW4odGMgPT4ge1xuICAgICAgICAgIGNvbnN0IHBhZ2VUZXh0ID0gdGMuaXRlbXMubWFwKGkgPT4gaS5zdHIpLmpvaW4oJyAnKTtcbiAgICAgICAgICBwYWdlcy5wdXNoKHBhZ2VUZXh0KTtcbiAgICAgICAgICByZXR1cm4gcGFnZVRleHQ7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgLy8gRmFsbGJhY2sgaWYgcGFnZXJlbmRlciB5aWVsZHMgbm90aGluZ1xuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuXG4gICAgLy8gQnVpbGQgcGFnZSBib3VuZGFyeSBtYXBcbiAgICBjb25zdCBwYWdlTWFwID0gW107XG4gICAgbGV0IGNoYXJQb3MgPSAwO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2xlYW5lZFBhZ2VzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBwYWdlTWFwLnB1c2goeyBwYWdlOiBpICsgMSwgc3RhcnQ6IGNoYXJQb3MsIGVuZDogY2hhclBvcyArIGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggfSk7XG4gICAgICBjaGFyUG9zICs9IGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggKyAxO1xuICAgIH1cblxuICAgIGNvbnN0IGZ1bGxUZXh0ID0gY2xlYW5lZFBhZ2VzLmpvaW4oJ1xcbicpO1xuICAgIHJldHVybiB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUERGIHBhcnNpbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBDb3JydXB0ZWRQREZFcnJvcigpO1xuICB9XG59XG5cbmZ1bmN0aW9uIGdldFBhZ2VOdW1iZXIoY2hhclN0YXJ0LCBwYWdlTWFwKSB7XG4gIGZvciAoY29uc3QgZW50cnkgb2YgcGFnZU1hcCkge1xuICAgIGlmIChjaGFyU3RhcnQgPj0gZW50cnkuc3RhcnQgJiYgY2hhclN0YXJ0IDwgZW50cnkuZW5kKSByZXR1cm4gZW50cnkucGFnZTtcbiAgfVxuICByZXR1cm4gcGFnZU1hcFtwYWdlTWFwLmxlbmd0aCAtIDFdPy5wYWdlIHx8IDE7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBmaWxlID0gcmVxLmZpbGU7XG4gICAgaWYgKCFmaWxlKSB0aHJvdyBuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKTtcblxuICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IG1heFBERnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTiB8fCAnMycpO1xuICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPj0gbWF4UERGcykge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgdGhyb3cgbmV3IFRvb01hbnlQREZzRXJyb3IobWF4UERGcyk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgdGhyb3cgbmV3IER1cGxpY2F0ZUZpbGVFcnJvcihjbGVhbkZpbGVuYW1lKTtcbiAgICB9XG5cbiAgICAvLyBQYXJzZSB3aXRoIGJvdW5kYXJ5IG1hcFxuICAgIGNvbnN0IHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfSA9IGF3YWl0IHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGUucGF0aCk7XG5cbiAgICBpZiAoIWZ1bGxUZXh0IHx8IGZ1bGxUZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNDIyKS5qc29uKHtcbiAgICAgICAgZXJyb3I6ICdObyBleHRyYWN0YWJsZSB0ZXh0IGZvdW5kIFx1MjAxNCBQREYgbWF5IGJlIHNjYW5uZWQgb3IgaW1hZ2Utb25seScsXG4gICAgICAgIGNvZGU6ICdFTVBUWV9QREYnXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBkb2N1bWVudElkID0gcGF0aC5wYXJzZShmaWxlLmZpbGVuYW1lKS5uYW1lO1xuXG4gICAgLy8gQ2h1bmsgZnVsbCBkb2N1bWVudCBhdCAxMDAwIHRva2VucyAvIDIwMCBvdmVybGFwXG4gICAgY29uc3QgcmF3Q2h1bmtzID0gY2h1bmtUZXh0KGZ1bGxUZXh0LCB7XG4gICAgICBjaHVua1NpemVUb2tlbnM6IDEwMDAsXG4gICAgICBvdmVybGFwVG9rZW5zOiAyMDBcbiAgICB9KTtcblxuICAgIGlmIChyYXdDaHVua3MubGVuZ3RoID09PSAwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MjIpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ05vIGNvbnRlbnQgY291bGQgYmUgZXh0cmFjdGVkIGZyb20gUERGJyxcbiAgICAgICAgY29kZTogJ0VNUFRZX1BERidcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIC8vIEJ1aWxkIGNodW5rcyB3aXRoIGFjY3VyYXRlIHBhZ2UgbnVtYmVycyBmcm9tIGJvdW5kYXJ5IG1hcFxuICAgIGNvbnN0IGNodW5rcyA9IHJhd0NodW5rcy5tYXAoKGNodW5rLCBpZHgpID0+ICh7XG4gICAgICB0ZXh0OiBjaHVuay50ZXh0LFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY3JlYXRlSGFzaCgnbWQ1JykudXBkYXRlKGAke2NsZWFuRmlsZW5hbWV9Ojoke2NodW5rLnRleHR9YCkuZGlnZXN0KCdoZXgnKS5zbGljZSgwLCAxNiksXG4gICAgICAgIGNodW5rX2luZGV4OiBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogcmF3Q2h1bmtzLmxlbmd0aCxcbiAgICAgICAgcGFnZV9udW1iZXI6IGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMsXG4gICAgICAgIHNvdXJjZV90eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIGNoYXJfc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgY2hhcl9lbmQ6IGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfSkpO1xuXG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG5cbiAgICAvLyBVc2Ugc2FtZSBiYXRjaGluZyBzdHJhdGVneSBhcyBzZWVkIHNjcmlwdFxuICAgIC8vICg3IGNodW5rcyBwZXIgYmF0Y2hFbWJlZENvbnRlbnRzLCA0IHBhcmFsbGVsIGNhbGxzLCA2MXMgd2FpdCBiZXR3ZWVuIGdyb3VwcylcbiAgICBjb25zdCBlbWJlZGRpbmdzID0gYXdhaXQgZ2VuZXJhdGVFbWJlZGRpbmdzKFxuICAgICAgY2h1bmtzLFxuICAgICAgJ1JFVFJJRVZBTF9ET0NVTUVOVCcsXG4gICAgICAoeyBjdXJyZW50X2JhdGNoLCB0b3RhbF9iYXRjaGVzIH0pID0+IHtcbiAgICAgICAgLy8gRW1pdCBTU0UgcHJvZ3Jlc3MgaWYgYXZhaWxhYmxlXG4gICAgICAgIGlmIChyZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcykge1xuICAgICAgICAgIHJlcS5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzLmVtaXQoYHByb2dyZXNzXyR7c2Vzc2lvbklkfWAsIHtcbiAgICAgICAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgICAgICBjdXJyZW50X2JhdGNoLFxuICAgICAgICAgICAgdG90YWxfYmF0Y2hlcyxcbiAgICAgICAgICAgIHN0YWdlOiAnZW1iZWRkaW5nJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgKTtcblxuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNTAzKS5qc29uKHtcbiAgICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZ2VuZXJhdGUgZW1iZWRkaW5ncycsXG4gICAgICAgIGNvZGU6ICdFTUJFRERJTkdfRkFJTEVEJ1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgIGNvbGxlY3Rpb24sXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgZW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgKTtcblxuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgaWQ6IGRvY3VtZW50SWQsXG4gICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsXG4gICAgICBjaHVua0NvdW50OiBlbWJlZGRpbmdzLmxlbmd0aFxuICAgIH0pO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGRvY3VtZW50OiB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLFxuICAgICAgICBjaHVua0NvdW50OiBlbWJlZGRpbmdzLmxlbmd0aCxcbiAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBzZXNzaW9uSWRcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGlmIChyZXEuZmlsZSAmJiBmcy5leGlzdHNTeW5jKHJlcS5maWxlLnBhdGgpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKHJlcS5maWxlLnBhdGgpO1xuICAgIH1cbiAgICBjb25zb2xlLmVycm9yKCdVcGxvYWQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDApLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdVUExPQURfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGF3YWl0IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICAgIGNvbnN0IGNvdW50ID0gYXdhaXQgZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpO1xuICAgICAgICBpZiAoY291bnQgPiAwKSByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gICAgICBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoZ2xvYmFsQ29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgfSBjYXRjaCAoZSkgeyAvKiBub3QgaW4gZ2xvYmFsICovIH1cblxuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSwgZG9jdW1lbnRJZCB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdEZWxldGUgZG9jdW1lbnQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50JywgY29kZTogJ0RFTEVURV9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZVBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBgJHtkb2N1bWVudElkfS5wZGZgKTtcbiAgICBpZiAoIWZzLmV4aXN0c1N5bmMoZmlsZVBhdGgpKSB7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgICB9XG4gICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBgYXR0YWNobWVudDsgZmlsZW5hbWU9XCIke3BhdGguYmFzZW5hbWUoZmlsZVBhdGgpfVwiYCk7XG4gICAgZnMuY3JlYXRlUmVhZFN0cmVhbShmaWxlUGF0aCkucGlwZShyZXMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbnJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG5yb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG5yb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjsiLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtpbXBvcnQgeyBnZXRHbG9iYWxDb2xsZWN0aW9uLCBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDU7XG5jb25zdCBSRUZVU0FMX1RIUkVTSE9MRCA9IHBhcnNlRmxvYXQocHJvY2Vzcy5lbnYuUkVGVVNBTF9USFJFU0hPTEQpIHx8IDAuMDU7XG5cbi8vIENhY2hlIHJlc29sdmVkIGNvbGxlY3Rpb24gb2JqZWN0cyBcdTIwMTQgbmV2ZXIgaGl0IENocm9tYSBtb3JlIHRoYW4gb25jZSBwZXIgc2Vzc2lvblxubGV0IGNhY2hlZEdsb2JhbENvbGxlY3Rpb24gPSBudWxsO1xuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlR2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFjYWNoZWRHbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY2FjaGVkR2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgfVxuICByZXR1cm4gY2FjaGVkR2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuYXN5bmMgZnVuY3Rpb24gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgfVxuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgfVxuICAgIHJldHVybiBjb2xsZWN0aW9uO1xuICB9IGNhdGNoIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxufVxuXG5mdW5jdGlvbiBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLID0gVE9QX0spIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAgfTtcbiAgfVxuXG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gci5zY29yZSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcblxuICByZXR1cm4ge1xuICAgIGNvbmZpZGVuY2U6IE1hdGgucm91bmQoYXZnU2NvcmUgKiAxMDApLCAvLyAwLjQ3IFx1MjE5MiA0N1xuICAgIHRvcFNjb3JlOiBNYXRoLm1heCguLi5zY29yZXMpXG4gIH07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiByZXRyaWV2ZUZvclF1ZXJ5KHF1ZXJ5LCBzZXNzaW9uSWQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0b3BLID0gb3B0aW9ucy50b3BLIHx8IFRPUF9LO1xuICBjb25zdCBpbmNsdWRlR2xvYmFsID0gb3B0aW9ucy5pbmNsdWRlR2xvYmFsICE9PSBmYWxzZTtcblxuICB0cnkge1xuICAgIC8vIFJ1biBlbWJlZGRpbmcgKyBib3RoIGNvbGxlY3Rpb24gZmV0Y2hlcyBpbiBwYXJhbGxlbFxuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgZ2xvYmFsQ29sbGVjdGlvbiwgc2Vzc2lvbkNvbGxlY3Rpb25dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBpbmNsdWRlR2xvYmFsID8gZ2V0T3JDYWNoZUdsb2JhbENvbGxlY3Rpb24oKSA6IFByb21pc2UucmVzb2x2ZShudWxsKSxcbiAgICAgIHNlc3Npb25JZCA/IGdldE9yQ2FjaGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIDogUHJvbWlzZS5yZXNvbHZlKG51bGwpXG4gICAgXSk7XG5cbiAgICAvLyBRdWVyeSBib3RoIGNvbGxlY3Rpb25zIGluIHBhcmFsbGVsXG4gICAgY29uc3QgcXVlcnlQcm9taXNlcyA9IFtdO1xuXG4gICAgaWYgKGdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICAgIHF1ZXJ5UHJvbWlzZXMucHVzaChcbiAgICAgICAgcXVlcnlDb2xsZWN0aW9uKGdsb2JhbENvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLKVxuICAgICAgICAgIC50aGVuKHJlc3VsdHMgPT4gKHsgdHlwZTogJ2dsb2JhbCcsIHJlc3VsdHMgfSkpXG4gICAgICAgICAgLmNhdGNoKCgpID0+ICh7IHR5cGU6ICdnbG9iYWwnLCByZXN1bHRzOiBbXSB9KSlcbiAgICAgICk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb25Db2xsZWN0aW9uKSB7XG4gICAgICBxdWVyeVByb21pc2VzLnB1c2goXG4gICAgICAgIHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAgICAgLnRoZW4ocmVzdWx0cyA9PiAoeyB0eXBlOiAnc2Vzc2lvbicsIHJlc3VsdHMgfSkpXG4gICAgICAgICAgLmNhdGNoKCgpID0+ICh7IHR5cGU6ICdzZXNzaW9uJywgcmVzdWx0czogW10gfSkpXG4gICAgICApO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5UmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKHF1ZXJ5UHJvbWlzZXMpO1xuXG4gICAgY29uc3QgYWxsUmVzdWx0cyA9IFtdO1xuICAgIGZvciAoY29uc3QgeyB0eXBlLCByZXN1bHRzOiB0eXBlUmVzdWx0cyB9IG9mIHF1ZXJ5UmVzdWx0cykge1xuICAgICAgZm9yIChjb25zdCByZXN1bHQgb2YgdHlwZVJlc3VsdHMpIHtcbiAgICAgICAgYWxsUmVzdWx0cy5wdXNoKHsgLi4ucmVzdWx0LCBzb3VyY2VfdHlwZTogdHlwZSB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBhbGxSZXN1bHRzLnNvcnQoKGEsIGIpID0+IGIuc2NvcmUgLSBhLnNjb3JlKTtcbiAgICBjb25zdCB0b3BSZXN1bHRzID0gYWxsUmVzdWx0cy5zbGljZSgwLCB0b3BLKTtcbiAgICBjb25zdCBjb3ZlcmFnZSA9IGNhbGN1bGF0ZUNvdmVyYWdlKHRvcFJlc3VsdHMsIHRvcEspO1xuXG4gICAgcmV0dXJuIHsgcmVzdWx0czogdG9wUmVzdWx0cywgY292ZXJhZ2UsIHF1ZXJ5RW1iZWRkaW5nIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8vIENhbGwgdGhpcyBhZnRlciBhIHVzZXIgdXBsb2FkcyBhIGRvY3VtZW50IHRvIGEgc2Vzc2lvblxuLy8gc28gdGhlIG5leHQgcXVlcnkgZmV0Y2hlcyB0aGUgdXBkYXRlZCBjb2xsZWN0aW9uIGZyZXNoXG5leHBvcnQgZnVuY3Rpb24gaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUoc2Vzc2lvbklkKSB7XG4gIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cywgbWF4VG9rZW5zID0gNzAwMCkge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiAnJztcblxuICBsZXQgdG90YWxUb2tlbnMgPSAwO1xuICBjb25zdCBjb250ZXh0UGFydHMgPSBbXTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IHJlc3VsdHMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCByZXN1bHQgPSByZXN1bHRzW2ldO1xuICAgIGNvbnN0IHRva2VuRXN0aW1hdGUgPSByZXN1bHQudGV4dC5sZW5ndGggLyA0O1xuICAgIGlmICh0b3RhbFRva2VucyArIHRva2VuRXN0aW1hdGUgPiBtYXhUb2tlbnMpIGJyZWFrO1xuICAgIHRvdGFsVG9rZW5zICs9IHRva2VuRXN0aW1hdGU7XG5cbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQuc2xpY2UoMCwgMjAwKSArIChyZXN1bHQudGV4dC5sZW5ndGggPiAyMDAgPyAnLi4uJyA6ICcnKSxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG4vLyBSZWZ1c2Ugb25seSBpZiB0b3Agc2NvcmUgaXMgZ2VudWluZWx5IG5lYXItemVybyBcdTIwMTQgbm8gcmVsZXZhbnQgY29udGVudCBhdCBhbGxcbmV4cG9ydCBmdW5jdGlvbiBzaG91bGRTaG93UmVmdXNhbChjb3ZlcmFnZSkge1xuICByZXR1cm4gY292ZXJhZ2UudG9wU2NvcmUgPCBSRUZVU0FMX1RIUkVTSE9MRDtcbn1cblxuZXhwb3J0IHsgY2FsY3VsYXRlQ292ZXJhZ2UgfTsiLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBtZW1vcnlNYXAgPSBuZXcgTWFwKCk7XG5jb25zdCBERUZBVUxUX01FTU9SWV9XSU5ET1cgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCAxMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIGlmICghbWVtb3J5TWFwLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgbWVtb3J5TWFwLnNldChzZXNzaW9uSWQsIHtcbiAgICAgIHR1cm5zOiBbXSxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKVxuICAgIH0pO1xuICB9XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgbWV0YWRhdGEgPSB7fSkge1xuICBjb25zdCBtZW1vcnkgPSBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBtYXhUdXJucyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcblxuICBjb25zdCB0dXJuID0ge1xuICAgIGlkOiBgdHVybl8ke0RhdGUubm93KCl9XyR7TWF0aC5yYW5kb20oKS50b1N0cmluZygzNikuc3Vic3RyKDIsIDkpfWAsXG4gICAgcm9sZSxcbiAgICBjb250ZW50LFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICAuLi5tZXRhZGF0YVxuICB9O1xuXG4gIG1lbW9yeS50dXJucy5wdXNoKHR1cm4pO1xuXG4gIC8vIEtlZXAgb25seSB0aGUgbGFzdCBOIHR1cm5zXG4gIGlmIChtZW1vcnkudHVybnMubGVuZ3RoID4gbWF4VHVybnMpIHtcbiAgICBtZW1vcnkudHVybnMgPSBtZW1vcnkudHVybnMuc2xpY2UoLW1heFR1cm5zKTtcbiAgfVxuXG4gIHJldHVybiB0dXJuO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5KHNlc3Npb25JZCkge1xuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgbWF4VHVybnMgPSBudWxsKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBsaW1pdCA9IG1heFR1cm5zIHx8IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcblxuICByZXR1cm4gbWVtb3J5LnR1cm5zLnNsaWNlKC1saW1pdCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb252ZXJzYXRpb25Db250ZXh0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIHJldHVybiB0dXJucy5tYXAodCA9PiAoe1xuICAgIHJvbGU6IHQucm9sZSxcbiAgICBjb250ZW50OiB0LmNvbnRlbnRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIGlmICh0dXJucy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cblxuICBjb25zdCBmb3JtYXR0ZWQgPSB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xuXG4gIHJldHVybiBmb3JtYXR0ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhck1lbW9yeShzZXNzaW9uSWQpIHtcbiAgbWVtb3J5TWFwLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5U3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIHR1cm5Db3VudDogbWVtb3J5LnR1cm5zLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IG1lbW9yeS5jcmVhdGVkQXQsXG4gICAgbGFzdFR1cm5BdDogbWVtb3J5LnR1cm5zLmxlbmd0aCA+IDAgPyBtZW1vcnkudHVybnNbbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDFdLnRpbWVzdGFtcCA6IG51bGxcbiAgfTtcbn1cblxuLy8gRklYRUQgXHUyMDE0IGFuc3dlcklkIHN0b3JlZCBhcyB0dXJuLmlkLCBvdmVycmlkaW5nIHRoZSBhdXRvLWdlbmVyYXRlZCBvbmVcbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksICAvLyBvdmVycmlkZSB0dXJuIGlkIHNvIGdldFNvdXJjZXMgY2FuIG1hdGNoIGl0XG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtpbXBvcnQgeyBmb3JtYXRNZW1vcnlGb3JQcm9tcHQgfSBmcm9tICcuL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZm9ybWF0Q29udGV4dEZvclByb21wdCwgY2FsY3VsYXRlQ292ZXJhZ2UgfSBmcm9tICcuL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCBTWVNURU1fSU5TVFJVQ1RJT04gPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgYmFzZWQgb24gaW5kZXhlZCBkb2N1bWVudHMgd2hlbiBhdmFpbGFibGUuXG5cblJVTEVTOlxuMS4gV2hlbiBjb250ZXh0IGlzIHByb3ZpZGVkLCBhbnN3ZXIgYmFzZWQgb24gaXQgYW5kIGNpdGUgc291cmNlcyB1c2luZyBbMV0sIFsyXSwgZXRjLlxuMi4gRm9yIGdlbmVyYWwgY29udmVyc2F0aW9uIChncmVldGluZ3MsIGNsYXJpZnlpbmcgcXVlc3Rpb25zLCBzbWFsbCB0YWxrKSwgcmVzcG9uZCBuYXR1cmFsbHkgYW5kIGhlbHBmdWxseSB3aXRob3V0IHJlcXVpcmluZyBjb250ZXh0LlxuMy4gSWYgYSBmYWN0dWFsIHF1ZXN0aW9uIGlzIGFza2VkIGJ1dCBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkgYW5kIHN1Z2dlc3QgdXBsb2FkaW5nIHJlbGV2YW50IGRvY3VtZW50cy5cbjQuIEJlIGNvbmNpc2UgYnV0IHRob3JvdWdoLiBVc2UgYnVsbGV0IHBvaW50cyBvciBudW1iZXJlZCBsaXN0cyBmb3IgY29tcGxleCBhbnN3ZXJzLlxuNS4gTWFpbnRhaW4gY29udmVyc2F0aW9uIGNvbnRpbnVpdHkgYnV0IGRvbid0IHJlcGVhdCBpbmZvcm1hdGlvbiB1bm5lY2Vzc2FyaWx5LlxuNi4gRm9ybWF0IHJlc3BvbnNlcyBpbiBjbGVhciwgcmVhZGFibGUgbWFya2Rvd24uYDtcblxuY29uc3QgUkVGVVNBTF9NRVNTQUdFID0gXCJJIGRvbid0IGhhdmUgZW5vdWdoIGluZm9ybWF0aW9uIGluIHRoZSBrbm93bGVkZ2UgYmFzZSB0byBhbnN3ZXIgdGhhdCBjb25maWRlbnRseS4gVHJ5IHVwbG9hZGluZyByZWxldmFudCBkb2N1bWVudHMsIG9yIGFzayBtZSBhIGdlbmVyYWwgcXVlc3Rpb24uXCI7XG5leHBvcnQgZnVuY3Rpb24gYnVpbGRQcm9tcHQoeyBxdWVyeSwgY29udGV4dCwgbWVtb3J5Q29udGV4dCwgY292ZXJhZ2UgfSkge1xuICBjb25zdCBwYXJ0cyA9IFtdO1xuXG4gIC8vIFN5c3RlbSBpbnN0cnVjdGlvblxuICBwYXJ0cy5wdXNoKFNZU1RFTV9JTlNUUlVDVElPTik7XG5cbiAgLy8gUGFzdCBjb252ZXJzYXRpb24gaWYgYXZhaWxhYmxlXG4gIGlmIChtZW1vcnlDb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKG1lbW9yeUNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgUFJFVklPVVMgQ09OVkVSU0FUSU9OIC0tLVxcbicpO1xuICB9XG5cbiAgLy8gUmV0cmlldmVkIGNvbnRleHRcbiAgaWYgKGNvbnRleHQpIHtcbiAgICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gUkVMRVZBTlQgQ09OVEVYVCBGUk9NIEtOT1dMRURHRSBCQVNFIC0tLVxcbicpO1xuICAgIHBhcnRzLnB1c2goY29udGV4dCk7XG4gICAgcGFydHMucHVzaCgnXFxuLS0tIEVORCBDT05URVhUIC0tLVxcbicpO1xuICB9XG5cbiAgLy8gQ3VycmVudCBxdWVzdGlvblxuICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gQ1VSUkVOVCBRVUVTVElPTiAtLS1cXG4nKTtcbiAgcGFydHMucHVzaChxdWVyeSk7XG4gIHBhcnRzLnB1c2goJ1xcblxcblJlbWVtYmVyOiBBbnN3ZXIgYmFzZWQgT05MWSBvbiB0aGUgcHJvdmlkZWQgY29udGV4dC4gVXNlIFsxXSwgWzJdLCBldGMuIGZvciBjaXRhdGlvbnMuIElmIHRoZSBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkuJyk7XG5cbiAgcmV0dXJuIHBhcnRzLmpvaW4oJycpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRTdHJlYW1pbmdQcm9tcHQocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCk7XG4gIGNvbnN0IGNvbnRleHRTdHJpbmcgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJldHJpZXZlZFJlc3VsdHMpO1xuXG4gIHJldHVybiBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFN0cmluZyxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiBjYWxjdWxhdGVDb3ZlcmFnZShyZXRyaWV2ZWRSZXN1bHRzKVxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxSZXNwb25zZSgpIHtcbiAgcmV0dXJuIFJFRlVTQUxfTUVTU0FHRTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFN5c3RlbUluc3RydWN0aW9uKCkge1xuICByZXR1cm4gU1lTVEVNX0lOU1RSVUNUSU9OO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRXZWJTZWFyY2hQcm9tcHQocXVlcnksIGdyb3VuZGluZ01ldGFkYXRhKSB7XG4gIHJldHVybiBgQmFzZWQgb24gd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIGZvbGxvd2luZyBxdWVzdGlvbjogJHtxdWVyeX1cblxuR3VpZGVsaW5lczpcbi0gVXNlIGluZm9ybWF0aW9uIGZyb20gdGhlIHdlYiBzZWFyY2hcbi0gUHJvdmlkZSBzb3VyY2VzL1VSTHMgd2hlcmUgYXBwbGljYWJsZVxuLSBCZSBjb25jaXNlIGFuZCBpbmZvcm1hdGl2ZVxuLSBJZiBtdWx0aXBsZSBzb3VyY2VzIGFncmVlIG9yIGNvbnRyYWRpY3QsIG1lbnRpb24gdGhhdGA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRHZW5lcmF0aW9uQ29uZmlnKGN1c3RvbUNvbmZpZyA9IHt9KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICB0b3BQOiAwLjk1LFxuICAgIHRvcEs6IDQwLFxuICAgIG1heE91dHB1dFRva2VuczogMjA0OCxcbiAgICAuLi5jdXN0b21Db25maWdcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RTb3VyY2VzRnJvbVJlc3BvbnNlKHJlc3BvbnNlKSB7XG4gIC8vIEV4dHJhY3QgY2l0YXRpb24gcGF0dGVybnMgbGlrZSBbMV0sIFsyXSwgZXRjLlxuICBjb25zdCBjaXRhdGlvblBhdHRlcm4gPSAvXFxbKFxcZCspXFxdL2c7XG4gIGNvbnN0IGNpdGF0aW9ucyA9IG5ldyBTZXQoKTtcbiAgbGV0IG1hdGNoO1xuXG4gIHdoaWxlICgobWF0Y2ggPSBjaXRhdGlvblBhdHRlcm4uZXhlYyhyZXNwb25zZSkpICE9PSBudWxsKSB7XG4gICAgY2l0YXRpb25zLmFkZChwYXJzZUludChtYXRjaFsxXSkpO1xuICB9XG5cbiAgcmV0dXJuIEFycmF5LmZyb20oY2l0YXRpb25zKS5zb3J0KChhLCBiKSA9PiBhIC0gYik7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgYnVpbGRQcm9tcHQsIGdldFJlZnVzYWxSZXNwb25zZSB9IGZyb20gJy4vcHJvbXB0U2VydmljZS5qcyc7XG5pbXBvcnQgeyBMTE1VbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuLy8gXHUyNzA1IExhenkgXHUyMDE0IHJlYWQgaW5zaWRlIHRoZSBmdW5jdGlvbiwgbm90IGF0IG1vZHVsZSB0b3AgbGV2ZWxcbmxldCBnZW5BSSA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldEdlbkFJKCkge1xuICBpZiAoIWdlbkFJKSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVk7XG4gICAgaWYgKCFhcGlLZXkpIHRocm93IG5ldyBFcnJvcignR0VNSU5JX0FQSV9LRVkgaXMgdW5kZWZpbmVkJyk7XG4gICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKGFwaUtleSk7XG4gIH1cbiAgcmV0dXJuIGdlbkFJO1xufVxuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5jb25zdCBGQUxMQkFDS19NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9GQUxMQkFDSyB8fCAnZ2VtaW5pLTIuNS1mbGFzaCc7XG5jb25zdCBGSVJTVF9UT0tFTl9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX0ZJUlNUX1RPS0VOX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDEyMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX1JFUVVFU1RfVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgNDUwMDA7XG5cbmxldCBwcmltYXJ5TW9kZWwgPSBudWxsO1xubGV0IGZhbGxiYWNrTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRQcmltYXJ5TW9kZWwoKSB7XG4gIGlmICghcHJpbWFyeU1vZGVsKSB7XG4gICAgcHJpbWFyeU1vZGVsID0gZ2V0R2VuQUkoKS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogUFJJTUFSWV9NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gcHJpbWFyeU1vZGVsO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsKCkge1xuICBpZiAoIWZhbGxiYWNrTW9kZWwpIHtcbiAgICBmYWxsYmFja01vZGVsID0gZ2V0R2VuQUkoKS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogRkFMTEJBQ0tfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIGZhbGxiYWNrTW9kZWw7XG59XG5cbmFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlV2l0aE1vZGVsKG1vZGVsLCBwcm9tcHQsIHNpZ25hbCkge1xuICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9XG4gIH0sIHsgc2lnbmFsIH0pO1xuXG4gIHJldHVybiByZXN1bHQucmVzcG9uc2UudGV4dCgpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVSZXNwb25zZShwcm9tcHQpIHtcbiAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcbiAgY29uc3QgdGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRQcmltYXJ5TW9kZWwoKS5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICByZXR1cm4gcmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbiAgfSBjYXRjaCAocHJpbWFyeUVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUHJpbWFyeSBtb2RlbCBmYWlsZWQ6JywgcHJpbWFyeUVycm9yLm1lc3NhZ2UpO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGZhbGxiYWNrUmVzdWx0ID0gYXdhaXQgZ2V0RmFsbGJhY2tNb2RlbCgpLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICAgIHJldHVybiBmYWxsYmFja1Jlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG4gICAgfSBjYXRjaCAoZmFsbGJhY2tFcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFsbGJhY2sgbW9kZWwgYWxzbyBmYWlsZWQ6JywgZmFsbGJhY2tFcnJvci5tZXNzYWdlKTtcbiAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbCA9IGdldFByaW1hcnlNb2RlbCgpO1xuICBsZXQgcmV0cmllcyA9IDA7XG4gIGNvbnN0IG1heFJldHJpZXMgPSAyO1xuXG4gIHdoaWxlIChyZXRyaWVzIDwgbWF4UmV0cmllcykge1xuICAgIHRyeSB7XG4gICAgICAvLyBcdTI3MDUgRklYOiBDcmVhdGUgQWJvcnRDb250cm9sbGVyIHBlciBhdHRlbXB0IGZvciB0aW1lb3V0IHNpZ25hbGxpbmdcbiAgICAgIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG5cbiAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudFN0cmVhbSh7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBsZXQgZmlyc3RUb2tlbiA9IHRydWU7XG5cbiAgICAgIC8vIFx1MjcwNSBGSVg6IFVzZSBjb250cm9sbGVyLmFib3J0KCkgaW5zdGVhZCBvZiB0aHJvdyBpbnNpZGUgc2V0VGltZW91dFxuICAgICAgLy8gKHRocm93IGluc2lkZSBzZXRUaW1lb3V0IGlzIHVuY2F1Z2h0IGFuZCBzaWxlbnRseSBraWxscyB0aGUgc3RyZWFtKVxuICAgICAgY29uc3QgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgICAvLyBcdTI3MDUgRklYOiBDaGVjayBhYm9ydCBzaWduYWwgb24gZWFjaCBpdGVyYXRpb25cbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignRmlyc3QgdG9rZW4gdGltZW91dCBcdTIwMTQgbm8gcmVzcG9uc2UgZnJvbSBtb2RlbCcpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgICBpZiAoZmlyc3RUb2tlbikge1xuICAgICAgICAgICAgZmlyc3RUb2tlbiA9IGZhbHNlO1xuICAgICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTsgLy8gZ290IGZpcnN0IHRva2VuLCBjYW5jZWwgdGltZW91dFxuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICAvLyBTdHJlYW0gY29tcGxldGVkIG5hdHVyYWxseSBcdTIwMTQgY2xlYW4gdXAgdGltZW91dFxuICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgIHJldHVybiB7IHN1Y2Nlc3M6IHRydWUgfTtcblxuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICByZXRyaWVzKys7XG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgLy8gU3dpdGNoIHRvIGZhbGxiYWNrIG1vZGVsIG9uIHJldHJ5XG4gICAgICBtb2RlbCA9IGdldEZhbGxiYWNrTW9kZWwoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1DaGF0UmVzcG9uc2UocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gbWVtb3J5U2VydmljZSA/IG1lbW9yeVNlcnZpY2UuZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkgOiAnJztcbiAgY29uc3QgY29udGV4dExpc3QgPSByZXRyaWV2ZWRSZXN1bHRzIHx8IFtdO1xuICBjb25zdCBjb250ZXh0VGV4dCA9IGNvbnRleHRMaXN0Lm1hcCgociwgaSkgPT5cbiAgICBgWyR7aSArIDF9XSAke3IubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfTogJHtyLnRleHR9YFxuICApLmpvaW4oJ1xcblxcbicpO1xuXG4gIGNvbnN0IHByb21wdCA9IGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0VGV4dCxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiB7IGxldmVsOiAnaGlnaCcgfVxuICB9KTtcblxuICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgdHJ5IHtcbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHsgdHlwZTogJ2NvbXBsZXRlJywgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFRleHQoKSB7XG4gIHJldHVybiBnZXRSZWZ1c2FsUmVzcG9uc2UoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlV2ViU2VhcmNoUmVzcG9uc2UocXVlcnksIGdyb3VuZGluZ0NvbnRlbnQpIHtcbiAgY29uc3QgbW9kZWwgPSBnZXRQcmltYXJ5TW9kZWwoKTtcblxuICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgIGNvbnRlbnRzOiBbe1xuICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgcGFydHM6IFt7IHRleHQ6IGBCYXNlZCBvbiB0aGVzZSB3ZWIgc2VhcmNoIHJlc3VsdHMsIGFuc3dlciB0aGUgcXVlc3Rpb246IFwiJHtxdWVyeX1cIlxcblxcbiR7Z3JvdW5kaW5nQ29udGVudH1gIH1dXG4gICAgfV0sXG4gICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9LFxuICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gIH0pO1xuXG4gIGNvbnN0IHJlc3BvbnNlID0gcmVzdWx0LnJlc3BvbnNlO1xuICBjb25zdCB0ZXh0ID0gcmVzcG9uc2UudGV4dCgpO1xuICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgcmV0dXJuIHtcbiAgICB0ZXh0LFxuICAgIGdyb3VuZGluZ01ldGFkYXRhLFxuICAgIGdyb3VuZGluZ0NodW5rczogZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcyB8fCBbXVxuICB9O1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgc2hvdWxkU2hvd1JlZnVzYWwsIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlLCBnZXRSZWZ1c2FsVGV4dCB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zIH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24gfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlQ2hhdFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5LCBzZXNzaW9uSWQ6IHByb3ZpZGVkU2Vzc2lvbklkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLCBjb2RlOiAnTUlTU0lOR19RVUVSWScgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgPSB1dWlkdjQoKTtcblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICAvLyBjb3ZlcmFnZSBpcyBub3cgeyBjb25maWRlbmNlOiA0NywgdG9wU2NvcmU6IDAuNDcgfVxuICAgIHNlbmRFdmVudCgncmV0cmlldmFsJywge1xuICAgICAgcmVzdWx0czogcmVzdWx0cy5sZW5ndGgsXG4gICAgICBjb25maWRlbmNlOiBjb3ZlcmFnZS5jb25maWRlbmNlICAgLy8gZS5nLiA0NyAgXHUyMTkyIGRpc3BsYXkgYXMgXCI0NyUgY29uZmlkZW5jZVwiXG4gICAgfSk7XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBjb25zdCBzb3VyY2VzID0gcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgY2h1bmtJZDogci5pZCxcbiAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgIHBhZ2VOdW1iZXI6IHIubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgICBleGNlcnB0OiByLnRleHQuc2xpY2UoMCwgMjAwKSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIGlmIChzaG91bGRTaG93UmVmdXNhbChjb3ZlcmFnZSkpIHtcbiAgICAgIGNvbnN0IHJlZnVzYWxUZXh0ID0gZ2V0UmVmdXNhbFRleHQoKTtcbiAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ2Fzc2lzdGFudCcsIHJlZnVzYWxUZXh0LCBjaXRhdGlvbnMsIGNvdmVyYWdlLCBhbnN3ZXJJZCk7XG5cbiAgICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICAgIGFuc3dlcklkLFxuICAgICAgICByZXNwb25zZTogcmVmdXNhbFRleHQsXG4gICAgICAgIGNpdGF0aW9ucyxcbiAgICAgICAgY292ZXJhZ2UsXG4gICAgICAgIHNvdXJjZXMsXG4gICAgICAgIGFjdGlvbjogJ3JlZnVzYWwnXG4gICAgICB9KTtcblxuICAgICAgcmVzLmVuZCgpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICAvLyBVc2VzIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgXHUyMDE0IGluY2x1ZGVzIFtTZWVkIERvY3VtZW50XSBsYWJlbHMgKyBwYWdlIG51bWJlcnNcbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCA1KVxuICAgICAgLm1hcCh0ID0+IGAke3Qucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXInIDogJ0Fzc2lzdGFudCd9OiAke3QuY29udGVudH1gKVxuICAgICAgLmpvaW4oJ1xcblxcbicpO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIFxcXG5JZiB0aGUgY29udGV4dCBjb250YWlucyByZWxldmFudCBpbmZvcm1hdGlvbiwgdXNlIGl0IHRvIGFuc3dlciBjbGVhcmx5IGFuZCBjaXRlIHNvdXJjZXMgd2l0aCBbMV0sIFsyXSBldGMuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0fVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dH1cblxuQ1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1cblxuQW5zd2VyIGNvbmNpc2VseSB3aXRoIGNpdGF0aW9ucyBsaWtlIFsxXSwgWzJdIHJlZmVycmluZyB0byBjb250ZXh0IG51bWJlcnMuYDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ2Fzc2lzdGFudCcsIGZ1bGxSZXNwb25zZSwgY2l0YXRpb25zLCBjb3ZlcmFnZSwgYW5zd2VySWQpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHsgYW5zd2VySWQsIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsIGNpdGF0aW9ucywgY292ZXJhZ2UsIHNvdXJjZXMgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnQW4gZXJyb3Igb2NjdXJyZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNvdXJjZXMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDIwKTtcblxuICAvLyBTdHJpY3QgbWF0Y2ggYnkgYW5zd2VySWQgZmlyc3RcbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIC8vIEZhbGxiYWNrOiBtb3N0IHJlY2VudCBhc3Npc3RhbnQgdHVybiB3aXRoIGNpdGF0aW9uc1xuICBjb25zdCBmYWxsYmFjayA9IFsuLi5yZWNlbnRUdXJuc10ucmV2ZXJzZSgpLmZpbmQodCA9PlxuICAgIHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcgJiYgdC5jaXRhdGlvbnM/Lmxlbmd0aCA+IDBcbiAgKTtcblxuICBpZiAoZmFsbGJhY2spIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG4gIH1cblxuICByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnU291cmNlcyBub3QgZm91bmQnLCBjb2RlOiAnU09VUkNFU19OT1RfRk9VTkQnIH0pO1xufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZUNoYXRTdHJlYW0pO1xucm91dGVyLmdldCgnL3NvdXJjZXMvOmFuc3dlcklkJywgZ2V0U291cmNlcyk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjsiLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuY29uc3QgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZKTtcblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuXG5sZXQgbW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRNb2RlbCgpIHtcbiAgaWYgKCFtb2RlbCkge1xuICAgIG1vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIG1vZGVsO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcGVyZm9ybVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICAvLyBFeHRyYWN0IHNlYXJjaCBxdWVyaWVzIGFuZCBzb3VyY2VzXG4gICAgY29uc3Qgd2ViU2VhcmNoUXVlcmllcyA9IFtdO1xuICAgIGNvbnN0IHdlYlNvdXJjZXMgPSBbXTtcblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGNodW5rIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoY2h1bmsud2ViKSB7XG4gICAgICAgICAgd2ViU291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogY2h1bmsud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBjaHVuay53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8ud2ViU2VhcmNoUXVlcmllcykge1xuICAgICAgd2ViU2VhcmNoUXVlcmllcy5wdXNoKC4uLmdyb3VuZGluZ01ldGFkYXRhLndlYlNlYXJjaFF1ZXJpZXMpO1xuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICB0ZXh0LFxuICAgICAgc291cmNlczogd2ViU291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHdlYlNlYXJjaFF1ZXJpZXMsXG4gICAgICByYXdNZXRhZGF0YTogZ3JvdW5kaW5nTWV0YWRhdGFcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSB0ZXh0O1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlPy5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgY29uc3Qgc291cmNlcyA9IFtdO1xuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChpdGVtLndlYikge1xuICAgICAgICAgIHNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGl0ZW0ud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBpdGVtLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQge1xuICAgICAgdHlwZTogJ2NvbXBsZXRlJyxcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbWluZyBlcnJvcjonLCBlcnJvcik7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdFdlYlNlYXJjaFJlc3BvbnNlKHJlc3VsdCkge1xuICByZXR1cm4ge1xuICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMubWFwKHMgPT4gKHtcbiAgICAgIHVyaTogcy51cmksXG4gICAgICB0aXRsZTogcy50aXRsZSxcbiAgICAgIHR5cGU6ICd3ZWInXG4gICAgfSkpLFxuICAgIHF1ZXJpZXNVc2VkOiByZXN1bHQucXVlcmllcyxcbiAgICBtZXRhZGF0YToge1xuICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9XG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBwZXJmb3JtV2ViU2VhcmNoLCBzdHJlYW1XZWJTZWFyY2ggfSBmcm9tICcuLi9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2gocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgcGVyZm9ybVdlYlNlYXJjaChxdWVyeS50cmltKCkpO1xuXG4gICAgcmVzLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHJlc3VsdC5xdWVyaWVzLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgICAgfVxuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDMpLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggdW5hdmFpbGFibGUnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoU3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICAvLyBTZXQgdXAgU1NFXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnc2VhcmNoaW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyB0aGUgd2ViLi4uJyB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcbiAgICBsZXQgc291cmNlcyA9IFtdO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1XZWJTZWFyY2gocXVlcnkudHJpbSgpKSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdXRUJfU0VBUkNIX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgICAgc291cmNlcyA9IGNodW5rLnNvdXJjZXMgfHwgW107XG4gICAgICB9XG4gICAgfVxuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywge1xuICAgICAgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCBmYWlsZWQnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVXZWJTZWFyY2gpO1xucm91dGVyLnBvc3QoJy9zdHJlYW0nLCBoYW5kbGVXZWJTZWFyY2hTdHJlYW0pO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHNlYXJjaFJvdXRlciBmcm9tICcuL2FwaS9zZWFyY2guanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcblxuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBST1VURVJTXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zb2xlLmxvZygnTW91bnRpbmcgcm91dGVycy4uLicpO1xuXG5hcHAudXNlKCcvaGVhbHRoJywgaGVhbHRoUm91dGVyKTtcbmFwcC51c2UoJy9kb2N1bWVudHMnLCBkb2N1bWVudHNSb3V0ZXIpO1xuYXBwLnVzZSgnL2NoYXQnLCBjaGF0Um91dGVyKTtcbmFwcC51c2UoJy9mZWVkYmFjaycsIGZlZWRiYWNrUm91dGVyKTtcbmFwcC51c2UoJy9zZWFyY2gnLCBzZWFyY2hSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuXG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxtQkFBbUI7QUFDelMsU0FBUyxNQUFNLGNBQWM7QUFNN0IsU0FBUyxpQkFBaUI7QUFDeEIsTUFBSSxDQUFDLGFBQWE7QUFDaEIsVUFBTSxTQUFTLFFBQVEsSUFBSTtBQUMzQixVQUFNLFNBQVMsUUFBUSxJQUFJLGlCQUFpQjtBQUM1QyxVQUFNLFdBQVcsUUFBUSxJQUFJLG1CQUFtQjtBQUNoRCxVQUFNLE9BQU8sUUFBUSxJQUFJLGVBQWU7QUFFeEMsWUFBUSxJQUFJLHFDQUFxQztBQUNqRCxZQUFRLElBQUksZUFBZSxRQUFRLDZCQUE2QjtBQUNoRSxZQUFRLElBQUksZUFBZSxNQUFNO0FBQ2pDLFlBQVEsSUFBSSxlQUFlLFFBQVE7QUFDbkMsWUFBUSxJQUFJLGVBQWUsU0FBUyxtQkFBbUIscUJBQXFCO0FBQzVFLFlBQVEsSUFBSSxxQ0FBcUM7QUFFakQsUUFBSSxDQUFDLFFBQVE7QUFDWCxZQUFNLElB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QUEsSUFDSCxNQUFNO0FBQUEsSUFDTixpQkFBaUIsU0FBVSxRQUFRO0FBQy9CLGFBQU8sVUFBVSxNQUFNLFFBQVEsUUFBUSxXQUFZO0FBQy9DLFlBQUk7QUFDSixlQUFPLFlBQVksTUFBTSxTQUFVLElBQUk7QUFDbkMsa0JBQVEsR0FBRyxPQUFPO0FBQUEsWUFDZCxLQUFLO0FBQUcscUJBQU8sQ0FBQyxHQUFhLHVEQUF5QjtBQUFBLFlBQ3RELEtBQUs7QUFDRCwyQkFBYyxHQUFHLEtBQUssRUFBRztBQUN6QixjQUFBQSxPQUFNO0FBQ04scUJBQU8sWUFBWSxJQUFJLFFBQVEsU0FBVSxLQUFLLEtBQUssTUFBTTtBQUNyRCxnQkFBQUEsS0FBSSxLQUFLLEtBQUssSUFBSTtBQUFBLGNBQ3RCLENBQUM7QUFDRCxxQkFBTztBQUFBLGdCQUFDO0FBQUE7QUFBQSxjQUFZO0FBQUEsVUFDNUI7QUFBQSxRQUNKLENBQUM7QUFBQSxNQUNMLENBQUM7QUFBQSxJQUNMO0FBQUEsRUFDSjtBQUNKO0FBQ0EsSUFBTyxzQkFBUSxhQUFhO0FBQUEsRUFDeEIsU0FBUyxDQUFDLE1BQU0sR0FBRyxjQUFjLENBQUM7QUFBQSxFQUNsQyxTQUFTO0FBQUEsSUFDTCxPQUFPO0FBQUEsTUFDSCxLQUFLRCxNQUFLLFFBQVEsV0FBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbIm1vZGVsIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgIlJvdXRlciIsICJwYXRoIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgInJvdXRlciIsICJ1dWlkdjQiLCAiZ2xvYmFsQ29sbGVjdGlvbiIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAiZ2VuQUkiLCAibW9kZWwiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuZXJhdGl2ZUFJIiwgImdlbkFJIiwgIlBSSU1BUllfTU9ERUwiLCAibW9kZWwiLCAiUm91dGVyIiwgInJvdXRlciIsICJwYXRoIiwgInBhdGgiLCAiYXBwIl0KfQo=
