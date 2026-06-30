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
import { fileURLToPath } from "url";
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
      return res.status(422).json({ error: "No content could be extracted from PDF", code: "EMPTY_PDF" });
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
      return res.status(503).json({ error: "Failed to generate embeddings", code: "EMBEDDING_FAILED" });
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
  const filename = req.query.filename;
  try {
    const uploadPath = path2.join(uploadDir, `${documentId}.pdf`);
    if (fs.existsSync(uploadPath)) {
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Disposition", `inline; filename="${documentId}.pdf"`);
      return fs.createReadStream(uploadPath).pipe(res);
    }
    if (filename) {
      const seedPath = path2.join(seedDir, filename);
      if (fs.existsSync(seedPath)) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", `inline; filename="${filename}"`);
        return fs.createReadStream(seedPath).pipe(res);
      }
    }
    const allPdfs = fs.readdirSync(seedDir).filter((f) => f.endsWith(".pdf"));
    const match = allPdfs.find((f) => filename && f.includes(path2.parse(filename).name));
    if (match) {
      const matchPath = path2.join(seedDir, match);
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Disposition", `inline; filename="${match}"`);
      return fs.createReadStream(matchPath).pipe(res);
    }
    return res.status(404).json({ error: "Document file not found", code: "FILE_NOT_FOUND" });
  } catch (error) {
    console.error("Get document file error:", error);
    res.status(500).json({ error: "Failed to retrieve document", code: "RETRIEVE_ERROR" });
  }
}
var __vite_injected_original_import_meta_url, router2, __filename, __dirname, uploadDir, seedDir, storage, upload, documents_default;
var init_documents = __esm({
  "server/api/documents.js"() {
    "use strict";
    init_sanitize();
    init_errors();
    init_chromaService();
    init_chunker();
    init_embeddingService();
    init_sessionService();
    __vite_injected_original_import_meta_url = "file:///home/project/server/api/documents.js";
    router2 = Router2();
    __filename = fileURLToPath(__vite_injected_original_import_meta_url);
    __dirname = path2.dirname(__filename);
    uploadDir = "/tmp/uploads";
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    seedDir = path2.resolve(__dirname, "../../");
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
function cleanExcerpt(text) {
  return text.replace(
    /(?<!\w)([A-Za-z])\s([A-Za-z])\s([A-Za-z])(\s[A-Za-z])*/g,
    (match) => match.replace(/\s/g, "")
  ).replace(/\s{2,}/g, " ").replace(/^\*\s*/, "").trim();
}
function expandQuery(query, sessionId) {
  const words = query.trim().split(/\s+/);
  if (words.length > 4) return query;
  const recentTurns = getRecentTurns(sessionId, 4);
  const recentContext = recentTurns.filter((t) => t.role === "user").map((t) => t.content).join(" ");
  const expansions = [
    "definition",
    "overview",
    "role",
    "responsibilities",
    "examples",
    "key concepts",
    "how it works",
    "purpose"
  ];
  const queryWords = query.toLowerCase().split(/\s+/);
  const contextRelevant = queryWords.some(
    (w) => w.length > 3 && recentContext.toLowerCase().includes(w)
  );
  const domainHint = contextRelevant ? `${recentContext.slice(0, 80)}: ` : "";
  return `${domainHint}${query} ${expansions.join(" ")}`;
}
async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({ error: "Query is required", code: "MISSING_QUERY" });
  }
  const sessionId = providedSessionId || uuidv45();
  const answerId = uuidv45();
  getOrCreateSession(sessionId);
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
  addTurnWithCitations(sessionId, "user", query.trim());
  try {
    sendEvent("status", { stage: "retrieving", message: "Searching knowledge base..." });
    const expandedQuery = expandQuery(query, sessionId);
    const { results, coverage } = await retrieveForQuery(expandedQuery, sessionId, { topK: 5 });
    sendEvent("retrieval", {
      results: results.length,
      confidence: coverage.confidence,
      topScore: coverage.topScore
    });
    const citations = generateCitations(results);
    const sources = results.map((r) => ({
      chunkId: r.id,
      documentId: r.metadata.document_id,
      filename: r.metadata.filename,
      pageNumber: r.metadata.page_number,
      excerpt: cleanExcerpt(r.text.slice(0, 200)),
      score: r.score,
      sourceType: r.source_type
    }));
    if (shouldShowRefusal(coverage)) {
      const refusalText = getRefusalText ? getRefusalText() : "I don't have information on that topic in my knowledge base.";
      addTurnWithCitations(sessionId, "assistant", refusalText, [], coverage, answerId);
      sendEvent("complete", {
        answerId,
        response: refusalText,
        citations: [],
        coverage,
        sources: [],
        action: "refusal"
      });
      res.end();
      return;
    }
    sendEvent("status", { stage: "generating", message: "Generating response..." });
    const contextText = formatContextForPrompt(results);
    const memoryContext = getRecentTurns(sessionId, 5).map((t) => `${t.role === "user" ? "User" : "Assistant"}: ${t.content}`).join("\n\n");
    const prompt = `You are an AI Knowledge Assistant. Answer ONLY using the numbered context below.
If the answer is not in the context, politely say you don't have information on that topic in your knowledge base. Do NOT answer from general knowledge. Do NOT add citations if the context is not relevant.

CONTEXT:
${contextText}

CONVERSATION HISTORY:
${memoryContext}

CURRENT QUESTION: ${query}

Rules:
- Cite sources inline as [1] [2] only for context chunks you actually used
- Always cite as separate brackets [1] [2], never as [1, 2]
- Never add citation numbers you did not use
- For greetings or small talk, respond naturally and briefly \u2014 do NOT mention the knowledge base topic, do NOT reference any documents, do NOT add citations
- If context is irrelevant, decline politely with no citations`;
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
    const citedIndices = [...fullResponse.matchAll(/\[(\d+(?:,\s*\d+)*)\]/g)].flatMap((m) => m[1].split(",").map((n) => parseInt(n.trim()))).filter((v, i, a) => a.indexOf(v) === i);
    const isOutOfScope = OUT_OF_SCOPE_PATTERN.test(fullResponse);
    const finalCitations = isOutOfScope || citedIndices.length === 0 ? [] : citations.filter((c) => citedIndices.includes(c.index)).map((c, i) => ({ ...c, index: i + 1 }));
    const finalSources = isOutOfScope || citedIndices.length === 0 ? [] : sources.filter((_, i) => citedIndices.includes(i + 1));
    addTurnWithCitations(sessionId, "assistant", fullResponse, finalCitations, coverage, answerId);
    sendEvent("complete", {
      answerId,
      response: fullResponse,
      citations: finalCitations,
      coverage,
      sources: finalSources
    });
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
var router3, OUT_OF_SCOPE_PATTERN, chat_default;
var init_chat = __esm({
  "server/api/chat.js"() {
    "use strict";
    init_retrievalService();
    init_geminiService();
    init_memoryService();
    init_sessionService();
    router3 = Router3();
    OUT_OF_SCOPE_PATTERN = /don't have information|do not have information|not in my knowledge|can't find|cannot find|no information|knowledge base doesn't|not covered|outside.*knowledge/i;
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
import { fileURLToPath as fileURLToPath2 } from "url";
var __vite_injected_original_import_meta_url2 = "file:///home/project/vite.config.js";
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
var __dirname2 = path3.dirname(fileURLToPath2(__vite_injected_original_import_meta_url2));
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
      "@": path3.resolve(__dirname2, "./src")
    }
  },
  server: {
    port: 5173
  }
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcblxuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuLy8gQWxpYXMgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbmZ1bmN0aW9uIGdldENsaWVudCgpIHtcbiAgcmV0dXJuIGdldENsb3VkQ2xpZW50KCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IEdsb2JhbCBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY29ubmVjdCB0byBnbG9iYWwgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGdsb2JhbENvbGxlY3Rpb247XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBzZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBgc2Vzc2lvbl8ke3Nlc3Npb25JZH1gO1xuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gY3JlYXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiBkZWxldGVkOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBkZWxldGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgaWRzLFxuICAgICAgZW1iZWRkaW5ncyxcbiAgICAgIGRvY3VtZW50czogdmVjdG9ycy5tYXAodiA9PiB2LnRleHQpLFxuICAgICAgbWV0YWRhdGFzOiB2ZWN0b3JzLm1hcCh2ID0+IHYubWV0YWRhdGEpXG4gICAgfSk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9XG4gICAgfSk7XG4gICAgaWYgKGV4aXN0aW5nLmlkcyAmJiBleGlzdGluZy5pZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGV4aXN0aW5nLmlkcyB9KTtcbiAgICAgIHJldHVybiBleGlzdGluZy5pZHMubGVuZ3RoO1xuICAgIH1cbiAgICByZXR1cm4gMDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudENvdW50KGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICByZXR1cm4gYXdhaXQgY29sbGVjdGlvbi5jb3VudCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBnZXQgZG9jdW1lbnQgY291bnQ6JywgZXJyb3IpO1xuICAgIHJldHVybiAwO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxJdGVtcyA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddXG4gICAgfSk7XG5cbiAgICBjb25zdCBkb2N1bWVudHNNYXAgPSBuZXcgTWFwKCk7XG5cbiAgICBpZiAoYWxsSXRlbXMuaWRzKSB7XG4gICAgICBhbGxJdGVtcy5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYWxsSXRlbXMubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYWxsSXRlbXMuZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBjbGVhbnVwU2Vzc2lvbkNvbGxlY3Rpb25zKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbnMgPSBhd2FpdCBjbGllbnQubGlzdENvbGxlY3Rpb25zKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uQ29sbGVjdGlvbk5hbWVzID0gY29sbGVjdGlvbnMuZmlsdGVyKGMgPT4gYy5zdGFydHNXaXRoKCdzZXNzaW9uXycpKTtcblxuICAgIGlmIChzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjcwNSBObyBzdGFsZSBzZXNzaW9uIGNvbGxlY3Rpb25zIGZvdW5kLicpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBcdUQ4M0VcdURERjkgQ2xlYW5pbmcgdXAgJHtzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLmxlbmd0aH0gc3RhbGUgc2Vzc2lvbiBjb2xsZWN0aW9uKHMpLi4uYCk7XG5cbiAgICBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBzZXNzaW9uQ29sbGVjdGlvbk5hbWVzLm1hcChhc3luYyBjb2xsZWN0aW9uTmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lOiBjb2xsZWN0aW9uTmFtZSB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgICBcdTI3MDUgRGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGAgIFx1MjZBMFx1RkUwRiBDb3VsZCBub3QgZGVsZXRlICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY2xlYW51cCBjb21wbGV0ZS4nKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLndhcm4oJ1x1MjZBMFx1RkUwRiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdXZWIgc2VhcmNoIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1dFQl9TRUFSQ0hfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xubGV0IGVtYmVkZGluZ01vZGVsID0gbnVsbDtcblxuLy8gRml4IDEwOiBMYXp5IGluaXQgXHUyMDE0IGF2b2lkcyBkb3RlbnYgdGltaW5nIGJ1Z1xuZnVuY3Rpb24gZ2V0RW1iZWRkaW5nTW9kZWwoKSB7XG4gIGlmICghZW1iZWRkaW5nTW9kZWwpIHtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkocHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkpO1xuICAgIGVtYmVkZGluZ01vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHtcbiAgICAgIG1vZGVsOiBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZW1iZWRkaW5nTW9kZWw7XG59XG5cbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0O1xuY29uc3QgT1VUUFVUX0RJTUVOU0lPTlMgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7IC8vIEZpeCA5XG5jb25zdCBHUk9VUF9XQUlUX01TID0gNjEwMDA7XG5cbi8vIEZpeCAxMSsxMjogVXNlIGJhdGNoRW1iZWRDb250ZW50cyBcdTIwMTQgb25lIEFQSSBjYWxsIHJldHVybnMgTiB2ZWN0b3JzIChvbmUgcGVyIHRleHQpXG4vLyBGaXggMTM6IHRhc2tUeXBlIHBhc3NlZCBwZXIgcmVxdWVzdFxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbWF4QXR0ZW1wdHMgPSA1O1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldEVtYmVkZGluZ01vZGVsKCk7XG5cbiAgICAvLyBGaXggMTI6IENvcnJlY3QgQVBJIGZvcm1hdCB1c2luZyBiYXRjaEVtYmVkQ29udGVudHNcbiAgICAvLyBGaXggOTogb3V0cHV0RGltZW5zaW9uYWxpdHkgZXhwbGljaXRseSBzZXRcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5iYXRjaEVtYmVkQ29udGVudHMoe1xuICAgICAgcmVxdWVzdHM6IHRleHRzLm1hcCh0ZXh0ID0+ICh7XG4gICAgICAgIG1vZGVsOiBgbW9kZWxzLyR7bW9kZWxOYW1lfWAsXG4gICAgICAgIGNvbnRlbnQ6IHsgcGFydHM6IFt7IHRleHQgfV0gfSxcbiAgICAgICAgdGFza1R5cGUsICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gRml4IDEzXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBPVVRQVVRfRElNRU5TSU9OUygpICAgIC8vIEZpeCA5XG4gICAgICB9KSlcbiAgICB9KTtcblxuICAgIGlmICghcmVzdWx0Py5lbWJlZGRpbmdzIHx8IHJlc3VsdC5lbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtyZXN1bHQ/LmVtYmVkZGluZ3M/Lmxlbmd0aCA/PyAwfWApO1xuICAgIH1cblxuICAgIC8vIFJldHVybnMgYXJyYXkgb2YgdmVjdG9ycyBcdTIwMTQgb25lIHBlciBpbnB1dCB0ZXh0IFx1MjcwNVxuICAgIHJldHVybiByZXN1bHQuZW1iZWRkaW5ncy5tYXAoZSA9PiB7XG4gICAgICBpZiAoIWU/LnZhbHVlcykgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdNaXNzaW5nIHZhbHVlcyBpbiBlbWJlZGRpbmcgcmVzcG9uc2UnKTtcbiAgICAgIHJldHVybiBlLnZhbHVlcztcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzNDI5ID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKTtcblxuICAgIGlmIChpczQyOSAmJiBhdHRlbXB0IDwgbWF4QXR0ZW1wdHMpIHtcbiAgICAgIGNvbnN0IHJldHJ5RGVsYXkgPSBlcnJvci5yZXRyeUFmdGVyIHx8IEdST1VQX1dBSVRfTVM7XG4gICAgICBjb25zb2xlLmxvZyhgUmF0ZSBsaW1pdGVkLCB3YWl0aW5nICR7cmV0cnlEZWxheSAvIDEwMDB9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9KWApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gU3RyYXRlZ3k6XG4vLyAgIC0gU3BsaXQgY2h1bmtzIGludG8gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIChkZWZhdWx0IDcpIFx1MjE5MiAxIEFQSSBjYWxsIHBlciBiYXRjaCBcdTIxOTIgNyB2ZWN0b3JzIGJhY2tcbi8vICAgLSBGaXJlIFBBUkFMTEVMX0NBTExTIChkZWZhdWx0IDQpIGJhdGNoZXMgc2ltdWx0YW5lb3VzbHkgXHUyMTkyIDI4IGNodW5rcyBwZXIgZ3JvdXBcbi8vICAgLSBXYWl0IDYxcyBiZXR3ZWVuIGdyb3VwcyB0byByZXNwZWN0IHJhdGUgbGltaXRzXG4vL1xuLy8gRm9yIDQ4IGNodW5rczogY2VpbCg0OC83KSA9IDcgYmF0Y2hlcyBcdTIxOTIgMiBwYXJhbGxlbCBncm91cHMgKDQrMykgXHUyMTkyIH4yIG1pbnV0ZXMgdG90YWxcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZUVtYmVkZGluZ3MoY2h1bmtzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBvblByb2dyZXNzKSB7XG4gIGlmICghY2h1bmtzIHx8IGNodW5rcy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICBjb25zdCBiYXRjaFNpemUgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuICBjb25zdCBlbWJlZGRpbmdzID0gW107XG5cbiAgLy8gU3BsaXQgY2h1bmtzIGludG8gYmF0Y2hlcyBvZiBiYXRjaFNpemVcbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSk7XG4gIH1cblxuICAvLyBHcm91cCBiYXRjaGVzIGludG8gcGFyYWxsZWwgc2V0cyBvZiBwYXJhbGxlbENhbGxzXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG4gICAgY29uc3QgY2h1bmtzQ292ZXJlZCA9IE1hdGgubWluKChpICsgcGFyYWxsZWxDYWxscykgKiBiYXRjaFNpemUsIGNodW5rcy5sZW5ndGgpO1xuXG4gICAgY29uc29sZS5sb2coYCAgRW1iZWRkaW5nIGdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCAke3BhcmFsbGVsQmF0Y2hlcy5sZW5ndGh9IGJhdGNoIGNhbGwocykgaW4gcGFyYWxsZWwgKGNodW5rcyAke2kgKiBiYXRjaFNpemUgKyAxfVx1MjAxMyR7Y2h1bmtzQ292ZXJlZH0pLi4uYCk7XG5cbiAgICAvLyBGaXJlIHBhcmFsbGVsQ2FsbHMgYmF0Y2ggY2FsbHMgc2ltdWx0YW5lb3VzbHlcbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgcGFyYWxsZWxCYXRjaGVzLm1hcChiYXRjaCA9PiBlbWJlZEJhdGNoKGJhdGNoLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgLy8gQ29sbGVjdCByZXN1bHRzIHBlciBiYXRjaFxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGJhdGNoID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlOyAvLyBhcnJheSBvZiBOIHZlY3RvcnNcbiAgICAgICAgYmF0Y2guZm9yRWFjaCgoY2h1bmssIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgZW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rXyR7aSAqIGJhdGNoU2l6ZSArIGJhdGNoSWR4ICogYmF0Y2hTaXplICsgY2h1bmtJZHh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1tjaHVua0lkeF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGAgIEJhdGNoICR7aSArIGJhdGNoSWR4fSBmYWlsZWQsIHdpbGwgcmV0cnkgaW5kaXZpZHVhbGx5OmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICBmYWlsZWRCYXRjaGVzLnB1c2goeyBiYXRjaCwgYmF0Y2hJZHg6IGkgKyBiYXRjaElkeCB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIC8vIFdhaXQgYmV0d2VlbiBncm91cHMgKHNraXAgd2FpdCBhZnRlciBsYXN0IGdyb3VwIHVubGVzcyB0aGVyZSBhcmUgcmV0cmllcylcbiAgICBjb25zdCBpc0xhc3RHcm91cCA9IGkgKyBwYXJhbGxlbENhbGxzID49IGJhdGNoZXMubGVuZ3RoO1xuICAgIGlmICghaXNMYXN0R3JvdXAgfHwgZmFpbGVkQmF0Y2hlcy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgbmV4dCBncm91cC4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIEdST1VQX1dBSVRfTVMpKTtcbiAgICB9XG5cbiAgICAvLyBSZXRyeSBmYWlsZWQgYmF0Y2hlcyBvbmUgY2h1bmsgYXQgYSB0aW1lXG4gICAgZm9yIChjb25zdCB7IGJhdGNoLCBiYXRjaElkeCB9IG9mIGZhaWxlZEJhdGNoZXMpIHtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgYmF0Y2gpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtiYXRjaElkeH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFx1MjcwNSBSZXRyeSBzdWNjZWVkZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLmVycm9yKGAgIFx1Mjc0QyBSZXRyeSBmYWlsZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkfTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICByZXR1cm4gZW1iZWRkaW5ncztcbn1cblxuLy8gRml4IDEzOiBRdWVyeSB1c2VzIFJFVFJJRVZBTF9RVUVSWSB0YXNrIHR5cGUsIHNpbmdsZSBlbWJlZCB2aWEgYmF0Y2ggb2YgMVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3F1ZXJ5XSwgJ1JFVFJJRVZBTF9RVUVSWScpO1xuICByZXR1cm4gdmVjdG9yc1swXTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlKHRleHQpIHtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3RleHRdLCAnUkVUUklFVkFMX0RPQ1VNRU5UJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmF0ZUxpbWl0U3RhdGUoKSB7XG4gIHJldHVybiB7XG4gICAgbWF4VG9rZW5zUGVyTWludXRlOiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUkFURV9MSU1JVF9UT0tFTlNfUEVSX01JTlVURSkgfHwgMzAwMDAsXG4gICAgcGFyYWxsZWxDYWxsczogUEFSQUxMRUxfQ0FMTFMoKSxcbiAgICBtYXhDaHVua3NQZXJDYWxsOiBCQVRDSF9TSVpFKCksXG4gICAgb3V0cHV0RGltZW5zaW9uczogT1VUUFVUX0RJTUVOU0lPTlMoKSAgLy8gRml4IDk6IGV4cG9zZSBmb3IgaGVhbHRoIGNoZWNrXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldFJhdGVMaW1pdFN0YXRlIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBDaGVjayBHZW1pbmkgKHZpYSBBUEkga2V5IHByZXNlbmNlKVxuICBoZWFsdGhTdGF0dXMuc2VydmljZXMuZ2VtaW5pID0ge1xuICAgIHN0YXR1czogcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkgPyAnY29uZmlndXJlZCcgOiAnbm90X2NvbmZpZ3VyZWQnXG4gIH07XG5cbiAgLy8gR2V0IHJhdGUgbGltaXQgc3RhdGVcbiAgaGVhbHRoU3RhdHVzLnJhdGVMaW1pdCA9IGdldFJhdGVMaW1pdFN0YXRlKCk7XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcblxuY29uc3QgQ0hBUlNfUEVSX1RPS0VOID0gNDtcbmNvbnN0IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlMgPSAxMDAwO1xuY29uc3QgREVGQVVMVF9PVkVSTEFQX1RPS0VOUyA9IDIwMDtcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG4vLyBGaXggNTogQ2xlYW4gcmF3IFBERiB0ZXh0IGJlZm9yZSBjaHVua2luZ1xuZXhwb3J0IGZ1bmN0aW9uIGNsZWFuVGV4dCh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAnJztcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvXFxmL2csICdcXG4nKSAgICAgICAgICAgICAgICAvLyBmb3JtIGZlZWRzIFx1MjE5MiBuZXdsaW5lXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpICAgICAvLyBjb2xsYXBzZSAzKyBibGFuayBsaW5lcyB0byAyXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJykgICAgICAgIC8vIHJlbW92ZSBsb25lIHBhZ2UgbnVtYmVyc1xuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJykgICAgICAgICAgLy8gY29sbGFwc2UgbXVsdGlwbGUgc3BhY2VzL3RhYnNcbiAgICAudHJpbSgpO1xufVxuXG4vLyBGaXggNDogQ29udGVudC1oYXNoIGJhc2VkIGNodW5rIElEIGZvciBkZWR1cGxpY2F0aW9uXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgY2h1bmtTaXplVG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRva2VucyA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBERUZBVUxUX09WRVJMQVBfVE9LRU5TO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICBjb25zdCBjaHVua1NpemVDaGFycyA9IGNodW5rU2l6ZVRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBjb25zdCBjaHVua3MgPSBbXTtcbiAgbGV0IHN0YXJ0ID0gMDtcbiAgbGV0IGNodW5rSW5kZXggPSAwO1xuXG4gIHdoaWxlIChzdGFydCA8IHRleHQubGVuZ3RoKSB7XG4gICAgbGV0IGVuZCA9IHN0YXJ0ICsgY2h1bmtTaXplQ2hhcnM7XG5cbiAgICBpZiAoZW5kIDwgdGV4dC5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IGJyZWFrUG9pbnRzID0gWycuICcsICcuXFxuJywgJyEgJywgJz8gJywgJ1xcblxcbicsICdcXG4nLCAnICddO1xuICAgICAgY29uc3Qgc2VhcmNoU3RhcnQgPSBlbmQgLSBNYXRoLmZsb29yKGNodW5rU2l6ZUNoYXJzICogMC4yKTtcblxuICAgICAgZm9yIChjb25zdCBicmVha3BvaW50IG9mIGJyZWFrUG9pbnRzKSB7XG4gICAgICAgIGNvbnN0IGlkeCA9IHRleHQubGFzdEluZGV4T2YoYnJlYWtwb2ludCwgZW5kKTtcbiAgICAgICAgaWYgKGlkeCA+IHNlYXJjaFN0YXJ0ICYmIGlkeCA+IHN0YXJ0KSB7XG4gICAgICAgICAgZW5kID0gaWR4ICsgYnJlYWtwb2ludC5sZW5ndGg7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBlbmQgPSBNYXRoLm1pbihlbmQsIHRleHQubGVuZ3RoKTtcbiAgICBjb25zdCBjaHVua0NvbnRlbnQgPSB0ZXh0LnNsaWNlKHN0YXJ0LCBlbmQpLnRyaW0oKTtcblxuICAgIC8vIEZpeCA2OiBTa2lwIGNodW5rcyBiZWxvdyBtaW5pbXVtIHNpemVcbiAgICBpZiAoY2h1bmtDb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY2h1bmtDb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2VucyhjaHVua0NvbnRlbnQpLFxuICAgICAgICBjaGFyU3RhcnQ6IHN0YXJ0LFxuICAgICAgICBjaGFyRW5kOiBlbmQsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgLy8gRml4IDE6IENvcnJlY3Qgb3ZlcmxhcCBcdTIwMTQgb25seSBza2lwIG92ZXJsYXAgaWYgaXQgd291bGQgY2F1c2UgaW5maW5pdGUgbG9vcFxuICAgIGNvbnN0IG5leHRTdGFydCA9IGVuZCAtIG92ZXJsYXBDaGFycztcbiAgICBzdGFydCA9IG5leHRTdGFydCA+IHN0YXJ0ID8gbmV4dFN0YXJ0IDogZW5kO1xuXG4gICAgaWYgKGNodW5rSW5kZXggPiAxMDAwMCkge1xuICAgICAgY29uc29sZS53YXJuKCdDaHVuayBsaW1pdCByZWFjaGVkLCBzdG9wcGluZycpO1xuICAgICAgYnJlYWs7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIGNodW5rcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rUERGQ29udGVudChwZGZEYXRhLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgeyBmaWxlbmFtZSwgZG9jdW1lbnRJZCwgcGFnZU51bWJlciwgdGV4dCwgdG90YWxQYWdlcyB9ID0gcGRmRGF0YTtcblxuICAvLyBGaXggMzogRGV0ZWN0IHNjYW5uZWQvZW1wdHkgUERGc1xuICBpZiAoIXRleHQgfHwgdGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICBjb25zb2xlLndhcm4oYFx1MjZBMFx1RkUwRiAgJHtmaWxlbmFtZX0gcGFnZSAke3BhZ2VOdW1iZXJ9OiBleHRyYWN0ZWQgdGV4dCB0b28gc2hvcnQgXHUyMDE0IG1heSBiZSBhIHNjYW5uZWQgcGFnZSwgc2tpcHBpbmdgKTtcbiAgICByZXR1cm4gW107XG4gIH1cblxuICAvLyBGaXggNTogQ2xlYW4gdGV4dCBiZWZvcmUgY2h1bmtpbmdcbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG5cbiAgY29uc3QgdGV4dENodW5rcyA9IGNodW5rVGV4dChjbGVhbmVkVGV4dCwgb3B0aW9ucyk7XG5cbiAgLy8gRml4IDc6IENvbXB1dGUgdG90YWxfY2h1bmtzIGFuZCBhdHRhY2ggdG8gYWxsIG1ldGFkYXRhXG4gIGNvbnN0IHRvdGFsQ2h1bmtzID0gdGV4dENodW5rcy5sZW5ndGg7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+IHtcbiAgICAvLyBGaXggNDogVXNlIGNvbnRlbnQgaGFzaCBhcyBjaHVuayBJRCBmb3IgZGVkdXBsaWNhdGlvblxuICAgIGNvbnN0IGNodW5rSWQgPSBnZW5lcmF0ZUNodW5rSWQoY2h1bmsudGV4dCwgZmlsZW5hbWUpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY2h1bmtJZCxcbiAgICAgICAgY2h1bmtfaW5kZXg6IGNodW5rLmNodW5rSW5kZXgsXG4gICAgICAgIHRvdGFsX2NodW5rczogdG90YWxDaHVua3MsICAgICAgICAgLy8gRml4IDdcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSwgICAgICAvLyBGaXggMjogY2FsbGVyIG11c3QgcGFzcyBjb3JyZWN0IHBhZ2VcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6ICdwZGYnLFxuICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIGNoYXJfc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgY2hhcl9lbmQ6IGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfTtcbiAgfSk7XG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RTZWN0aW9uVGl0bGUodGV4dCkge1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbi8vIEZpeCA4OiBtZXJnZUNodW5rcyByZW1vdmVkIFx1MjAxNCB3YXMgZGVhZCBjb2RlIGZyb20gb2xkIGJyb2tlbiBiYXRjaGluZyBzdHJhdGVneSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBnZXRHbG9iYWxDb2xsZWN0aW9uLCBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgbGlzdERvY3VtZW50cyB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTID0gNjA7XG5jb25zdCBzZXNzaW9ucyA9IG5ldyBNYXAoKTtcbmNvbnN0IE1BWF9QREZTX1BFUl9TRVNTSU9OID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04pIHx8IDM7XG5jb25zdCBNQVhfVVBMT0FEX1NJWkVfTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIpIHx8IDU7XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVTZXNzaW9uKCkge1xuICBjb25zdCBzZXNzaW9uSWQgPSB1dWlkdjQoKTtcbiAgY29uc3Qgc2Vzc2lvbiA9IHtcbiAgICBpZDogc2Vzc2lvbklkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcblxuICBzZXNzaW9ucy5zZXQoc2Vzc2lvbklkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG5cbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHtcbiAgICAgIHJldHVybiBleGlzdGluZztcbiAgICB9XG4gIH1cblxuICByZXR1cm4gY3JlYXRlU2Vzc2lvbigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSB7XG4gIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gIGNvbnN0IGxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKHNlc3Npb24ubGFzdEFjY2Vzc2VkKS5nZXRUaW1lKCk7XG4gIGNvbnN0IHRpbWVvdXRNcyA9IHNlc3Npb24udGltZW91dE1pbnV0ZXMgKiA2MCAqIDEwMDA7XG4gIHJldHVybiAobm93IC0gbGFzdEFjY2Vzc2VkKSA+IHRpbWVvdXRNcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIHNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgaWQ6IGRvY3VtZW50SW5mby5pZCxcbiAgICBmaWxlbmFtZTogZG9jdW1lbnRJbmZvLmZpbGVuYW1lLFxuICAgIGZpbGVTaXplOiBkb2N1bWVudEluZm8uZmlsZVNpemUsXG4gICAgcGFnZUNvdW50OiBkb2N1bWVudEluZm8ucGFnZUNvdW50LFxuICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICBjaHVua0NvdW50OiBkb2N1bWVudEluZm8uY2h1bmtDb3VudCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnXG4gIH0pO1xuXG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogJ1Nlc3Npb24gbm90IGZvdW5kJyB9O1xuICB9XG5cbiAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuXG4gIHJldHVybiB7IGNhblVwbG9hZDogdHJ1ZSB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVVcGxvYWQoc2Vzc2lvbklkLCBmaWxlLCBmaWxlbmFtZSkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBjb25zdCBlcnJvcnMgPSBbXTtcblxuICAvLyBDaGVjayBmaWxlIHNpemVcbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgLy8gQ2hlY2sgbWF4IFBERnNcbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgLy8gQ2hlY2sgZm9yIGR1cGxpY2F0ZSBmaWxlbmFtZVxuICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gZmlsZW5hbWUpKSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmApO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpc1ZhbGlkOiBlcnJvcnMubGVuZ3RoID09PSAwLFxuICAgIGVycm9ycyxcbiAgICBpc0xhcmdlRmlsZTogZmlsZS5zaXplID4gKE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0ICogMC42KSAvLyA2MCUgb2YgbWF4XG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgcmV0dXJuIGZhbHNlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSB7XG4gICAgcmV0dXJuIFtdO1xuICB9XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbkRvY3MgPSBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gIGNvbnN0IGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIGNvbnN0IGdsb2JhbERvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKGdsb2JhbENvbGxlY3Rpb24pO1xuXG4gIHJldHVybiB7XG4gICAgc2Vzc2lvbkRvY3VtZW50czogc2Vzc2lvbkRvY3MsXG4gICAgZ2xvYmFsRG9jdW1lbnRzOiBnbG9iYWxEb2NzXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpZDogc2Vzc2lvbi5pZCxcbiAgICBkb2N1bWVudENvdW50OiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBzZXNzaW9uLmNyZWF0ZWRBdCxcbiAgICBsYXN0QWNjZXNzZWQ6IHNlc3Npb24ubGFzdEFjY2Vzc2VkLFxuICAgIHRvdGFsU2l6ZTogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmZpbGVTaXplIHx8IDApLCAwKSxcbiAgICB0b3RhbENodW5rczogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsaXN0QWN0aXZlU2Vzc2lvbnMoKSB7XG4gIHJldHVybiBBcnJheS5mcm9tKHNlc3Npb25zLnZhbHVlcygpKS5maWx0ZXIocyA9PiAhaXNTZXNzaW9uRXhwaXJlZChzKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhbnVwRXhwaXJlZFNlc3Npb25zKCkge1xuICBsZXQgY2xlYW5lZCA9IDA7XG4gIGZvciAoY29uc3QgW2lkLCBzZXNzaW9uXSBvZiBzZXNzaW9ucykge1xuICAgIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgICBzZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IGZzIGZyb20gJ2ZzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5pbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbmltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUsIHZhbGlkYXRlUERGRmlsZSwgdmFsaWRhdGVGaWxlU2l6ZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbmltcG9ydCB7XG4gIENvcnJ1cHRlZFBERkVycm9yLFxuICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgRmlsZVRvb0xhcmdlRXJyb3IsXG4gIFRvb01hbnlQREZzRXJyb3IsXG4gIER1cGxpY2F0ZUZpbGVFcnJvclxufSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuaW1wb3J0IHsgZ2V0R2xvYmFsQ29sbGVjdGlvbiwgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbmltcG9ydCB7IGdlbmVyYXRlRW1iZWRkaW5ncyB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBjYW5BY2NlcHRVcGxvYWQsIGFkZERvY3VtZW50VG9TZXNzaW9uLCByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uLCBnZXRBbGxEb2N1bWVudHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG4vLyBTZWVkIFBERnMgYXJlIGF0IHJlcG8gcm9vdCBcdTIwMTQgdHdvIGxldmVscyB1cCBmcm9tIHNlcnZlci9hcGkvXG5jb25zdCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uLycpO1xuXG5jb25zdCBzdG9yYWdlID0gbXVsdGVyLmRpc2tTdG9yYWdlKHtcbiAgZGVzdGluYXRpb246IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCB1cGxvYWREaXIpLFxuICBmaWxlbmFtZTogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIGAke3V1aWR2NCgpfSR7cGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKX1gKVxufSk7XG5cbmNvbnN0IHVwbG9hZCA9IG11bHRlcih7XG4gIHN0b3JhZ2UsXG4gIGxpbWl0czogeyBmaWxlU2l6ZTogcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CIHx8ICc1JykgKiAxMDI0ICogMTAyNCB9LFxuICBmaWxlRmlsdGVyOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgIGlmIChmaWxlLm1pbWV0eXBlID09PSAnYXBwbGljYXRpb24vcGRmJyAmJiBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpLnRvTG93ZXJDYXNlKCkgPT09ICcucGRmJykge1xuICAgICAgY2IobnVsbCwgdHJ1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNiKG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpKTtcbiAgICB9XG4gIH1cbn0pO1xuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IHRjLml0ZW1zLm1hcChpID0+IGkuc3RyKS5qb2luKCcgJyk7XG4gICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuXG4gICAgY29uc3QgcGFnZU1hcCA9IFtdO1xuICAgIGxldCBjaGFyUG9zID0gMDtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICB9XG5cbiAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRQYWdlTnVtYmVyKGNoYXJTdGFydCwgcGFnZU1hcCkge1xuICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICBpZiAoY2hhclN0YXJ0ID49IGVudHJ5LnN0YXJ0ICYmIGNoYXJTdGFydCA8IGVudHJ5LmVuZCkgcmV0dXJuIGVudHJ5LnBhZ2U7XG4gIH1cbiAgcmV0dXJuIHBhZ2VNYXBbcGFnZU1hcC5sZW5ndGggLSAxXT8ucGFnZSB8fCAxO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlVXBsb2FkKHJlcSwgcmVzKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLmJvZHkuc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICAgIGNvbnN0IHNlc3Npb24gPSBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBtYXhQREZzID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04gfHwgJzMnKTtcbiAgICBjb25zdCBjbGVhbkZpbGVuYW1lID0gc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSk7XG5cbiAgICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBUb29NYW55UERGc0Vycm9yKG1heFBERnMpO1xuICAgIH1cblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBEdXBsaWNhdGVGaWxlRXJyb3IoY2xlYW5GaWxlbmFtZSk7XG4gICAgfVxuXG4gICAgY29uc3QgeyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9ID0gYXdhaXQgcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZS5wYXRoKTtcblxuICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MjIpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ05vIGV4dHJhY3RhYmxlIHRleHQgZm91bmQgXHUyMDE0IFBERiBtYXkgYmUgc2Nhbm5lZCBvciBpbWFnZS1vbmx5JyxcbiAgICAgICAgY29kZTogJ0VNUFRZX1BERidcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGRvY3VtZW50SWQgPSBwYXRoLnBhcnNlKGZpbGUuZmlsZW5hbWUpLm5hbWU7XG5cbiAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQsIHtcbiAgICAgIGNodW5rU2l6ZVRva2VuczogMTAwMCxcbiAgICAgIG92ZXJsYXBUb2tlbnM6IDIwMFxuICAgIH0pO1xuXG4gICAgaWYgKHJhd0NodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQyMikuanNvbih7IGVycm9yOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgY2h1bmtfaWQ6IGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogaWR4LFxuICAgICAgICB0b3RhbF9jaHVua3M6IHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiBnZXRQYWdlTnVtYmVyKGNodW5rLmNoYXJTdGFydCwgcGFnZU1hcCksXG4gICAgICAgIHRvdGFsX3BhZ2VzOiB0b3RhbFBhZ2VzLFxuICAgICAgICBzb3VyY2VfdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH0pKTtcblxuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IGF3YWl0IGdlbmVyYXRlRW1iZWRkaW5ncyhcbiAgICAgIGNodW5rcyxcbiAgICAgICdSRVRSSUVWQUxfRE9DVU1FTlQnLFxuICAgICAgKHsgY3VycmVudF9iYXRjaCwgdG90YWxfYmF0Y2hlcyB9KSA9PiB7XG4gICAgICAgIGlmIChyZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcykge1xuICAgICAgICAgIHJlcS5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzLmVtaXQoYHByb2dyZXNzXyR7c2Vzc2lvbklkfWAsIHtcbiAgICAgICAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgICAgICBjdXJyZW50X2JhdGNoLFxuICAgICAgICAgICAgdG90YWxfYmF0Y2hlcyxcbiAgICAgICAgICAgIHN0YWdlOiAnZW1iZWRkaW5nJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgKTtcblxuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNTAzKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZ2VuZXJhdGUgZW1iZWRkaW5ncycsIGNvZGU6ICdFTUJFRERJTkdfRkFJTEVEJyB9KTtcbiAgICB9XG5cbiAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgY29sbGVjdGlvbixcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICApO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCxcbiAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcyxcbiAgICAgIGNodW5rQ291bnQ6IGVtYmVkZGluZ3MubGVuZ3RoXG4gICAgfSk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZG9jdW1lbnQ6IHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgICBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsXG4gICAgICAgIGNodW5rQ291bnQ6IGVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIHNlc3Npb25JZFxuICAgIH0pO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaWYgKHJlcS5maWxlICYmIGZzLmV4aXN0c1N5bmMocmVxLmZpbGUucGF0aCkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMocmVxLmZpbGUucGF0aCk7XG4gICAgfVxuICAgIGNvbnNvbGUuZXJyb3IoJ1VwbG9hZCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyhlcnJvci5zdGF0dXNDb2RlIHx8IDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1VQTE9BRF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdERvY3VtZW50c0hhbmRsZXIocmVxLCByZXMpIHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG4gIHRyeSB7XG4gICAgY29uc3QgZG9jdW1lbnRzID0gYXdhaXQgZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oZG9jdW1lbnRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdMaXN0IGRvY3VtZW50cyBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50cycsIGNvZGU6ICdMSVNUX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnQocmVxLCByZXMpIHtcbiAgY29uc3QgeyBkb2N1bWVudElkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgY29uc3QgY291bnQgPSBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgICAgIGlmIChjb3VudCA+IDApIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhnbG9iYWxDb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICB9IGNhdGNoIChlKSB7IC8qIG5vdCBpbiBnbG9iYWwgKi8gfVxuXG4gICAgcmVzLmpzb24oeyBzdWNjZXNzOiB0cnVlLCBkb2N1bWVudElkIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQnLCBjb2RlOiAnREVMRVRFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRGaWxlKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgdHJ5IHtcbiAgICAvLyBTdGVwIDE6IHNlc3Npb24gdXBsb2FkIGJ5IFVVSURcbiAgICBjb25zdCB1cGxvYWRQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgYCR7ZG9jdW1lbnRJZH0ucGRmYCk7XG4gICAgaWYgKGZzLmV4aXN0c1N5bmModXBsb2FkUGF0aCkpIHtcbiAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBgaW5saW5lOyBmaWxlbmFtZT1cIiR7ZG9jdW1lbnRJZH0ucGRmXCJgKTtcbiAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHVwbG9hZFBhdGgpLnBpcGUocmVzKTtcbiAgICB9XG5cbiAgICAvLyBTdGVwIDI6IHNlZWQgZG9jIFx1MjAxNCBtYXRjaCBieSBvcmlnaW5hbCBmaWxlbmFtZSBmcm9tIHF1ZXJ5IHBhcmFtXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgYGlubGluZTsgZmlsZW5hbWU9XCIke2ZpbGVuYW1lfVwiYCk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gU3RlcCAzOiBzY2FuIGFsbCBQREZzIGluIHNlZWREaXIgYXMgbGFzdCByZXNvcnRcbiAgICBjb25zdCBhbGxQZGZzID0gZnMucmVhZGRpclN5bmMoc2VlZERpcikuZmlsdGVyKGYgPT4gZi5lbmRzV2l0aCgnLnBkZicpKTtcbiAgICBjb25zdCBtYXRjaCA9IGFsbFBkZnMuZmluZChmID0+IGZpbGVuYW1lICYmIGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgIGlmIChtYXRjaCkge1xuICAgICAgY29uc3QgbWF0Y2hQYXRoID0gcGF0aC5qb2luKHNlZWREaXIsIG1hdGNoKTtcbiAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBgaW5saW5lOyBmaWxlbmFtZT1cIiR7bWF0Y2h9XCJgKTtcbiAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKG1hdGNoUGF0aCkucGlwZShyZXMpO1xuICAgIH1cblxuICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbnJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG5yb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG5yb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldEdsb2JhbENvbGxlY3Rpb24sIGdldFNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgNTtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuLy8gQ2FjaGUgcmVzb2x2ZWQgY29sbGVjdGlvbiBvYmplY3RzIFx1MjAxNCBuZXZlciBoaXQgQ2hyb21hIG1vcmUgdGhhbiBvbmNlIHBlciBzZXNzaW9uXG5sZXQgY2FjaGVkR2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5jb25zdCBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMgPSBuZXcgTWFwKCk7XG5cbmFzeW5jIGZ1bmN0aW9uIGdldE9yQ2FjaGVHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWNhY2hlZEdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjYWNoZWRHbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICB9XG4gIHJldHVybiBjYWNoZWRHbG9iYWxDb2xsZWN0aW9uO1xufVxuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICB9XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4geyBjb25maWRlbmNlOiAwLCB0b3BTY29yZTogMCB9O1xuICB9XG5cbiAgY29uc3Qgc2NvcmVzID0gcmVzdWx0cy5zbGljZSgwLCB0b3BLKS5tYXAociA9PiByLnNjb3JlKTtcbiAgY29uc3QgYXZnU2NvcmUgPSBzY29yZXMucmVkdWNlKChhLCBiKSA9PiBhICsgYiwgMCkgLyBzY29yZXMubGVuZ3RoO1xuXG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksIC8vIDAuNDcgXHUyMTkyIDQ3XG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG4gIGNvbnN0IGluY2x1ZGVHbG9iYWwgPSBvcHRpb25zLmluY2x1ZGVHbG9iYWwgIT09IGZhbHNlO1xuXG4gIHRyeSB7XG4gICAgLy8gUnVuIGVtYmVkZGluZyArIGJvdGggY29sbGVjdGlvbiBmZXRjaGVzIGluIHBhcmFsbGVsXG4gICAgY29uc3QgW3F1ZXJ5RW1iZWRkaW5nLCBnbG9iYWxDb2xsZWN0aW9uLCBzZXNzaW9uQ29sbGVjdGlvbl0gPSBhd2FpdCBQcm9taXNlLmFsbChbXG4gICAgICBlbWJlZFF1ZXJ5KHF1ZXJ5KSxcbiAgICAgIGluY2x1ZGVHbG9iYWwgPyBnZXRPckNhY2hlR2xvYmFsQ29sbGVjdGlvbigpIDogUHJvbWlzZS5yZXNvbHZlKG51bGwpLFxuICAgICAgc2Vzc2lvbklkID8gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkgOiBQcm9taXNlLnJlc29sdmUobnVsbClcbiAgICBdKTtcblxuICAgIC8vIFF1ZXJ5IGJvdGggY29sbGVjdGlvbnMgaW4gcGFyYWxsZWxcbiAgICBjb25zdCBxdWVyeVByb21pc2VzID0gW107XG5cbiAgICBpZiAoZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgICAgcXVlcnlQcm9taXNlcy5wdXNoKFxuICAgICAgICBxdWVyeUNvbGxlY3Rpb24oZ2xvYmFsQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAgICAgLnRoZW4ocmVzdWx0cyA9PiAoeyB0eXBlOiAnZ2xvYmFsJywgcmVzdWx0cyB9KSlcbiAgICAgICAgICAuY2F0Y2goKCkgPT4gKHsgdHlwZTogJ2dsb2JhbCcsIHJlc3VsdHM6IFtdIH0pKVxuICAgICAgKTtcbiAgICB9XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb24pIHtcbiAgICAgIHF1ZXJ5UHJvbWlzZXMucHVzaChcbiAgICAgICAgcXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgICAgICAudGhlbihyZXN1bHRzID0+ICh7IHR5cGU6ICdzZXNzaW9uJywgcmVzdWx0cyB9KSlcbiAgICAgICAgICAuY2F0Y2goKCkgPT4gKHsgdHlwZTogJ3Nlc3Npb24nLCByZXN1bHRzOiBbXSB9KSlcbiAgICAgICk7XG4gICAgfVxuXG4gICAgY29uc3QgcXVlcnlSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGwocXVlcnlQcm9taXNlcyk7XG5cbiAgICBjb25zdCBhbGxSZXN1bHRzID0gW107XG4gICAgZm9yIChjb25zdCB7IHR5cGUsIHJlc3VsdHM6IHR5cGVSZXN1bHRzIH0gb2YgcXVlcnlSZXN1bHRzKSB7XG4gICAgICBmb3IgKGNvbnN0IHJlc3VsdCBvZiB0eXBlUmVzdWx0cykge1xuICAgICAgICBhbGxSZXN1bHRzLnB1c2goeyAuLi5yZXN1bHQsIHNvdXJjZV90eXBlOiB0eXBlIH0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGFsbFJlc3VsdHMuc29ydCgoYSwgYikgPT4gYi5zY29yZSAtIGEuc2NvcmUpO1xuICAgIGNvbnN0IHRvcFJlc3VsdHMgPSBhbGxSZXN1bHRzLnNsaWNlKDAsIHRvcEspO1xuICAgIGNvbnN0IGNvdmVyYWdlID0gY2FsY3VsYXRlQ292ZXJhZ2UodG9wUmVzdWx0cywgdG9wSyk7XG5cbiAgICByZXR1cm4geyByZXN1bHRzOiB0b3BSZXN1bHRzLCBjb3ZlcmFnZSwgcXVlcnlFbWJlZGRpbmcgfTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1JldHJpZXZhbCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLy8gQ2FsbCB0aGlzIGFmdGVyIGEgdXNlciB1cGxvYWRzIGEgZG9jdW1lbnQgdG8gYSBzZXNzaW9uXG4vLyBzbyB0aGUgbmV4dCBxdWVyeSBmZXRjaGVzIHRoZSB1cGRhdGVkIGNvbGxlY3Rpb24gZnJlc2hcbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcblxuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnZ2xvYmFsJyA/ICdbU2VlZCBEb2N1bWVudF0nIDogJ1tTZXNzaW9uIFVwbG9hZF0nO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG4gICAgY29udGV4dFBhcnRzLnB1c2goYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIHJldHVybiByZXN1bHRzLm1hcCgocmVzdWx0LCBpZHgpID0+ICh7XG4gICAgaWQ6IHV1aWR2NCgpLFxuICAgIGluZGV4OiBpZHggKyAxLFxuICAgIGRvY3VtZW50SWQ6IHJlc3VsdC5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICBmaWxlbmFtZTogcmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgIHBhZ2VOdW1iZXI6IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICBzZWN0aW9uOiByZXN1bHQubWV0YWRhdGEuc2VjdGlvbl90aXRsZSxcbiAgICBleGNlcnB0OiByZXN1bHQudGV4dC5zbGljZSgwLCAyMDApICsgKHJlc3VsdC50ZXh0Lmxlbmd0aCA+IDIwMCA/ICcuLi4nIDogJycpLFxuICAgIHNjb3JlOiByZXN1bHQuc2NvcmUsXG4gICAgc291cmNlVHlwZTogcmVzdWx0LnNvdXJjZV90eXBlLFxuICAgIGNodW5rSWQ6IHJlc3VsdC5pZFxuICB9KSk7XG59XG5cbi8vIFJlZnVzZSBvbmx5IGlmIHRvcCBzY29yZSBpcyBnZW51aW5lbHkgbmVhci16ZXJvIFx1MjAxNCBubyByZWxldmFudCBjb250ZW50IGF0IGFsbFxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9OyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IG1lbW9yeU1hcCA9IG5ldyBNYXAoKTtcbmNvbnN0IERFRkFVTFRfTUVNT1JZX1dJTkRPVyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IDEwO1xuXG5leHBvcnQgZnVuY3Rpb24gaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpIHtcbiAgaWYgKCFtZW1vcnlNYXAuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBtZW1vcnlNYXAuc2V0KHNlc3Npb25JZCwge1xuICAgICAgdHVybnM6IFtdLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBtZXRhZGF0YSA9IHt9KSB7XG4gIGNvbnN0IG1lbW9yeSA9IG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IG1heFR1cm5zID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuXG4gIGNvbnN0IHR1cm4gPSB7XG4gICAgaWQ6IGB0dXJuXyR7RGF0ZS5ub3coKX1fJHtNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zdWJzdHIoMiwgOSl9YCxcbiAgICByb2xlLFxuICAgIGNvbnRlbnQsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIC4uLm1ldGFkYXRhXG4gIH07XG5cbiAgbWVtb3J5LnR1cm5zLnB1c2godHVybik7XG5cbiAgaWYgKG1lbW9yeS50dXJucy5sZW5ndGggPiBtYXhUdXJucykge1xuICAgIG1lbW9yeS50dXJucyA9IG1lbW9yeS50dXJucy5zbGljZSgtbWF4VHVybnMpO1xuICB9XG5cbiAgcmV0dXJuIHR1cm47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCBtYXhUdXJucyA9IG51bGwpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IGxpbWl0ID0gbWF4VHVybnMgfHwgcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuICByZXR1cm4gbWVtb3J5LnR1cm5zLnNsaWNlKC1saW1pdCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb252ZXJzYXRpb25Db250ZXh0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIHJldHVybiB0dXJucy5tYXAodCA9PiAoe1xuICAgIHJvbGU6IHQucm9sZSxcbiAgICBjb250ZW50OiB0LmNvbnRlbnRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIGlmICh0dXJucy5sZW5ndGggPT09IDApIHJldHVybiAnJztcblxuICByZXR1cm4gdHVybnMubWFwKHQgPT4ge1xuICAgIGNvbnN0IHByZWZpeCA9IHQucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXI6JyA6ICdBc3Npc3RhbnQ6JztcbiAgICByZXR1cm4gYCR7cHJlZml4fSAke3QuY29udGVudH1gO1xuICB9KS5qb2luKCdcXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFyTWVtb3J5KHNlc3Npb25JZCkge1xuICBtZW1vcnlNYXAuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnlTdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIHJldHVybiB7XG4gICAgdHVybkNvdW50OiBtZW1vcnkudHVybnMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogbWVtb3J5LmNyZWF0ZWRBdCxcbiAgICBsYXN0VHVybkF0OiBtZW1vcnkudHVybnMubGVuZ3RoID4gMCA/IG1lbW9yeS50dXJuc1ttZW1vcnkudHVybnMubGVuZ3RoIC0gMV0udGltZXN0YW1wIDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBjaXRhdGlvbnMgPSBbXSwgY292ZXJhZ2UgPSBudWxsLCBhbnN3ZXJJZCA9IG51bGwpIHtcbiAgcmV0dXJuIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCB7XG4gICAgLi4uKGFuc3dlcklkICYmIHsgaWQ6IGFuc3dlcklkIH0pLFxuICAgIGNpdGF0aW9ucyxcbiAgICBjb3ZlcmFnZSxcbiAgICBoYXNDaXRhdGlvbnM6IGNpdGF0aW9ucy5sZW5ndGggPiAwXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdFVzZXJNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ3VzZXInKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50JykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcHJvbXB0U2VydmljZS5qc1wiO2ltcG9ydCB7IGZvcm1hdE1lbW9yeUZvclByb21wdCB9IGZyb20gJy4vbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0LCBjYWxjdWxhdGVDb3ZlcmFnZSB9IGZyb20gJy4vcmV0cmlldmFsU2VydmljZS5qcyc7XG5cbmNvbnN0IFNZU1RFTV9JTlNUUlVDVElPTiA9IGBZb3UgYXJlIGFuIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQgdGhhdCBhbnN3ZXJzIHF1ZXN0aW9ucyBiYXNlZCBvbiBpbmRleGVkIGRvY3VtZW50cyB3aGVuIGF2YWlsYWJsZS5cblxuUlVMRVM6XG4xLiBXaGVuIGNvbnRleHQgaXMgcHJvdmlkZWQsIGFuc3dlciBiYXNlZCBvbiBpdCBhbmQgY2l0ZSBzb3VyY2VzIHVzaW5nIFsxXSwgWzJdLCBldGMuXG4yLiBGb3IgZ2VuZXJhbCBjb252ZXJzYXRpb24gKGdyZWV0aW5ncywgY2xhcmlmeWluZyBxdWVzdGlvbnMsIHNtYWxsIHRhbGspLCByZXNwb25kIG5hdHVyYWxseSBhbmQgaGVscGZ1bGx5IHdpdGhvdXQgcmVxdWlyaW5nIGNvbnRleHQuXG4zLiBJZiBhIGZhY3R1YWwgcXVlc3Rpb24gaXMgYXNrZWQgYnV0IGNvbnRleHQgaXMgaW5zdWZmaWNpZW50LCBzYXkgc28gY2xlYXJseSBhbmQgc3VnZ2VzdCB1cGxvYWRpbmcgcmVsZXZhbnQgZG9jdW1lbnRzLlxuNC4gQmUgY29uY2lzZSBidXQgdGhvcm91Z2guIFVzZSBidWxsZXQgcG9pbnRzIG9yIG51bWJlcmVkIGxpc3RzIGZvciBjb21wbGV4IGFuc3dlcnMuXG41LiBNYWludGFpbiBjb252ZXJzYXRpb24gY29udGludWl0eSBidXQgZG9uJ3QgcmVwZWF0IGluZm9ybWF0aW9uIHVubmVjZXNzYXJpbHkuXG42LiBGb3JtYXQgcmVzcG9uc2VzIGluIGNsZWFyLCByZWFkYWJsZSBtYXJrZG93bi5gO1xuXG5jb25zdCBSRUZVU0FMX01FU1NBR0UgPSBcIkkgZG9uJ3QgaGF2ZSBlbm91Z2ggaW5mb3JtYXRpb24gaW4gdGhlIGtub3dsZWRnZSBiYXNlIHRvIGFuc3dlciB0aGF0IGNvbmZpZGVudGx5LiBUcnkgdXBsb2FkaW5nIHJlbGV2YW50IGRvY3VtZW50cywgb3IgYXNrIG1lIGEgZ2VuZXJhbCBxdWVzdGlvbi5cIjtcbmV4cG9ydCBmdW5jdGlvbiBidWlsZFByb21wdCh7IHF1ZXJ5LCBjb250ZXh0LCBtZW1vcnlDb250ZXh0LCBjb3ZlcmFnZSB9KSB7XG4gIGNvbnN0IHBhcnRzID0gW107XG5cbiAgLy8gU3lzdGVtIGluc3RydWN0aW9uXG4gIHBhcnRzLnB1c2goU1lTVEVNX0lOU1RSVUNUSU9OKTtcblxuICAvLyBQYXN0IGNvbnZlcnNhdGlvbiBpZiBhdmFpbGFibGVcbiAgaWYgKG1lbW9yeUNvbnRleHQpIHtcbiAgICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gUFJFVklPVVMgQ09OVkVSU0FUSU9OIC0tLVxcbicpO1xuICAgIHBhcnRzLnB1c2gobWVtb3J5Q29udGV4dCk7XG4gICAgcGFydHMucHVzaCgnXFxuLS0tIEVORCBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gIH1cblxuICAvLyBSZXRyaWV2ZWQgY29udGV4dFxuICBpZiAoY29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBSRUxFVkFOVCBDT05URVhUIEZST00gS05PV0xFREdFIEJBU0UgLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChjb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIENPTlRFWFQgLS0tXFxuJyk7XG4gIH1cblxuICAvLyBDdXJyZW50IHF1ZXN0aW9uXG4gIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBDVVJSRU5UIFFVRVNUSU9OIC0tLVxcbicpO1xuICBwYXJ0cy5wdXNoKHF1ZXJ5KTtcbiAgcGFydHMucHVzaCgnXFxuXFxuUmVtZW1iZXI6IEFuc3dlciBiYXNlZCBPTkxZIG9uIHRoZSBwcm92aWRlZCBjb250ZXh0LiBVc2UgWzFdLCBbMl0sIGV0Yy4gZm9yIGNpdGF0aW9ucy4gSWYgdGhlIGNvbnRleHQgaXMgaW5zdWZmaWNpZW50LCBzYXkgc28gY2xlYXJseS4nKTtcblxuICByZXR1cm4gcGFydHMuam9pbignJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFN0cmVhbWluZ1Byb21wdChxdWVyeSwgcmV0cmlldmVkUmVzdWx0cywgc2Vzc2lvbklkLCBtZW1vcnlTZXJ2aWNlKSB7XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKTtcbiAgY29uc3QgY29udGV4dFN0cmluZyA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmV0cmlldmVkUmVzdWx0cyk7XG5cbiAgcmV0dXJuIGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0U3RyaW5nLFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IGNhbGN1bGF0ZUNvdmVyYWdlKHJldHJpZXZlZFJlc3VsdHMpXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFJlc3BvbnNlKCkge1xuICByZXR1cm4gUkVGVVNBTF9NRVNTQUdFO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U3lzdGVtSW5zdHJ1Y3Rpb24oKSB7XG4gIHJldHVybiBTWVNURU1fSU5TVFJVQ1RJT047XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFdlYlNlYXJjaFByb21wdChxdWVyeSwgZ3JvdW5kaW5nTWV0YWRhdGEpIHtcbiAgcmV0dXJuIGBCYXNlZCBvbiB3ZWIgc2VhcmNoIHJlc3VsdHMsIGFuc3dlciB0aGUgZm9sbG93aW5nIHF1ZXN0aW9uOiAke3F1ZXJ5fVxuXG5HdWlkZWxpbmVzOlxuLSBVc2UgaW5mb3JtYXRpb24gZnJvbSB0aGUgd2ViIHNlYXJjaFxuLSBQcm92aWRlIHNvdXJjZXMvVVJMcyB3aGVyZSBhcHBsaWNhYmxlXG4tIEJlIGNvbmNpc2UgYW5kIGluZm9ybWF0aXZlXG4tIElmIG11bHRpcGxlIHNvdXJjZXMgYWdyZWUgb3IgY29udHJhZGljdCwgbWVudGlvbiB0aGF0YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdEdlbmVyYXRpb25Db25maWcoY3VzdG9tQ29uZmlnID0ge30pIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgIHRvcFA6IDAuOTUsXG4gICAgdG9wSzogNDAsXG4gICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4LFxuICAgIC4uLmN1c3RvbUNvbmZpZ1xuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFNvdXJjZXNGcm9tUmVzcG9uc2UocmVzcG9uc2UpIHtcbiAgLy8gRXh0cmFjdCBjaXRhdGlvbiBwYXR0ZXJucyBsaWtlIFsxXSwgWzJdLCBldGMuXG4gIGNvbnN0IGNpdGF0aW9uUGF0dGVybiA9IC9cXFsoXFxkKylcXF0vZztcbiAgY29uc3QgY2l0YXRpb25zID0gbmV3IFNldCgpO1xuICBsZXQgbWF0Y2g7XG5cbiAgd2hpbGUgKChtYXRjaCA9IGNpdGF0aW9uUGF0dGVybi5leGVjKHJlc3BvbnNlKSkgIT09IG51bGwpIHtcbiAgICBjaXRhdGlvbnMuYWRkKHBhcnNlSW50KG1hdGNoWzFdKSk7XG4gIH1cblxuICByZXR1cm4gQXJyYXkuZnJvbShjaXRhdGlvbnMpLnNvcnQoKGEsIGIpID0+IGEgLSBiKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBidWlsZFByb21wdCwgZ2V0UmVmdXNhbFJlc3BvbnNlIH0gZnJvbSAnLi9wcm9tcHRTZXJ2aWNlLmpzJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG4vLyBcdTI3MDUgTGF6eSBcdTIwMTQgcmVhZCBpbnNpZGUgdGhlIGZ1bmN0aW9uLCBub3QgYXQgbW9kdWxlIHRvcCBsZXZlbFxubGV0IGdlbkFJID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0R2VuQUkoKSB7XG4gIGlmICghZ2VuQUkpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWTtcbiAgICBpZiAoIWFwaUtleSkgdGhyb3cgbmV3IEVycm9yKCdHRU1JTklfQVBJX0tFWSBpcyB1bmRlZmluZWQnKTtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkoYXBpS2V5KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxubGV0IHByaW1hcnlNb2RlbCA9IG51bGw7XG5sZXQgZmFsbGJhY2tNb2RlbCA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldFByaW1hcnlNb2RlbCgpIHtcbiAgaWYgKCFwcmltYXJ5TW9kZWwpIHtcbiAgICBwcmltYXJ5TW9kZWwgPSBnZXRHZW5BSSgpLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBQUklNQVJZX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBwcmltYXJ5TW9kZWw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWwoKSB7XG4gIGlmICghZmFsbGJhY2tNb2RlbCkge1xuICAgIGZhbGxiYWNrTW9kZWwgPSBnZXRHZW5BSSgpLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBGQUxMQkFDS19NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gZmFsbGJhY2tNb2RlbDtcbn1cblxuYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVXaXRoTW9kZWwobW9kZWwsIHByb21wdCwgc2lnbmFsKSB7XG4gIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH1cbiAgfSwgeyBzaWduYWwgfSk7XG5cbiAgcmV0dXJuIHJlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVJlc3BvbnNlKHByb21wdCkge1xuICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuICBjb25zdCB0aW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IGdldFByaW1hcnlNb2RlbCgpLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuICAgIHJldHVybiByZXN1bHQucmVzcG9uc2UudGV4dCgpO1xuICB9IGNhdGNoIChwcmltYXJ5RXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdQcmltYXJ5IG1vZGVsIGZhaWxlZDonLCBwcmltYXJ5RXJyb3IubWVzc2FnZSk7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgZmFsbGJhY2tSZXN1bHQgPSBhd2FpdCBnZXRGYWxsYmFja01vZGVsKCkuZ2VuZXJhdGVDb250ZW50KHtcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuICAgICAgcmV0dXJuIGZhbGxiYWNrUmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbiAgICB9IGNhdGNoIChmYWxsYmFja0Vycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWxsYmFjayBtb2RlbCBhbHNvIGZhaWxlZDonLCBmYWxsYmFja0Vycm9yLm1lc3NhZ2UpO1xuICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1SZXNwb25zZShwcm9tcHQpIHtcbiAgbGV0IG1vZGVsID0gZ2V0UHJpbWFyeU1vZGVsKCk7XG4gIGxldCByZXRyaWVzID0gMDtcbiAgY29uc3QgbWF4UmV0cmllcyA9IDI7XG5cbiAgd2hpbGUgKHJldHJpZXMgPCBtYXhSZXRyaWVzKSB7XG4gICAgdHJ5IHtcbiAgICAgIC8vIFx1MjcwNSBGSVg6IENyZWF0ZSBBYm9ydENvbnRyb2xsZXIgcGVyIGF0dGVtcHQgZm9yIHRpbWVvdXQgc2lnbmFsbGluZ1xuICAgICAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcblxuICAgICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50U3RyZWFtKHtcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcblxuICAgICAgLy8gXHUyNzA1IEZJWDogVXNlIGNvbnRyb2xsZXIuYWJvcnQoKSBpbnN0ZWFkIG9mIHRocm93IGluc2lkZSBzZXRUaW1lb3V0XG4gICAgICAvLyAodGhyb3cgaW5zaWRlIHNldFRpbWVvdXQgaXMgdW5jYXVnaHQgYW5kIHNpbGVudGx5IGtpbGxzIHRoZSBzdHJlYW0pXG4gICAgICBjb25zdCBmaXJzdFRva2VuVGltZW91dCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBGSVJTVF9UT0tFTl9USU1FT1VUKTtcblxuICAgICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiByZXN1bHQuc3RyZWFtKSB7XG4gICAgICAgIC8vIFx1MjcwNSBGSVg6IENoZWNrIGFib3J0IHNpZ25hbCBvbiBlYWNoIGl0ZXJhdGlvblxuICAgICAgICBpZiAoY29udHJvbGxlci5zaWduYWwuYWJvcnRlZCkge1xuICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdGaXJzdCB0b2tlbiB0aW1lb3V0IFx1MjAxNCBubyByZXNwb25zZSBmcm9tIG1vZGVsJyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCB0ZXh0ID0gY2h1bmsudGV4dCgpO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpOyAvLyBnb3QgZmlyc3QgdG9rZW4sIGNhbmNlbCB0aW1lb3V0XG4gICAgICAgICAgfVxuICAgICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIC8vIFN0cmVhbSBjb21wbGV0ZWQgbmF0dXJhbGx5IFx1MjAxNCBjbGVhbiB1cCB0aW1lb3V0XG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgcmV0dXJuIHsgc3VjY2VzczogdHJ1ZSB9O1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcbiAgICAgIGNvbnNvbGUuZXJyb3IoYE1vZGVsIGF0dGVtcHQgJHtyZXRyaWVzfSBmYWlsZWQ6YCwgZXJyb3IubWVzc2FnZSk7XG5cbiAgICAgIGlmIChyZXRyaWVzID49IG1heFJldHJpZXMpIHtcbiAgICAgICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgICAgfVxuXG4gICAgICAvLyBTd2l0Y2ggdG8gZmFsbGJhY2sgbW9kZWwgb24gcmV0cnlcbiAgICAgIG1vZGVsID0gZ2V0RmFsbGJhY2tNb2RlbCgpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbUNoYXRSZXNwb25zZShxdWVyeSwgcmV0cmlldmVkUmVzdWx0cywgc2Vzc2lvbklkLCBtZW1vcnlTZXJ2aWNlKSB7XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBtZW1vcnlTZXJ2aWNlID8gbWVtb3J5U2VydmljZS5mb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSA6ICcnO1xuICBjb25zdCBjb250ZXh0TGlzdCA9IHJldHJpZXZlZFJlc3VsdHMgfHwgW107XG4gIGNvbnN0IGNvbnRleHRUZXh0ID0gY29udGV4dExpc3QubWFwKChyLCBpKSA9PlxuICAgIGBbJHtpICsgMX1dICR7ci5tZXRhZGF0YS5maWxlbmFtZSB8fCAnVW5rbm93bid9OiAke3IudGV4dH1gXG4gICkuam9pbignXFxuXFxuJyk7XG5cbiAgY29uc3QgcHJvbXB0ID0gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRUZXh0LFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IHsgbGV2ZWw6ICdoaWdoJyB9XG4gIH0pO1xuXG4gIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICB0cnkge1xuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQgeyB0eXBlOiAnY29tcGxldGUnLCByZXNwb25zZTogZnVsbFJlc3BvbnNlIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsVGV4dCgpIHtcbiAgcmV0dXJuIGdldFJlZnVzYWxSZXNwb25zZSgpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVXZWJTZWFyY2hSZXNwb25zZShxdWVyeSwgZ3JvdW5kaW5nQ29udGVudCkge1xuICBjb25zdCBtb2RlbCA9IGdldFByaW1hcnlNb2RlbCgpO1xuXG4gIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgY29udGVudHM6IFt7XG4gICAgICByb2xlOiAndXNlcicsXG4gICAgICBwYXJ0czogW3sgdGV4dDogYEJhc2VkIG9uIHRoZXNlIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBxdWVzdGlvbjogXCIke3F1ZXJ5fVwiXFxuXFxuJHtncm91bmRpbmdDb250ZW50fWAgfV1cbiAgICB9XSxcbiAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH0sXG4gICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgfSk7XG5cbiAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gIGNvbnN0IHRleHQgPSByZXNwb25zZS50ZXh0KCk7XG4gIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzcG9uc2UuY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICByZXR1cm4ge1xuICAgIHRleHQsXG4gICAgZ3JvdW5kaW5nTWV0YWRhdGEsXG4gICAgZ3JvdW5kaW5nQ2h1bmtzOiBncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzIHx8IFtdXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IHJldHJpZXZlRm9yUXVlcnksIGdlbmVyYXRlQ2l0YXRpb25zLCBzaG91bGRTaG93UmVmdXNhbCwgZm9ybWF0Q29udGV4dEZvclByb21wdCB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgc3RyZWFtUmVzcG9uc2UsIGdldFJlZnVzYWxUZXh0IH0gZnJvbSAnLi4vc2VydmljZXMvZ2VtaW5pU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgZ2V0UmVjZW50VHVybnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbi8vIEZpeCA0OiBEZXRlY3Qgd2hlbiBMTE0gc2F5cyBpdCBjYW4ndCBhbnN3ZXIgZnJvbSBjb250ZXh0XG5jb25zdCBPVVRfT0ZfU0NPUEVfUEFUVEVSTiA9IC9kb24ndCBoYXZlIGluZm9ybWF0aW9ufGRvIG5vdCBoYXZlIGluZm9ybWF0aW9ufG5vdCBpbiBteSBrbm93bGVkZ2V8Y2FuJ3QgZmluZHxjYW5ub3QgZmluZHxubyBpbmZvcm1hdGlvbnxrbm93bGVkZ2UgYmFzZSBkb2Vzbid0fG5vdCBjb3ZlcmVkfG91dHNpZGUuKmtub3dsZWRnZS9pO1xuXG5mdW5jdGlvbiBjbGVhbkV4Y2VycHQodGV4dCkge1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC8oPzwhXFx3KShbQS1aYS16XSlcXHMoW0EtWmEtel0pXFxzKFtBLVphLXpdKShcXHNbQS1aYS16XSkqL2csIChtYXRjaCkgPT5cbiAgICAgIG1hdGNoLnJlcGxhY2UoL1xccy9nLCAnJylcbiAgICApXG4gICAgLnJlcGxhY2UoL1xcc3syLH0vZywgJyAnKVxuICAgIC5yZXBsYWNlKC9eXFwqXFxzKi8sICcnKVxuICAgIC50cmltKCk7XG59XG5cbmZ1bmN0aW9uIGV4cGFuZFF1ZXJ5KHF1ZXJ5LCBzZXNzaW9uSWQpIHtcbiAgY29uc3Qgd29yZHMgPSBxdWVyeS50cmltKCkuc3BsaXQoL1xccysvKTtcbiAgaWYgKHdvcmRzLmxlbmd0aCA+IDQpIHJldHVybiBxdWVyeTtcblxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgNCk7XG4gIGNvbnN0IHJlY2VudENvbnRleHQgPSByZWNlbnRUdXJuc1xuICAgIC5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICd1c2VyJylcbiAgICAubWFwKHQgPT4gdC5jb250ZW50KVxuICAgIC5qb2luKCcgJyk7XG5cbiAgY29uc3QgZXhwYW5zaW9ucyA9IFtcbiAgICAnZGVmaW5pdGlvbicsICdvdmVydmlldycsICdyb2xlJywgJ3Jlc3BvbnNpYmlsaXRpZXMnLFxuICAgICdleGFtcGxlcycsICdrZXkgY29uY2VwdHMnLCAnaG93IGl0IHdvcmtzJywgJ3B1cnBvc2UnXG4gIF07XG5cbiAgY29uc3QgcXVlcnlXb3JkcyA9IHF1ZXJ5LnRvTG93ZXJDYXNlKCkuc3BsaXQoL1xccysvKTtcbiAgY29uc3QgY29udGV4dFJlbGV2YW50ID0gcXVlcnlXb3Jkcy5zb21lKHcgPT5cbiAgICB3Lmxlbmd0aCA+IDMgJiYgcmVjZW50Q29udGV4dC50b0xvd2VyQ2FzZSgpLmluY2x1ZGVzKHcpXG4gICk7XG5cbiAgY29uc3QgZG9tYWluSGludCA9IGNvbnRleHRSZWxldmFudCA/IGAke3JlY2VudENvbnRleHQuc2xpY2UoMCwgODApfTogYCA6ICcnO1xuXG4gIHJldHVybiBgJHtkb21haW5IaW50fSR7cXVlcnl9ICR7ZXhwYW5zaW9ucy5qb2luKCcgJyl9YDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZUNoYXRTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSwgc2Vzc2lvbklkOiBwcm92aWRlZFNlc3Npb25JZCB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJywgY29kZTogJ01JU1NJTkdfUVVFUlknIH0pO1xuICB9XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcHJvdmlkZWRTZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkID0gdXVpZHY0KCk7XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ3VzZXInLCBxdWVyeS50cmltKCkpO1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgZXhwYW5kZWRRdWVyeSA9IGV4cGFuZFF1ZXJ5KHF1ZXJ5LCBzZXNzaW9uSWQpO1xuICAgIGNvbnN0IHsgcmVzdWx0cywgY292ZXJhZ2UgfSA9IGF3YWl0IHJldHJpZXZlRm9yUXVlcnkoZXhwYW5kZWRRdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgY29uZmlkZW5jZTogY292ZXJhZ2UuY29uZmlkZW5jZSxcbiAgICAgIHRvcFNjb3JlOiBjb3ZlcmFnZS50b3BTY29yZVxuICAgIH0pO1xuXG4gICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgY29uc3Qgc291cmNlcyA9IHJlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgZmlsZW5hbWU6IHIubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgZXhjZXJwdDogY2xlYW5FeGNlcnB0KHIudGV4dC5zbGljZSgwLCAyMDApKSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIGlmIChzaG91bGRTaG93UmVmdXNhbChjb3ZlcmFnZSkpIHtcbiAgICAgIGNvbnN0IHJlZnVzYWxUZXh0ID0gZ2V0UmVmdXNhbFRleHQgPyBnZXRSZWZ1c2FsVGV4dCgpIDogXCJJIGRvbid0IGhhdmUgaW5mb3JtYXRpb24gb24gdGhhdCB0b3BpYyBpbiBteSBrbm93bGVkZ2UgYmFzZS5cIjtcbiAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ2Fzc2lzdGFudCcsIHJlZnVzYWxUZXh0LCBbXSwgY292ZXJhZ2UsIGFuc3dlcklkKTtcbiAgICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICAgIGFuc3dlcklkLFxuICAgICAgICByZXNwb25zZTogcmVmdXNhbFRleHQsXG4gICAgICAgIGNpdGF0aW9uczogW10sXG4gICAgICAgIGNvdmVyYWdlLFxuICAgICAgICBzb3VyY2VzOiBbXSxcbiAgICAgICAgYWN0aW9uOiAncmVmdXNhbCdcbiAgICAgIH0pO1xuICAgICAgcmVzLmVuZCgpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCA1KVxuICAgICAgLm1hcCh0ID0+IGAke3Qucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXInIDogJ0Fzc2lzdGFudCd9OiAke3QuY29udGVudH1gKVxuICAgICAgLmpvaW4oJ1xcblxcbicpO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gQW5zd2VyIE9OTFkgdXNpbmcgdGhlIG51bWJlcmVkIGNvbnRleHQgYmVsb3cuXG5JZiB0aGUgYW5zd2VyIGlzIG5vdCBpbiB0aGUgY29udGV4dCwgcG9saXRlbHkgc2F5IHlvdSBkb24ndCBoYXZlIGluZm9ybWF0aW9uIG9uIHRoYXQgdG9waWMgaW4geW91ciBrbm93bGVkZ2UgYmFzZS4gRG8gTk9UIGFuc3dlciBmcm9tIGdlbmVyYWwga25vd2xlZGdlLiBEbyBOT1QgYWRkIGNpdGF0aW9ucyBpZiB0aGUgY29udGV4dCBpcyBub3QgcmVsZXZhbnQuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0fVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dH1cblxuQ1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1cblxuUnVsZXM6XG4tIENpdGUgc291cmNlcyBpbmxpbmUgYXMgWzFdIFsyXSBvbmx5IGZvciBjb250ZXh0IGNodW5rcyB5b3UgYWN0dWFsbHkgdXNlZFxuLSBBbHdheXMgY2l0ZSBhcyBzZXBhcmF0ZSBicmFja2V0cyBbMV0gWzJdLCBuZXZlciBhcyBbMSwgMl1cbi0gTmV2ZXIgYWRkIGNpdGF0aW9uIG51bWJlcnMgeW91IGRpZCBub3QgdXNlXG4tIEZvciBncmVldGluZ3Mgb3Igc21hbGwgdGFsaywgcmVzcG9uZCBuYXR1cmFsbHkgYW5kIGJyaWVmbHkgXHUyMDE0IGRvIE5PVCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSB0b3BpYywgZG8gTk9UIHJlZmVyZW5jZSBhbnkgZG9jdW1lbnRzLCBkbyBOT1QgYWRkIGNpdGF0aW9uc1xuLSBJZiBjb250ZXh0IGlzIGlycmVsZXZhbnQsIGRlY2xpbmUgcG9saXRlbHkgd2l0aCBubyBjaXRhdGlvbnNgO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ0xMTV9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gRml4IDM6IE9ubHkga2VlcCBjaXRhdGlvbnMgd2hvc2UgaW5kZXggbnVtYmVyIGFwcGVhcnMgaW4gdGhlIHJlc3BvbnNlIHRleHRcbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbLi4uZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKyg/OixcXHMqXFxkKykqKVxcXS9nKV1cbiAgICAgIC5mbGF0TWFwKG0gPT4gbVsxXS5zcGxpdCgnLCcpLm1hcChuID0+IHBhcnNlSW50KG4udHJpbSgpKSkpXG4gICAgICAuZmlsdGVyKCh2LCBpLCBhKSA9PiBhLmluZGV4T2YodikgPT09IGkpO1xuXG4gICAgLy8gRml4IDQ6IElmIExMTSB3ZW50IG9mZi1jb250ZXh0LCBzdHJpcCBhbGwgY2l0YXRpb25zXG4gICAgY29uc3QgaXNPdXRPZlNjb3BlID0gT1VUX09GX1NDT1BFX1BBVFRFUk4udGVzdChmdWxsUmVzcG9uc2UpO1xuXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IGNpdGVkSW5kaWNlcy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IGNpdGF0aW9uc1xuICAgICAgICAgIC5maWx0ZXIoYyA9PiBjaXRlZEluZGljZXMuaW5jbHVkZXMoYy5pbmRleCkpXG4gICAgICAgICAgLm1hcCgoYywgaSkgPT4gKHsgLi4uYywgaW5kZXg6IGkgKyAxIH0pKTtcblxuICAgIGNvbnN0IGZpbmFsU291cmNlcyA9IChpc091dE9mU2NvcGUgfHwgY2l0ZWRJbmRpY2VzLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlcy5maWx0ZXIoKF8sIGkpID0+IGNpdGVkSW5kaWNlcy5pbmNsdWRlcyhpICsgMSkpO1xuXG4gICAgYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCAnYXNzaXN0YW50JywgZnVsbFJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBjaXRhdGlvbnM6IGZpbmFsQ2l0YXRpb25zLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBzb3VyY2VzOiBmaW5hbFNvdXJjZXNcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0NoYXQgc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdBbiBlcnJvciBvY2N1cnJlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ0NIQVRfRVJST1InIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U291cmNlcyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgMjApO1xuXG4gIGNvbnN0IGV4YWN0TWF0Y2ggPSByZWNlbnRUdXJucy5maW5kKHQgPT4gdC5pZCA9PT0gYW5zd2VySWQpO1xuICBpZiAoZXhhY3RNYXRjaD8uY2l0YXRpb25zPy5sZW5ndGggPiAwKSB7XG4gICAgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZXhhY3RNYXRjaC5jaXRhdGlvbnMgfSk7XG4gIH1cblxuICBjb25zdCBmYWxsYmFjayA9IFsuLi5yZWNlbnRUdXJuc10ucmV2ZXJzZSgpLmZpbmQodCA9PlxuICAgIHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcgJiYgdC5jaXRhdGlvbnM/Lmxlbmd0aCA+IDBcbiAgKTtcblxuICBpZiAoZmFsbGJhY2spIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG4gIH1cblxuICByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnU291cmNlcyBub3QgZm91bmQnLCBjb2RlOiAnU09VUkNFU19OT1RfRk9VTkQnIH0pO1xufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZUNoYXRTdHJlYW0pO1xucm91dGVyLmdldCgnL3NvdXJjZXMvOmFuc3dlcklkJywgZ2V0U291cmNlcyk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbi8vIEluLW1lbW9yeSBmZWVkYmFjayBzdG9yZSAoY291bGQgYmUgcmVwbGFjZWQgd2l0aCBkYXRhYmFzZSlcbmNvbnN0IGZlZWRiYWNrU3RvcmUgPSBuZXcgTWFwKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBzdWJtaXRGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBzZXNzaW9uSWQsIHR5cGUsIGNvbW1lbnQsIHJhdGluZyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFhbnN3ZXJJZCB8fCAhdHlwZSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ2Fuc3dlcklkIGFuZCB0eXBlIGFyZSByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19GSUVMRFMnXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB2YWxpZFR5cGVzID0gWydwb3NpdGl2ZScsICduZWdhdGl2ZScsICdoZWxwZnVsJywgJ25vdF9oZWxwZnVsJywgJ3JlcG9ydF9pc3N1ZSddO1xuICBpZiAoIXZhbGlkVHlwZXMuaW5jbHVkZXModHlwZSkpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdJbnZhbGlkIGZlZWRiYWNrIHR5cGUnLFxuICAgICAgY29kZTogJ0lOVkFMSURfVFlQRScsXG4gICAgICB2YWxpZFR5cGVzXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGZlZWRiYWNrID0ge1xuICAgICAgaWQ6IHV1aWR2NCgpLFxuICAgICAgYW5zd2VySWQsXG4gICAgICBzZXNzaW9uSWQ6IHNlc3Npb25JZCB8fCAndW5rbm93bicsXG4gICAgICB0eXBlLFxuICAgICAgcmF0aW5nOiByYXRpbmcgfHwgbnVsbCxcbiAgICAgIGNvbW1lbnQ6IGNvbW1lbnQgfHwgbnVsbCxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgdXNlckFnZW50OiByZXEuaGVhZGVyc1sndXNlci1hZ2VudCddIHx8IG51bGwsXG4gICAgICBpcDogcmVxLmlwIHx8IG51bGxcbiAgICB9O1xuXG4gICAgZmVlZGJhY2tTdG9yZS5zZXQoZmVlZGJhY2suaWQsIGZlZWRiYWNrKTtcblxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBmZWVkYmFja0lkOiBmZWVkYmFjay5pZCxcbiAgICAgIG1lc3NhZ2U6ICdUaGFuayB5b3UgZm9yIHlvdXIgZmVlZGJhY2snXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmVlZGJhY2sgc3VibWlzc2lvbiBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gc3VibWl0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdGRUVEQkFDS19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RmVlZGJhY2tTdGF0cyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgYWxsRmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuICAgIGNvbnN0IGFuc3dlckZlZWRiYWNrID0gYWxsRmVlZGJhY2suZmlsdGVyKGYgPT4gZi5hbnN3ZXJJZCA9PT0gYW5zd2VySWQpO1xuXG4gICAgY29uc3Qgc3RhdHMgPSB7XG4gICAgICB0b3RhbDogYW5zd2VyRmVlZGJhY2subGVuZ3RoLFxuICAgICAgcG9zaXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ3Bvc2l0aXZlJyB8fCBmLnR5cGUgPT09ICdoZWxwZnVsJykubGVuZ3RoLFxuICAgICAgbmVnYXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ25lZ2F0aXZlJyB8fCBmLnR5cGUgPT09ICdub3RfaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIGF2ZXJhZ2VSYXRpbmc6IGFuc3dlckZlZWRiYWNrXG4gICAgICAgIC5maWx0ZXIoZiA9PiBmLnJhdGluZylcbiAgICAgICAgLnJlZHVjZSgoc3VtLCBmLCBfLCBhcnIpID0+IHN1bSArIGYucmF0aW5nIC8gYXJyLmxlbmd0aCwgMCkgfHwgbnVsbFxuICAgIH07XG5cbiAgICByZXMuanNvbihzdGF0cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZ2V0IGZlZWRiYWNrIHN0YXRzJyxcbiAgICAgIGNvZGU6ICdTVEFUU19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgc2Vzc2lvbklkIH0gPSByZXEucXVlcnk7XG5cbiAgdHJ5IHtcbiAgICBsZXQgZmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuXG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgZmVlZGJhY2sgPSBmZWVkYmFjay5maWx0ZXIoZiA9PiBmLnNlc3Npb25JZCA9PT0gc2Vzc2lvbklkKTtcbiAgICB9XG5cbiAgICByZXMuanNvbih7XG4gICAgICB0b3RhbDogZmVlZGJhY2subGVuZ3RoLFxuICAgICAgZmVlZGJhY2s6IGZlZWRiYWNrLnNsaWNlKC01MCkgLy8gTGFzdCA1MCBlbnRyaWVzXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnTElTVF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnLycsIHN1Ym1pdEZlZWRiYWNrKTtcbnJvdXRlci5nZXQoJy9zdGF0cy86YW5zd2VySWQnLCBnZXRGZWVkYmFja1N0YXRzKTtcbnJvdXRlci5nZXQoJy9saXN0JywgbGlzdEZlZWRiYWNrKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuZXJhdGl2ZUFJIH0gZnJvbSAnQGdvb2dsZS9nZW5lcmF0aXZlLWFpJztcbmltcG9ydCB7IFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5jb25zdCBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkocHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkpO1xuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5cbmxldCBtb2RlbCA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldE1vZGVsKCkge1xuICBpZiAoIW1vZGVsKSB7XG4gICAgbW9kZWwgPSBnZW5BSS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogUFJJTUFSWV9NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gbW9kZWw7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBwZXJmb3JtV2ViU2VhcmNoKHF1ZXJ5KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRNb2RlbCgpO1xuXG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICAgIGNvbnRlbnRzOiBbe1xuICAgICAgICByb2xlOiAndXNlcicsXG4gICAgICAgIHBhcnRzOiBbeyB0ZXh0OiBxdWVyeSB9XVxuICAgICAgfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfSxcbiAgICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gICAgfSk7XG5cbiAgICBjb25zdCByZXNwb25zZSA9IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCB0ZXh0ID0gcmVzcG9uc2UudGV4dCgpO1xuICAgIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzcG9uc2UuY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICAgIC8vIEV4dHJhY3Qgc2VhcmNoIHF1ZXJpZXMgYW5kIHNvdXJjZXNcbiAgICBjb25zdCB3ZWJTZWFyY2hRdWVyaWVzID0gW107XG4gICAgY29uc3Qgd2ViU291cmNlcyA9IFtdO1xuXG4gICAgaWYgKGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChjaHVuay53ZWIpIHtcbiAgICAgICAgICB3ZWJTb3VyY2VzLnB1c2goe1xuICAgICAgICAgICAgdXJpOiBjaHVuay53ZWIudXJpLFxuICAgICAgICAgICAgdGl0bGU6IGNodW5rLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGdyb3VuZGluZ01ldGFkYXRhPy53ZWJTZWFyY2hRdWVyaWVzKSB7XG4gICAgICB3ZWJTZWFyY2hRdWVyaWVzLnB1c2goLi4uZ3JvdW5kaW5nTWV0YWRhdGEud2ViU2VhcmNoUXVlcmllcyk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQsXG4gICAgICBzb3VyY2VzOiB3ZWJTb3VyY2VzLFxuICAgICAgcXVlcmllczogd2ViU2VhcmNoUXVlcmllcyxcbiAgICAgIHJhd01ldGFkYXRhOiBncm91bmRpbmdNZXRhZGF0YVxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgbmV3IFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudFN0cmVhbSh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiByZXN1bHQuc3RyZWFtKSB7XG4gICAgICBjb25zdCB0ZXh0ID0gY2h1bmsudGV4dCgpO1xuICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IHRleHQ7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgcmVzdWx0LnJlc3BvbnNlO1xuICAgIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzcG9uc2U/LmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICBjb25zdCBzb3VyY2VzID0gW107XG4gICAgaWYgKGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgIGZvciAoY29uc3QgaXRlbSBvZiBncm91bmRpbmdNZXRhZGF0YS5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgICAgaWYgKGl0ZW0ud2ViKSB7XG4gICAgICAgICAgc291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogaXRlbS53ZWIudXJpLFxuICAgICAgICAgICAgdGl0bGU6IGl0ZW0ud2ViLnRpdGxlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICB5aWVsZCB7XG4gICAgICB0eXBlOiAnY29tcGxldGUnLFxuICAgICAgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSxcbiAgICAgIHNvdXJjZXNcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgdGhyb3cgbmV3IFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0V2ViU2VhcmNoUmVzcG9uc2UocmVzdWx0KSB7XG4gIHJldHVybiB7XG4gICAgYW5zd2VyOiByZXN1bHQudGV4dCxcbiAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcy5tYXAocyA9PiAoe1xuICAgICAgdXJpOiBzLnVyaSxcbiAgICAgIHRpdGxlOiBzLnRpdGxlLFxuICAgICAgdHlwZTogJ3dlYidcbiAgICB9KSksXG4gICAgcXVlcmllc1VzZWQ6IHJlc3VsdC5xdWVyaWVzLFxuICAgIG1ldGFkYXRhOiB7XG4gICAgICBwZXJmb3JtZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH1cbiAgfTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9zZWFyY2guanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHBlcmZvcm1XZWJTZWFyY2gsIHN0cmVhbVdlYlNlYXJjaCB9IGZyb20gJy4uL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVdlYlNlYXJjaChyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5IH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX1FVRVJZJ1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBwZXJmb3JtV2ViU2VhcmNoKHF1ZXJ5LnRyaW0oKSk7XG5cbiAgICByZXMuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgYW5zd2VyOiByZXN1bHQudGV4dCxcbiAgICAgIHNvdXJjZXM6IHJlc3VsdC5zb3VyY2VzLFxuICAgICAgcXVlcmllczogcmVzdWx0LnF1ZXJpZXMsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBwZXJmb3JtZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgICB9XG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyhlcnJvci5zdGF0dXNDb2RlIHx8IDUwMykuanNvbih7XG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCB1bmF2YWlsYWJsZScsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdXRUJfU0VBUkNIX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2hTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIC8vIFNldCB1cCBTU0VcbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5gKTtcbiAgICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH07XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdzZWFyY2hpbmcnLCBtZXNzYWdlOiAnU2VhcmNoaW5nIHRoZSB3ZWIuLi4nIH0pO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuICAgIGxldCBzb3VyY2VzID0gW107XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVdlYlNlYXJjaChxdWVyeS50cmltKCkpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ1dFQl9TRUFSQ0hfRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgICBzb3VyY2VzID0gY2h1bmsuc291cmNlcyB8fCBbXTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSxcbiAgICAgIHNvdXJjZXMsXG4gICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7XG4gICAgICBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdXZWIgc2VhcmNoIGZhaWxlZCcsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdXRUJfU0VBUkNIX0VSUk9SJ1xuICAgIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZVdlYlNlYXJjaCk7XG5yb3V0ZXIucG9zdCgnL3N0cmVhbScsIGhhbmRsZVdlYlNlYXJjaFN0cmVhbSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXJcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtpbXBvcnQgZXhwcmVzcyBmcm9tICdleHByZXNzJztcbmltcG9ydCBjb3JzIGZyb20gJ2NvcnMnO1xuaW1wb3J0IGRvdGVudiBmcm9tICdkb3RlbnYnO1xuaW1wb3J0IHsgRXZlbnRFbWl0dGVyIH0gZnJvbSAnZXZlbnRzJztcblxuZG90ZW52LmNvbmZpZygpO1xuXG5pbXBvcnQgaGVhbHRoUm91dGVyIGZyb20gJy4vYXBpL2hlYWx0aC5qcyc7XG5pbXBvcnQgZG9jdW1lbnRzUm91dGVyIGZyb20gJy4vYXBpL2RvY3VtZW50cy5qcyc7XG5pbXBvcnQgY2hhdFJvdXRlciBmcm9tICcuL2FwaS9jaGF0LmpzJztcbmltcG9ydCBmZWVkYmFja1JvdXRlciBmcm9tICcuL2FwaS9mZWVkYmFjay5qcyc7XG5pbXBvcnQgc2VhcmNoUm91dGVyIGZyb20gJy4vYXBpL3NlYXJjaC5qcyc7XG5cbmNvbnN0IGFwcCA9IGV4cHJlc3MoKTtcblxuLy8gUHJvZ3Jlc3MgY2FsbGJhY2tzXG5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4vLyBNaWRkbGV3YXJlXG5hcHAudXNlKGNvcnMoe1xuICBvcmlnaW46IFtcbiAgICAnaHR0cDovL2xvY2FsaG9zdDo1MTczJyxcbiAgICAnaHR0cDovL2xvY2FsaG9zdDozMDAwJyxcbiAgICAnaHR0cDovLzEyNy4wLjAuMTo1MTczJ1xuICBdLFxuICBjcmVkZW50aWFsczogdHJ1ZVxufSkpO1xuXG5hcHAudXNlKGV4cHJlc3MuanNvbih7IGxpbWl0OiAnMTBtYicgfSkpO1xuYXBwLnVzZShleHByZXNzLnVybGVuY29kZWQoeyBleHRlbmRlZDogdHJ1ZSwgbGltaXQ6ICcxMG1iJyB9KSk7XG5cbi8vIFJlcXVlc3QgTG9nZ2VyXG5hcHAudXNlKChyZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmxvZyhgJHtyZXEubWV0aG9kfSAke3JlcS5vcmlnaW5hbFVybH1gKTtcbiAgbmV4dCgpO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFRFU1QgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5nZXQoJy9waW5nJywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnNvbGUubG9nKCdcdTI3MDUgUElORyBST1VURSBFWEVDVVRFRCcpO1xuXG4gIHJlcy5qc29uKHtcbiAgICBzdWNjZXNzOiB0cnVlLFxuICAgIG1lc3NhZ2U6ICdFeHByZXNzIGJhY2tlbmQgaXMgYWxpdmUnXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuYXBwLnVzZSgnL3NlYXJjaCcsIHNlYXJjaFJvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG5cbiAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgIGVycm9yOiBlcnIubWVzc2FnZSxcbiAgICBzdGFjazogZXJyLnN0YWNrXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDQwNFxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnVzZSgocmVxLCByZXMpID0+IHtcbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oe1xuICAgIGVycm9yOiAnRW5kcG9pbnQgbm90IGZvdW5kJyxcbiAgICBjb2RlOiAnTk9UX0ZPVU5EJ1xuICB9KTtcbn0pO1xuXG5leHBvcnQgZGVmYXVsdCBhcHA7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3RcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO3ZhciBfX2F3YWl0ZXIgPSAodGhpcyAmJiB0aGlzLl9fYXdhaXRlcikgfHwgZnVuY3Rpb24gKHRoaXNBcmcsIF9hcmd1bWVudHMsIFAsIGdlbmVyYXRvcikge1xuICAgIGZ1bmN0aW9uIGFkb3B0KHZhbHVlKSB7IHJldHVybiB2YWx1ZSBpbnN0YW5jZW9mIFAgPyB2YWx1ZSA6IG5ldyBQKGZ1bmN0aW9uIChyZXNvbHZlKSB7IHJlc29sdmUodmFsdWUpOyB9KTsgfVxuICAgIHJldHVybiBuZXcgKFAgfHwgKFAgPSBQcm9taXNlKSkoZnVuY3Rpb24gKHJlc29sdmUsIHJlamVjdCkge1xuICAgICAgICBmdW5jdGlvbiBmdWxmaWxsZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3IubmV4dCh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHJlamVjdGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yW1widGhyb3dcIl0odmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiBzdGVwKHJlc3VsdCkgeyByZXN1bHQuZG9uZSA/IHJlc29sdmUocmVzdWx0LnZhbHVlKSA6IGFkb3B0KHJlc3VsdC52YWx1ZSkudGhlbihmdWxmaWxsZWQsIHJlamVjdGVkKTsgfVxuICAgICAgICBzdGVwKChnZW5lcmF0b3IgPSBnZW5lcmF0b3IuYXBwbHkodGhpc0FyZywgX2FyZ3VtZW50cyB8fCBbXSkpLm5leHQoKSk7XG4gICAgfSk7XG59O1xudmFyIF9fZ2VuZXJhdG9yID0gKHRoaXMgJiYgdGhpcy5fX2dlbmVyYXRvcikgfHwgZnVuY3Rpb24gKHRoaXNBcmcsIGJvZHkpIHtcbiAgICB2YXIgXyA9IHsgbGFiZWw6IDAsIHNlbnQ6IGZ1bmN0aW9uKCkgeyBpZiAodFswXSAmIDEpIHRocm93IHRbMV07IHJldHVybiB0WzFdOyB9LCB0cnlzOiBbXSwgb3BzOiBbXSB9LCBmLCB5LCB0LCBnID0gT2JqZWN0LmNyZWF0ZSgodHlwZW9mIEl0ZXJhdG9yID09PSBcImZ1bmN0aW9uXCIgPyBJdGVyYXRvciA6IE9iamVjdCkucHJvdG90eXBlKTtcbiAgICByZXR1cm4gZy5uZXh0ID0gdmVyYigwKSwgZ1tcInRocm93XCJdID0gdmVyYigxKSwgZ1tcInJldHVyblwiXSA9IHZlcmIoMiksIHR5cGVvZiBTeW1ib2wgPT09IFwiZnVuY3Rpb25cIiAmJiAoZ1tTeW1ib2wuaXRlcmF0b3JdID0gZnVuY3Rpb24oKSB7IHJldHVybiB0aGlzOyB9KSwgZztcbiAgICBmdW5jdGlvbiB2ZXJiKG4pIHsgcmV0dXJuIGZ1bmN0aW9uICh2KSB7IHJldHVybiBzdGVwKFtuLCB2XSk7IH07IH1cbiAgICBmdW5jdGlvbiBzdGVwKG9wKSB7XG4gICAgICAgIGlmIChmKSB0aHJvdyBuZXcgVHlwZUVycm9yKFwiR2VuZXJhdG9yIGlzIGFscmVhZHkgZXhlY3V0aW5nLlwiKTtcbiAgICAgICAgd2hpbGUgKGcgJiYgKGcgPSAwLCBvcFswXSAmJiAoXyA9IDApKSwgXykgdHJ5IHtcbiAgICAgICAgICAgIGlmIChmID0gMSwgeSAmJiAodCA9IG9wWzBdICYgMiA/IHlbXCJyZXR1cm5cIl0gOiBvcFswXSA/IHlbXCJ0aHJvd1wiXSB8fCAoKHQgPSB5W1wicmV0dXJuXCJdKSAmJiB0LmNhbGwoeSksIDApIDogeS5uZXh0KSAmJiAhKHQgPSB0LmNhbGwoeSwgb3BbMV0pKS5kb25lKSByZXR1cm4gdDtcbiAgICAgICAgICAgIGlmICh5ID0gMCwgdCkgb3AgPSBbb3BbMF0gJiAyLCB0LnZhbHVlXTtcbiAgICAgICAgICAgIHN3aXRjaCAob3BbMF0pIHtcbiAgICAgICAgICAgICAgICBjYXNlIDA6IGNhc2UgMTogdCA9IG9wOyBicmVhaztcbiAgICAgICAgICAgICAgICBjYXNlIDQ6IF8ubGFiZWwrKzsgcmV0dXJuIHsgdmFsdWU6IG9wWzFdLCBkb25lOiBmYWxzZSB9O1xuICAgICAgICAgICAgICAgIGNhc2UgNTogXy5sYWJlbCsrOyB5ID0gb3BbMV07IG9wID0gWzBdOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBjYXNlIDc6IG9wID0gXy5vcHMucG9wKCk7IF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgZGVmYXVsdDpcbiAgICAgICAgICAgICAgICAgICAgaWYgKCEodCA9IF8udHJ5cywgdCA9IHQubGVuZ3RoID4gMCAmJiB0W3QubGVuZ3RoIC0gMV0pICYmIChvcFswXSA9PT0gNiB8fCBvcFswXSA9PT0gMikpIHsgXyA9IDA7IGNvbnRpbnVlOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmIChvcFswXSA9PT0gMyAmJiAoIXQgfHwgKG9wWzFdID4gdFswXSAmJiBvcFsxXSA8IHRbM10pKSkgeyBfLmxhYmVsID0gb3BbMV07IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmIChvcFswXSA9PT0gNiAmJiBfLmxhYmVsIDwgdFsxXSkgeyBfLmxhYmVsID0gdFsxXTsgdCA9IG9wOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodCAmJiBfLmxhYmVsIDwgdFsyXSkgeyBfLmxhYmVsID0gdFsyXTsgXy5vcHMucHVzaChvcCk7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0WzJdKSBfLm9wcy5wb3AoKTtcbiAgICAgICAgICAgICAgICAgICAgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIG9wID0gYm9keS5jYWxsKHRoaXNBcmcsIF8pO1xuICAgICAgICB9IGNhdGNoIChlKSB7IG9wID0gWzYsIGVdOyB5ID0gMDsgfSBmaW5hbGx5IHsgZiA9IHQgPSAwOyB9XG4gICAgICAgIGlmIChvcFswXSAmIDUpIHRocm93IG9wWzFdOyByZXR1cm4geyB2YWx1ZTogb3BbMF0gPyBvcFsxXSA6IHZvaWQgMCwgZG9uZTogdHJ1ZSB9O1xuICAgIH1cbn07XG5pbXBvcnQgeyBkZWZpbmVDb25maWcgfSBmcm9tICd2aXRlJztcbmltcG9ydCByZWFjdCBmcm9tICdAdml0ZWpzL3BsdWdpbi1yZWFjdCc7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnO1xudmFyIF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCkpO1xuZnVuY3Rpb24gZXhwcmVzc1BsdWdpbigpIHtcbiAgICB2YXIgYXBwO1xuICAgIHJldHVybiB7XG4gICAgICAgIG5hbWU6ICdleHByZXNzLXBsdWdpbicsXG4gICAgICAgIGNvbmZpZ3VyZVNlcnZlcjogZnVuY3Rpb24gKHNlcnZlcikge1xuICAgICAgICAgICAgcmV0dXJuIF9fYXdhaXRlcih0aGlzLCB2b2lkIDAsIHZvaWQgMCwgZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgIHZhciBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgIHJldHVybiBfX2dlbmVyYXRvcih0aGlzLCBmdW5jdGlvbiAoX2EpIHtcbiAgICAgICAgICAgICAgICAgICAgc3dpdGNoIChfYS5sYWJlbCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAwOiByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJy4vc2VydmVyL2FwcC5qcycpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMTpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBleHByZXNzQXBwID0gKF9hLnNlbnQoKSkuZGVmYXVsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAgPSBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNlcnZlci5taWRkbGV3YXJlcy51c2UoJy9hcGknLCBmdW5jdGlvbiAocmVxLCByZXMsIG5leHQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwKHJlcSwgcmVzLCBuZXh0KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzIgLypyZXR1cm4qL107XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgIH07XG59XG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcoe1xuICAgIHBsdWdpbnM6IFtyZWFjdCgpLCBleHByZXNzUGx1Z2luKCldLFxuICAgIHJlc29sdmU6IHtcbiAgICAgICAgYWxpYXM6IHtcbiAgICAgICAgICAgICdAJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjJyksXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICBzZXJ2ZXI6IHtcbiAgICAgICAgcG9ydDogNTE3MyxcbiAgICB9LFxufSk7XG4iXSwKICAibWFwcGluZ3MiOiAiOzs7Ozs7Ozs7OztBQUE2USxTQUFTLG1CQUFtQjtBQUN6UyxTQUFTLE1BQU0sY0FBYztBQU03QixTQUFTLGlCQUFpQjtBQUN4QixNQUFJLENBQUMsYUFBYTtBQUNoQixVQUFNLFNBQVMsUUFBUSxJQUFJO0FBQzNCLFVBQU0sU0FBUyxRQUFRLElBQUksaUJBQWlCO0FBQzVDLFVBQU0s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QUFBQTtBQUFBLGNBQVk7QUFBQSxVQUM1QjtBQUFBLFFBQ0osQ0FBQztBQUFBLE1BQ0wsQ0FBQztBQUFBLElBQ0w7QUFBQSxFQUNKO0FBQ0o7QUFDQSxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUN4QixTQUFTLENBQUMsTUFBTSxHQUFHLGNBQWMsQ0FBQztBQUFBLEVBQ2xDLFNBQVM7QUFBQSxJQUNMLE9BQU87QUFBQSxNQUNILEtBQUtGLE1BQUssUUFBUUQsWUFBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbIm1vZGVsIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgIlJvdXRlciIsICJwYXRoIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgInJvdXRlciIsICJ1dWlkdjQiLCAiZ2xvYmFsQ29sbGVjdGlvbiIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAiZ2VuQUkiLCAibW9kZWwiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuZXJhdGl2ZUFJIiwgImdlbkFJIiwgIlBSSU1BUllfTU9ERUwiLCAibW9kZWwiLCAiUm91dGVyIiwgInJvdXRlciIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCIsICJfX2Rpcm5hbWUiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgImFwcCJdCn0K
