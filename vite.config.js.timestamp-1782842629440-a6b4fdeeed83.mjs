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
    return { collection: sessionCollections.get(sessionId), isNew: false };
  }
  const client = getCloudClient();
  const collectionName = `session_${sessionId}`;
  let collection;
  let isNew;
  try {
    collection = await client.getCollection({
      name: collectionName,
      embeddingFunction: null
    });
    isNew = false;
    console.log(`\u267B\uFE0F  Session collection exists, reusing: ${collectionName}`);
  } catch {
    collection = await client.createCollection({
      name: collectionName,
      metadata: {
        type: "session_upload",
        session_id: sessionId,
        created: (/* @__PURE__ */ new Date()).toISOString()
      },
      embeddingFunction: null
    });
    isNew = true;
    console.log(`\u2705 Session collection created: ${collectionName}`);
  }
  sessionCollections.set(sessionId, collection);
  return { collection, isNew };
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
        outputDimensionality: OUTPUT_DIMENSIONS()
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
          const absoluteChunkIdx = (i + batchIdx) * batchSize + chunkIdx;
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_${absoluteChunkIdx}`,
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
      console.log(`  Waiting ${RETRY_WAIT_MS / 1e3}s before retrying failed batch ${batchIdx}...`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_WAIT_MS));
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
  };
}
var genAI, embeddingModel, BATCH_SIZE, PARALLEL_CALLS, OUTPUT_DIMENSIONS, GROUP_WAIT_MS, RETRY_WAIT_MS;
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
    RETRY_WAIT_MS = 15e3;
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
function createSession(sessionId) {
  const id = sessionId || uuidv42();
  const session = {
    id,
    createdAt: /* @__PURE__ */ new Date(),
    lastAccessed: /* @__PURE__ */ new Date(),
    documents: [],
    timeoutMinutes: DEFAULT_TIMEOUT_MINUTES
  };
  sessions.set(id, session);
  return session;
}
function getSession(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return null;
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
    if (existing) return existing;
    return createSession(sessionId);
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
  seededSessions.delete(sessionId);
}
async function initSessionWithGlobalDocs(sessionId) {
  console.log(`\u{1F511} Session init: ${sessionId}`);
  if (seededSessions.has(sessionId)) return;
  try {
    const globalCollection2 = await getGlobalCollection();
    const { collection: sessionCollection, isNew } = await getSessionCollection(sessionId);
    if (!isNew) {
      console.log(`\u267B\uFE0F  Session exists, reconstructing document list from Chroma...`);
      const session2 = getSession(sessionId);
      if (session2 && session2.documents.length === 0) {
        const docs = await listDocuments(sessionCollection);
        docs.forEach((doc) => {
          session2.documents.push({
            id: doc.document_id,
            filename: doc.filename,
            fileSize: null,
            pageCount: doc.page_count || null,
            chunkCount: doc.chunk_count,
            sourceType: doc.source_type,
            uploadTimestamp: doc.upload_timestamp
          });
        });
        console.log(`\u2705 Reconstructed ${docs.length} document(s) for session ${sessionId}`);
      }
      seededSessions.add(sessionId);
      return;
    }
    console.log(`\u{1F331} New session \u2014 seeding from global collection...`);
    const BATCH_SIZE2 = 300;
    let offset = 0;
    const allIds = [], allEmbeddings = [], allDocuments = [], allMetadatas = [];
    while (true) {
      const batch = await globalCollection2.get({
        include: ["embeddings", "documents", "metadatas"],
        limit: BATCH_SIZE2,
        offset
      });
      if (!batch.ids || batch.ids.length === 0) break;
      allIds.push(...batch.ids);
      allEmbeddings.push(...batch.embeddings);
      allDocuments.push(...batch.documents);
      allMetadatas.push(...batch.metadatas);
      if (batch.ids.length < BATCH_SIZE2) break;
      offset += BATCH_SIZE2;
    }
    if (allIds.length === 0) {
      console.log("\u26A0\uFE0F  Global collection is empty \u2014 nothing to seed.");
      seededSessions.add(sessionId);
      return;
    }
    for (let i = 0; i < allIds.length; i += BATCH_SIZE2) {
      await sessionCollection.add({
        ids: allIds.slice(i, i + BATCH_SIZE2),
        embeddings: allEmbeddings.slice(i, i + BATCH_SIZE2),
        documents: allDocuments.slice(i, i + BATCH_SIZE2),
        metadatas: allMetadatas.slice(i, i + BATCH_SIZE2).map((m) => ({ ...m, source_type: "global" }))
      });
      console.log(`  \u{1F4E6} Added batch ${Math.floor(i / BATCH_SIZE2) + 1}: records ${i + 1}\u2013${Math.min(i + BATCH_SIZE2, allIds.length)}`);
    }
    console.log(`\u2705 Seeded ${allIds.length} vectors into session ${sessionId}`);
    seededSessions.add(sessionId);
    const session = getSession(sessionId);
    if (session) {
      const docsMap = /* @__PURE__ */ new Map();
      allMetadatas.forEach((meta) => {
        if (!docsMap.has(meta.document_id)) {
          docsMap.set(meta.document_id, {
            id: meta.document_id,
            filename: meta.filename,
            fileSize: null,
            pageCount: meta.total_pages || null,
            chunkCount: 0,
            sourceType: "global",
            uploadTimestamp: meta.upload_timestamp
          });
        }
        docsMap.get(meta.document_id).chunkCount++;
      });
      for (const doc of docsMap.values()) {
        if (!session.documents.some((d) => d.id === doc.id)) {
          session.documents.push(doc);
        }
      }
    }
  } catch (error) {
    console.error(`\u274C Failed to seed session ${sessionId}:`, error.message);
  }
}
function addDocumentToSession(sessionId, documentInfo) {
  const session = getSession(sessionId);
  if (!session) return false;
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
  if (!session) return false;
  const idx = session.documents.findIndex((d) => d.id === documentId);
  if (idx >= 0) {
    session.documents.splice(idx, 1);
    session.lastAccessed = /* @__PURE__ */ new Date();
    return true;
  }
  return false;
}
function getAllDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) return { sessionDocuments: [], globalDocuments: [] };
  const normalize = (doc) => ({
    document_id: doc.id,
    filename: doc.filename,
    chunk_count: doc.chunkCount ?? 0,
    page_count: doc.pageCount ?? 0,
    upload_timestamp: doc.uploadTimestamp || null,
    source_type: doc.sourceType === "session_upload" ? "session_upload" : "seed",
    fileSize: doc.fileSize || null
  });
  return {
    sessionDocuments: session.documents.filter((d) => d.sourceType === "session_upload").map(normalize),
    globalDocuments: session.documents.filter((d) => d.sourceType === "global").map(normalize)
  };
}
var DEFAULT_TIMEOUT_MINUTES, sessions, MAX_PDFS_PER_SESSION, MAX_UPLOAD_SIZE_MB, seededSessions;
var init_sessionService = __esm({
  "server/services/sessionService.js"() {
    "use strict";
    init_chromaService();
    DEFAULT_TIMEOUT_MINUTES = 60;
    sessions = /* @__PURE__ */ new Map();
    MAX_PDFS_PER_SESSION = parseInt(process.env.MAX_PDFS_PER_SESSION) || 3;
    MAX_UPLOAD_SIZE_MB = parseInt(process.env.MAX_UPLOAD_SIZE_MB) || 5;
    seededSessions = /* @__PURE__ */ new Set();
  }
});

// server/services/retrievalService.js
import { v4 as uuidv43 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
async function getOrCacheSessionCollection(sessionId) {
  if (cachedSessionCollections.has(sessionId)) {
    return cachedSessionCollections.get(sessionId);
  }
  try {
    const { collection } = await getSessionCollection(sessionId);
    if (collection) cachedSessionCollections.set(sessionId, collection);
    return collection;
  } catch {
    return null;
  }
}
function calculateCoverage(results, topK = TOP_K) {
  if (!results || results.length === 0) return { confidence: 0, topScore: 0 };
  const scores = results.slice(0, topK).map((r) => Math.max(0, r.score));
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  return {
    confidence: Math.round(avgScore * 100),
    topScore: Math.max(...scores)
  };
}
async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || TOP_K;
  try {
    const [queryEmbedding, sessionCollection] = await Promise.all([
      embedQuery(query),
      sessionId ? getOrCacheSessionCollection(sessionId) : Promise.resolve(null)
    ]);
    if (!sessionCollection) {
      console.warn(`\u26A0\uFE0F  No session collection found for ${sessionId}`);
      return { results: [], coverage: { confidence: 0, topScore: 0, level: "low", score: 0 }, queryEmbedding };
    }
    const rawResults = await queryCollection(sessionCollection, queryEmbedding, topK).catch(() => []);
    const results = rawResults.map((r) => ({
      ...r,
      source_type: r.metadata?.source_type || "session"
    }));
    const coverage = calculateCoverage(results, topK);
    const topScore = coverage.topScore;
    const level = topScore >= 0.6 ? "high" : topScore >= 0.3 ? "medium" : "low";
    console.log("\u{1F50D} Query:", query);
    console.log("\u{1F4CA} Coverage:", { ...coverage, level });
    console.log("\u{1F4C8} Raw scores:", results.map((r) => r.score.toFixed(4)));
    return {
      results,
      coverage: { ...coverage, level, score: topScore },
      queryEmbedding
    };
  } catch (error) {
    console.error("Retrieval error:", error);
    throw error;
  }
}
function invalidateSessionCollectionCache(sessionId) {
  cachedSessionCollections.delete(sessionId);
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
    id: uuidv43(),
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
var TOP_K, REFUSAL_THRESHOLD, cachedSessionCollections;
var init_retrievalService = __esm({
  "server/services/retrievalService.js"() {
    "use strict";
    init_chromaService();
    init_embeddingService();
    TOP_K = parseInt(process.env.TOP_K) || 5;
    REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;
    cachedSessionCollections = /* @__PURE__ */ new Map();
  }
});

// server/api/documents.js
import { Router as Router2 } from "file:///home/project/node_modules/express/index.js";
import multer from "file:///home/project/node_modules/multer/index.js";
import path2 from "path";
import fs from "fs";
import { v4 as uuidv44 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
import { createHash } from "crypto";
import pdf from "file:///home/project/node_modules/pdf-parse/index.js";
import { fileURLToPath } from "url";
function contentDisposition(displayName) {
  const encoded = encodeURIComponent(displayName).replace(/'/g, "%27").replace(/\(/g, "%28").replace(/\)/g, "%29");
  return `inline; filename="document.pdf"; filename*=UTF-8''${encoded}`;
}
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
    const sessionId = req.headers["x-session-id"] || req.body.sessionId || uuidv44();
    const session = getOrCreateSession(sessionId);
    const maxPDFs = parseInt(process.env.MAX_PDFS_PER_SESSION || "3");
    const cleanFilename = sanitizeFilename(file.originalname);
    const uploadedCount = session.documents.filter((d) => d.sourceType === "session_upload").length;
    if (uploadedCount >= maxPDFs) {
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
    const { collection } = await getSessionCollection(sessionId);
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
    invalidateSessionCollectionCache(sessionId);
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
    getOrCreateSession(sessionId);
    const documents = getAllDocuments(sessionId);
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
      const { collection } = await getSessionCollection(sessionId);
      if (collection) {
        const count = await deleteDocumentVectors(collection, documentId);
        if (count > 0) {
          removeDocumentFromSession(sessionId, documentId);
          invalidateSessionCollectionCache(sessionId);
        }
      }
    }
    const tmpPath = path2.join(uploadDir, `${documentId}.pdf`);
    if (fs.existsSync(tmpPath)) {
      fs.unlinkSync(tmpPath);
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
      res.setHeader("Content-Disposition", contentDisposition(`${documentId}.pdf`));
      return fs.createReadStream(uploadPath).pipe(res);
    }
    if (filename) {
      const seedPath = path2.join(seedDir, filename);
      if (fs.existsSync(seedPath)) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", contentDisposition(filename));
        return fs.createReadStream(seedPath).pipe(res);
      }
      if (fs.existsSync(seedDir)) {
        const allPdfs = fs.readdirSync(seedDir).filter((f) => f.endsWith(".pdf"));
        const match = allPdfs.find((f) => f.includes(path2.parse(filename).name));
        if (match) {
          const matchPath = path2.join(seedDir, match);
          res.setHeader("Content-Type", "application/pdf");
          res.setHeader("Content-Disposition", contentDisposition(match));
          return fs.createReadStream(matchPath).pipe(res);
        }
      }
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
    init_retrievalService();
    __vite_injected_original_import_meta_url = "file:///home/project/server/api/documents.js";
    router2 = Router2();
    __filename = fileURLToPath(__vite_injected_original_import_meta_url);
    __dirname = path2.dirname(__filename);
    uploadDir = "/tmp/uploads";
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    seedDir = path2.resolve(__dirname, "../../seed_documents");
    storage = multer.diskStorage({
      destination: (req, file, cb) => cb(null, uploadDir),
      filename: (req, file, cb) => cb(null, `${uuidv44()}${path2.extname(file.originalname)}`)
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
function clearMemory(sessionId) {
  memoryMap.delete(sessionId);
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
var init_promptService = __esm({
  "server/services/promptService.js"() {
    "use strict";
    init_memoryService();
    init_retrievalService();
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
function expandQuery(query) {
  const words = query.trim().split(/\s+/);
  if (words.length > 4) return query;
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
  return `${query} ${expansions.join(" ")}`;
}
async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId, convId: providedConvId } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({ error: "Query is required", code: "MISSING_QUERY" });
  }
  const sessionId = providedSessionId || uuidv45();
  const convId = providedConvId || uuidv45();
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
  addTurnWithCitations(convId, "user", query.trim());
  try {
    sendEvent("status", { stage: "retrieving", message: "Searching knowledge base..." });
    const expandedQuery = expandQuery(query);
    const { results, coverage } = await retrieveForQuery(expandedQuery, sessionId, { topK: 5 });
    sendEvent("retrieval", {
      results: results.length,
      level: coverage.level,
      score: coverage.score,
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
    sendEvent("status", { stage: "generating", message: "Generating response..." });
    const contextText = formatContextForPrompt(results);
    const recentTurns = getRecentTurns(convId, 10);
    const questions = recentTurns.filter((t) => t.role === "user");
    const answers = recentTurns.filter((t) => t.role === "assistant");
    const qSection = questions.map((t, i) => `Q${i + 1}: ${t.content}`).join("\n");
    const aSection = answers.map((t, i) => `A${i + 1}: ${t.content}`).join("\n");
    const memoryContext = recentTurns.length > 0 ? `Previous Questions:
${qSection}

Previous Answers:
${aSection}` : "";
    const prompt = `You are an AI Knowledge Assistant. Your behaviour depends on the type of input:

1. GREETINGS & SMALL TALK (hi, hello, how are you, do you have a life, jokes, general chat):
   - Respond warmly and naturally. Do NOT mention the knowledge base or documents at all.
   - Do NOT add any citations.

2. FACTUAL QUESTIONS WITH CONTEXT (context below is relevant):
   - Answer strictly using the numbered context provided.
   - Cite sources inline as [1] [2] \u2014 always separate brackets, never [1, 2].
   - Only cite numbers you actually used.

3. FACTUAL QUESTIONS WITHOUT CONTEXT (context is empty or irrelevant):
   - Politely decline in your own words \u2014 vary your phrasing naturally.
   - Do NOT add citations.
   - Do NOT use a fixed template or robotic response.

CONTEXT:
${contextText || "(No relevant documents found in knowledge base)"}

CONVERSATION HISTORY:
${memoryContext || "(No previous conversation)"}

CURRENT QUESTION: ${query}`;
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
    const citedIndices = [];
    const seen = /* @__PURE__ */ new Set();
    for (const match of fullResponse.matchAll(/\[(\d+)\]/g)) {
      const num = parseInt(match[1]);
      if (!seen.has(num)) {
        seen.add(num);
        citedIndices.push(num);
      }
    }
    const isOutOfScope = OUT_OF_SCOPE_PATTERN.test(fullResponse);
    const matchedCitations = citations.filter((c) => citedIndices.includes(c.index));
    const indexMap = /* @__PURE__ */ new Map();
    citedIndices.forEach((oldIdx, i) => {
      indexMap.set(oldIdx, i + 1);
    });
    const rewrittenResponse = fullResponse.replace(/\[(\d+)\]/g, (match, num) => {
      const newIdx = indexMap.get(parseInt(num));
      return newIdx !== void 0 ? `[${newIdx}]` : match;
    });
    const finalCitations = isOutOfScope || matchedCitations.length === 0 ? [] : matchedCitations.map((c) => ({ ...c, index: indexMap.get(c.index) })).filter((c) => c.index !== void 0).sort((a, b) => a.index - b.index);
    const matchedChunkIds = new Set(matchedCitations.map((c) => c.chunkId));
    const finalSources = isOutOfScope || matchedCitations.length === 0 ? [] : sources.filter((s) => matchedChunkIds.has(s.chunkId)).sort((a, b) => {
      const idxA = finalCitations.find((c) => c.chunkId === a.chunkId)?.index ?? 99;
      const idxB = finalCitations.find((c) => c.chunkId === b.chunkId)?.index ?? 99;
      return idxA - idxB;
    });
    addTurnWithCitations(convId, "assistant", rewrittenResponse, finalCitations, coverage, answerId);
    sendEvent("complete", {
      answerId,
      response: rewrittenResponse,
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
  if (fallback) return res.json({ sources: fallback.citations });
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
    init_sessionService();
    init_memoryService();
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
    app.post("/session/init", async (req, res) => {
      const sessionId = req.headers["x-session-id"];
      if (!sessionId) {
        return res.status(400).json({ error: "Missing x-session-id header", code: "MISSING_SESSION" });
      }
      getOrCreateSession(sessionId);
      try {
        await initSessionWithGlobalDocs(sessionId);
        res.json({ ready: true, sessionId });
      } catch (err) {
        console.warn("Session init warning:", err.message);
        res.json({ ready: false, sessionId, warning: err.message });
      }
    });
    app.post("/session/restore-memory", (req, res) => {
      const { convId, messages } = req.body;
      if (!convId || !Array.isArray(messages)) {
        return res.status(400).json({ error: "convId and messages are required", code: "BAD_REQUEST" });
      }
      try {
        clearMemory(convId);
        for (const msg of messages) {
          if ((msg.role === "user" || msg.role === "assistant") && typeof msg.content === "string") {
            addTurnWithCitations(convId, msg.role, msg.content);
          }
        }
        res.json({ ok: true, convId, restored: messages.length });
      } catch (err) {
        console.warn("Memory restore warning:", err.message);
        res.json({ ok: false, convId, warning: err.message });
      }
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcbiAgICBjbG91ZENsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xvdWRDbGllbnQ7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IEdsb2JhbCBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY29ubmVjdCB0byBnbG9iYWwgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGdsb2JhbENvbGxlY3Rpb247XG59XG5cbi8qKlxuICogUmV0dXJucyB7IGNvbGxlY3Rpb24sIGlzTmV3IH0uXG4gKiBpc05ldyA9IHRydWUgIFx1MjE5MiBmcmVzaGx5IGNyZWF0ZWQsIG5lZWRzIHNlZWRpbmcgZnJvbSBnbG9iYWwuXG4gKiBpc05ldyA9IGZhbHNlIFx1MjE5MiBhbHJlYWR5IGV4aXN0ZWQgb24gQ2hyb21hIENsb3VkLCByZXNwZWN0IGl0cyBjdXJyZW50IHN0YXRlICh1c2VyIG1heSBoYXZlIGFkZGVkL2RlbGV0ZWQgUERGcykuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiB7IGNvbGxlY3Rpb246IHNlc3Npb25Db2xsZWN0aW9ucy5nZXQoc2Vzc2lvbklkKSwgaXNOZXc6IGZhbHNlIH07XG4gIH1cblxuICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG5cbiAgbGV0IGNvbGxlY3Rpb247XG4gIGxldCBpc05ldztcblxuICB0cnkge1xuICAgIGNvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0Q29sbGVjdGlvbih7XG4gICAgICBuYW1lOiBjb2xsZWN0aW9uTmFtZSxcbiAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgfSk7XG4gICAgaXNOZXcgPSBmYWxzZTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNjdCXHVGRTBGICBTZXNzaW9uIGNvbGxlY3Rpb24gZXhpc3RzLCByZXVzaW5nOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICB9IGNhdGNoIHtcbiAgICBjb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmNyZWF0ZUNvbGxlY3Rpb24oe1xuICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICB0eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICBzZXNzaW9uX2lkOiBzZXNzaW9uSWQsXG4gICAgICAgIGNyZWF0ZWQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgfSk7XG4gICAgaXNOZXcgPSB0cnVlO1xuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGNyZWF0ZWQ6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gIH1cblxuICBzZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gIHJldHVybiB7IGNvbGxlY3Rpb24sIGlzTmV3IH07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBgc2Vzc2lvbl8ke3Nlc3Npb25JZH1gO1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lOiBjb2xsZWN0aW9uTmFtZSB9KTtcbiAgICBzZXNzaW9uQ29sbGVjdGlvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gICAgY29uc29sZS5sb2coYFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gZGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBhZGRWZWN0b3JzKGNvbGxlY3Rpb24sIHZlY3RvcnMsIGVtYmVkZGluZ3MsIGlkcykge1xuICB0cnkge1xuICAgIGF3YWl0IGNvbGxlY3Rpb24uYWRkKHtcbiAgICAgIGlkcyxcbiAgICAgIGVtYmVkZGluZ3MsXG4gICAgICBkb2N1bWVudHM6IHZlY3RvcnMubWFwKHYgPT4gdi50ZXh0KSxcbiAgICAgIG1ldGFkYXRhczogdmVjdG9ycy5tYXAodiA9PiB2Lm1ldGFkYXRhKVxuICAgIH0pO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBhZGQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSyA9IDUpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5xdWVyeSh7XG4gICAgICBxdWVyeUVtYmVkZGluZ3M6IFtxdWVyeUVtYmVkZGluZ10sXG4gICAgICBuUmVzdWx0czogdG9wSyxcbiAgICAgIGluY2x1ZGU6IFsnZG9jdW1lbnRzJywgJ21ldGFkYXRhcycsICdkaXN0YW5jZXMnXVxuICAgIH0pO1xuXG4gICAgaWYgKCFyZXN1bHRzLmlkcyB8fCByZXN1bHRzLmlkcy5sZW5ndGggPT09IDAgfHwgcmVzdWx0cy5pZHNbMF0ubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdHMuaWRzWzBdLm1hcCgoaWQsIGlkeCkgPT4gKHtcbiAgICAgIGlkLFxuICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHNbMF1baWR4XSxcbiAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhc1swXVtpZHhdLFxuICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF0sXG4gICAgICBzY29yZTogMSAtIHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF1cbiAgICB9KSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIHF1ZXJ5IGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgd2hlcmU6IHsgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQgfVxuICAgIH0pO1xuICAgIGlmIChleGlzdGluZy5pZHMgJiYgZXhpc3RpbmcuaWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBleGlzdGluZy5pZHMgfSk7XG4gICAgICByZXR1cm4gZXhpc3RpbmcuaWRzLmxlbmd0aDtcbiAgICB9XG4gICAgcmV0dXJuIDA7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdERvY3VtZW50cyhjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYWxsSXRlbXMgPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICBpbmNsdWRlOiBbJ21ldGFkYXRhcycsICdkb2N1bWVudHMnXVxuICAgIH0pO1xuXG4gICAgY29uc3QgZG9jdW1lbnRzTWFwID0gbmV3IE1hcCgpO1xuXG4gICAgaWYgKGFsbEl0ZW1zLmlkcykge1xuICAgICAgYWxsSXRlbXMuaWRzLmZvckVhY2goKGlkLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgbWV0YSA9IGFsbEl0ZW1zLm1ldGFkYXRhc1tpZHhdO1xuICAgICAgICBjb25zdCBkb2NJZCA9IG1ldGEuZG9jdW1lbnRfaWQ7XG5cbiAgICAgICAgaWYgKCFkb2N1bWVudHNNYXAuaGFzKGRvY0lkKSkge1xuICAgICAgICAgIGRvY3VtZW50c01hcC5zZXQoZG9jSWQsIHtcbiAgICAgICAgICAgIGRvY3VtZW50X2lkOiBkb2NJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiBtZXRhLmZpbGVuYW1lLFxuICAgICAgICAgICAgY2h1bmtfY291bnQ6IDAsXG4gICAgICAgICAgICBwYWdlX2NvdW50OiBtZXRhLnBhZ2VfbnVtYmVyIHx8IDEsXG4gICAgICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBtZXRhLnVwbG9hZF90aW1lc3RhbXAsXG4gICAgICAgICAgICBzb3VyY2VfdHlwZTogbWV0YS5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIGZpcnN0X2NodW5rX3RleHQ6IGFsbEl0ZW1zLmRvY3VtZW50c1tpZHhdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBkb2MgPSBkb2N1bWVudHNNYXAuZ2V0KGRvY0lkKTtcbiAgICAgICAgZG9jLmNodW5rX2NvdW50Kys7XG4gICAgICAgIGRvYy5wYWdlX2NvdW50ID0gTWF0aC5tYXgoZG9jLnBhZ2VfY291bnQsIG1ldGEucGFnZV9udW1iZXIgfHwgMSk7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShkb2N1bWVudHNNYXAudmFsdWVzKCkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50czonLCBlcnJvcik7XG4gICAgcmV0dXJuIFtdO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGhDaGVjaygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY2xlYW51cFNlc3Npb25Db2xsZWN0aW9ucygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25zID0gYXdhaXQgY2xpZW50Lmxpc3RDb2xsZWN0aW9ucygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcdTI3MDUgTm8gc3RhbGUgc2Vzc2lvbiBjb2xsZWN0aW9ucyBmb3VuZC4nKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHVEODNFXHVEREY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFx1MjcwNSBEZWxldGVkOiAke25hbWV9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgICBcdTI2QTBcdUZFMEYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY2xlYW51cCBjb21wbGV0ZS4nKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLndhcm4oJ1x1MjZBMFx1RkUwRiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7ZXhwb3J0IGNsYXNzIEFwcEVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlLCBzdGF0dXNDb2RlID0gNTAwKSB7XG4gICAgc3VwZXIobWVzc2FnZSk7XG4gICAgdGhpcy5jb2RlID0gY29kZTtcbiAgICB0aGlzLnN0YXR1c0NvZGUgPSBzdGF0dXNDb2RlO1xuICAgIHRoaXMuaXNPcGVyYXRpb25hbCA9IHRydWU7XG4gICAgRXJyb3IuY2FwdHVyZVN0YWNrVHJhY2UodGhpcywgdGhpcy5jb25zdHJ1Y3Rvcik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFZhbGlkYXRpb25FcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdWQUxJREFUSU9OX0VSUk9SJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFVwbG9hZExpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVVBMT0FEX0xJTUlUX0VYQ0VFREVEJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEZpbGVUb29MYXJnZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXhTaXplTUIpIHtcbiAgICBzdXBlcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmAsICdGSUxFX1RPT19MQVJHRScsIDQxMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEludmFsaWRGaWxlVHlwZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignT25seSBQREYgZmlsZXMgYXJlIGFsbG93ZWQnLCAnSU5WQUxJRF9GSUxFX1RZUEUnLCA0MTUpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBUb29NYW55UERGc0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXgpIHtcbiAgICBzdXBlcihgTWF4aW11bSAke21heH0gUERGcyBhbGxvd2VkIHBlciBzZXNzaW9uYCwgJ1RPT19NQU5ZX1BERlMnLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBEdXBsaWNhdGVGaWxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKGZpbGVuYW1lKSB7XG4gICAgc3VwZXIoYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmAsICdEVVBMSUNBVEVfRklMRScsIDQwOSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvcnJ1cHRlZFBERkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRmFpbGVkIHRvIHBhcnNlIFBERiBmaWxlLiBJdCBtYXkgYmUgY29ycnVwdGVkLicsICdDT1JSVVBURURfUERGJywgNDIyKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmF0ZUxpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKHJldHJ5QWZ0ZXIgPSA2MCkge1xuICAgIHN1cGVyKCdSYXRlIGxpbWl0IGV4Y2VlZGVkLiBQbGVhc2UgdHJ5IGFnYWluIGxhdGVyLicsICdSQVRFX0xJTUlUX0VYQ0VFREVEJywgNDI5KTtcbiAgICB0aGlzLnJldHJ5QWZ0ZXIgPSByZXRyeUFmdGVyO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBMTE1VbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignQUkgc2VydmljZSBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZS4gUGxlYXNlIHRyeSBhZ2Fpbi4nLCAnTExNX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRW1iZWRkaW5nRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UgPSAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgJ0VNQkVERElOR19FUlJPUicsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJldHJpZXZhbFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdEb2N1bWVudCByZXRyaWV2YWwgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnUkVUUklFVkFMX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ1dlYiBzZWFyY2ggaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnV0VCX1NFQVJDSF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBFbWJlZGRpbmdFcnJvciwgaXM0MjlFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmxldCBnZW5BSSA9IG51bGw7XG5sZXQgZW1iZWRkaW5nTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRFbWJlZGRpbmdNb2RlbCgpIHtcbiAgaWYgKCFlbWJlZGRpbmdNb2RlbCkge1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSk7XG4gICAgZW1iZWRkaW5nTW9kZWwgPSBnZW5BSS5nZXRHZW5lcmF0aXZlTW9kZWwoe1xuICAgICAgbW9kZWw6IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMDAxJ1xuICAgIH0pO1xuICB9XG4gIHJldHVybiBlbWJlZGRpbmdNb2RlbDtcbn1cblxuY29uc3QgQkFUQ0hfU0laRSA9ICgpID0+IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19CQVRDSF9NQVhfQ0hVTktTKSB8fCA3O1xuY29uc3QgUEFSQUxMRUxfQ0FMTFMgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUEFSQUxMRUxfQ0FMTFMpIHx8IDQ7XG5jb25zdCBPVVRQVVRfRElNRU5TSU9OUyA9ICgpID0+IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfRElNRU5TSU9OUykgfHwgMzA3MjtcbmNvbnN0IEdST1VQX1dBSVRfTVMgPSA2MTAwMDtcbmNvbnN0IFJFVFJZX1dBSVRfTVMgPSAxNTAwMDsgLy8gRklYIDM6IHdhaXQgYmVmb3JlIGluZGl2aWR1YWwgY2h1bmsgcmV0cmllc1xuXG5hc3luYyBmdW5jdGlvbiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBhdHRlbXB0ID0gMSkge1xuICBjb25zdCBtYXhBdHRlbXB0cyA9IDU7XG4gIGNvbnN0IG1vZGVsTmFtZSA9IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMDAxJztcblxuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0RW1iZWRkaW5nTW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmJhdGNoRW1iZWRDb250ZW50cyh7XG4gICAgICByZXF1ZXN0czogdGV4dHMubWFwKHRleHQgPT4gKHtcbiAgICAgICAgbW9kZWw6IGBtb2RlbHMvJHttb2RlbE5hbWV9YCxcbiAgICAgICAgY29udGVudDogeyBwYXJ0czogW3sgdGV4dCB9XSB9LFxuICAgICAgICB0YXNrVHlwZSxcbiAgICAgICAgb3V0cHV0RGltZW5zaW9uYWxpdHk6IE9VVFBVVF9ESU1FTlNJT05TKClcbiAgICAgIH0pKVxuICAgIH0pO1xuXG4gICAgaWYgKCFyZXN1bHQ/LmVtYmVkZGluZ3MgfHwgcmVzdWx0LmVtYmVkZGluZ3MubGVuZ3RoICE9PSB0ZXh0cy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihgRXhwZWN0ZWQgJHt0ZXh0cy5sZW5ndGh9IGVtYmVkZGluZ3MsIGdvdCAke3Jlc3VsdD8uZW1iZWRkaW5ncz8ubGVuZ3RoID8/IDB9YCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdC5lbWJlZGRpbmdzLm1hcChlID0+IHtcbiAgICAgIGlmICghZT8udmFsdWVzKSB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoJ01pc3NpbmcgdmFsdWVzIGluIGVtYmVkZGluZyByZXNwb25zZScpO1xuICAgICAgcmV0dXJuIGUudmFsdWVzO1xuICAgIH0pO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc3QgaXM0MjkgPSBpczQyOUVycm9yKGVycm9yKSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpO1xuXG4gICAgaWYgKGlzNDI5ICYmIGF0dGVtcHQgPCBtYXhBdHRlbXB0cykge1xuICAgICAgY29uc3QgcmV0cnlEZWxheSA9IGVycm9yLnJldHJ5QWZ0ZXIgfHwgR1JPVVBfV0FJVF9NUztcbiAgICAgIGNvbnNvbGUubG9nKGBSYXRlIGxpbWl0ZWQsIHdhaXRpbmcgJHtyZXRyeURlbGF5IC8gMTAwMH1zIChhdHRlbXB0ICR7YXR0ZW1wdH0vJHttYXhBdHRlbXB0c30pYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgcmV0cnlEZWxheSkpO1xuICAgICAgcmV0dXJuIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlLCBhdHRlbXB0ICsgMSk7XG4gICAgfVxuXG4gICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGVycm9yLm1lc3NhZ2UgfHwgJ0JhdGNoIGVtYmVkZGluZyBmYWlsZWQnKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVFbWJlZGRpbmdzKGNodW5rcywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgb25Qcm9ncmVzcykge1xuICBpZiAoIWNodW5rcyB8fCBjaHVua3MubGVuZ3RoID09PSAwKSByZXR1cm4gW107XG5cbiAgY29uc3QgYmF0Y2hTaXplID0gQkFUQ0hfU0laRSgpO1xuICBjb25zdCBwYXJhbGxlbENhbGxzID0gUEFSQUxMRUxfQ0FMTFMoKTtcbiAgY29uc3QgZW1iZWRkaW5ncyA9IFtdO1xuXG4gIGNvbnN0IGJhdGNoZXMgPSBbXTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IGJhdGNoU2l6ZSkge1xuICAgIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIGJhdGNoU2l6ZSkpO1xuICB9XG5cbiAgY29uc3QgdG90YWxHcm91cHMgPSBNYXRoLmNlaWwoYmF0Y2hlcy5sZW5ndGggLyBwYXJhbGxlbENhbGxzKTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IHBhcmFsbGVsQ2FsbHMpIHtcbiAgICBjb25zdCBwYXJhbGxlbEJhdGNoZXMgPSBiYXRjaGVzLnNsaWNlKGksIGkgKyBwYXJhbGxlbENhbGxzKTtcbiAgICBjb25zdCBncm91cE51bSA9IE1hdGguZmxvb3IoaSAvIHBhcmFsbGVsQ2FsbHMpICsgMTtcbiAgICBjb25zdCBjaHVua3NDb3ZlcmVkID0gTWF0aC5taW4oKGkgKyBwYXJhbGxlbENhbGxzKSAqIGJhdGNoU2l6ZSwgY2h1bmtzLmxlbmd0aCk7XG5cbiAgICBjb25zb2xlLmxvZyhgICBFbWJlZGRpbmcgZ3JvdXAgJHtncm91cE51bX0vJHt0b3RhbEdyb3Vwc30gXHUyMDE0ICR7cGFyYWxsZWxCYXRjaGVzLmxlbmd0aH0gYmF0Y2ggY2FsbChzKSBpbiBwYXJhbGxlbCAoY2h1bmtzICR7aSAqIGJhdGNoU2l6ZSArIDF9XHUyMDEzJHtjaHVua3NDb3ZlcmVkfSkuLi5gKTtcblxuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBwYXJhbGxlbEJhdGNoZXMubWFwKGJhdGNoID0+IGVtYmVkQmF0Y2goYmF0Y2gubWFwKGMgPT4gYy50ZXh0KSwgdGFza1R5cGUpKVxuICAgICk7XG5cbiAgICBjb25zdCBmYWlsZWRCYXRjaGVzID0gW107XG4gICAgcmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGJhdGNoSWR4KSA9PiB7XG4gICAgICBjb25zdCBiYXRjaCA9IHBhcmFsbGVsQmF0Y2hlc1tiYXRjaElkeF07XG4gICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgY29uc3QgdmVjdG9ycyA9IHJlc3VsdC52YWx1ZTtcbiAgICAgICAgYmF0Y2guZm9yRWFjaCgoY2h1bmssIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgLy8gRklYIDI6IGNvcnJlY3QgZmFsbGJhY2sgY2h1bmsgSUQgXHUyMDE0IChpICsgYmF0Y2hJZHgpIGlzIHRoZSBhYnNvbHV0ZSBiYXRjaCBpbmRleFxuICAgICAgICAgIGNvbnN0IGFic29sdXRlQ2h1bmtJZHggPSAoaSArIGJhdGNoSWR4KSAqIGJhdGNoU2l6ZSArIGNodW5rSWR4O1xuICAgICAgICAgIGVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua18ke2Fic29sdXRlQ2h1bmtJZHh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1tjaHVua0lkeF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGAgIEJhdGNoICR7aSArIGJhdGNoSWR4fSBmYWlsZWQsIHdpbGwgcmV0cnkgaW5kaXZpZHVhbGx5OmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICBmYWlsZWRCYXRjaGVzLnB1c2goeyBiYXRjaCwgYmF0Y2hJZHg6IGkgKyBiYXRjaElkeCB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGlzTGFzdEdyb3VwID0gaSArIHBhcmFsbGVsQ2FsbHMgPj0gYmF0Y2hlcy5sZW5ndGg7XG4gICAgaWYgKCFpc0xhc3RHcm91cCB8fCBmYWlsZWRCYXRjaGVzLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFdhaXRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIGJlZm9yZSBuZXh0IGdyb3VwLi4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgR1JPVVBfV0FJVF9NUykpO1xuICAgIH1cblxuICAgIC8vIEZJWCAzOiB3YWl0IGJlZm9yZSByZXRyeWluZyBpbmRpdmlkdWFsIGNodW5rcyB0byBhdm9pZCBpbW1lZGlhdGUgNDI5XG4gICAgZm9yIChjb25zdCB7IGJhdGNoLCBiYXRjaElkeCB9IG9mIGZhaWxlZEJhdGNoZXMpIHtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFdhaXRpbmcgJHtSRVRSWV9XQUlUX01TIC8gMTAwMH1zIGJlZm9yZSByZXRyeWluZyBmYWlsZWQgYmF0Y2ggJHtiYXRjaElkeH0uLi5gKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCBSRVRSWV9XQUlUX01TKSk7XG4gICAgICBmb3IgKGNvbnN0IGNodW5rIG9mIGJhdGNoKSB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW2NodW5rLnRleHRdLCB0YXNrVHlwZSk7XG4gICAgICAgICAgZW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rX3JldHJ5XyR7YmF0Y2hJZHh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1swXSxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVuay5tZXRhZGF0YSxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rLnRleHRcbiAgICAgICAgICB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgICBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgICBcdTI3NEMgUmV0cnkgZmFpbGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZH06YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIGVtYmVkZGluZ3M7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFF1ZXJ5KHF1ZXJ5KSB7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtxdWVyeV0sICdSRVRSSUVWQUxfUVVFUlknKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFNpbmdsZSh0ZXh0KSB7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFt0ZXh0XSwgJ1JFVFJJRVZBTF9ET0NVTUVOVCcpO1xuICByZXR1cm4gdmVjdG9yc1swXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJhdGVMaW1pdFN0YXRlKCkge1xuICByZXR1cm4ge1xuICAgIG1heFRva2Vuc1Blck1pbnV0ZTogcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1JBVEVfTElNSVRfVE9LRU5TX1BFUl9NSU5VVEUpIHx8IDMwMDAwLFxuICAgIHBhcmFsbGVsQ2FsbHM6IFBBUkFMTEVMX0NBTExTKCksXG4gICAgbWF4Q2h1bmtzUGVyQ2FsbDogQkFUQ0hfU0laRSgpLFxuICAgIG91dHB1dERpbWVuc2lvbnM6IE9VVFBVVF9ESU1FTlNJT05TKClcbiAgfTtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0UmF0ZUxpbWl0U3RhdGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGgocmVxLCByZXMpIHtcbiAgY29uc3QgaGVhbHRoU3RhdHVzID0ge1xuICAgIHN0YXR1czogJ29rJyxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICBzZXJ2aWNlczoge31cbiAgfTtcblxuICAvLyBDaGVjayBDaHJvbWFEQlxuICB0cnkge1xuICAgIGNvbnN0IGNocm9tYUhlYWx0aCA9IGF3YWl0IGNocm9tYUhlYWx0aENoZWNrKCk7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0gY2hyb21hSGVhbHRoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IHtcbiAgICAgIHN0YXR1czogJ2Vycm9yJyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlXG4gICAgfTtcbiAgfVxuXG4gIC8vIENoZWNrIEdlbWluaSAodmlhIEFQSSBrZXkgcHJlc2VuY2UpXG4gIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5nZW1pbmkgPSB7XG4gICAgc3RhdHVzOiBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSA/ICdjb25maWd1cmVkJyA6ICdub3RfY29uZmlndXJlZCdcbiAgfTtcblxuICAvLyBHZXQgcmF0ZSBsaW1pdCBzdGF0ZVxuICBoZWFsdGhTdGF0dXMucmF0ZUxpbWl0ID0gZ2V0UmF0ZUxpbWl0U3RhdGUoKTtcblxuICAvLyBPdmVyYWxsIHN0YXR1c1xuICBjb25zdCBoYXNFcnJvcnMgPSBPYmplY3QudmFsdWVzKGhlYWx0aFN0YXR1cy5zZXJ2aWNlcykuc29tZShcbiAgICBzID0+IHMuc3RhdHVzID09PSAnZXJyb3InIHx8IHMuc3RhdHVzID09PSAndW5oZWFsdGh5J1xuICApO1xuXG4gIGlmIChoYXNFcnJvcnMpIHtcbiAgICBoZWFsdGhTdGF0dXMuc3RhdHVzID0gJ2RlZ3JhZGVkJztcbiAgfVxuXG4gIHJlcy5qc29uKGhlYWx0aFN0YXR1cyk7XG59XG5cbnJvdXRlci5nZXQoJy8nLCBoZWFsdGgpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2ltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgVmFsaWRhdGlvbkVycm9yIH0gZnJvbSAnLi9lcnJvcnMuanMnO1xuXG5jb25zdCBEQU5HRVJPVVNfUEFUVEVSTlMgPSAvWzw+OlwifD8qXFx4MDAtXFx4MWZdL2c7XG5jb25zdCBQQVRIX1RSQVZFUlNBTCA9IC9cXC5cXC4vZztcblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplRmlsZW5hbWUoZmlsZW5hbWUpIHtcbiAgaWYgKCFmaWxlbmFtZSB8fCB0eXBlb2YgZmlsZW5hbWUgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZScpO1xuICB9XG5cbiAgLy8gUmVtb3ZlIHBhdGggY29tcG9uZW50cyBhbmQgZ2V0IGJhc2VuYW1lXG4gIGNvbnN0IGJhc2VuYW1lID0gcGF0aC5iYXNlbmFtZShmaWxlbmFtZSk7XG5cbiAgLy8gUmVtb3ZlIGRhbmdlcm91cyBjaGFyYWN0ZXJzXG4gIGxldCBzYW5pdGl6ZWQgPSBiYXNlbmFtZS5yZXBsYWNlKERBTkdFUk9VU19QQVRURVJOUywgJ18nKTtcblxuICAvLyBSZW1vdmUgcGF0aCB0cmF2ZXJzYWwgYXR0ZW1wdHNcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnJlcGxhY2UoUEFUSF9UUkFWRVJTQUwsICcnKTtcblxuICAvLyBUcmltIHdoaXRlc3BhY2UgYW5kIGxpbWl0IGxlbmd0aFxuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQudHJpbSgpLnNsaWNlKDAsIDI1NSk7XG5cbiAgaWYgKCFzYW5pdGl6ZWQpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lIGFmdGVyIHNhbml0aXphdGlvbicpO1xuICB9XG5cbiAgcmV0dXJuIHNhbml0aXplZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlUERGRmlsZShmaWxlKSB7XG4gIGlmICghZmlsZSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ05vIGZpbGUgcHJvdmlkZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIE1JTUUgdHlwZVxuICBjb25zdCB2YWxpZE1pbWVUeXBlcyA9IFsnYXBwbGljYXRpb24vcGRmJ107XG4gIGlmICghdmFsaWRNaW1lVHlwZXMuaW5jbHVkZXMoZmlsZS5taW1ldHlwZSkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdPbmx5IFBERiBmaWxlcyBhcmUgYWNjZXB0ZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIGV4dGVuc2lvblxuICBjb25zdCBleHQgPSBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUgfHwgJycpLnRvTG93ZXJDYXNlKCk7XG4gIGlmIChleHQgIT09ICcucGRmJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ZpbGUgbXVzdCBoYXZlIC5wZGYgZXh0ZW5zaW9uJyk7XG4gIH1cblxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRmlsZVNpemUoc2l6ZUJ5dGVzLCBtYXhTaXplTUIpIHtcbiAgY29uc3QgbWF4Qnl0ZXMgPSBtYXhTaXplTUIgKiAxMDI0ICogMTAyNDtcbiAgaWYgKHNpemVCeXRlcyA+IG1heEJ5dGVzKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmApO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVJbnB1dChpbnB1dCwgbWF4TGVuZ3RoID0gMTAwMDApIHtcbiAgaWYgKCFpbnB1dCB8fCB0eXBlb2YgaW5wdXQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG4gIHJldHVybiBpbnB1dC50cmltKCkuc2xpY2UoMCwgbWF4TGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRG9jdW1lbnRJZChpZCkge1xuICBpZiAoIWlkIHx8IHR5cGVvZiBpZCAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEJyk7XG4gIH1cbiAgY29uc3QgdXVpZFJlZ2V4ID0gL15bMC05YS1mXXs4fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXsxMn0kL2k7XG4gIGlmICghdXVpZFJlZ2V4LnRlc3QoaWQpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCBmb3JtYXQnKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RUZXh0RnJvbVBERkJ1ZmZlcihidWZmZXIpIHtcbiAgLy8gVGhpcyB3aWxsIGJlIHVzZWQgd2l0aCBwZGYtcGFyc2VcbiAgcmV0dXJuIGJ1ZmZlcjtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2ltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuXG5jb25zdCBDSEFSU19QRVJfVE9LRU4gPSA0O1xuY29uc3QgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUyA9IDEwMDA7XG5jb25zdCBERUZBVUxUX09WRVJMQVBfVE9LRU5TID0gMjAwO1xuY29uc3QgTUlOX0NIVU5LX0NIQVJTID0gMTAwO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblRleHQodGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gJyc7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoL1xcZi9nLCAnXFxuJylcbiAgICAucmVwbGFjZSgvKFxccypcXG4pezMsfS9nLCAnXFxuXFxuJylcbiAgICAucmVwbGFjZSgvXlxccypcXGQrXFxzKiQvZ20sICcnKVxuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgY2h1bmtTaXplVG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRva2VucyA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBERUZBVUxUX09WRVJMQVBfVE9LRU5TO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICBjb25zdCBjaHVua1NpemVDaGFycyA9IGNodW5rU2l6ZVRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBjb25zdCBjaHVua3MgPSBbXTtcbiAgbGV0IHN0YXJ0ID0gMDtcbiAgbGV0IGNodW5rSW5kZXggPSAwO1xuXG4gIHdoaWxlIChzdGFydCA8IHRleHQubGVuZ3RoKSB7XG4gICAgbGV0IGVuZCA9IHN0YXJ0ICsgY2h1bmtTaXplQ2hhcnM7XG5cbiAgICBpZiAoZW5kIDwgdGV4dC5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IGJyZWFrUG9pbnRzID0gWycuICcsICcuXFxuJywgJyEgJywgJz8gJywgJ1xcblxcbicsICdcXG4nLCAnICddO1xuICAgICAgY29uc3Qgc2VhcmNoU3RhcnQgPSBlbmQgLSBNYXRoLmZsb29yKGNodW5rU2l6ZUNoYXJzICogMC4yKTtcblxuICAgICAgZm9yIChjb25zdCBicmVha3BvaW50IG9mIGJyZWFrUG9pbnRzKSB7XG4gICAgICAgIGNvbnN0IGlkeCA9IHRleHQubGFzdEluZGV4T2YoYnJlYWtwb2ludCwgZW5kKTtcbiAgICAgICAgaWYgKGlkeCA+IHNlYXJjaFN0YXJ0ICYmIGlkeCA+IHN0YXJ0KSB7XG4gICAgICAgICAgZW5kID0gaWR4ICsgYnJlYWtwb2ludC5sZW5ndGg7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBlbmQgPSBNYXRoLm1pbihlbmQsIHRleHQubGVuZ3RoKTtcbiAgICBjb25zdCBjaHVua0NvbnRlbnQgPSB0ZXh0LnNsaWNlKHN0YXJ0LCBlbmQpLnRyaW0oKTtcblxuICAgIGlmIChjaHVua0NvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiBjaHVua0NvbnRlbnQsXG4gICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKGNodW5rQ29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogc3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGVuZCxcbiAgICAgICAgY2h1bmtJbmRleDogY2h1bmtJbmRleCsrXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBuZXh0U3RhcnQgPSBlbmQgLSBvdmVybGFwQ2hhcnM7XG4gICAgc3RhcnQgPSBuZXh0U3RhcnQgPiBzdGFydCA/IG5leHRTdGFydCA6IGVuZDtcblxuICAgIGlmIChjaHVua0luZGV4ID4gMTAwMDApIHtcbiAgICAgIGNvbnNvbGUud2FybignQ2h1bmsgbGltaXQgcmVhY2hlZCwgc3RvcHBpbmcnKTtcbiAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQoY2xlYW5lZFRleHQsIG9wdGlvbnMpO1xuICBjb25zdCB0b3RhbENodW5rcyA9IHRleHRDaHVua3MubGVuZ3RoO1xuXG4gIC8vIEZJWCA0OiB1c2Ugc291cmNlVHlwZSBmcm9tIG9wdGlvbnMsIGZhbGwgYmFjayB0byAncGRmJ1xuICBjb25zdCBzb3VyY2VUeXBlID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogZmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6IHNvdXJjZVR5cGUsICAgICAgICAgICAgLy8gRklYIDRcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH07XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0U2VjdGlvblRpdGxlKHRleHQpIHtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQge1xuICBnZXRHbG9iYWxDb2xsZWN0aW9uLFxuICBnZXRTZXNzaW9uQ29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50cyxcbiAgYWRkVmVjdG9yc1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGlkID0gc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcbiAgc2Vzc2lvbnMuc2V0KGlkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHJldHVybiBleGlzdGluZztcbiAgICByZXR1cm4gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIElmIGNvbGxlY3Rpb24gaXMgTkVXIFx1MjE5MiBzZWVkIGZyb20gZ2xvYmFsIChwYWdpbmF0ZWQsIDMwMC9iYXRjaClcbiAqIC0gSWYgY29sbGVjdGlvbiBFWElTVFMgXHUyMTkyIHNraXAgc2VlZCwgcmVjb25zdHJ1Y3QgaW4tbWVtb3J5IGRvYyBsaXN0IGZyb20gQ2hyb21hXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhgXHVEODNEXHVERDExIFNlc3Npb24gaW5pdDogJHtzZXNzaW9uSWR9YCk7XG4gIGlmIChzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKSkgcmV0dXJuO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb246IHNlc3Npb25Db2xsZWN0aW9uLCBpc05ldyB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGlmICghaXNOZXcpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI2N0JcdUZFMEYgIFNlc3Npb24gZXhpc3RzLCByZWNvbnN0cnVjdGluZyBkb2N1bWVudCBsaXN0IGZyb20gQ2hyb21hLi4uYCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKHNlc3Npb25Db2xsZWN0aW9uKTtcbiAgICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgICAgc291cmNlVHlwZTogZG9jLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IGRvY3VtZW50KHMpIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFx1RDgzQ1x1REYzMSBOZXcgc2Vzc2lvbiBcdTIwMTQgc2VlZGluZyBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uLi4uYCk7XG5cbiAgICBjb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdLCBhbGxFbWJlZGRpbmdzID0gW10sIGFsbERvY3VtZW50cyA9IFtdLCBhbGxNZXRhZGF0YXMgPSBbXTtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGdsb2JhbENvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydlbWJlZGRpbmdzJywgJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uYmF0Y2guZW1iZWRkaW5ncyk7XG4gICAgICBhbGxEb2N1bWVudHMucHVzaCguLi5iYXRjaC5kb2N1bWVudHMpO1xuICAgICAgYWxsTWV0YWRhdGFzLnB1c2goLi4uYmF0Y2gubWV0YWRhdGFzKTtcbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjZBMFx1RkUwRiAgR2xvYmFsIGNvbGxlY3Rpb24gaXMgZW1wdHkgXHUyMDE0IG5vdGhpbmcgdG8gc2VlZC4nKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYWxsSWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6IGFsbElkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIGVtYmVkZGluZ3M6IGFsbEVtYmVkZGluZ3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLFxuICAgICAgICBkb2N1bWVudHM6IGFsbERvY3VtZW50cy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIG1ldGFkYXRhczogYWxsTWV0YWRhdGFzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAobSA9PiAoeyAuLi5tLCBzb3VyY2VfdHlwZTogJ2dsb2JhbCcgfSkpXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFx1RDgzRFx1RENFNiBBZGRlZCBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IHJlY29yZHMgJHtpICsgMX1cdTIwMTMke01hdGgubWluKGkgKyBCQVRDSF9TSVpFLCBhbGxJZHMubGVuZ3RoKX1gKTtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlZWRlZCAke2FsbElkcy5sZW5ndGh9IHZlY3RvcnMgaW50byBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoc2Vzc2lvbikge1xuICAgICAgY29uc3QgZG9jc01hcCA9IG5ldyBNYXAoKTtcbiAgICAgIGFsbE1ldGFkYXRhcy5mb3JFYWNoKG1ldGEgPT4ge1xuICAgICAgICBpZiAoIWRvY3NNYXAuaGFzKG1ldGEuZG9jdW1lbnRfaWQpKSB7XG4gICAgICAgICAgZG9jc01hcC5zZXQobWV0YS5kb2N1bWVudF9pZCwge1xuICAgICAgICAgICAgaWQ6IG1ldGEuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBtZXRhLnRvdGFsX3BhZ2VzIHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiAwLFxuICAgICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGRvY3NNYXAuZ2V0KG1ldGEuZG9jdW1lbnRfaWQpLmNodW5rQ291bnQrKztcbiAgICAgIH0pO1xuXG4gICAgICBmb3IgKGNvbnN0IGRvYyBvZiBkb2NzTWFwLnZhbHVlcygpKSB7XG4gICAgICAgIGlmICghc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuaWQgPT09IGRvYy5pZCkpIHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKGRvYyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJbmZvKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICBpZDogZG9jdW1lbnRJbmZvLmlkLFxuICAgIGZpbGVuYW1lOiBkb2N1bWVudEluZm8uZmlsZW5hbWUsXG4gICAgZmlsZVNpemU6IGRvY3VtZW50SW5mby5maWxlU2l6ZSxcbiAgICBwYWdlQ291bnQ6IGRvY3VtZW50SW5mby5wYWdlQ291bnQsXG4gICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIGNodW5rQ291bnQ6IGRvY3VtZW50SW5mby5jaHVua0NvdW50LFxuICAgIHNvdXJjZVR5cGU6ICdzZXNzaW9uX3VwbG9hZCdcbiAgfSk7XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiAnU2Vzc2lvbiBub3QgZm91bmQnIH07XG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgaWYgKHVwbG9hZGVkQ291bnQgPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246IGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gIH07XG4gIH1cbiAgcmV0dXJuIHsgY2FuVXBsb2FkOiB0cnVlIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVVwbG9hZChzZXNzaW9uSWQsIGZpbGUsIGZpbGVuYW1lKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGNvbnN0IGVycm9ycyA9IFtdO1xuXG4gIGlmIChmaWxlLnNpemUgPiBNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIGV4Y2VlZHMgJHtNQVhfVVBMT0FEX1NJWkVfTUJ9TUIgbGltaXRgKTtcbiAgfVxuXG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uXG4gICAgPyBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aFxuICAgIDogMDtcblxuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIGVycm9ycy5wdXNoKGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gKTtcbiAgfVxuXG4gIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBmaWxlbmFtZSkpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCk7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGlzVmFsaWQ6IGVycm9ycy5sZW5ndGggPT09IDAsXG4gICAgZXJyb3JzLFxuICAgIGlzTGFyZ2VGaWxlOiBmaWxlLnNpemUgPiAoTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQgKiAwLjYpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcbiAgY29uc3QgaWR4ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZEluZGV4KGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJZCk7XG4gIGlmIChpZHggPj0gMCkge1xuICAgIHNlc3Npb24uZG9jdW1lbnRzLnNwbGljZShpZHgsIDEpO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBbXTtcbiAgcmV0dXJuIHNlc3Npb24uZG9jdW1lbnRzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IHNlc3Npb25Eb2N1bWVudHM6IFtdLCBnbG9iYWxEb2N1bWVudHM6IFtdIH07XG5cbiAgY29uc3Qgbm9ybWFsaXplID0gKGRvYykgPT4gKHtcbiAgICBkb2N1bWVudF9pZDogZG9jLmlkLFxuICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgY2h1bmtfY291bnQ6IGRvYy5jaHVua0NvdW50ID8/IDAsXG4gICAgcGFnZV9jb3VudDogZG9jLnBhZ2VDb3VudCA/PyAwLFxuICAgIHVwbG9hZF90aW1lc3RhbXA6IGRvYy51cGxvYWRUaW1lc3RhbXAgfHwgbnVsbCxcbiAgICBzb3VyY2VfdHlwZTogZG9jLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcgPyAnc2Vzc2lvbl91cGxvYWQnIDogJ3NlZWQnLFxuICAgIGZpbGVTaXplOiBkb2MuZmlsZVNpemUgfHwgbnVsbFxuICB9KTtcblxuICByZXR1cm4ge1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKVxuICAgICAgLm1hcChub3JtYWxpemUpLFxuICAgIGdsb2JhbERvY3VtZW50czogc2Vzc2lvbi5kb2N1bWVudHNcbiAgICAgIC5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdnbG9iYWwnKVxuICAgICAgLm1hcChub3JtYWxpemUpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIHJldHVybiB7XG4gICAgaWQ6IHNlc3Npb24uaWQsXG4gICAgZG9jdW1lbnRDb3VudDogc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogc2Vzc2lvbi5jcmVhdGVkQXQsXG4gICAgbGFzdEFjY2Vzc2VkOiBzZXNzaW9uLmxhc3RBY2Nlc3NlZCxcbiAgICB0b3RhbFNpemU6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5maWxlU2l6ZSB8fCAwKSwgMCksXG4gICAgdG90YWxDaHVua3M6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdEFjdGl2ZVNlc3Npb25zKCkge1xuICByZXR1cm4gQXJyYXkuZnJvbShzZXNzaW9ucy52YWx1ZXMoKSkuZmlsdGVyKHMgPT4gIWlzU2Vzc2lvbkV4cGlyZWQocykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW51cEV4cGlyZWRTZXNzaW9ucygpIHtcbiAgbGV0IGNsZWFuZWQgPSAwO1xuICBmb3IgKGNvbnN0IFtpZCwgc2Vzc2lvbl0gb2Ygc2Vzc2lvbnMpIHtcbiAgICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgICAgc2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBjbGVhbmVkKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBjbGVhbmVkO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIHF1ZXJ5Q29sbGVjdGlvbiB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBlbWJlZFF1ZXJ5IH0gZnJvbSAnLi9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBUT1BfSyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LlRPUF9LKSB8fCA1O1xuY29uc3QgUkVGVVNBTF9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LlJFRlVTQUxfVEhSRVNIT0xEKSB8fCAwLjA1O1xuXG5jb25zdCBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMgPSBuZXcgTWFwKCk7XG5cbmFzeW5jIGZ1bmN0aW9uIGdldE9yQ2FjaGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgdHJ5IHtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7IC8vIGRlc3RydWN0dXJlXG4gICAgaWYgKGNvbGxlY3Rpb24pIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IFRPUF9LKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAgfTtcbiAgY29uc3Qgc2NvcmVzID0gcmVzdWx0cy5zbGljZSgwLCB0b3BLKS5tYXAociA9PiBNYXRoLm1heCgwLCByLnNjb3JlKSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcbiAgcmV0dXJuIHtcbiAgICBjb25maWRlbmNlOiBNYXRoLnJvdW5kKGF2Z1Njb3JlICogMTAwKSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKVxuICB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdG9wSyA9IG9wdGlvbnMudG9wSyB8fCBUT1BfSztcblxuICB0cnkge1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgc2Vzc2lvbkNvbGxlY3Rpb25dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBzZXNzaW9uSWQgPyBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSA6IFByb21pc2UucmVzb2x2ZShudWxsKVxuICAgIF0pO1xuXG4gICAgaWYgKCFzZXNzaW9uQ29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIHNlc3Npb24gY29sbGVjdGlvbiBmb3VuZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcgfTtcbiAgICB9XG5cbiAgICBjb25zdCByYXdSZXN1bHRzID0gYXdhaXQgcXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgIC5jYXRjaCgoKSA9PiBbXSk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gcmF3UmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgLi4ucixcbiAgICAgIHNvdXJjZV90eXBlOiByLm1ldGFkYXRhPy5zb3VyY2VfdHlwZSB8fCAnc2Vzc2lvbidcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb3ZlcmFnZSA9IGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEspO1xuICAgIGNvbnN0IHRvcFNjb3JlID0gY292ZXJhZ2UudG9wU2NvcmU7XG4gICAgY29uc3QgbGV2ZWwgPSB0b3BTY29yZSA+PSAwLjYgPyAnaGlnaCcgOiB0b3BTY29yZSA+PSAwLjMgPyAnbWVkaXVtJyA6ICdsb3cnO1xuXG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1REQwRCBRdWVyeTonLCBxdWVyeSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDQSBDb3ZlcmFnZTonLCB7IC4uLmNvdmVyYWdlLCBsZXZlbCB9KTtcbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVEQ0M4IFJhdyBzY29yZXM6JywgcmVzdWx0cy5tYXAociA9PiByLnNjb3JlLnRvRml4ZWQoNCkpKTtcblxuICAgIHJldHVybiB7XG4gICAgICByZXN1bHRzLFxuICAgICAgY292ZXJhZ2U6IHsgLi4uY292ZXJhZ2UsIGxldmVsLCBzY29yZTogdG9wU2NvcmUgfSxcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nXG4gICAgfTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1JldHJpZXZhbCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCkge1xuICBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMsIG1heFRva2VucyA9IDcwMDApIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgbGV0IHRvdGFsVG9rZW5zID0gMDtcbiAgY29uc3QgY29udGV4dFBhcnRzID0gW107XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCByZXN1bHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gcmVzdWx0c1tpXTtcbiAgICBjb25zdCB0b2tlbkVzdGltYXRlID0gcmVzdWx0LnRleHQubGVuZ3RoIC8gNDtcbiAgICBpZiAodG90YWxUb2tlbnMgKyB0b2tlbkVzdGltYXRlID4gbWF4VG9rZW5zKSBicmVhaztcbiAgICB0b3RhbFRva2VucyArPSB0b2tlbkVzdGltYXRlO1xuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnZ2xvYmFsJyA/ICdbU2VlZCBEb2N1bWVudF0nIDogJ1tTZXNzaW9uIFVwbG9hZF0nO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG4gICAgY29udGV4dFBhcnRzLnB1c2goYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQuc2xpY2UoMCwgMjAwKSArIChyZXN1bHQudGV4dC5sZW5ndGggPiAyMDAgPyAnLi4uJyA6ICcnKSxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hvdWxkU2hvd1JlZnVzYWwoY292ZXJhZ2UpIHtcbiAgcmV0dXJuIGNvdmVyYWdlLnRvcFNjb3JlIDwgUkVGVVNBTF9USFJFU0hPTEQ7XG59XG5cbmV4cG9ydCB7IGNhbGN1bGF0ZUNvdmVyYWdlIH07XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgbXVsdGVyIGZyb20gJ211bHRlcic7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCBmcyBmcm9tICdmcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuaW1wb3J0IHBkZiBmcm9tICdwZGYtcGFyc2UnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7O1xuaW1wb3J0IHsgc2FuaXRpemVGaWxlbmFtZSwgdmFsaWRhdGVQREZGaWxlLCB2YWxpZGF0ZUZpbGVTaXplIH0gZnJvbSAnLi4vdXRpbHMvc2FuaXRpemUuanMnO1xuaW1wb3J0IHtcbiAgQ29ycnVwdGVkUERGRXJyb3IsXG4gIEludmFsaWRGaWxlVHlwZUVycm9yLFxuICBGaWxlVG9vTGFyZ2VFcnJvcixcbiAgVG9vTWFueVBERnNFcnJvcixcbiAgRHVwbGljYXRlRmlsZUVycm9yXG59IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5pbXBvcnQgeyBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgYWRkVmVjdG9ycywgZGVsZXRlRG9jdW1lbnRWZWN0b3JzIH0gZnJvbSAnLi4vc2VydmljZXMvY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBjaHVua1RleHQsIGNsZWFuVGV4dCB9IGZyb20gJy4uL3V0aWxzL2NodW5rZXIuanMnO1xuaW1wb3J0IHsgZ2VuZXJhdGVFbWJlZGRpbmdzIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQge1xuICBnZXRPckNyZWF0ZVNlc3Npb24sXG4gIGNhbkFjY2VwdFVwbG9hZCxcbiAgYWRkRG9jdW1lbnRUb1Nlc3Npb24sXG4gIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sXG4gIGdldEFsbERvY3VtZW50c1xufSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZSB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgX19maWxlbmFtZSA9IGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKTtcbmNvbnN0IF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShfX2ZpbGVuYW1lKTtcblxuY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG5pZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbn1cblxuY29uc3Qgc2VlZERpciA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuLi8uLi9zZWVkX2RvY3VtZW50cycpO1xuXG5jb25zdCBzdG9yYWdlID0gbXVsdGVyLmRpc2tTdG9yYWdlKHtcbiAgZGVzdGluYXRpb246IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCB1cGxvYWREaXIpLFxuICBmaWxlbmFtZTogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIGAke3V1aWR2NCgpfSR7cGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKX1gKVxufSk7XG5cbmNvbnN0IHVwbG9hZCA9IG11bHRlcih7XG4gIHN0b3JhZ2UsXG4gIGxpbWl0czogeyBmaWxlU2l6ZTogcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CIHx8ICc1JykgKiAxMDI0ICogMTAyNCB9LFxuICBmaWxlRmlsdGVyOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgIGlmIChmaWxlLm1pbWV0eXBlID09PSAnYXBwbGljYXRpb24vcGRmJyAmJiBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpLnRvTG93ZXJDYXNlKCkgPT09ICcucGRmJykge1xuICAgICAgY2IobnVsbCwgdHJ1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNiKG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpKTtcbiAgICB9XG4gIH1cbn0pO1xuXG5mdW5jdGlvbiBjb250ZW50RGlzcG9zaXRpb24oZGlzcGxheU5hbWUpIHtcbiAgY29uc3QgZW5jb2RlZCA9IGVuY29kZVVSSUNvbXBvbmVudChkaXNwbGF5TmFtZSlcbiAgICAucmVwbGFjZSgvJy9nLCAnJTI3JylcbiAgICAucmVwbGFjZSgvXFwoL2csICclMjgnKVxuICAgIC5yZXBsYWNlKC9cXCkvZywgJyUyOScpO1xuICByZXR1cm4gYGlubGluZTsgZmlsZW5hbWU9XCJkb2N1bWVudC5wZGZcIjsgZmlsZW5hbWUqPVVURi04Jycke2VuY29kZWR9YDtcbn1cblxuYXN5bmMgZnVuY3Rpb24gcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZVBhdGgpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBidWZmZXIgPSBmcy5yZWFkRmlsZVN5bmMoZmlsZVBhdGgpO1xuXG4gICAgY29uc3QgcGFnZXMgPSBbXTtcbiAgICBhd2FpdCBwZGYoYnVmZmVyLCB7XG4gICAgICBwYWdlcmVuZGVyOiAocGFnZURhdGEpID0+IHtcbiAgICAgICAgcmV0dXJuIHBhZ2VEYXRhLmdldFRleHRDb250ZW50KCkudGhlbih0YyA9PiB7XG4gICAgICAgICAgY29uc3QgcGFnZVRleHQgPSB0Yy5pdGVtcy5tYXAoaSA9PiBpLnN0cikuam9pbignICcpO1xuICAgICAgICAgIHBhZ2VzLnB1c2gocGFnZVRleHQpO1xuICAgICAgICAgIHJldHVybiBwYWdlVGV4dDtcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAocGFnZXMubGVuZ3RoID09PSAwIHx8IHBhZ2VzLmV2ZXJ5KHAgPT4gIXAudHJpbSgpKSkge1xuICAgICAgY29uc3QgZnVsbCA9IGF3YWl0IHBkZihidWZmZXIpO1xuICAgICAgcGFnZXMucHVzaChmdWxsLnRleHQpO1xuICAgIH1cblxuICAgIGNvbnN0IHRvdGFsUGFnZXMgPSBwYWdlcy5sZW5ndGg7XG4gICAgY29uc3QgY2xlYW5lZFBhZ2VzID0gcGFnZXMubWFwKHAgPT4gY2xlYW5UZXh0KHApKTtcbiAgICBjb25zdCBwYWdlTWFwID0gW107XG4gICAgbGV0IGNoYXJQb3MgPSAwO1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjbGVhbmVkUGFnZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHBhZ2VNYXAucHVzaCh7IHBhZ2U6IGkgKyAxLCBzdGFydDogY2hhclBvcywgZW5kOiBjaGFyUG9zICsgY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCB9KTtcbiAgICAgIGNoYXJQb3MgKz0gY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCArIDE7XG4gICAgfVxuXG4gICAgY29uc3QgZnVsbFRleHQgPSBjbGVhbmVkUGFnZXMuam9pbignXFxuJyk7XG4gICAgcmV0dXJuIHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdQREYgcGFyc2luZyBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgbmV3IENvcnJ1cHRlZFBERkVycm9yKCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0UGFnZU51bWJlcihjaGFyU3RhcnQsIHBhZ2VNYXApIHtcbiAgZm9yIChjb25zdCBlbnRyeSBvZiBwYWdlTWFwKSB7XG4gICAgaWYgKGNoYXJTdGFydCA+PSBlbnRyeS5zdGFydCAmJiBjaGFyU3RhcnQgPCBlbnRyeS5lbmQpIHJldHVybiBlbnRyeS5wYWdlO1xuICB9XG4gIHJldHVybiBwYWdlTWFwW3BhZ2VNYXAubGVuZ3RoIC0gMV0/LnBhZ2UgfHwgMTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICB0cnkge1xuICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICBpZiAoIWZpbGUpIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgbWF4UERGcyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG4gICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgIGlmICh1cGxvYWRlZENvdW50ID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBUb29NYW55UERGc0Vycm9yKG1heFBERnMpO1xuICAgIH1cblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBEdXBsaWNhdGVGaWxlRXJyb3IoY2xlYW5GaWxlbmFtZSk7XG4gICAgfVxuXG4gICAgY29uc3QgeyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9ID0gYXdhaXQgcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZS5wYXRoKTtcblxuICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MjIpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ05vIGV4dHJhY3RhYmxlIHRleHQgZm91bmQgXHUyMDE0IFBERiBtYXkgYmUgc2Nhbm5lZCBvciBpbWFnZS1vbmx5JyxcbiAgICAgICAgY29kZTogJ0VNUFRZX1BERidcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGRvY3VtZW50SWQgPSBwYXRoLnBhcnNlKGZpbGUuZmlsZW5hbWUpLm5hbWU7XG5cbiAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQsIHtcbiAgICAgIGNodW5rU2l6ZVRva2VuczogMTAwMCxcbiAgICAgIG92ZXJsYXBUb2tlbnM6IDIwMFxuICAgIH0pO1xuXG4gICAgaWYgKHJhd0NodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQyMikuanNvbih7IGVycm9yOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgY2h1bmtfaWQ6IGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogaWR4LFxuICAgICAgICB0b3RhbF9jaHVua3M6IHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiBnZXRQYWdlTnVtYmVyKGNodW5rLmNoYXJTdGFydCwgcGFnZU1hcCksXG4gICAgICAgIHRvdGFsX3BhZ2VzOiB0b3RhbFBhZ2VzLFxuICAgICAgICBzb3VyY2VfdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH0pKTtcblxuICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGVtYmVkZGluZ3MgPSBhd2FpdCBnZW5lcmF0ZUVtYmVkZGluZ3MoXG4gICAgICBjaHVua3MsXG4gICAgICAnUkVUUklFVkFMX0RPQ1VNRU5UJyxcbiAgICAgICh7IGN1cnJlbnRfYmF0Y2gsIHRvdGFsX2JhdGNoZXMgfSkgPT4ge1xuICAgICAgICBpZiAocmVxLmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MpIHtcbiAgICAgICAgICByZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcy5lbWl0KGBwcm9ncmVzc18ke3Nlc3Npb25JZH1gLCB7XG4gICAgICAgICAgICBkb2N1bWVudElkLFxuICAgICAgICAgICAgY3VycmVudF9iYXRjaCxcbiAgICAgICAgICAgIHRvdGFsX2JhdGNoZXMsXG4gICAgICAgICAgICBzdGFnZTogJ2VtYmVkZGluZydcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICk7XG5cbiAgICBpZiAoZW1iZWRkaW5ncy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDUwMykuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnLCBjb2RlOiAnRU1CRURESU5HX0ZBSUxFRCcgfSk7XG4gICAgfVxuXG4gICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgIGNvbGxlY3Rpb24sXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgZW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgKTtcblxuICAgIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCk7XG5cbiAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgIGlkOiBkb2N1bWVudElkLFxuICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLFxuICAgICAgY2h1bmtDb3VudDogZW1iZWRkaW5ncy5sZW5ndGhcbiAgICB9KTtcblxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBkb2N1bWVudDoge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcyxcbiAgICAgICAgY2h1bmtDb3VudDogZW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9LFxuICAgICAgc2Vzc2lvbklkXG4gICAgfSk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTtcbiAgICB9XG4gICAgY29uc29sZS5lcnJvcignVXBsb2FkIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKGVycm9yLnN0YXR1c0NvZGUgfHwgNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcbiAgdHJ5IHtcbiAgICAvLyBFbnN1cmUgc2Vzc2lvbiBvYmplY3QgZXhpc3RzIGluIG1lbW9yeSAobm8tb3AgaWYgYWxyZWFkeSBjcmVhdGVkKVxuICAgIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICBjb25zdCBjb3VudCA9IGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgaWYgKGNvdW50ID4gMCkge1xuICAgICAgICAgIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKTtcbiAgICAgICAgICBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgdG1wUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGAke2RvY3VtZW50SWR9LnBkZmApO1xuICAgIGlmIChmcy5leGlzdHNTeW5jKHRtcFBhdGgpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKHRtcFBhdGgpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSwgZG9jdW1lbnRJZCB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdEZWxldGUgZG9jdW1lbnQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50JywgY29kZTogJ0RFTEVURV9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgdXBsb2FkUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGAke2RvY3VtZW50SWR9LnBkZmApO1xuICAgIGlmIChmcy5leGlzdHNTeW5jKHVwbG9hZFBhdGgpKSB7XG4gICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGAke2RvY3VtZW50SWR9LnBkZmApKTtcbiAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHVwbG9hZFBhdGgpLnBpcGUocmVzKTtcbiAgICB9XG5cbiAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgIGNvbnN0IHNlZWRQYXRoID0gcGF0aC5qb2luKHNlZWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWRQYXRoKSkge1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0oc2VlZFBhdGgpLnBpcGUocmVzKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZERpcikpIHtcbiAgICAgICAgY29uc3QgYWxsUGRmcyA9IGZzLnJlYWRkaXJTeW5jKHNlZWREaXIpLmZpbHRlcihmID0+IGYuZW5kc1dpdGgoJy5wZGYnKSk7XG4gICAgICAgIGNvbnN0IG1hdGNoID0gYWxsUGRmcy5maW5kKGYgPT4gZi5pbmNsdWRlcyhwYXRoLnBhcnNlKGZpbGVuYW1lKS5uYW1lKSk7XG4gICAgICAgIGlmIChtYXRjaCkge1xuICAgICAgICAgIGNvbnN0IG1hdGNoUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBtYXRjaCk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24obWF0Y2gpKTtcbiAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShtYXRjaFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbnJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG5yb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG5yb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IG1lbW9yeU1hcCA9IG5ldyBNYXAoKTtcbmNvbnN0IERFRkFVTFRfTUVNT1JZX1dJTkRPVyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IDEwO1xuXG5leHBvcnQgZnVuY3Rpb24gaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpIHtcbiAgaWYgKCFtZW1vcnlNYXAuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBtZW1vcnlNYXAuc2V0KHNlc3Npb25JZCwge1xuICAgICAgdHVybnM6IFtdLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBtZXRhZGF0YSA9IHt9KSB7XG4gIGNvbnN0IG1lbW9yeSA9IG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IG1heFR1cm5zID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuXG4gIGNvbnN0IHR1cm4gPSB7XG4gICAgaWQ6IGB0dXJuXyR7RGF0ZS5ub3coKX1fJHtNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zdWJzdHIoMiwgOSl9YCxcbiAgICByb2xlLFxuICAgIGNvbnRlbnQsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIC4uLm1ldGFkYXRhXG4gIH07XG5cbiAgbWVtb3J5LnR1cm5zLnB1c2godHVybik7XG5cbiAgaWYgKG1lbW9yeS50dXJucy5sZW5ndGggPiBtYXhUdXJucykge1xuICAgIG1lbW9yeS50dXJucyA9IG1lbW9yeS50dXJucy5zbGljZSgtbWF4VHVybnMpO1xuICB9XG5cbiAgcmV0dXJuIHR1cm47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCBtYXhUdXJucyA9IG51bGwpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IGxpbWl0ID0gbWF4VHVybnMgfHwgcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuICByZXR1cm4gbWVtb3J5LnR1cm5zLnNsaWNlKC1saW1pdCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb252ZXJzYXRpb25Db250ZXh0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIHJldHVybiB0dXJucy5tYXAodCA9PiAoe1xuICAgIHJvbGU6IHQucm9sZSxcbiAgICBjb250ZW50OiB0LmNvbnRlbnRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIGlmICh0dXJucy5sZW5ndGggPT09IDApIHJldHVybiAnJztcblxuICByZXR1cm4gdHVybnMubWFwKHQgPT4ge1xuICAgIGNvbnN0IHByZWZpeCA9IHQucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXI6JyA6ICdBc3Npc3RhbnQ6JztcbiAgICByZXR1cm4gYCR7cHJlZml4fSAke3QuY29udGVudH1gO1xuICB9KS5qb2luKCdcXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFyTWVtb3J5KHNlc3Npb25JZCkge1xuICBtZW1vcnlNYXAuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnlTdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIHJldHVybiB7XG4gICAgdHVybkNvdW50OiBtZW1vcnkudHVybnMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogbWVtb3J5LmNyZWF0ZWRBdCxcbiAgICBsYXN0VHVybkF0OiBtZW1vcnkudHVybnMubGVuZ3RoID4gMCA/IG1lbW9yeS50dXJuc1ttZW1vcnkudHVybnMubGVuZ3RoIC0gMV0udGltZXN0YW1wIDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBjaXRhdGlvbnMgPSBbXSwgY292ZXJhZ2UgPSBudWxsLCBhbnN3ZXJJZCA9IG51bGwpIHtcbiAgcmV0dXJuIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCB7XG4gICAgLi4uKGFuc3dlcklkICYmIHsgaWQ6IGFuc3dlcklkIH0pLFxuICAgIGNpdGF0aW9ucyxcbiAgICBjb3ZlcmFnZSxcbiAgICBoYXNDaXRhdGlvbnM6IGNpdGF0aW9ucy5sZW5ndGggPiAwXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdFVzZXJNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ3VzZXInKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50JykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcHJvbXB0U2VydmljZS5qc1wiO2ltcG9ydCB7IGZvcm1hdE1lbW9yeUZvclByb21wdCB9IGZyb20gJy4vbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0LCBjYWxjdWxhdGVDb3ZlcmFnZSB9IGZyb20gJy4vcmV0cmlldmFsU2VydmljZS5qcyc7XG5cbmNvbnN0IFNZU1RFTV9JTlNUUlVDVElPTiA9IGBZb3UgYXJlIGFuIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQgdGhhdCBhbnN3ZXJzIHF1ZXN0aW9ucyBiYXNlZCBvbiBpbmRleGVkIGRvY3VtZW50cyB3aGVuIGF2YWlsYWJsZS5cblxuUlVMRVM6XG4xLiBXaGVuIGNvbnRleHQgaXMgcHJvdmlkZWQsIGFuc3dlciBiYXNlZCBvbiBpdCBhbmQgY2l0ZSBzb3VyY2VzIHVzaW5nIFsxXSwgWzJdLCBldGMuXG4yLiBGb3IgZ2VuZXJhbCBjb252ZXJzYXRpb24gKGdyZWV0aW5ncywgY2xhcmlmeWluZyBxdWVzdGlvbnMsIHNtYWxsIHRhbGspLCByZXNwb25kIG5hdHVyYWxseSBhbmQgaGVscGZ1bGx5IHdpdGhvdXQgcmVxdWlyaW5nIGNvbnRleHQuXG4zLiBJZiBhIGZhY3R1YWwgcXVlc3Rpb24gaXMgYXNrZWQgYnV0IGNvbnRleHQgaXMgaW5zdWZmaWNpZW50LCBzYXkgc28gY2xlYXJseSBhbmQgc3VnZ2VzdCB1cGxvYWRpbmcgcmVsZXZhbnQgZG9jdW1lbnRzLlxuNC4gQmUgY29uY2lzZSBidXQgdGhvcm91Z2guIFVzZSBidWxsZXQgcG9pbnRzIG9yIG51bWJlcmVkIGxpc3RzIGZvciBjb21wbGV4IGFuc3dlcnMuXG41LiBNYWludGFpbiBjb252ZXJzYXRpb24gY29udGludWl0eSBidXQgZG9uJ3QgcmVwZWF0IGluZm9ybWF0aW9uIHVubmVjZXNzYXJpbHkuXG42LiBGb3JtYXQgcmVzcG9uc2VzIGluIGNsZWFyLCByZWFkYWJsZSBtYXJrZG93bi5gO1xuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRQcm9tcHQoeyBxdWVyeSwgY29udGV4dCwgbWVtb3J5Q29udGV4dCwgY292ZXJhZ2UgfSkge1xuICBjb25zdCBwYXJ0cyA9IFtdO1xuICBwYXJ0cy5wdXNoKFNZU1RFTV9JTlNUUlVDVElPTik7XG4gIGlmIChtZW1vcnlDb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKG1lbW9yeUNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgUFJFVklPVVMgQ09OVkVSU0FUSU9OIC0tLVxcbicpO1xuICB9XG4gIGlmIChjb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFJFTEVWQU5UIENPTlRFWFQgRlJPTSBLTk9XTEVER0UgQkFTRSAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKGNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgQ09OVEVYVCAtLS1cXG4nKTtcbiAgfVxuICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gQ1VSUkVOVCBRVUVTVElPTiAtLS1cXG4nKTtcbiAgcGFydHMucHVzaChxdWVyeSk7XG4gIHBhcnRzLnB1c2goJ1xcblxcblJlbWVtYmVyOiBBbnN3ZXIgYmFzZWQgT05MWSBvbiB0aGUgcHJvdmlkZWQgY29udGV4dC4gVXNlIFsxXSwgWzJdLCBldGMuIGZvciBjaXRhdGlvbnMuIElmIHRoZSBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkuJyk7XG4gIHJldHVybiBwYXJ0cy5qb2luKCcnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkU3RyZWFtaW5nUHJvbXB0KHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpO1xuICBjb25zdCBjb250ZXh0U3RyaW5nID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXRyaWV2ZWRSZXN1bHRzKTtcbiAgcmV0dXJuIGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0U3RyaW5nLFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IGNhbGN1bGF0ZUNvdmVyYWdlKHJldHJpZXZlZFJlc3VsdHMpXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFJlc3BvbnNlKCkge1xuICAvLyBObyBsb25nZXIgdXNlZCBcdTIwMTQgTExNIGdlbmVyYXRlcyBpdHMgb3duIG5hdHVyYWwgcmVmdXNhbFxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFN5c3RlbUluc3RydWN0aW9uKCkge1xuICByZXR1cm4gU1lTVEVNX0lOU1RSVUNUSU9OO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRXZWJTZWFyY2hQcm9tcHQocXVlcnksIGdyb3VuZGluZ01ldGFkYXRhKSB7XG4gIHJldHVybiBgQmFzZWQgb24gd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIGZvbGxvd2luZyBxdWVzdGlvbjogJHtxdWVyeX1cblxuR3VpZGVsaW5lczpcbi0gVXNlIGluZm9ybWF0aW9uIGZyb20gdGhlIHdlYiBzZWFyY2hcbi0gUHJvdmlkZSBzb3VyY2VzL1VSTHMgd2hlcmUgYXBwbGljYWJsZVxuLSBCZSBjb25jaXNlIGFuZCBpbmZvcm1hdGl2ZVxuLSBJZiBtdWx0aXBsZSBzb3VyY2VzIGFncmVlIG9yIGNvbnRyYWRpY3QsIG1lbnRpb24gdGhhdGA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRHZW5lcmF0aW9uQ29uZmlnKGN1c3RvbUNvbmZpZyA9IHt9KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICB0b3BQOiAwLjk1LFxuICAgIHRvcEs6IDQwLFxuICAgIG1heE91dHB1dFRva2VuczogMjA0OCxcbiAgICAuLi5jdXN0b21Db25maWdcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RTb3VyY2VzRnJvbVJlc3BvbnNlKHJlc3BvbnNlKSB7XG4gIGNvbnN0IGNpdGF0aW9uUGF0dGVybiA9IC9cXFsoXFxkKylcXF0vZztcbiAgY29uc3QgY2l0YXRpb25zID0gbmV3IFNldCgpO1xuICBsZXQgbWF0Y2g7XG4gIHdoaWxlICgobWF0Y2ggPSBjaXRhdGlvblBhdHRlcm4uZXhlYyhyZXNwb25zZSkpICE9PSBudWxsKSB7XG4gICAgY2l0YXRpb25zLmFkZChwYXJzZUludChtYXRjaFsxXSkpO1xuICB9XG4gIHJldHVybiBBcnJheS5mcm9tKGNpdGF0aW9ucykuc29ydCgoYSwgYikgPT4gYSAtIGIpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuZXJhdGl2ZUFJIH0gZnJvbSAnQGdvb2dsZS9nZW5lcmF0aXZlLWFpJztcbmltcG9ydCB7IGJ1aWxkUHJvbXB0LCBnZXRSZWZ1c2FsUmVzcG9uc2UgfSBmcm9tICcuL3Byb21wdFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgTExNVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmxldCBnZW5BSSA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldEdlbkFJKCkge1xuICBpZiAoIWdlbkFJKSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVk7XG4gICAgaWYgKCFhcGlLZXkpIHRocm93IG5ldyBFcnJvcignR0VNSU5JX0FQSV9LRVkgaXMgdW5kZWZpbmVkJyk7XG4gICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKGFwaUtleSk7XG4gIH1cbiAgcmV0dXJuIGdlbkFJO1xufVxuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5jb25zdCBGQUxMQkFDS19NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9GQUxMQkFDSyB8fCAnZ2VtaW5pLTIuNS1mbGFzaCc7XG5jb25zdCBGSVJTVF9UT0tFTl9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX0ZJUlNUX1RPS0VOX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDEyMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX1JFUVVFU1RfVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgNDUwMDA7XG5cbmxldCBwcmltYXJ5TW9kZWwgPSBudWxsO1xubGV0IGZhbGxiYWNrTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRQcmltYXJ5TW9kZWwoKSB7XG4gIGlmICghcHJpbWFyeU1vZGVsKSB7XG4gICAgcHJpbWFyeU1vZGVsID0gZ2V0R2VuQUkoKS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogUFJJTUFSWV9NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gcHJpbWFyeU1vZGVsO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsKCkge1xuICBpZiAoIWZhbGxiYWNrTW9kZWwpIHtcbiAgICBmYWxsYmFja01vZGVsID0gZ2V0R2VuQUkoKS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogRkFMTEJBQ0tfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIGZhbGxiYWNrTW9kZWw7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVJlc3BvbnNlKHByb21wdCkge1xuICAvLyBGSVggNjogY3JlYXRlIGNvbnRyb2xsZXIgYW5kIGFjdHVhbGx5IHBhc3Mgc2lnbmFsIHRvIGdlbmVyYXRlQ29udGVudFxuICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuICBjb25zdCB0aW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IGdldFByaW1hcnlNb2RlbCgpLmdlbmVyYXRlQ29udGVudChcbiAgICAgIHtcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSxcbiAgICAgIHsgc2lnbmFsOiBjb250cm9sbGVyLnNpZ25hbCB9ICAvLyBGSVggNjogcGFzcyBzaWduYWxcbiAgICApO1xuXG4gICAgY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG4gICAgcmV0dXJuIHJlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG4gIH0gY2F0Y2ggKHByaW1hcnlFcnJvcikge1xuICAgIGNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuICAgIGNvbnNvbGUuZXJyb3IoJ1ByaW1hcnkgbW9kZWwgZmFpbGVkOicsIHByaW1hcnlFcnJvci5tZXNzYWdlKTtcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBmYWxsYmFja1Jlc3VsdCA9IGF3YWl0IGdldEZhbGxiYWNrTW9kZWwoKS5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcmV0dXJuIGZhbGxiYWNrUmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbiAgICB9IGNhdGNoIChmYWxsYmFja0Vycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWxsYmFjayBtb2RlbCBhbHNvIGZhaWxlZDonLCBmYWxsYmFja0Vycm9yLm1lc3NhZ2UpO1xuICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1SZXNwb25zZShwcm9tcHQpIHtcbiAgbGV0IG1vZGVsID0gZ2V0UHJpbWFyeU1vZGVsKCk7XG4gIGxldCByZXRyaWVzID0gMDtcbiAgY29uc3QgbWF4UmV0cmllcyA9IDI7XG5cbiAgd2hpbGUgKHJldHJpZXMgPCBtYXhSZXRyaWVzKSB7XG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG5cbiAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudFN0cmVhbSh7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBsZXQgZmlyc3RUb2tlbiA9IHRydWU7XG4gICAgICBjb25zdCBmaXJzdFRva2VuVGltZW91dCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBGSVJTVF9UT0tFTl9USU1FT1VUKTtcblxuICAgICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiByZXN1bHQuc3RyZWFtKSB7XG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ0ZpcnN0IHRva2VuIHRpbWVvdXQgXHUyMDE0IG5vIHJlc3BvbnNlIGZyb20gbW9kZWwnKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHRleHQgPSBjaHVuay50ZXh0KCk7XG4gICAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgICAgaWYgKGZpcnN0VG9rZW4pIHtcbiAgICAgICAgICAgIGZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICByZXR1cm4geyBzdWNjZXNzOiB0cnVlIH07XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgcmV0cmllcysrO1xuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIG1vZGVsID0gZ2V0RmFsbGJhY2tNb2RlbCgpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbUNoYXRSZXNwb25zZShxdWVyeSwgcmV0cmlldmVkUmVzdWx0cywgc2Vzc2lvbklkLCBtZW1vcnlTZXJ2aWNlKSB7XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBtZW1vcnlTZXJ2aWNlID8gbWVtb3J5U2VydmljZS5mb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSA6ICcnO1xuICBjb25zdCBjb250ZXh0TGlzdCA9IHJldHJpZXZlZFJlc3VsdHMgfHwgW107XG4gIGNvbnN0IGNvbnRleHRUZXh0ID0gY29udGV4dExpc3QubWFwKChyLCBpKSA9PlxuICAgIGBbJHtpICsgMX1dICR7ci5tZXRhZGF0YS5maWxlbmFtZSB8fCAnVW5rbm93bid9OiAke3IudGV4dH1gXG4gICkuam9pbignXFxuXFxuJyk7XG5cbiAgY29uc3QgcHJvbXB0ID0gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRUZXh0LFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IHsgbGV2ZWw6ICdoaWdoJyB9XG4gIH0pO1xuXG4gIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICB0cnkge1xuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQgeyB0eXBlOiAnY29tcGxldGUnLCByZXNwb25zZTogZnVsbFJlc3BvbnNlIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsVGV4dCgpIHtcbiAgcmV0dXJuIGdldFJlZnVzYWxSZXNwb25zZSgpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVXZWJTZWFyY2hSZXNwb25zZShxdWVyeSwgZ3JvdW5kaW5nQ29udGVudCkge1xuICBjb25zdCBtb2RlbCA9IGdldFByaW1hcnlNb2RlbCgpO1xuXG4gIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgY29udGVudHM6IFt7XG4gICAgICByb2xlOiAndXNlcicsXG4gICAgICBwYXJ0czogW3sgdGV4dDogYEJhc2VkIG9uIHRoZXNlIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBxdWVzdGlvbjogXCIke3F1ZXJ5fVwiXFxuXFxuJHtncm91bmRpbmdDb250ZW50fWAgfV1cbiAgICB9XSxcbiAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH0sXG4gICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgfSk7XG5cbiAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gIGNvbnN0IHRleHQgPSByZXNwb25zZS50ZXh0KCk7XG4gIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzcG9uc2UuY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICByZXR1cm4ge1xuICAgIHRleHQsXG4gICAgZ3JvdW5kaW5nTWV0YWRhdGEsXG4gICAgZ3JvdW5kaW5nQ2h1bmtzOiBncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzIHx8IFtdXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IHJldHJpZXZlRm9yUXVlcnksIGdlbmVyYXRlQ2l0YXRpb25zLCBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0IH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5pbXBvcnQgeyBzdHJlYW1SZXNwb25zZSB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zIH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24gfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBPVVRfT0ZfU0NPUEVfUEFUVEVSTiA9IC9kb24ndCBoYXZlIGluZm9ybWF0aW9ufGRvIG5vdCBoYXZlIGluZm9ybWF0aW9ufG5vdCBpbiBteSBrbm93bGVkZ2V8Y2FuJ3QgZmluZHxjYW5ub3QgZmluZHxubyBpbmZvcm1hdGlvbnxrbm93bGVkZ2UgYmFzZSBkb2Vzbid0fG5vdCBjb3ZlcmVkfG91dHNpZGUuKmtub3dsZWRnZS9pO1xuXG5mdW5jdGlvbiBjbGVhbkV4Y2VycHQodGV4dCkge1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC8oPzwhXFx3KShbQS1aYS16XSlcXHMoW0EtWmEtel0pXFxzKFtBLVphLXpdKShcXHNbQS1aYS16XSkqL2csIChtYXRjaCkgPT5cbiAgICAgIG1hdGNoLnJlcGxhY2UoL1xccy9nLCAnJylcbiAgICApXG4gICAgLnJlcGxhY2UoL1xcc3syLH0vZywgJyAnKVxuICAgIC5yZXBsYWNlKC9eXFwqXFxzKi8sICcnKVxuICAgIC50cmltKCk7XG59XG5cbi8vIElzc3VlIDQgZml4OiByZW1vdmUgZG9tYWluSGludCBcdTIwMTQgc2hvcnQgcXVlcmllcyBubyBsb25nZXIgaW5oZXJpdCBwcmV2aW91cyBjb252ZXJzYXRpb24gY29udGV4dFxuZnVuY3Rpb24gZXhwYW5kUXVlcnkocXVlcnkpIHtcbiAgY29uc3Qgd29yZHMgPSBxdWVyeS50cmltKCkuc3BsaXQoL1xccysvKTtcbiAgaWYgKHdvcmRzLmxlbmd0aCA+IDQpIHJldHVybiBxdWVyeTtcblxuICBjb25zdCBleHBhbnNpb25zID0gW1xuICAgICdkZWZpbml0aW9uJywgJ292ZXJ2aWV3JywgJ3JvbGUnLCAncmVzcG9uc2liaWxpdGllcycsXG4gICAgJ2V4YW1wbGVzJywgJ2tleSBjb25jZXB0cycsICdob3cgaXQgd29ya3MnLCAncHVycG9zZSdcbiAgXTtcblxuICByZXR1cm4gYCR7cXVlcnl9ICR7ZXhwYW5zaW9ucy5qb2luKCcgJyl9YDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZUNoYXRTdHJlYW0ocmVxLCByZXMpIHtcbiAgLy8gSXNzdWUgNSBmaXg6IGV4dHJhY3QgY29udklkIGZyb20gYm9keSBcdTIwMTQgdXNlIGFzIG1lbW9yeSBrZXkgaW5zdGVhZCBvZiBzZXNzaW9uSWRcbiAgY29uc3QgeyBxdWVyeSwgc2Vzc2lvbklkOiBwcm92aWRlZFNlc3Npb25JZCwgY29udklkOiBwcm92aWRlZENvbnZJZCB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJywgY29kZTogJ01JU1NJTkdfUVVFUlknIH0pO1xuICB9XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcHJvdmlkZWRTZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIC8vIElzc3VlIDUgZml4OiBlYWNoIGNvbnZlcnNhdGlvbiBnZXRzIGl0cyBvd24gbWVtb3J5IHNsb3RcbiAgY29uc3QgY29udklkID0gcHJvdmlkZWRDb252SWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkID0gdXVpZHY0KCk7XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIC8vIElzc3VlIDUgZml4OiB1c2UgY29udklkIGZvciBtZW1vcnksIG5vdCBzZXNzaW9uSWRcbiAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCBleHBhbmRlZFF1ZXJ5ID0gZXhwYW5kUXVlcnkocXVlcnkpO1xuICAgIGNvbnN0IHsgcmVzdWx0cywgY292ZXJhZ2UgfSA9IGF3YWl0IHJldHJpZXZlRm9yUXVlcnkoZXhwYW5kZWRRdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgbGV2ZWw6IGNvdmVyYWdlLmxldmVsLFxuICAgICAgc2NvcmU6IGNvdmVyYWdlLnNjb3JlLFxuICAgICAgdG9wU2NvcmU6IGNvdmVyYWdlLnRvcFNjb3JlXG4gICAgfSk7XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBjb25zdCBzb3VyY2VzID0gcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgY2h1bmtJZDogci5pZCxcbiAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgIHBhZ2VOdW1iZXI6IHIubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgICBleGNlcnB0OiBjbGVhbkV4Y2VycHQoci50ZXh0LnNsaWNlKDAsIDIwMCkpLFxuICAgICAgc2NvcmU6IHIuc2NvcmUsXG4gICAgICBzb3VyY2VUeXBlOiByLnNvdXJjZV90eXBlXG4gICAgfSkpO1xuXG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnZ2VuZXJhdGluZycsIG1lc3NhZ2U6ICdHZW5lcmF0aW5nIHJlc3BvbnNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGNvbnRleHRUZXh0ID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzKTtcblxuICAgIC8vIElzc3VlcyAzICYgNiBmaXg6IHVzZSBjb252SWQgZm9yIG1lbW9yeTsgc3RydWN0dXJlZCBRJkEgZm9ybWF0IHZpYSBmb3JtYXRNZW1vcnlGb3JQcm9tcHRcbiAgICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKGNvbnZJZCwgMTApO1xuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IHJlY2VudFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ3VzZXInKTtcbiAgICBjb25zdCBhbnN3ZXJzICAgPSByZWNlbnRUdXJucy5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICdhc3Npc3RhbnQnKTtcbiAgICBjb25zdCBxU2VjdGlvbiAgPSBxdWVzdGlvbnMubWFwKCh0LCBpKSA9PiBgUSR7aSArIDF9OiAke3QuY29udGVudH1gKS5qb2luKCdcXG4nKTtcbiAgICBjb25zdCBhU2VjdGlvbiAgPSBhbnN3ZXJzLm1hcCgodCwgaSkgPT4gYEEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IHJlY2VudFR1cm5zLmxlbmd0aCA+IDBcbiAgICAgID8gYFByZXZpb3VzIFF1ZXN0aW9uczpcXG4ke3FTZWN0aW9ufVxcblxcblByZXZpb3VzIEFuc3dlcnM6XFxuJHthU2VjdGlvbn1gXG4gICAgICA6ICcnO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gWW91ciBiZWhhdmlvdXIgZGVwZW5kcyBvbiB0aGUgdHlwZSBvZiBpbnB1dDpcblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMSyAoaGksIGhlbGxvLCBob3cgYXJlIHlvdSwgZG8geW91IGhhdmUgYSBsaWZlLCBqb2tlcywgZ2VuZXJhbCBjaGF0KTpcbiAgIC0gUmVzcG9uZCB3YXJtbHkgYW5kIG5hdHVyYWxseS4gRG8gTk9UIG1lbnRpb24gdGhlIGtub3dsZWRnZSBiYXNlIG9yIGRvY3VtZW50cyBhdCBhbGwuXG4gICAtIERvIE5PVCBhZGQgYW55IGNpdGF0aW9ucy5cblxuMi4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSCBDT05URVhUIChjb250ZXh0IGJlbG93IGlzIHJlbGV2YW50KTpcbiAgIC0gQW5zd2VyIHN0cmljdGx5IHVzaW5nIHRoZSBudW1iZXJlZCBjb250ZXh0IHByb3ZpZGVkLlxuICAgLSBDaXRlIHNvdXJjZXMgaW5saW5lIGFzIFsxXSBbMl0gXHUyMDE0IGFsd2F5cyBzZXBhcmF0ZSBicmFja2V0cywgbmV2ZXIgWzEsIDJdLlxuICAgLSBPbmx5IGNpdGUgbnVtYmVycyB5b3UgYWN0dWFsbHkgdXNlZC5cblxuMy4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSE9VVCBDT05URVhUIChjb250ZXh0IGlzIGVtcHR5IG9yIGlycmVsZXZhbnQpOlxuICAgLSBQb2xpdGVseSBkZWNsaW5lIGluIHlvdXIgb3duIHdvcmRzIFx1MjAxNCB2YXJ5IHlvdXIgcGhyYXNpbmcgbmF0dXJhbGx5LlxuICAgLSBEbyBOT1QgYWRkIGNpdGF0aW9ucy5cbiAgIC0gRG8gTk9UIHVzZSBhIGZpeGVkIHRlbXBsYXRlIG9yIHJvYm90aWMgcmVzcG9uc2UuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0IHx8ICcoTm8gcmVsZXZhbnQgZG9jdW1lbnRzIGZvdW5kIGluIGtub3dsZWRnZSBiYXNlKSd9XG5cbkNPTlZFUlNBVElPTiBISVNUT1JZOlxuJHttZW1vcnlDb250ZXh0IHx8ICcoTm8gcHJldmlvdXMgY29udmVyc2F0aW9uKSd9XG5cbkNVUlJFTlQgUVVFU1RJT046ICR7cXVlcnl9YDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIEV4dHJhY3QgY2l0ZWQgaW5kaWNlcyBpbiBPUkRFUiBPRiBGSVJTVCBBUFBFQVJBTkNFXG4gICAgY29uc3QgY2l0ZWRJbmRpY2VzID0gW107XG4gICAgY29uc3Qgc2VlbiA9IG5ldyBTZXQoKTtcbiAgICBmb3IgKGNvbnN0IG1hdGNoIG9mIGZ1bGxSZXNwb25zZS5tYXRjaEFsbCgvXFxbKFxcZCspXFxdL2cpKSB7XG4gICAgICBjb25zdCBudW0gPSBwYXJzZUludChtYXRjaFsxXSk7XG4gICAgICBpZiAoIXNlZW4uaGFzKG51bSkpIHtcbiAgICAgICAgc2Vlbi5hZGQobnVtKTtcbiAgICAgICAgY2l0ZWRJbmRpY2VzLnB1c2gobnVtKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBpc091dE9mU2NvcGUgPSBPVVRfT0ZfU0NPUEVfUEFUVEVSTi50ZXN0KGZ1bGxSZXNwb25zZSk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2l0YXRpb25zID0gY2l0YXRpb25zLmZpbHRlcihjID0+IGNpdGVkSW5kaWNlcy5pbmNsdWRlcyhjLmluZGV4KSk7XG5cbiAgICBjb25zdCBpbmRleE1hcCA9IG5ldyBNYXAoKTtcbiAgICBjaXRlZEluZGljZXMuZm9yRWFjaCgob2xkSWR4LCBpKSA9PiB7XG4gICAgICBpbmRleE1hcC5zZXQob2xkSWR4LCBpICsgMSk7XG4gICAgfSk7XG5cbiAgICBjb25zdCByZXdyaXR0ZW5SZXNwb25zZSA9IGZ1bGxSZXNwb25zZS5yZXBsYWNlKC9cXFsoXFxkKylcXF0vZywgKG1hdGNoLCBudW0pID0+IHtcbiAgICAgIGNvbnN0IG5ld0lkeCA9IGluZGV4TWFwLmdldChwYXJzZUludChudW0pKTtcbiAgICAgIHJldHVybiBuZXdJZHggIT09IHVuZGVmaW5lZCA/IGBbJHtuZXdJZHh9XWAgOiBtYXRjaDtcbiAgICB9KTtcblxuICAgIGNvbnN0IGZpbmFsQ2l0YXRpb25zID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogbWF0Y2hlZENpdGF0aW9uc1xuICAgICAgICAgIC5tYXAoYyA9PiAoeyAuLi5jLCBpbmRleDogaW5kZXhNYXAuZ2V0KGMuaW5kZXgpIH0pKVxuICAgICAgICAgIC5maWx0ZXIoYyA9PiBjLmluZGV4ICE9PSB1bmRlZmluZWQpXG4gICAgICAgICAgLnNvcnQoKGEsIGIpID0+IGEuaW5kZXggLSBiLmluZGV4KTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaHVua0lkcyA9IG5ldyBTZXQobWF0Y2hlZENpdGF0aW9ucy5tYXAoYyA9PiBjLmNodW5rSWQpKTtcblxuICAgIGNvbnN0IGZpbmFsU291cmNlcyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IHNvdXJjZXNcbiAgICAgICAgICAuZmlsdGVyKHMgPT4gbWF0Y2hlZENodW5rSWRzLmhhcyhzLmNodW5rSWQpKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiB7XG4gICAgICAgICAgICBjb25zdCBpZHhBID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYS5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgICBjb25zdCBpZHhCID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYi5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgICByZXR1cm4gaWR4QSAtIGlkeEI7XG4gICAgICAgICAgfSk7XG5cbiAgICAvLyBJc3N1ZSA1IGZpeDogc3RvcmUgYXNzaXN0YW50IHR1cm4gdW5kZXIgY29udklkXG4gICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAnYXNzaXN0YW50JywgcmV3cml0dGVuUmVzcG9uc2UsIGZpbmFsQ2l0YXRpb25zLCBjb3ZlcmFnZSwgYW5zd2VySWQpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIGFuc3dlcklkLFxuICAgICAgcmVzcG9uc2U6IHJld3JpdHRlblJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zOiBmaW5hbENpdGF0aW9ucyxcbiAgICAgIGNvdmVyYWdlLFxuICAgICAgc291cmNlczogZmluYWxTb3VyY2VzXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnQW4gZXJyb3Igb2NjdXJyZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNvdXJjZXMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDIwKTtcblxuICBjb25zdCBleGFjdE1hdGNoID0gcmVjZW50VHVybnMuZmluZCh0ID0+IHQuaWQgPT09IGFuc3dlcklkKTtcbiAgaWYgKGV4YWN0TWF0Y2g/LmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGV4YWN0TWF0Y2guY2l0YXRpb25zIH0pO1xuICB9XG5cbiAgY29uc3QgZmFsbGJhY2sgPSBbLi4ucmVjZW50VHVybnNdLnJldmVyc2UoKS5maW5kKHQgPT5cbiAgICB0LnJvbGUgPT09ICdhc3Npc3RhbnQnICYmIHQuY2l0YXRpb25zPy5sZW5ndGggPiAwXG4gICk7XG5cbiAgaWYgKGZhbGxiYWNrKSByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG5cbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kJywgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJyB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuY29uc3QgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZKTtcblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuXG5sZXQgbW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRNb2RlbCgpIHtcbiAgaWYgKCFtb2RlbCkge1xuICAgIG1vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIG1vZGVsO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcGVyZm9ybVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICAvLyBFeHRyYWN0IHNlYXJjaCBxdWVyaWVzIGFuZCBzb3VyY2VzXG4gICAgY29uc3Qgd2ViU2VhcmNoUXVlcmllcyA9IFtdO1xuICAgIGNvbnN0IHdlYlNvdXJjZXMgPSBbXTtcblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGNodW5rIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoY2h1bmsud2ViKSB7XG4gICAgICAgICAgd2ViU291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogY2h1bmsud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBjaHVuay53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8ud2ViU2VhcmNoUXVlcmllcykge1xuICAgICAgd2ViU2VhcmNoUXVlcmllcy5wdXNoKC4uLmdyb3VuZGluZ01ldGFkYXRhLndlYlNlYXJjaFF1ZXJpZXMpO1xuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICB0ZXh0LFxuICAgICAgc291cmNlczogd2ViU291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHdlYlNlYXJjaFF1ZXJpZXMsXG4gICAgICByYXdNZXRhZGF0YTogZ3JvdW5kaW5nTWV0YWRhdGFcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSB0ZXh0O1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlPy5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgY29uc3Qgc291cmNlcyA9IFtdO1xuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChpdGVtLndlYikge1xuICAgICAgICAgIHNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGl0ZW0ud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBpdGVtLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQge1xuICAgICAgdHlwZTogJ2NvbXBsZXRlJyxcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbWluZyBlcnJvcjonLCBlcnJvcik7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdFdlYlNlYXJjaFJlc3BvbnNlKHJlc3VsdCkge1xuICByZXR1cm4ge1xuICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMubWFwKHMgPT4gKHtcbiAgICAgIHVyaTogcy51cmksXG4gICAgICB0aXRsZTogcy50aXRsZSxcbiAgICAgIHR5cGU6ICd3ZWInXG4gICAgfSkpLFxuICAgIHF1ZXJpZXNVc2VkOiByZXN1bHQucXVlcmllcyxcbiAgICBtZXRhZGF0YToge1xuICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9XG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBwZXJmb3JtV2ViU2VhcmNoLCBzdHJlYW1XZWJTZWFyY2ggfSBmcm9tICcuLi9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2gocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgcGVyZm9ybVdlYlNlYXJjaChxdWVyeS50cmltKCkpO1xuXG4gICAgcmVzLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHJlc3VsdC5xdWVyaWVzLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgICAgfVxuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDMpLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggdW5hdmFpbGFibGUnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoU3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICAvLyBTZXQgdXAgU1NFXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnc2VhcmNoaW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyB0aGUgd2ViLi4uJyB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcbiAgICBsZXQgc291cmNlcyA9IFtdO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1XZWJTZWFyY2gocXVlcnkudHJpbSgpKSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdXRUJfU0VBUkNIX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgICAgc291cmNlcyA9IGNodW5rLnNvdXJjZXMgfHwgW107XG4gICAgICB9XG4gICAgfVxuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywge1xuICAgICAgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCBmYWlsZWQnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVXZWJTZWFyY2gpO1xucm91dGVyLnBvc3QoJy9zdHJlYW0nLCBoYW5kbGVXZWJTZWFyY2hTdHJlYW0pO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHNlYXJjaFJvdXRlciBmcm9tICcuL2FwaS9zZWFyY2guanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzIH0gZnJvbSAnLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgY2xlYXJNZW1vcnkgfSBmcm9tICcuL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBJTklUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vaW5pdCcsIGFzeW5jIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ107XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ01pc3NpbmcgeC1zZXNzaW9uLWlkIGhlYWRlcicsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICB9XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgdHJ5IHtcbiAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogdHJ1ZSwgc2Vzc2lvbklkIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ1Nlc3Npb24gaW5pdCB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiBmYWxzZSwgc2Vzc2lvbklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gUkVTVE9SRSBNRU1PUlkgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9yZXN0b3JlLW1lbW9yeScsIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCB7IGNvbnZJZCwgbWVzc2FnZXMgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghY29udklkIHx8ICFBcnJheS5pc0FycmF5KG1lc3NhZ2VzKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnY29udklkIGFuZCBtZXNzYWdlcyBhcmUgcmVxdWlyZWQnLCBjb2RlOiAnQkFEX1JFUVVFU1QnIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICAvLyBBbHdheXMgd2lwZSB0aGUgY29udklkIG1lbW9yeSBmaXJzdCBzbyByZXBsYXlpbmcgbmV2ZXIgZG91YmxlcyB1cCB0dXJuc1xuICAgIGNsZWFyTWVtb3J5KGNvbnZJZCk7XG5cbiAgICBmb3IgKGNvbnN0IG1zZyBvZiBtZXNzYWdlcykge1xuICAgICAgaWYgKChtc2cucm9sZSA9PT0gJ3VzZXInIHx8IG1zZy5yb2xlID09PSAnYXNzaXN0YW50JykgJiYgdHlwZW9mIG1zZy5jb250ZW50ID09PSAnc3RyaW5nJykge1xuICAgICAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsIG1zZy5yb2xlLCBtc2cuY29udGVudCk7XG4gICAgICB9XG4gICAgfVxuICAgIHJlcy5qc29uKHsgb2s6IHRydWUsIGNvbnZJZCwgcmVzdG9yZWQ6IG1lc3NhZ2VzLmxlbmd0aCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdNZW1vcnkgcmVzdG9yZSB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IG9rOiBmYWxzZSwgY29udklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuYXBwLnVzZSgnL3NlYXJjaCcsIHNlYXJjaFJvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxtQkFBbUI7QUFDelMsU0FBUyxNQUFNLGNBQWM7QUFNN0IsU0FBUyxpQkFBaUI7QUFDeEIsTUFBSSxDQUFDLGFBQWE7QUFDaEIsVUFBTSxTQUFTLFFBQVEsSUFBSTtBQUMzQixVQUFNLFNBQVMsUUFBUSxJQUFJLGlCQUFpQjtBQUM1QyxVQUFNLFdBQVcsUUFBUSxJQUFJLG1CQUFtQjtBQUNoRCxVQUFNLE9BQU8sUUFBUSxJ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LElBQUksR0FBRyxDQUFDLElBQUksUUFBUSxNQUFNLEtBQUs7QUFBQSxFQUNuRjtBQUNKO0FBS0EsSUFBSUMsYUFBWUMsTUFBSyxRQUFRQyxlQUFjSCx5Q0FBZSxDQUFDO0FBQzNELFNBQVMsZ0JBQWdCO0FBQ3JCLE1BQUlJO0FBQ0osU0FBTztBQUFBLElBQ0gsTUFBTTtBQUFBLElBQ04saUJBQWlCLFNBQVUsUUFBUTtBQUMvQixhQUFPLFVBQVUsTUFBTSxRQUFRLFFBQVEsV0FBWTtBQUMvQyxZQUFJO0FBQ0osZUFBTyxZQUFZLE1BQU0sU0FBVSxJQUFJO0FBQ25DLGtCQUFRLEdBQUcsT0FBTztBQUFBLFlBQ2QsS0FBSztBQUFHLHFCQUFPLENBQUMsR0FBYSx1REFBeUI7QUFBQSxZQUN0RCxLQUFLO0FBQ0QsMkJBQWMsR0FBRyxLQUFLLEVBQUc7QUFDekIsY0FBQUEsT0FBTTtBQUNOLHFCQUFPLFlBQVksSUFBSSxRQUFRLFNBQVUsS0FBSyxLQUFLLE1BQU07QUFDckQsZ0JBQUFBLEtBQUksS0FBSyxLQUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0YsTUFBSyxRQUFRRCxZQUFXLE9BQU87QUFBQSxJQUN4QztBQUFBLEVBQ0o7QUFBQSxFQUNBLFFBQVE7QUFBQSxJQUNKLE1BQU07QUFBQSxFQUNWO0FBQ0osQ0FBQzsiLAogICJuYW1lcyI6IFsibW9kZWwiLCAidXVpZHY0IiwgImdsb2JhbENvbGxlY3Rpb24iLCAic2Vzc2lvbiIsICJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAicm91dGVyIiwgIkdvb2dsZUdlbmVyYXRpdmVBSSIsICJnZW5BSSIsICJtb2RlbCIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAiZ2VuQUkiLCAiUFJJTUFSWV9NT0RFTCIsICJtb2RlbCIsICJSb3V0ZXIiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIl0KfQo=
