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
    for (let i = 0; i < ids.length; i += BATCH_SIZE) {
      const batchIds = ids.slice(i, i + BATCH_SIZE);
      const batchEmbeddings = embeddings.slice(i, i + BATCH_SIZE);
      const batchDocuments = vectors.slice(i, i + BATCH_SIZE).map((v) => v.text);
      const batchMetadatas = vectors.slice(i, i + BATCH_SIZE).map((v) => v.metadata);
      await collection.add({
        ids: batchIds,
        embeddings: batchEmbeddings,
        documents: batchDocuments,
        metadatas: batchMetadatas
      });
      console.log(`  [addVectors] batch ${Math.floor(i / BATCH_SIZE) + 1}: added ${batchIds.length} vectors`);
    }
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
    const allIds = [];
    let offset = 0;
    while (true) {
      const batch = await collection.get({
        where: { document_id: documentId },
        include: [],
        limit: BATCH_SIZE,
        offset
      });
      if (!batch.ids || batch.ids.length === 0) break;
      allIds.push(...batch.ids);
      if (batch.ids.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }
    if (allIds.length > 0) {
      await collection.delete({ ids: allIds });
    }
    return allIds.length;
  } catch (error) {
    console.error("Failed to delete document vectors:", error);
    throw error;
  }
}
async function listDocuments(collection) {
  try {
    const documentsMap = /* @__PURE__ */ new Map();
    let offset = 0;
    while (true) {
      const batch = await collection.get({
        include: ["metadatas", "documents"],
        limit: BATCH_SIZE,
        offset
      });
      if (!batch.ids || batch.ids.length === 0) break;
      batch.ids.forEach((id, idx) => {
        const meta = batch.metadatas[idx];
        const docId = meta.document_id;
        if (!documentsMap.has(docId)) {
          documentsMap.set(docId, {
            document_id: docId,
            filename: meta.filename,
            chunk_count: 0,
            page_count: meta.page_number || 1,
            upload_timestamp: meta.upload_timestamp,
            source_type: meta.source_type,
            first_chunk_text: batch.documents[idx]
          });
        }
        const doc = documentsMap.get(docId);
        doc.chunk_count++;
        doc.page_count = Math.max(doc.page_count, meta.page_number || 1);
      });
      console.log(`  [listDocuments] offset=${offset}, got=${batch.ids.length}, unique so far=${documentsMap.size}`);
      if (batch.ids.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
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
var BATCH_SIZE, cloudClient, globalCollection, sessionCollections;
var init_chromaService = __esm({
  "server/services/chromaService.js"() {
    "use strict";
    BATCH_SIZE = 300;
    cloudClient = null;
    globalCollection = null;
    sessionCollections = /* @__PURE__ */ new Map();
  }
});

// server/utils/errors.js
function is429Error(error) {
  return error?.code === 429 || error?.status === 429 || error?.message?.includes("429") || error?.message?.includes("RESOURCE_EXHAUSTED") || error?.message?.includes("Too Many Requests");
}
var AppError, ValidationError, InvalidFileTypeError, CorruptedPDFError, LLMUnavailableError, EmbeddingError, WebSearchUnavailableError;
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
import { GoogleGenAI } from "file:///home/project/node_modules/@google/genai/dist/node/index.mjs";
function getEmbeddingModel() {
  if (!embeddingModel) {
    ai = new GoogleGenAI({
      vertexai: true,
      project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || "project-d48e2f39-2685-4746-aa0",
      location: process.env.GOOGLE_CLOUD_LOCATION || "us-central1"
    });
    embeddingModel = ai.models;
  }
  return embeddingModel;
}
async function embedBatch(texts, taskType = "RETRIEVAL_DOCUMENT", attempt = 1) {
  const maxAttempts = 5;
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-001";
  try {
    const model2 = getEmbeddingModel();
    const embeddingPromises = texts.map(async (rawText) => {
      const text = typeof rawText === "string" ? rawText : String(rawText);
      if (!text || text.trim() === "") {
        throw new EmbeddingError("Cannot embed an empty or missing text block");
      }
      const response = await model2.embedContent({
        model: modelName,
        contents: text,
        config: {
          taskType,
          outputDimensionality: OUTPUT_DIMENSIONS()
        }
      });
      const values = response?.embeddings?.[0]?.values || response?.embedding?.values || response?.values;
      if (!values) {
        console.error("[embedding] Unexpected API response shape:", JSON.stringify(response));
        throw new EmbeddingError("Missing values in embedding response");
      }
      return values;
    });
    const embeddings = await Promise.all(embeddingPromises);
    if (embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${embeddings.length}`);
    }
    return embeddings;
  } catch (error) {
    const isRetryable = is429Error(error) || error?.status === 429 || error?.status === 502 || error?.status === 503 || error?.message?.includes("RESOURCE_EXHAUSTED") || error?.message?.includes("Service Unavailable") || error?.message?.includes("Bad Gateway");
    if (isRetryable && attempt < maxAttempts) {
      const baseDelay = error.retryAfter || attempt * RETRY_WAIT_MS;
      const retryDelay = error?.status === 429 ? GROUP_WAIT_MS : baseDelay;
      console.log(`[embedding] Transient error (${error?.status || "unknown"}), waiting ${retryDelay / 1e3}s (attempt ${attempt}/${maxAttempts})...`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      return embedBatch(texts, taskType, attempt + 1);
    }
    throw new EmbeddingError(error.message || "Batch embedding failed");
  }
}
async function embedSingleBatchGroup(texts, taskType = "RETRIEVAL_DOCUMENT") {
  console.log(`[embedding] embedSingleBatchGroup \u2014 ${texts.length} texts, taskType=${taskType}`);
  const vectors = await embedBatch(texts, taskType);
  console.log(`[embedding] embedSingleBatchGroup \u2014 got ${vectors.length} vectors`);
  return vectors;
}
async function embedQuery(query) {
  const vectors = await embedBatch([query], "RETRIEVAL_QUERY");
  return vectors[0];
}
function getRateLimitState() {
  return {
    maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 3e4,
    parallelCalls: PARALLEL_CALLS(),
    maxChunksPerCall: BATCH_SIZE2(),
    outputDimensions: OUTPUT_DIMENSIONS()
  };
}
var ai, embeddingModel, BATCH_SIZE2, PARALLEL_CALLS, OUTPUT_DIMENSIONS, GROUP_WAIT_MS, RETRY_WAIT_MS;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    ai = null;
    embeddingModel = null;
    BATCH_SIZE2 = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
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
    deletedDocumentIds: /* @__PURE__ */ new Set(),
    // track deleted doc IDs to filter prompt memory
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
  if (seededSessions.has(sessionId)) {
    console.log(`[session] Already seeded ${sessionId}, skipping`);
    return;
  }
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
    const BATCH_SIZE3 = 300;
    let offset = 0;
    const allIds = [], allEmbeddings = [], allDocuments = [], allMetadatas = [];
    while (true) {
      const batch = await globalCollection2.get({
        include: ["embeddings", "documents", "metadatas"],
        limit: BATCH_SIZE3,
        offset
      });
      if (!batch.ids || batch.ids.length === 0) break;
      allIds.push(...batch.ids);
      allEmbeddings.push(...batch.embeddings);
      allDocuments.push(...batch.documents);
      allMetadatas.push(...batch.metadatas);
      if (batch.ids.length < BATCH_SIZE3) break;
      offset += BATCH_SIZE3;
    }
    if (allIds.length === 0) {
      console.log("\u26A0\uFE0F  Global collection is empty \u2014 nothing to seed.");
      seededSessions.add(sessionId);
      return;
    }
    for (let i = 0; i < allIds.length; i += BATCH_SIZE3) {
      await sessionCollection.add({
        ids: allIds.slice(i, i + BATCH_SIZE3),
        embeddings: allEmbeddings.slice(i, i + BATCH_SIZE3),
        documents: allDocuments.slice(i, i + BATCH_SIZE3),
        metadatas: allMetadatas.slice(i, i + BATCH_SIZE3).map((m) => ({ ...m, source_type: "global" }))
      });
      console.log(`  \u{1F4E6} Added batch ${Math.floor(i / BATCH_SIZE3) + 1}: records ${i + 1}\u2013${Math.min(i + BATCH_SIZE3, allIds.length)}`);
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
  const existing = session.documents.find((d) => d.id === documentInfo.id);
  if (existing) {
    if (documentInfo.chunkCount !== void 0) existing.chunkCount = documentInfo.chunkCount;
    if (documentInfo.pageCount !== void 0) existing.pageCount = documentInfo.pageCount;
    if (documentInfo.fileSize !== void 0) existing.fileSize = documentInfo.fileSize;
    if (documentInfo.status !== void 0) existing.status = documentInfo.status;
    if (documentInfo.filename !== void 0) existing.filename = documentInfo.filename;
    session.lastAccessed = /* @__PURE__ */ new Date();
    console.log(`[session] Updated doc ${documentInfo.id} \u2014 status=${existing.status}, chunks=${existing.chunkCount}`);
    return true;
  }
  session.documents.push({
    id: documentInfo.id,
    filename: documentInfo.filename,
    fileSize: documentInfo.fileSize,
    pageCount: documentInfo.pageCount,
    uploadTimestamp: /* @__PURE__ */ new Date(),
    chunkCount: documentInfo.chunkCount ?? 0,
    sourceType: "session_upload",
    status: documentInfo.status ?? "indexing"
  });
  session.lastAccessed = /* @__PURE__ */ new Date();
  console.log(`[session] Added doc ${documentInfo.id} \u2014 status=${documentInfo.status ?? "indexing"}`);
  return true;
}
function removeDocumentFromSession(sessionId, documentId) {
  const session = getSession(sessionId);
  if (!session) return false;
  const idx = session.documents.findIndex((d) => d.id === documentId);
  if (idx >= 0) {
    session.documents.splice(idx, 1);
    session.deletedDocumentIds.add(documentId);
    session.lastAccessed = /* @__PURE__ */ new Date();
    console.log(`[session] Removed doc ${documentId}, added to deletedDocumentIds`);
    return true;
  }
  return false;
}
function getDeletedDocumentIds(sessionId) {
  const session = getSession(sessionId);
  return session?.deletedDocumentIds ?? /* @__PURE__ */ new Set();
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
    fileSize: doc.fileSize || null,
    status: doc.status ?? null
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
function sseEvent(res, event, data) {
  res.write(`event: ${event}
data: ${JSON.stringify(data)}

`);
}
async function handleUpload(req, res) {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  const BATCH_SIZE3 = parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
  const PARALLEL_CALLS2 = parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
  const GROUP_WAIT_MS2 = parseInt(process.env.EMBEDDING_GROUP_WAIT_MS) || 61e3;
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
      sseEvent(res, "error", { message: `Maximum ${maxPDFs} uploads reached`, code: "TOO_MANY_PDFS" });
      return res.end();
    }
    if (session.documents.some((d) => d.filename === cleanFilename)) {
      fs.unlinkSync(file.path);
      sseEvent(res, "error", { message: `"${cleanFilename}" already uploaded`, code: "DUPLICATE_FILE" });
      return res.end();
    }
    console.log(`[upload] [${sessionId}] Phase 1 \u2014 parsing ${cleanFilename} (${file.size} bytes)`);
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.path);
    if (!fullText || fullText.trim().length < 50) {
      fs.unlinkSync(file.path);
      sseEvent(res, "error", { message: "No extractable text \u2014 PDF may be scanned or image-only", code: "EMPTY_PDF" });
      return res.end();
    }
    const documentId = uuidv44();
    const rawChunks = chunkText(fullText, { chunkSizeTokens: 1e3, overlapTokens: 200 });
    if (rawChunks.length === 0) {
      fs.unlinkSync(file.path);
      sseEvent(res, "error", { message: "No content could be extracted from PDF", code: "EMPTY_PDF" });
      return res.end();
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
    const totalChunks = chunks.length;
    const totalBatches = Math.ceil(totalChunks / BATCH_SIZE3);
    const totalSets = Math.ceil(totalBatches / PARALLEL_CALLS2);
    console.log(`[upload] [${sessionId}] ${totalChunks} chunks \u2192 ${totalBatches} API calls \u2192 ${totalSets} sets of ${PARALLEL_CALLS2} parallel`);
    sseEvent(res, "upload_complete", {
      documentId,
      filename: cleanFilename,
      fileSize: file.size,
      pageCount: totalPages,
      totalChunks,
      totalBatches,
      totalSets
    });
    addDocumentToSession(sessionId, {
      id: documentId,
      filename: cleanFilename,
      fileSize: file.size,
      pageCount: totalPages,
      chunkCount: 0,
      status: "indexing"
    });
    console.log(`[upload] [${sessionId}] Phase 1 done \u2014 ${cleanFilename} added to session as indexing`);
    const { collection } = await getSessionCollection(sessionId);
    let processedChunks = 0;
    const allEmbeddings = [];
    const batches = [];
    for (let i = 0; i < chunks.length; i += BATCH_SIZE3) batches.push(chunks.slice(i, i + BATCH_SIZE3));
    const sets = [];
    for (let i = 0; i < batches.length; i += PARALLEL_CALLS2) sets.push(batches.slice(i, i + PARALLEL_CALLS2));
    console.log(`[upload] [${sessionId}] Phase 2 start \u2014 ${sets.length} sets`);
    for (let setIdx = 0; setIdx < sets.length; setIdx++) {
      const isLastSet = setIdx === sets.length - 1;
      const currentSet = sets[setIdx];
      const setChunkCount = currentSet.reduce((acc, b) => acc + b.length, 0);
      console.log(`[upload] [${sessionId}] Set ${setIdx + 1}/${sets.length} \u2014 embedding ${currentSet.length} batch call(s) (${setChunkCount} chunks) in parallel`);
      const embedResults = await Promise.allSettled(
        currentSet.map((batch) => embedSingleBatchGroup(batch.map((c) => c.text)))
      );
      const setEmbeddings = [];
      embedResults.forEach((result, batchIdx) => {
        const batch = currentSet[batchIdx];
        if (result.status === "fulfilled") {
          result.value.forEach((vector, chunkIdx) => {
            setEmbeddings.push({
              id: batch[chunkIdx].metadata.chunk_id,
              embedding: vector,
              metadata: batch[chunkIdx].metadata,
              text: batch[chunkIdx].text
            });
          });
          console.log(`[upload] [${sessionId}]   Batch ${setIdx * PARALLEL_CALLS2 + batchIdx + 1} embedded OK (${batch.length} chunks)`);
        } else {
          console.error(`[upload] [${sessionId}]   Batch ${setIdx * PARALLEL_CALLS2 + batchIdx + 1} FAILED:`, result.reason?.message);
        }
      });
      processedChunks += setEmbeddings.length;
      allEmbeddings.push(...setEmbeddings);
      console.log(`[upload] [${sessionId}] Set ${setIdx + 1} embedded \u2014 ${processedChunks}/${totalChunks} chunks so far`);
      if (!isLastSet) {
        console.log(`[upload] [${sessionId}] Starting ${GROUP_WAIT_MS2 / 1e3}s timer + Chroma write concurrently for set ${setIdx + 1}`);
        const timer = new Promise((r) => setTimeout(r, GROUP_WAIT_MS2));
        const chromaWrite = addVectors(
          collection,
          setEmbeddings.map((e) => ({ text: e.text, metadata: e.metadata })),
          setEmbeddings.map((e) => e.embedding),
          setEmbeddings.map((e) => e.id)
        ).then(() => console.log(`[upload] [${sessionId}] Chroma write done for set ${setIdx + 1} (${setEmbeddings.length} vectors)`)).catch((err) => console.error(`[upload] [${sessionId}] Chroma write FAILED for set ${setIdx + 1}:`, err.message));
        sseEvent(res, "embedding_progress", {
          processedChunks,
          totalChunks,
          setIndex: setIdx + 1,
          totalSets,
          waitingMs: GROUP_WAIT_MS2,
          chromaWriteComplete: false
        });
        await Promise.all([timer, chromaWrite]);
        console.log(`[upload] [${sessionId}] Timer + Chroma both done for set ${setIdx + 1}, proceeding to set ${setIdx + 2}`);
      } else {
        console.log(`[upload] [${sessionId}] Last set ${setIdx + 1} \u2014 awaiting Chroma write directly`);
        await addVectors(
          collection,
          setEmbeddings.map((e) => ({ text: e.text, metadata: e.metadata })),
          setEmbeddings.map((e) => e.embedding),
          setEmbeddings.map((e) => e.id)
        );
        console.log(`[upload] [${sessionId}] Chroma write complete for last set (${setEmbeddings.length} vectors)`);
        sseEvent(res, "embedding_progress", {
          processedChunks,
          totalChunks,
          setIndex: setIdx + 1,
          totalSets,
          waitingMs: 0,
          chromaWriteComplete: true
        });
      }
    }
    invalidateSessionCollectionCache(sessionId);
    addDocumentToSession(sessionId, {
      id: documentId,
      filename: cleanFilename,
      fileSize: file.size,
      pageCount: totalPages,
      chunkCount: allEmbeddings.length,
      status: "ready"
    });
    console.log(`[upload] [${sessionId}] \u2705 Done \u2014 ${allEmbeddings.length} vectors in Chroma for ${cleanFilename}`);
    sseEvent(res, "done", {
      document: {
        id: documentId,
        filename: cleanFilename,
        fileSize: file.size,
        pageCount: totalPages,
        chunkCount: allEmbeddings.length,
        uploadTimestamp: (/* @__PURE__ */ new Date()).toISOString()
      },
      sessionId
    });
    res.end();
  } catch (error) {
    if (req.file && fs.existsSync(req.file.path)) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {
      }
    }
    console.error("[upload] Unhandled error:", error);
    sseEvent(res, "error", { message: error.message || "Upload failed", code: error.code || "UPLOAD_ERROR" });
    res.end();
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
  const filename = req.query.filename;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
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
      const filePath = path2.join(uploadDir, filename);
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        console.log(`[delete] Removed file: ${filePath}`);
      } else {
        console.warn(`[delete] File not found on disk: ${filePath}`);
      }
    }
    res.json({ success: true, documentId });
  } catch (error) {
    console.error("Delete document error:", error);
    res.status(500).json({ error: "Failed to delete document", code: "DELETE_ERROR" });
  }
}
async function getDocumentFile(req, res) {
  const filename = req.query.filename;
  try {
    if (filename) {
      const uploadPath = path2.join(uploadDir, filename);
      if (fs.existsSync(uploadPath)) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", contentDisposition(filename));
        return fs.createReadStream(uploadPath).pipe(res);
      }
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
    init_memoryService();
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
      filename: (req, file, cb) => cb(null, sanitizeFilename(file.originalname))
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

// server/services/promptService.js
var init_promptService = __esm({
  "server/services/promptService.js"() {
    "use strict";
    init_memoryService();
    init_retrievalService();
  }
});

// server/services/geminiService.js
import { GoogleGenAI as GoogleGenAI2 } from "file:///home/project/node_modules/@google/genai/dist/node/index.mjs";
function getGenAI() {
  if (!genAI) {
    genAI = new GoogleGenAI2({
      vertexai: true,
      project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || "project-d48e2f39-2685-4746-aa0",
      location: process.env.GOOGLE_CLOUD_LOCATION || "us-central1"
    });
  }
  return genAI;
}
function getPrimaryModelName() {
  return PRIMARY_MODEL;
}
function getFallbackModelName() {
  return FALLBACK_MODEL;
}
function getTextFromChunk(chunk) {
  if (typeof chunk?.text === "string") return chunk.text;
  if (typeof chunk?.text === "function") return chunk.text();
  return "";
}
async function* streamResponse(prompt) {
  let modelName = getPrimaryModelName();
  let retries = 0;
  const maxRetries = 2;
  while (retries < maxRetries) {
    try {
      const controller = new AbortController();
      const result = await getGenAI().models.generateContentStream({
        model: modelName,
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        config: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      }, {
        signal: controller.signal
      });
      let firstToken = true;
      const firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);
      for await (const chunk of result.stream) {
        if (controller.signal.aborted) {
          clearTimeout(firstTokenTimeout);
          throw new Error("First token timeout \u2014 no response from model");
        }
        const text = getTextFromChunk(chunk);
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
      modelName = getFallbackModelName();
    }
  }
}
var genAI, PRIMARY_MODEL, FALLBACK_MODEL, FIRST_TOKEN_TIMEOUT, REQUEST_TIMEOUT;
var init_geminiService = __esm({
  "server/services/geminiService.js"() {
    "use strict";
    init_promptService();
    init_errors();
    genAI = null;
    PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || "gemini-3.1-flash-lite";
    FALLBACK_MODEL = process.env.GEMINI_MODEL_FALLBACK || "gemini-2.5-flash";
    FIRST_TOKEN_TIMEOUT = parseInt(process.env.LLM_FIRST_TOKEN_TIMEOUT_SECONDS) * 1e3 || 12e3;
    REQUEST_TIMEOUT = parseInt(process.env.LLM_REQUEST_TIMEOUT_SECONDS) * 1e3 || 45e3;
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
    const deletedDocIds = getDeletedDocumentIds(sessionId);
    const allRecentTurns = getRecentTurns(convId, 10);
    const filteredTurns = [];
    for (let i = 0; i < allRecentTurns.length; i++) {
      const turn = allRecentTurns[i];
      if (turn.role === "assistant") {
        const citesDeletedDoc = turn.citations?.some((c) => deletedDocIds.has(c.documentId));
        if (citesDeletedDoc) {
          if (filteredTurns.length > 0 && filteredTurns[filteredTurns.length - 1].role === "user") {
            filteredTurns.pop();
          }
          continue;
        }
      }
      filteredTurns.push(turn);
    }
    const questions = filteredTurns.filter((t) => t.role === "user");
    const answers = filteredTurns.filter((t) => t.role === "assistant");
    const qSection = questions.map((t, i) => `Q${i + 1}: ${t.content}`).join("\n");
    const aSection = answers.map((t, i) => `A${i + 1}: ${t.content}`).join("\n");
    const memoryContext = filteredTurns.length > 0 ? `Previous Questions:
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
import { GoogleGenerativeAI } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
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
    genAI2 = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcbiAgICBjbG91ZENsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xvdWRDbGllbnQ7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBHbG9iYWwgY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGNvbm5lY3QgdG8gZ2xvYmFsIGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG4gIHJldHVybiBnbG9iYWxDb2xsZWN0aW9uO1xufVxuXG4vKipcbiAqIFJldHVybnMgeyBjb2xsZWN0aW9uLCBpc05ldyB9LlxuICogaXNOZXcgPSB0cnVlICBcdTIxOTIgZnJlc2hseSBjcmVhdGVkLCBuZWVkcyBzZWVkaW5nIGZyb20gZ2xvYmFsLlxuICogaXNOZXcgPSBmYWxzZSBcdTIxOTIgYWxyZWFkeSBleGlzdGVkIG9uIENocm9tYSBDbG91ZCwgcmVzcGVjdCBpdHMgY3VycmVudCBzdGF0ZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHsgY29sbGVjdGlvbjogc2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpLCBpc05ldzogZmFsc2UgfTtcbiAgfVxuXG4gIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcblxuICBsZXQgY29sbGVjdGlvbjtcbiAgbGV0IGlzTmV3O1xuXG4gIHRyeSB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICB9KTtcbiAgICBpc05ldyA9IGZhbHNlO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNjdiXFx1ZmUwZiAgU2Vzc2lvbiBjb2xsZWN0aW9uIGV4aXN0cywgcmV1c2luZzogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfSBjYXRjaCB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5jcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIGlzTmV3ID0gdHJ1ZTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY3JlYXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfVxuXG4gIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgcmV0dXJuIHsgY29sbGVjdGlvbiwgaXNOZXcgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gZGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbi8qKlxuICogQWRkIHZlY3RvcnMgaW4gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIHRvIGF2b2lkIENocm9tYSBwYXlsb2FkIGxpbWl0cy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBpZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGNvbnN0IGJhdGNoSWRzICAgICAgICA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzICA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyAgPSB2ZWN0b3JzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAodiA9PiB2Lm1ldGFkYXRhKTtcblxuICAgICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6ICAgICAgICBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6ICBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiAgYmF0Y2hNZXRhZGF0YXNcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgW2FkZFZlY3RvcnNdIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogYWRkZWQgJHtiYXRjaElkcy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPiBkZWZhdWx0IDEwMCBsaW1pdCkgYXJlIGZ1bGx5IGRlbGV0ZWQuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9LFxuICAgICAgICBpbmNsdWRlOiBbXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGFsbElkcyB9KTtcbiAgICB9XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG4vKipcbiAqIExpc3QgYWxsIHVuaXF1ZSBkb2N1bWVudHMgaW4gYSBjb2xsZWN0aW9uLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgd2l0aCBCQVRDSF9TSVpFPTMwMCBzbyBjb2xsZWN0aW9ucyBsYXJnZXJcbiAqIHRoYW4gQ2hyb21hJ3MgZGVmYXVsdCBnZXQoKSBsaW1pdCAoMTAwKSBhcmUgZnVsbHkgZW51bWVyYXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcblxuICAgICAgYmF0Y2guaWRzLmZvckVhY2goKGlkLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgbWV0YSAgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2NJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiAgICAgICAgIG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogICAgICAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogICAgICAgbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6ICAgICAgbWV0YS5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIGZpcnN0X2NodW5rX3RleHQ6IGJhdGNoLmRvY3VtZW50c1tpZHhdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBkb2MgPSBkb2N1bWVudHNNYXAuZ2V0KGRvY0lkKTtcbiAgICAgICAgZG9jLmNodW5rX2NvdW50Kys7XG4gICAgICAgIGRvYy5wYWdlX2NvdW50ID0gTWF0aC5tYXgoZG9jLnBhZ2VfY291bnQsIG1ldGEucGFnZV9udW1iZXIgfHwgMSk7XG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYCAgW2xpc3REb2N1bWVudHNdIG9mZnNldD0ke29mZnNldH0sIGdvdD0ke2JhdGNoLmlkcy5sZW5ndGh9LCB1bmlxdWUgc28gZmFyPSR7ZG9jdW1lbnRzTWFwLnNpemV9YCk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShkb2N1bWVudHNNYXAudmFsdWVzKCkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50czonLCBlcnJvcik7XG4gICAgcmV0dXJuIFtdO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGhDaGVjaygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY2xlYW51cFNlc3Npb25Db2xsZWN0aW9ucygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25zID0gYXdhaXQgY2xpZW50Lmxpc3RDb2xsZWN0aW9ucygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcXHUyNzA1IE5vIHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbnMgZm91bmQuJyk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFxcdWQ4M2VcXHVkZGY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFxcdTI3MDUgRGVsZXRlZDogJHtuYW1lfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYCAgXFx1MjZhMFxcdWZlMGYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1xcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGNsZWFudXAgY29tcGxldGUuJyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS53YXJuKCdcXHUyNmEwXFx1ZmUwZiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7ZXhwb3J0IGNsYXNzIEFwcEVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlLCBzdGF0dXNDb2RlID0gNTAwKSB7XG4gICAgc3VwZXIobWVzc2FnZSk7XG4gICAgdGhpcy5jb2RlID0gY29kZTtcbiAgICB0aGlzLnN0YXR1c0NvZGUgPSBzdGF0dXNDb2RlO1xuICAgIHRoaXMuaXNPcGVyYXRpb25hbCA9IHRydWU7XG4gICAgRXJyb3IuY2FwdHVyZVN0YWNrVHJhY2UodGhpcywgdGhpcy5jb25zdHJ1Y3Rvcik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFZhbGlkYXRpb25FcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdWQUxJREFUSU9OX0VSUk9SJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFVwbG9hZExpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVVBMT0FEX0xJTUlUX0VYQ0VFREVEJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEZpbGVUb29MYXJnZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXhTaXplTUIpIHtcbiAgICBzdXBlcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmAsICdGSUxFX1RPT19MQVJHRScsIDQxMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEludmFsaWRGaWxlVHlwZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignT25seSBQREYgZmlsZXMgYXJlIGFsbG93ZWQnLCAnSU5WQUxJRF9GSUxFX1RZUEUnLCA0MTUpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBUb29NYW55UERGc0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXgpIHtcbiAgICBzdXBlcihgTWF4aW11bSAke21heH0gUERGcyBhbGxvd2VkIHBlciBzZXNzaW9uYCwgJ1RPT19NQU5ZX1BERlMnLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBEdXBsaWNhdGVGaWxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKGZpbGVuYW1lKSB7XG4gICAgc3VwZXIoYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmAsICdEVVBMSUNBVEVfRklMRScsIDQwOSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvcnJ1cHRlZFBERkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRmFpbGVkIHRvIHBhcnNlIFBERiBmaWxlLiBJdCBtYXkgYmUgY29ycnVwdGVkLicsICdDT1JSVVBURURfUERGJywgNDIyKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmF0ZUxpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKHJldHJ5QWZ0ZXIgPSA2MCkge1xuICAgIHN1cGVyKCdSYXRlIGxpbWl0IGV4Y2VlZGVkLiBQbGVhc2UgdHJ5IGFnYWluIGxhdGVyLicsICdSQVRFX0xJTUlUX0VYQ0VFREVEJywgNDI5KTtcbiAgICB0aGlzLnJldHJ5QWZ0ZXIgPSByZXRyeUFmdGVyO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBMTE1VbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignQUkgc2VydmljZSBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZS4gUGxlYXNlIHRyeSBhZ2Fpbi4nLCAnTExNX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRW1iZWRkaW5nRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UgPSAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgJ0VNQkVERElOR19FUlJPUicsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJldHJpZXZhbFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdEb2N1bWVudCByZXRyaWV2YWwgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnUkVUUklFVkFMX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ1dlYiBzZWFyY2ggaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnV0VCX1NFQVJDSF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBFbWJlZGRpbmdFcnJvciwgaXM0MjlFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmxldCBhaSA9IG51bGw7XG5sZXQgZW1iZWRkaW5nTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRFbWJlZGRpbmdNb2RlbCgpIHtcbiAgaWYgKCFlbWJlZGRpbmdNb2RlbCkge1xuICAgIGFpID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdDogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgcHJvY2Vzcy5lbnYuR0NQX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCcsXG4gICAgICBsb2NhdGlvbjogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX0xPQ0FUSU9OIHx8ICd1cy1jZW50cmFsMSdcbiAgICB9KTtcblxuICAgIGVtYmVkZGluZ01vZGVsID0gYWkubW9kZWxzO1xuICB9XG4gIHJldHVybiBlbWJlZGRpbmdNb2RlbDtcbn1cblxuY29uc3QgQkFUQ0hfU0laRSAgICAgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSAgfHwgNDtcbmNvbnN0IE9VVFBVVF9ESU1FTlNJT05TID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19ESU1FTlNJT05TKSB8fCAzMDcyO1xuY29uc3QgR1JPVVBfV0FJVF9NUyAgPSA2MTAwMDtcbmNvbnN0IFJFVFJZX1dBSVRfTVMgID0gMTUwMDA7XG5cbi8vIEVtYmVkIGEgc2luZ2xlIGJhdGNoIG9mIHRleHRzICh1cCB0byBCQVRDSF9TSVpFKS5cbi8vIFJldHJpZXMgb24gNDI5IGFuZCB0cmFuc2llbnQgNTAyLzUwMyBlcnJvcnMgdXAgdG8gNSB0aW1lcy5cbmFzeW5jIGZ1bmN0aW9uIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIGF0dGVtcHQgPSAxKSB7XG4gIGNvbnN0IG1heEF0dGVtcHRzID0gNTtcbiAgY29uc3QgbW9kZWxOYW1lID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19NT0RFTCB8fCAnZ2VtaW5pLWVtYmVkZGluZy0wMDEnO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRFbWJlZGRpbmdNb2RlbCgpO1xuXG4gICAgY29uc3QgZW1iZWRkaW5nUHJvbWlzZXMgPSB0ZXh0cy5tYXAoYXN5bmMgKHJhd1RleHQpID0+IHtcbiAgICAgIC8vIENvZXJjZSBzYWZlbHkgdG8gc3RyaW5nIHRvIHByZXZlbnQgQVBJIGlucHV0IHZhbGlkYXRpb24gZmFpbHVyZXNcbiAgICAgIGNvbnN0IHRleHQgPSB0eXBlb2YgcmF3VGV4dCA9PT0gJ3N0cmluZycgPyByYXdUZXh0IDogU3RyaW5nKHJhd1RleHQpO1xuICAgICAgXG4gICAgICBpZiAoIXRleHQgfHwgdGV4dC50cmltKCkgPT09ICcnKSB7XG4gICAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcignQ2Fubm90IGVtYmVkIGFuIGVtcHR5IG9yIG1pc3NpbmcgdGV4dCBibG9jaycpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IG1vZGVsLmVtYmVkQ29udGVudCh7XG4gICAgICAgIG1vZGVsOiBtb2RlbE5hbWUsXG4gICAgICAgIGNvbnRlbnRzOiB0ZXh0LFxuICAgICAgICBjb25maWc6IHtcbiAgICAgICAgICB0YXNrVHlwZSxcbiAgICAgICAgICBvdXRwdXREaW1lbnNpb25hbGl0eTogT1VUUFVUX0RJTUVOU0lPTlMoKVxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgLy8gSGFuZGxlIHN0cnVjdHVyYWwgdmFyaWF0aW9ucyBpbiB0aGUgU0RLIHJlc3BvbnNlIHBheWxvYWRcbiAgICAgIGNvbnN0IHZhbHVlcyA9IHJlc3BvbnNlPy5lbWJlZGRpbmdzPy5bMF0/LnZhbHVlcyB8fCBcbiAgICAgICAgICAgICAgICAgICAgIHJlc3BvbnNlPy5lbWJlZGRpbmc/LnZhbHVlcyB8fCBcbiAgICAgICAgICAgICAgICAgICAgIHJlc3BvbnNlPy52YWx1ZXM7XG5cbiAgICAgIGlmICghdmFsdWVzKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoJ1tlbWJlZGRpbmddIFVuZXhwZWN0ZWQgQVBJIHJlc3BvbnNlIHNoYXBlOicsIEpTT04uc3RyaW5naWZ5KHJlc3BvbnNlKSk7XG4gICAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcignTWlzc2luZyB2YWx1ZXMgaW4gZW1iZWRkaW5nIHJlc3BvbnNlJyk7XG4gICAgICB9XG5cbiAgICAgIHJldHVybiB2YWx1ZXM7XG4gICAgfSk7XG5cbiAgICBjb25zdCBlbWJlZGRpbmdzID0gYXdhaXQgUHJvbWlzZS5hbGwoZW1iZWRkaW5nUHJvbWlzZXMpO1xuXG4gICAgaWYgKGVtYmVkZGluZ3MubGVuZ3RoICE9PSB0ZXh0cy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihgRXhwZWN0ZWQgJHt0ZXh0cy5sZW5ndGh9IGVtYmVkZGluZ3MsIGdvdCAke2VtYmVkZGluZ3MubGVuZ3RofWApO1xuICAgIH1cblxuICAgIHJldHVybiBlbWJlZGRpbmdzO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgLy8gUmV0cnkgb24gcmF0ZSBsaW1pdHMgKDQyOSkgYXMgd2VsbCBhcyB0ZW1wb3JhcnkgZ2F0ZXdheS9zZXJ2aWNlIGRpc3J1cHRpb25zICg1MDIsIDUwMylcbiAgICBjb25zdCBpc1JldHJ5YWJsZSA9IGlzNDI5RXJyb3IoZXJyb3IpIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMiB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAzIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1NlcnZpY2UgVW5hdmFpbGFibGUnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdCYWQgR2F0ZXdheScpO1xuXG4gICAgaWYgKGlzUmV0cnlhYmxlICYmIGF0dGVtcHQgPCBtYXhBdHRlbXB0cykge1xuICAgICAgLy8gU2NhbGUgd2FpdCBkeW5hbWljYWxseSBpZiBpdCdzIGEgc3RydWN0dXJhbCBnYXRld2F5IGVycm9yXG4gICAgICBjb25zdCBiYXNlRGVsYXkgPSBlcnJvci5yZXRyeUFmdGVyIHx8IChhdHRlbXB0ICogUkVUUllfV0FJVF9NUyk7XG4gICAgICBjb25zdCByZXRyeURlbGF5ID0gZXJyb3I/LnN0YXR1cyA9PT0gNDI5ID8gR1JPVVBfV0FJVF9NUyA6IGJhc2VEZWxheTtcblxuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFRyYW5zaWVudCBlcnJvciAoJHtlcnJvcj8uc3RhdHVzIHx8ICd1bmtub3duJ30pLCB3YWl0aW5nICR7cmV0cnlEZWxheSAvIDEwMDB9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9KS4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gRXhwb3J0ZWQgZm9yIGRvY3VtZW50cy5qcyB1cGxvYWQgaGFuZGxlciBcdTIwMTQgZW1iZWRzIG9uZSBiYXRjaCBncm91cCAodXAgdG8gQkFUQ0hfU0laRSB0ZXh0cylcbi8vIGFuZCByZXR1cm5zIHJhdyB2ZWN0b3JzIGFycmF5LiBDYWxsZXIgbWFuYWdlcyBwYXJhbGxlbGlzbSwgd2FpdGluZywgYW5kIENocm9tYSB3cml0ZXMuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnKSB7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0ICR7dGV4dHMubGVuZ3RofSB0ZXh0cywgdGFza1R5cGU9JHt0YXNrVHlwZX1gKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlKTtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgZ290ICR7dmVjdG9ycy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgcmV0dXJuIHZlY3RvcnM7XG59XG5cbi8vIEZ1bGwgcGlwZWxpbmU6IGVtYmVkIGFsbCBjaHVua3Mgd2l0aCBidWlsdC1pbiBiYXRjaGluZyArIHdhaXRpbmcuXG4vLyBVc2VkIGJ5IHNlZWQgaW5nZXN0aW9uIGFuZCBhbnkgY2FsbGVycyB0aGF0IGRvbid0IG5lZWQgc3RyZWFtaW5nIHByb2dyZXNzLlxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSAgICAgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuICBjb25zdCBlbWJlZGRpbmdzICAgID0gW107XG5cbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSk7XG4gIH1cblxuICBjb25zdCB0b3RhbEdyb3VwcyA9IE1hdGguY2VpbChiYXRjaGVzLmxlbmd0aCAvIHBhcmFsbGVsQ2FsbHMpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IHBhcmFsbGVsQmF0Y2hlcyA9IGJhdGNoZXMuc2xpY2UoaSwgaSArIHBhcmFsbGVsQ2FsbHMpO1xuICAgIGNvbnN0IGdyb3VwTnVtICAgICAgICA9IE1hdGguZmxvb3IoaSAvIHBhcmFsbGVsQ2FsbHMpICsgMTtcbiAgICBjb25zdCBjaHVua3NDb3ZlcmVkICAgPSBNYXRoLm1pbigoaSArIHBhcmFsbGVsQ2FsbHMpICogYmF0Y2hTaXplLCBjaHVua3MubGVuZ3RoKTtcblxuICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBHcm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSBcdTIwMTQgJHtwYXJhbGxlbEJhdGNoZXMubGVuZ3RofSBiYXRjaCBjYWxsKHMpIGluIHBhcmFsbGVsIChjaHVua3MgJHtpICogYmF0Y2hTaXplICsgMX1cdTIwMTMke2NodW5rc0NvdmVyZWR9KS4uLmApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHBhcmFsbGVsQmF0Y2hlcy5tYXAoYmF0Y2ggPT4gZW1iZWRCYXRjaChiYXRjaC5tYXAoYyA9PiBjLnRleHQpLCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGJhdGNoID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlO1xuICAgICAgICBiYXRjaC5mb3JFYWNoKChjaHVuaywgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBhYnNvbHV0ZUNodW5rSWR4ID0gKGkgKyBiYXRjaElkeCkgKiBiYXRjaFNpemUgKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHthYnNvbHV0ZUNodW5rSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gQmF0Y2ggJHtpICsgYmF0Y2hJZHh9IGZhaWxlZCwgd2lsbCByZXRyeSBpbmRpdmlkdWFsbHk6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZEJhdGNoZXMucHVzaCh7IGJhdGNoLCBiYXRjaElkeDogaSArIGJhdGNoSWR4IH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKG9uUHJvZ3Jlc3MpIHtcbiAgICAgIG9uUHJvZ3Jlc3MoeyBjdXJyZW50X2JhdGNoOiBncm91cE51bSwgdG90YWxfYmF0Y2hlczogdG90YWxHcm91cHMgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgaXNMYXN0R3JvdXAgPSBpICsgcGFyYWxsZWxDYWxscyA+PSBiYXRjaGVzLmxlbmd0aDtcbiAgICBpZiAoIWlzTGFzdEdyb3VwIHx8IGZhaWxlZEJhdGNoZXMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFdhaXRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIGJlZm9yZSBuZXh0IGdyb3VwLi4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgR1JPVVBfV0FJVF9NUykpO1xuICAgIH1cblxuICAgIGZvciAoY29uc3QgeyBiYXRjaCwgYmF0Y2hJZHggfSBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gV2FpdGluZyAke1JFVFJZX1dBSVRfTVMgLyAxMDAwfXMgYmVmb3JlIHJldHJ5aW5nIGZhaWxlZCBiYXRjaCAke2JhdGNoSWR4fS4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIFJFVFJZX1dBSVRfTVMpKTtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgYmF0Y2gpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtiYXRjaElkeH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW2VtYmVkZGluZ10gXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbdGV4dF0sICdSRVRSSUVWQUxfRE9DVU1FTlQnKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSYXRlTGltaXRTdGF0ZSgpIHtcbiAgcmV0dXJuIHtcbiAgICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgICBwYXJhbGxlbENhbGxzOiBQQVJBTExFTF9DQUxMUygpLFxuICAgIG1heENodW5rc1BlckNhbGw6IEJBVENIX1NJWkUoKSxcbiAgICBvdXRwdXREaW1lbnNpb25zOiBPVVRQVVRfRElNRU5TSU9OUygpXG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0UmF0ZUxpbWl0U3RhdGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGgocmVxLCByZXMpIHtcbiAgY29uc3QgaGVhbHRoU3RhdHVzID0ge1xuICAgIHN0YXR1czogJ29rJyxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICBzZXJ2aWNlczoge31cbiAgfTtcblxuICAvLyBDaGVjayBDaHJvbWFEQlxuICB0cnkge1xuICAgIGNvbnN0IGNocm9tYUhlYWx0aCA9IGF3YWl0IGNocm9tYUhlYWx0aENoZWNrKCk7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0gY2hyb21hSGVhbHRoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IHtcbiAgICAgIHN0YXR1czogJ2Vycm9yJyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlXG4gICAgfTtcbiAgfVxuXG4gIC8vIENoZWNrIEdlbWluaSAodmlhIEFQSSBrZXkgcHJlc2VuY2UpXG4gIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5nZW1pbmkgPSB7XG4gICAgc3RhdHVzOiBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSA/ICdjb25maWd1cmVkJyA6ICdub3RfY29uZmlndXJlZCdcbiAgfTtcblxuICAvLyBHZXQgcmF0ZSBsaW1pdCBzdGF0ZVxuICBoZWFsdGhTdGF0dXMucmF0ZUxpbWl0ID0gZ2V0UmF0ZUxpbWl0U3RhdGUoKTtcblxuICAvLyBPdmVyYWxsIHN0YXR1c1xuICBjb25zdCBoYXNFcnJvcnMgPSBPYmplY3QudmFsdWVzKGhlYWx0aFN0YXR1cy5zZXJ2aWNlcykuc29tZShcbiAgICBzID0+IHMuc3RhdHVzID09PSAnZXJyb3InIHx8IHMuc3RhdHVzID09PSAndW5oZWFsdGh5J1xuICApO1xuXG4gIGlmIChoYXNFcnJvcnMpIHtcbiAgICBoZWFsdGhTdGF0dXMuc3RhdHVzID0gJ2RlZ3JhZGVkJztcbiAgfVxuXG4gIHJlcy5qc29uKGhlYWx0aFN0YXR1cyk7XG59XG5cbnJvdXRlci5nZXQoJy8nLCBoZWFsdGgpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2ltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgVmFsaWRhdGlvbkVycm9yIH0gZnJvbSAnLi9lcnJvcnMuanMnO1xuXG5jb25zdCBEQU5HRVJPVVNfUEFUVEVSTlMgPSAvWzw+OlwifD8qXFx4MDAtXFx4MWZdL2c7XG5jb25zdCBQQVRIX1RSQVZFUlNBTCA9IC9cXC5cXC4vZztcblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplRmlsZW5hbWUoZmlsZW5hbWUpIHtcbiAgaWYgKCFmaWxlbmFtZSB8fCB0eXBlb2YgZmlsZW5hbWUgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZScpO1xuICB9XG5cbiAgLy8gUmVtb3ZlIHBhdGggY29tcG9uZW50cyBhbmQgZ2V0IGJhc2VuYW1lXG4gIGNvbnN0IGJhc2VuYW1lID0gcGF0aC5iYXNlbmFtZShmaWxlbmFtZSk7XG5cbiAgLy8gUmVtb3ZlIGRhbmdlcm91cyBjaGFyYWN0ZXJzXG4gIGxldCBzYW5pdGl6ZWQgPSBiYXNlbmFtZS5yZXBsYWNlKERBTkdFUk9VU19QQVRURVJOUywgJ18nKTtcblxuICAvLyBSZW1vdmUgcGF0aCB0cmF2ZXJzYWwgYXR0ZW1wdHNcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnJlcGxhY2UoUEFUSF9UUkFWRVJTQUwsICcnKTtcblxuICAvLyBUcmltIHdoaXRlc3BhY2UgYW5kIGxpbWl0IGxlbmd0aFxuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQudHJpbSgpLnNsaWNlKDAsIDI1NSk7XG5cbiAgaWYgKCFzYW5pdGl6ZWQpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lIGFmdGVyIHNhbml0aXphdGlvbicpO1xuICB9XG5cbiAgcmV0dXJuIHNhbml0aXplZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlUERGRmlsZShmaWxlKSB7XG4gIGlmICghZmlsZSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ05vIGZpbGUgcHJvdmlkZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIE1JTUUgdHlwZVxuICBjb25zdCB2YWxpZE1pbWVUeXBlcyA9IFsnYXBwbGljYXRpb24vcGRmJ107XG4gIGlmICghdmFsaWRNaW1lVHlwZXMuaW5jbHVkZXMoZmlsZS5taW1ldHlwZSkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdPbmx5IFBERiBmaWxlcyBhcmUgYWNjZXB0ZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIGV4dGVuc2lvblxuICBjb25zdCBleHQgPSBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUgfHwgJycpLnRvTG93ZXJDYXNlKCk7XG4gIGlmIChleHQgIT09ICcucGRmJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ZpbGUgbXVzdCBoYXZlIC5wZGYgZXh0ZW5zaW9uJyk7XG4gIH1cblxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRmlsZVNpemUoc2l6ZUJ5dGVzLCBtYXhTaXplTUIpIHtcbiAgY29uc3QgbWF4Qnl0ZXMgPSBtYXhTaXplTUIgKiAxMDI0ICogMTAyNDtcbiAgaWYgKHNpemVCeXRlcyA+IG1heEJ5dGVzKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmApO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVJbnB1dChpbnB1dCwgbWF4TGVuZ3RoID0gMTAwMDApIHtcbiAgaWYgKCFpbnB1dCB8fCB0eXBlb2YgaW5wdXQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG4gIHJldHVybiBpbnB1dC50cmltKCkuc2xpY2UoMCwgbWF4TGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRG9jdW1lbnRJZChpZCkge1xuICBpZiAoIWlkIHx8IHR5cGVvZiBpZCAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEJyk7XG4gIH1cbiAgY29uc3QgdXVpZFJlZ2V4ID0gL15bMC05YS1mXXs4fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXsxMn0kL2k7XG4gIGlmICghdXVpZFJlZ2V4LnRlc3QoaWQpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCBmb3JtYXQnKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RUZXh0RnJvbVBERkJ1ZmZlcihidWZmZXIpIHtcbiAgLy8gVGhpcyB3aWxsIGJlIHVzZWQgd2l0aCBwZGYtcGFyc2VcbiAgcmV0dXJuIGJ1ZmZlcjtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2ltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuXG5jb25zdCBDSEFSU19QRVJfVE9LRU4gPSA0O1xuY29uc3QgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUyA9IDEwMDA7XG5jb25zdCBERUZBVUxUX09WRVJMQVBfVE9LRU5TID0gMjAwO1xuY29uc3QgTUlOX0NIVU5LX0NIQVJTID0gMTAwO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblRleHQodGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gJyc7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoL1xcZi9nLCAnXFxuJylcbiAgICAucmVwbGFjZSgvKFxccypcXG4pezMsfS9nLCAnXFxuXFxuJylcbiAgICAucmVwbGFjZSgvXlxccypcXGQrXFxzKiQvZ20sICcnKVxuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgY2h1bmtTaXplVG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRva2VucyA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBERUZBVUxUX09WRVJMQVBfVE9LRU5TO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICBjb25zdCBjaHVua1NpemVDaGFycyA9IGNodW5rU2l6ZVRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBjb25zdCBjaHVua3MgPSBbXTtcbiAgbGV0IHN0YXJ0ID0gMDtcbiAgbGV0IGNodW5rSW5kZXggPSAwO1xuXG4gIHdoaWxlIChzdGFydCA8IHRleHQubGVuZ3RoKSB7XG4gICAgbGV0IGVuZCA9IHN0YXJ0ICsgY2h1bmtTaXplQ2hhcnM7XG5cbiAgICBpZiAoZW5kIDwgdGV4dC5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IGJyZWFrUG9pbnRzID0gWycuICcsICcuXFxuJywgJyEgJywgJz8gJywgJ1xcblxcbicsICdcXG4nLCAnICddO1xuICAgICAgY29uc3Qgc2VhcmNoU3RhcnQgPSBlbmQgLSBNYXRoLmZsb29yKGNodW5rU2l6ZUNoYXJzICogMC4yKTtcblxuICAgICAgZm9yIChjb25zdCBicmVha3BvaW50IG9mIGJyZWFrUG9pbnRzKSB7XG4gICAgICAgIGNvbnN0IGlkeCA9IHRleHQubGFzdEluZGV4T2YoYnJlYWtwb2ludCwgZW5kKTtcbiAgICAgICAgaWYgKGlkeCA+IHNlYXJjaFN0YXJ0ICYmIGlkeCA+IHN0YXJ0KSB7XG4gICAgICAgICAgZW5kID0gaWR4ICsgYnJlYWtwb2ludC5sZW5ndGg7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBlbmQgPSBNYXRoLm1pbihlbmQsIHRleHQubGVuZ3RoKTtcbiAgICBjb25zdCBjaHVua0NvbnRlbnQgPSB0ZXh0LnNsaWNlKHN0YXJ0LCBlbmQpLnRyaW0oKTtcblxuICAgIGlmIChjaHVua0NvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiBjaHVua0NvbnRlbnQsXG4gICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKGNodW5rQ29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogc3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGVuZCxcbiAgICAgICAgY2h1bmtJbmRleDogY2h1bmtJbmRleCsrXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBuZXh0U3RhcnQgPSBlbmQgLSBvdmVybGFwQ2hhcnM7XG4gICAgc3RhcnQgPSBuZXh0U3RhcnQgPiBzdGFydCA/IG5leHRTdGFydCA6IGVuZDtcblxuICAgIGlmIChjaHVua0luZGV4ID4gMTAwMDApIHtcbiAgICAgIGNvbnNvbGUud2FybignQ2h1bmsgbGltaXQgcmVhY2hlZCwgc3RvcHBpbmcnKTtcbiAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQoY2xlYW5lZFRleHQsIG9wdGlvbnMpO1xuICBjb25zdCB0b3RhbENodW5rcyA9IHRleHRDaHVua3MubGVuZ3RoO1xuXG4gIC8vIEZJWCA0OiB1c2Ugc291cmNlVHlwZSBmcm9tIG9wdGlvbnMsIGZhbGwgYmFjayB0byAncGRmJ1xuICBjb25zdCBzb3VyY2VUeXBlID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogZmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6IHNvdXJjZVR5cGUsICAgICAgICAgICAgLy8gRklYIDRcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH07XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0U2VjdGlvblRpdGxlKHRleHQpIHtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQge1xuICBnZXRHbG9iYWxDb2xsZWN0aW9uLFxuICBnZXRTZXNzaW9uQ29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50cyxcbiAgYWRkVmVjdG9yc1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGlkID0gc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICBkZWxldGVkRG9jdW1lbnRJZHM6IG5ldyBTZXQoKSwgICAvLyB0cmFjayBkZWxldGVkIGRvYyBJRHMgdG8gZmlsdGVyIHByb21wdCBtZW1vcnlcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcbiAgc2Vzc2lvbnMuc2V0KGlkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHJldHVybiBleGlzdGluZztcbiAgICByZXR1cm4gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIElmIGNvbGxlY3Rpb24gaXMgTkVXIFx1MjE5MiBzZWVkIGZyb20gZ2xvYmFsIChwYWdpbmF0ZWQsIDMwMC9iYXRjaClcbiAqIC0gSWYgY29sbGVjdGlvbiBFWElTVFMgXHUyMTkyIHNraXAgc2VlZCwgcmVjb25zdHJ1Y3QgaW4tbWVtb3J5IGRvYyBsaXN0IGZyb20gQ2hyb21hXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhgXHVEODNEXHVERDExIFNlc3Npb24gaW5pdDogJHtzZXNzaW9uSWR9YCk7XG4gIGlmIChzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWxyZWFkeSBzZWVkZWQgJHtzZXNzaW9uSWR9LCBza2lwcGluZ2ApO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb246IHNlc3Npb25Db2xsZWN0aW9uLCBpc05ldyB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGlmICghaXNOZXcpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI2N0JcdUZFMEYgIFNlc3Npb24gZXhpc3RzLCByZWNvbnN0cnVjdGluZyBkb2N1bWVudCBsaXN0IGZyb20gQ2hyb21hLi4uYCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKHNlc3Npb25Db2xsZWN0aW9uKTtcbiAgICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgICAgc291cmNlVHlwZTogZG9jLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IGRvY3VtZW50KHMpIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFx1RDgzQ1x1REYzMSBOZXcgc2Vzc2lvbiBcdTIwMTQgc2VlZGluZyBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uLi4uYCk7XG5cbiAgICBjb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdLCBhbGxFbWJlZGRpbmdzID0gW10sIGFsbERvY3VtZW50cyA9IFtdLCBhbGxNZXRhZGF0YXMgPSBbXTtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGdsb2JhbENvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydlbWJlZGRpbmdzJywgJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uYmF0Y2guZW1iZWRkaW5ncyk7XG4gICAgICBhbGxEb2N1bWVudHMucHVzaCguLi5iYXRjaC5kb2N1bWVudHMpO1xuICAgICAgYWxsTWV0YWRhdGFzLnB1c2goLi4uYmF0Y2gubWV0YWRhdGFzKTtcbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjZBMFx1RkUwRiAgR2xvYmFsIGNvbGxlY3Rpb24gaXMgZW1wdHkgXHUyMDE0IG5vdGhpbmcgdG8gc2VlZC4nKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYWxsSWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6IGFsbElkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIGVtYmVkZGluZ3M6IGFsbEVtYmVkZGluZ3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLFxuICAgICAgICBkb2N1bWVudHM6IGFsbERvY3VtZW50cy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIG1ldGFkYXRhczogYWxsTWV0YWRhdGFzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAobSA9PiAoeyAuLi5tLCBzb3VyY2VfdHlwZTogJ2dsb2JhbCcgfSkpXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFx1RDgzRFx1RENFNiBBZGRlZCBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IHJlY29yZHMgJHtpICsgMX1cdTIwMTMke01hdGgubWluKGkgKyBCQVRDSF9TSVpFLCBhbGxJZHMubGVuZ3RoKX1gKTtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlZWRlZCAke2FsbElkcy5sZW5ndGh9IHZlY3RvcnMgaW50byBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoc2Vzc2lvbikge1xuICAgICAgY29uc3QgZG9jc01hcCA9IG5ldyBNYXAoKTtcbiAgICAgIGFsbE1ldGFkYXRhcy5mb3JFYWNoKG1ldGEgPT4ge1xuICAgICAgICBpZiAoIWRvY3NNYXAuaGFzKG1ldGEuZG9jdW1lbnRfaWQpKSB7XG4gICAgICAgICAgZG9jc01hcC5zZXQobWV0YS5kb2N1bWVudF9pZCwge1xuICAgICAgICAgICAgaWQ6IG1ldGEuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBtZXRhLnRvdGFsX3BhZ2VzIHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiAwLFxuICAgICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGRvY3NNYXAuZ2V0KG1ldGEuZG9jdW1lbnRfaWQpLmNodW5rQ291bnQrKztcbiAgICAgIH0pO1xuXG4gICAgICBmb3IgKGNvbnN0IGRvYyBvZiBkb2NzTWFwLnZhbHVlcygpKSB7XG4gICAgICAgIGlmICghc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuaWQgPT09IGRvYy5pZCkpIHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKGRvYyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBVcHNlcnQgYSBkb2N1bWVudCBpbnRvIHRoZSBzZXNzaW9uLlxuICogSWYgYSBkb2Mgd2l0aCB0aGUgc2FtZSBpZCBhbHJlYWR5IGV4aXN0cywgdXBkYXRlIGl0IGluIHBsYWNlIChubyBkdXBsaWNhdGUpLlxuICogU3VwcG9ydHMgcGFydGlhbCB1cGRhdGVzIFx1MjAxNCBvbmx5IHByb3ZpZGVkIGZpZWxkcyBvdmVyd3JpdGUgZXhpc3RpbmcgdmFsdWVzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG5cbiAgY29uc3QgZXhpc3RpbmcgPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kKGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJbmZvLmlkKTtcblxuICBpZiAoZXhpc3RpbmcpIHtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmNodW5rQ291bnQgID0gZG9jdW1lbnRJbmZvLmNodW5rQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5wYWdlQ291bnQgICAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5wYWdlQ291bnQgICA9IGRvY3VtZW50SW5mby5wYWdlQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlU2l6ZSAgICAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlU2l6ZSAgICA9IGRvY3VtZW50SW5mby5maWxlU2l6ZTtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnN0YXR1cyAgICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnN0YXR1cyAgICAgID0gZG9jdW1lbnRJbmZvLnN0YXR1cztcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVuYW1lICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVuYW1lICAgID0gZG9jdW1lbnRJbmZvLmZpbGVuYW1lO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFVwZGF0ZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZXhpc3Rpbmcuc3RhdHVzfSwgY2h1bmtzPSR7ZXhpc3RpbmcuY2h1bmtDb3VudH1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgPz8gMCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgIHN0YXR1czogZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnXG4gIH0pO1xuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWRkZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnfWApO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgLy8gVHJhY2sgZGVsZXRlZCBkb2Mgc28gaXRzIG1lbW9yeSB0dXJucyBhcmUgZXhjbHVkZWQgZnJvbSBmdXR1cmUgcHJvbXB0c1xuICAgIHNlc3Npb24uZGVsZXRlZERvY3VtZW50SWRzLmFkZChkb2N1bWVudElkKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBSZW1vdmVkIGRvYyAke2RvY3VtZW50SWR9LCBhZGRlZCB0byBkZWxldGVkRG9jdW1lbnRJZHNgKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIHJldHVybiBzZXNzaW9uPy5kZWxldGVkRG9jdW1lbnRJZHMgPz8gbmV3IFNldCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gW107XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBzZXNzaW9uRG9jdW1lbnRzOiBbXSwgZ2xvYmFsRG9jdW1lbnRzOiBbXSB9O1xuXG4gIGNvbnN0IG5vcm1hbGl6ZSA9IChkb2MpID0+ICh7XG4gICAgZG9jdW1lbnRfaWQ6IGRvYy5pZCxcbiAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgIGNodW5rX2NvdW50OiBkb2MuY2h1bmtDb3VudCA/PyAwLFxuICAgIHBhZ2VfY291bnQ6IGRvYy5wYWdlQ291bnQgPz8gMCxcbiAgICB1cGxvYWRfdGltZXN0YW1wOiBkb2MudXBsb2FkVGltZXN0YW1wIHx8IG51bGwsXG4gICAgc291cmNlX3R5cGU6IGRvYy5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ3Nlc3Npb25fdXBsb2FkJyA6ICdzZWVkJyxcbiAgICBmaWxlU2l6ZTogZG9jLmZpbGVTaXplIHx8IG51bGwsXG4gICAgc3RhdHVzOiBkb2Muc3RhdHVzID8/IG51bGxcbiAgfSk7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uLmRvY3VtZW50c1xuICAgICAgLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJylcbiAgICAgIC5tYXAobm9ybWFsaXplKSxcbiAgICBnbG9iYWxEb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnZ2xvYmFsJylcbiAgICAgIC5tYXAobm9ybWFsaXplKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgNTtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpOyAvLyBkZXN0cnVjdHVyZVxuICAgIGlmIChjb2xsZWN0aW9uKSBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwIH07XG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gTWF0aC5tYXgoMCwgci5zY29yZSkpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBbcXVlcnlFbWJlZGRpbmcsIHNlc3Npb25Db2xsZWN0aW9uXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIGVtYmVkUXVlcnkocXVlcnkpLFxuICAgICAgc2Vzc2lvbklkID8gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkgOiBQcm9taXNlLnJlc29sdmUobnVsbClcbiAgICBdKTtcblxuICAgIGlmICghc2Vzc2lvbkNvbGxlY3Rpb24pIHtcbiAgICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICBObyBzZXNzaW9uIGNvbGxlY3Rpb24gZm91bmQgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgcmV0dXJuIHsgcmVzdWx0czogW10sIGNvdmVyYWdlOiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwLCBsZXZlbDogJ2xvdycsIHNjb3JlOiAwIH0sIHF1ZXJ5RW1iZWRkaW5nIH07XG4gICAgfVxuXG4gICAgY29uc3QgcmF3UmVzdWx0cyA9IGF3YWl0IHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAuY2F0Y2goKCkgPT4gW10pO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBSYXcgc2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IG11bHRlciBmcm9tICdtdWx0ZXInO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcbmltcG9ydCBwZGYgZnJvbSAncGRmLXBhcnNlJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnOztcbmltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUsIHZhbGlkYXRlUERGRmlsZSwgdmFsaWRhdGVGaWxlU2l6ZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbmltcG9ydCB7XG4gIENvcnJ1cHRlZFBERkVycm9yLFxuICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgRmlsZVRvb0xhcmdlRXJyb3IsXG4gIFRvb01hbnlQREZzRXJyb3IsXG4gIER1cGxpY2F0ZUZpbGVFcnJvclxufSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuaW1wb3J0IHsgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbmltcG9ydCB7IGVtYmVkU2luZ2xlQmF0Y2hHcm91cCB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHtcbiAgZ2V0T3JDcmVhdGVTZXNzaW9uLFxuICBjYW5BY2NlcHRVcGxvYWQsXG4gIGFkZERvY3VtZW50VG9TZXNzaW9uLFxuICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uLFxuICBnZXRBbGxEb2N1bWVudHNcbn0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNsZWFyTWVtb3J5IH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG5jb25zdCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL3NlZWRfZG9jdW1lbnRzJyk7XG5cbmNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHVwbG9hZERpciksXG4gIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG59KTtcblxuY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgc3RvcmFnZSxcbiAgbGltaXRzOiB7IGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0IH0sXG4gIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICBjYihudWxsLCB0cnVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgIH1cbiAgfVxufSk7XG5cbmZ1bmN0aW9uIGNvbnRlbnREaXNwb3NpdGlvbihkaXNwbGF5TmFtZSkge1xuICBjb25zdCBlbmNvZGVkID0gZW5jb2RlVVJJQ29tcG9uZW50KGRpc3BsYXlOYW1lKVxuICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgIC5yZXBsYWNlKC9cXCgvZywgJyUyOCcpXG4gICAgLnJlcGxhY2UoL1xcKS9nLCAnJTI5Jyk7XG4gIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xufVxuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IHRjLml0ZW1zLm1hcChpID0+IGkuc3RyKS5qb2luKCcgJyk7XG4gICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgIGNvbnN0IHBhZ2VNYXAgPSBbXTtcbiAgICBsZXQgY2hhclBvcyA9IDA7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICB9XG5cbiAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRQYWdlTnVtYmVyKGNoYXJTdGFydCwgcGFnZU1hcCkge1xuICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICBpZiAoY2hhclN0YXJ0ID49IGVudHJ5LnN0YXJ0ICYmIGNoYXJTdGFydCA8IGVudHJ5LmVuZCkgcmV0dXJuIGVudHJ5LnBhZ2U7XG4gIH1cbiAgcmV0dXJuIHBhZ2VNYXBbcGFnZU1hcC5sZW5ndGggLSAxXT8ucGFnZSB8fCAxO1xufVxuXG5mdW5jdGlvbiBzc2VFdmVudChyZXMsIGV2ZW50LCBkYXRhKSB7XG4gIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gIGNvbnN0IEJBVENIX1NJWkUgICAgID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX0JBVENIX01BWF9DSFVOS1MpIHx8IDc7XG4gIGNvbnN0IFBBUkFMTEVMX0NBTExTID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSAgfHwgNDtcbiAgY29uc3QgR1JPVVBfV0FJVF9NUyAgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfR1JPVVBfV0FJVF9NUykgICB8fCA2MTAwMDtcblxuICB0cnkge1xuICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICBpZiAoIWZpbGUpIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbklkICAgICA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgY29uc3Qgc2Vzc2lvbiAgICAgICA9IGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IG1heFBERnMgICAgICAgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTiB8fCAnMycpO1xuICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgICBpZiAodXBsb2FkZWRDb3VudCA+PSBtYXhQREZzKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogYE1heGltdW0gJHttYXhQREZzfSB1cGxvYWRzIHJlYWNoZWRgLCBjb2RlOiAnVE9PX01BTllfUERGUycgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgXCIke2NsZWFuRmlsZW5hbWV9XCIgYWxyZWFkeSB1cGxvYWRlZGAsIGNvZGU6ICdEVVBMSUNBVEVfRklMRScgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAxIFx1MjAxNCBwYXJzaW5nICR7Y2xlYW5GaWxlbmFtZX0gKCR7ZmlsZS5zaXplfSBieXRlcylgKTtcbiAgICBjb25zdCB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH0gPSBhd2FpdCBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlLnBhdGgpO1xuXG4gICAgaWYgKCFmdWxsVGV4dCB8fCBmdWxsVGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gZXh0cmFjdGFibGUgdGV4dCBcdTIwMTQgUERGIG1heSBiZSBzY2FubmVkIG9yIGltYWdlLW9ubHknLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc3QgZG9jdW1lbnRJZCA9IHV1aWR2NCgpO1xuICAgIGNvbnN0IHJhd0NodW5rcyAgPSBjaHVua1RleHQoZnVsbFRleHQsIHsgY2h1bmtTaXplVG9rZW5zOiAxMDAwLCBvdmVybGFwVG9rZW5zOiAyMDAgfSk7XG5cbiAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiAgICAgICAgIGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiAgICAgICAgIGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogICAgIHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiAgICAgIGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6ICAgICAgdG90YWxQYWdlcyxcbiAgICAgICAgc291cmNlX3R5cGU6ICAgICAgJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiAgICAgICBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiAgICAgICAgIGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiAgICAgIGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9KSk7XG5cbiAgICBjb25zdCB0b3RhbENodW5rcyAgPSBjaHVua3MubGVuZ3RoO1xuICAgIGNvbnN0IHRvdGFsQmF0Y2hlcyA9IE1hdGguY2VpbCh0b3RhbENodW5rcyAvIEJBVENIX1NJWkUpO1xuICAgIGNvbnN0IHRvdGFsU2V0cyAgICA9IE1hdGguY2VpbCh0b3RhbEJhdGNoZXMgLyBQQVJBTExFTF9DQUxMUyk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgc3NlRXZlbnQocmVzLCAndXBsb2FkX2NvbXBsZXRlJywge1xuICAgICAgZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgIH0pO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IDAsIHN0YXR1czogJ2luZGV4aW5nJ1xuICAgIH0pO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgbGV0IHByb2Nlc3NlZENodW5rcyAgPSAwO1xuICAgIGNvbnN0IGFsbEVtYmVkZGluZ3MgID0gW107XG5cbiAgICBjb25zdCBiYXRjaGVzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpKTtcblxuICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IFBBUkFMTEVMX0NBTExTKSBzZXRzLnB1c2goYmF0Y2hlcy5zbGljZShpLCBpICsgUEFSQUxMRUxfQ0FMTFMpKTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAyIHN0YXJ0IFx1MjAxNCAke3NldHMubGVuZ3RofSBzZXRzYCk7XG5cbiAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgIGNvbnN0IGlzTGFzdFNldCAgICA9IHNldElkeCA9PT0gc2V0cy5sZW5ndGggLSAxO1xuICAgICAgY29uc3QgY3VycmVudFNldCAgID0gc2V0c1tzZXRJZHhdO1xuICAgICAgY29uc3Qgc2V0Q2h1bmtDb3VudCA9IGN1cnJlbnRTZXQucmVkdWNlKChhY2MsIGIpID0+IGFjYyArIGIubGVuZ3RoLCAwKTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9LyR7c2V0cy5sZW5ndGh9IFx1MjAxNCBlbWJlZGRpbmcgJHtjdXJyZW50U2V0Lmxlbmd0aH0gYmF0Y2ggY2FsbChzKSAoJHtzZXRDaHVua0NvdW50fSBjaHVua3MpIGluIHBhcmFsbGVsYCk7XG5cbiAgICAgIGNvbnN0IGVtYmVkUmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgICAgY3VycmVudFNldC5tYXAoYmF0Y2ggPT4gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKGJhdGNoLm1hcChjID0+IGMudGV4dCkpKVxuICAgICAgKTtcblxuICAgICAgY29uc3Qgc2V0RW1iZWRkaW5ncyA9IFtdO1xuICAgICAgZW1iZWRSZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgICAgY29uc3QgYmF0Y2ggPSBjdXJyZW50U2V0W2JhdGNoSWR4XTtcbiAgICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgICAgcmVzdWx0LnZhbHVlLmZvckVhY2goKHZlY3RvciwgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICAgIGlkOiAgICAgICAgYmF0Y2hbY2h1bmtJZHhdLm1ldGFkYXRhLmNodW5rX2lkLFxuICAgICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcixcbiAgICAgICAgICAgICAgbWV0YWRhdGE6ICBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEsXG4gICAgICAgICAgICAgIHRleHQ6ICAgICAgYmF0Y2hbY2h1bmtJZHhdLnRleHRcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IGVtYmVkZGVkIE9LICgke2JhdGNoLmxlbmd0aH0gY2h1bmtzKWApO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gRkFJTEVEOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcHJvY2Vzc2VkQ2h1bmtzICs9IHNldEVtYmVkZGluZ3MubGVuZ3RoO1xuICAgICAgYWxsRW1iZWRkaW5ncy5wdXNoKC4uLnNldEVtYmVkZGluZ3MpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0gZW1iZWRkZWQgXHUyMDE0ICR7cHJvY2Vzc2VkQ2h1bmtzfS8ke3RvdGFsQ2h1bmtzfSBjaHVua3Mgc28gZmFyYCk7XG5cbiAgICAgIGlmICghaXNMYXN0U2V0KSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTdGFydGluZyAke0dST1VQX1dBSVRfTVMgLyAxMDAwfXMgdGltZXIgKyBDaHJvbWEgd3JpdGUgY29uY3VycmVudGx5IGZvciBzZXQgJHtzZXRJZHggKyAxfWApO1xuICAgICAgICBjb25zdCB0aW1lciA9IG5ldyBQcm9taXNlKHIgPT4gc2V0VGltZW91dChyLCBHUk9VUF9XQUlUX01TKSk7XG4gICAgICAgIGNvbnN0IGNocm9tYVdyaXRlID0gYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApLnRoZW4oKCkgPT4gY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBkb25lIGZvciBzZXQgJHtzZXRJZHggKyAxfSAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKSlcbiAgICAgICAgLmNhdGNoKGVyciA9PiBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgRkFJTEVEIGZvciBzZXQgJHtzZXRJZHggKyAxfTpgLCBlcnIubWVzc2FnZSkpO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiBHUk9VUF9XQUlUX01TLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiBmYWxzZVxuICAgICAgICB9KTtcblxuICAgICAgICBhd2FpdCBQcm9taXNlLmFsbChbdGltZXIsIGNocm9tYVdyaXRlXSk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBUaW1lciArIENocm9tYSBib3RoIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9LCBwcm9jZWVkaW5nIHRvIHNldCAke3NldElkeCArIDJ9YCk7XG5cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBMYXN0IHNldCAke3NldElkeCArIDF9IFx1MjAxNCBhd2FpdGluZyBDaHJvbWEgd3JpdGUgZGlyZWN0bHlgKTtcbiAgICAgICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApO1xuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGNvbXBsZXRlIGZvciBsYXN0IHNldCAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKTtcblxuICAgICAgICBzc2VFdmVudChyZXMsICdlbWJlZGRpbmdfcHJvZ3Jlc3MnLCB7XG4gICAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzLCB0b3RhbENodW5rcyxcbiAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgIHdhaXRpbmdNczogMCwgY2hyb21hV3JpdGVDb21wbGV0ZTogdHJ1ZVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpO1xuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCwgc3RhdHVzOiAncmVhZHknXG4gICAgfSk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gXHUyNzA1IERvbmUgXHUyMDE0ICR7YWxsRW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMgaW4gQ2hyb21hIGZvciAke2NsZWFuRmlsZW5hbWV9YCk7XG5cbiAgICBzc2VFdmVudChyZXMsICdkb25lJywge1xuICAgICAgZG9jdW1lbnQ6IHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIHNlc3Npb25JZFxuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaWYgKHJlcS5maWxlICYmIGZzLmV4aXN0c1N5bmMocmVxLmZpbGUucGF0aCkpIHtcbiAgICAgIHRyeSB7IGZzLnVubGlua1N5bmMocmVxLmZpbGUucGF0aCk7IH0gY2F0Y2gge31cbiAgICB9XG4gICAgY29uc29sZS5lcnJvcignW3VwbG9hZF0gVW5oYW5kbGVkIGVycm9yOicsIGVycm9yKTtcbiAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnVXBsb2FkIGZhaWxlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1VQTE9BRF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcbiAgdHJ5IHtcbiAgICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBkb2N1bWVudHMgPSBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0xpc3QgZG9jdW1lbnRzIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJywgY29kZTogJ0xJU1RfRVJST1InIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgICBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgICAgIH1cbiAgICAgIH0gY2F0Y2ggKGNocm9tYUVycikge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtkZWxldGVdIENocm9tYSBkZWxldGUgZmFpbGVkIGZvciAke2RvY3VtZW50SWR9OmAsIGNocm9tYUVyci5tZXNzYWdlKTtcbiAgICAgIH1cblxuICAgICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpO1xuXG4gICAgICAvLyBDbGVhciBiYWNrZW5kIG1lbW9yeSBzbyBMTE0gZm9yZ2V0cyBkZWxldGVkIGRvYyBjb250ZXh0XG4gICAgICBjbGVhck1lbW9yeShzZXNzaW9uSWQpO1xuICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIENsZWFyZWQgbWVtb3J5IGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgIH1cblxuICAgIGlmIChmaWxlbmFtZSkge1xuICAgICAgY29uc3QgZmlsZVBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhmaWxlUGF0aCkpIHtcbiAgICAgICAgZnMudW5saW5rU3luYyhmaWxlUGF0aCk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbZGVsZXRlXSBSZW1vdmVkIGZpbGU6ICR7ZmlsZVBhdGh9YCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtkZWxldGVdIEZpbGUgbm90IGZvdW5kIG9uIGRpc2s6ICR7ZmlsZVBhdGh9YCk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgcmVzLmpzb24oeyBzdWNjZXNzOiB0cnVlLCBkb2N1bWVudElkIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQnLCBjb2RlOiAnREVMRVRFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRGaWxlKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuXG4gIHRyeSB7XG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCB1cGxvYWRQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmModXBsb2FkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHVwbG9hZFBhdGgpLnBpcGUocmVzKTtcbiAgICAgIH1cblxuICAgICAgY29uc3Qgc2VlZFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgZmlsZW5hbWUpO1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZFBhdGgpKSB7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihmaWxlbmFtZSkpO1xuICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShzZWVkUGF0aCkucGlwZShyZXMpO1xuICAgICAgfVxuXG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkRGlyKSkge1xuICAgICAgICBjb25zdCBhbGxQZGZzID0gZnMucmVhZGRpclN5bmMoc2VlZERpcikuZmlsdGVyKGYgPT4gZi5lbmRzV2l0aCgnLnBkZicpKTtcbiAgICAgICAgY29uc3QgbWF0Y2ggICA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKG1hdGNoKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG5yb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xucm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xucm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtpbXBvcnQgeyBmb3JtYXRNZW1vcnlGb3JQcm9tcHQgfSBmcm9tICcuL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZm9ybWF0Q29udGV4dEZvclByb21wdCwgY2FsY3VsYXRlQ292ZXJhZ2UgfSBmcm9tICcuL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCBTWVNURU1fSU5TVFJVQ1RJT04gPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgYmFzZWQgb24gaW5kZXhlZCBkb2N1bWVudHMgd2hlbiBhdmFpbGFibGUuXG5cblJVTEVTOlxuMS4gV2hlbiBjb250ZXh0IGlzIHByb3ZpZGVkLCBhbnN3ZXIgYmFzZWQgb24gaXQgYW5kIGNpdGUgc291cmNlcyB1c2luZyBbMV0sIFsyXSwgZXRjLlxuMi4gRm9yIGdlbmVyYWwgY29udmVyc2F0aW9uIChncmVldGluZ3MsIGNsYXJpZnlpbmcgcXVlc3Rpb25zLCBzbWFsbCB0YWxrKSwgcmVzcG9uZCBuYXR1cmFsbHkgYW5kIGhlbHBmdWxseSB3aXRob3V0IHJlcXVpcmluZyBjb250ZXh0LlxuMy4gSWYgYSBmYWN0dWFsIHF1ZXN0aW9uIGlzIGFza2VkIGJ1dCBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkgYW5kIHN1Z2dlc3QgdXBsb2FkaW5nIHJlbGV2YW50IGRvY3VtZW50cy5cbjQuIEJlIGNvbmNpc2UgYnV0IHRob3JvdWdoLiBVc2UgYnVsbGV0IHBvaW50cyBvciBudW1iZXJlZCBsaXN0cyBmb3IgY29tcGxleCBhbnN3ZXJzLlxuNS4gTWFpbnRhaW4gY29udmVyc2F0aW9uIGNvbnRpbnVpdHkgYnV0IGRvbid0IHJlcGVhdCBpbmZvcm1hdGlvbiB1bm5lY2Vzc2FyaWx5LlxuNi4gRm9ybWF0IHJlc3BvbnNlcyBpbiBjbGVhciwgcmVhZGFibGUgbWFya2Rvd24uYDtcblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkUHJvbXB0KHsgcXVlcnksIGNvbnRleHQsIG1lbW9yeUNvbnRleHQsIGNvdmVyYWdlIH0pIHtcbiAgY29uc3QgcGFydHMgPSBbXTtcbiAgcGFydHMucHVzaChTWVNURU1fSU5TVFJVQ1RJT04pO1xuICBpZiAobWVtb3J5Q29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChtZW1vcnlDb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgfVxuICBpZiAoY29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBSRUxFVkFOVCBDT05URVhUIEZST00gS05PV0xFREdFIEJBU0UgLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChjb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIENPTlRFWFQgLS0tXFxuJyk7XG4gIH1cbiAgcGFydHMucHVzaCgnXFxuXFxuLS0tIENVUlJFTlQgUVVFU1RJT04gLS0tXFxuJyk7XG4gIHBhcnRzLnB1c2gocXVlcnkpO1xuICBwYXJ0cy5wdXNoKCdcXG5cXG5SZW1lbWJlcjogQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIFVzZSBbMV0sIFsyXSwgZXRjLiBmb3IgY2l0YXRpb25zLiBJZiB0aGUgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5LicpO1xuICByZXR1cm4gcGFydHMuam9pbignJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFN0cmVhbWluZ1Byb21wdChxdWVyeSwgcmV0cmlldmVkUmVzdWx0cywgc2Vzc2lvbklkLCBtZW1vcnlTZXJ2aWNlKSB7XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKTtcbiAgY29uc3QgY29udGV4dFN0cmluZyA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmV0cmlldmVkUmVzdWx0cyk7XG4gIHJldHVybiBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFN0cmluZyxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiBjYWxjdWxhdGVDb3ZlcmFnZShyZXRyaWV2ZWRSZXN1bHRzKVxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxSZXNwb25zZSgpIHtcbiAgLy8gTm8gbG9uZ2VyIHVzZWQgXHUyMDE0IExMTSBnZW5lcmF0ZXMgaXRzIG93biBuYXR1cmFsIHJlZnVzYWxcbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTeXN0ZW1JbnN0cnVjdGlvbigpIHtcbiAgcmV0dXJuIFNZU1RFTV9JTlNUUlVDVElPTjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkV2ViU2VhcmNoUHJvbXB0KHF1ZXJ5LCBncm91bmRpbmdNZXRhZGF0YSkge1xuICByZXR1cm4gYEJhc2VkIG9uIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBmb2xsb3dpbmcgcXVlc3Rpb246ICR7cXVlcnl9XG5cbkd1aWRlbGluZXM6XG4tIFVzZSBpbmZvcm1hdGlvbiBmcm9tIHRoZSB3ZWIgc2VhcmNoXG4tIFByb3ZpZGUgc291cmNlcy9VUkxzIHdoZXJlIGFwcGxpY2FibGVcbi0gQmUgY29uY2lzZSBhbmQgaW5mb3JtYXRpdmVcbi0gSWYgbXVsdGlwbGUgc291cmNlcyBhZ3JlZSBvciBjb250cmFkaWN0LCBtZW50aW9uIHRoYXRgO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0R2VuZXJhdGlvbkNvbmZpZyhjdXN0b21Db25maWcgPSB7fSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgdG9wUDogMC45NSxcbiAgICB0b3BLOiA0MCxcbiAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDgsXG4gICAgLi4uY3VzdG9tQ29uZmlnXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0U291cmNlc0Zyb21SZXNwb25zZShyZXNwb25zZSkge1xuICBjb25zdCBjaXRhdGlvblBhdHRlcm4gPSAvXFxbKFxcZCspXFxdL2c7XG4gIGNvbnN0IGNpdGF0aW9ucyA9IG5ldyBTZXQoKTtcbiAgbGV0IG1hdGNoO1xuICB3aGlsZSAoKG1hdGNoID0gY2l0YXRpb25QYXR0ZXJuLmV4ZWMocmVzcG9uc2UpKSAhPT0gbnVsbCkge1xuICAgIGNpdGF0aW9ucy5hZGQocGFyc2VJbnQobWF0Y2hbMV0pKTtcbiAgfVxuICByZXR1cm4gQXJyYXkuZnJvbShjaXRhdGlvbnMpLnNvcnQoKGEsIGIpID0+IGEgLSBiKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBidWlsZFByb21wdCwgZ2V0UmVmdXNhbFJlc3BvbnNlIH0gZnJvbSAnLi9wcm9tcHRTZXJ2aWNlLmpzJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdDogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgcHJvY2Vzcy5lbnYuR0NQX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCcsXG4gICAgICBsb2NhdGlvbjogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX0xPQ0FUSU9OIHx8ICd1cy1jZW50cmFsMSdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIFBSSU1BUllfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWxOYW1lKCkge1xuICByZXR1cm4gRkFMTEJBQ0tfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tUmVzcG9uc2UocmVzdWx0KSB7XG4gIHJldHVybiByZXN1bHQ/LnRleHQgfHwgcmVzdWx0Py5yZXNwb25zZT8udGV4dD8uKCkgfHwgJyc7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tQ2h1bmsoY2h1bmspIHtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ3N0cmluZycpIHJldHVybiBjaHVuay50ZXh0O1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnZnVuY3Rpb24nKSByZXR1cm4gY2h1bmsudGV4dCgpO1xuICByZXR1cm4gJyc7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVJlc3BvbnNlKHByb21wdCkge1xuICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuICBjb25zdCB0aW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IGdldEdlbkFJKCkubW9kZWxzLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBtb2RlbDogZ2V0UHJpbWFyeU1vZGVsTmFtZSgpLFxuICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgIGNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH1cbiAgICB9LCB7XG4gICAgICBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsXG4gICAgfSk7XG5cbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICByZXR1cm4gZ2V0VGV4dEZyb21SZXNwb25zZShyZXN1bHQpO1xuICB9IGNhdGNoIChwcmltYXJ5RXJyb3IpIHtcbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICBjb25zb2xlLmVycm9yKCdQcmltYXJ5IG1vZGVsIGZhaWxlZDonLCBwcmltYXJ5RXJyb3IubWVzc2FnZSk7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgZmFsbGJhY2tSZXN1bHQgPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgICBtb2RlbDogZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKSxcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgY29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcmV0dXJuIGdldFRleHRGcm9tUmVzcG9uc2UoZmFsbGJhY2tSZXN1bHQpO1xuICAgIH0gY2F0Y2ggKGZhbGxiYWNrRXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhbGxiYWNrIG1vZGVsIGFsc28gZmFpbGVkOicsIGZhbGxiYWNrRXJyb3IubWVzc2FnZSk7XG4gICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVJlc3BvbnNlKHByb21wdCkge1xuICBsZXQgbW9kZWxOYW1lID0gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpO1xuICBsZXQgcmV0cmllcyA9IDA7XG4gIGNvbnN0IG1heFJldHJpZXMgPSAyO1xuXG4gIHdoaWxlIChyZXRyaWVzIDwgbWF4UmV0cmllcykge1xuICAgIHRyeSB7XG4gICAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgICBtb2RlbDogbW9kZWxOYW1lLFxuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBjb25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0sIHtcbiAgICAgICAgc2lnbmFsOiBjb250cm9sbGVyLnNpZ25hbFxuICAgICAgfSk7XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGNvbnN0IGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignRmlyc3QgdG9rZW4gdGltZW91dCBcdTIwMTQgbm8gcmVzcG9uc2UgZnJvbSBtb2RlbCcpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGdldFRleHRGcm9tQ2h1bmsoY2h1bmspO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgcmV0dXJuIHsgc3VjY2VzczogdHJ1ZSB9O1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICByZXRyaWVzKys7XG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgbW9kZWxOYW1lID0gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1DaGF0UmVzcG9uc2UocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gbWVtb3J5U2VydmljZSA/IG1lbW9yeVNlcnZpY2UuZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkgOiAnJztcbiAgY29uc3QgY29udGV4dExpc3QgPSByZXRyaWV2ZWRSZXN1bHRzIHx8IFtdO1xuICBjb25zdCBjb250ZXh0VGV4dCA9IGNvbnRleHRMaXN0Lm1hcCgociwgaSkgPT5cbiAgICBgWyR7aSArIDF9XSAke3IubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfTogJHtyLnRleHR9YFxuICApLmpvaW4oJ1xcblxcbicpO1xuXG4gIGNvbnN0IHByb21wdCA9IGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0VGV4dCxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiB7IGxldmVsOiAnaGlnaCcgfVxuICB9KTtcblxuICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgdHJ5IHtcbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHsgdHlwZTogJ2NvbXBsZXRlJywgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFRleHQoKSB7XG4gIHJldHVybiBnZXRSZWZ1c2FsUmVzcG9uc2UoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlV2ViU2VhcmNoUmVzcG9uc2UocXVlcnksIGdyb3VuZGluZ0NvbnRlbnQpIHtcbiAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZ2V0R2VuQUkoKS5tb2RlbHMuZ2VuZXJhdGVDb250ZW50KHtcbiAgICBtb2RlbDogZ2V0UHJpbWFyeU1vZGVsTmFtZSgpLFxuICAgIGNvbnRlbnRzOiBbe1xuICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgcGFydHM6IFt7IHRleHQ6IGBCYXNlZCBvbiB0aGVzZSB3ZWIgc2VhcmNoIHJlc3VsdHMsIGFuc3dlciB0aGUgcXVlc3Rpb246IFwiJHtxdWVyeX1cIlxcblxcbiR7Z3JvdW5kaW5nQ29udGVudH1gIH1dXG4gICAgfV0sXG4gICAgY29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OCxcbiAgICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gICAgfVxuICB9KTtcblxuICBjb25zdCB0ZXh0ID0gZ2V0VGV4dEZyb21SZXNwb25zZShyZXN1bHQpO1xuICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3VsdD8uY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICByZXR1cm4ge1xuICAgIHRleHQsXG4gICAgZ3JvdW5kaW5nTWV0YWRhdGEsXG4gICAgZ3JvdW5kaW5nQ2h1bmtzOiBncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzIHx8IFtdXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IHJldHJpZXZlRm9yUXVlcnksIGdlbmVyYXRlQ2l0YXRpb25zLCBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0IH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5pbXBvcnQgeyBzdHJlYW1SZXNwb25zZSB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zIH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGdldERlbGV0ZWREb2N1bWVudElkcyB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmNvbnN0IE9VVF9PRl9TQ09QRV9QQVRURVJOID0gL2Rvbid0IGhhdmUgaW5mb3JtYXRpb258ZG8gbm90IGhhdmUgaW5mb3JtYXRpb258bm90IGluIG15IGtub3dsZWRnZXxjYW4ndCBmaW5kfGNhbm5vdCBmaW5kfG5vIGluZm9ybWF0aW9ufGtub3dsZWRnZSBiYXNlIGRvZXNuJ3R8bm90IGNvdmVyZWR8b3V0c2lkZS4qa25vd2xlZGdlL2k7XG5cbmZ1bmN0aW9uIGNsZWFuRXhjZXJwdCh0ZXh0KSB7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoLyg/PCFcXHcpKFtBLVphLXpdKVxccyhbQS1aYS16XSlcXHMoW0EtWmEtel0pKFxcc1tBLVphLXpdKSovZywgKG1hdGNoKSA9PlxuICAgICAgbWF0Y2gucmVwbGFjZSgvXFxzL2csICcnKVxuICAgIClcbiAgICAucmVwbGFjZSgvXFxzezIsfS9nLCAnICcpXG4gICAgLnJlcGxhY2UoL15cXCpcXHMqLywgJycpXG4gICAgLnRyaW0oKTtcbn1cblxuLy8gSXNzdWUgNCBmaXg6IHJlbW92ZSBkb21haW5IaW50IFx1MjAxNCBzaG9ydCBxdWVyaWVzIG5vIGxvbmdlciBpbmhlcml0IHByZXZpb3VzIGNvbnZlcnNhdGlvbiBjb250ZXh0XG5mdW5jdGlvbiBleHBhbmRRdWVyeShxdWVyeSkge1xuICBjb25zdCB3b3JkcyA9IHF1ZXJ5LnRyaW0oKS5zcGxpdCgvXFxzKy8pO1xuICBpZiAod29yZHMubGVuZ3RoID4gNCkgcmV0dXJuIHF1ZXJ5O1xuXG4gIGNvbnN0IGV4cGFuc2lvbnMgPSBbXG4gICAgJ2RlZmluaXRpb24nLCAnb3ZlcnZpZXcnLCAncm9sZScsICdyZXNwb25zaWJpbGl0aWVzJyxcbiAgICAnZXhhbXBsZXMnLCAna2V5IGNvbmNlcHRzJywgJ2hvdyBpdCB3b3JrcycsICdwdXJwb3NlJ1xuICBdO1xuXG4gIHJldHVybiBgJHtxdWVyeX0gJHtleHBhbnNpb25zLmpvaW4oJyAnKX1gO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlQ2hhdFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5LCBzZXNzaW9uSWQ6IHByb3ZpZGVkU2Vzc2lvbklkLCBjb252SWQ6IHByb3ZpZGVkQ29udklkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLCBjb2RlOiAnTUlTU0lOR19RVUVSWScgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgY29udklkICAgID0gcHJvdmlkZWRDb252SWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkICA9IHV1aWR2NCgpO1xuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3JldHJpZXZpbmcnLCBtZXNzYWdlOiAnU2VhcmNoaW5nIGtub3dsZWRnZSBiYXNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGV4cGFuZGVkUXVlcnkgPSBleHBhbmRRdWVyeShxdWVyeSk7XG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShleHBhbmRlZFF1ZXJ5LCBzZXNzaW9uSWQsIHsgdG9wSzogNSB9KTtcblxuICAgIHNlbmRFdmVudCgncmV0cmlldmFsJywge1xuICAgICAgcmVzdWx0czogcmVzdWx0cy5sZW5ndGgsXG4gICAgICBsZXZlbDogY292ZXJhZ2UubGV2ZWwsXG4gICAgICBzY29yZTogY292ZXJhZ2Uuc2NvcmUsXG4gICAgICB0b3BTY29yZTogY292ZXJhZ2UudG9wU2NvcmVcbiAgICB9KTtcblxuICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuICAgIGNvbnN0IHNvdXJjZXMgPSByZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICBjaHVua0lkOiByLmlkLFxuICAgICAgZG9jdW1lbnRJZDogci5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICAgIGZpbGVuYW1lOiByLm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgICAgcGFnZU51bWJlcjogci5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICAgIGV4Y2VycHQ6IGNsZWFuRXhjZXJwdChyLnRleHQuc2xpY2UoMCwgMjAwKSksXG4gICAgICBzY29yZTogci5zY29yZSxcbiAgICAgIHNvdXJjZVR5cGU6IHIuc291cmNlX3R5cGVcbiAgICB9KSk7XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdnZW5lcmF0aW5nJywgbWVzc2FnZTogJ0dlbmVyYXRpbmcgcmVzcG9uc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgY29udGV4dFRleHQgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMpO1xuXG4gICAgLy8gR2V0IGRlbGV0ZWQgZG9jIElEcyBmb3IgdGhpcyBzZXNzaW9uIHRvIGZpbHRlciBzdGFsZSBtZW1vcnkgdHVybnNcbiAgICBjb25zdCBkZWxldGVkRG9jSWRzID0gZ2V0RGVsZXRlZERvY3VtZW50SWRzKHNlc3Npb25JZCk7XG5cbiAgICBjb25zdCBhbGxSZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKGNvbnZJZCwgMTApO1xuXG4gICAgLy8gRmlsdGVyIG91dCBhc3Npc3RhbnQgdHVybnMgKGFuZCB0aGVpciBwcmVjZWRpbmcgdXNlciB0dXJucykgdGhhdCBjaXRlZCBkZWxldGVkIGRvY3NcbiAgICBjb25zdCBmaWx0ZXJlZFR1cm5zID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhbGxSZWNlbnRUdXJucy5sZW5ndGg7IGkrKykge1xuICAgICAgY29uc3QgdHVybiA9IGFsbFJlY2VudFR1cm5zW2ldO1xuICAgICAgaWYgKHR1cm4ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHtcbiAgICAgICAgY29uc3QgY2l0ZXNEZWxldGVkRG9jID0gdHVybi5jaXRhdGlvbnM/LnNvbWUoYyA9PiBkZWxldGVkRG9jSWRzLmhhcyhjLmRvY3VtZW50SWQpKTtcbiAgICAgICAgaWYgKGNpdGVzRGVsZXRlZERvYykge1xuICAgICAgICAgIC8vIEFsc28gcmVtb3ZlIHRoZSBwcmVjZWRpbmcgdXNlciB0dXJuIGlmIGl0J3MgdGhlIG9uZSB0aGF0IHByb21wdGVkIHRoaXMgYW5zd2VyXG4gICAgICAgICAgaWYgKGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMCAmJiBmaWx0ZXJlZFR1cm5zW2ZpbHRlcmVkVHVybnMubGVuZ3RoIC0gMV0ucm9sZSA9PT0gJ3VzZXInKSB7XG4gICAgICAgICAgICBmaWx0ZXJlZFR1cm5zLnBvcCgpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjb250aW51ZTsgLy8gc2tpcCB0aGlzIGFzc2lzdGFudCB0dXJuXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGZpbHRlcmVkVHVybnMucHVzaCh0dXJuKTtcbiAgICB9XG5cbiAgICBjb25zdCBxdWVzdGlvbnMgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ3VzZXInKTtcbiAgICBjb25zdCBhbnN3ZXJzICAgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcpO1xuICAgIGNvbnN0IHFTZWN0aW9uICA9IHF1ZXN0aW9ucy5tYXAoKHQsIGkpID0+IGBRJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IGFTZWN0aW9uICA9IGFuc3dlcnMubWFwKCh0LCBpKSA9PiBgQSR7aSArIDF9OiAke3QuY29udGVudH1gKS5qb2luKCdcXG4nKTtcbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwXG4gICAgICA/IGBQcmV2aW91cyBRdWVzdGlvbnM6XFxuJHtxU2VjdGlvbn1cXG5cXG5QcmV2aW91cyBBbnN3ZXJzOlxcbiR7YVNlY3Rpb259YFxuICAgICAgOiAnJztcblxuICAgIGNvbnN0IHByb21wdCA9IGBZb3UgYXJlIGFuIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQuIFlvdXIgYmVoYXZpb3VyIGRlcGVuZHMgb24gdGhlIHR5cGUgb2YgaW5wdXQ6XG5cbjEuIEdSRUVUSU5HUyAmIFNNQUxMIFRBTEsgKGhpLCBoZWxsbywgaG93IGFyZSB5b3UsIGRvIHlvdSBoYXZlIGEgbGlmZSwgam9rZXMsIGdlbmVyYWwgY2hhdCk6XG4gICAtIFJlc3BvbmQgd2FybWx5IGFuZCBuYXR1cmFsbHkuIERvIE5PVCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSBvciBkb2N1bWVudHMgYXQgYWxsLlxuICAgLSBEbyBOT1QgYWRkIGFueSBjaXRhdGlvbnMuXG5cbjIuIEZBQ1RVQUwgUVVFU1RJT05TIFdJVEggQ09OVEVYVCAoY29udGV4dCBiZWxvdyBpcyByZWxldmFudCk6XG4gICAtIEFuc3dlciBzdHJpY3RseSB1c2luZyB0aGUgbnVtYmVyZWQgY29udGV4dCBwcm92aWRlZC5cbiAgIC0gQ2l0ZSBzb3VyY2VzIGlubGluZSBhcyBbMV0gWzJdIFx1MjAxNCBhbHdheXMgc2VwYXJhdGUgYnJhY2tldHMsIG5ldmVyIFsxLCAyXS5cbiAgIC0gT25seSBjaXRlIG51bWJlcnMgeW91IGFjdHVhbGx5IHVzZWQuXG5cbjMuIEZBQ1RVQUwgUVVFU1RJT05TIFdJVEhPVVQgQ09OVEVYVCAoY29udGV4dCBpcyBlbXB0eSBvciBpcnJlbGV2YW50KTpcbiAgIC0gUG9saXRlbHkgZGVjbGluZSBpbiB5b3VyIG93biB3b3JkcyBcdTIwMTQgdmFyeSB5b3VyIHBocmFzaW5nIG5hdHVyYWxseS5cbiAgIC0gRG8gTk9UIGFkZCBjaXRhdGlvbnMuXG4gICAtIERvIE5PVCB1c2UgYSBmaXhlZCB0ZW1wbGF0ZSBvciByb2JvdGljIHJlc3BvbnNlLlxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dCB8fCAnKE5vIHJlbGV2YW50IGRvY3VtZW50cyBmb3VuZCBpbiBrbm93bGVkZ2UgYmFzZSknfVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dCB8fCAnKE5vIHByZXZpb3VzIGNvbnZlcnNhdGlvbiknfVxuXG5DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fWA7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnTExNX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbXTtcbiAgICBjb25zdCBzZWVuID0gbmV3IFNldCgpO1xuICAgIGZvciAoY29uc3QgbWF0Y2ggb2YgZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKylcXF0vZykpIHtcbiAgICAgIGNvbnN0IG51bSA9IHBhcnNlSW50KG1hdGNoWzFdKTtcbiAgICAgIGlmICghc2Vlbi5oYXMobnVtKSkge1xuICAgICAgICBzZWVuLmFkZChudW0pO1xuICAgICAgICBjaXRlZEluZGljZXMucHVzaChudW0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGlzT3V0T2ZTY29wZSA9IE9VVF9PRl9TQ09QRV9QQVRURVJOLnRlc3QoZnVsbFJlc3BvbnNlKTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaXRhdGlvbnMgPSBjaXRhdGlvbnMuZmlsdGVyKGMgPT4gY2l0ZWRJbmRpY2VzLmluY2x1ZGVzKGMuaW5kZXgpKTtcblxuICAgIGNvbnN0IGluZGV4TWFwID0gbmV3IE1hcCgpO1xuICAgIGNpdGVkSW5kaWNlcy5mb3JFYWNoKChvbGRJZHgsIGkpID0+IHtcbiAgICAgIGluZGV4TWFwLnNldChvbGRJZHgsIGkgKyAxKTtcbiAgICB9KTtcblxuICAgIGNvbnN0IHJld3JpdHRlblJlc3BvbnNlID0gZnVsbFJlc3BvbnNlLnJlcGxhY2UoL1xcWyhcXGQrKVxcXS9nLCAobWF0Y2gsIG51bSkgPT4ge1xuICAgICAgY29uc3QgbmV3SWR4ID0gaW5kZXhNYXAuZ2V0KHBhcnNlSW50KG51bSkpO1xuICAgICAgcmV0dXJuIG5ld0lkeCAhPT0gdW5kZWZpbmVkID8gYFske25ld0lkeH1dYCA6IG1hdGNoO1xuICAgIH0pO1xuXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBtYXRjaGVkQ2l0YXRpb25zXG4gICAgICAgICAgLm1hcChjID0+ICh7IC4uLmMsIGluZGV4OiBpbmRleE1hcC5nZXQoYy5pbmRleCkgfSkpXG4gICAgICAgICAgLmZpbHRlcihjID0+IGMuaW5kZXggIT09IHVuZGVmaW5lZClcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4gYS5pbmRleCAtIGIuaW5kZXgpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENodW5rSWRzID0gbmV3IFNldChtYXRjaGVkQ2l0YXRpb25zLm1hcChjID0+IGMuY2h1bmtJZCkpO1xuXG4gICAgY29uc3QgZmluYWxTb3VyY2VzID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlc1xuICAgICAgICAgIC5maWx0ZXIocyA9PiBtYXRjaGVkQ2h1bmtJZHMuaGFzKHMuY2h1bmtJZCkpXG4gICAgICAgICAgLnNvcnQoKGEsIGIpID0+IHtcbiAgICAgICAgICAgIGNvbnN0IGlkeEEgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBhLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIGNvbnN0IGlkeEIgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBiLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIHJldHVybiBpZHhBIC0gaWR4QjtcbiAgICAgICAgICB9KTtcblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ2Fzc2lzdGFudCcsIHJld3JpdHRlblJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZSxcbiAgICAgIGNpdGF0aW9uczogZmluYWxDaXRhdGlvbnMsXG4gICAgICBjb3ZlcmFnZSxcbiAgICAgIHNvdXJjZXM6IGZpbmFsU291cmNlc1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnQ0hBVF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAyMCk7XG5cbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIGNvbnN0IGZhbGxiYWNrID0gWy4uLnJlY2VudFR1cm5zXS5yZXZlcnNlKCkuZmluZCh0ID0+XG4gICAgdC5yb2xlID09PSAnYXNzaXN0YW50JyAmJiB0LmNpdGF0aW9ucz8ubGVuZ3RoID4gMFxuICApO1xuXG4gIGlmIChmYWxsYmFjaykgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZmFsbGJhY2suY2l0YXRpb25zIH0pO1xuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdTb3VyY2VzIG5vdCBmb3VuZCcsIGNvZGU6ICdTT1VSQ0VTX05PVF9GT1VORCcgfSk7XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlQ2hhdFN0cmVhbSk7XG5yb3V0ZXIuZ2V0KCcvc291cmNlcy86YW5zd2VySWQnLCBnZXRTb3VyY2VzKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmNvbnN0IGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSk7XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcblxubGV0IG1vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0TW9kZWwoKSB7XG4gIGlmICghbW9kZWwpIHtcbiAgICBtb2RlbCA9IGdlbkFJLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBQUklNQVJZX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBtb2RlbDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHBlcmZvcm1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGNvbnN0IHJlc3BvbnNlID0gcmVzdWx0LnJlc3BvbnNlO1xuICAgIGNvbnN0IHRleHQgPSByZXNwb25zZS50ZXh0KCk7XG4gICAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZS5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgLy8gRXh0cmFjdCBzZWFyY2ggcXVlcmllcyBhbmQgc291cmNlc1xuICAgIGNvbnN0IHdlYlNlYXJjaFF1ZXJpZXMgPSBbXTtcbiAgICBjb25zdCB3ZWJTb3VyY2VzID0gW107XG5cbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBncm91bmRpbmdNZXRhZGF0YS5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgICAgaWYgKGNodW5rLndlYikge1xuICAgICAgICAgIHdlYlNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGNodW5rLndlYi51cmksXG4gICAgICAgICAgICB0aXRsZTogY2h1bmsud2ViLnRpdGxlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/LndlYlNlYXJjaFF1ZXJpZXMpIHtcbiAgICAgIHdlYlNlYXJjaFF1ZXJpZXMucHVzaCguLi5ncm91bmRpbmdNZXRhZGF0YS53ZWJTZWFyY2hRdWVyaWVzKTtcbiAgICB9XG5cbiAgICByZXR1cm4ge1xuICAgICAgdGV4dCxcbiAgICAgIHNvdXJjZXM6IHdlYlNvdXJjZXMsXG4gICAgICBxdWVyaWVzOiB3ZWJTZWFyY2hRdWVyaWVzLFxuICAgICAgcmF3TWV0YWRhdGE6IGdyb3VuZGluZ01ldGFkYXRhXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtV2ViU2VhcmNoKHF1ZXJ5KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRNb2RlbCgpO1xuXG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50U3RyZWFtKHtcbiAgICAgIGNvbnRlbnRzOiBbe1xuICAgICAgICByb2xlOiAndXNlcicsXG4gICAgICAgIHBhcnRzOiBbeyB0ZXh0OiBxdWVyeSB9XVxuICAgICAgfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfSxcbiAgICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gICAgfSk7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgIGNvbnN0IHRleHQgPSBjaHVuay50ZXh0KCk7XG4gICAgICBpZiAodGV4dCkge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gdGV4dDtcbiAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZT8uY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICAgIGNvbnN0IHNvdXJjZXMgPSBbXTtcbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgZm9yIChjb25zdCBpdGVtIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoaXRlbS53ZWIpIHtcbiAgICAgICAgICBzb3VyY2VzLnB1c2goe1xuICAgICAgICAgICAgdXJpOiBpdGVtLndlYi51cmksXG4gICAgICAgICAgICB0aXRsZTogaXRlbS53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHtcbiAgICAgIHR5cGU6ICdjb21wbGV0ZScsXG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgc291cmNlc1xuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBzdHJlYW1pbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICB0aHJvdyBuZXcgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRXZWJTZWFyY2hSZXNwb25zZShyZXN1bHQpIHtcbiAgcmV0dXJuIHtcbiAgICBhbnN3ZXI6IHJlc3VsdC50ZXh0LFxuICAgIHNvdXJjZXM6IHJlc3VsdC5zb3VyY2VzLm1hcChzID0+ICh7XG4gICAgICB1cmk6IHMudXJpLFxuICAgICAgdGl0bGU6IHMudGl0bGUsXG4gICAgICB0eXBlOiAnd2ViJ1xuICAgIH0pKSxcbiAgICBxdWVyaWVzVXNlZDogcmVzdWx0LnF1ZXJpZXMsXG4gICAgbWV0YWRhdGE6IHtcbiAgICAgIHBlcmZvcm1lZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgfVxuICB9O1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9zZWFyY2guanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgcGVyZm9ybVdlYlNlYXJjaCwgc3RyZWFtV2ViU2VhcmNoIH0gZnJvbSAnLi4vc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHBlcmZvcm1XZWJTZWFyY2gocXVlcnkudHJpbSgpKTtcblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBhbnN3ZXI6IHJlc3VsdC50ZXh0LFxuICAgICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMsXG4gICAgICBxdWVyaWVzOiByZXN1bHQucXVlcmllcyxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIHBlcmZvcm1lZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICAgIH1cbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKGVycm9yLnN0YXR1c0NvZGUgfHwgNTAzKS5qc29uKHtcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdXZWIgc2VhcmNoIHVuYXZhaWxhYmxlJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1dFQl9TRUFSQ0hfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVdlYlNlYXJjaFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5IH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX1FVRVJZJ1xuICAgIH0pO1xuICB9XG5cbiAgLy8gU2V0IHVwIFNTRVxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3NlYXJjaGluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcgdGhlIHdlYi4uLicgfSk7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG4gICAgbGV0IHNvdXJjZXMgPSBbXTtcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtV2ViU2VhcmNoKHF1ZXJ5LnRyaW0oKSkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnV0VCX1NFQVJDSF9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICAgIHNvdXJjZXMgPSBjaHVuay5zb3VyY2VzIHx8IFtdO1xuICAgICAgfVxuICAgIH1cblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgc291cmNlcyxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHtcbiAgICAgIG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggZmFpbGVkJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1dFQl9TRUFSQ0hfRVJST1InXG4gICAgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlV2ViU2VhcmNoKTtcbnJvdXRlci5wb3N0KCcvc3RyZWFtJywgaGFuZGxlV2ViU2VhcmNoU3RyZWFtKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlclwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2ltcG9ydCBleHByZXNzIGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IGNvcnMgZnJvbSAnY29ycyc7XG5pbXBvcnQgZG90ZW52IGZyb20gJ2RvdGVudic7XG5pbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuXG5kb3RlbnYuY29uZmlnKCk7XG5cbmltcG9ydCBoZWFsdGhSb3V0ZXIgZnJvbSAnLi9hcGkvaGVhbHRoLmpzJztcbmltcG9ydCBkb2N1bWVudHNSb3V0ZXIgZnJvbSAnLi9hcGkvZG9jdW1lbnRzLmpzJztcbmltcG9ydCBjaGF0Um91dGVyIGZyb20gJy4vYXBpL2NoYXQuanMnO1xuaW1wb3J0IGZlZWRiYWNrUm91dGVyIGZyb20gJy4vYXBpL2ZlZWRiYWNrLmpzJztcbmltcG9ydCBzZWFyY2hSb3V0ZXIgZnJvbSAnLi9hcGkvc2VhcmNoLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyB9IGZyb20gJy4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGNsZWFyTWVtb3J5IH0gZnJvbSAnLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcblxuY29uc3QgYXBwID0gZXhwcmVzcygpO1xuXG4vLyBQcm9ncmVzcyBjYWxsYmFja3NcbmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbi8vIE1pZGRsZXdhcmVcbmFwcC51c2UoY29ycyh7XG4gIG9yaWdpbjogW1xuICAgICdodHRwOi8vbG9jYWxob3N0OjUxNzMnLFxuICAgICdodHRwOi8vbG9jYWxob3N0OjMwMDAnLFxuICAgICdodHRwOi8vMTI3LjAuMC4xOjUxNzMnXG4gIF0sXG4gIGNyZWRlbnRpYWxzOiB0cnVlXG59KSk7XG5cbmFwcC51c2UoZXhwcmVzcy5qc29uKHsgbGltaXQ6ICcxMG1iJyB9KSk7XG5hcHAudXNlKGV4cHJlc3MudXJsZW5jb2RlZCh7IGV4dGVuZGVkOiB0cnVlLCBsaW1pdDogJzEwbWInIH0pKTtcblxuLy8gUmVxdWVzdCBMb2dnZXJcbmFwcC51c2UoKHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUubG9nKGAke3JlcS5tZXRob2R9ICR7cmVxLm9yaWdpbmFsVXJsfWApO1xuICBuZXh0KCk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gVEVTVCBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLmdldCgnL3BpbmcnLCAocmVxLCByZXMpID0+IHtcbiAgY29uc29sZS5sb2coJ1x1MjcwNSBQSU5HIFJPVVRFIEVYRUNVVEVEJyk7XG4gIHJlcy5qc29uKHtcbiAgICBzdWNjZXNzOiB0cnVlLFxuICAgIG1lc3NhZ2U6ICdFeHByZXNzIGJhY2tlbmQgaXMgYWxpdmUnXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gSU5JVCBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL2luaXQnLCBhc3luYyAocmVxLCByZXMpID0+IHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddO1xuXG4gIGlmICghc2Vzc2lvbklkKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdNaXNzaW5nIHgtc2Vzc2lvbi1pZCBoZWFkZXInLCBjb2RlOiAnTUlTU0lOR19TRVNTSU9OJyB9KTtcbiAgfVxuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHRyeSB7XG4gICAgYXdhaXQgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IHRydWUsIHNlc3Npb25JZCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdTZXNzaW9uIGluaXQgd2FybmluZzonLCBlcnIubWVzc2FnZSk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogZmFsc2UsIHNlc3Npb25JZCwgd2FybmluZzogZXJyLm1lc3NhZ2UgfSk7XG4gIH1cbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIFJFU1RPUkUgTUVNT1JZIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vcmVzdG9yZS1tZW1vcnknLCAocmVxLCByZXMpID0+IHtcbiAgY29uc3QgeyBjb252SWQsIG1lc3NhZ2VzIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWNvbnZJZCB8fCAhQXJyYXkuaXNBcnJheShtZXNzYWdlcykpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ2NvbnZJZCBhbmQgbWVzc2FnZXMgYXJlIHJlcXVpcmVkJywgY29kZTogJ0JBRF9SRVFVRVNUJyB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgLy8gQWx3YXlzIHdpcGUgdGhlIGNvbnZJZCBtZW1vcnkgZmlyc3Qgc28gcmVwbGF5aW5nIG5ldmVyIGRvdWJsZXMgdXAgdHVybnNcbiAgICBjbGVhck1lbW9yeShjb252SWQpO1xuXG4gICAgZm9yIChjb25zdCBtc2cgb2YgbWVzc2FnZXMpIHtcbiAgICAgIGlmICgobXNnLnJvbGUgPT09ICd1c2VyJyB8fCBtc2cucm9sZSA9PT0gJ2Fzc2lzdGFudCcpICYmIHR5cGVvZiBtc2cuY29udGVudCA9PT0gJ3N0cmluZycpIHtcbiAgICAgICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCBtc2cucm9sZSwgbXNnLmNvbnRlbnQpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXMuanNvbih7IG9rOiB0cnVlLCBjb252SWQsIHJlc3RvcmVkOiBtZXNzYWdlcy5sZW5ndGggfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignTWVtb3J5IHJlc3RvcmUgd2FybmluZzonLCBlcnIubWVzc2FnZSk7XG4gICAgcmVzLmpzb24oeyBvazogZmFsc2UsIGNvbnZJZCwgd2FybmluZzogZXJyLm1lc3NhZ2UgfSk7XG4gIH1cbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBST1VURVJTXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zb2xlLmxvZygnTW91bnRpbmcgcm91dGVycy4uLicpO1xuXG5hcHAudXNlKCcvaGVhbHRoJywgaGVhbHRoUm91dGVyKTtcbmFwcC51c2UoJy9kb2N1bWVudHMnLCBkb2N1bWVudHNSb3V0ZXIpO1xuYXBwLnVzZSgnL2NoYXQnLCBjaGF0Um91dGVyKTtcbmFwcC51c2UoJy9mZWVkYmFjaycsIGZlZWRiYWNrUm91dGVyKTtcbmFwcC51c2UoJy9zZWFyY2gnLCBzZWFyY2hSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgZXJyb3I6IGVyci5tZXNzYWdlLFxuICAgIHN0YWNrOiBlcnIuc3RhY2tcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNDA0XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChyZXEsIHJlcykgPT4ge1xuICByZXMuc3RhdHVzKDQwNCkuanNvbih7XG4gICAgZXJyb3I6ICdFbmRwb2ludCBub3QgZm91bmQnLFxuICAgIGNvZGU6ICdOT1RfRk9VTkQnXG4gIH0pO1xufSk7XG5cbmV4cG9ydCBkZWZhdWx0IGFwcDtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7dmFyIF9fYXdhaXRlciA9ICh0aGlzICYmIHRoaXMuX19hd2FpdGVyKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgX2FyZ3VtZW50cywgUCwgZ2VuZXJhdG9yKSB7XG4gICAgZnVuY3Rpb24gYWRvcHQodmFsdWUpIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgUCA/IHZhbHVlIDogbmV3IFAoZnVuY3Rpb24gKHJlc29sdmUpIHsgcmVzb2x2ZSh2YWx1ZSk7IH0pOyB9XG4gICAgcmV0dXJuIG5ldyAoUCB8fCAoUCA9IFByb21pc2UpKShmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICAgIGZ1bmN0aW9uIGZ1bGZpbGxlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvci5uZXh0KHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gcmVqZWN0ZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3JbXCJ0aHJvd1wiXSh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHN0ZXAocmVzdWx0KSB7IHJlc3VsdC5kb25lID8gcmVzb2x2ZShyZXN1bHQudmFsdWUpIDogYWRvcHQocmVzdWx0LnZhbHVlKS50aGVuKGZ1bGZpbGxlZCwgcmVqZWN0ZWQpOyB9XG4gICAgICAgIHN0ZXAoKGdlbmVyYXRvciA9IGdlbmVyYXRvci5hcHBseSh0aGlzQXJnLCBfYXJndW1lbnRzIHx8IFtdKSkubmV4dCgpKTtcbiAgICB9KTtcbn07XG52YXIgX19nZW5lcmF0b3IgPSAodGhpcyAmJiB0aGlzLl9fZ2VuZXJhdG9yKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgYm9keSkge1xuICAgIHZhciBfID0geyBsYWJlbDogMCwgc2VudDogZnVuY3Rpb24oKSB7IGlmICh0WzBdICYgMSkgdGhyb3cgdFsxXTsgcmV0dXJuIHRbMV07IH0sIHRyeXM6IFtdLCBvcHM6IFtdIH0sIGYsIHksIHQsIGcgPSBPYmplY3QuY3JlYXRlKCh0eXBlb2YgSXRlcmF0b3IgPT09IFwiZnVuY3Rpb25cIiA/IEl0ZXJhdG9yIDogT2JqZWN0KS5wcm90b3R5cGUpO1xuICAgIHJldHVybiBnLm5leHQgPSB2ZXJiKDApLCBnW1widGhyb3dcIl0gPSB2ZXJiKDEpLCBnW1wicmV0dXJuXCJdID0gdmVyYigyKSwgdHlwZW9mIFN5bWJvbCA9PT0gXCJmdW5jdGlvblwiICYmIChnW1N5bWJvbC5pdGVyYXRvcl0gPSBmdW5jdGlvbigpIHsgcmV0dXJuIHRoaXM7IH0pLCBnO1xuICAgIGZ1bmN0aW9uIHZlcmIobikgeyByZXR1cm4gZnVuY3Rpb24gKHYpIHsgcmV0dXJuIHN0ZXAoW24sIHZdKTsgfTsgfVxuICAgIGZ1bmN0aW9uIHN0ZXAob3ApIHtcbiAgICAgICAgaWYgKGYpIHRocm93IG5ldyBUeXBlRXJyb3IoXCJHZW5lcmF0b3IgaXMgYWxyZWFkeSBleGVjdXRpbmcuXCIpO1xuICAgICAgICB3aGlsZSAoZyAmJiAoZyA9IDAsIG9wWzBdICYmIChfID0gMCkpLCBfKSB0cnkge1xuICAgICAgICAgICAgaWYgKGYgPSAxLCB5ICYmICh0ID0gb3BbMF0gJiAyID8geVtcInJldHVyblwiXSA6IG9wWzBdID8geVtcInRocm93XCJdIHx8ICgodCA9IHlbXCJyZXR1cm5cIl0pICYmIHQuY2FsbCh5KSwgMCkgOiB5Lm5leHQpICYmICEodCA9IHQuY2FsbCh5LCBvcFsxXSkpLmRvbmUpIHJldHVybiB0O1xuICAgICAgICAgICAgaWYgKHkgPSAwLCB0KSBvcCA9IFtvcFswXSAmIDIsIHQudmFsdWVdO1xuICAgICAgICAgICAgc3dpdGNoIChvcFswXSkge1xuICAgICAgICAgICAgICAgIGNhc2UgMDogY2FzZSAxOiB0ID0gb3A7IGJyZWFrO1xuICAgICAgICAgICAgICAgIGNhc2UgNDogXy5sYWJlbCsrOyByZXR1cm4geyB2YWx1ZTogb3BbMV0sIGRvbmU6IGZhbHNlIH07XG4gICAgICAgICAgICAgICAgY2FzZSA1OiBfLmxhYmVsKys7IHkgPSBvcFsxXTsgb3AgPSBbMF07IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGNhc2UgNzogb3AgPSBfLm9wcy5wb3AoKTsgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICAgICAgICBpZiAoISh0ID0gXy50cnlzLCB0ID0gdC5sZW5ndGggPiAwICYmIHRbdC5sZW5ndGggLSAxXSkgJiYgKG9wWzBdID09PSA2IHx8IG9wWzBdID09PSAyKSkgeyBfID0gMDsgY29udGludWU7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSAzICYmICghdCB8fCAob3BbMV0gPiB0WzBdICYmIG9wWzFdIDwgdFszXSkpKSB7IF8ubGFiZWwgPSBvcFsxXTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSA2ICYmIF8ubGFiZWwgPCB0WzFdKSB7IF8ubGFiZWwgPSB0WzFdOyB0ID0gb3A7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0ICYmIF8ubGFiZWwgPCB0WzJdKSB7IF8ubGFiZWwgPSB0WzJdOyBfLm9wcy5wdXNoKG9wKTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHRbMl0pIF8ub3BzLnBvcCgpO1xuICAgICAgICAgICAgICAgICAgICBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgb3AgPSBib2R5LmNhbGwodGhpc0FyZywgXyk7XG4gICAgICAgIH0gY2F0Y2ggKGUpIHsgb3AgPSBbNiwgZV07IHkgPSAwOyB9IGZpbmFsbHkgeyBmID0gdCA9IDA7IH1cbiAgICAgICAgaWYgKG9wWzBdICYgNSkgdGhyb3cgb3BbMV07IHJldHVybiB7IHZhbHVlOiBvcFswXSA/IG9wWzFdIDogdm9pZCAwLCBkb25lOiB0cnVlIH07XG4gICAgfVxufTtcbmltcG9ydCB7IGRlZmluZUNvbmZpZyB9IGZyb20gJ3ZpdGUnO1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0JztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgcmV0dXJuIF9fZ2VuZXJhdG9yKHRoaXMsIGZ1bmN0aW9uIChfYSkge1xuICAgICAgICAgICAgICAgICAgICBzd2l0Y2ggKF9hLmxhYmVsKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDA6IHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnLi9zZXJ2ZXIvYXBwLmpzJyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAxOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3NBcHAgPSAoX2Euc2VudCgpKS5kZWZhdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcCA9IGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgc2VydmVyLm1pZGRsZXdhcmVzLnVzZSgnL2FwaScsIGZ1bmN0aW9uIChyZXEsIHJlcywgbmV4dCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAocmVxLCByZXMsIG5leHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbMiAvKnJldHVybiovXTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgfTtcbn1cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gICAgcGx1Z2luczogW3JlYWN0KCksIGV4cHJlc3NQbHVnaW4oKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsbUJBQW1CO0FBQ3pTLFNBQVMsTUFBTSxjQUFjO0FBUTdCLFNBQVMsaUJBQWlCO0FBQ3hCLE1B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SixlQUFPLFlBQVksTUFBTSxTQUFVLElBQUk7QUFDbkMsa0JBQVEsR0FBRyxPQUFPO0FBQUEsWUFDZCxLQUFLO0FBQUcscUJBQU8sQ0FBQyxHQUFhLHVEQUF5QjtBQUFBLFlBQ3RELEtBQUs7QUFDRCwyQkFBYyxHQUFHLEtBQUssRUFBRztBQUN6QixjQUFBQSxPQUFNO0FBQ04scUJBQU8sWUFBWSxJQUFJLFFBQVEsU0FBVSxLQUFLLEtBQUssTUFBTTtBQUNyRCxnQkFBQUEsS0FBSSxLQUFLLEtBQUssSUFBSTtBQUFBLGNBQ3RCLENBQUM7QUFDRCxxQkFBTztBQUFBLGdCQUFDO0FBQUE7QUFBQSxjQUFZO0FBQUEsVUFDNUI7QUFBQSxRQUNKLENBQUM7QUFBQSxNQUNMLENBQUM7QUFBQSxJQUNMO0FBQUEsRUFDSjtBQUNKO0FBQ0EsSUFBTyxzQkFBUSxhQUFhO0FBQUEsRUFDeEIsU0FBUyxDQUFDLE1BQU0sR0FBRyxjQUFjLENBQUM7QUFBQSxFQUNsQyxTQUFTO0FBQUEsSUFDTCxPQUFPO0FBQUEsTUFDSCxLQUFLRixNQUFLLFFBQVFELFlBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJtb2RlbCIsICJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgInNlc3Npb24iLCAiQkFUQ0hfU0laRSIsICJ1dWlkdjQiLCAiUm91dGVyIiwgInBhdGgiLCAidXVpZHY0IiwgIkJBVENIX1NJWkUiLCAiUEFSQUxMRUxfQ0FMTFMiLCAiR1JPVVBfV0FJVF9NUyIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuQUkiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiZ2VuQUkiLCAiUFJJTUFSWV9NT0RFTCIsICJtb2RlbCIsICJSb3V0ZXIiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIl0KfQo=
