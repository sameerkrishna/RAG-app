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
      location: "global"
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
    let firstTokenTimeout = null;
    let requestTimeoutId = null;
    const controller = new AbortController();
    try {
      requestTimeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
      const result = await getGenAI().models.generateContentStream({
        model: "gemini-3.5-flash",
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        config: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      }, {
        signal: controller.signal
      });
      if (!result?.stream || typeof result.stream[Symbol.asyncIterator] !== "function") {
        throw new Error(`Streaming unavailable for model ${modelName}`);
      }
      let firstToken = true;
      firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);
      for await (const chunk of result.stream) {
        if (controller.signal.aborted) {
          throw new Error("Stream execution aborted by timeout constraint.");
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
      clearTimeout(requestTimeoutId);
      return { success: true };
    } catch (error) {
      retries++;
      if (firstTokenTimeout) clearTimeout(firstTokenTimeout);
      if (requestTimeoutId) clearTimeout(requestTimeoutId);
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcbiAgICBjbG91ZENsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xvdWRDbGllbnQ7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBHbG9iYWwgY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGNvbm5lY3QgdG8gZ2xvYmFsIGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG4gIHJldHVybiBnbG9iYWxDb2xsZWN0aW9uO1xufVxuXG4vKipcbiAqIFJldHVybnMgeyBjb2xsZWN0aW9uLCBpc05ldyB9LlxuICogaXNOZXcgPSB0cnVlICBcdTIxOTIgZnJlc2hseSBjcmVhdGVkLCBuZWVkcyBzZWVkaW5nIGZyb20gZ2xvYmFsLlxuICogaXNOZXcgPSBmYWxzZSBcdTIxOTIgYWxyZWFkeSBleGlzdGVkIG9uIENocm9tYSBDbG91ZCwgcmVzcGVjdCBpdHMgY3VycmVudCBzdGF0ZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHsgY29sbGVjdGlvbjogc2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpLCBpc05ldzogZmFsc2UgfTtcbiAgfVxuXG4gIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcblxuICBsZXQgY29sbGVjdGlvbjtcbiAgbGV0IGlzTmV3O1xuXG4gIHRyeSB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICB9KTtcbiAgICBpc05ldyA9IGZhbHNlO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNjdiXFx1ZmUwZiAgU2Vzc2lvbiBjb2xsZWN0aW9uIGV4aXN0cywgcmV1c2luZzogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfSBjYXRjaCB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5jcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIGlzTmV3ID0gdHJ1ZTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY3JlYXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfVxuXG4gIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgcmV0dXJuIHsgY29sbGVjdGlvbiwgaXNOZXcgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gZGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbi8qKlxuICogQWRkIHZlY3RvcnMgaW4gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIHRvIGF2b2lkIENocm9tYSBwYXlsb2FkIGxpbWl0cy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBpZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGNvbnN0IGJhdGNoSWRzICAgICAgICA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzICA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyAgPSB2ZWN0b3JzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAodiA9PiB2Lm1ldGFkYXRhKTtcblxuICAgICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6ICAgICAgICBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6ICBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiAgYmF0Y2hNZXRhZGF0YXNcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgW2FkZFZlY3RvcnNdIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogYWRkZWQgJHtiYXRjaElkcy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPiBkZWZhdWx0IDEwMCBsaW1pdCkgYXJlIGZ1bGx5IGRlbGV0ZWQuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9LFxuICAgICAgICBpbmNsdWRlOiBbXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGFsbElkcyB9KTtcbiAgICB9XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG4vKipcbiAqIExpc3QgYWxsIHVuaXF1ZSBkb2N1bWVudHMgaW4gYSBjb2xsZWN0aW9uLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgd2l0aCBCQVRDSF9TSVpFPTMwMCBzbyBjb2xsZWN0aW9ucyBsYXJnZXJcbiAqIHRoYW4gQ2hyb21hJ3MgZGVmYXVsdCBnZXQoKSBsaW1pdCAoMTAwKSBhcmUgZnVsbHkgZW51bWVyYXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcblxuICAgICAgYmF0Y2guaWRzLmZvckVhY2goKGlkLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgbWV0YSAgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2NJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiAgICAgICAgIG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogICAgICAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogICAgICAgbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6ICAgICAgbWV0YS5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIGZpcnN0X2NodW5rX3RleHQ6IGJhdGNoLmRvY3VtZW50c1tpZHhdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBkb2MgPSBkb2N1bWVudHNNYXAuZ2V0KGRvY0lkKTtcbiAgICAgICAgZG9jLmNodW5rX2NvdW50Kys7XG4gICAgICAgIGRvYy5wYWdlX2NvdW50ID0gTWF0aC5tYXgoZG9jLnBhZ2VfY291bnQsIG1ldGEucGFnZV9udW1iZXIgfHwgMSk7XG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYCAgW2xpc3REb2N1bWVudHNdIG9mZnNldD0ke29mZnNldH0sIGdvdD0ke2JhdGNoLmlkcy5sZW5ndGh9LCB1bmlxdWUgc28gZmFyPSR7ZG9jdW1lbnRzTWFwLnNpemV9YCk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShkb2N1bWVudHNNYXAudmFsdWVzKCkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50czonLCBlcnJvcik7XG4gICAgcmV0dXJuIFtdO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGhDaGVjaygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY2xlYW51cFNlc3Npb25Db2xsZWN0aW9ucygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25zID0gYXdhaXQgY2xpZW50Lmxpc3RDb2xsZWN0aW9ucygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcXHUyNzA1IE5vIHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbnMgZm91bmQuJyk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFxcdWQ4M2VcXHVkZGY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFxcdTI3MDUgRGVsZXRlZDogJHtuYW1lfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYCAgXFx1MjZhMFxcdWZlMGYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1xcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGNsZWFudXAgY29tcGxldGUuJyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS53YXJuKCdcXHUyNmEwXFx1ZmUwZiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7ZXhwb3J0IGNsYXNzIEFwcEVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlLCBzdGF0dXNDb2RlID0gNTAwKSB7XG4gICAgc3VwZXIobWVzc2FnZSk7XG4gICAgdGhpcy5jb2RlID0gY29kZTtcbiAgICB0aGlzLnN0YXR1c0NvZGUgPSBzdGF0dXNDb2RlO1xuICAgIHRoaXMuaXNPcGVyYXRpb25hbCA9IHRydWU7XG4gICAgRXJyb3IuY2FwdHVyZVN0YWNrVHJhY2UodGhpcywgdGhpcy5jb25zdHJ1Y3Rvcik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFZhbGlkYXRpb25FcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdWQUxJREFUSU9OX0VSUk9SJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFVwbG9hZExpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVVBMT0FEX0xJTUlUX0VYQ0VFREVEJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEZpbGVUb29MYXJnZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXhTaXplTUIpIHtcbiAgICBzdXBlcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmAsICdGSUxFX1RPT19MQVJHRScsIDQxMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEludmFsaWRGaWxlVHlwZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignT25seSBQREYgZmlsZXMgYXJlIGFsbG93ZWQnLCAnSU5WQUxJRF9GSUxFX1RZUEUnLCA0MTUpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBUb29NYW55UERGc0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXgpIHtcbiAgICBzdXBlcihgTWF4aW11bSAke21heH0gUERGcyBhbGxvd2VkIHBlciBzZXNzaW9uYCwgJ1RPT19NQU5ZX1BERlMnLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBEdXBsaWNhdGVGaWxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKGZpbGVuYW1lKSB7XG4gICAgc3VwZXIoYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmAsICdEVVBMSUNBVEVfRklMRScsIDQwOSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvcnJ1cHRlZFBERkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRmFpbGVkIHRvIHBhcnNlIFBERiBmaWxlLiBJdCBtYXkgYmUgY29ycnVwdGVkLicsICdDT1JSVVBURURfUERGJywgNDIyKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmF0ZUxpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKHJldHJ5QWZ0ZXIgPSA2MCkge1xuICAgIHN1cGVyKCdSYXRlIGxpbWl0IGV4Y2VlZGVkLiBQbGVhc2UgdHJ5IGFnYWluIGxhdGVyLicsICdSQVRFX0xJTUlUX0VYQ0VFREVEJywgNDI5KTtcbiAgICB0aGlzLnJldHJ5QWZ0ZXIgPSByZXRyeUFmdGVyO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBMTE1VbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignQUkgc2VydmljZSBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZS4gUGxlYXNlIHRyeSBhZ2Fpbi4nLCAnTExNX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRW1iZWRkaW5nRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UgPSAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgJ0VNQkVERElOR19FUlJPUicsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJldHJpZXZhbFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdEb2N1bWVudCByZXRyaWV2YWwgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnUkVUUklFVkFMX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ1dlYiBzZWFyY2ggaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnV0VCX1NFQVJDSF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBFbWJlZGRpbmdFcnJvciwgaXM0MjlFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmxldCBhaSA9IG51bGw7XG5sZXQgZW1iZWRkaW5nTW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRFbWJlZGRpbmdNb2RlbCgpIHtcbiAgaWYgKCFlbWJlZGRpbmdNb2RlbCkge1xuICAgIGFpID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdDogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgcHJvY2Vzcy5lbnYuR0NQX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCcsXG4gICAgICBsb2NhdGlvbjogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX0xPQ0FUSU9OIHx8ICd1cy1jZW50cmFsMSdcbiAgICB9KTtcblxuICAgIGVtYmVkZGluZ01vZGVsID0gYWkubW9kZWxzO1xuICB9XG4gIHJldHVybiBlbWJlZGRpbmdNb2RlbDtcbn1cblxuY29uc3QgQkFUQ0hfU0laRSAgICAgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSAgfHwgNDtcbmNvbnN0IE9VVFBVVF9ESU1FTlNJT05TID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19ESU1FTlNJT05TKSB8fCAzMDcyO1xuY29uc3QgR1JPVVBfV0FJVF9NUyAgPSA2MTAwMDtcbmNvbnN0IFJFVFJZX1dBSVRfTVMgID0gMTUwMDA7XG5cbi8vIEVtYmVkIGEgc2luZ2xlIGJhdGNoIG9mIHRleHRzICh1cCB0byBCQVRDSF9TSVpFKS5cbi8vIFJldHJpZXMgb24gNDI5IGFuZCB0cmFuc2llbnQgNTAyLzUwMyBlcnJvcnMgdXAgdG8gNSB0aW1lcy5cbmFzeW5jIGZ1bmN0aW9uIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIGF0dGVtcHQgPSAxKSB7XG4gIGNvbnN0IG1heEF0dGVtcHRzID0gNTtcbiAgY29uc3QgbW9kZWxOYW1lID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19NT0RFTCB8fCAnZ2VtaW5pLWVtYmVkZGluZy0wMDEnO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRFbWJlZGRpbmdNb2RlbCgpO1xuXG4gICAgY29uc3QgZW1iZWRkaW5nUHJvbWlzZXMgPSB0ZXh0cy5tYXAoYXN5bmMgKHJhd1RleHQpID0+IHtcbiAgICAgIC8vIENvZXJjZSBzYWZlbHkgdG8gc3RyaW5nIHRvIHByZXZlbnQgQVBJIGlucHV0IHZhbGlkYXRpb24gZmFpbHVyZXNcbiAgICAgIGNvbnN0IHRleHQgPSB0eXBlb2YgcmF3VGV4dCA9PT0gJ3N0cmluZycgPyByYXdUZXh0IDogU3RyaW5nKHJhd1RleHQpO1xuICAgICAgXG4gICAgICBpZiAoIXRleHQgfHwgdGV4dC50cmltKCkgPT09ICcnKSB7XG4gICAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcignQ2Fubm90IGVtYmVkIGFuIGVtcHR5IG9yIG1pc3NpbmcgdGV4dCBibG9jaycpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IG1vZGVsLmVtYmVkQ29udGVudCh7XG4gICAgICAgIG1vZGVsOiBtb2RlbE5hbWUsXG4gICAgICAgIGNvbnRlbnRzOiB0ZXh0LFxuICAgICAgICBjb25maWc6IHtcbiAgICAgICAgICB0YXNrVHlwZSxcbiAgICAgICAgICBvdXRwdXREaW1lbnNpb25hbGl0eTogT1VUUFVUX0RJTUVOU0lPTlMoKVxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgLy8gSGFuZGxlIHN0cnVjdHVyYWwgdmFyaWF0aW9ucyBpbiB0aGUgU0RLIHJlc3BvbnNlIHBheWxvYWRcbiAgICAgIGNvbnN0IHZhbHVlcyA9IHJlc3BvbnNlPy5lbWJlZGRpbmdzPy5bMF0/LnZhbHVlcyB8fCBcbiAgICAgICAgICAgICAgICAgICAgIHJlc3BvbnNlPy5lbWJlZGRpbmc/LnZhbHVlcyB8fCBcbiAgICAgICAgICAgICAgICAgICAgIHJlc3BvbnNlPy52YWx1ZXM7XG5cbiAgICAgIGlmICghdmFsdWVzKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoJ1tlbWJlZGRpbmddIFVuZXhwZWN0ZWQgQVBJIHJlc3BvbnNlIHNoYXBlOicsIEpTT04uc3RyaW5naWZ5KHJlc3BvbnNlKSk7XG4gICAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcignTWlzc2luZyB2YWx1ZXMgaW4gZW1iZWRkaW5nIHJlc3BvbnNlJyk7XG4gICAgICB9XG5cbiAgICAgIHJldHVybiB2YWx1ZXM7XG4gICAgfSk7XG5cbiAgICBjb25zdCBlbWJlZGRpbmdzID0gYXdhaXQgUHJvbWlzZS5hbGwoZW1iZWRkaW5nUHJvbWlzZXMpO1xuXG4gICAgaWYgKGVtYmVkZGluZ3MubGVuZ3RoICE9PSB0ZXh0cy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihgRXhwZWN0ZWQgJHt0ZXh0cy5sZW5ndGh9IGVtYmVkZGluZ3MsIGdvdCAke2VtYmVkZGluZ3MubGVuZ3RofWApO1xuICAgIH1cblxuICAgIHJldHVybiBlbWJlZGRpbmdzO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgLy8gUmV0cnkgb24gcmF0ZSBsaW1pdHMgKDQyOSkgYXMgd2VsbCBhcyB0ZW1wb3JhcnkgZ2F0ZXdheS9zZXJ2aWNlIGRpc3J1cHRpb25zICg1MDIsIDUwMylcbiAgICBjb25zdCBpc1JldHJ5YWJsZSA9IGlzNDI5RXJyb3IoZXJyb3IpIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMiB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAzIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1NlcnZpY2UgVW5hdmFpbGFibGUnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdCYWQgR2F0ZXdheScpO1xuXG4gICAgaWYgKGlzUmV0cnlhYmxlICYmIGF0dGVtcHQgPCBtYXhBdHRlbXB0cykge1xuICAgICAgLy8gU2NhbGUgd2FpdCBkeW5hbWljYWxseSBpZiBpdCdzIGEgc3RydWN0dXJhbCBnYXRld2F5IGVycm9yXG4gICAgICBjb25zdCBiYXNlRGVsYXkgPSBlcnJvci5yZXRyeUFmdGVyIHx8IChhdHRlbXB0ICogUkVUUllfV0FJVF9NUyk7XG4gICAgICBjb25zdCByZXRyeURlbGF5ID0gZXJyb3I/LnN0YXR1cyA9PT0gNDI5ID8gR1JPVVBfV0FJVF9NUyA6IGJhc2VEZWxheTtcblxuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFRyYW5zaWVudCBlcnJvciAoJHtlcnJvcj8uc3RhdHVzIHx8ICd1bmtub3duJ30pLCB3YWl0aW5nICR7cmV0cnlEZWxheSAvIDEwMDB9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9KS4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gRXhwb3J0ZWQgZm9yIGRvY3VtZW50cy5qcyB1cGxvYWQgaGFuZGxlciBcdTIwMTQgZW1iZWRzIG9uZSBiYXRjaCBncm91cCAodXAgdG8gQkFUQ0hfU0laRSB0ZXh0cylcbi8vIGFuZCByZXR1cm5zIHJhdyB2ZWN0b3JzIGFycmF5LiBDYWxsZXIgbWFuYWdlcyBwYXJhbGxlbGlzbSwgd2FpdGluZywgYW5kIENocm9tYSB3cml0ZXMuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnKSB7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0ICR7dGV4dHMubGVuZ3RofSB0ZXh0cywgdGFza1R5cGU9JHt0YXNrVHlwZX1gKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlKTtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgZ290ICR7dmVjdG9ycy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgcmV0dXJuIHZlY3RvcnM7XG59XG5cbi8vIEZ1bGwgcGlwZWxpbmU6IGVtYmVkIGFsbCBjaHVua3Mgd2l0aCBidWlsdC1pbiBiYXRjaGluZyArIHdhaXRpbmcuXG4vLyBVc2VkIGJ5IHNlZWQgaW5nZXN0aW9uIGFuZCBhbnkgY2FsbGVycyB0aGF0IGRvbid0IG5lZWQgc3RyZWFtaW5nIHByb2dyZXNzLlxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSAgICAgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuICBjb25zdCBlbWJlZGRpbmdzICAgID0gW107XG5cbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSk7XG4gIH1cblxuICBjb25zdCB0b3RhbEdyb3VwcyA9IE1hdGguY2VpbChiYXRjaGVzLmxlbmd0aCAvIHBhcmFsbGVsQ2FsbHMpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IHBhcmFsbGVsQmF0Y2hlcyA9IGJhdGNoZXMuc2xpY2UoaSwgaSArIHBhcmFsbGVsQ2FsbHMpO1xuICAgIGNvbnN0IGdyb3VwTnVtICAgICAgICA9IE1hdGguZmxvb3IoaSAvIHBhcmFsbGVsQ2FsbHMpICsgMTtcbiAgICBjb25zdCBjaHVua3NDb3ZlcmVkICAgPSBNYXRoLm1pbigoaSArIHBhcmFsbGVsQ2FsbHMpICogYmF0Y2hTaXplLCBjaHVua3MubGVuZ3RoKTtcblxuICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBHcm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSBcdTIwMTQgJHtwYXJhbGxlbEJhdGNoZXMubGVuZ3RofSBiYXRjaCBjYWxsKHMpIGluIHBhcmFsbGVsIChjaHVua3MgJHtpICogYmF0Y2hTaXplICsgMX1cdTIwMTMke2NodW5rc0NvdmVyZWR9KS4uLmApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHBhcmFsbGVsQmF0Y2hlcy5tYXAoYmF0Y2ggPT4gZW1iZWRCYXRjaChiYXRjaC5tYXAoYyA9PiBjLnRleHQpLCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGJhdGNoID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlO1xuICAgICAgICBiYXRjaC5mb3JFYWNoKChjaHVuaywgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBhYnNvbHV0ZUNodW5rSWR4ID0gKGkgKyBiYXRjaElkeCkgKiBiYXRjaFNpemUgKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHthYnNvbHV0ZUNodW5rSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gQmF0Y2ggJHtpICsgYmF0Y2hJZHh9IGZhaWxlZCwgd2lsbCByZXRyeSBpbmRpdmlkdWFsbHk6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZEJhdGNoZXMucHVzaCh7IGJhdGNoLCBiYXRjaElkeDogaSArIGJhdGNoSWR4IH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKG9uUHJvZ3Jlc3MpIHtcbiAgICAgIG9uUHJvZ3Jlc3MoeyBjdXJyZW50X2JhdGNoOiBncm91cE51bSwgdG90YWxfYmF0Y2hlczogdG90YWxHcm91cHMgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgaXNMYXN0R3JvdXAgPSBpICsgcGFyYWxsZWxDYWxscyA+PSBiYXRjaGVzLmxlbmd0aDtcbiAgICBpZiAoIWlzTGFzdEdyb3VwIHx8IGZhaWxlZEJhdGNoZXMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFdhaXRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIGJlZm9yZSBuZXh0IGdyb3VwLi4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgR1JPVVBfV0FJVF9NUykpO1xuICAgIH1cblxuICAgIGZvciAoY29uc3QgeyBiYXRjaCwgYmF0Y2hJZHggfSBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gV2FpdGluZyAke1JFVFJZX1dBSVRfTVMgLyAxMDAwfXMgYmVmb3JlIHJldHJ5aW5nIGZhaWxlZCBiYXRjaCAke2JhdGNoSWR4fS4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIFJFVFJZX1dBSVRfTVMpKTtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgYmF0Y2gpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtiYXRjaElkeH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW2VtYmVkZGluZ10gXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbdGV4dF0sICdSRVRSSUVWQUxfRE9DVU1FTlQnKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSYXRlTGltaXRTdGF0ZSgpIHtcbiAgcmV0dXJuIHtcbiAgICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgICBwYXJhbGxlbENhbGxzOiBQQVJBTExFTF9DQUxMUygpLFxuICAgIG1heENodW5rc1BlckNhbGw6IEJBVENIX1NJWkUoKSxcbiAgICBvdXRwdXREaW1lbnNpb25zOiBPVVRQVVRfRElNRU5TSU9OUygpXG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0UmF0ZUxpbWl0U3RhdGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGgocmVxLCByZXMpIHtcbiAgY29uc3QgaGVhbHRoU3RhdHVzID0ge1xuICAgIHN0YXR1czogJ29rJyxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICBzZXJ2aWNlczoge31cbiAgfTtcblxuICAvLyBDaGVjayBDaHJvbWFEQlxuICB0cnkge1xuICAgIGNvbnN0IGNocm9tYUhlYWx0aCA9IGF3YWl0IGNocm9tYUhlYWx0aENoZWNrKCk7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0gY2hyb21hSGVhbHRoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IHtcbiAgICAgIHN0YXR1czogJ2Vycm9yJyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlXG4gICAgfTtcbiAgfVxuXG4gIC8vIENoZWNrIEdlbWluaSAodmlhIEFQSSBrZXkgcHJlc2VuY2UpXG4gIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5nZW1pbmkgPSB7XG4gICAgc3RhdHVzOiBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSA/ICdjb25maWd1cmVkJyA6ICdub3RfY29uZmlndXJlZCdcbiAgfTtcblxuICAvLyBHZXQgcmF0ZSBsaW1pdCBzdGF0ZVxuICBoZWFsdGhTdGF0dXMucmF0ZUxpbWl0ID0gZ2V0UmF0ZUxpbWl0U3RhdGUoKTtcblxuICAvLyBPdmVyYWxsIHN0YXR1c1xuICBjb25zdCBoYXNFcnJvcnMgPSBPYmplY3QudmFsdWVzKGhlYWx0aFN0YXR1cy5zZXJ2aWNlcykuc29tZShcbiAgICBzID0+IHMuc3RhdHVzID09PSAnZXJyb3InIHx8IHMuc3RhdHVzID09PSAndW5oZWFsdGh5J1xuICApO1xuXG4gIGlmIChoYXNFcnJvcnMpIHtcbiAgICBoZWFsdGhTdGF0dXMuc3RhdHVzID0gJ2RlZ3JhZGVkJztcbiAgfVxuXG4gIHJlcy5qc29uKGhlYWx0aFN0YXR1cyk7XG59XG5cbnJvdXRlci5nZXQoJy8nLCBoZWFsdGgpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2ltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgVmFsaWRhdGlvbkVycm9yIH0gZnJvbSAnLi9lcnJvcnMuanMnO1xuXG5jb25zdCBEQU5HRVJPVVNfUEFUVEVSTlMgPSAvWzw+OlwifD8qXFx4MDAtXFx4MWZdL2c7XG5jb25zdCBQQVRIX1RSQVZFUlNBTCA9IC9cXC5cXC4vZztcblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplRmlsZW5hbWUoZmlsZW5hbWUpIHtcbiAgaWYgKCFmaWxlbmFtZSB8fCB0eXBlb2YgZmlsZW5hbWUgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZScpO1xuICB9XG5cbiAgLy8gUmVtb3ZlIHBhdGggY29tcG9uZW50cyBhbmQgZ2V0IGJhc2VuYW1lXG4gIGNvbnN0IGJhc2VuYW1lID0gcGF0aC5iYXNlbmFtZShmaWxlbmFtZSk7XG5cbiAgLy8gUmVtb3ZlIGRhbmdlcm91cyBjaGFyYWN0ZXJzXG4gIGxldCBzYW5pdGl6ZWQgPSBiYXNlbmFtZS5yZXBsYWNlKERBTkdFUk9VU19QQVRURVJOUywgJ18nKTtcblxuICAvLyBSZW1vdmUgcGF0aCB0cmF2ZXJzYWwgYXR0ZW1wdHNcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnJlcGxhY2UoUEFUSF9UUkFWRVJTQUwsICcnKTtcblxuICAvLyBUcmltIHdoaXRlc3BhY2UgYW5kIGxpbWl0IGxlbmd0aFxuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQudHJpbSgpLnNsaWNlKDAsIDI1NSk7XG5cbiAgaWYgKCFzYW5pdGl6ZWQpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lIGFmdGVyIHNhbml0aXphdGlvbicpO1xuICB9XG5cbiAgcmV0dXJuIHNhbml0aXplZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlUERGRmlsZShmaWxlKSB7XG4gIGlmICghZmlsZSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ05vIGZpbGUgcHJvdmlkZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIE1JTUUgdHlwZVxuICBjb25zdCB2YWxpZE1pbWVUeXBlcyA9IFsnYXBwbGljYXRpb24vcGRmJ107XG4gIGlmICghdmFsaWRNaW1lVHlwZXMuaW5jbHVkZXMoZmlsZS5taW1ldHlwZSkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdPbmx5IFBERiBmaWxlcyBhcmUgYWNjZXB0ZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIGV4dGVuc2lvblxuICBjb25zdCBleHQgPSBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUgfHwgJycpLnRvTG93ZXJDYXNlKCk7XG4gIGlmIChleHQgIT09ICcucGRmJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ZpbGUgbXVzdCBoYXZlIC5wZGYgZXh0ZW5zaW9uJyk7XG4gIH1cblxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRmlsZVNpemUoc2l6ZUJ5dGVzLCBtYXhTaXplTUIpIHtcbiAgY29uc3QgbWF4Qnl0ZXMgPSBtYXhTaXplTUIgKiAxMDI0ICogMTAyNDtcbiAgaWYgKHNpemVCeXRlcyA+IG1heEJ5dGVzKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmApO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVJbnB1dChpbnB1dCwgbWF4TGVuZ3RoID0gMTAwMDApIHtcbiAgaWYgKCFpbnB1dCB8fCB0eXBlb2YgaW5wdXQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG4gIHJldHVybiBpbnB1dC50cmltKCkuc2xpY2UoMCwgbWF4TGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRG9jdW1lbnRJZChpZCkge1xuICBpZiAoIWlkIHx8IHR5cGVvZiBpZCAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEJyk7XG4gIH1cbiAgY29uc3QgdXVpZFJlZ2V4ID0gL15bMC05YS1mXXs4fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXsxMn0kL2k7XG4gIGlmICghdXVpZFJlZ2V4LnRlc3QoaWQpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCBmb3JtYXQnKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RUZXh0RnJvbVBERkJ1ZmZlcihidWZmZXIpIHtcbiAgLy8gVGhpcyB3aWxsIGJlIHVzZWQgd2l0aCBwZGYtcGFyc2VcbiAgcmV0dXJuIGJ1ZmZlcjtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2ltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuXG5jb25zdCBDSEFSU19QRVJfVE9LRU4gPSA0O1xuY29uc3QgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUyA9IDEwMDA7XG5jb25zdCBERUZBVUxUX09WRVJMQVBfVE9LRU5TID0gMjAwO1xuY29uc3QgTUlOX0NIVU5LX0NIQVJTID0gMTAwO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblRleHQodGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gJyc7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoL1xcZi9nLCAnXFxuJylcbiAgICAucmVwbGFjZSgvKFxccypcXG4pezMsfS9nLCAnXFxuXFxuJylcbiAgICAucmVwbGFjZSgvXlxccypcXGQrXFxzKiQvZ20sICcnKVxuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgY2h1bmtTaXplVG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgREVGQVVMVF9DSFVOS19TSVpFX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRva2VucyA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBERUZBVUxUX09WRVJMQVBfVE9LRU5TO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICBjb25zdCBjaHVua1NpemVDaGFycyA9IGNodW5rU2l6ZVRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBjb25zdCBjaHVua3MgPSBbXTtcbiAgbGV0IHN0YXJ0ID0gMDtcbiAgbGV0IGNodW5rSW5kZXggPSAwO1xuXG4gIHdoaWxlIChzdGFydCA8IHRleHQubGVuZ3RoKSB7XG4gICAgbGV0IGVuZCA9IHN0YXJ0ICsgY2h1bmtTaXplQ2hhcnM7XG5cbiAgICBpZiAoZW5kIDwgdGV4dC5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IGJyZWFrUG9pbnRzID0gWycuICcsICcuXFxuJywgJyEgJywgJz8gJywgJ1xcblxcbicsICdcXG4nLCAnICddO1xuICAgICAgY29uc3Qgc2VhcmNoU3RhcnQgPSBlbmQgLSBNYXRoLmZsb29yKGNodW5rU2l6ZUNoYXJzICogMC4yKTtcblxuICAgICAgZm9yIChjb25zdCBicmVha3BvaW50IG9mIGJyZWFrUG9pbnRzKSB7XG4gICAgICAgIGNvbnN0IGlkeCA9IHRleHQubGFzdEluZGV4T2YoYnJlYWtwb2ludCwgZW5kKTtcbiAgICAgICAgaWYgKGlkeCA+IHNlYXJjaFN0YXJ0ICYmIGlkeCA+IHN0YXJ0KSB7XG4gICAgICAgICAgZW5kID0gaWR4ICsgYnJlYWtwb2ludC5sZW5ndGg7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBlbmQgPSBNYXRoLm1pbihlbmQsIHRleHQubGVuZ3RoKTtcbiAgICBjb25zdCBjaHVua0NvbnRlbnQgPSB0ZXh0LnNsaWNlKHN0YXJ0LCBlbmQpLnRyaW0oKTtcblxuICAgIGlmIChjaHVua0NvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiBjaHVua0NvbnRlbnQsXG4gICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKGNodW5rQ29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogc3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGVuZCxcbiAgICAgICAgY2h1bmtJbmRleDogY2h1bmtJbmRleCsrXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBuZXh0U3RhcnQgPSBlbmQgLSBvdmVybGFwQ2hhcnM7XG4gICAgc3RhcnQgPSBuZXh0U3RhcnQgPiBzdGFydCA/IG5leHRTdGFydCA6IGVuZDtcblxuICAgIGlmIChjaHVua0luZGV4ID4gMTAwMDApIHtcbiAgICAgIGNvbnNvbGUud2FybignQ2h1bmsgbGltaXQgcmVhY2hlZCwgc3RvcHBpbmcnKTtcbiAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQoY2xlYW5lZFRleHQsIG9wdGlvbnMpO1xuICBjb25zdCB0b3RhbENodW5rcyA9IHRleHRDaHVua3MubGVuZ3RoO1xuXG4gIC8vIEZJWCA0OiB1c2Ugc291cmNlVHlwZSBmcm9tIG9wdGlvbnMsIGZhbGwgYmFjayB0byAncGRmJ1xuICBjb25zdCBzb3VyY2VUeXBlID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogZmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6IHNvdXJjZVR5cGUsICAgICAgICAgICAgLy8gRklYIDRcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH07XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0U2VjdGlvblRpdGxlKHRleHQpIHtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQge1xuICBnZXRHbG9iYWxDb2xsZWN0aW9uLFxuICBnZXRTZXNzaW9uQ29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50cyxcbiAgYWRkVmVjdG9yc1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGlkID0gc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICBkZWxldGVkRG9jdW1lbnRJZHM6IG5ldyBTZXQoKSwgICAvLyB0cmFjayBkZWxldGVkIGRvYyBJRHMgdG8gZmlsdGVyIHByb21wdCBtZW1vcnlcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcbiAgc2Vzc2lvbnMuc2V0KGlkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHJldHVybiBleGlzdGluZztcbiAgICByZXR1cm4gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIElmIGNvbGxlY3Rpb24gaXMgTkVXIFx1MjE5MiBzZWVkIGZyb20gZ2xvYmFsIChwYWdpbmF0ZWQsIDMwMC9iYXRjaClcbiAqIC0gSWYgY29sbGVjdGlvbiBFWElTVFMgXHUyMTkyIHNraXAgc2VlZCwgcmVjb25zdHJ1Y3QgaW4tbWVtb3J5IGRvYyBsaXN0IGZyb20gQ2hyb21hXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhgXHVEODNEXHVERDExIFNlc3Npb24gaW5pdDogJHtzZXNzaW9uSWR9YCk7XG4gIGlmIChzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWxyZWFkeSBzZWVkZWQgJHtzZXNzaW9uSWR9LCBza2lwcGluZ2ApO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb246IHNlc3Npb25Db2xsZWN0aW9uLCBpc05ldyB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGlmICghaXNOZXcpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI2N0JcdUZFMEYgIFNlc3Npb24gZXhpc3RzLCByZWNvbnN0cnVjdGluZyBkb2N1bWVudCBsaXN0IGZyb20gQ2hyb21hLi4uYCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKHNlc3Npb25Db2xsZWN0aW9uKTtcbiAgICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgICAgc291cmNlVHlwZTogZG9jLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IGRvY3VtZW50KHMpIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFx1RDgzQ1x1REYzMSBOZXcgc2Vzc2lvbiBcdTIwMTQgc2VlZGluZyBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uLi4uYCk7XG5cbiAgICBjb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdLCBhbGxFbWJlZGRpbmdzID0gW10sIGFsbERvY3VtZW50cyA9IFtdLCBhbGxNZXRhZGF0YXMgPSBbXTtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGdsb2JhbENvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydlbWJlZGRpbmdzJywgJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uYmF0Y2guZW1iZWRkaW5ncyk7XG4gICAgICBhbGxEb2N1bWVudHMucHVzaCguLi5iYXRjaC5kb2N1bWVudHMpO1xuICAgICAgYWxsTWV0YWRhdGFzLnB1c2goLi4uYmF0Y2gubWV0YWRhdGFzKTtcbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjZBMFx1RkUwRiAgR2xvYmFsIGNvbGxlY3Rpb24gaXMgZW1wdHkgXHUyMDE0IG5vdGhpbmcgdG8gc2VlZC4nKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYWxsSWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6IGFsbElkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIGVtYmVkZGluZ3M6IGFsbEVtYmVkZGluZ3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLFxuICAgICAgICBkb2N1bWVudHM6IGFsbERvY3VtZW50cy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIG1ldGFkYXRhczogYWxsTWV0YWRhdGFzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAobSA9PiAoeyAuLi5tLCBzb3VyY2VfdHlwZTogJ2dsb2JhbCcgfSkpXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFx1RDgzRFx1RENFNiBBZGRlZCBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IHJlY29yZHMgJHtpICsgMX1cdTIwMTMke01hdGgubWluKGkgKyBCQVRDSF9TSVpFLCBhbGxJZHMubGVuZ3RoKX1gKTtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlZWRlZCAke2FsbElkcy5sZW5ndGh9IHZlY3RvcnMgaW50byBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoc2Vzc2lvbikge1xuICAgICAgY29uc3QgZG9jc01hcCA9IG5ldyBNYXAoKTtcbiAgICAgIGFsbE1ldGFkYXRhcy5mb3JFYWNoKG1ldGEgPT4ge1xuICAgICAgICBpZiAoIWRvY3NNYXAuaGFzKG1ldGEuZG9jdW1lbnRfaWQpKSB7XG4gICAgICAgICAgZG9jc01hcC5zZXQobWV0YS5kb2N1bWVudF9pZCwge1xuICAgICAgICAgICAgaWQ6IG1ldGEuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBtZXRhLnRvdGFsX3BhZ2VzIHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiAwLFxuICAgICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGRvY3NNYXAuZ2V0KG1ldGEuZG9jdW1lbnRfaWQpLmNodW5rQ291bnQrKztcbiAgICAgIH0pO1xuXG4gICAgICBmb3IgKGNvbnN0IGRvYyBvZiBkb2NzTWFwLnZhbHVlcygpKSB7XG4gICAgICAgIGlmICghc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuaWQgPT09IGRvYy5pZCkpIHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKGRvYyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBVcHNlcnQgYSBkb2N1bWVudCBpbnRvIHRoZSBzZXNzaW9uLlxuICogSWYgYSBkb2Mgd2l0aCB0aGUgc2FtZSBpZCBhbHJlYWR5IGV4aXN0cywgdXBkYXRlIGl0IGluIHBsYWNlIChubyBkdXBsaWNhdGUpLlxuICogU3VwcG9ydHMgcGFydGlhbCB1cGRhdGVzIFx1MjAxNCBvbmx5IHByb3ZpZGVkIGZpZWxkcyBvdmVyd3JpdGUgZXhpc3RpbmcgdmFsdWVzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG5cbiAgY29uc3QgZXhpc3RpbmcgPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kKGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJbmZvLmlkKTtcblxuICBpZiAoZXhpc3RpbmcpIHtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmNodW5rQ291bnQgID0gZG9jdW1lbnRJbmZvLmNodW5rQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5wYWdlQ291bnQgICAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5wYWdlQ291bnQgICA9IGRvY3VtZW50SW5mby5wYWdlQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlU2l6ZSAgICAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlU2l6ZSAgICA9IGRvY3VtZW50SW5mby5maWxlU2l6ZTtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnN0YXR1cyAgICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnN0YXR1cyAgICAgID0gZG9jdW1lbnRJbmZvLnN0YXR1cztcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVuYW1lICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVuYW1lICAgID0gZG9jdW1lbnRJbmZvLmZpbGVuYW1lO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFVwZGF0ZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZXhpc3Rpbmcuc3RhdHVzfSwgY2h1bmtzPSR7ZXhpc3RpbmcuY2h1bmtDb3VudH1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgPz8gMCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgIHN0YXR1czogZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnXG4gIH0pO1xuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWRkZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnfWApO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgLy8gVHJhY2sgZGVsZXRlZCBkb2Mgc28gaXRzIG1lbW9yeSB0dXJucyBhcmUgZXhjbHVkZWQgZnJvbSBmdXR1cmUgcHJvbXB0c1xuICAgIHNlc3Npb24uZGVsZXRlZERvY3VtZW50SWRzLmFkZChkb2N1bWVudElkKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBSZW1vdmVkIGRvYyAke2RvY3VtZW50SWR9LCBhZGRlZCB0byBkZWxldGVkRG9jdW1lbnRJZHNgKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIHJldHVybiBzZXNzaW9uPy5kZWxldGVkRG9jdW1lbnRJZHMgPz8gbmV3IFNldCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gW107XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBzZXNzaW9uRG9jdW1lbnRzOiBbXSwgZ2xvYmFsRG9jdW1lbnRzOiBbXSB9O1xuXG4gIGNvbnN0IG5vcm1hbGl6ZSA9IChkb2MpID0+ICh7XG4gICAgZG9jdW1lbnRfaWQ6IGRvYy5pZCxcbiAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgIGNodW5rX2NvdW50OiBkb2MuY2h1bmtDb3VudCA/PyAwLFxuICAgIHBhZ2VfY291bnQ6IGRvYy5wYWdlQ291bnQgPz8gMCxcbiAgICB1cGxvYWRfdGltZXN0YW1wOiBkb2MudXBsb2FkVGltZXN0YW1wIHx8IG51bGwsXG4gICAgc291cmNlX3R5cGU6IGRvYy5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ3Nlc3Npb25fdXBsb2FkJyA6ICdzZWVkJyxcbiAgICBmaWxlU2l6ZTogZG9jLmZpbGVTaXplIHx8IG51bGwsXG4gICAgc3RhdHVzOiBkb2Muc3RhdHVzID8/IG51bGxcbiAgfSk7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uLmRvY3VtZW50c1xuICAgICAgLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJylcbiAgICAgIC5tYXAobm9ybWFsaXplKSxcbiAgICBnbG9iYWxEb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnZ2xvYmFsJylcbiAgICAgIC5tYXAobm9ybWFsaXplKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgNTtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpOyAvLyBkZXN0cnVjdHVyZVxuICAgIGlmIChjb2xsZWN0aW9uKSBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwIH07XG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gTWF0aC5tYXgoMCwgci5zY29yZSkpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBbcXVlcnlFbWJlZGRpbmcsIHNlc3Npb25Db2xsZWN0aW9uXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIGVtYmVkUXVlcnkocXVlcnkpLFxuICAgICAgc2Vzc2lvbklkID8gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkgOiBQcm9taXNlLnJlc29sdmUobnVsbClcbiAgICBdKTtcblxuICAgIGlmICghc2Vzc2lvbkNvbGxlY3Rpb24pIHtcbiAgICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICBObyBzZXNzaW9uIGNvbGxlY3Rpb24gZm91bmQgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgcmV0dXJuIHsgcmVzdWx0czogW10sIGNvdmVyYWdlOiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwLCBsZXZlbDogJ2xvdycsIHNjb3JlOiAwIH0sIHF1ZXJ5RW1iZWRkaW5nIH07XG4gICAgfVxuXG4gICAgY29uc3QgcmF3UmVzdWx0cyA9IGF3YWl0IHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAuY2F0Y2goKCkgPT4gW10pO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBSYXcgc2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IG11bHRlciBmcm9tICdtdWx0ZXInO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcbmltcG9ydCBwZGYgZnJvbSAncGRmLXBhcnNlJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnOztcbmltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUsIHZhbGlkYXRlUERGRmlsZSwgdmFsaWRhdGVGaWxlU2l6ZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbmltcG9ydCB7XG4gIENvcnJ1cHRlZFBERkVycm9yLFxuICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgRmlsZVRvb0xhcmdlRXJyb3IsXG4gIFRvb01hbnlQREZzRXJyb3IsXG4gIER1cGxpY2F0ZUZpbGVFcnJvclxufSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuaW1wb3J0IHsgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbmltcG9ydCB7IGVtYmVkU2luZ2xlQmF0Y2hHcm91cCB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHtcbiAgZ2V0T3JDcmVhdGVTZXNzaW9uLFxuICBjYW5BY2NlcHRVcGxvYWQsXG4gIGFkZERvY3VtZW50VG9TZXNzaW9uLFxuICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uLFxuICBnZXRBbGxEb2N1bWVudHNcbn0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNsZWFyTWVtb3J5IH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG5jb25zdCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL3NlZWRfZG9jdW1lbnRzJyk7XG5cbmNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHVwbG9hZERpciksXG4gIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG59KTtcblxuY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgc3RvcmFnZSxcbiAgbGltaXRzOiB7IGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0IH0sXG4gIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICBjYihudWxsLCB0cnVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgIH1cbiAgfVxufSk7XG5cbmZ1bmN0aW9uIGNvbnRlbnREaXNwb3NpdGlvbihkaXNwbGF5TmFtZSkge1xuICBjb25zdCBlbmNvZGVkID0gZW5jb2RlVVJJQ29tcG9uZW50KGRpc3BsYXlOYW1lKVxuICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgIC5yZXBsYWNlKC9cXCgvZywgJyUyOCcpXG4gICAgLnJlcGxhY2UoL1xcKS9nLCAnJTI5Jyk7XG4gIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xufVxuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IHRjLml0ZW1zLm1hcChpID0+IGkuc3RyKS5qb2luKCcgJyk7XG4gICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgIGNvbnN0IHBhZ2VNYXAgPSBbXTtcbiAgICBsZXQgY2hhclBvcyA9IDA7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICB9XG5cbiAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRQYWdlTnVtYmVyKGNoYXJTdGFydCwgcGFnZU1hcCkge1xuICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICBpZiAoY2hhclN0YXJ0ID49IGVudHJ5LnN0YXJ0ICYmIGNoYXJTdGFydCA8IGVudHJ5LmVuZCkgcmV0dXJuIGVudHJ5LnBhZ2U7XG4gIH1cbiAgcmV0dXJuIHBhZ2VNYXBbcGFnZU1hcC5sZW5ndGggLSAxXT8ucGFnZSB8fCAxO1xufVxuXG5mdW5jdGlvbiBzc2VFdmVudChyZXMsIGV2ZW50LCBkYXRhKSB7XG4gIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gIGNvbnN0IEJBVENIX1NJWkUgICAgID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX0JBVENIX01BWF9DSFVOS1MpIHx8IDc7XG4gIGNvbnN0IFBBUkFMTEVMX0NBTExTID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSAgfHwgNDtcbiAgY29uc3QgR1JPVVBfV0FJVF9NUyAgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfR1JPVVBfV0FJVF9NUykgICB8fCA2MTAwMDtcblxuICB0cnkge1xuICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICBpZiAoIWZpbGUpIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbklkICAgICA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgY29uc3Qgc2Vzc2lvbiAgICAgICA9IGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IG1heFBERnMgICAgICAgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTiB8fCAnMycpO1xuICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgICBpZiAodXBsb2FkZWRDb3VudCA+PSBtYXhQREZzKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogYE1heGltdW0gJHttYXhQREZzfSB1cGxvYWRzIHJlYWNoZWRgLCBjb2RlOiAnVE9PX01BTllfUERGUycgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgXCIke2NsZWFuRmlsZW5hbWV9XCIgYWxyZWFkeSB1cGxvYWRlZGAsIGNvZGU6ICdEVVBMSUNBVEVfRklMRScgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAxIFx1MjAxNCBwYXJzaW5nICR7Y2xlYW5GaWxlbmFtZX0gKCR7ZmlsZS5zaXplfSBieXRlcylgKTtcbiAgICBjb25zdCB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH0gPSBhd2FpdCBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlLnBhdGgpO1xuXG4gICAgaWYgKCFmdWxsVGV4dCB8fCBmdWxsVGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gZXh0cmFjdGFibGUgdGV4dCBcdTIwMTQgUERGIG1heSBiZSBzY2FubmVkIG9yIGltYWdlLW9ubHknLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc3QgZG9jdW1lbnRJZCA9IHV1aWR2NCgpO1xuICAgIGNvbnN0IHJhd0NodW5rcyAgPSBjaHVua1RleHQoZnVsbFRleHQsIHsgY2h1bmtTaXplVG9rZW5zOiAxMDAwLCBvdmVybGFwVG9rZW5zOiAyMDAgfSk7XG5cbiAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiAgICAgICAgIGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiAgICAgICAgIGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogICAgIHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiAgICAgIGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6ICAgICAgdG90YWxQYWdlcyxcbiAgICAgICAgc291cmNlX3R5cGU6ICAgICAgJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiAgICAgICBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiAgICAgICAgIGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiAgICAgIGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9KSk7XG5cbiAgICBjb25zdCB0b3RhbENodW5rcyAgPSBjaHVua3MubGVuZ3RoO1xuICAgIGNvbnN0IHRvdGFsQmF0Y2hlcyA9IE1hdGguY2VpbCh0b3RhbENodW5rcyAvIEJBVENIX1NJWkUpO1xuICAgIGNvbnN0IHRvdGFsU2V0cyAgICA9IE1hdGguY2VpbCh0b3RhbEJhdGNoZXMgLyBQQVJBTExFTF9DQUxMUyk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgc3NlRXZlbnQocmVzLCAndXBsb2FkX2NvbXBsZXRlJywge1xuICAgICAgZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgIH0pO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IDAsIHN0YXR1czogJ2luZGV4aW5nJ1xuICAgIH0pO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgbGV0IHByb2Nlc3NlZENodW5rcyAgPSAwO1xuICAgIGNvbnN0IGFsbEVtYmVkZGluZ3MgID0gW107XG5cbiAgICBjb25zdCBiYXRjaGVzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpKTtcblxuICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IFBBUkFMTEVMX0NBTExTKSBzZXRzLnB1c2goYmF0Y2hlcy5zbGljZShpLCBpICsgUEFSQUxMRUxfQ0FMTFMpKTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAyIHN0YXJ0IFx1MjAxNCAke3NldHMubGVuZ3RofSBzZXRzYCk7XG5cbiAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgIGNvbnN0IGlzTGFzdFNldCAgICA9IHNldElkeCA9PT0gc2V0cy5sZW5ndGggLSAxO1xuICAgICAgY29uc3QgY3VycmVudFNldCAgID0gc2V0c1tzZXRJZHhdO1xuICAgICAgY29uc3Qgc2V0Q2h1bmtDb3VudCA9IGN1cnJlbnRTZXQucmVkdWNlKChhY2MsIGIpID0+IGFjYyArIGIubGVuZ3RoLCAwKTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9LyR7c2V0cy5sZW5ndGh9IFx1MjAxNCBlbWJlZGRpbmcgJHtjdXJyZW50U2V0Lmxlbmd0aH0gYmF0Y2ggY2FsbChzKSAoJHtzZXRDaHVua0NvdW50fSBjaHVua3MpIGluIHBhcmFsbGVsYCk7XG5cbiAgICAgIGNvbnN0IGVtYmVkUmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgICAgY3VycmVudFNldC5tYXAoYmF0Y2ggPT4gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKGJhdGNoLm1hcChjID0+IGMudGV4dCkpKVxuICAgICAgKTtcblxuICAgICAgY29uc3Qgc2V0RW1iZWRkaW5ncyA9IFtdO1xuICAgICAgZW1iZWRSZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgICAgY29uc3QgYmF0Y2ggPSBjdXJyZW50U2V0W2JhdGNoSWR4XTtcbiAgICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgICAgcmVzdWx0LnZhbHVlLmZvckVhY2goKHZlY3RvciwgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICAgIGlkOiAgICAgICAgYmF0Y2hbY2h1bmtJZHhdLm1ldGFkYXRhLmNodW5rX2lkLFxuICAgICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcixcbiAgICAgICAgICAgICAgbWV0YWRhdGE6ICBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEsXG4gICAgICAgICAgICAgIHRleHQ6ICAgICAgYmF0Y2hbY2h1bmtJZHhdLnRleHRcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IGVtYmVkZGVkIE9LICgke2JhdGNoLmxlbmd0aH0gY2h1bmtzKWApO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gRkFJTEVEOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcHJvY2Vzc2VkQ2h1bmtzICs9IHNldEVtYmVkZGluZ3MubGVuZ3RoO1xuICAgICAgYWxsRW1iZWRkaW5ncy5wdXNoKC4uLnNldEVtYmVkZGluZ3MpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0gZW1iZWRkZWQgXHUyMDE0ICR7cHJvY2Vzc2VkQ2h1bmtzfS8ke3RvdGFsQ2h1bmtzfSBjaHVua3Mgc28gZmFyYCk7XG5cbiAgICAgIGlmICghaXNMYXN0U2V0KSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTdGFydGluZyAke0dST1VQX1dBSVRfTVMgLyAxMDAwfXMgdGltZXIgKyBDaHJvbWEgd3JpdGUgY29uY3VycmVudGx5IGZvciBzZXQgJHtzZXRJZHggKyAxfWApO1xuICAgICAgICBjb25zdCB0aW1lciA9IG5ldyBQcm9taXNlKHIgPT4gc2V0VGltZW91dChyLCBHUk9VUF9XQUlUX01TKSk7XG4gICAgICAgIGNvbnN0IGNocm9tYVdyaXRlID0gYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApLnRoZW4oKCkgPT4gY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBkb25lIGZvciBzZXQgJHtzZXRJZHggKyAxfSAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKSlcbiAgICAgICAgLmNhdGNoKGVyciA9PiBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgRkFJTEVEIGZvciBzZXQgJHtzZXRJZHggKyAxfTpgLCBlcnIubWVzc2FnZSkpO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiBHUk9VUF9XQUlUX01TLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiBmYWxzZVxuICAgICAgICB9KTtcblxuICAgICAgICBhd2FpdCBQcm9taXNlLmFsbChbdGltZXIsIGNocm9tYVdyaXRlXSk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBUaW1lciArIENocm9tYSBib3RoIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9LCBwcm9jZWVkaW5nIHRvIHNldCAke3NldElkeCArIDJ9YCk7XG5cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBMYXN0IHNldCAke3NldElkeCArIDF9IFx1MjAxNCBhd2FpdGluZyBDaHJvbWEgd3JpdGUgZGlyZWN0bHlgKTtcbiAgICAgICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApO1xuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGNvbXBsZXRlIGZvciBsYXN0IHNldCAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKTtcblxuICAgICAgICBzc2VFdmVudChyZXMsICdlbWJlZGRpbmdfcHJvZ3Jlc3MnLCB7XG4gICAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzLCB0b3RhbENodW5rcyxcbiAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgIHdhaXRpbmdNczogMCwgY2hyb21hV3JpdGVDb21wbGV0ZTogdHJ1ZVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpO1xuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCwgc3RhdHVzOiAncmVhZHknXG4gICAgfSk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gXHUyNzA1IERvbmUgXHUyMDE0ICR7YWxsRW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMgaW4gQ2hyb21hIGZvciAke2NsZWFuRmlsZW5hbWV9YCk7XG5cbiAgICBzc2VFdmVudChyZXMsICdkb25lJywge1xuICAgICAgZG9jdW1lbnQ6IHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIHNlc3Npb25JZFxuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaWYgKHJlcS5maWxlICYmIGZzLmV4aXN0c1N5bmMocmVxLmZpbGUucGF0aCkpIHtcbiAgICAgIHRyeSB7IGZzLnVubGlua1N5bmMocmVxLmZpbGUucGF0aCk7IH0gY2F0Y2gge31cbiAgICB9XG4gICAgY29uc29sZS5lcnJvcignW3VwbG9hZF0gVW5oYW5kbGVkIGVycm9yOicsIGVycm9yKTtcbiAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnVXBsb2FkIGZhaWxlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1VQTE9BRF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcbiAgdHJ5IHtcbiAgICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBkb2N1bWVudHMgPSBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0xpc3QgZG9jdW1lbnRzIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJywgY29kZTogJ0xJU1RfRVJST1InIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgICBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgICAgIH1cbiAgICAgIH0gY2F0Y2ggKGNocm9tYUVycikge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtkZWxldGVdIENocm9tYSBkZWxldGUgZmFpbGVkIGZvciAke2RvY3VtZW50SWR9OmAsIGNocm9tYUVyci5tZXNzYWdlKTtcbiAgICAgIH1cblxuICAgICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpO1xuXG4gICAgICAvLyBDbGVhciBiYWNrZW5kIG1lbW9yeSBzbyBMTE0gZm9yZ2V0cyBkZWxldGVkIGRvYyBjb250ZXh0XG4gICAgICBjbGVhck1lbW9yeShzZXNzaW9uSWQpO1xuICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIENsZWFyZWQgbWVtb3J5IGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgIH1cblxuICAgIGlmIChmaWxlbmFtZSkge1xuICAgICAgY29uc3QgZmlsZVBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhmaWxlUGF0aCkpIHtcbiAgICAgICAgZnMudW5saW5rU3luYyhmaWxlUGF0aCk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbZGVsZXRlXSBSZW1vdmVkIGZpbGU6ICR7ZmlsZVBhdGh9YCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtkZWxldGVdIEZpbGUgbm90IGZvdW5kIG9uIGRpc2s6ICR7ZmlsZVBhdGh9YCk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgcmVzLmpzb24oeyBzdWNjZXNzOiB0cnVlLCBkb2N1bWVudElkIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQnLCBjb2RlOiAnREVMRVRFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRGaWxlKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuXG4gIHRyeSB7XG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCB1cGxvYWRQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmModXBsb2FkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHVwbG9hZFBhdGgpLnBpcGUocmVzKTtcbiAgICAgIH1cblxuICAgICAgY29uc3Qgc2VlZFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgZmlsZW5hbWUpO1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZFBhdGgpKSB7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihmaWxlbmFtZSkpO1xuICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShzZWVkUGF0aCkucGlwZShyZXMpO1xuICAgICAgfVxuXG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkRGlyKSkge1xuICAgICAgICBjb25zdCBhbGxQZGZzID0gZnMucmVhZGRpclN5bmMoc2VlZERpcikuZmlsdGVyKGYgPT4gZi5lbmRzV2l0aCgnLnBkZicpKTtcbiAgICAgICAgY29uc3QgbWF0Y2ggICA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKG1hdGNoKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG5yb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xucm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xucm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtpbXBvcnQgeyBmb3JtYXRNZW1vcnlGb3JQcm9tcHQgfSBmcm9tICcuL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZm9ybWF0Q29udGV4dEZvclByb21wdCwgY2FsY3VsYXRlQ292ZXJhZ2UgfSBmcm9tICcuL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCBTWVNURU1fSU5TVFJVQ1RJT04gPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgYmFzZWQgb24gaW5kZXhlZCBkb2N1bWVudHMgd2hlbiBhdmFpbGFibGUuXG5cblJVTEVTOlxuMS4gV2hlbiBjb250ZXh0IGlzIHByb3ZpZGVkLCBhbnN3ZXIgYmFzZWQgb24gaXQgYW5kIGNpdGUgc291cmNlcyB1c2luZyBbMV0sIFsyXSwgZXRjLlxuMi4gRm9yIGdlbmVyYWwgY29udmVyc2F0aW9uIChncmVldGluZ3MsIGNsYXJpZnlpbmcgcXVlc3Rpb25zLCBzbWFsbCB0YWxrKSwgcmVzcG9uZCBuYXR1cmFsbHkgYW5kIGhlbHBmdWxseSB3aXRob3V0IHJlcXVpcmluZyBjb250ZXh0LlxuMy4gSWYgYSBmYWN0dWFsIHF1ZXN0aW9uIGlzIGFza2VkIGJ1dCBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkgYW5kIHN1Z2dlc3QgdXBsb2FkaW5nIHJlbGV2YW50IGRvY3VtZW50cy5cbjQuIEJlIGNvbmNpc2UgYnV0IHRob3JvdWdoLiBVc2UgYnVsbGV0IHBvaW50cyBvciBudW1iZXJlZCBsaXN0cyBmb3IgY29tcGxleCBhbnN3ZXJzLlxuNS4gTWFpbnRhaW4gY29udmVyc2F0aW9uIGNvbnRpbnVpdHkgYnV0IGRvbid0IHJlcGVhdCBpbmZvcm1hdGlvbiB1bm5lY2Vzc2FyaWx5LlxuNi4gRm9ybWF0IHJlc3BvbnNlcyBpbiBjbGVhciwgcmVhZGFibGUgbWFya2Rvd24uYDtcblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkUHJvbXB0KHsgcXVlcnksIGNvbnRleHQsIG1lbW9yeUNvbnRleHQsIGNvdmVyYWdlIH0pIHtcbiAgY29uc3QgcGFydHMgPSBbXTtcbiAgcGFydHMucHVzaChTWVNURU1fSU5TVFJVQ1RJT04pO1xuICBpZiAobWVtb3J5Q29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChtZW1vcnlDb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgfVxuICBpZiAoY29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBSRUxFVkFOVCBDT05URVhUIEZST00gS05PV0xFREdFIEJBU0UgLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChjb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIENPTlRFWFQgLS0tXFxuJyk7XG4gIH1cbiAgcGFydHMucHVzaCgnXFxuXFxuLS0tIENVUlJFTlQgUVVFU1RJT04gLS0tXFxuJyk7XG4gIHBhcnRzLnB1c2gocXVlcnkpO1xuICBwYXJ0cy5wdXNoKCdcXG5cXG5SZW1lbWJlcjogQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIFVzZSBbMV0sIFsyXSwgZXRjLiBmb3IgY2l0YXRpb25zLiBJZiB0aGUgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5LicpO1xuICByZXR1cm4gcGFydHMuam9pbignJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFN0cmVhbWluZ1Byb21wdChxdWVyeSwgcmV0cmlldmVkUmVzdWx0cywgc2Vzc2lvbklkLCBtZW1vcnlTZXJ2aWNlKSB7XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKTtcbiAgY29uc3QgY29udGV4dFN0cmluZyA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmV0cmlldmVkUmVzdWx0cyk7XG4gIHJldHVybiBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFN0cmluZyxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiBjYWxjdWxhdGVDb3ZlcmFnZShyZXRyaWV2ZWRSZXN1bHRzKVxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxSZXNwb25zZSgpIHtcbiAgLy8gTm8gbG9uZ2VyIHVzZWQgXHUyMDE0IExMTSBnZW5lcmF0ZXMgaXRzIG93biBuYXR1cmFsIHJlZnVzYWxcbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTeXN0ZW1JbnN0cnVjdGlvbigpIHtcbiAgcmV0dXJuIFNZU1RFTV9JTlNUUlVDVElPTjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkV2ViU2VhcmNoUHJvbXB0KHF1ZXJ5LCBncm91bmRpbmdNZXRhZGF0YSkge1xuICByZXR1cm4gYEJhc2VkIG9uIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBmb2xsb3dpbmcgcXVlc3Rpb246ICR7cXVlcnl9XG5cbkd1aWRlbGluZXM6XG4tIFVzZSBpbmZvcm1hdGlvbiBmcm9tIHRoZSB3ZWIgc2VhcmNoXG4tIFByb3ZpZGUgc291cmNlcy9VUkxzIHdoZXJlIGFwcGxpY2FibGVcbi0gQmUgY29uY2lzZSBhbmQgaW5mb3JtYXRpdmVcbi0gSWYgbXVsdGlwbGUgc291cmNlcyBhZ3JlZSBvciBjb250cmFkaWN0LCBtZW50aW9uIHRoYXRgO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0R2VuZXJhdGlvbkNvbmZpZyhjdXN0b21Db25maWcgPSB7fSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgdG9wUDogMC45NSxcbiAgICB0b3BLOiA0MCxcbiAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDgsXG4gICAgLi4uY3VzdG9tQ29uZmlnXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0U291cmNlc0Zyb21SZXNwb25zZShyZXNwb25zZSkge1xuICBjb25zdCBjaXRhdGlvblBhdHRlcm4gPSAvXFxbKFxcZCspXFxdL2c7XG4gIGNvbnN0IGNpdGF0aW9ucyA9IG5ldyBTZXQoKTtcbiAgbGV0IG1hdGNoO1xuICB3aGlsZSAoKG1hdGNoID0gY2l0YXRpb25QYXR0ZXJuLmV4ZWMocmVzcG9uc2UpKSAhPT0gbnVsbCkge1xuICAgIGNpdGF0aW9ucy5hZGQocGFyc2VJbnQobWF0Y2hbMV0pKTtcbiAgfVxuICByZXR1cm4gQXJyYXkuZnJvbShjaXRhdGlvbnMpLnNvcnQoKGEsIGIpID0+IGEgLSBiKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBidWlsZFByb21wdCwgZ2V0UmVmdXNhbFJlc3BvbnNlIH0gZnJvbSAnLi9wcm9tcHRTZXJ2aWNlLmpzJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdDogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgcHJvY2Vzcy5lbnYuR0NQX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCcsXG4gICAgICBsb2NhdGlvbjogJ2dsb2JhbCdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIFBSSU1BUllfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWxOYW1lKCkge1xuICByZXR1cm4gRkFMTEJBQ0tfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tUmVzcG9uc2UocmVzdWx0KSB7XG4gIHJldHVybiByZXN1bHQ/LnRleHQgfHwgcmVzdWx0Py5yZXNwb25zZT8udGV4dD8uKCkgfHwgJyc7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tQ2h1bmsoY2h1bmspIHtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ3N0cmluZycpIHJldHVybiBjaHVuay50ZXh0O1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnZnVuY3Rpb24nKSByZXR1cm4gY2h1bmsudGV4dCgpO1xuICByZXR1cm4gJyc7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVJlc3BvbnNlKHByb21wdCkge1xuICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuICBjb25zdCB0aW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IGdldEdlbkFJKCkubW9kZWxzLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBtb2RlbDogZ2V0UHJpbWFyeU1vZGVsTmFtZSgpLFxuICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgIGNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH1cbiAgICB9LCB7XG4gICAgICBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsXG4gICAgfSk7XG5cbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICByZXR1cm4gZ2V0VGV4dEZyb21SZXNwb25zZShyZXN1bHQpO1xuICB9IGNhdGNoIChwcmltYXJ5RXJyb3IpIHtcbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICBjb25zb2xlLmVycm9yKCdQcmltYXJ5IG1vZGVsIGZhaWxlZDonLCBwcmltYXJ5RXJyb3IubWVzc2FnZSk7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgZmFsbGJhY2tSZXN1bHQgPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgICBtb2RlbDogZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKSxcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgY29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcmV0dXJuIGdldFRleHRGcm9tUmVzcG9uc2UoZmFsbGJhY2tSZXN1bHQpO1xuICAgIH0gY2F0Y2ggKGZhbGxiYWNrRXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhbGxiYWNrIG1vZGVsIGFsc28gZmFpbGVkOicsIGZhbGxiYWNrRXJyb3IubWVzc2FnZSk7XG4gICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVJlc3BvbnNlKHByb21wdCkge1xuICBsZXQgbW9kZWxOYW1lID0gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpO1xuICBsZXQgcmV0cmllcyA9IDA7XG4gIGNvbnN0IG1heFJldHJpZXMgPSAyO1xuXG4gIHdoaWxlIChyZXRyaWVzIDwgbWF4UmV0cmllcykge1xuICAgIGxldCBmaXJzdFRva2VuVGltZW91dCA9IG51bGw7XG4gICAgbGV0IHJlcXVlc3RUaW1lb3V0SWQgPSBudWxsO1xuICAgIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG5cbiAgICB0cnkge1xuICAgICAgcmVxdWVzdFRpbWVvdXRJZCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBSRVFVRVNUX1RJTUVPVVQpO1xuXG4gICAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgICBtb2RlbDogXCJnZW1pbmktMy41LWZsYXNoXCIsXG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSwge1xuICAgICAgICBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsXG4gICAgICB9KTtcblxuICAgICAgaWYgKCFyZXN1bHQ/LnN0cmVhbSB8fCB0eXBlb2YgcmVzdWx0LnN0cmVhbVtTeW1ib2wuYXN5bmNJdGVyYXRvcl0gIT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBTdHJlYW1pbmcgdW5hdmFpbGFibGUgZm9yIG1vZGVsICR7bW9kZWxOYW1lfWApO1xuICAgICAgfVxuXG4gICAgICBsZXQgZmlyc3RUb2tlbiA9IHRydWU7XG4gICAgICBmaXJzdFRva2VuVGltZW91dCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBGSVJTVF9UT0tFTl9USU1FT1VUKTtcblxuICAgICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiByZXN1bHQuc3RyZWFtKSB7XG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdTdHJlYW0gZXhlY3V0aW9uIGFib3J0ZWQgYnkgdGltZW91dCBjb25zdHJhaW50LicpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGdldFRleHRGcm9tQ2h1bmsoY2h1bmspO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuICAgICAgcmV0dXJuIHsgc3VjY2VzczogdHJ1ZSB9O1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcblxuICAgICAgaWYgKGZpcnN0VG9rZW5UaW1lb3V0KSBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgaWYgKHJlcXVlc3RUaW1lb3V0SWQpIGNsZWFyVGltZW91dChyZXF1ZXN0VGltZW91dElkKTtcblxuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIG1vZGVsTmFtZSA9IGdldEZhbGxiYWNrTW9kZWxOYW1lKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtQ2hhdFJlc3BvbnNlKHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IG1lbW9yeVNlcnZpY2UgPyBtZW1vcnlTZXJ2aWNlLmZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIDogJyc7XG4gIGNvbnN0IGNvbnRleHRMaXN0ID0gcmV0cmlldmVkUmVzdWx0cyB8fCBbXTtcbiAgY29uc3QgY29udGV4dFRleHQgPSBjb250ZXh0TGlzdC5tYXAoKHIsIGkpID0+XG4gICAgYFske2kgKyAxfV0gJHtyLm1ldGFkYXRhPy5maWxlbmFtZSB8fCAnVW5rbm93bid9OiAke3IudGV4dH1gXG4gICkuam9pbignXFxuXFxuJyk7XG5cbiAgY29uc3QgcHJvbXB0ID0gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRUZXh0LFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IHsgbGV2ZWw6ICdoaWdoJyB9XG4gIH0pO1xuXG4gIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICB0cnkge1xuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQgeyB0eXBlOiAnY29tcGxldGUnLCByZXNwb25zZTogZnVsbFJlc3BvbnNlIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsVGV4dCgpIHtcbiAgcmV0dXJuIGdldFJlZnVzYWxSZXNwb25zZSgpO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVXZWJTZWFyY2hSZXNwb25zZShxdWVyeSwgZ3JvdW5kaW5nQ29udGVudCkge1xuICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgIG1vZGVsOiBnZXRQcmltYXJ5TW9kZWxOYW1lKCksXG4gICAgY29udGVudHM6IFt7XG4gICAgICByb2xlOiAndXNlcicsXG4gICAgICBwYXJ0czogW3sgdGV4dDogYEJhc2VkIG9uIHRoZXNlIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBxdWVzdGlvbjogXCIke3F1ZXJ5fVwiXFxuXFxuJHtncm91bmRpbmdDb250ZW50fWAgfV1cbiAgICB9XSxcbiAgICBjb25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9XG4gIH0pO1xuXG4gIGNvbnN0IHRleHQgPSBnZXRUZXh0RnJvbVJlc3BvbnNlKHJlc3VsdCk7XG4gIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzdWx0Py5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gIHJldHVybiB7XG4gICAgdGV4dCxcbiAgICBncm91bmRpbmdNZXRhZGF0YSxcbiAgICBncm91bmRpbmdDaHVua3M6IGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MgfHwgW11cbiAgfTtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgcmV0cmlldmVGb3JRdWVyeSwgZ2VuZXJhdGVDaXRhdGlvbnMsIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlIH0gZnJvbSAnLi4vc2VydmljZXMvZ2VtaW5pU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgZ2V0UmVjZW50VHVybnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgZ2V0RGVsZXRlZERvY3VtZW50SWRzIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgT1VUX09GX1NDT1BFX1BBVFRFUk4gPSAvZG9uJ3QgaGF2ZSBpbmZvcm1hdGlvbnxkbyBub3QgaGF2ZSBpbmZvcm1hdGlvbnxub3QgaW4gbXkga25vd2xlZGdlfGNhbid0IGZpbmR8Y2Fubm90IGZpbmR8bm8gaW5mb3JtYXRpb258a25vd2xlZGdlIGJhc2UgZG9lc24ndHxub3QgY292ZXJlZHxvdXRzaWRlLiprbm93bGVkZ2UvaTtcblxuZnVuY3Rpb24gY2xlYW5FeGNlcnB0KHRleHQpIHtcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvKD88IVxcdykoW0EtWmEtel0pXFxzKFtBLVphLXpdKVxccyhbQS1aYS16XSkoXFxzW0EtWmEtel0pKi9nLCAobWF0Y2gpID0+XG4gICAgICBtYXRjaC5yZXBsYWNlKC9cXHMvZywgJycpXG4gICAgKVxuICAgIC5yZXBsYWNlKC9cXHN7Mix9L2csICcgJylcbiAgICAucmVwbGFjZSgvXlxcKlxccyovLCAnJylcbiAgICAudHJpbSgpO1xufVxuXG4vLyBJc3N1ZSA0IGZpeDogcmVtb3ZlIGRvbWFpbkhpbnQgXHUyMDE0IHNob3J0IHF1ZXJpZXMgbm8gbG9uZ2VyIGluaGVyaXQgcHJldmlvdXMgY29udmVyc2F0aW9uIGNvbnRleHRcbmZ1bmN0aW9uIGV4cGFuZFF1ZXJ5KHF1ZXJ5KSB7XG4gIGNvbnN0IHdvcmRzID0gcXVlcnkudHJpbSgpLnNwbGl0KC9cXHMrLyk7XG4gIGlmICh3b3Jkcy5sZW5ndGggPiA0KSByZXR1cm4gcXVlcnk7XG5cbiAgY29uc3QgZXhwYW5zaW9ucyA9IFtcbiAgICAnZGVmaW5pdGlvbicsICdvdmVydmlldycsICdyb2xlJywgJ3Jlc3BvbnNpYmlsaXRpZXMnLFxuICAgICdleGFtcGxlcycsICdrZXkgY29uY2VwdHMnLCAnaG93IGl0IHdvcmtzJywgJ3B1cnBvc2UnXG4gIF07XG5cbiAgcmV0dXJuIGAke3F1ZXJ5fSAke2V4cGFuc2lvbnMuam9pbignICcpfWA7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQsIGNvbnZJZDogcHJvdmlkZWRDb252SWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsIGNvZGU6ICdNSVNTSU5HX1FVRVJZJyB9KTtcbiAgfVxuXG4gIGNvbnN0IHNlc3Npb25JZCA9IHByb3ZpZGVkU2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBjb252SWQgICAgPSBwcm92aWRlZENvbnZJZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgID0gdXVpZHY0KCk7XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ3VzZXInLCBxdWVyeS50cmltKCkpO1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgZXhwYW5kZWRRdWVyeSA9IGV4cGFuZFF1ZXJ5KHF1ZXJ5KTtcbiAgICBjb25zdCB7IHJlc3VsdHMsIGNvdmVyYWdlIH0gPSBhd2FpdCByZXRyaWV2ZUZvclF1ZXJ5KGV4cGFuZGVkUXVlcnksIHNlc3Npb25JZCwgeyB0b3BLOiA1IH0pO1xuXG4gICAgc2VuZEV2ZW50KCdyZXRyaWV2YWwnLCB7XG4gICAgICByZXN1bHRzOiByZXN1bHRzLmxlbmd0aCxcbiAgICAgIGxldmVsOiBjb3ZlcmFnZS5sZXZlbCxcbiAgICAgIHNjb3JlOiBjb3ZlcmFnZS5zY29yZSxcbiAgICAgIHRvcFNjb3JlOiBjb3ZlcmFnZS50b3BTY29yZVxuICAgIH0pO1xuXG4gICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgY29uc3Qgc291cmNlcyA9IHJlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgZmlsZW5hbWU6IHIubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgZXhjZXJwdDogY2xlYW5FeGNlcnB0KHIudGV4dC5zbGljZSgwLCAyMDApKSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICAvLyBHZXQgZGVsZXRlZCBkb2MgSURzIGZvciB0aGlzIHNlc3Npb24gdG8gZmlsdGVyIHN0YWxlIG1lbW9yeSB0dXJuc1xuICAgIGNvbnN0IGRlbGV0ZWREb2NJZHMgPSBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGFsbFJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoY29udklkLCAxMCk7XG5cbiAgICAvLyBGaWx0ZXIgb3V0IGFzc2lzdGFudCB0dXJucyAoYW5kIHRoZWlyIHByZWNlZGluZyB1c2VyIHR1cm5zKSB0aGF0IGNpdGVkIGRlbGV0ZWQgZG9jc1xuICAgIGNvbnN0IGZpbHRlcmVkVHVybnMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbFJlY2VudFR1cm5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCB0dXJuID0gYWxsUmVjZW50VHVybnNbaV07XG4gICAgICBpZiAodHVybi5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgICBjb25zdCBjaXRlc0RlbGV0ZWREb2MgPSB0dXJuLmNpdGF0aW9ucz8uc29tZShjID0+IGRlbGV0ZWREb2NJZHMuaGFzKGMuZG9jdW1lbnRJZCkpO1xuICAgICAgICBpZiAoY2l0ZXNEZWxldGVkRG9jKSB7XG4gICAgICAgICAgLy8gQWxzbyByZW1vdmUgdGhlIHByZWNlZGluZyB1c2VyIHR1cm4gaWYgaXQncyB0aGUgb25lIHRoYXQgcHJvbXB0ZWQgdGhpcyBhbnN3ZXJcbiAgICAgICAgICBpZiAoZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwICYmIGZpbHRlcmVkVHVybnNbZmlsdGVyZWRUdXJucy5sZW5ndGggLSAxXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgICAgICAgIGZpbHRlcmVkVHVybnMucG9wKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnRpbnVlOyAvLyBza2lwIHRoaXMgYXNzaXN0YW50IHR1cm5cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZmlsdGVyZWRUdXJucy5wdXNoKHR1cm4pO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpO1xuICAgIGNvbnN0IGFuc3dlcnMgICA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAnYXNzaXN0YW50Jyk7XG4gICAgY29uc3QgcVNlY3Rpb24gID0gcXVlc3Rpb25zLm1hcCgodCwgaSkgPT4gYFEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgYVNlY3Rpb24gID0gYW5zd2Vycy5tYXAoKHQsIGkpID0+IGBBJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDBcbiAgICAgID8gYFByZXZpb3VzIFF1ZXN0aW9uczpcXG4ke3FTZWN0aW9ufVxcblxcblByZXZpb3VzIEFuc3dlcnM6XFxuJHthU2VjdGlvbn1gXG4gICAgICA6ICcnO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gWW91ciBiZWhhdmlvdXIgZGVwZW5kcyBvbiB0aGUgdHlwZSBvZiBpbnB1dDpcblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMSyAoaGksIGhlbGxvLCBob3cgYXJlIHlvdSwgZG8geW91IGhhdmUgYSBsaWZlLCBqb2tlcywgZ2VuZXJhbCBjaGF0KTpcbiAgIC0gUmVzcG9uZCB3YXJtbHkgYW5kIG5hdHVyYWxseS4gRG8gTk9UIG1lbnRpb24gdGhlIGtub3dsZWRnZSBiYXNlIG9yIGRvY3VtZW50cyBhdCBhbGwuXG4gICAtIERvIE5PVCBhZGQgYW55IGNpdGF0aW9ucy5cblxuMi4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSCBDT05URVhUIChjb250ZXh0IGJlbG93IGlzIHJlbGV2YW50KTpcbiAgIC0gQW5zd2VyIHN0cmljdGx5IHVzaW5nIHRoZSBudW1iZXJlZCBjb250ZXh0IHByb3ZpZGVkLlxuICAgLSBDaXRlIHNvdXJjZXMgaW5saW5lIGFzIFsxXSBbMl0gXHUyMDE0IGFsd2F5cyBzZXBhcmF0ZSBicmFja2V0cywgbmV2ZXIgWzEsIDJdLlxuICAgLSBPbmx5IGNpdGUgbnVtYmVycyB5b3UgYWN0dWFsbHkgdXNlZC5cblxuMy4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSE9VVCBDT05URVhUIChjb250ZXh0IGlzIGVtcHR5IG9yIGlycmVsZXZhbnQpOlxuICAgLSBQb2xpdGVseSBkZWNsaW5lIGluIHlvdXIgb3duIHdvcmRzIFx1MjAxNCB2YXJ5IHlvdXIgcGhyYXNpbmcgbmF0dXJhbGx5LlxuICAgLSBEbyBOT1QgYWRkIGNpdGF0aW9ucy5cbiAgIC0gRG8gTk9UIHVzZSBhIGZpeGVkIHRlbXBsYXRlIG9yIHJvYm90aWMgcmVzcG9uc2UuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0IHx8ICcoTm8gcmVsZXZhbnQgZG9jdW1lbnRzIGZvdW5kIGluIGtub3dsZWRnZSBiYXNlKSd9XG5cbkNPTlZFUlNBVElPTiBISVNUT1JZOlxuJHttZW1vcnlDb250ZXh0IHx8ICcoTm8gcHJldmlvdXMgY29udmVyc2F0aW9uKSd9XG5cbkNVUlJFTlQgUVVFU1RJT046ICR7cXVlcnl9YDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGNpdGVkSW5kaWNlcyA9IFtdO1xuICAgIGNvbnN0IHNlZW4gPSBuZXcgU2V0KCk7XG4gICAgZm9yIChjb25zdCBtYXRjaCBvZiBmdWxsUmVzcG9uc2UubWF0Y2hBbGwoL1xcWyhcXGQrKVxcXS9nKSkge1xuICAgICAgY29uc3QgbnVtID0gcGFyc2VJbnQobWF0Y2hbMV0pO1xuICAgICAgaWYgKCFzZWVuLmhhcyhudW0pKSB7XG4gICAgICAgIHNlZW4uYWRkKG51bSk7XG4gICAgICAgIGNpdGVkSW5kaWNlcy5wdXNoKG51bSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgaXNPdXRPZlNjb3BlID0gT1VUX09GX1NDT1BFX1BBVFRFUk4udGVzdChmdWxsUmVzcG9uc2UpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENpdGF0aW9ucyA9IGNpdGF0aW9ucy5maWx0ZXIoYyA9PiBjaXRlZEluZGljZXMuaW5jbHVkZXMoYy5pbmRleCkpO1xuXG4gICAgY29uc3QgaW5kZXhNYXAgPSBuZXcgTWFwKCk7XG4gICAgY2l0ZWRJbmRpY2VzLmZvckVhY2goKG9sZElkeCwgaSkgPT4ge1xuICAgICAgaW5kZXhNYXAuc2V0KG9sZElkeCwgaSArIDEpO1xuICAgIH0pO1xuXG4gICAgY29uc3QgcmV3cml0dGVuUmVzcG9uc2UgPSBmdWxsUmVzcG9uc2UucmVwbGFjZSgvXFxbKFxcZCspXFxdL2csIChtYXRjaCwgbnVtKSA9PiB7XG4gICAgICBjb25zdCBuZXdJZHggPSBpbmRleE1hcC5nZXQocGFyc2VJbnQobnVtKSk7XG4gICAgICByZXR1cm4gbmV3SWR4ICE9PSB1bmRlZmluZWQgPyBgWyR7bmV3SWR4fV1gIDogbWF0Y2g7XG4gICAgfSk7XG5cbiAgICBjb25zdCBmaW5hbENpdGF0aW9ucyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IG1hdGNoZWRDaXRhdGlvbnNcbiAgICAgICAgICAubWFwKGMgPT4gKHsgLi4uYywgaW5kZXg6IGluZGV4TWFwLmdldChjLmluZGV4KSB9KSlcbiAgICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiBhLmluZGV4IC0gYi5pbmRleCk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2h1bmtJZHMgPSBuZXcgU2V0KG1hdGNoZWRDaXRhdGlvbnMubWFwKGMgPT4gYy5jaHVua0lkKSk7XG5cbiAgICBjb25zdCBmaW5hbFNvdXJjZXMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBzb3VyY2VzXG4gICAgICAgICAgLmZpbHRlcihzID0+IG1hdGNoZWRDaHVua0lkcy5oYXMocy5jaHVua0lkKSlcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4ge1xuICAgICAgICAgICAgY29uc3QgaWR4QSA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGEuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgY29uc3QgaWR4QiA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGIuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgcmV0dXJuIGlkeEEgLSBpZHhCO1xuICAgICAgICAgIH0pO1xuXG4gICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAnYXNzaXN0YW50JywgcmV3cml0dGVuUmVzcG9uc2UsIGZpbmFsQ2l0YXRpb25zLCBjb3ZlcmFnZSwgYW5zd2VySWQpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIGFuc3dlcklkLFxuICAgICAgcmVzcG9uc2U6IHJld3JpdHRlblJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zOiBmaW5hbENpdGF0aW9ucyxcbiAgICAgIGNvdmVyYWdlLFxuICAgICAgc291cmNlczogZmluYWxTb3VyY2VzXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnQW4gZXJyb3Igb2NjdXJyZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNvdXJjZXMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDIwKTtcblxuICBjb25zdCBleGFjdE1hdGNoID0gcmVjZW50VHVybnMuZmluZCh0ID0+IHQuaWQgPT09IGFuc3dlcklkKTtcbiAgaWYgKGV4YWN0TWF0Y2g/LmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGV4YWN0TWF0Y2guY2l0YXRpb25zIH0pO1xuICB9XG5cbiAgY29uc3QgZmFsbGJhY2sgPSBbLi4ucmVjZW50VHVybnNdLnJldmVyc2UoKS5maW5kKHQgPT5cbiAgICB0LnJvbGUgPT09ICdhc3Npc3RhbnQnICYmIHQuY2l0YXRpb25zPy5sZW5ndGggPiAwXG4gICk7XG5cbiAgaWYgKGZhbGxiYWNrKSByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG5cbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kJywgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJyB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuY29uc3QgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuZXJhdGl2ZUFJKHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZKTtcblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuXG5sZXQgbW9kZWwgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRNb2RlbCgpIHtcbiAgaWYgKCFtb2RlbCkge1xuICAgIG1vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIG1vZGVsO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcGVyZm9ybVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgY29uc3QgcmVzcG9uc2UgPSByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICAvLyBFeHRyYWN0IHNlYXJjaCBxdWVyaWVzIGFuZCBzb3VyY2VzXG4gICAgY29uc3Qgd2ViU2VhcmNoUXVlcmllcyA9IFtdO1xuICAgIGNvbnN0IHdlYlNvdXJjZXMgPSBbXTtcblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGNodW5rIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoY2h1bmsud2ViKSB7XG4gICAgICAgICAgd2ViU291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogY2h1bmsud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBjaHVuay53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8ud2ViU2VhcmNoUXVlcmllcykge1xuICAgICAgd2ViU2VhcmNoUXVlcmllcy5wdXNoKC4uLmdyb3VuZGluZ01ldGFkYXRhLndlYlNlYXJjaFF1ZXJpZXMpO1xuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICB0ZXh0LFxuICAgICAgc291cmNlczogd2ViU291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHdlYlNlYXJjaFF1ZXJpZXMsXG4gICAgICByYXdNZXRhZGF0YTogZ3JvdW5kaW5nTWV0YWRhdGFcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSB0ZXh0O1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlPy5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgY29uc3Qgc291cmNlcyA9IFtdO1xuICAgIGlmIChncm91bmRpbmdNZXRhZGF0YT8uZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChpdGVtLndlYikge1xuICAgICAgICAgIHNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGl0ZW0ud2ViLnVyaSxcbiAgICAgICAgICAgIHRpdGxlOiBpdGVtLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQge1xuICAgICAgdHlwZTogJ2NvbXBsZXRlJyxcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbWluZyBlcnJvcjonLCBlcnJvcik7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgIHRocm93IG5ldyBXZWJTZWFyY2hVbmF2YWlsYWJsZUVycm9yKCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdFdlYlNlYXJjaFJlc3BvbnNlKHJlc3VsdCkge1xuICByZXR1cm4ge1xuICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMubWFwKHMgPT4gKHtcbiAgICAgIHVyaTogcy51cmksXG4gICAgICB0aXRsZTogcy50aXRsZSxcbiAgICAgIHR5cGU6ICd3ZWInXG4gICAgfSkpLFxuICAgIHF1ZXJpZXNVc2VkOiByZXN1bHQucXVlcmllcyxcbiAgICBtZXRhZGF0YToge1xuICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9XG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBwZXJmb3JtV2ViU2VhcmNoLCBzdHJlYW1XZWJTZWFyY2ggfSBmcm9tICcuLi9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2gocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgcGVyZm9ybVdlYlNlYXJjaChxdWVyeS50cmltKCkpO1xuXG4gICAgcmVzLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGFuc3dlcjogcmVzdWx0LnRleHQsXG4gICAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcyxcbiAgICAgIHF1ZXJpZXM6IHJlc3VsdC5xdWVyaWVzLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgcGVyZm9ybWVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgICAgfVxuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDMpLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggdW5hdmFpbGFibGUnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoU3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICAvLyBTZXQgdXAgU1NFXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnc2VhcmNoaW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyB0aGUgd2ViLi4uJyB9KTtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcbiAgICBsZXQgc291cmNlcyA9IFtdO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1XZWJTZWFyY2gocXVlcnkudHJpbSgpKSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdXRUJfU0VBUkNIX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgICAgc291cmNlcyA9IGNodW5rLnNvdXJjZXMgfHwgW107XG4gICAgICB9XG4gICAgfVxuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UsXG4gICAgICBzb3VyY2VzLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywge1xuICAgICAgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCBmYWlsZWQnLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnV0VCX1NFQVJDSF9FUlJPUidcbiAgICB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVXZWJTZWFyY2gpO1xucm91dGVyLnBvc3QoJy9zdHJlYW0nLCBoYW5kbGVXZWJTZWFyY2hTdHJlYW0pO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHNlYXJjaFJvdXRlciBmcm9tICcuL2FwaS9zZWFyY2guanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzIH0gZnJvbSAnLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgY2xlYXJNZW1vcnkgfSBmcm9tICcuL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBJTklUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vaW5pdCcsIGFzeW5jIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ107XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ01pc3NpbmcgeC1zZXNzaW9uLWlkIGhlYWRlcicsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICB9XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgdHJ5IHtcbiAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogdHJ1ZSwgc2Vzc2lvbklkIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ1Nlc3Npb24gaW5pdCB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiBmYWxzZSwgc2Vzc2lvbklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gUkVTVE9SRSBNRU1PUlkgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9yZXN0b3JlLW1lbW9yeScsIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCB7IGNvbnZJZCwgbWVzc2FnZXMgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghY29udklkIHx8ICFBcnJheS5pc0FycmF5KG1lc3NhZ2VzKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnY29udklkIGFuZCBtZXNzYWdlcyBhcmUgcmVxdWlyZWQnLCBjb2RlOiAnQkFEX1JFUVVFU1QnIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICAvLyBBbHdheXMgd2lwZSB0aGUgY29udklkIG1lbW9yeSBmaXJzdCBzbyByZXBsYXlpbmcgbmV2ZXIgZG91YmxlcyB1cCB0dXJuc1xuICAgIGNsZWFyTWVtb3J5KGNvbnZJZCk7XG5cbiAgICBmb3IgKGNvbnN0IG1zZyBvZiBtZXNzYWdlcykge1xuICAgICAgaWYgKChtc2cucm9sZSA9PT0gJ3VzZXInIHx8IG1zZy5yb2xlID09PSAnYXNzaXN0YW50JykgJiYgdHlwZW9mIG1zZy5jb250ZW50ID09PSAnc3RyaW5nJykge1xuICAgICAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsIG1zZy5yb2xlLCBtc2cuY29udGVudCk7XG4gICAgICB9XG4gICAgfVxuICAgIHJlcy5qc29uKHsgb2s6IHRydWUsIGNvbnZJZCwgcmVzdG9yZWQ6IG1lc3NhZ2VzLmxlbmd0aCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdNZW1vcnkgcmVzdG9yZSB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IG9rOiBmYWxzZSwgY29udklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuYXBwLnVzZSgnL3NlYXJjaCcsIHNlYXJjaFJvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxtQkFBbUI7QUFDelMsU0FBUyxNQUFNLGNBQWM7QUFRN0IsU0FBUyxpQkFBaUI7QUFDeEIsTUFBSSxDQUFDLGFBQWE7QUFDaEIsVUFBTSxTQUFTLFFBQVEsSUFBSTtBQUMzQixVQUFNLFNBQVMsUUFBUSxJQUFJLGlCQUFpQjtBQUM1QyxVQUFNLFdBQVcsUUFBUSxJQUFJLG1CQUFtQjtBQUNoRCxVQUFNLE9BQU8sUUFBUSxJQUFJLGVBQWU7QUFFeEMsWUFBUSxJQUFJLHFDQUFxQztBQUNqRCxZQUFRLElBQUksZUFBZSxRQUFRLDZCQUE2QjtBQUNoRSxZQUFRLElBQUksZUFBZSxNQUFNO0FBQ2pDLFlBQVEsSUFBSSxlQUFlLFFBQVE7QUFDbkMsWUFBUSxJQUFJLGVBQWUsU0FBUyxtQkFBbUIscUJBQXFCO0FBQzVFLFlBQVEsSUFBSSxxQ0FBcUM7QUFFakQsUUFBSSxDQUFDLFFBQVE7QUFDWCxZQUFNLElBQUk7QUFBQSxRQUNSO0FBQUEsTUFFRjtBQUFBLElBQ0Y7QUFFQSxVQUFNLGdCQUFnQixFQUFFLFFBQVEsUUFBUSxTQUFTO0FBQ2pELFFBQUksS0FBTSxlQUFjLE9BQU87QUFDL0Isa0JBQWMsSUFBSSxZQUFZLGFBQWE7QUFBQSxFQUM3QztBQUNBLFNBQU87QUFD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Z0JBQUM7QUFBQTtBQUFBLGNBQVk7QUFBQSxVQUM1QjtBQUFBLFFBQ0osQ0FBQztBQUFBLE1BQ0wsQ0FBQztBQUFBLElBQ0w7QUFBQSxFQUNKO0FBQ0o7QUFDQSxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUN4QixTQUFTLENBQUMsTUFBTSxHQUFHLGNBQWMsQ0FBQztBQUFBLEVBQ2xDLFNBQVM7QUFBQSxJQUNMLE9BQU87QUFBQSxNQUNILEtBQUtGLE1BQUssUUFBUUQsWUFBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbIm1vZGVsIiwgIkJBVENIX1NJWkUiLCAidXVpZHY0IiwgImdsb2JhbENvbGxlY3Rpb24iLCAic2Vzc2lvbiIsICJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJQQVJBTExFTF9DQUxMUyIsICJHUk9VUF9XQUlUX01TIiwgInJvdXRlciIsICJHb29nbGVHZW5BSSIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJnZW5BSSIsICJQUklNQVJZX01PREVMIiwgIm1vZGVsIiwgIlJvdXRlciIsICJyb3V0ZXIiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgIl9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwiLCAiX19kaXJuYW1lIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJhcHAiXQp9Cg==
