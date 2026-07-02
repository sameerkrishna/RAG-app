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
var AppError, ValidationError, InvalidFileTypeError, CorruptedPDFError, LLMUnavailableError, EmbeddingError;
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
    const model = getEmbeddingModel();
    const embeddingPromises = texts.map(async (rawText) => {
      const text = typeof rawText === "string" ? rawText : String(rawText);
      if (!text || text.trim() === "") {
        throw new EmbeddingError("Cannot embed an empty or missing text block");
      }
      const response = await model.embedContent({
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
  const targetTokens = options.chunkSizeTokens || TARGET_CHUNK_TOKENS;
  const maxTokens = options.maxChunkTokens || MAX_CHUNK_TOKENS;
  const overlapTk = options.overlapTokens || OVERLAP_TOKENS;
  const targetChars = targetTokens * CHARS_PER_TOKEN;
  const maxChars = maxTokens * CHARS_PER_TOKEN;
  const overlapChars = overlapTk * CHARS_PER_TOKEN;
  if (!text || typeof text !== "string") return [];
  const rawParas = text.split(/\n{2,}/).map((p) => p.trim()).filter((p) => p.length >= MIN_CHUNK_CHARS);
  const chunks = [];
  let buffer = "";
  let bufStart = 0;
  let chunkIndex = 0;
  let charCursor = 0;
  const flush = (forceText) => {
    const content = (forceText ?? buffer).trim();
    if (content.length >= MIN_CHUNK_CHARS) {
      chunks.push({
        text: content,
        tokenCount: estimateTokens(content),
        charStart: bufStart,
        charEnd: bufStart + content.length,
        chunkIndex: chunkIndex++
      });
    }
    buffer = "";
    bufStart = charCursor;
  };
  for (const para of rawParas) {
    const isHeading = HEADING_RE.test(para.split("\n")[0]);
    if (isHeading && buffer.length > 0) flush();
    if (para.length > maxChars) {
      if (buffer.length > 0) flush();
      let s = 0;
      while (s < para.length) {
        let e = s + targetChars;
        if (e < para.length) {
          const searchFrom = e - Math.floor(targetChars * 0.2);
          for (const bp of [". ", ".\n", "? ", "! ", "\n"]) {
            const idx = para.lastIndexOf(bp, e);
            if (idx > searchFrom) {
              e = idx + bp.length;
              break;
            }
          }
        }
        e = Math.min(e, para.length);
        const slice = para.slice(s, e).trim();
        if (slice.length >= MIN_CHUNK_CHARS) {
          chunks.push({
            text: slice,
            tokenCount: estimateTokens(slice),
            charStart: charCursor + s,
            charEnd: charCursor + e,
            chunkIndex: chunkIndex++
          });
        }
        const next = e - overlapChars;
        s = next > s ? next : e;
      }
      charCursor += para.length + 2;
      bufStart = charCursor;
      continue;
    }
    if (buffer.length > 0 && buffer.length + para.length + 2 > maxChars) {
      flush();
    }
    buffer = buffer ? buffer + "\n\n" + para : para;
    charCursor += para.length + 2;
    if (buffer.length >= targetChars) {
      flush();
    }
  }
  flush();
  return chunks;
}
var CHARS_PER_TOKEN, TARGET_CHUNK_TOKENS, MAX_CHUNK_TOKENS, OVERLAP_TOKENS, MIN_CHUNK_CHARS, HEADING_RE;
var init_chunker = __esm({
  "server/utils/chunker.js"() {
    "use strict";
    CHARS_PER_TOKEN = 4;
    TARGET_CHUNK_TOKENS = 600;
    MAX_CHUNK_TOKENS = 750;
    OVERLAP_TOKENS = 100;
    MIN_CHUNK_CHARS = 100;
    HEADING_RE = /^(?:[A-Z][A-Z\s]{2,60}$|#{1,4}\s.+|(?:\d+\.)+\s.+)/m;
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
    const rawChunks = chunkText(fullText);
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
      project: process.env.GOOGLE_CLOUD_PROJECT || "project-d48e2f39-2685-4746-aa0",
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
function buildGenerationRequest(model, prompt) {
  return {
    model,
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    config: {
      temperature: 0.7,
      topP: 0.95,
      maxOutputTokens: 2048
    }
  };
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
      const responseStream = await getGenAI().models.generateContentStream(
        buildGenerationRequest(modelName, prompt),
        { signal: controller.signal }
      );
      if (!responseStream || typeof responseStream[Symbol.asyncIterator] !== "function") {
        throw new Error(`Streaming unavailable for model ${modelName}`);
      }
      let firstToken = true;
      firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);
      for await (const chunk of responseStream) {
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
      return;
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcbiAgICBjbG91ZENsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xvdWRDbGllbnQ7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBHbG9iYWwgY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGNvbm5lY3QgdG8gZ2xvYmFsIGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG4gIHJldHVybiBnbG9iYWxDb2xsZWN0aW9uO1xufVxuXG4vKipcbiAqIFJldHVybnMgeyBjb2xsZWN0aW9uLCBpc05ldyB9LlxuICogaXNOZXcgPSB0cnVlICBcdTIxOTIgZnJlc2hseSBjcmVhdGVkLCBuZWVkcyBzZWVkaW5nIGZyb20gZ2xvYmFsLlxuICogaXNOZXcgPSBmYWxzZSBcdTIxOTIgYWxyZWFkeSBleGlzdGVkIG9uIENocm9tYSBDbG91ZCwgcmVzcGVjdCBpdHMgY3VycmVudCBzdGF0ZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHsgY29sbGVjdGlvbjogc2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpLCBpc05ldzogZmFsc2UgfTtcbiAgfVxuXG4gIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcblxuICBsZXQgY29sbGVjdGlvbjtcbiAgbGV0IGlzTmV3O1xuXG4gIHRyeSB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICB9KTtcbiAgICBpc05ldyA9IGZhbHNlO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNjdiXFx1ZmUwZiAgU2Vzc2lvbiBjb2xsZWN0aW9uIGV4aXN0cywgcmV1c2luZzogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfSBjYXRjaCB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5jcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIGlzTmV3ID0gdHJ1ZTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY3JlYXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfVxuXG4gIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgcmV0dXJuIHsgY29sbGVjdGlvbiwgaXNOZXcgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gZGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbi8qKlxuICogQWRkIHZlY3RvcnMgaW4gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIHRvIGF2b2lkIENocm9tYSBwYXlsb2FkIGxpbWl0cy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBpZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGNvbnN0IGJhdGNoSWRzICAgICAgICA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzICA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyAgPSB2ZWN0b3JzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAodiA9PiB2Lm1ldGFkYXRhKTtcblxuICAgICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6ICAgICAgICBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6ICBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiAgYmF0Y2hNZXRhZGF0YXNcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgW2FkZFZlY3RvcnNdIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogYWRkZWQgJHtiYXRjaElkcy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPiBkZWZhdWx0IDEwMCBsaW1pdCkgYXJlIGZ1bGx5IGRlbGV0ZWQuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9LFxuICAgICAgICBpbmNsdWRlOiBbXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGFsbElkcyB9KTtcbiAgICB9XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG4vKipcbiAqIExpc3QgYWxsIHVuaXF1ZSBkb2N1bWVudHMgaW4gYSBjb2xsZWN0aW9uLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgd2l0aCBCQVRDSF9TSVpFPTMwMCBzbyBjb2xsZWN0aW9ucyBsYXJnZXJcbiAqIHRoYW4gQ2hyb21hJ3MgZGVmYXVsdCBnZXQoKSBsaW1pdCAoMTAwKSBhcmUgZnVsbHkgZW51bWVyYXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcblxuICAgICAgYmF0Y2guaWRzLmZvckVhY2goKGlkLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgbWV0YSAgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2NJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiAgICAgICAgIG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogICAgICAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogICAgICAgbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6ICAgICAgbWV0YS5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIGZpcnN0X2NodW5rX3RleHQ6IGJhdGNoLmRvY3VtZW50c1tpZHhdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBkb2MgPSBkb2N1bWVudHNNYXAuZ2V0KGRvY0lkKTtcbiAgICAgICAgZG9jLmNodW5rX2NvdW50Kys7XG4gICAgICAgIGRvYy5wYWdlX2NvdW50ID0gTWF0aC5tYXgoZG9jLnBhZ2VfY291bnQsIG1ldGEucGFnZV9udW1iZXIgfHwgMSk7XG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYCAgW2xpc3REb2N1bWVudHNdIG9mZnNldD0ke29mZnNldH0sIGdvdD0ke2JhdGNoLmlkcy5sZW5ndGh9LCB1bmlxdWUgc28gZmFyPSR7ZG9jdW1lbnRzTWFwLnNpemV9YCk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShkb2N1bWVudHNNYXAudmFsdWVzKCkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50czonLCBlcnJvcik7XG4gICAgcmV0dXJuIFtdO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGhDaGVjaygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY2xlYW51cFNlc3Npb25Db2xsZWN0aW9ucygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25zID0gYXdhaXQgY2xpZW50Lmxpc3RDb2xsZWN0aW9ucygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcXHUyNzA1IE5vIHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbnMgZm91bmQuJyk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFxcdWQ4M2VcXHVkZGY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFxcdTI3MDUgRGVsZXRlZDogJHtuYW1lfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYCAgXFx1MjZhMFxcdWZlMGYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1xcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGNsZWFudXAgY29tcGxldGUuJyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS53YXJuKCdcXHUyNmEwXFx1ZmUwZiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7ZXhwb3J0IGNsYXNzIEFwcEVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlLCBzdGF0dXNDb2RlID0gNTAwKSB7XG4gICAgc3VwZXIobWVzc2FnZSk7XG4gICAgdGhpcy5jb2RlID0gY29kZTtcbiAgICB0aGlzLnN0YXR1c0NvZGUgPSBzdGF0dXNDb2RlO1xuICAgIHRoaXMuaXNPcGVyYXRpb25hbCA9IHRydWU7XG4gICAgRXJyb3IuY2FwdHVyZVN0YWNrVHJhY2UodGhpcywgdGhpcy5jb25zdHJ1Y3Rvcik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFZhbGlkYXRpb25FcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdWQUxJREFUSU9OX0VSUk9SJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFVwbG9hZExpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVVBMT0FEX0xJTUlUX0VYQ0VFREVEJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEZpbGVUb29MYXJnZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXhTaXplTUIpIHtcbiAgICBzdXBlcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmAsICdGSUxFX1RPT19MQVJHRScsIDQxMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEludmFsaWRGaWxlVHlwZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignT25seSBQREYgZmlsZXMgYXJlIGFsbG93ZWQnLCAnSU5WQUxJRF9GSUxFX1RZUEUnLCA0MTUpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBUb29NYW55UERGc0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXgpIHtcbiAgICBzdXBlcihgTWF4aW11bSAke21heH0gUERGcyBhbGxvd2VkIHBlciBzZXNzaW9uYCwgJ1RPT19NQU5ZX1BERlMnLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBEdXBsaWNhdGVGaWxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKGZpbGVuYW1lKSB7XG4gICAgc3VwZXIoYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmAsICdEVVBMSUNBVEVfRklMRScsIDQwOSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvcnJ1cHRlZFBERkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRmFpbGVkIHRvIHBhcnNlIFBERiBmaWxlLiBJdCBtYXkgYmUgY29ycnVwdGVkLicsICdDT1JSVVBURURfUERGJywgNDIyKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmF0ZUxpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKHJldHJ5QWZ0ZXIgPSA2MCkge1xuICAgIHN1cGVyKCdSYXRlIGxpbWl0IGV4Y2VlZGVkLiBQbGVhc2UgdHJ5IGFnYWluIGxhdGVyLicsICdSQVRFX0xJTUlUX0VYQ0VFREVEJywgNDI5KTtcbiAgICB0aGlzLnJldHJ5QWZ0ZXIgPSByZXRyeUFmdGVyO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBMTE1VbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignQUkgc2VydmljZSBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZS4gUGxlYXNlIHRyeSBhZ2Fpbi4nLCAnTExNX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRW1iZWRkaW5nRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UgPSAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgJ0VNQkVERElOR19FUlJPUicsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJldHJpZXZhbFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdEb2N1bWVudCByZXRyaWV2YWwgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnUkVUUklFVkFMX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ292ZXJhZ2VUb29Mb3dFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0luc3VmZmljaWVudCBpbmZvcm1hdGlvbiBpbiBrbm93bGVkZ2UgYmFzZScsICdDT1ZFUkFHRV9UT09fTE9XJywgMjAwKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNSZXRyeWFibGVFcnJvcihlcnJvcikge1xuICBjb25zdCByZXRyeWFibGVDb2RlcyA9IFsnUkFURV9MSU1JVF9FWENFRURFRCcsICdFTUJFRERJTkdfRVJST1InLCAnTExNX1VOQVZBSUxBQkxFJ107XG4gIHJldHVybiByZXRyeWFibGVDb2Rlcy5pbmNsdWRlcyhlcnJvci5jb2RlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzNDI5RXJyb3IoZXJyb3IpIHtcbiAgcmV0dXJuIGVycm9yPy5jb2RlID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCc0MjknKSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdUb28gTWFueSBSZXF1ZXN0cycpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IEVtYmVkZGluZ0Vycm9yLCBpczQyOUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxubGV0IGFpID0gbnVsbDtcbmxldCBlbWJlZGRpbmdNb2RlbCA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldEVtYmVkZGluZ01vZGVsKCkge1xuICBpZiAoIWVtYmVkZGluZ01vZGVsKSB7XG4gICAgYWkgPSBuZXcgR29vZ2xlR2VuQUkoe1xuICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICBwcm9qZWN0OiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCBwcm9jZXNzLmVudi5HQ1BfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJyxcbiAgICAgIGxvY2F0aW9uOiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfTE9DQVRJT04gfHwgJ3VzLWNlbnRyYWwxJ1xuICAgIH0pO1xuXG4gICAgZW1iZWRkaW5nTW9kZWwgPSBhaS5tb2RlbHM7XG4gIH1cbiAgcmV0dXJuIGVtYmVkZGluZ01vZGVsO1xufVxuXG5jb25zdCBCQVRDSF9TSVpFID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX0JBVENIX01BWF9DSFVOS1MpIHx8IDc7XG5jb25zdCBQQVJBTExFTF9DQUxMUyA9ICgpID0+IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19QQVJBTExFTF9DQUxMUykgfHwgNDtcbmNvbnN0IE9VVFBVVF9ESU1FTlNJT05TID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19ESU1FTlNJT05TKSB8fCAzMDcyO1xuY29uc3QgR1JPVVBfV0FJVF9NUyA9IDYxMDAwO1xuY29uc3QgUkVUUllfV0FJVF9NUyA9IDE1MDAwO1xuXG4vLyBFbWJlZCBhIHNpbmdsZSBiYXRjaCBvZiB0ZXh0cyAodXAgdG8gQkFUQ0hfU0laRSkuXG4vLyBSZXRyaWVzIG9uIDQyOSBhbmQgdHJhbnNpZW50IDUwMi81MDMgZXJyb3JzIHVwIHRvIDUgdGltZXMuXG5hc3luYyBmdW5jdGlvbiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBhdHRlbXB0ID0gMSkge1xuICBjb25zdCBtYXhBdHRlbXB0cyA9IDU7XG4gIGNvbnN0IG1vZGVsTmFtZSA9IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMDAxJztcblxuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0RW1iZWRkaW5nTW9kZWwoKTtcblxuICAgIGNvbnN0IGVtYmVkZGluZ1Byb21pc2VzID0gdGV4dHMubWFwKGFzeW5jIChyYXdUZXh0KSA9PiB7XG4gICAgICAvLyBDb2VyY2Ugc2FmZWx5IHRvIHN0cmluZyB0byBwcmV2ZW50IEFQSSBpbnB1dCB2YWxpZGF0aW9uIGZhaWx1cmVzXG4gICAgICBjb25zdCB0ZXh0ID0gdHlwZW9mIHJhd1RleHQgPT09ICdzdHJpbmcnID8gcmF3VGV4dCA6IFN0cmluZyhyYXdUZXh0KTtcbiAgICAgIGlmICghdGV4dCB8fCB0ZXh0LnRyaW0oKSA9PT0gJycpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdDYW5ub3QgZW1iZWQgYW4gZW1wdHkgb3IgbWlzc2luZyB0ZXh0IGJsb2NrJyk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgbW9kZWwuZW1iZWRDb250ZW50KHtcbiAgICAgICAgbW9kZWw6IG1vZGVsTmFtZSxcbiAgICAgICAgY29udGVudHM6IHRleHQsXG4gICAgICAgIGNvbmZpZzoge1xuICAgICAgICAgIHRhc2tUeXBlLFxuICAgICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBPVVRQVVRfRElNRU5TSU9OUygpXG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICAvLyBIYW5kbGUgc3RydWN0dXJhbCB2YXJpYXRpb25zIGluIHRoZSBTREsgcmVzcG9uc2UgcGF5bG9hZFxuICAgICAgY29uc3QgdmFsdWVzID0gcmVzcG9uc2U/LmVtYmVkZGluZ3M/LlswXT8udmFsdWVzIHx8XG4gICAgICAgIHJlc3BvbnNlPy5lbWJlZGRpbmc/LnZhbHVlcyB8fFxuICAgICAgICByZXNwb25zZT8udmFsdWVzO1xuXG4gICAgICBpZiAoIXZhbHVlcykge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdbZW1iZWRkaW5nXSBVbmV4cGVjdGVkIEFQSSByZXNwb25zZSBzaGFwZTonLCBKU09OLnN0cmluZ2lmeShyZXNwb25zZSkpO1xuICAgICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoJ01pc3NpbmcgdmFsdWVzIGluIGVtYmVkZGluZyByZXNwb25zZScpO1xuICAgICAgfVxuXG4gICAgICByZXR1cm4gdmFsdWVzO1xuICAgIH0pO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IGF3YWl0IFByb21pc2UuYWxsKGVtYmVkZGluZ1Byb21pc2VzKTtcblxuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtlbWJlZGRpbmdzLmxlbmd0aH1gKTtcbiAgICB9XG5cbiAgICByZXR1cm4gZW1iZWRkaW5ncztcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIC8vIFJldHJ5IG9uIHJhdGUgbGltaXRzICg0MjkpIGFzIHdlbGwgYXMgdGVtcG9yYXJ5IGdhdGV3YXkvc2VydmljZSBkaXNydXB0aW9ucyAoNTAyLCA1MDMpXG4gICAgY29uc3QgaXNSZXRyeWFibGUgPSBpczQyOUVycm9yKGVycm9yKSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDIgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMyB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdTZXJ2aWNlIFVuYXZhaWxhYmxlJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnQmFkIEdhdGV3YXknKTtcblxuICAgIGlmIChpc1JldHJ5YWJsZSAmJiBhdHRlbXB0IDwgbWF4QXR0ZW1wdHMpIHtcbiAgICAgIC8vIFNjYWxlIHdhaXQgZHluYW1pY2FsbHkgaWYgaXQncyBhIHN0cnVjdHVyYWwgZ2F0ZXdheSBlcnJvclxuICAgICAgY29uc3QgYmFzZURlbGF5ID0gZXJyb3IucmV0cnlBZnRlciB8fCAoYXR0ZW1wdCAqIFJFVFJZX1dBSVRfTVMpO1xuICAgICAgY29uc3QgcmV0cnlEZWxheSA9IGVycm9yPy5zdGF0dXMgPT09IDQyOSA/IEdST1VQX1dBSVRfTVMgOiBiYXNlRGVsYXk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBUcmFuc2llbnQgZXJyb3IgKCR7ZXJyb3I/LnN0YXR1cyB8fCAndW5rbm93bid9KSwgd2FpdGluZyAke3JldHJ5RGVsYXkgLyAxMDAwfXMgKGF0dGVtcHQgJHthdHRlbXB0fS8ke21heEF0dGVtcHRzfSkuLi5gKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCByZXRyeURlbGF5KSk7XG4gICAgICByZXR1cm4gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUsIGF0dGVtcHQgKyAxKTtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnQmF0Y2ggZW1iZWRkaW5nIGZhaWxlZCcpO1xuICB9XG59XG5cbi8vIEV4cG9ydGVkIGZvciBkb2N1bWVudHMuanMgdXBsb2FkIGhhbmRsZXIgXHUyMDE0IGVtYmVkcyBvbmUgYmF0Y2ggZ3JvdXAgKHVwIHRvIEJBVENIX1NJWkUgdGV4dHMpXG4vLyBhbmQgcmV0dXJucyByYXcgdmVjdG9ycyBhcnJheS4gQ2FsbGVyIG1hbmFnZXMgcGFyYWxsZWxpc20sIHdhaXRpbmcsIGFuZCBDaHJvbWEgd3JpdGVzLlxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJykge1xuICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gZW1iZWRTaW5nbGVCYXRjaEdyb3VwIFx1MjAxNCAke3RleHRzLmxlbmd0aH0gdGV4dHMsIHRhc2tUeXBlPSR7dGFza1R5cGV9YCk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSk7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0IGdvdCAke3ZlY3RvcnMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gIHJldHVybiB2ZWN0b3JzO1xufVxuXG4vLyBGdWxsIHBpcGVsaW5lOiBlbWJlZCBhbGwgY2h1bmtzIHdpdGggYnVpbHQtaW4gYmF0Y2hpbmcgKyB3YWl0aW5nLlxuLy8gVXNlZCBieSBzZWVkIGluZ2VzdGlvbiBhbmQgYW55IGNhbGxlcnMgdGhhdCBkb24ndCBuZWVkIHN0cmVhbWluZyBwcm9ncmVzcy5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZUVtYmVkZGluZ3MoY2h1bmtzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBvblByb2dyZXNzKSB7XG4gIGlmICghY2h1bmtzIHx8IGNodW5rcy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICBjb25zdCBiYXRjaFNpemUgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuICBjb25zdCBlbWJlZGRpbmdzID0gW107XG5cbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSk7XG4gIH1cblxuICBjb25zdCB0b3RhbEdyb3VwcyA9IE1hdGguY2VpbChiYXRjaGVzLmxlbmd0aCAvIHBhcmFsbGVsQ2FsbHMpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IHBhcmFsbGVsQmF0Y2hlcyA9IGJhdGNoZXMuc2xpY2UoaSwgaSArIHBhcmFsbGVsQ2FsbHMpO1xuICAgIGNvbnN0IGdyb3VwTnVtID0gTWF0aC5mbG9vcihpIC8gcGFyYWxsZWxDYWxscykgKyAxO1xuICAgIGNvbnN0IGNodW5rc0NvdmVyZWQgPSBNYXRoLm1pbigoaSArIHBhcmFsbGVsQ2FsbHMpICogYmF0Y2hTaXplLCBjaHVua3MubGVuZ3RoKTtcblxuICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBHcm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSBcdTIwMTQgJHtwYXJhbGxlbEJhdGNoZXMubGVuZ3RofSBiYXRjaCBjYWxsKHMpIGluIHBhcmFsbGVsIChjaHVua3MgJHtpICogYmF0Y2hTaXplICsgMX1cdTIwMTMke2NodW5rc0NvdmVyZWR9KS4uLmApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHBhcmFsbGVsQmF0Y2hlcy5tYXAoYmF0Y2ggPT4gZW1iZWRCYXRjaChiYXRjaC5tYXAoYyA9PiBjLnRleHQpLCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGJhdGNoID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlO1xuICAgICAgICBiYXRjaC5mb3JFYWNoKChjaHVuaywgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBhYnNvbHV0ZUNodW5rSWR4ID0gKGkgKyBiYXRjaElkeCkgKiBiYXRjaFNpemUgKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHthYnNvbHV0ZUNodW5rSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gQmF0Y2ggJHtpICsgYmF0Y2hJZHh9IGZhaWxlZCwgd2lsbCByZXRyeSBpbmRpdmlkdWFsbHk6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZEJhdGNoZXMucHVzaCh7IGJhdGNoLCBiYXRjaElkeDogaSArIGJhdGNoSWR4IH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKG9uUHJvZ3Jlc3MpIHtcbiAgICAgIG9uUHJvZ3Jlc3MoeyBjdXJyZW50X2JhdGNoOiBncm91cE51bSwgdG90YWxfYmF0Y2hlczogdG90YWxHcm91cHMgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgaXNMYXN0R3JvdXAgPSBpICsgcGFyYWxsZWxDYWxscyA+PSBiYXRjaGVzLmxlbmd0aDtcbiAgICBpZiAoIWlzTGFzdEdyb3VwIHx8IGZhaWxlZEJhdGNoZXMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFdhaXRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIGJlZm9yZSBuZXh0IGdyb3VwLi4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgR1JPVVBfV0FJVF9NUykpO1xuICAgIH1cblxuICAgIGZvciAoY29uc3QgeyBiYXRjaCwgYmF0Y2hJZHggfSBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gV2FpdGluZyAke1JFVFJZX1dBSVRfTVMgLyAxMDAwfXMgYmVmb3JlIHJldHJ5aW5nIGZhaWxlZCBiYXRjaCAke2JhdGNoSWR4fS4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIFJFVFJZX1dBSVRfTVMpKTtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgYmF0Y2gpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtiYXRjaElkeH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW2VtYmVkZGluZ10gXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbdGV4dF0sICdSRVRSSUVWQUxfRE9DVU1FTlQnKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSYXRlTGltaXRTdGF0ZSgpIHtcbiAgcmV0dXJuIHtcbiAgICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgICBwYXJhbGxlbENhbGxzOiBQQVJBTExFTF9DQUxMUygpLFxuICAgIG1heENodW5rc1BlckNhbGw6IEJBVENIX1NJWkUoKSxcbiAgICBvdXRwdXREaW1lbnNpb25zOiBPVVRQVVRfRElNRU5TSU9OUygpXG4gIH07XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0UmF0ZUxpbWl0U3RhdGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGgocmVxLCByZXMpIHtcbiAgY29uc3QgaGVhbHRoU3RhdHVzID0ge1xuICAgIHN0YXR1czogJ29rJyxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICBzZXJ2aWNlczoge31cbiAgfTtcblxuICAvLyBDaGVjayBDaHJvbWFEQlxuICB0cnkge1xuICAgIGNvbnN0IGNocm9tYUhlYWx0aCA9IGF3YWl0IGNocm9tYUhlYWx0aENoZWNrKCk7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0gY2hyb21hSGVhbHRoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IHtcbiAgICAgIHN0YXR1czogJ2Vycm9yJyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlXG4gICAgfTtcbiAgfVxuXG4gIC8vIENoZWNrIEdlbWluaSAodmlhIEFQSSBrZXkgcHJlc2VuY2UpXG4gIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5nZW1pbmkgPSB7XG4gICAgc3RhdHVzOiBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSA/ICdjb25maWd1cmVkJyA6ICdub3RfY29uZmlndXJlZCdcbiAgfTtcblxuICAvLyBHZXQgcmF0ZSBsaW1pdCBzdGF0ZVxuICBoZWFsdGhTdGF0dXMucmF0ZUxpbWl0ID0gZ2V0UmF0ZUxpbWl0U3RhdGUoKTtcblxuICAvLyBPdmVyYWxsIHN0YXR1c1xuICBjb25zdCBoYXNFcnJvcnMgPSBPYmplY3QudmFsdWVzKGhlYWx0aFN0YXR1cy5zZXJ2aWNlcykuc29tZShcbiAgICBzID0+IHMuc3RhdHVzID09PSAnZXJyb3InIHx8IHMuc3RhdHVzID09PSAndW5oZWFsdGh5J1xuICApO1xuXG4gIGlmIChoYXNFcnJvcnMpIHtcbiAgICBoZWFsdGhTdGF0dXMuc3RhdHVzID0gJ2RlZ3JhZGVkJztcbiAgfVxuXG4gIHJlcy5qc29uKGhlYWx0aFN0YXR1cyk7XG59XG5cbnJvdXRlci5nZXQoJy8nLCBoZWFsdGgpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2ltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgVmFsaWRhdGlvbkVycm9yIH0gZnJvbSAnLi9lcnJvcnMuanMnO1xuXG5jb25zdCBEQU5HRVJPVVNfUEFUVEVSTlMgPSAvWzw+OlwifD8qXFx4MDAtXFx4MWZdL2c7XG5jb25zdCBQQVRIX1RSQVZFUlNBTCA9IC9cXC5cXC4vZztcblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplRmlsZW5hbWUoZmlsZW5hbWUpIHtcbiAgaWYgKCFmaWxlbmFtZSB8fCB0eXBlb2YgZmlsZW5hbWUgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZScpO1xuICB9XG5cbiAgLy8gUmVtb3ZlIHBhdGggY29tcG9uZW50cyBhbmQgZ2V0IGJhc2VuYW1lXG4gIGNvbnN0IGJhc2VuYW1lID0gcGF0aC5iYXNlbmFtZShmaWxlbmFtZSk7XG5cbiAgLy8gUmVtb3ZlIGRhbmdlcm91cyBjaGFyYWN0ZXJzXG4gIGxldCBzYW5pdGl6ZWQgPSBiYXNlbmFtZS5yZXBsYWNlKERBTkdFUk9VU19QQVRURVJOUywgJ18nKTtcblxuICAvLyBSZW1vdmUgcGF0aCB0cmF2ZXJzYWwgYXR0ZW1wdHNcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnJlcGxhY2UoUEFUSF9UUkFWRVJTQUwsICcnKTtcblxuICAvLyBUcmltIHdoaXRlc3BhY2UgYW5kIGxpbWl0IGxlbmd0aFxuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQudHJpbSgpLnNsaWNlKDAsIDI1NSk7XG5cbiAgaWYgKCFzYW5pdGl6ZWQpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lIGFmdGVyIHNhbml0aXphdGlvbicpO1xuICB9XG5cbiAgcmV0dXJuIHNhbml0aXplZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlUERGRmlsZShmaWxlKSB7XG4gIGlmICghZmlsZSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ05vIGZpbGUgcHJvdmlkZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIE1JTUUgdHlwZVxuICBjb25zdCB2YWxpZE1pbWVUeXBlcyA9IFsnYXBwbGljYXRpb24vcGRmJ107XG4gIGlmICghdmFsaWRNaW1lVHlwZXMuaW5jbHVkZXMoZmlsZS5taW1ldHlwZSkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdPbmx5IFBERiBmaWxlcyBhcmUgYWNjZXB0ZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIGV4dGVuc2lvblxuICBjb25zdCBleHQgPSBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUgfHwgJycpLnRvTG93ZXJDYXNlKCk7XG4gIGlmIChleHQgIT09ICcucGRmJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ZpbGUgbXVzdCBoYXZlIC5wZGYgZXh0ZW5zaW9uJyk7XG4gIH1cblxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRmlsZVNpemUoc2l6ZUJ5dGVzLCBtYXhTaXplTUIpIHtcbiAgY29uc3QgbWF4Qnl0ZXMgPSBtYXhTaXplTUIgKiAxMDI0ICogMTAyNDtcbiAgaWYgKHNpemVCeXRlcyA+IG1heEJ5dGVzKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmApO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVJbnB1dChpbnB1dCwgbWF4TGVuZ3RoID0gMTAwMDApIHtcbiAgaWYgKCFpbnB1dCB8fCB0eXBlb2YgaW5wdXQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG4gIHJldHVybiBpbnB1dC50cmltKCkuc2xpY2UoMCwgbWF4TGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRG9jdW1lbnRJZChpZCkge1xuICBpZiAoIWlkIHx8IHR5cGVvZiBpZCAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEJyk7XG4gIH1cbiAgY29uc3QgdXVpZFJlZ2V4ID0gL15bMC05YS1mXXs4fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXsxMn0kL2k7XG4gIGlmICghdXVpZFJlZ2V4LnRlc3QoaWQpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCBmb3JtYXQnKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RUZXh0RnJvbVBERkJ1ZmZlcihidWZmZXIpIHtcbiAgLy8gVGhpcyB3aWxsIGJlIHVzZWQgd2l0aCBwZGYtcGFyc2VcbiAgcmV0dXJuIGJ1ZmZlcjtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2ltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuXG5jb25zdCBDSEFSU19QRVJfVE9LRU4gICAgID0gNDtcbmNvbnN0IFRBUkdFVF9DSFVOS19UT0tFTlMgPSA2MDA7ICAgLy8gc29mdCB0YXJnZXQgcGVyIGNodW5rXG5jb25zdCBNQVhfQ0hVTktfVE9LRU5TICAgID0gNzUwOyAgIC8vIGhhcmQgY2FwIGJlZm9yZSBmb3JjZWQgc3BsaXRcbmNvbnN0IE9WRVJMQVBfVE9LRU5TICAgICAgPSAxMDA7ICAgLy8gb3ZlcmxhcCBvbmx5IG9uIG92ZXJzaXplZCBwYXJhZ3JhcGhzXG5jb25zdCBNSU5fQ0hVTktfQ0hBUlMgICAgID0gMTAwO1xuXG4vLyBNYXRjaGVzIEFMTC1DQVBTIGhlYWRpbmdzLCBtYXJrZG93biBoZWFkaW5ncywgb3IgbnVtYmVyZWQgc2VjdGlvbiBoZWFkaW5nc1xuY29uc3QgSEVBRElOR19SRSA9IC9eKD86W0EtWl1bQS1aXFxzXXsyLDYwfSR8I3sxLDR9XFxzLit8KD86XFxkK1xcLikrXFxzLispL207XG5cbmV4cG9ydCBmdW5jdGlvbiBlc3RpbWF0ZVRva2Vucyh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAwO1xuICByZXR1cm4gTWF0aC5jZWlsKHRleHQubGVuZ3RoIC8gQ0hBUlNfUEVSX1RPS0VOKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFuVGV4dCh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAnJztcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvXFxmL2csICdcXG4nKVxuICAgIC5yZXBsYWNlKC8oXFxzKlxcbil7Myx9L2csICdcXG5cXG4nKVxuICAgIC5yZXBsYWNlKC9eXFxzKlxcZCtcXHMqJC9nbSwgJycpXG4gICAgLnJlcGxhY2UoL1sgXFx0XXsyLH0vZywgJyAnKVxuICAgIC50cmltKCk7XG59XG5cbmZ1bmN0aW9uIGdlbmVyYXRlQ2h1bmtJZCh0ZXh0LCBmaWxlbmFtZSkge1xuICByZXR1cm4gY3JlYXRlSGFzaCgnbWQ1JylcbiAgICAudXBkYXRlKGAke2ZpbGVuYW1lfTo6JHt0ZXh0fWApXG4gICAgLmRpZ2VzdCgnaGV4JylcbiAgICAuc2xpY2UoMCwgMTYpO1xufVxuXG4vKipcbiAqIFN0cnVjdHVyZS1hd2FyZSBjaHVua2luZzpcbiAqICAxLiBTcGxpdCBvbiBibGFuayBsaW5lcyAoXFxuXFxuKSBpbnRvIHBhcmFncmFwaHMuXG4gKiAgMi4gQSBsaW5lIG1hdGNoaW5nIEhFQURJTkdfUkUgYWx3YXlzIHN0YXJ0cyBhIGZyZXNoIGNodW5rLlxuICogIDMuIEFjY3VtdWxhdGUgcGFyYWdyYXBocyB1bnRpbCB0aGUgc29mdCBUQVJHRVQgaXMgcmVhY2hlZCwgdGhlbiBmbHVzaC5cbiAqICA0LiBQYXJhZ3JhcGhzIGxhcmdlciB0aGFuIE1BWCBhcmUgc3BsaXQgd2l0aCBhIHNsaWRpbmcgd2luZG93ICsgb3ZlcmxhcCBhcyBmYWxsYmFjay5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdGFyZ2V0VG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgVEFSR0VUX0NIVU5LX1RPS0VOUztcbiAgY29uc3QgbWF4VG9rZW5zICAgID0gb3B0aW9ucy5tYXhDaHVua1Rva2VucyAgfHwgTUFYX0NIVU5LX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRrICAgID0gb3B0aW9ucy5vdmVybGFwVG9rZW5zICAgfHwgT1ZFUkxBUF9UT0tFTlM7XG5cbiAgY29uc3QgdGFyZ2V0Q2hhcnMgID0gdGFyZ2V0VG9rZW5zICogQ0hBUlNfUEVSX1RPS0VOO1xuICBjb25zdCBtYXhDaGFycyAgICAgPSBtYXhUb2tlbnMgICAgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUayAgICAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgLy8gMS4gU3BsaXQgaW50byBwYXJhZ3JhcGhzXG4gIGNvbnN0IHJhd1BhcmFzID0gdGV4dFxuICAgIC5zcGxpdCgvXFxuezIsfS8pXG4gICAgLm1hcChwID0+IHAudHJpbSgpKVxuICAgIC5maWx0ZXIocCA9PiBwLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpO1xuXG4gIGNvbnN0IGNodW5rcyAgICAgPSBbXTtcbiAgbGV0ICAgYnVmZmVyICAgICA9ICcnO1xuICBsZXQgICBidWZTdGFydCAgID0gMDtcbiAgbGV0ICAgY2h1bmtJbmRleCA9IDA7XG4gIGxldCAgIGNoYXJDdXJzb3IgPSAwO1xuXG4gIGNvbnN0IGZsdXNoID0gKGZvcmNlVGV4dCkgPT4ge1xuICAgIGNvbnN0IGNvbnRlbnQgPSAoZm9yY2VUZXh0ID8/IGJ1ZmZlcikudHJpbSgpO1xuICAgIGlmIChjb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogICAgICAgY29udGVudCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogIGJ1ZlN0YXJ0LFxuICAgICAgICBjaGFyRW5kOiAgICBidWZTdGFydCArIGNvbnRlbnQubGVuZ3RoLFxuICAgICAgICBjaHVua0luZGV4OiBjaHVua0luZGV4KytcbiAgICAgIH0pO1xuICAgIH1cbiAgICBidWZmZXIgICA9ICcnO1xuICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgfTtcblxuICBmb3IgKGNvbnN0IHBhcmEgb2YgcmF3UGFyYXMpIHtcbiAgICBjb25zdCBpc0hlYWRpbmcgPSBIRUFESU5HX1JFLnRlc3QocGFyYS5zcGxpdCgnXFxuJylbMF0pO1xuXG4gICAgLy8gMi4gSGVhZGluZyBhbHdheXMgc3RhcnRzIGEgbmV3IGNodW5rXG4gICAgaWYgKGlzSGVhZGluZyAmJiBidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgIGlmIChwYXJhLmxlbmd0aCA+IG1heENoYXJzKSB7XG4gICAgICAvLyAzLiBPdmVyc2l6ZWQgcGFyYWdyYXBoIC0+IHNsaWRpbmctd2luZG93IGNoYXIgZmFsbGJhY2tcbiAgICAgIGlmIChidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgICAgbGV0IHMgPSAwO1xuICAgICAgd2hpbGUgKHMgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICBsZXQgZSA9IHMgKyB0YXJnZXRDaGFycztcbiAgICAgICAgaWYgKGUgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICAgIGNvbnN0IHNlYXJjaEZyb20gPSBlIC0gTWF0aC5mbG9vcih0YXJnZXRDaGFycyAqIDAuMik7XG4gICAgICAgICAgZm9yIChjb25zdCBicCBvZiBbJy4gJywgJy5cXG4nLCAnPyAnLCAnISAnLCAnXFxuJ10pIHtcbiAgICAgICAgICAgIGNvbnN0IGlkeCA9IHBhcmEubGFzdEluZGV4T2YoYnAsIGUpO1xuICAgICAgICAgICAgaWYgKGlkeCA+IHNlYXJjaEZyb20pIHsgZSA9IGlkeCArIGJwLmxlbmd0aDsgYnJlYWs7IH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgZSA9IE1hdGgubWluKGUsIHBhcmEubGVuZ3RoKTtcbiAgICAgICAgY29uc3Qgc2xpY2UgPSBwYXJhLnNsaWNlKHMsIGUpLnRyaW0oKTtcbiAgICAgICAgaWYgKHNsaWNlLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgICAgICB0ZXh0OiAgICAgICBzbGljZSxcbiAgICAgICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKHNsaWNlKSxcbiAgICAgICAgICAgIGNoYXJTdGFydDogIGNoYXJDdXJzb3IgKyBzLFxuICAgICAgICAgICAgY2hhckVuZDogICAgY2hhckN1cnNvciArIGUsXG4gICAgICAgICAgICBjaHVua0luZGV4OiBjaHVua0luZGV4KytcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCBuZXh0ID0gZSAtIG92ZXJsYXBDaGFycztcbiAgICAgICAgcyA9IG5leHQgPiBzID8gbmV4dCA6IGU7XG4gICAgICB9XG4gICAgICBjaGFyQ3Vyc29yICs9IHBhcmEubGVuZ3RoICsgMjtcbiAgICAgIGJ1ZlN0YXJ0ICAgID0gY2hhckN1cnNvcjtcbiAgICAgIGNvbnRpbnVlO1xuICAgIH1cblxuICAgIC8vIDQuIE5vcm1hbCBwYXJhZ3JhcGggXHUyMDE0IGhhcmQgY2FwIGxvb2thaGVhZCBCRUZPUkUgYWNjdW11bGF0aW5nXG4gICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwICYmIChidWZmZXIubGVuZ3RoICsgcGFyYS5sZW5ndGggKyAyKSA+IG1heENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cblxuICAgIGJ1ZmZlciAgICAgPSBidWZmZXIgPyBidWZmZXIgKyAnXFxuXFxuJyArIHBhcmEgOiBwYXJhO1xuICAgIGNoYXJDdXJzb3IgKz0gcGFyYS5sZW5ndGggKyAyO1xuXG4gICAgLy8gU29mdCBjYXA6IGZsdXNoIG9uY2UgdGFyZ2V0IGlzIHJlYWNoZWRcbiAgICBpZiAoYnVmZmVyLmxlbmd0aCA+PSB0YXJnZXRDaGFycykge1xuICAgICAgZmx1c2goKTtcbiAgICB9XG4gIH1cblxuICAvLyA1LiBGbHVzaCByZW1haW5kZXJcbiAgZmx1c2goKTtcblxuICByZXR1cm4gY2h1bmtzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtQREZDb250ZW50KHBkZkRhdGEsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB7IGZpbGVuYW1lLCBkb2N1bWVudElkLCBwYWdlTnVtYmVyLCB0ZXh0LCB0b3RhbFBhZ2VzIH0gPSBwZGZEYXRhO1xuXG4gIGlmICghdGV4dCB8fCB0ZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICAke2ZpbGVuYW1lfSBwYWdlICR7cGFnZU51bWJlcn06IGV4dHJhY3RlZCB0ZXh0IHRvbyBzaG9ydCBcdTIwMTQgbWF5IGJlIGEgc2Nhbm5lZCBwYWdlLCBza2lwcGluZ2ApO1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGNsZWFuZWRUZXh0ID0gY2xlYW5UZXh0KHRleHQpO1xuICBjb25zdCB0ZXh0Q2h1bmtzICA9IGNodW5rVGV4dChjbGVhbmVkVGV4dCwgb3B0aW9ucyk7XG4gIGNvbnN0IHRvdGFsQ2h1bmtzID0gdGV4dENodW5rcy5sZW5ndGg7XG4gIGNvbnN0IHNvdXJjZVR5cGUgID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZSxcbiAgICAgICAgY2h1bmtfaWQ6ICAgICAgICAgY2h1bmtJZCxcbiAgICAgICAgY2h1bmtfaW5kZXg6ICAgICAgY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiAgICAgdG90YWxDaHVua3MsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiAgICAgIHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6ICAgICAgdG90YWxQYWdlcyB8fCBudWxsLFxuICAgICAgICBzZWN0aW9uX3RpdGxlOiAgICBleHRyYWN0U2VjdGlvblRpdGxlKGNodW5rLnRleHQpLFxuICAgICAgICBzb3VyY2VfdHlwZTogICAgICBzb3VyY2VUeXBlLFxuICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIGNoYXJfc3RhcnQ6ICAgICAgIGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgY2hhcl9lbmQ6ICAgICAgICAgY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6ICAgICAgY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH07XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0U2VjdGlvblRpdGxlKHRleHQpIHtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7XG4gIGdldEdsb2JhbENvbGxlY3Rpb24sXG4gIGdldFNlc3Npb25Db2xsZWN0aW9uLFxuICBsaXN0RG9jdW1lbnRzLFxuICBhZGRWZWN0b3JzXG59IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTID0gNjA7XG5jb25zdCBzZXNzaW9ucyA9IG5ldyBNYXAoKTtcbmNvbnN0IE1BWF9QREZTX1BFUl9TRVNTSU9OID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04pIHx8IDM7XG5jb25zdCBNQVhfVVBMT0FEX1NJWkVfTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIpIHx8IDU7XG5cbmNvbnN0IHNlZWRlZFNlc3Npb25zID0gbmV3IFNldCgpO1xuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3QgaWQgPSBzZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IHNlc3Npb24gPSB7XG4gICAgaWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIGRlbGV0ZWREb2N1bWVudElkczogbmV3IFNldCgpLCAgIC8vIHRyYWNrIGRlbGV0ZWQgZG9jIElEcyB0byBmaWx0ZXIgcHJvbXB0IG1lbW9yeVxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuICBzZXNzaW9ucy5zZXQoaWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykgcmV0dXJuIGV4aXN0aW5nO1xuICAgIHJldHVybiBjcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gIH1cbiAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikge1xuICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICBjb25zdCBsYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZShzZXNzaW9uLmxhc3RBY2Nlc3NlZCkuZ2V0VGltZSgpO1xuICBjb25zdCB0aW1lb3V0TXMgPSBzZXNzaW9uLnRpbWVvdXRNaW51dGVzICogNjAgKiAxMDAwO1xuICByZXR1cm4gKG5vdyAtIGxhc3RBY2Nlc3NlZCkgPiB0aW1lb3V0TXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBzZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgc2VlZGVkU2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbi8qKlxuICogT24gc2Vzc2lvbiBzdGFydDpcbiAqIC0gSWYgY29sbGVjdGlvbiBpcyBORVcgXHUyMTkyIHNlZWQgZnJvbSBnbG9iYWwgKHBhZ2luYXRlZCwgMzAwL2JhdGNoKVxuICogLSBJZiBjb2xsZWN0aW9uIEVYSVNUUyBcdTIxOTIgc2tpcCBzZWVkLCByZWNvbnN0cnVjdCBpbi1tZW1vcnkgZG9jIGxpc3QgZnJvbSBDaHJvbWFcbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKSB7XG4gIGNvbnNvbGUubG9nKGBcdUQ4M0RcdUREMTEgU2Vzc2lvbiBpbml0OiAke3Nlc3Npb25JZH1gKTtcbiAgaWYgKHNlZWRlZFNlc3Npb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBBbHJlYWR5IHNlZWRlZCAke3Nlc3Npb25JZH0sIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgIGNvbnN0IHsgY29sbGVjdGlvbjogc2Vzc2lvbkNvbGxlY3Rpb24sIGlzTmV3IH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuXG4gICAgaWYgKCFpc05ldykge1xuICAgICAgY29uc29sZS5sb2coYFx1MjY3Qlx1RkUwRiAgU2Vzc2lvbiBleGlzdHMsIHJlY29uc3RydWN0aW5nIGRvY3VtZW50IGxpc3QgZnJvbSBDaHJvbWEuLi5gKTtcbiAgICAgIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgY29uc3QgZG9jcyA9IGF3YWl0IGxpc3REb2N1bWVudHMoc2Vzc2lvbkNvbGxlY3Rpb24pO1xuICAgICAgICBkb2NzLmZvckVhY2goZG9jID0+IHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICAgICAgICAgIGlkOiBkb2MuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgICBwYWdlQ291bnQ6IGRvYy5wYWdlX2NvdW50IHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiBkb2MuY2h1bmtfY291bnQsXG4gICAgICAgICAgICBzb3VyY2VUeXBlOiBkb2Muc291cmNlX3R5cGUsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IGRvYy51cGxvYWRfdGltZXN0YW1wXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0pO1xuICAgICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFJlY29uc3RydWN0ZWQgJHtkb2NzLmxlbmd0aH0gZG9jdW1lbnQocykgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgICB9XG4gICAgICBzZWVkZWRTZXNzaW9ucy5hZGQoc2Vzc2lvbklkKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHVEODNDXHVERjMxIE5ldyBzZXNzaW9uIFx1MjAxNCBzZWVkaW5nIGZyb20gZ2xvYmFsIGNvbGxlY3Rpb24uLi5gKTtcblxuICAgIGNvbnN0IEJBVENIX1NJWkUgPSAzMDA7XG4gICAgbGV0IG9mZnNldCA9IDA7XG4gICAgY29uc3QgYWxsSWRzID0gW10sIGFsbEVtYmVkZGluZ3MgPSBbXSwgYWxsRG9jdW1lbnRzID0gW10sIGFsbE1ldGFkYXRhcyA9IFtdO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgZ2xvYmFsQ29sbGVjdGlvbi5nZXQoe1xuICAgICAgICBpbmNsdWRlOiBbJ2VtYmVkZGluZ3MnLCAnZG9jdW1lbnRzJywgJ21ldGFkYXRhcyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcbiAgICAgIGFsbEVtYmVkZGluZ3MucHVzaCguLi5iYXRjaC5lbWJlZGRpbmdzKTtcbiAgICAgIGFsbERvY3VtZW50cy5wdXNoKC4uLmJhdGNoLmRvY3VtZW50cyk7XG4gICAgICBhbGxNZXRhZGF0YXMucHVzaCguLi5iYXRjaC5tZXRhZGF0YXMpO1xuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID09PSAwKSB7XG4gICAgICBjb25zb2xlLmxvZygnXHUyNkEwXHVGRTBGICBHbG9iYWwgY29sbGVjdGlvbiBpcyBlbXB0eSBcdTIwMTQgbm90aGluZyB0byBzZWVkLicpO1xuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhbGxJZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGF3YWl0IHNlc3Npb25Db2xsZWN0aW9uLmFkZCh7XG4gICAgICAgIGlkczogYWxsSWRzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSxcbiAgICAgICAgZW1iZWRkaW5nczogYWxsRW1iZWRkaW5ncy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIGRvY3VtZW50czogYWxsRG9jdW1lbnRzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSxcbiAgICAgICAgbWV0YWRhdGFzOiBhbGxNZXRhZGF0YXMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcChtID0+ICh7IC4uLm0sIHNvdXJjZV90eXBlOiAnZ2xvYmFsJyB9KSlcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgXHVEODNEXHVEQ0U2IEFkZGVkIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogcmVjb3JkcyAke2kgKyAxfVx1MjAxMyR7TWF0aC5taW4oaSArIEJBVENIX1NJWkUsIGFsbElkcy5sZW5ndGgpfWApO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2VlZGVkICR7YWxsSWRzLmxlbmd0aH0gdmVjdG9ycyBpbnRvIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG5cbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChzZXNzaW9uKSB7XG4gICAgICBjb25zdCBkb2NzTWFwID0gbmV3IE1hcCgpO1xuICAgICAgYWxsTWV0YWRhdGFzLmZvckVhY2gobWV0YSA9PiB7XG4gICAgICAgIGlmICghZG9jc01hcC5oYXMobWV0YS5kb2N1bWVudF9pZCkpIHtcbiAgICAgICAgICBkb2NzTWFwLnNldChtZXRhLmRvY3VtZW50X2lkLCB7XG4gICAgICAgICAgICBpZDogbWV0YS5kb2N1bWVudF9pZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiBtZXRhLmZpbGVuYW1lLFxuICAgICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgICBwYWdlQ291bnQ6IG1ldGEudG90YWxfcGFnZXMgfHwgbnVsbCxcbiAgICAgICAgICAgIGNodW5rQ291bnQ6IDAsXG4gICAgICAgICAgICBzb3VyY2VUeXBlOiAnZ2xvYmFsJyxcbiAgICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgZG9jc01hcC5nZXQobWV0YS5kb2N1bWVudF9pZCkuY2h1bmtDb3VudCsrO1xuICAgICAgfSk7XG5cbiAgICAgIGZvciAoY29uc3QgZG9jIG9mIGRvY3NNYXAudmFsdWVzKCkpIHtcbiAgICAgICAgaWYgKCFzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5pZCA9PT0gZG9jLmlkKSkge1xuICAgICAgICAgIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goZG9jKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYFx1Mjc0QyBGYWlsZWQgdG8gc2VlZCBzZXNzaW9uICR7c2Vzc2lvbklkfTpgLCBlcnJvci5tZXNzYWdlKTtcbiAgfVxufVxuXG4vKipcbiAqIFVwc2VydCBhIGRvY3VtZW50IGludG8gdGhlIHNlc3Npb24uXG4gKiBJZiBhIGRvYyB3aXRoIHRoZSBzYW1lIGlkIGFscmVhZHkgZXhpc3RzLCB1cGRhdGUgaXQgaW4gcGxhY2UgKG5vIGR1cGxpY2F0ZSkuXG4gKiBTdXBwb3J0cyBwYXJ0aWFsIHVwZGF0ZXMgXHUyMDE0IG9ubHkgcHJvdmlkZWQgZmllbGRzIG92ZXJ3cml0ZSBleGlzdGluZyB2YWx1ZXMuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SW5mbykge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcblxuICBjb25zdCBleGlzdGluZyA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmQoZCA9PiBkLmlkID09PSBkb2N1bWVudEluZm8uaWQpO1xuXG4gIGlmIChleGlzdGluZykge1xuICAgIGlmIChkb2N1bWVudEluZm8uY2h1bmtDb3VudCAgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuY2h1bmtDb3VudCAgPSBkb2N1bWVudEluZm8uY2h1bmtDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnBhZ2VDb3VudCAgID0gZG9jdW1lbnRJbmZvLnBhZ2VDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVTaXplICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVTaXplICAgID0gZG9jdW1lbnRJbmZvLmZpbGVTaXplO1xuICAgIGlmIChkb2N1bWVudEluZm8uc3RhdHVzICAgICAgIT09IHVuZGVmaW5lZCkgZXhpc3Rpbmcuc3RhdHVzICAgICAgPSBkb2N1bWVudEluZm8uc3RhdHVzO1xuICAgIGlmIChkb2N1bWVudEluZm8uZmlsZW5hbWUgICAgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuZmlsZW5hbWUgICAgPSBkb2N1bWVudEluZm8uZmlsZW5hbWU7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gVXBkYXRlZCBkb2MgJHtkb2N1bWVudEluZm8uaWR9IFx1MjAxNCBzdGF0dXM9JHtleGlzdGluZy5zdGF0dXN9LCBjaHVua3M9JHtleGlzdGluZy5jaHVua0NvdW50fWApO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgaWQ6IGRvY3VtZW50SW5mby5pZCxcbiAgICBmaWxlbmFtZTogZG9jdW1lbnRJbmZvLmZpbGVuYW1lLFxuICAgIGZpbGVTaXplOiBkb2N1bWVudEluZm8uZmlsZVNpemUsXG4gICAgcGFnZUNvdW50OiBkb2N1bWVudEluZm8ucGFnZUNvdW50LFxuICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICBjaHVua0NvdW50OiBkb2N1bWVudEluZm8uY2h1bmtDb3VudCA/PyAwLFxuICAgIHNvdXJjZVR5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgc3RhdHVzOiBkb2N1bWVudEluZm8uc3RhdHVzID8/ICdpbmRleGluZydcbiAgfSk7XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBBZGRlZCBkb2MgJHtkb2N1bWVudEluZm8uaWR9IFx1MjAxNCBzdGF0dXM9JHtkb2N1bWVudEluZm8uc3RhdHVzID8/ICdpbmRleGluZyd9YCk7XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2FuQWNjZXB0VXBsb2FkKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogJ1Nlc3Npb24gbm90IGZvdW5kJyB9O1xuICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGg7XG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiBgTWF4aW11bSAke01BWF9QREZTX1BFUl9TRVNTSU9OfSBQREZzIHBlciBzZXNzaW9uYCB9O1xuICB9XG4gIHJldHVybiB7IGNhblVwbG9hZDogdHJ1ZSB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVVcGxvYWQoc2Vzc2lvbklkLCBmaWxlLCBmaWxlbmFtZSkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBjb25zdCBlcnJvcnMgPSBbXTtcblxuICBpZiAoZmlsZS5zaXplID4gTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBleGNlZWRzICR7TUFYX1VQTE9BRF9TSVpFX01CfU1CIGxpbWl0YCk7XG4gIH1cblxuICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvblxuICAgID8gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGhcbiAgICA6IDA7XG5cbiAgaWYgKHVwbG9hZGVkQ291bnQgPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICBlcnJvcnMucHVzaChgTWF4aW11bSAke01BWF9QREZTX1BFUl9TRVNTSU9OfSBQREZzIHBlciBzZXNzaW9uYCk7XG4gIH1cblxuICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gZmlsZW5hbWUpKSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmApO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpc1ZhbGlkOiBlcnJvcnMubGVuZ3RoID09PSAwLFxuICAgIGVycm9ycyxcbiAgICBpc0xhcmdlRmlsZTogZmlsZS5zaXplID4gKE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0ICogMC42KVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG4gIGNvbnN0IGlkeCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmRJbmRleChkID0+IGQuaWQgPT09IGRvY3VtZW50SWQpO1xuICBpZiAoaWR4ID49IDApIHtcbiAgICBzZXNzaW9uLmRvY3VtZW50cy5zcGxpY2UoaWR4LCAxKTtcbiAgICAvLyBUcmFjayBkZWxldGVkIGRvYyBzbyBpdHMgbWVtb3J5IHR1cm5zIGFyZSBleGNsdWRlZCBmcm9tIGZ1dHVyZSBwcm9tcHRzXG4gICAgc2Vzc2lvbi5kZWxldGVkRG9jdW1lbnRJZHMuYWRkKGRvY3VtZW50SWQpO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFJlbW92ZWQgZG9jICR7ZG9jdW1lbnRJZH0sIGFkZGVkIHRvIGRlbGV0ZWREb2N1bWVudElkc2ApO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldERlbGV0ZWREb2N1bWVudElkcyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHNlc3Npb24/LmRlbGV0ZWREb2N1bWVudElkcyA/PyBuZXcgU2V0KCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBbXTtcbiAgcmV0dXJuIHNlc3Npb24uZG9jdW1lbnRzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IHNlc3Npb25Eb2N1bWVudHM6IFtdLCBnbG9iYWxEb2N1bWVudHM6IFtdIH07XG5cbiAgY29uc3Qgbm9ybWFsaXplID0gKGRvYykgPT4gKHtcbiAgICBkb2N1bWVudF9pZDogZG9jLmlkLFxuICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgY2h1bmtfY291bnQ6IGRvYy5jaHVua0NvdW50ID8/IDAsXG4gICAgcGFnZV9jb3VudDogZG9jLnBhZ2VDb3VudCA/PyAwLFxuICAgIHVwbG9hZF90aW1lc3RhbXA6IGRvYy51cGxvYWRUaW1lc3RhbXAgfHwgbnVsbCxcbiAgICBzb3VyY2VfdHlwZTogZG9jLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcgPyAnc2Vzc2lvbl91cGxvYWQnIDogJ3NlZWQnLFxuICAgIGZpbGVTaXplOiBkb2MuZmlsZVNpemUgfHwgbnVsbCxcbiAgICBzdGF0dXM6IGRvYy5zdGF0dXMgPz8gbnVsbFxuICB9KTtcblxuICByZXR1cm4ge1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKVxuICAgICAgLm1hcChub3JtYWxpemUpLFxuICAgIGdsb2JhbERvY3VtZW50czogc2Vzc2lvbi5kb2N1bWVudHNcbiAgICAgIC5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdnbG9iYWwnKVxuICAgICAgLm1hcChub3JtYWxpemUpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIHJldHVybiB7XG4gICAgaWQ6IHNlc3Npb24uaWQsXG4gICAgZG9jdW1lbnRDb3VudDogc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogc2Vzc2lvbi5jcmVhdGVkQXQsXG4gICAgbGFzdEFjY2Vzc2VkOiBzZXNzaW9uLmxhc3RBY2Nlc3NlZCxcbiAgICB0b3RhbFNpemU6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5maWxlU2l6ZSB8fCAwKSwgMCksXG4gICAgdG90YWxDaHVua3M6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdEFjdGl2ZVNlc3Npb25zKCkge1xuICByZXR1cm4gQXJyYXkuZnJvbShzZXNzaW9ucy52YWx1ZXMoKSkuZmlsdGVyKHMgPT4gIWlzU2Vzc2lvbkV4cGlyZWQocykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW51cEV4cGlyZWRTZXNzaW9ucygpIHtcbiAgbGV0IGNsZWFuZWQgPSAwO1xuICBmb3IgKGNvbnN0IFtpZCwgc2Vzc2lvbl0gb2Ygc2Vzc2lvbnMpIHtcbiAgICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgICAgc2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBjbGVhbmVkKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBjbGVhbmVkO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIHF1ZXJ5Q29sbGVjdGlvbiB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBlbWJlZFF1ZXJ5IH0gZnJvbSAnLi9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBUT1BfSyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LlRPUF9LKSB8fCA1O1xuY29uc3QgUkVGVVNBTF9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LlJFRlVTQUxfVEhSRVNIT0xEKSB8fCAwLjA1O1xuXG5jb25zdCBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMgPSBuZXcgTWFwKCk7XG5cbmFzeW5jIGZ1bmN0aW9uIGdldE9yQ2FjaGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgdHJ5IHtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7IC8vIGRlc3RydWN0dXJlXG4gICAgaWYgKGNvbGxlY3Rpb24pIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IFRPUF9LKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAgfTtcbiAgY29uc3Qgc2NvcmVzID0gcmVzdWx0cy5zbGljZSgwLCB0b3BLKS5tYXAociA9PiBNYXRoLm1heCgwLCByLnNjb3JlKSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcbiAgcmV0dXJuIHtcbiAgICBjb25maWRlbmNlOiBNYXRoLnJvdW5kKGF2Z1Njb3JlICogMTAwKSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKVxuICB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdG9wSyA9IG9wdGlvbnMudG9wSyB8fCBUT1BfSztcblxuICB0cnkge1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgc2Vzc2lvbkNvbGxlY3Rpb25dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBzZXNzaW9uSWQgPyBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSA6IFByb21pc2UucmVzb2x2ZShudWxsKVxuICAgIF0pO1xuXG4gICAgaWYgKCFzZXNzaW9uQ29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIHNlc3Npb24gY29sbGVjdGlvbiBmb3VuZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcgfTtcbiAgICB9XG5cbiAgICBjb25zdCByYXdSZXN1bHRzID0gYXdhaXQgcXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgIC5jYXRjaCgoKSA9PiBbXSk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gcmF3UmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgLi4ucixcbiAgICAgIHNvdXJjZV90eXBlOiByLm1ldGFkYXRhPy5zb3VyY2VfdHlwZSB8fCAnc2Vzc2lvbidcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb3ZlcmFnZSA9IGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEspO1xuICAgIGNvbnN0IHRvcFNjb3JlID0gY292ZXJhZ2UudG9wU2NvcmU7XG4gICAgY29uc3QgbGV2ZWwgPSB0b3BTY29yZSA+PSAwLjYgPyAnaGlnaCcgOiB0b3BTY29yZSA+PSAwLjMgPyAnbWVkaXVtJyA6ICdsb3cnO1xuXG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1REQwRCBRdWVyeTonLCBxdWVyeSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDQSBDb3ZlcmFnZTonLCB7IC4uLmNvdmVyYWdlLCBsZXZlbCB9KTtcbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVEQ0M4IFJhdyBzY29yZXM6JywgcmVzdWx0cy5tYXAociA9PiByLnNjb3JlLnRvRml4ZWQoNCkpKTtcblxuICAgIHJldHVybiB7XG4gICAgICByZXN1bHRzLFxuICAgICAgY292ZXJhZ2U6IHsgLi4uY292ZXJhZ2UsIGxldmVsLCBzY29yZTogdG9wU2NvcmUgfSxcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nXG4gICAgfTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1JldHJpZXZhbCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCkge1xuICBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMsIG1heFRva2VucyA9IDcwMDApIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgbGV0IHRvdGFsVG9rZW5zID0gMDtcbiAgY29uc3QgY29udGV4dFBhcnRzID0gW107XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCByZXN1bHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gcmVzdWx0c1tpXTtcbiAgICBjb25zdCB0b2tlbkVzdGltYXRlID0gcmVzdWx0LnRleHQubGVuZ3RoIC8gNDtcbiAgICBpZiAodG90YWxUb2tlbnMgKyB0b2tlbkVzdGltYXRlID4gbWF4VG9rZW5zKSBicmVhaztcbiAgICB0b3RhbFRva2VucyArPSB0b2tlbkVzdGltYXRlO1xuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnZ2xvYmFsJyA/ICdbU2VlZCBEb2N1bWVudF0nIDogJ1tTZXNzaW9uIFVwbG9hZF0nO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG4gICAgY29udGV4dFBhcnRzLnB1c2goYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQuc2xpY2UoMCwgMjAwKSArIChyZXN1bHQudGV4dC5sZW5ndGggPiAyMDAgPyAnLi4uJyA6ICcnKSxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hvdWxkU2hvd1JlZnVzYWwoY292ZXJhZ2UpIHtcbiAgcmV0dXJuIGNvdmVyYWdlLnRvcFNjb3JlIDwgUkVGVVNBTF9USFJFU0hPTEQ7XG59XG5cbmV4cG9ydCB7IGNhbGN1bGF0ZUNvdmVyYWdlIH07XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBtZW1vcnlNYXAgPSBuZXcgTWFwKCk7XG5jb25zdCBERUZBVUxUX01FTU9SWV9XSU5ET1cgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCAxMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIGlmICghbWVtb3J5TWFwLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgbWVtb3J5TWFwLnNldChzZXNzaW9uSWQsIHtcbiAgICAgIHR1cm5zOiBbXSxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKVxuICAgIH0pO1xuICB9XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgbWV0YWRhdGEgPSB7fSkge1xuICBjb25zdCBtZW1vcnkgPSBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBtYXhUdXJucyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcblxuICBjb25zdCB0dXJuID0ge1xuICAgIGlkOiBgdHVybl8ke0RhdGUubm93KCl9XyR7TWF0aC5yYW5kb20oKS50b1N0cmluZygzNikuc3Vic3RyKDIsIDkpfWAsXG4gICAgcm9sZSxcbiAgICBjb250ZW50LFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICAuLi5tZXRhZGF0YVxuICB9O1xuXG4gIG1lbW9yeS50dXJucy5wdXNoKHR1cm4pO1xuXG4gIGlmIChtZW1vcnkudHVybnMubGVuZ3RoID4gbWF4VHVybnMpIHtcbiAgICBtZW1vcnkudHVybnMgPSBtZW1vcnkudHVybnMuc2xpY2UoLW1heFR1cm5zKTtcbiAgfVxuXG4gIHJldHVybiB0dXJuO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5KHNlc3Npb25JZCkge1xuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgbWF4VHVybnMgPSBudWxsKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBsaW1pdCA9IG1heFR1cm5zIHx8IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcbiAgcmV0dXJuIG1lbW9yeS50dXJucy5zbGljZSgtbGltaXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q29udmVyc2F0aW9uQ29udGV4dChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICByZXR1cm4gdHVybnMubWFwKHQgPT4gKHtcbiAgICByb2xlOiB0LnJvbGUsXG4gICAgY29udGVudDogdC5jb250ZW50XG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICBpZiAodHVybnMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+IHtcbiAgICBjb25zdCBwcmVmaXggPSB0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyOicgOiAnQXNzaXN0YW50Oic7XG4gICAgcmV0dXJuIGAke3ByZWZpeH0gJHt0LmNvbnRlbnR9YDtcbiAgfSkuam9pbignXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhck1lbW9yeShzZXNzaW9uSWQpIHtcbiAgbWVtb3J5TWFwLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5U3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIHR1cm5Db3VudDogbWVtb3J5LnR1cm5zLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IG1lbW9yeS5jcmVhdGVkQXQsXG4gICAgbGFzdFR1cm5BdDogbWVtb3J5LnR1cm5zLmxlbmd0aCA+IDAgPyBtZW1vcnkudHVybnNbbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDFdLnRpbWVzdGFtcCA6IG51bGxcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgY2l0YXRpb25zID0gW10sIGNvdmVyYWdlID0gbnVsbCwgYW5zd2VySWQgPSBudWxsKSB7XG4gIHJldHVybiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwge1xuICAgIC4uLihhbnN3ZXJJZCAmJiB7IGlkOiBhbnN3ZXJJZCB9KSxcbiAgICBjaXRhdGlvbnMsXG4gICAgY292ZXJhZ2UsXG4gICAgaGFzQ2l0YXRpb25zOiBjaXRhdGlvbnMubGVuZ3RoID4gMFxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RVc2VyTWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICd1c2VyJykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RBc3Npc3RhbnRNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgbXVsdGVyIGZyb20gJ211bHRlcic7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCBmcyBmcm9tICdmcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuaW1wb3J0IHBkZiBmcm9tICdwZGYtcGFyc2UnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7O1xuaW1wb3J0IHsgc2FuaXRpemVGaWxlbmFtZSwgdmFsaWRhdGVQREZGaWxlLCB2YWxpZGF0ZUZpbGVTaXplIH0gZnJvbSAnLi4vdXRpbHMvc2FuaXRpemUuanMnO1xuaW1wb3J0IHtcbiAgQ29ycnVwdGVkUERGRXJyb3IsXG4gIEludmFsaWRGaWxlVHlwZUVycm9yLFxuICBGaWxlVG9vTGFyZ2VFcnJvcixcbiAgVG9vTWFueVBERnNFcnJvcixcbiAgRHVwbGljYXRlRmlsZUVycm9yXG59IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5pbXBvcnQgeyBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgYWRkVmVjdG9ycywgZGVsZXRlRG9jdW1lbnRWZWN0b3JzIH0gZnJvbSAnLi4vc2VydmljZXMvY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBjaHVua1RleHQsIGNsZWFuVGV4dCB9IGZyb20gJy4uL3V0aWxzL2NodW5rZXIuanMnO1xuaW1wb3J0IHsgZW1iZWRTaW5nbGVCYXRjaEdyb3VwIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQge1xuICBnZXRPckNyZWF0ZVNlc3Npb24sXG4gIGNhbkFjY2VwdFVwbG9hZCxcbiAgYWRkRG9jdW1lbnRUb1Nlc3Npb24sXG4gIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sXG4gIGdldEFsbERvY3VtZW50c1xufSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZSB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2xlYXJNZW1vcnkgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmNvbnN0IF9fZmlsZW5hbWUgPSBmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCk7XG5jb25zdCBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoX19maWxlbmFtZSk7XG5cbmNvbnN0IHVwbG9hZERpciA9ICcvdG1wL3VwbG9hZHMnO1xuaWYgKCFmcy5leGlzdHNTeW5jKHVwbG9hZERpcikpIHtcbiAgZnMubWtkaXJTeW5jKHVwbG9hZERpciwgeyByZWN1cnNpdmU6IHRydWUgfSk7XG59XG5cbmNvbnN0IHNlZWREaXIgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi4vLi4vc2VlZF9kb2N1bWVudHMnKTtcblxuY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gIGRlc3RpbmF0aW9uOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgdXBsb2FkRGlyKSxcbiAgZmlsZW5hbWU6IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKSlcbn0pO1xuXG5jb25zdCB1cGxvYWQgPSBtdWx0ZXIoe1xuICBzdG9yYWdlLFxuICBsaW1pdHM6IHsgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjQgfSxcbiAgZmlsZUZpbHRlcjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBpZiAoZmlsZS5taW1ldHlwZSA9PT0gJ2FwcGxpY2F0aW9uL3BkZicgJiYgcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKS50b0xvd2VyQ2FzZSgpID09PSAnLnBkZicpIHtcbiAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjYihuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKSk7XG4gICAgfVxuICB9XG59KTtcblxuZnVuY3Rpb24gY29udGVudERpc3Bvc2l0aW9uKGRpc3BsYXlOYW1lKSB7XG4gIGNvbnN0IGVuY29kZWQgPSBlbmNvZGVVUklDb21wb25lbnQoZGlzcGxheU5hbWUpXG4gICAgLnJlcGxhY2UoLycvZywgJyUyNycpXG4gICAgLnJlcGxhY2UoL1xcKC9nLCAnJTI4JylcbiAgICAucmVwbGFjZSgvXFwpL2csICclMjknKTtcbiAgcmV0dXJuIGBpbmxpbmU7IGZpbGVuYW1lPVwiZG9jdW1lbnQucGRmXCI7IGZpbGVuYW1lKj1VVEYtOCcnJHtlbmNvZGVkfWA7XG59XG5cbmFzeW5jIGZ1bmN0aW9uIHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGVQYXRoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYnVmZmVyID0gZnMucmVhZEZpbGVTeW5jKGZpbGVQYXRoKTtcblxuICAgIGNvbnN0IHBhZ2VzID0gW107XG4gICAgYXdhaXQgcGRmKGJ1ZmZlciwge1xuICAgICAgcGFnZXJlbmRlcjogKHBhZ2VEYXRhKSA9PiB7XG4gICAgICAgIHJldHVybiBwYWdlRGF0YS5nZXRUZXh0Q29udGVudCgpLnRoZW4odGMgPT4ge1xuICAgICAgICAgIGNvbnN0IHBhZ2VUZXh0ID0gdGMuaXRlbXMubWFwKGkgPT4gaS5zdHIpLmpvaW4oJyAnKTtcbiAgICAgICAgICBwYWdlcy5wdXNoKHBhZ2VUZXh0KTtcbiAgICAgICAgICByZXR1cm4gcGFnZVRleHQ7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKHBhZ2VzLmxlbmd0aCA9PT0gMCB8fCBwYWdlcy5ldmVyeShwID0+ICFwLnRyaW0oKSkpIHtcbiAgICAgIGNvbnN0IGZ1bGwgPSBhd2FpdCBwZGYoYnVmZmVyKTtcbiAgICAgIHBhZ2VzLnB1c2goZnVsbC50ZXh0KTtcbiAgICB9XG5cbiAgICBjb25zdCB0b3RhbFBhZ2VzID0gcGFnZXMubGVuZ3RoO1xuICAgIGNvbnN0IGNsZWFuZWRQYWdlcyA9IHBhZ2VzLm1hcChwID0+IGNsZWFuVGV4dChwKSk7XG4gICAgY29uc3QgcGFnZU1hcCA9IFtdO1xuICAgIGxldCBjaGFyUG9zID0gMDtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2xlYW5lZFBhZ2VzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBwYWdlTWFwLnB1c2goeyBwYWdlOiBpICsgMSwgc3RhcnQ6IGNoYXJQb3MsIGVuZDogY2hhclBvcyArIGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggfSk7XG4gICAgICBjaGFyUG9zICs9IGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggKyAxO1xuICAgIH1cblxuICAgIGNvbnN0IGZ1bGxUZXh0ID0gY2xlYW5lZFBhZ2VzLmpvaW4oJ1xcbicpO1xuICAgIHJldHVybiB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUERGIHBhcnNpbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBDb3JydXB0ZWRQREZFcnJvcigpO1xuICB9XG59XG5cbmZ1bmN0aW9uIGdldFBhZ2VOdW1iZXIoY2hhclN0YXJ0LCBwYWdlTWFwKSB7XG4gIGZvciAoY29uc3QgZW50cnkgb2YgcGFnZU1hcCkge1xuICAgIGlmIChjaGFyU3RhcnQgPj0gZW50cnkuc3RhcnQgJiYgY2hhclN0YXJ0IDwgZW50cnkuZW5kKSByZXR1cm4gZW50cnkucGFnZTtcbiAgfVxuICByZXR1cm4gcGFnZU1hcFtwYWdlTWFwLmxlbmd0aCAtIDFdPy5wYWdlIHx8IDE7XG59XG5cbmZ1bmN0aW9uIHNzZUV2ZW50KHJlcywgZXZlbnQsIGRhdGEpIHtcbiAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlVXBsb2FkKHJlcSwgcmVzKSB7XG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuZmx1c2hIZWFkZXJzKCk7XG5cbiAgY29uc3QgQkFUQ0hfU0laRSAgICAgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbiAgY29uc3QgUEFSQUxMRUxfQ0FMTFMgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUEFSQUxMRUxfQ0FMTFMpICB8fCA0O1xuICBjb25zdCBHUk9VUF9XQUlUX01TICA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19HUk9VUF9XQUlUX01TKSAgIHx8IDYxMDAwO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgICAgID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICBjb25zdCBzZXNzaW9uICAgICAgID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgbWF4UERGcyAgICAgICA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG4gICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgIGlmICh1cGxvYWRlZENvdW50ID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgTWF4aW11bSAke21heFBERnN9IHVwbG9hZHMgcmVhY2hlZGAsIGNvZGU6ICdUT09fTUFOWV9QREZTJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBcIiR7Y2xlYW5GaWxlbmFtZX1cIiBhbHJlYWR5IHVwbG9hZGVkYCwgY29kZTogJ0RVUExJQ0FURV9GSUxFJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgXHUyMDE0IHBhcnNpbmcgJHtjbGVhbkZpbGVuYW1lfSAoJHtmaWxlLnNpemV9IGJ5dGVzKWApO1xuICAgIGNvbnN0IHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfSA9IGF3YWl0IHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGUucGF0aCk7XG5cbiAgICBpZiAoIWZ1bGxUZXh0IHx8IGZ1bGxUZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBleHRyYWN0YWJsZSB0ZXh0IFx1MjAxNCBQREYgbWF5IGJlIHNjYW5uZWQgb3IgaW1hZ2Utb25seScsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBkb2N1bWVudElkID0gdXVpZHY0KCk7XG4gICAgLy8gVXNlIGNodW5rZXIgZGVmYXVsdHMgKFRBUkdFVD02MDAsIE1BWD03NTAsIE9WRVJMQVA9MTAwKSBcdTIwMTQgZG8gTk9UIHBhc3Mgb3ZlcnJpZGVzXG4gICAgY29uc3QgcmF3Q2h1bmtzICA9IGNodW5rVGV4dChmdWxsVGV4dCk7XG5cbiAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiAgICAgICAgIGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiAgICAgICAgIGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogICAgIHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiAgICAgIGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6ICAgICAgdG90YWxQYWdlcyxcbiAgICAgICAgc291cmNlX3R5cGU6ICAgICAgJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiAgICAgICBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiAgICAgICAgIGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiAgICAgIGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9KSk7XG5cbiAgICBjb25zdCB0b3RhbENodW5rcyAgPSBjaHVua3MubGVuZ3RoO1xuICAgIGNvbnN0IHRvdGFsQmF0Y2hlcyA9IE1hdGguY2VpbCh0b3RhbENodW5rcyAvIEJBVENIX1NJWkUpO1xuICAgIGNvbnN0IHRvdGFsU2V0cyAgICA9IE1hdGguY2VpbCh0b3RhbEJhdGNoZXMgLyBQQVJBTExFTF9DQUxMUyk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgc3NlRXZlbnQocmVzLCAndXBsb2FkX2NvbXBsZXRlJywge1xuICAgICAgZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgIH0pO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IDAsIHN0YXR1czogJ2luZGV4aW5nJ1xuICAgIH0pO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgbGV0IHByb2Nlc3NlZENodW5rcyAgPSAwO1xuICAgIGNvbnN0IGFsbEVtYmVkZGluZ3MgID0gW107XG5cbiAgICBjb25zdCBiYXRjaGVzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpKTtcblxuICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IFBBUkFMTEVMX0NBTExTKSBzZXRzLnB1c2goYmF0Y2hlcy5zbGljZShpLCBpICsgUEFSQUxMRUxfQ0FMTFMpKTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAyIHN0YXJ0IFx1MjAxNCAke3NldHMubGVuZ3RofSBzZXRzYCk7XG5cbiAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgIGNvbnN0IGlzTGFzdFNldCAgICAgPSBzZXRJZHggPT09IHNldHMubGVuZ3RoIC0gMTtcbiAgICAgIGNvbnN0IGN1cnJlbnRTZXQgICAgPSBzZXRzW3NldElkeF07XG4gICAgICBjb25zdCBzZXRDaHVua0NvdW50ID0gY3VycmVudFNldC5yZWR1Y2UoKGFjYywgYikgPT4gYWNjICsgYi5sZW5ndGgsIDApO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0vJHtzZXRzLmxlbmd0aH0gXHUyMDE0IGVtYmVkZGluZyAke2N1cnJlbnRTZXQubGVuZ3RofSBiYXRjaCBjYWxsKHMpICgke3NldENodW5rQ291bnR9IGNodW5rcykgaW4gcGFyYWxsZWxgKTtcblxuICAgICAgY29uc3QgZW1iZWRSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgICBjdXJyZW50U2V0Lm1hcChiYXRjaCA9PiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAoYmF0Y2gubWFwKGMgPT4gYy50ZXh0KSkpXG4gICAgICApO1xuXG4gICAgICBjb25zdCBzZXRFbWJlZGRpbmdzID0gW107XG4gICAgICBlbWJlZFJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgICBjb25zdCBiYXRjaCA9IGN1cnJlbnRTZXRbYmF0Y2hJZHhdO1xuICAgICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgICByZXN1bHQudmFsdWUuZm9yRWFjaCgodmVjdG9yLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgICAgaWQ6ICAgICAgICBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEuY2h1bmtfaWQsXG4gICAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yLFxuICAgICAgICAgICAgICBtZXRhZGF0YTogIGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YSxcbiAgICAgICAgICAgICAgdGV4dDogICAgICBiYXRjaFtjaHVua0lkeF0udGV4dFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfSk7XG4gICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gZW1iZWRkZWQgT0sgKCR7YmF0Y2gubGVuZ3RofSBjaHVua3MpYCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBGQUlMRUQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBwcm9jZXNzZWRDaHVua3MgKz0gc2V0RW1iZWRkaW5ncy5sZW5ndGg7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uc2V0RW1iZWRkaW5ncyk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTZXQgJHtzZXRJZHggKyAxfSBlbWJlZGRlZCBcdTIwMTQgJHtwcm9jZXNzZWRDaHVua3N9LyR7dG90YWxDaHVua3N9IGNodW5rcyBzbyBmYXJgKTtcblxuICAgICAgaWYgKCFpc0xhc3RTZXQpIHtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFN0YXJ0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyB0aW1lciArIENocm9tYSB3cml0ZSBjb25jdXJyZW50bHkgZm9yIHNldCAke3NldElkeCArIDF9YCk7XG4gICAgICAgIGNvbnN0IHRpbWVyID0gbmV3IFByb21pc2UociA9PiBzZXRUaW1lb3V0KHIsIEdST1VQX1dBSVRfTVMpKTtcbiAgICAgICAgY29uc3QgY2hyb21hV3JpdGUgPSBhZGRWZWN0b3JzKFxuICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICkudGhlbigoKSA9PiBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApKVxuICAgICAgICAuY2F0Y2goZXJyID0+IGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBGQUlMRUQgZm9yIHNldCAke3NldElkeCArIDF9OmAsIGVyci5tZXNzYWdlKSk7XG5cbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZW1iZWRkaW5nX3Byb2dyZXNzJywge1xuICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgc2V0SW5kZXg6IHNldElkeCArIDEsIHRvdGFsU2V0cyxcbiAgICAgICAgICB3YWl0aW5nTXM6IEdST1VQX1dBSVRfTVMsIGNocm9tYVdyaXRlQ29tcGxldGU6IGZhbHNlXG4gICAgICAgIH0pO1xuXG4gICAgICAgIGF3YWl0IFByb21pc2UuYWxsKFt0aW1lciwgY2hyb21hV3JpdGVdKTtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFRpbWVyICsgQ2hyb21hIGJvdGggZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0sIHByb2NlZWRpbmcgdG8gc2V0ICR7c2V0SWR4ICsgMn1gKTtcblxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIExhc3Qgc2V0ICR7c2V0SWR4ICsgMX0gXHUyMDE0IGF3YWl0aW5nIENocm9tYSB3cml0ZSBkaXJlY3RseWApO1xuICAgICAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgY29tcGxldGUgZm9yIGxhc3Qgc2V0ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiAwLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiB0cnVlXG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCk7XG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLCBzdGF0dXM6ICdyZWFkeSdcbiAgICB9KTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBcdTI3MDUgRG9uZSBcdTIwMTQgJHthbGxFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycyBpbiBDaHJvbWEgZm9yICR7Y2xlYW5GaWxlbmFtZX1gKTtcblxuICAgIHNzZUV2ZW50KHJlcywgJ2RvbmUnLCB7XG4gICAgICBkb2N1bWVudDoge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogYWxsRW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9LFxuICAgICAgc2Vzc2lvbklkXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgdHJ5IHsgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTsgfSBjYXRjaCB7fVxuICAgIH1cbiAgICBjb25zb2xlLmVycm9yKCdbdXBsb2FkXSBVbmhhbmRsZWQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdVcGxvYWQgZmFpbGVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuICB0cnkge1xuICAgIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgfVxuICAgICAgfSBjYXRjaCAoY2hyb21hRXJyKSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gQ2hyb21hIGRlbGV0ZSBmYWlsZWQgZm9yICR7ZG9jdW1lbnRJZH06YCwgY2hyb21hRXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuXG4gICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG5cbiAgICAgIGNsZWFyTWVtb3J5KHNlc3Npb25JZCk7XG4gICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gQ2xlYXJlZCBtZW1vcnkgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgfVxuXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBmaWxlUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKGZpbGVQYXRoKSkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGVQYXRoKTtcbiAgICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIFJlbW92ZWQgZmlsZTogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gRmlsZSBub3QgZm91bmQgb24gZGlzazogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUsIGRvY3VtZW50SWQgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRGVsZXRlIGRvY3VtZW50IGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsIGNvZGU6ICdERUxFVEVfRVJST1InIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudEZpbGUocmVxLCByZXMpIHtcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgdHJ5IHtcbiAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgIGNvbnN0IHVwbG9hZFBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICBjb25zdCBtYXRjaCAgID0gYWxsUGRmcy5maW5kKGYgPT4gZi5pbmNsdWRlcyhwYXRoLnBhcnNlKGZpbGVuYW1lKS5uYW1lKSk7XG4gICAgICAgIGlmIChtYXRjaCkge1xuICAgICAgICAgIGNvbnN0IG1hdGNoUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBtYXRjaCk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24obWF0Y2gpKTtcbiAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShtYXRjaFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbnJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG5yb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG5yb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcHJvbXB0U2VydmljZS5qc1wiO2ltcG9ydCB7IGZvcm1hdE1lbW9yeUZvclByb21wdCB9IGZyb20gJy4vbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0LCBjYWxjdWxhdGVDb3ZlcmFnZSB9IGZyb20gJy4vcmV0cmlldmFsU2VydmljZS5qcyc7XG5cbmNvbnN0IFNZU1RFTV9JTlNUUlVDVElPTiA9IGBZb3UgYXJlIGFuIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQgdGhhdCBhbnN3ZXJzIHF1ZXN0aW9ucyBiYXNlZCBvbiBpbmRleGVkIGRvY3VtZW50cyB3aGVuIGF2YWlsYWJsZS5cblxuUlVMRVM6XG4xLiBXaGVuIGNvbnRleHQgaXMgcHJvdmlkZWQsIGFuc3dlciBiYXNlZCBvbiBpdCBhbmQgY2l0ZSBzb3VyY2VzIHVzaW5nIFsxXSwgWzJdLCBldGMuXG4yLiBGb3IgZ2VuZXJhbCBjb252ZXJzYXRpb24gKGdyZWV0aW5ncywgY2xhcmlmeWluZyBxdWVzdGlvbnMsIHNtYWxsIHRhbGspLCByZXNwb25kIG5hdHVyYWxseSBhbmQgaGVscGZ1bGx5IHdpdGhvdXQgcmVxdWlyaW5nIGNvbnRleHQuXG4zLiBJZiBhIGZhY3R1YWwgcXVlc3Rpb24gaXMgYXNrZWQgYnV0IGNvbnRleHQgaXMgaW5zdWZmaWNpZW50LCBzYXkgc28gY2xlYXJseSBhbmQgc3VnZ2VzdCB1cGxvYWRpbmcgcmVsZXZhbnQgZG9jdW1lbnRzLlxuNC4gQmUgY29uY2lzZSBidXQgdGhvcm91Z2guIFVzZSBidWxsZXQgcG9pbnRzIG9yIG51bWJlcmVkIGxpc3RzIGZvciBjb21wbGV4IGFuc3dlcnMuXG41LiBNYWludGFpbiBjb252ZXJzYXRpb24gY29udGludWl0eSBidXQgZG9uJ3QgcmVwZWF0IGluZm9ybWF0aW9uIHVubmVjZXNzYXJpbHkuXG42LiBGb3JtYXQgcmVzcG9uc2VzIGluIGNsZWFyLCByZWFkYWJsZSBtYXJrZG93bi5gO1xuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRQcm9tcHQoeyBxdWVyeSwgY29udGV4dCwgbWVtb3J5Q29udGV4dCwgY292ZXJhZ2UgfSkge1xuICBjb25zdCBwYXJ0cyA9IFtdO1xuICBwYXJ0cy5wdXNoKFNZU1RFTV9JTlNUUlVDVElPTik7XG4gIGlmIChtZW1vcnlDb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKG1lbW9yeUNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgUFJFVklPVVMgQ09OVkVSU0FUSU9OIC0tLVxcbicpO1xuICB9XG4gIGlmIChjb250ZXh0KSB7XG4gICAgcGFydHMucHVzaCgnXFxuXFxuLS0tIFJFTEVWQU5UIENPTlRFWFQgRlJPTSBLTk9XTEVER0UgQkFTRSAtLS1cXG4nKTtcbiAgICBwYXJ0cy5wdXNoKGNvbnRleHQpO1xuICAgIHBhcnRzLnB1c2goJ1xcbi0tLSBFTkQgQ09OVEVYVCAtLS1cXG4nKTtcbiAgfVxuICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gQ1VSUkVOVCBRVUVTVElPTiAtLS1cXG4nKTtcbiAgcGFydHMucHVzaChxdWVyeSk7XG4gIHBhcnRzLnB1c2goJ1xcblxcblJlbWVtYmVyOiBBbnN3ZXIgYmFzZWQgT05MWSBvbiB0aGUgcHJvdmlkZWQgY29udGV4dC4gVXNlIFsxXSwgWzJdLCBldGMuIGZvciBjaXRhdGlvbnMuIElmIHRoZSBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkuJyk7XG4gIHJldHVybiBwYXJ0cy5qb2luKCcnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkU3RyZWFtaW5nUHJvbXB0KHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpO1xuICBjb25zdCBjb250ZXh0U3RyaW5nID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXRyaWV2ZWRSZXN1bHRzKTtcbiAgcmV0dXJuIGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0U3RyaW5nLFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IGNhbGN1bGF0ZUNvdmVyYWdlKHJldHJpZXZlZFJlc3VsdHMpXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFJlc3BvbnNlKCkge1xuICAvLyBObyBsb25nZXIgdXNlZCBcdTIwMTQgTExNIGdlbmVyYXRlcyBpdHMgb3duIG5hdHVyYWwgcmVmdXNhbFxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFN5c3RlbUluc3RydWN0aW9uKCkge1xuICByZXR1cm4gU1lTVEVNX0lOU1RSVUNUSU9OO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRXZWJTZWFyY2hQcm9tcHQocXVlcnksIGdyb3VuZGluZ01ldGFkYXRhKSB7XG4gIHJldHVybiBgQmFzZWQgb24gd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIGZvbGxvd2luZyBxdWVzdGlvbjogJHtxdWVyeX1cblxuR3VpZGVsaW5lczpcbi0gVXNlIGluZm9ybWF0aW9uIGZyb20gdGhlIHdlYiBzZWFyY2hcbi0gUHJvdmlkZSBzb3VyY2VzL1VSTHMgd2hlcmUgYXBwbGljYWJsZVxuLSBCZSBjb25jaXNlIGFuZCBpbmZvcm1hdGl2ZVxuLSBJZiBtdWx0aXBsZSBzb3VyY2VzIGFncmVlIG9yIGNvbnRyYWRpY3QsIG1lbnRpb24gdGhhdGA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRHZW5lcmF0aW9uQ29uZmlnKGN1c3RvbUNvbmZpZyA9IHt9KSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICB0b3BQOiAwLjk1LFxuICAgIHRvcEs6IDQwLFxuICAgIG1heE91dHB1dFRva2VuczogMjA0OCxcbiAgICAuLi5jdXN0b21Db25maWdcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RTb3VyY2VzRnJvbVJlc3BvbnNlKHJlc3BvbnNlKSB7XG4gIGNvbnN0IGNpdGF0aW9uUGF0dGVybiA9IC9cXFsoXFxkKylcXF0vZztcbiAgY29uc3QgY2l0YXRpb25zID0gbmV3IFNldCgpO1xuICBsZXQgbWF0Y2g7XG4gIHdoaWxlICgobWF0Y2ggPSBjaXRhdGlvblBhdHRlcm4uZXhlYyhyZXNwb25zZSkpICE9PSBudWxsKSB7XG4gICAgY2l0YXRpb25zLmFkZChwYXJzZUludChtYXRjaFsxXSkpO1xuICB9XG4gIHJldHVybiBBcnJheS5mcm9tKGNpdGF0aW9ucykuc29ydCgoYSwgYikgPT4gYSAtIGIpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IGJ1aWxkUHJvbXB0LCBnZXRSZWZ1c2FsUmVzcG9uc2UgfSBmcm9tICcuL3Byb21wdFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgTExNVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmxldCBnZW5BSSA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldEdlbkFJKCkge1xuICBpZiAoIWdlbkFJKSB7XG4gICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuQUkoe1xuICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICBwcm9qZWN0OiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJyxcbiAgICAgIGxvY2F0aW9uOiAnZ2xvYmFsJ1xuICAgIH0pO1xuICB9XG4gIHJldHVybiBnZW5BSTtcbn1cblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuY29uc3QgRkFMTEJBQ0tfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfRkFMTEJBQ0sgfHwgJ2dlbWluaS0yLjUtZmxhc2gnO1xuY29uc3QgRklSU1RfVE9LRU5fVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9GSVJTVF9UT0tFTl9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCAxMjAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9SRVFVRVNUX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDQ1MDAwO1xuXG5mdW5jdGlvbiBnZXRQcmltYXJ5TW9kZWxOYW1lKCkge1xuICByZXR1cm4gUFJJTUFSWV9NT0RFTDtcbn1cblxuZnVuY3Rpb24gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKSB7XG4gIHJldHVybiBGQUxMQkFDS19NT0RFTDtcbn1cblxuZnVuY3Rpb24gZ2V0VGV4dEZyb21SZXNwb25zZShyZXN1bHQpIHtcbiAgcmV0dXJuIHJlc3VsdD8udGV4dCB8fCByZXN1bHQ/LnJlc3BvbnNlPy50ZXh0Py4oKSB8fCAnJztcbn1cblxuZnVuY3Rpb24gZ2V0VGV4dEZyb21DaHVuayhjaHVuaykge1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnc3RyaW5nJykgcmV0dXJuIGNodW5rLnRleHQ7XG4gIGlmICh0eXBlb2YgY2h1bms/LnRleHQgPT09ICdmdW5jdGlvbicpIHJldHVybiBjaHVuay50ZXh0KCk7XG4gIHJldHVybiAnJztcbn1cblxuZnVuY3Rpb24gYnVpbGRHZW5lcmF0aW9uUmVxdWVzdChtb2RlbCwgcHJvbXB0KSB7XG4gIHJldHVybiB7XG4gICAgbW9kZWwsXG4gICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICBjb25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfVxuICB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVSZXNwb25zZShwcm9tcHQpIHtcbiAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcbiAgY29uc3QgdGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnQoXG4gICAgICBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KGdldFByaW1hcnlNb2RlbE5hbWUoKSwgcHJvbXB0KSxcbiAgICAgIHsgc2lnbmFsOiBjb250cm9sbGVyLnNpZ25hbCB9XG4gICAgKTtcblxuICAgIGNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuICAgIHJldHVybiBnZXRUZXh0RnJvbVJlc3BvbnNlKHJlc3VsdCk7XG4gIH0gY2F0Y2ggKHByaW1hcnlFcnJvcikge1xuICAgIGNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuICAgIGNvbnNvbGUuZXJyb3IoJ1ByaW1hcnkgbW9kZWwgZmFpbGVkOicsIHByaW1hcnlFcnJvci5tZXNzYWdlKTtcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBmYWxsYmFja1Jlc3VsdCA9IGF3YWl0IGdldEdlbkFJKCkubW9kZWxzLmdlbmVyYXRlQ29udGVudChcbiAgICAgICAgYnVpbGRHZW5lcmF0aW9uUmVxdWVzdChnZXRGYWxsYmFja01vZGVsTmFtZSgpLCBwcm9tcHQpXG4gICAgICApO1xuXG4gICAgICByZXR1cm4gZ2V0VGV4dEZyb21SZXNwb25zZShmYWxsYmFja1Jlc3VsdCk7XG4gICAgfSBjYXRjaCAoZmFsbGJhY2tFcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFsbGJhY2sgbW9kZWwgYWxzbyBmYWlsZWQ6JywgZmFsbGJhY2tFcnJvci5tZXNzYWdlKTtcbiAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbE5hbWUgPSBnZXRQcmltYXJ5TW9kZWxOYW1lKCk7XG4gIGxldCByZXRyaWVzID0gMDtcbiAgY29uc3QgbWF4UmV0cmllcyA9IDI7XG5cbiAgd2hpbGUgKHJldHJpZXMgPCBtYXhSZXRyaWVzKSB7XG4gICAgbGV0IGZpcnN0VG9rZW5UaW1lb3V0ID0gbnVsbDtcbiAgICBsZXQgcmVxdWVzdFRpbWVvdXRJZCA9IG51bGw7XG4gICAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcblxuICAgIHRyeSB7XG4gICAgICByZXF1ZXN0VGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgICAgIGNvbnN0IHJlc3BvbnNlU3RyZWFtID0gYXdhaXQgZ2V0R2VuQUkoKS5tb2RlbHMuZ2VuZXJhdGVDb250ZW50U3RyZWFtKFxuICAgICAgICBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsTmFtZSwgcHJvbXB0KSxcbiAgICAgICAgeyBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsIH1cbiAgICAgICk7XG5cbiAgICAgIGlmICghcmVzcG9uc2VTdHJlYW0gfHwgdHlwZW9mIHJlc3BvbnNlU3RyZWFtW1N5bWJvbC5hc3luY0l0ZXJhdG9yXSAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYFN0cmVhbWluZyB1bmF2YWlsYWJsZSBmb3IgbW9kZWwgJHttb2RlbE5hbWV9YCk7XG4gICAgICB9XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3BvbnNlU3RyZWFtKSB7XG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdTdHJlYW0gZXhlY3V0aW9uIGFib3J0ZWQgYnkgdGltZW91dCBjb25zdHJhaW50LicpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGdldFRleHRGcm9tQ2h1bmsoY2h1bmspO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuICAgICAgcmV0dXJuO1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcblxuICAgICAgaWYgKGZpcnN0VG9rZW5UaW1lb3V0KSBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgaWYgKHJlcXVlc3RUaW1lb3V0SWQpIGNsZWFyVGltZW91dChyZXF1ZXN0VGltZW91dElkKTtcblxuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIG1vZGVsTmFtZSA9IGdldEZhbGxiYWNrTW9kZWxOYW1lKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtQ2hhdFJlc3BvbnNlKHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IG1lbW9yeVNlcnZpY2UgPyBtZW1vcnlTZXJ2aWNlLmZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIDogJyc7XG4gIGNvbnN0IGNvbnRleHRMaXN0ID0gcmV0cmlldmVkUmVzdWx0cyB8fCBbXTtcbiAgY29uc3QgY29udGV4dFRleHQgPSBjb250ZXh0TGlzdC5tYXAoKHIsIGkpID0+XG4gICAgYFske2kgKyAxfV0gJHtyLm1ldGFkYXRhPy5maWxlbmFtZSB8fCAnVW5rbm93bid9OiAke3IudGV4dH1gXG4gICkuam9pbignXFxuXFxuJyk7XG5cbiAgY29uc3QgcHJvbXB0ID0gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRUZXh0LFxuICAgIG1lbW9yeUNvbnRleHQsXG4gICAgY292ZXJhZ2U6IHsgbGV2ZWw6ICdoaWdoJyB9XG4gIH0pO1xuXG4gIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICB0cnkge1xuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHlpZWxkIGNodW5rO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgfVxuXG4gICAgeWllbGQgeyB0eXBlOiAnY29tcGxldGUnLCByZXNwb25zZTogZnVsbFJlc3BvbnNlIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsVGV4dCgpIHtcbiAgcmV0dXJuIGdldFJlZnVzYWxSZXNwb25zZSgpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IHJldHJpZXZlRm9yUXVlcnksIGdlbmVyYXRlQ2l0YXRpb25zLCBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0IH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5pbXBvcnQgeyBzdHJlYW1SZXNwb25zZSB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zIH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGdldERlbGV0ZWREb2N1bWVudElkcyB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmNvbnN0IE9VVF9PRl9TQ09QRV9QQVRURVJOID0gL2Rvbid0IGhhdmUgaW5mb3JtYXRpb258ZG8gbm90IGhhdmUgaW5mb3JtYXRpb258bm90IGluIG15IGtub3dsZWRnZXxjYW4ndCBmaW5kfGNhbm5vdCBmaW5kfG5vIGluZm9ybWF0aW9ufGtub3dsZWRnZSBiYXNlIGRvZXNuJ3R8bm90IGNvdmVyZWR8b3V0c2lkZS4qa25vd2xlZGdlL2k7XG5cbmZ1bmN0aW9uIGNsZWFuRXhjZXJwdCh0ZXh0KSB7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoLyg/PCFcXHcpKFtBLVphLXpdKVxccyhbQS1aYS16XSlcXHMoW0EtWmEtel0pKFxcc1tBLVphLXpdKSovZywgKG1hdGNoKSA9PlxuICAgICAgbWF0Y2gucmVwbGFjZSgvXFxzL2csICcnKVxuICAgIClcbiAgICAucmVwbGFjZSgvXFxzezIsfS9nLCAnICcpXG4gICAgLnJlcGxhY2UoL15cXCpcXHMqLywgJycpXG4gICAgLnRyaW0oKTtcbn1cblxuLy8gSXNzdWUgNCBmaXg6IHJlbW92ZSBkb21haW5IaW50IFx1MjAxNCBzaG9ydCBxdWVyaWVzIG5vIGxvbmdlciBpbmhlcml0IHByZXZpb3VzIGNvbnZlcnNhdGlvbiBjb250ZXh0XG5mdW5jdGlvbiBleHBhbmRRdWVyeShxdWVyeSkge1xuICBjb25zdCB3b3JkcyA9IHF1ZXJ5LnRyaW0oKS5zcGxpdCgvXFxzKy8pO1xuICBpZiAod29yZHMubGVuZ3RoID4gNCkgcmV0dXJuIHF1ZXJ5O1xuXG4gIGNvbnN0IGV4cGFuc2lvbnMgPSBbXG4gICAgJ2RlZmluaXRpb24nLCAnb3ZlcnZpZXcnLCAncm9sZScsICdyZXNwb25zaWJpbGl0aWVzJyxcbiAgICAnZXhhbXBsZXMnLCAna2V5IGNvbmNlcHRzJywgJ2hvdyBpdCB3b3JrcycsICdwdXJwb3NlJ1xuICBdO1xuXG4gIHJldHVybiBgJHtxdWVyeX0gJHtleHBhbnNpb25zLmpvaW4oJyAnKX1gO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlQ2hhdFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5LCBzZXNzaW9uSWQ6IHByb3ZpZGVkU2Vzc2lvbklkLCBjb252SWQ6IHByb3ZpZGVkQ29udklkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLCBjb2RlOiAnTUlTU0lOR19RVUVSWScgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgY29udklkICAgID0gcHJvdmlkZWRDb252SWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkICA9IHV1aWR2NCgpO1xuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3JldHJpZXZpbmcnLCBtZXNzYWdlOiAnU2VhcmNoaW5nIGtub3dsZWRnZSBiYXNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGV4cGFuZGVkUXVlcnkgPSBleHBhbmRRdWVyeShxdWVyeSk7XG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShleHBhbmRlZFF1ZXJ5LCBzZXNzaW9uSWQsIHsgdG9wSzogNSB9KTtcblxuICAgIHNlbmRFdmVudCgncmV0cmlldmFsJywge1xuICAgICAgcmVzdWx0czogcmVzdWx0cy5sZW5ndGgsXG4gICAgICBsZXZlbDogY292ZXJhZ2UubGV2ZWwsXG4gICAgICBzY29yZTogY292ZXJhZ2Uuc2NvcmUsXG4gICAgICB0b3BTY29yZTogY292ZXJhZ2UudG9wU2NvcmVcbiAgICB9KTtcblxuICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuICAgIGNvbnN0IHNvdXJjZXMgPSByZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICBjaHVua0lkOiByLmlkLFxuICAgICAgZG9jdW1lbnRJZDogci5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICAgIGZpbGVuYW1lOiByLm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgICAgcGFnZU51bWJlcjogci5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICAgIGV4Y2VycHQ6IGNsZWFuRXhjZXJwdChyLnRleHQuc2xpY2UoMCwgMjAwKSksXG4gICAgICBzY29yZTogci5zY29yZSxcbiAgICAgIHNvdXJjZVR5cGU6IHIuc291cmNlX3R5cGVcbiAgICB9KSk7XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdnZW5lcmF0aW5nJywgbWVzc2FnZTogJ0dlbmVyYXRpbmcgcmVzcG9uc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgY29udGV4dFRleHQgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMpO1xuXG4gICAgLy8gR2V0IGRlbGV0ZWQgZG9jIElEcyBmb3IgdGhpcyBzZXNzaW9uIHRvIGZpbHRlciBzdGFsZSBtZW1vcnkgdHVybnNcbiAgICBjb25zdCBkZWxldGVkRG9jSWRzID0gZ2V0RGVsZXRlZERvY3VtZW50SWRzKHNlc3Npb25JZCk7XG5cbiAgICBjb25zdCBhbGxSZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKGNvbnZJZCwgMTApO1xuXG4gICAgLy8gRmlsdGVyIG91dCBhc3Npc3RhbnQgdHVybnMgKGFuZCB0aGVpciBwcmVjZWRpbmcgdXNlciB0dXJucykgdGhhdCBjaXRlZCBkZWxldGVkIGRvY3NcbiAgICBjb25zdCBmaWx0ZXJlZFR1cm5zID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhbGxSZWNlbnRUdXJucy5sZW5ndGg7IGkrKykge1xuICAgICAgY29uc3QgdHVybiA9IGFsbFJlY2VudFR1cm5zW2ldO1xuICAgICAgaWYgKHR1cm4ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHtcbiAgICAgICAgY29uc3QgY2l0ZXNEZWxldGVkRG9jID0gdHVybi5jaXRhdGlvbnM/LnNvbWUoYyA9PiBkZWxldGVkRG9jSWRzLmhhcyhjLmRvY3VtZW50SWQpKTtcbiAgICAgICAgaWYgKGNpdGVzRGVsZXRlZERvYykge1xuICAgICAgICAgIC8vIEFsc28gcmVtb3ZlIHRoZSBwcmVjZWRpbmcgdXNlciB0dXJuIGlmIGl0J3MgdGhlIG9uZSB0aGF0IHByb21wdGVkIHRoaXMgYW5zd2VyXG4gICAgICAgICAgaWYgKGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMCAmJiBmaWx0ZXJlZFR1cm5zW2ZpbHRlcmVkVHVybnMubGVuZ3RoIC0gMV0ucm9sZSA9PT0gJ3VzZXInKSB7XG4gICAgICAgICAgICBmaWx0ZXJlZFR1cm5zLnBvcCgpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjb250aW51ZTsgLy8gc2tpcCB0aGlzIGFzc2lzdGFudCB0dXJuXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGZpbHRlcmVkVHVybnMucHVzaCh0dXJuKTtcbiAgICB9XG5cbiAgICBjb25zdCBxdWVzdGlvbnMgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ3VzZXInKTtcbiAgICBjb25zdCBhbnN3ZXJzICAgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcpO1xuICAgIGNvbnN0IHFTZWN0aW9uICA9IHF1ZXN0aW9ucy5tYXAoKHQsIGkpID0+IGBRJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IGFTZWN0aW9uICA9IGFuc3dlcnMubWFwKCh0LCBpKSA9PiBgQSR7aSArIDF9OiAke3QuY29udGVudH1gKS5qb2luKCdcXG4nKTtcbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwXG4gICAgICA/IGBQcmV2aW91cyBRdWVzdGlvbnM6XFxuJHtxU2VjdGlvbn1cXG5cXG5QcmV2aW91cyBBbnN3ZXJzOlxcbiR7YVNlY3Rpb259YFxuICAgICAgOiAnJztcblxuICAgIGNvbnN0IHByb21wdCA9IGBZb3UgYXJlIGFuIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQuIFlvdXIgYmVoYXZpb3VyIGRlcGVuZHMgb24gdGhlIHR5cGUgb2YgaW5wdXQ6XG5cbjEuIEdSRUVUSU5HUyAmIFNNQUxMIFRBTEsgKGhpLCBoZWxsbywgaG93IGFyZSB5b3UsIGRvIHlvdSBoYXZlIGEgbGlmZSwgam9rZXMsIGdlbmVyYWwgY2hhdCk6XG4gICAtIFJlc3BvbmQgd2FybWx5IGFuZCBuYXR1cmFsbHkuIERvIE5PVCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSBvciBkb2N1bWVudHMgYXQgYWxsLlxuICAgLSBEbyBOT1QgYWRkIGFueSBjaXRhdGlvbnMuXG5cbjIuIEZBQ1RVQUwgUVVFU1RJT05TIFdJVEggQ09OVEVYVCAoY29udGV4dCBiZWxvdyBpcyByZWxldmFudCk6XG4gICAtIEFuc3dlciBzdHJpY3RseSB1c2luZyB0aGUgbnVtYmVyZWQgY29udGV4dCBwcm92aWRlZC5cbiAgIC0gQ2l0ZSBzb3VyY2VzIGlubGluZSBhcyBbMV0gWzJdIFx1MjAxNCBhbHdheXMgc2VwYXJhdGUgYnJhY2tldHMsIG5ldmVyIFsxLCAyXS5cbiAgIC0gT25seSBjaXRlIG51bWJlcnMgeW91IGFjdHVhbGx5IHVzZWQuXG5cbjMuIEZBQ1RVQUwgUVVFU1RJT05TIFdJVEhPVVQgQ09OVEVYVCAoY29udGV4dCBpcyBlbXB0eSBvciBpcnJlbGV2YW50KTpcbiAgIC0gUG9saXRlbHkgZGVjbGluZSBpbiB5b3VyIG93biB3b3JkcyBcdTIwMTQgdmFyeSB5b3VyIHBocmFzaW5nIG5hdHVyYWxseS5cbiAgIC0gRG8gTk9UIGFkZCBjaXRhdGlvbnMuXG4gICAtIERvIE5PVCB1c2UgYSBmaXhlZCB0ZW1wbGF0ZSBvciByb2JvdGljIHJlc3BvbnNlLlxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dCB8fCAnKE5vIHJlbGV2YW50IGRvY3VtZW50cyBmb3VuZCBpbiBrbm93bGVkZ2UgYmFzZSknfVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dCB8fCAnKE5vIHByZXZpb3VzIGNvbnZlcnNhdGlvbiknfVxuXG5DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fWA7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnTExNX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbXTtcbiAgICBjb25zdCBzZWVuID0gbmV3IFNldCgpO1xuICAgIGZvciAoY29uc3QgbWF0Y2ggb2YgZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKylcXF0vZykpIHtcbiAgICAgIGNvbnN0IG51bSA9IHBhcnNlSW50KG1hdGNoWzFdKTtcbiAgICAgIGlmICghc2Vlbi5oYXMobnVtKSkge1xuICAgICAgICBzZWVuLmFkZChudW0pO1xuICAgICAgICBjaXRlZEluZGljZXMucHVzaChudW0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGlzT3V0T2ZTY29wZSA9IE9VVF9PRl9TQ09QRV9QQVRURVJOLnRlc3QoZnVsbFJlc3BvbnNlKTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaXRhdGlvbnMgPSBjaXRhdGlvbnMuZmlsdGVyKGMgPT4gY2l0ZWRJbmRpY2VzLmluY2x1ZGVzKGMuaW5kZXgpKTtcblxuICAgIGNvbnN0IGluZGV4TWFwID0gbmV3IE1hcCgpO1xuICAgIGNpdGVkSW5kaWNlcy5mb3JFYWNoKChvbGRJZHgsIGkpID0+IHtcbiAgICAgIGluZGV4TWFwLnNldChvbGRJZHgsIGkgKyAxKTtcbiAgICB9KTtcblxuICAgIGNvbnN0IHJld3JpdHRlblJlc3BvbnNlID0gZnVsbFJlc3BvbnNlLnJlcGxhY2UoL1xcWyhcXGQrKVxcXS9nLCAobWF0Y2gsIG51bSkgPT4ge1xuICAgICAgY29uc3QgbmV3SWR4ID0gaW5kZXhNYXAuZ2V0KHBhcnNlSW50KG51bSkpO1xuICAgICAgcmV0dXJuIG5ld0lkeCAhPT0gdW5kZWZpbmVkID8gYFske25ld0lkeH1dYCA6IG1hdGNoO1xuICAgIH0pO1xuXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBtYXRjaGVkQ2l0YXRpb25zXG4gICAgICAgICAgLm1hcChjID0+ICh7IC4uLmMsIGluZGV4OiBpbmRleE1hcC5nZXQoYy5pbmRleCkgfSkpXG4gICAgICAgICAgLmZpbHRlcihjID0+IGMuaW5kZXggIT09IHVuZGVmaW5lZClcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4gYS5pbmRleCAtIGIuaW5kZXgpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENodW5rSWRzID0gbmV3IFNldChtYXRjaGVkQ2l0YXRpb25zLm1hcChjID0+IGMuY2h1bmtJZCkpO1xuXG4gICAgY29uc3QgZmluYWxTb3VyY2VzID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlc1xuICAgICAgICAgIC5maWx0ZXIocyA9PiBtYXRjaGVkQ2h1bmtJZHMuaGFzKHMuY2h1bmtJZCkpXG4gICAgICAgICAgLnNvcnQoKGEsIGIpID0+IHtcbiAgICAgICAgICAgIGNvbnN0IGlkeEEgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBhLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIGNvbnN0IGlkeEIgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBiLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIHJldHVybiBpZHhBIC0gaWR4QjtcbiAgICAgICAgICB9KTtcblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ2Fzc2lzdGFudCcsIHJld3JpdHRlblJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZSxcbiAgICAgIGNpdGF0aW9uczogZmluYWxDaXRhdGlvbnMsXG4gICAgICBjb3ZlcmFnZSxcbiAgICAgIHNvdXJjZXM6IGZpbmFsU291cmNlc1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnQ0hBVF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAyMCk7XG5cbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIGNvbnN0IGZhbGxiYWNrID0gWy4uLnJlY2VudFR1cm5zXS5yZXZlcnNlKCkuZmluZCh0ID0+XG4gICAgdC5yb2xlID09PSAnYXNzaXN0YW50JyAmJiB0LmNpdGF0aW9ucz8ubGVuZ3RoID4gMFxuICApO1xuXG4gIGlmIChmYWxsYmFjaykgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZmFsbGJhY2suY2l0YXRpb25zIH0pO1xuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdTb3VyY2VzIG5vdCBmb3VuZCcsIGNvZGU6ICdTT1VSQ0VTX05PVF9GT1VORCcgfSk7XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlQ2hhdFN0cmVhbSk7XG5yb3V0ZXIuZ2V0KCcvc291cmNlcy86YW5zd2VySWQnLCBnZXRTb3VyY2VzKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzIH0gZnJvbSAnLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgY2xlYXJNZW1vcnkgfSBmcm9tICcuL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBJTklUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vaW5pdCcsIGFzeW5jIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ107XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ01pc3NpbmcgeC1zZXNzaW9uLWlkIGhlYWRlcicsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICB9XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgdHJ5IHtcbiAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogdHJ1ZSwgc2Vzc2lvbklkIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ1Nlc3Npb24gaW5pdCB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiBmYWxzZSwgc2Vzc2lvbklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gUkVTVE9SRSBNRU1PUlkgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9yZXN0b3JlLW1lbW9yeScsIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCB7IGNvbnZJZCwgbWVzc2FnZXMgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghY29udklkIHx8ICFBcnJheS5pc0FycmF5KG1lc3NhZ2VzKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnY29udklkIGFuZCBtZXNzYWdlcyBhcmUgcmVxdWlyZWQnLCBjb2RlOiAnQkFEX1JFUVVFU1QnIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICAvLyBBbHdheXMgd2lwZSB0aGUgY29udklkIG1lbW9yeSBmaXJzdCBzbyByZXBsYXlpbmcgbmV2ZXIgZG91YmxlcyB1cCB0dXJuc1xuICAgIGNsZWFyTWVtb3J5KGNvbnZJZCk7XG5cbiAgICBmb3IgKGNvbnN0IG1zZyBvZiBtZXNzYWdlcykge1xuICAgICAgaWYgKChtc2cucm9sZSA9PT0gJ3VzZXInIHx8IG1zZy5yb2xlID09PSAnYXNzaXN0YW50JykgJiYgdHlwZW9mIG1zZy5jb250ZW50ID09PSAnc3RyaW5nJykge1xuICAgICAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsIG1zZy5yb2xlLCBtc2cuY29udGVudCk7XG4gICAgICB9XG4gICAgfVxuICAgIHJlcy5qc29uKHsgb2s6IHRydWUsIGNvbnZJZCwgcmVzdG9yZWQ6IG1lc3NhZ2VzLmxlbmd0aCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdNZW1vcnkgcmVzdG9yZSB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IG9rOiBmYWxzZSwgY29udklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgZXJyb3I6IGVyci5tZXNzYWdlLFxuICAgIHN0YWNrOiBlcnIuc3RhY2tcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNDA0XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChyZXEsIHJlcykgPT4ge1xuICByZXMuc3RhdHVzKDQwNCkuanNvbih7XG4gICAgZXJyb3I6ICdFbmRwb2ludCBub3QgZm91bmQnLFxuICAgIGNvZGU6ICdOT1RfRk9VTkQnXG4gIH0pO1xufSk7XG5cbmV4cG9ydCBkZWZhdWx0IGFwcDtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7dmFyIF9fYXdhaXRlciA9ICh0aGlzICYmIHRoaXMuX19hd2FpdGVyKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgX2FyZ3VtZW50cywgUCwgZ2VuZXJhdG9yKSB7XG4gICAgZnVuY3Rpb24gYWRvcHQodmFsdWUpIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgUCA/IHZhbHVlIDogbmV3IFAoZnVuY3Rpb24gKHJlc29sdmUpIHsgcmVzb2x2ZSh2YWx1ZSk7IH0pOyB9XG4gICAgcmV0dXJuIG5ldyAoUCB8fCAoUCA9IFByb21pc2UpKShmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICAgIGZ1bmN0aW9uIGZ1bGZpbGxlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvci5uZXh0KHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gcmVqZWN0ZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3JbXCJ0aHJvd1wiXSh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHN0ZXAocmVzdWx0KSB7IHJlc3VsdC5kb25lID8gcmVzb2x2ZShyZXN1bHQudmFsdWUpIDogYWRvcHQocmVzdWx0LnZhbHVlKS50aGVuKGZ1bGZpbGxlZCwgcmVqZWN0ZWQpOyB9XG4gICAgICAgIHN0ZXAoKGdlbmVyYXRvciA9IGdlbmVyYXRvci5hcHBseSh0aGlzQXJnLCBfYXJndW1lbnRzIHx8IFtdKSkubmV4dCgpKTtcbiAgICB9KTtcbn07XG52YXIgX19nZW5lcmF0b3IgPSAodGhpcyAmJiB0aGlzLl9fZ2VuZXJhdG9yKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgYm9keSkge1xuICAgIHZhciBfID0geyBsYWJlbDogMCwgc2VudDogZnVuY3Rpb24oKSB7IGlmICh0WzBdICYgMSkgdGhyb3cgdFsxXTsgcmV0dXJuIHRbMV07IH0sIHRyeXM6IFtdLCBvcHM6IFtdIH0sIGYsIHksIHQsIGcgPSBPYmplY3QuY3JlYXRlKCh0eXBlb2YgSXRlcmF0b3IgPT09IFwiZnVuY3Rpb25cIiA/IEl0ZXJhdG9yIDogT2JqZWN0KS5wcm90b3R5cGUpO1xuICAgIHJldHVybiBnLm5leHQgPSB2ZXJiKDApLCBnW1widGhyb3dcIl0gPSB2ZXJiKDEpLCBnW1wicmV0dXJuXCJdID0gdmVyYigyKSwgdHlwZW9mIFN5bWJvbCA9PT0gXCJmdW5jdGlvblwiICYmIChnW1N5bWJvbC5pdGVyYXRvcl0gPSBmdW5jdGlvbigpIHsgcmV0dXJuIHRoaXM7IH0pLCBnO1xuICAgIGZ1bmN0aW9uIHZlcmIobikgeyByZXR1cm4gZnVuY3Rpb24gKHYpIHsgcmV0dXJuIHN0ZXAoW24sIHZdKTsgfTsgfVxuICAgIGZ1bmN0aW9uIHN0ZXAob3ApIHtcbiAgICAgICAgaWYgKGYpIHRocm93IG5ldyBUeXBlRXJyb3IoXCJHZW5lcmF0b3IgaXMgYWxyZWFkeSBleGVjdXRpbmcuXCIpO1xuICAgICAgICB3aGlsZSAoZyAmJiAoZyA9IDAsIG9wWzBdICYmIChfID0gMCkpLCBfKSB0cnkge1xuICAgICAgICAgICAgaWYgKGYgPSAxLCB5ICYmICh0ID0gb3BbMF0gJiAyID8geVtcInJldHVyblwiXSA6IG9wWzBdID8geVtcInRocm93XCJdIHx8ICgodCA9IHlbXCJyZXR1cm5cIl0pICYmIHQuY2FsbCh5KSwgMCkgOiB5Lm5leHQpICYmICEodCA9IHQuY2FsbCh5LCBvcFsxXSkpLmRvbmUpIHJldHVybiB0O1xuICAgICAgICAgICAgaWYgKHkgPSAwLCB0KSBvcCA9IFtvcFswXSAmIDIsIHQudmFsdWVdO1xuICAgICAgICAgICAgc3dpdGNoIChvcFswXSkge1xuICAgICAgICAgICAgICAgIGNhc2UgMDogY2FzZSAxOiB0ID0gb3A7IGJyZWFrO1xuICAgICAgICAgICAgICAgIGNhc2UgNDogXy5sYWJlbCsrOyByZXR1cm4geyB2YWx1ZTogb3BbMV0sIGRvbmU6IGZhbHNlIH07XG4gICAgICAgICAgICAgICAgY2FzZSA1OiBfLmxhYmVsKys7IHkgPSBvcFsxXTsgb3AgPSBbMF07IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGNhc2UgNzogb3AgPSBfLm9wcy5wb3AoKTsgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICAgICAgICBpZiAoISh0ID0gXy50cnlzLCB0ID0gdC5sZW5ndGggPiAwICYmIHRbdC5sZW5ndGggLSAxXSkgJiYgKG9wWzBdID09PSA2IHx8IG9wWzBdID09PSAyKSkgeyBfID0gMDsgY29udGludWU7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSAzICYmICghdCB8fCAob3BbMV0gPiB0WzBdICYmIG9wWzFdIDwgdFszXSkpKSB7IF8ubGFiZWwgPSBvcFsxXTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSA2ICYmIF8ubGFiZWwgPCB0WzFdKSB7IF8ubGFiZWwgPSB0WzFdOyB0ID0gb3A7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0ICYmIF8ubGFiZWwgPCB0WzJdKSB7IF8ubGFiZWwgPSB0WzJdOyBfLm9wcy5wdXNoKG9wKTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHRbMl0pIF8ub3BzLnBvcCgpO1xuICAgICAgICAgICAgICAgICAgICBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgb3AgPSBib2R5LmNhbGwodGhpc0FyZywgXyk7XG4gICAgICAgIH0gY2F0Y2ggKGUpIHsgb3AgPSBbNiwgZV07IHkgPSAwOyB9IGZpbmFsbHkgeyBmID0gdCA9IDA7IH1cbiAgICAgICAgaWYgKG9wWzBdICYgNSkgdGhyb3cgb3BbMV07IHJldHVybiB7IHZhbHVlOiBvcFswXSA/IG9wWzFdIDogdm9pZCAwLCBkb25lOiB0cnVlIH07XG4gICAgfVxufTtcbmltcG9ydCB7IGRlZmluZUNvbmZpZyB9IGZyb20gJ3ZpdGUnO1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0JztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgcmV0dXJuIF9fZ2VuZXJhdG9yKHRoaXMsIGZ1bmN0aW9uIChfYSkge1xuICAgICAgICAgICAgICAgICAgICBzd2l0Y2ggKF9hLmxhYmVsKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDA6IHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnLi9zZXJ2ZXIvYXBwLmpzJyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAxOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3NBcHAgPSAoX2Euc2VudCgpKS5kZWZhdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcCA9IGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgc2VydmVyLm1pZGRsZXdhcmVzLnVzZSgnL2FwaScsIGZ1bmN0aW9uIChyZXEsIHJlcywgbmV4dCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAocmVxLCByZXMsIG5leHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbMiAvKnJldHVybiovXTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgfTtcbn1cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gICAgcGx1Z2luczogW3JlYWN0KCksIGV4cHJlc3NQbHVnaW4oKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsbUJBQW1CO0FBQ3pTLFNBQVMsTUFBTSxjQUFjO0FBUTdCLFNBQVMsaUJBQWlCO0FBQ3hCLE1BQUksQ0FBQyxhQUFhO0FBQ2hCLFVBQU0sU0FBUyxRQUFRLElBQUk7QUFDM0IsVUFBTSxTQUFTLFFBQVEsSUFBSSxpQkFBaUI7QUFDNUMsVUFBTSxXQUFXLFFBQVEsSUFBSSxtQkFBbUI7QUFDaEQsVUFBTSxPQUFPLFFBQVEsSUFBSSxlQUFlO0FBRXhDLFlBQVEsSUFBSSxxQ0FBcUM7QUFDakQsWUFBUSxJQUFJLGVBQWUsUUFBUSw2QkFBNkI7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QyxHQUFHLENBQUM7QUFBRyxVQUFJO0FBQUEsSUFBRyxVQUFFO0FBQVUsVUFBSSxJQUFJO0FBQUEsSUFBRztBQUN6RCxRQUFJLEdBQUcsQ0FBQyxJQUFJLEVBQUcsT0FBTSxHQUFHLENBQUM7QUFBRyxXQUFPLEVBQUUsT0FBTyxHQUFHLENBQUMsSUFBSSxHQUFHLENBQUMsSUFBSSxRQUFRLE1BQU0sS0FBSztBQUFBLEVBQ25GO0FBQ0o7QUFLQSxJQUFJQyxhQUFZQyxNQUFLLFFBQVFDLGVBQWNILHlDQUFlLENBQUM7QUFDM0QsU0FBUyxnQkFBZ0I7QUFDckIsTUFBSUk7QUFDSixTQUFPO0FBQUEsSUFDSCxNQUFNO0FBQUEsSUFDTixpQkFBaUIsU0FBVSxRQUFRO0FBQy9CLGFBQU8sVUFBVSxNQUFNLFFBQVEsUUFBUSxXQUFZO0FBQy9DLFlBQUk7QUFDSixlQUFPLFlBQVksTUFBTSxTQUFVLElBQUk7QUFDbkMsa0JBQVEsR0FBRyxPQUFPO0FBQUEsWUFDZCxLQUFLO0FBQUcscUJBQU8sQ0FBQyxHQUFhLHVEQUF5QjtBQUFBLFlBQ3RELEtBQUs7QUFDRCwyQkFBYyxHQUFHLEtBQUssRUFBRztBQUN6QixjQUFBQSxPQUFNO0FBQ04scUJBQU8sWUFBWSxJQUFJLFFBQVEsU0FBVSxLQUFLLEtBQUssTUFBTTtBQUNyRCxnQkFBQUEsS0FBSSxLQUFLLEtBQUssSUFBSTtBQUFBLGNBQ3RCLENBQUM7QUFDRCxxQkFBTztBQUFBLGdCQUFDO0FBQUE7QUFBQSxjQUFZO0FBQUEsVUFDNUI7QUFBQSxRQUNKLENBQUM7QUFBQSxNQUNMLENBQUM7QUFBQSxJQUNMO0FBQUEsRUFDSjtBQUNKO0FBQ0EsSUFBTyxzQkFBUSxhQUFhO0FBQUEsRUFDeEIsU0FBUyxDQUFDLE1BQU0sR0FBRyxjQUFjLENBQUM7QUFBQSxFQUNsQyxTQUFTO0FBQUEsSUFDTCxPQUFPO0FBQUEsTUFDSCxLQUFLRixNQUFLLFFBQVFELFlBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgInNlc3Npb24iLCAiQkFUQ0hfU0laRSIsICJ1dWlkdjQiLCAiUm91dGVyIiwgInBhdGgiLCAidXVpZHY0IiwgIkJBVENIX1NJWkUiLCAiUEFSQUxMRUxfQ0FMTFMiLCAiR1JPVVBfV0FJVF9NUyIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuQUkiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgIl9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwiLCAiX19kaXJuYW1lIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJhcHAiXQp9Cg==
