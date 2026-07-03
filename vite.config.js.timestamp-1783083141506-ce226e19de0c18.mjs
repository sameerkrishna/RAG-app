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
    router = Router();
    router.get("/", health);
    health_default = router;
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

// server/services/embeddingService.js
import { GoogleGenAI } from "file:///home/project/node_modules/@google/genai/dist/node/index.mjs";
function estimateTokensForTexts(texts) {
  return texts.reduce((sum, text) => sum + Math.ceil(String(text).length / 4), 0);
}
async function embedBatch(texts, taskType = "RETRIEVAL_DOCUMENT", attempt = 1) {
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-001";
  const outputDimensionality = parseInt(process.env.GEMINI_EMBEDDING_DIMENSIONS) || 3072;
  try {
    const response = await ai.models.embedContent({
      model: modelName,
      contents: texts.map((text) => typeof text === "string" ? text : String(text)),
      config: {
        taskType,
        outputDimensionality
      }
    });
    const embeddings = response?.embeddings?.map((e) => e.values) || [];
    if (embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${embeddings.length}`);
    }
    return embeddings;
  } catch (error) {
    const isRetryable = is429Error(error) || error?.status === 429 || error?.status === 502 || error?.status === 503 || error?.message?.includes("RESOURCE_EXHAUSTED") || error?.message?.includes("Service Unavailable") || error?.message?.includes("Bad Gateway");
    if (isRetryable && attempt < MAX_RETRY_ATTEMPTS) {
      let delay = Math.min(RETRY_MAX_DELAY_MS, RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1));
      const jitter = 0.8 + 0.4 * Math.random();
      delay = Math.floor(delay * jitter);
      if (error.retryAfter) {
        delay = Math.max(delay, error.retryAfter * 1e3);
      }
      console.log(
        `[embedding] \u23F3 Retryable error (${error?.status || "unknown"}), waiting ${(delay / 1e3).toFixed(1)}s (attempt ${attempt}/${MAX_RETRY_ATTEMPTS})...`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      await RATE_LIMITER.consume(estimateTokensForTexts(texts));
      return embedBatch(texts, taskType, attempt + 1);
    }
    throw new EmbeddingError(error.message || "Batch embedding failed");
  }
}
async function embedQuery(query) {
  await RATE_LIMITER.consume(estimateTokensForTexts([query]));
  const vectors = await embedBatch([query], "RETRIEVAL_QUERY");
  return vectors[0];
}
async function embedSingleBatchGroup(texts, taskType = "RETRIEVAL_DOCUMENT") {
  console.log(`[embedding] embedSingleBatchGroup \u2014 ${texts.length} texts, taskType=${taskType}`);
  await RATE_LIMITER.consume(estimateTokensForTexts(texts));
  const vectors = await embedBatch(texts, taskType);
  console.log(`[embedding] embedSingleBatchGroup \u2014 got ${vectors.length} vectors`);
  return vectors;
}
var SlidingWindowRateLimiter, TPM_LIMIT, RATE_LIMITER, RETRY_BASE_DELAY_MS, RETRY_MAX_DELAY_MS, MAX_RETRY_ATTEMPTS, ai;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    SlidingWindowRateLimiter = class {
      constructor(limitPerMinute) {
        this.limitPerMinute = limitPerMinute;
        this.windowMs = 6e4;
        this.requests = [];
        this._lock = Promise.resolve();
      }
      async consume(tokens) {
        let resolveLock;
        const lockPromise = new Promise((resolve) => {
          resolveLock = resolve;
        });
        await this._lock;
        this._lock = lockPromise;
        try {
          const now = Date.now();
          this.requests = this.requests.filter((req) => req.timestamp > now - this.windowMs);
          const currentTotal = this.requests.reduce((sum, req) => sum + req.tokens, 0);
          if (currentTotal + tokens <= this.limitPerMinute) {
            this.requests.push({ timestamp: now, tokens });
            return;
          }
          const needed = tokens - (this.limitPerMinute - currentTotal);
          let accumulatedExpired = 0;
          let waitUntil = now + this.windowMs;
          const sorted = [...this.requests].sort((a, b) => a.timestamp - b.timestamp);
          for (const req of sorted) {
            accumulatedExpired += req.tokens;
            if (accumulatedExpired >= needed) {
              waitUntil = req.timestamp + this.windowMs + 10;
              break;
            }
          }
          const delay = waitUntil - now;
          if (delay > 0) {
            console.log(
              `[rate-limit] Window full (${currentTotal}/${this.limitPerMinute}). Waiting ${(delay / 1e3).toFixed(1)}s to send ${tokens} tokens...`
            );
            await new Promise((resolve) => setTimeout(resolve, delay));
          }
          this.requests.push({ timestamp: Date.now(), tokens });
          this.requests = this.requests.filter((req) => req.timestamp > Date.now() - this.windowMs);
        } finally {
          resolveLock();
        }
      }
    };
    TPM_LIMIT = parseInt(process.env.GEMINI_EMBEDDING_TPM_LIMIT) || 5e5;
    RATE_LIMITER = new SlidingWindowRateLimiter(TPM_LIMIT);
    RETRY_BASE_DELAY_MS = 2e3;
    RETRY_MAX_DELAY_MS = 6e4;
    MAX_RETRY_ATTEMPTS = 5;
    ai = new GoogleGenAI({
      vertexai: true,
      project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || "project-d48e2f39-2685-4746-aa0",
      location: process.env.GOOGLE_CLOUD_LOCATION || "us-central1"
    });
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
    if (charStart >= entry.start && charStart <= entry.end) return entry.page;
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
  const BATCH_SIZE2 = parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 10;
  const PARALLEL_CALLS = parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 10;
  const GROUP_WAIT_MS = parseInt(process.env.EMBEDDING_GROUP_WAIT_MS) || 5;
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
    const totalBatches = Math.ceil(totalChunks / BATCH_SIZE2);
    const totalSets = Math.ceil(totalBatches / PARALLEL_CALLS);
    console.log(`[upload] [${sessionId}] ${totalChunks} chunks \u2192 ${totalBatches} API calls \u2192 ${totalSets} sets of ${PARALLEL_CALLS} parallel`);
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
    for (let i = 0; i < chunks.length; i += BATCH_SIZE2) batches.push(chunks.slice(i, i + BATCH_SIZE2));
    const sets = [];
    for (let i = 0; i < batches.length; i += PARALLEL_CALLS) sets.push(batches.slice(i, i + PARALLEL_CALLS));
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
          console.log(`[upload] [${sessionId}]   Batch ${setIdx * PARALLEL_CALLS + batchIdx + 1} embedded OK (${batch.length} chunks)`);
        } else {
          console.error(`[upload] [${sessionId}]   Batch ${setIdx * PARALLEL_CALLS + batchIdx + 1} FAILED:`, result.reason?.message);
        }
      });
      processedChunks += setEmbeddings.length;
      allEmbeddings.push(...setEmbeddings);
      console.log(`[upload] [${sessionId}] Set ${setIdx + 1} embedded \u2014 ${processedChunks}/${totalChunks} chunks so far`);
      if (!isLastSet) {
        console.log(`[upload] [${sessionId}] Starting ${GROUP_WAIT_MS / 1e3}s timer + Chroma write concurrently for set ${setIdx + 1}`);
        const timer = new Promise((r) => setTimeout(r, GROUP_WAIT_MS));
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
          waitingMs: GROUP_WAIT_MS,
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcbiAgICBjbG91ZENsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xvdWRDbGllbnQ7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBHbG9iYWwgY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGNvbm5lY3QgdG8gZ2xvYmFsIGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG4gIHJldHVybiBnbG9iYWxDb2xsZWN0aW9uO1xufVxuXG4vKipcbiAqIFJldHVybnMgeyBjb2xsZWN0aW9uLCBpc05ldyB9LlxuICogaXNOZXcgPSB0cnVlICBcdTIxOTIgZnJlc2hseSBjcmVhdGVkLCBuZWVkcyBzZWVkaW5nIGZyb20gZ2xvYmFsLlxuICogaXNOZXcgPSBmYWxzZSBcdTIxOTIgYWxyZWFkeSBleGlzdGVkIG9uIENocm9tYSBDbG91ZCwgcmVzcGVjdCBpdHMgY3VycmVudCBzdGF0ZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHsgY29sbGVjdGlvbjogc2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpLCBpc05ldzogZmFsc2UgfTtcbiAgfVxuXG4gIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcblxuICBsZXQgY29sbGVjdGlvbjtcbiAgbGV0IGlzTmV3O1xuXG4gIHRyeSB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICB9KTtcbiAgICBpc05ldyA9IGZhbHNlO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNjdiXFx1ZmUwZiAgU2Vzc2lvbiBjb2xsZWN0aW9uIGV4aXN0cywgcmV1c2luZzogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfSBjYXRjaCB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5jcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIGlzTmV3ID0gdHJ1ZTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY3JlYXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfVxuXG4gIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgcmV0dXJuIHsgY29sbGVjdGlvbiwgaXNOZXcgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gZGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbi8qKlxuICogQWRkIHZlY3RvcnMgaW4gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIHRvIGF2b2lkIENocm9tYSBwYXlsb2FkIGxpbWl0cy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBpZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGNvbnN0IGJhdGNoSWRzICAgICAgICA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzICA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyAgPSB2ZWN0b3JzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAodiA9PiB2Lm1ldGFkYXRhKTtcblxuICAgICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6ICAgICAgICBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6ICBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiAgYmF0Y2hNZXRhZGF0YXNcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgW2FkZFZlY3RvcnNdIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogYWRkZWQgJHtiYXRjaElkcy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPiBkZWZhdWx0IDEwMCBsaW1pdCkgYXJlIGZ1bGx5IGRlbGV0ZWQuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9LFxuICAgICAgICBpbmNsdWRlOiBbXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGFsbElkcyB9KTtcbiAgICB9XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG4vKipcbiAqIExpc3QgYWxsIHVuaXF1ZSBkb2N1bWVudHMgaW4gYSBjb2xsZWN0aW9uLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgd2l0aCBCQVRDSF9TSVpFPTMwMCBzbyBjb2xsZWN0aW9ucyBsYXJnZXJcbiAqIHRoYW4gQ2hyb21hJ3MgZGVmYXVsdCBnZXQoKSBsaW1pdCAoMTAwKSBhcmUgZnVsbHkgZW51bWVyYXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcblxuICAgICAgYmF0Y2guaWRzLmZvckVhY2goKGlkLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgbWV0YSAgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2NJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiAgICAgICAgIG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogICAgICAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogICAgICAgbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6ICAgICAgbWV0YS5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIGZpcnN0X2NodW5rX3RleHQ6IGJhdGNoLmRvY3VtZW50c1tpZHhdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBkb2MgPSBkb2N1bWVudHNNYXAuZ2V0KGRvY0lkKTtcbiAgICAgICAgZG9jLmNodW5rX2NvdW50Kys7XG4gICAgICAgIGRvYy5wYWdlX2NvdW50ID0gTWF0aC5tYXgoZG9jLnBhZ2VfY291bnQsIG1ldGEucGFnZV9udW1iZXIgfHwgMSk7XG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYCAgW2xpc3REb2N1bWVudHNdIG9mZnNldD0ke29mZnNldH0sIGdvdD0ke2JhdGNoLmlkcy5sZW5ndGh9LCB1bmlxdWUgc28gZmFyPSR7ZG9jdW1lbnRzTWFwLnNpemV9YCk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShkb2N1bWVudHNNYXAudmFsdWVzKCkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50czonLCBlcnJvcik7XG4gICAgcmV0dXJuIFtdO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGhDaGVjaygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY2xlYW51cFNlc3Npb25Db2xsZWN0aW9ucygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25zID0gYXdhaXQgY2xpZW50Lmxpc3RDb2xsZWN0aW9ucygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcXHUyNzA1IE5vIHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbnMgZm91bmQuJyk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFxcdWQ4M2VcXHVkZGY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFxcdTI3MDUgRGVsZXRlZDogJHtuYW1lfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYCAgXFx1MjZhMFxcdWZlMGYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1xcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGNsZWFudXAgY29tcGxldGUuJyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS53YXJuKCdcXHUyNmEwXFx1ZmUwZiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7aW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5cbmNvbnN0IENIQVJTX1BFUl9UT0tFTiAgICAgPSA0O1xuY29uc3QgVEFSR0VUX0NIVU5LX1RPS0VOUyA9IDYwMDsgICAvLyBzb2Z0IHRhcmdldCBwZXIgY2h1bmtcbmNvbnN0IE1BWF9DSFVOS19UT0tFTlMgICAgPSA3NTA7ICAgLy8gaGFyZCBjYXAgYmVmb3JlIGZvcmNlZCBzcGxpdFxuY29uc3QgT1ZFUkxBUF9UT0tFTlMgICAgICA9IDEwMDsgICAvLyBvdmVybGFwIG9ubHkgb24gb3ZlcnNpemVkIHBhcmFncmFwaHNcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyAgICAgPSAxMDA7XG5cbi8vIE1hdGNoZXMgQUxMLUNBUFMgaGVhZGluZ3MsIG1hcmtkb3duIGhlYWRpbmdzLCBvciBudW1iZXJlZCBzZWN0aW9uIGhlYWRpbmdzXG5jb25zdCBIRUFESU5HX1JFID0gL14oPzpbQS1aXVtBLVpcXHNdezIsNjB9JHwjezEsNH1cXHMuK3woPzpcXGQrXFwuKStcXHMuKykvbTtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW5UZXh0KHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuICcnO1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC9cXGYvZywgJ1xcbicpXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJylcbiAgICAucmVwbGFjZSgvWyBcXHRdezIsfS9nLCAnICcpXG4gICAgLnRyaW0oKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDaHVua0lkKHRleHQsIGZpbGVuYW1lKSB7XG4gIHJldHVybiBjcmVhdGVIYXNoKCdtZDUnKVxuICAgIC51cGRhdGUoYCR7ZmlsZW5hbWV9Ojoke3RleHR9YClcbiAgICAuZGlnZXN0KCdoZXgnKVxuICAgIC5zbGljZSgwLCAxNik7XG59XG5cbi8qKlxuICogU3RydWN0dXJlLWF3YXJlIGNodW5raW5nOlxuICogIDEuIFNwbGl0IG9uIGJsYW5rIGxpbmVzIChcXG5cXG4pIGludG8gcGFyYWdyYXBocy5cbiAqICAyLiBBIGxpbmUgbWF0Y2hpbmcgSEVBRElOR19SRSBhbHdheXMgc3RhcnRzIGEgZnJlc2ggY2h1bmsuXG4gKiAgMy4gQWNjdW11bGF0ZSBwYXJhZ3JhcGhzIHVudGlsIHRoZSBzb2Z0IFRBUkdFVCBpcyByZWFjaGVkLCB0aGVuIGZsdXNoLlxuICogIDQuIFBhcmFncmFwaHMgbGFyZ2VyIHRoYW4gTUFYIGFyZSBzcGxpdCB3aXRoIGEgc2xpZGluZyB3aW5kb3cgKyBvdmVybGFwIGFzIGZhbGxiYWNrLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0YXJnZXRUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBUQVJHRVRfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBtYXhUb2tlbnMgICAgPSBvcHRpb25zLm1heENodW5rVG9rZW5zICB8fCBNQVhfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBvdmVybGFwVGsgICAgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgICB8fCBPVkVSTEFQX1RPS0VOUztcblxuICBjb25zdCB0YXJnZXRDaGFycyAgPSB0YXJnZXRUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG1heENoYXJzICAgICA9IG1heFRva2VucyAgICAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRrICAgICogQ0hBUlNfUEVSX1RPS0VOO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICAvLyAxLiBTcGxpdCBpbnRvIHBhcmFncmFwaHNcbiAgY29uc3QgcmF3UGFyYXMgPSB0ZXh0XG4gICAgLnNwbGl0KC9cXG57Mix9LylcbiAgICAubWFwKHAgPT4gcC50cmltKCkpXG4gICAgLmZpbHRlcihwID0+IHAubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUyk7XG5cbiAgY29uc3QgY2h1bmtzICAgICA9IFtdO1xuICBsZXQgICBidWZmZXIgICAgID0gJyc7XG4gIGxldCAgIGJ1ZlN0YXJ0ICAgPSAwO1xuICBsZXQgICBjaHVua0luZGV4ID0gMDtcbiAgbGV0ICAgY2hhckN1cnNvciA9IDA7XG5cbiAgY29uc3QgZmx1c2ggPSAoZm9yY2VUZXh0KSA9PiB7XG4gICAgY29uc3QgY29udGVudCA9IChmb3JjZVRleHQgPz8gYnVmZmVyKS50cmltKCk7XG4gICAgaWYgKGNvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiAgICAgICBjb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2Vucyhjb250ZW50KSxcbiAgICAgICAgY2hhclN0YXJ0OiAgYnVmU3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6ICAgIGJ1ZlN0YXJ0ICsgY29udGVudC5sZW5ndGgsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuICAgIGJ1ZmZlciAgID0gJyc7XG4gICAgYnVmU3RhcnQgPSBjaGFyQ3Vyc29yO1xuICB9O1xuXG4gIGZvciAoY29uc3QgcGFyYSBvZiByYXdQYXJhcykge1xuICAgIGNvbnN0IGlzSGVhZGluZyA9IEhFQURJTkdfUkUudGVzdChwYXJhLnNwbGl0KCdcXG4nKVswXSk7XG5cbiAgICAvLyAyLiBIZWFkaW5nIGFsd2F5cyBzdGFydHMgYSBuZXcgY2h1bmtcbiAgICBpZiAoaXNIZWFkaW5nICYmIGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgaWYgKHBhcmEubGVuZ3RoID4gbWF4Q2hhcnMpIHtcbiAgICAgIC8vIDMuIE92ZXJzaXplZCBwYXJhZ3JhcGggLT4gc2xpZGluZy13aW5kb3cgY2hhciBmYWxsYmFja1xuICAgICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgICBsZXQgcyA9IDA7XG4gICAgICB3aGlsZSAocyA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgIGxldCBlID0gcyArIHRhcmdldENoYXJzO1xuICAgICAgICBpZiAoZSA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgICAgY29uc3Qgc2VhcmNoRnJvbSA9IGUgLSBNYXRoLmZsb29yKHRhcmdldENoYXJzICogMC4yKTtcbiAgICAgICAgICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgICAgICAgICAgY29uc3QgaWR4ID0gcGFyYS5sYXN0SW5kZXhPZihicCwgZSk7XG4gICAgICAgICAgICBpZiAoaWR4ID4gc2VhcmNoRnJvbSkgeyBlID0gaWR4ICsgYnAubGVuZ3RoOyBicmVhazsgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBlID0gTWF0aC5taW4oZSwgcGFyYS5sZW5ndGgpO1xuICAgICAgICBjb25zdCBzbGljZSA9IHBhcmEuc2xpY2UocywgZSkudHJpbSgpO1xuICAgICAgICBpZiAoc2xpY2UubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgICAgIHRleHQ6ICAgICAgIHNsaWNlLFxuICAgICAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoc2xpY2UpLFxuICAgICAgICAgICAgY2hhclN0YXJ0OiAgY2hhckN1cnNvciArIHMsXG4gICAgICAgICAgICBjaGFyRW5kOiAgICBjaGFyQ3Vyc29yICsgZSxcbiAgICAgICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IG5leHQgPSBlIC0gb3ZlcmxhcENoYXJzO1xuICAgICAgICBzID0gbmV4dCA+IHMgPyBuZXh0IDogZTtcbiAgICAgIH1cbiAgICAgIGNoYXJDdXJzb3IgKz0gcGFyYS5sZW5ndGggKyAyO1xuICAgICAgYnVmU3RhcnQgICAgPSBjaGFyQ3Vyc29yO1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgLy8gNC4gTm9ybWFsIHBhcmFncmFwaCBcdTIwMTQgaGFyZCBjYXAgbG9va2FoZWFkIEJFRk9SRSBhY2N1bXVsYXRpbmdcbiAgICBpZiAoYnVmZmVyLmxlbmd0aCA+IDAgJiYgKGJ1ZmZlci5sZW5ndGggKyBwYXJhLmxlbmd0aCArIDIpID4gbWF4Q2hhcnMpIHtcbiAgICAgIGZsdXNoKCk7XG4gICAgfVxuXG4gICAgYnVmZmVyICAgICA9IGJ1ZmZlciA/IGJ1ZmZlciArICdcXG5cXG4nICsgcGFyYSA6IHBhcmE7XG4gICAgY2hhckN1cnNvciArPSBwYXJhLmxlbmd0aCArIDI7XG5cbiAgICAvLyBTb2Z0IGNhcDogZmx1c2ggb25jZSB0YXJnZXQgaXMgcmVhY2hlZFxuICAgIGlmIChidWZmZXIubGVuZ3RoID49IHRhcmdldENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDUuIEZsdXNoIHJlbWFpbmRlclxuICBmbHVzaCgpO1xuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgID0gY2h1bmtUZXh0KGNsZWFuZWRUZXh0LCBvcHRpb25zKTtcbiAgY29uc3QgdG90YWxDaHVua3MgPSB0ZXh0Q2h1bmtzLmxlbmd0aDtcbiAgY29uc3Qgc291cmNlVHlwZSAgPSBvcHRpb25zLnNvdXJjZVR5cGUgfHwgJ3BkZic7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+IHtcbiAgICBjb25zdCBjaHVua0lkID0gZ2VuZXJhdGVDaHVua0lkKGNodW5rLnRleHQsIGZpbGVuYW1lKTtcbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogICAgICAgICBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBjaHVuay5jaHVua0luZGV4LFxuICAgICAgICB0b3RhbF9jaHVua3M6ICAgICB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6ICAgICAgcGFnZU51bWJlciB8fCAxLFxuICAgICAgICB0b3RhbF9wYWdlczogICAgICB0b3RhbFBhZ2VzIHx8IG51bGwsXG4gICAgICAgIHNlY3Rpb25fdGl0bGU6ICAgIGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICAgIHNvdXJjZV90eXBlOiAgICAgIHNvdXJjZVR5cGUsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogICAgICAgY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogICAgICAgICBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogICAgICBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfTtcbiAgfSk7XG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RTZWN0aW9uVGl0bGUodGV4dCkge1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5BSSB9IGZyb20gJ0Bnb29nbGUvZ2VuYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDEuIFNMSURJTkcgV0lORE9XIFJBVEUgTElNSVRFUlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jbGFzcyBTbGlkaW5nV2luZG93UmF0ZUxpbWl0ZXIge1xuICBjb25zdHJ1Y3RvcihsaW1pdFBlck1pbnV0ZSkge1xuICAgIHRoaXMubGltaXRQZXJNaW51dGUgPSBsaW1pdFBlck1pbnV0ZTtcbiAgICB0aGlzLndpbmRvd01zID0gNjAwMDA7XG4gICAgdGhpcy5yZXF1ZXN0cyA9IFtdO1xuICAgIHRoaXMuX2xvY2sgPSBQcm9taXNlLnJlc29sdmUoKTsgLy8gXHVEODNEXHVERDEyIE11dGV4IGxvY2sgZm9yIGNvbmN1cnJlbmN5IHNhZmV0eVxuICB9XG5cbiAgYXN5bmMgY29uc3VtZSh0b2tlbnMpIHtcbiAgICAvLyBBY3F1aXJlIHRoZSBsb2NrIFx1MjAxMyBlbnN1cmVzIG9ubHkgb25lIGNvbnN1bWUgcnVucyBhdCBhIHRpbWVcbiAgICBsZXQgcmVzb2x2ZUxvY2s7XG4gICAgY29uc3QgbG9ja1Byb21pc2UgPSBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHsgcmVzb2x2ZUxvY2sgPSByZXNvbHZlOyB9KTtcbiAgICBhd2FpdCB0aGlzLl9sb2NrO1xuICAgIHRoaXMuX2xvY2sgPSBsb2NrUHJvbWlzZTtcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICAgICAgLy8gUmVtb3ZlIGVudHJpZXMgb2xkZXIgdGhhbiA2MCBzZWNvbmRzXG4gICAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBub3cgLSB0aGlzLndpbmRvd01zKTtcblxuICAgICAgY29uc3QgY3VycmVudFRvdGFsID0gdGhpcy5yZXF1ZXN0cy5yZWR1Y2UoKHN1bSwgcmVxKSA9PiBzdW0gKyByZXEudG9rZW5zLCAwKTtcblxuICAgICAgLy8gSWYgd2UgaGF2ZSByb29tLCBjb25zdW1lIGluc3RhbnRseSAoYnVyc3QpXG4gICAgICBpZiAoY3VycmVudFRvdGFsICsgdG9rZW5zIDw9IHRoaXMubGltaXRQZXJNaW51dGUpIHtcbiAgICAgICAgdGhpcy5yZXF1ZXN0cy5wdXNoKHsgdGltZXN0YW1wOiBub3csIHRva2VucyB9KTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICAvLyBPdGhlcndpc2UsIHdhaXQgdW50aWwgdGhlIG9sZGVzdCByZXF1ZXN0IGV4cGlyZXMgKHBsdXMgYSBzbWFsbCBidWZmZXIpXG4gICAgICBjb25zdCBuZWVkZWQgPSB0b2tlbnMgLSAodGhpcy5saW1pdFBlck1pbnV0ZSAtIGN1cnJlbnRUb3RhbCk7XG4gICAgICBsZXQgYWNjdW11bGF0ZWRFeHBpcmVkID0gMDtcbiAgICAgIGxldCB3YWl0VW50aWwgPSBub3cgKyB0aGlzLndpbmRvd01zOyAvLyBmYWxsYmFja1xuXG4gICAgICBjb25zdCBzb3J0ZWQgPSBbLi4udGhpcy5yZXF1ZXN0c10uc29ydCgoYSwgYikgPT4gYS50aW1lc3RhbXAgLSBiLnRpbWVzdGFtcCk7XG4gICAgICBmb3IgKGNvbnN0IHJlcSBvZiBzb3J0ZWQpIHtcbiAgICAgICAgYWNjdW11bGF0ZWRFeHBpcmVkICs9IHJlcS50b2tlbnM7XG4gICAgICAgIGlmIChhY2N1bXVsYXRlZEV4cGlyZWQgPj0gbmVlZGVkKSB7XG4gICAgICAgICAgLy8gKzEwbXMgYnVmZmVyIHRvIHNsaWRlIHRoZSB3aW5kb3cgY2xlYW5seVxuICAgICAgICAgIHdhaXRVbnRpbCA9IHJlcS50aW1lc3RhbXAgKyB0aGlzLndpbmRvd01zICsgMTA7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgY29uc3QgZGVsYXkgPSB3YWl0VW50aWwgLSBub3c7XG4gICAgICBpZiAoZGVsYXkgPiAwKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKFxuICAgICAgICAgIGBbcmF0ZS1saW1pdF0gV2luZG93IGZ1bGwgKCR7Y3VycmVudFRvdGFsfS8ke3RoaXMubGltaXRQZXJNaW51dGV9KS4gYCArXG4gICAgICAgICAgYFdhaXRpbmcgJHsoZGVsYXkgLyAxMDAwKS50b0ZpeGVkKDEpfXMgdG8gc2VuZCAke3Rva2Vuc30gdG9rZW5zLi4uYFxuICAgICAgICApO1xuICAgICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgZGVsYXkpKTtcbiAgICAgIH1cblxuICAgICAgLy8gUmVjb3JkIHRoZSBjb25zdW1wdGlvbiBhdCB0aGUgbmV3IHRpbWVcbiAgICAgIHRoaXMucmVxdWVzdHMucHVzaCh7IHRpbWVzdGFtcDogRGF0ZS5ub3coKSwgdG9rZW5zIH0pO1xuICAgICAgLy8gQ2xlYW51cCBhZ2FpbiBqdXN0IGluIGNhc2VcbiAgICAgIHRoaXMucmVxdWVzdHMgPSB0aGlzLnJlcXVlc3RzLmZpbHRlcihyZXEgPT4gcmVxLnRpbWVzdGFtcCA+IERhdGUubm93KCkgLSB0aGlzLndpbmRvd01zKTtcblxuICAgIH0gZmluYWxseSB7XG4gICAgICAvLyBcdUQ4M0RcdUREMTMgUmVsZWFzZSB0aGUgbG9ja1xuICAgICAgcmVzb2x2ZUxvY2soKTtcbiAgICB9XG4gIH1cbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAyLiBDT05GSUdVUkFUSU9OXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnN0IFRQTV9MSU1JVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfVFBNX0xJTUlUKSB8fCA1MDAwMDA7XG5jb25zdCBSQVRFX0xJTUlURVIgPSBuZXcgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyKFRQTV9MSU1JVCk7XG5cbi8vIEJBVENIX1NJWkU6IG51bWJlciBvZiBjaHVua3MgcGVyIGVtYmVkQ29udGVudCBjYWxsXG4vLyAoa2VwdCBhdCAxMDsgbm90ZSB0aGUgcmVhbCBjZWlsaW5nIGlzIHRoZSBBUEkncyB+MTAwLXJlcXVlc3RzLXBlci1jYWxsIGxpbWl0LFxuLy8gbm90IGEgXCJjb250ZXh0IHdpbmRvd1wiIGxpbWl0IFx1MjAxNCAxMCBqdXN0IGtlZXBzIGJhdGNoZXMgc21hbGwgYW5kIHJldHJ5LWZyaWVuZGx5KVxuY29uc3QgQkFUQ0hfU0laRSA9ICgpID0+IDEwOyAgIC8vIDEwIGNodW5rcyBcdTAwRDcgNzUwIHRva2VucyA9IDcsNTAwIHRva2VucyBwZXIgQVBJIHJlcXVlc3RcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gMTA7IC8vIFNlbmQgMTAgYmF0Y2hlcyBjb25jdXJyZW50bHkgdG8gY2xlYXIgdGhlIGJ1cnN0IGZhc3RcblxuLy8gUmV0cnkgY29uZmlndXJhdGlvbiAoZXhwb25lbnRpYWwgYmFja29mZiArIGppdHRlcilcbmNvbnN0IFJFVFJZX0JBU0VfREVMQVlfTVMgPSAyMDAwOyAgIC8vIDIgc2Vjb25kc1xuY29uc3QgUkVUUllfTUFYX0RFTEFZX01TID0gNjAwMDA7ICAgLy8gNjAgc2Vjb25kcyBjYXBcbmNvbnN0IE1BWF9SRVRSWV9BVFRFTVBUUyA9IDU7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMy4gQUkgQ0xJRU5UIChzaW5nbGUsIHJldXNhYmxlIGluc3RhbmNlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zdCBhaSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gIHZlcnRleGFpOiB0cnVlLFxuICBwcm9qZWN0OiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCBwcm9jZXNzLmVudi5HQ1BfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJyxcbiAgbG9jYXRpb246IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9MT0NBVElPTiB8fCAndXMtY2VudHJhbDEnXG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0LiBUT0tFTiBDQUxDVUxBVElPTiAodXNlcyBzdG9yZWQgdG9rZW5fY291bnQgaWYgYXZhaWxhYmxlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5mdW5jdGlvbiBnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKGNodW5rcykge1xuICByZXR1cm4gY2h1bmtzLnJlZHVjZSgoc3VtLCBjaHVuaykgPT4ge1xuICAgIC8vIFByZWZlciB0aGUgZXhhY3QgdG9rZW4gY291bnQgZnJvbSBjaHVua2VyLCBvdGhlcndpc2UgZmFsbGJhY2sgdG8gcm91Z2ggZXN0aW1hdGVcbiAgICBjb25zdCB0b2tlbkNvdW50ID0gY2h1bmsubWV0YWRhdGE/LnRva2VuX2NvdW50IHx8IE1hdGguY2VpbChjaHVuay50ZXh0Lmxlbmd0aCAvIDQpO1xuICAgIHJldHVybiBzdW0gKyB0b2tlbkNvdW50O1xuICB9LCAwKTtcbn1cblxuLy8gU2FtZSByb3VnaCBlc3RpbWF0ZSBhcyBhYm92ZSwgYnV0IGZvciByYXcgc3RyaW5ncyB0aGF0IGRvbid0IGNhcnJ5IGNodW5rIG1ldGFkYXRhXG4vLyAodXNlZCBmb3IgcmV0cmllcyBpbnNpZGUgZW1iZWRCYXRjaCwgYW5kIGZvciBlbWJlZFF1ZXJ5KS5cbmZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpIHtcbiAgcmV0dXJuIHRleHRzLnJlZHVjZSgoc3VtLCB0ZXh0KSA9PiBzdW0gKyBNYXRoLmNlaWwoU3RyaW5nKHRleHQpLmxlbmd0aCAvIDQpLCAwKTtcbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA1LiBFTUJFRCBCQVRDSCAod2l0aCBleHBvbmVudGlhbCBiYWNrb2ZmICsgaml0dGVyKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hc3luYyBmdW5jdGlvbiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBhdHRlbXB0ID0gMSkge1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG4gIGNvbnN0IG91dHB1dERpbWVuc2lvbmFsaXR5ID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19ESU1FTlNJT05TKSB8fCAzMDcyO1xuXG4gIHRyeSB7XG4gICAgLy8gRklYOiBgYWkuYmF0Y2hlcy5jcmVhdGVFbWJlZGRpbmdzYCBpcyBub3QgYSByZWFsIG1ldGhvZCBvbiB0aGUgQGdvb2dsZS9nZW5haSBTREsuXG4gICAgLy8gYGFpLmJhdGNoZXNgIGlzIGZvciBhc3luYyBiYXRjaC1wcmVkaWN0aW9uIGpvYnMuIFN5bmNocm9ub3VzIGVtYmVkZGluZyBjYWxscyBnb1xuICAgIC8vIHRocm91Z2ggYGFpLm1vZGVscy5lbWJlZENvbnRlbnRgLCB3aXRoIG9uZSBzaGFyZWQgdGFza1R5cGUvb3V0cHV0RGltZW5zaW9uYWxpdHlcbiAgICAvLyBjb25maWcgYXBwbGllZCBhY3Jvc3MgYWxsIGBjb250ZW50c2AgaW4gdGhlIGNhbGwuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCBhaS5tb2RlbHMuZW1iZWRDb250ZW50KHtcbiAgICAgIG1vZGVsOiBtb2RlbE5hbWUsXG4gICAgICBjb250ZW50czogdGV4dHMubWFwKHRleHQgPT4gKHR5cGVvZiB0ZXh0ID09PSAnc3RyaW5nJyA/IHRleHQgOiBTdHJpbmcodGV4dCkpKSxcbiAgICAgIGNvbmZpZzoge1xuICAgICAgICB0YXNrVHlwZTogdGFza1R5cGUsXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBvdXRwdXREaW1lbnNpb25hbGl0eVxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IHJlc3BvbnNlPy5lbWJlZGRpbmdzPy5tYXAoZSA9PiBlLnZhbHVlcykgfHwgW107XG4gICAgaWYgKGVtYmVkZGluZ3MubGVuZ3RoICE9PSB0ZXh0cy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihgRXhwZWN0ZWQgJHt0ZXh0cy5sZW5ndGh9IGVtYmVkZGluZ3MsIGdvdCAke2VtYmVkZGluZ3MubGVuZ3RofWApO1xuICAgIH1cbiAgICByZXR1cm4gZW1iZWRkaW5ncztcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzUmV0cnlhYmxlID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAyIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDMgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnU2VydmljZSBVbmF2YWlsYWJsZScpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ0JhZCBHYXRld2F5Jyk7XG5cbiAgICBpZiAoaXNSZXRyeWFibGUgJiYgYXR0ZW1wdCA8IE1BWF9SRVRSWV9BVFRFTVBUUykge1xuICAgICAgLy8gRXhwb25lbnRpYWwgYmFja29mZjogMl5hdHRlbXB0ICogYmFzZSAoY2FwcGVkKVxuICAgICAgbGV0IGRlbGF5ID0gTWF0aC5taW4oUkVUUllfTUFYX0RFTEFZX01TLCBSRVRSWV9CQVNFX0RFTEFZX01TICogTWF0aC5wb3coMiwgYXR0ZW1wdCAtIDEpKTtcbiAgICAgIC8vIEFkZCBqaXR0ZXIgKDAuOFx1MjAxMzEuMngpIHRvIGF2b2lkIHRodW5kZXJpbmcgaGVyZFxuICAgICAgY29uc3Qgaml0dGVyID0gMC44ICsgKDAuNCAqIE1hdGgucmFuZG9tKCkpO1xuICAgICAgZGVsYXkgPSBNYXRoLmZsb29yKGRlbGF5ICogaml0dGVyKTtcbiAgICAgIC8vIFJlc3BlY3QgcmV0cnktYWZ0ZXIgaGVhZGVyIGlmIHByZXNlbnRcbiAgICAgIGlmIChlcnJvci5yZXRyeUFmdGVyKSB7XG4gICAgICAgIGRlbGF5ID0gTWF0aC5tYXgoZGVsYXksIGVycm9yLnJldHJ5QWZ0ZXIgKiAxMDAwKTtcbiAgICAgIH1cblxuICAgICAgY29uc29sZS5sb2coXG4gICAgICAgIGBbZW1iZWRkaW5nXSBcdTIzRjMgUmV0cnlhYmxlIGVycm9yICgke2Vycm9yPy5zdGF0dXMgfHwgJ3Vua25vd24nfSksIGAgK1xuICAgICAgICBgd2FpdGluZyAkeyhkZWxheSAvIDEwMDApLnRvRml4ZWQoMSl9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7TUFYX1JFVFJZX0FUVEVNUFRTfSkuLi5gXG4gICAgICApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIGRlbGF5KSk7XG5cbiAgICAgIC8vIEZJWDogYSByZXRyeSBpcyBhIGJyYW5kIG5ldyBBUEkgY2FsbCBhbmQgY29uc3VtZXMgcmVhbCBxdW90YSwgZXZlbiB0aG91Z2hcbiAgICAgIC8vIHRoZSBvcmlnaW5hbCBjYWxsIGZhaWxlZC4gU2tpcHBpbmcgY29uc3VtcHRpb24gaGVyZSAoYXMgYmVmb3JlKSBsZXQgdGhlIGxvY2FsXG4gICAgICAvLyBsaW1pdGVyIHVuZGVyLXJlcG9ydCBhY3R1YWwgdXNhZ2UgZHVyaW5nIGVycm9yIHN0b3Jtcywgd2hpY2ggbWVhbnQgaXQga2VwdFxuICAgICAgLy8gd2F2aW5nIHRocm91Z2ggbmV3IGdyb3VwcyB3aGlsZSByZXRyaWVzIHdlcmUgYWxzbyBoaXR0aW5nIHRoZSBBUEkgXHUyMDE0IG1ha2luZ1xuICAgICAgLy8gNDI5IHN0b3JtcyB3b3JzZSBpbnN0ZWFkIG9mIGJhY2tpbmcgb2ZmIGZyb20gdGhlbS5cbiAgICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpKTtcblxuICAgICAgcmV0dXJuIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlLCBhdHRlbXB0ICsgMSk7XG4gICAgfVxuXG4gICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGVycm9yLm1lc3NhZ2UgfHwgJ0JhdGNoIGVtYmVkZGluZyBmYWlsZWQnKTtcbiAgfVxufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDYuIEVYUE9SVEVEIGdlbmVyYXRlRW1iZWRkaW5ncyAod2l0aCByYXRlIGxpbWl0ZXIgJiBhY2N1cmF0ZSB0b2tlbnMpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZUVtYmVkZGluZ3MoY2h1bmtzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBvblByb2dyZXNzKSB7XG4gIGlmICghY2h1bmtzIHx8IGNodW5rcy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICBjb25zdCBiYXRjaFNpemUgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuXG4gIC8vIEZpeGVkLXNpemUgYXJyYXkgdG8gcHJlc2VydmUgY2hyb25vbG9naWNhbCBvcmRlclxuICBjb25zdCBlbWJlZGRpbmdzID0gbmV3IEFycmF5KGNodW5rcy5sZW5ndGgpO1xuXG4gIC8vIEdyb3VwIGNodW5rcyBpbnRvIGJhdGNoZXMgd2l0aCB0aGVpciBzdGFydGluZyBpbmRleFxuICBjb25zdCBiYXRjaGVzID0gW107XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBiYXRjaFNpemUpIHtcbiAgICBiYXRjaGVzLnB1c2goe1xuICAgICAgY2h1bmtzOiBjaHVua3Muc2xpY2UoaSwgaSArIGJhdGNoU2l6ZSksXG4gICAgICBzdGFydEluZGV4OiBpXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB0b3RhbEdyb3VwcyA9IE1hdGguY2VpbChiYXRjaGVzLmxlbmd0aCAvIHBhcmFsbGVsQ2FsbHMpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IHBhcmFsbGVsQmF0Y2hlcyA9IGJhdGNoZXMuc2xpY2UoaSwgaSArIHBhcmFsbGVsQ2FsbHMpO1xuICAgIGNvbnN0IGdyb3VwTnVtID0gTWF0aC5mbG9vcihpIC8gcGFyYWxsZWxDYWxscykgKyAxO1xuXG4gICAgLy8gQ2FsY3VsYXRlIGV4YWN0IHRva2VucyB1c2luZyBzdG9yZWQgdG9rZW5fY291bnQgKG9yIGZhbGxiYWNrKVxuICAgIGNvbnN0IGFsbENodW5rc0luR3JvdXAgPSBwYXJhbGxlbEJhdGNoZXMuZmxhdE1hcChiID0+IGIuY2h1bmtzKTtcbiAgICBjb25zdCB0b2tlbnNUb0NvbnN1bWUgPSBnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKGFsbENodW5rc0luR3JvdXApO1xuICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKHRva2Vuc1RvQ29uc3VtZSk7XG5cbiAgICBjb25zb2xlLmxvZyhcbiAgICAgIGBbZW1iZWRkaW5nXSBHcm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSBcdTIwMTQgZmlyaW5nICR7cGFyYWxsZWxCYXRjaGVzLmxlbmd0aH0gYmF0Y2hlcyBgICtcbiAgICAgIGBpbiBwYXJhbGxlbCAoJHt0b2tlbnNUb0NvbnN1bWV9IHRva2VucylgXG4gICAgKTtcblxuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBwYXJhbGxlbEJhdGNoZXMubWFwKGIgPT4gZW1iZWRCYXRjaChiLmNodW5rcy5tYXAoYyA9PiBjLnRleHQpLCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGN1cnJlbnRCYXRjaEluZm8gPSBwYXJhbGxlbEJhdGNoZXNbYmF0Y2hJZHhdO1xuICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgIGNvbnN0IHZlY3RvcnMgPSByZXN1bHQudmFsdWU7XG4gICAgICAgIGN1cnJlbnRCYXRjaEluZm8uY2h1bmtzLmZvckVhY2goKGNodW5rLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgIGNvbnN0IGdsb2JhbEluZGV4ID0gY3VycmVudEJhdGNoSW5mby5zdGFydEluZGV4ICsgY2h1bmtJZHg7XG4gICAgICAgICAgZW1iZWRkaW5nc1tnbG9iYWxJbmRleF0gPSB7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua18ke2dsb2JhbEluZGV4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH07XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGBbZW1iZWRkaW5nXSBCYXRjaCBzdGFydGluZyBhdCBpbmRleCAke2N1cnJlbnRCYXRjaEluZm8uc3RhcnRJbmRleH0gZmFpbGVkOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICBmYWlsZWRCYXRjaGVzLnB1c2goY3VycmVudEJhdGNoSW5mbyk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAob25Qcm9ncmVzcykge1xuICAgICAgb25Qcm9ncmVzcyh7IGN1cnJlbnRfYmF0Y2g6IGdyb3VwTnVtLCB0b3RhbF9iYXRjaGVzOiB0b3RhbEdyb3VwcyB9KTtcbiAgICB9XG5cbiAgICAvLyBSZXRyeSBmYWlsZWQgYmF0Y2hlcyBpbmRpdmlkdWFsbHlcbiAgICBmb3IgKGNvbnN0IGZhaWxlZEJhdGNoIG9mIGZhaWxlZEJhdGNoZXMpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBSZXRyeWluZyBmYWlsZWQgYmF0Y2ggZWxlbWVudHMgc3RhcnRpbmcgYXQgaW5kZXggJHtmYWlsZWRCYXRjaC5zdGFydEluZGV4fS4uLmApO1xuICAgICAgZm9yIChsZXQgY2h1bmtJZHggPSAwOyBjaHVua0lkeCA8IGZhaWxlZEJhdGNoLmNodW5rcy5sZW5ndGg7IGNodW5rSWR4KyspIHtcbiAgICAgICAgY29uc3QgY2h1bmsgPSBmYWlsZWRCYXRjaC5jaHVua3NbY2h1bmtJZHhdO1xuICAgICAgICBjb25zdCBnbG9iYWxJbmRleCA9IGZhaWxlZEJhdGNoLnN0YXJ0SW5kZXggKyBjaHVua0lkeDtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAvLyBGSVg6IHRoaXMgcmV0cnkgaXMgYSBmcmVzaCwgcmVhbCBBUEkgY2FsbCBcdTIwMTQgdHJhY2sgaXRzIHRva2VucyBhZ2FpbnN0XG4gICAgICAgICAgLy8gdGhlIGxpbWl0ZXIgaW5zdGVhZCBvZiBhc3N1bWluZyBpdCB3YXMgXCJhbHJlYWR5IHBhaWQgZm9yXCIuXG4gICAgICAgICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZ2V0VG9rZW5Db3VudEZvckNodW5rcyhbY2h1bmtdKSk7XG4gICAgICAgICAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW2NodW5rLnRleHRdLCB0YXNrVHlwZSk7XG4gICAgICAgICAgZW1iZWRkaW5nc1tnbG9iYWxJbmRleF0gPSB7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua19yZXRyeV8ke2dsb2JhbEluZGV4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbMF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gXHUyNzA1IFJldHJ5IHN1Y2NlZWRlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgZ2xvYmFsSW5kZXh9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFtlbWJlZGRpbmddIFx1Mjc0QyBSZXRyeSBmYWlsZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGdsb2JhbEluZGV4fTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvLyBGSVg6IHBlcm1hbmVudGx5LWZhaWxlZCBjaHVua3MgYXJlIGRyb3BwZWQgaGVyZSwgd2hpY2ggc2hpZnRzIGFycmF5IGluZGljZXNcbiAgLy8gcmVsYXRpdmUgdG8gdGhlIG9yaWdpbmFsIGBjaHVua3NgIGlucHV0LiBUaGlzIGxvZyBtYWtlcyB0aGF0IGxvc3MgdmlzaWJsZVxuICAvLyBpbnN0ZWFkIG9mIHNpbGVudDsgY2FsbGVycyB0aGF0IG5lZWQgdG8ga25vdyBleGFjdGx5IHdoaWNoIGNodW5rcyB3ZXJlIGxvc3RcbiAgLy8gY2FuIGNvbXBhcmUgcmV0dXJuZWQgYGlkYHMgYWdhaW5zdCB0aGVpciBvcmlnaW5hbCBjaHVuayBsaXN0LlxuICBjb25zdCBmYWlsZWRDb3VudCA9IGVtYmVkZGluZ3MuZmlsdGVyKGUgPT4gIWUpLmxlbmd0aDtcbiAgaWYgKGZhaWxlZENvdW50ID4gMCkge1xuICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gJHtmYWlsZWRDb3VudH0vJHtjaHVua3MubGVuZ3RofSBjaHVuayhzKSBwZXJtYW5lbnRseSBmYWlsZWQgdG8gZW1iZWQgYW5kIHdlcmUgZHJvcHBlZC5gKTtcbiAgfVxuXG4gIC8vIEZpbHRlciBvdXQgYW55IGVsZW1lbnRzIHRoYXQgcGVybWFuZW50bHkgZmFpbGVkXG4gIHJldHVybiBlbWJlZGRpbmdzLmZpbHRlcihCb29sZWFuKTtcbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA3LiBFWFBPUlRFRCBlbWJlZFF1ZXJ5XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFF1ZXJ5KHF1ZXJ5KSB7XG4gIC8vIEZJWDogdGhpcyBjYWxsIHdhcyBieXBhc3NpbmcgdGhlIHJhdGUgbGltaXRlciBlbnRpcmVseS4gSWYgaXQgcnVucyBjb25jdXJyZW50bHlcbiAgLy8gd2l0aCBkb2N1bWVudCBpbmdlc3Rpb24gKGUuZy4gYSB1c2VyIHNlYXJjaGVzIHdoaWxlIGEgYmF0Y2ggam9iIGlzIGluIGZsaWdodCksXG4gIC8vIGl0IGNvdWxkIHB1c2ggdG90YWwgdXNhZ2Ugb3ZlciB0aGUgY29uZmlndXJlZCBUUE0gYnVkZ2V0IHVubm90aWNlZC5cbiAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyhbcXVlcnldKSk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtxdWVyeV0sICdSRVRSSUVWQUxfUVVFUlknKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAodGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcpIHtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgJHt0ZXh0cy5sZW5ndGh9IHRleHRzLCB0YXNrVHlwZT0ke3Rhc2tUeXBlfWApO1xuICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSk7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0IGdvdCAke3ZlY3RvcnMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gIHJldHVybiB2ZWN0b3JzO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQge1xuICBnZXRHbG9iYWxDb2xsZWN0aW9uLFxuICBnZXRTZXNzaW9uQ29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50cyxcbiAgYWRkVmVjdG9yc1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGlkID0gc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICBkZWxldGVkRG9jdW1lbnRJZHM6IG5ldyBTZXQoKSwgICAvLyB0cmFjayBkZWxldGVkIGRvYyBJRHMgdG8gZmlsdGVyIHByb21wdCBtZW1vcnlcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcbiAgc2Vzc2lvbnMuc2V0KGlkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHJldHVybiBleGlzdGluZztcbiAgICByZXR1cm4gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIElmIGNvbGxlY3Rpb24gaXMgTkVXIFx1MjE5MiBzZWVkIGZyb20gZ2xvYmFsIChwYWdpbmF0ZWQsIDMwMC9iYXRjaClcbiAqIC0gSWYgY29sbGVjdGlvbiBFWElTVFMgXHUyMTkyIHNraXAgc2VlZCwgcmVjb25zdHJ1Y3QgaW4tbWVtb3J5IGRvYyBsaXN0IGZyb20gQ2hyb21hXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhgXHVEODNEXHVERDExIFNlc3Npb24gaW5pdDogJHtzZXNzaW9uSWR9YCk7XG4gIGlmIChzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWxyZWFkeSBzZWVkZWQgJHtzZXNzaW9uSWR9LCBza2lwcGluZ2ApO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb246IHNlc3Npb25Db2xsZWN0aW9uLCBpc05ldyB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGlmICghaXNOZXcpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI2N0JcdUZFMEYgIFNlc3Npb24gZXhpc3RzLCByZWNvbnN0cnVjdGluZyBkb2N1bWVudCBsaXN0IGZyb20gQ2hyb21hLi4uYCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKHNlc3Npb25Db2xsZWN0aW9uKTtcbiAgICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgICAgc291cmNlVHlwZTogZG9jLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IGRvY3VtZW50KHMpIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFx1RDgzQ1x1REYzMSBOZXcgc2Vzc2lvbiBcdTIwMTQgc2VlZGluZyBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uLi4uYCk7XG5cbiAgICBjb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdLCBhbGxFbWJlZGRpbmdzID0gW10sIGFsbERvY3VtZW50cyA9IFtdLCBhbGxNZXRhZGF0YXMgPSBbXTtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGdsb2JhbENvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydlbWJlZGRpbmdzJywgJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uYmF0Y2guZW1iZWRkaW5ncyk7XG4gICAgICBhbGxEb2N1bWVudHMucHVzaCguLi5iYXRjaC5kb2N1bWVudHMpO1xuICAgICAgYWxsTWV0YWRhdGFzLnB1c2goLi4uYmF0Y2gubWV0YWRhdGFzKTtcbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjZBMFx1RkUwRiAgR2xvYmFsIGNvbGxlY3Rpb24gaXMgZW1wdHkgXHUyMDE0IG5vdGhpbmcgdG8gc2VlZC4nKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYWxsSWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6IGFsbElkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIGVtYmVkZGluZ3M6IGFsbEVtYmVkZGluZ3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLFxuICAgICAgICBkb2N1bWVudHM6IGFsbERvY3VtZW50cy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIG1ldGFkYXRhczogYWxsTWV0YWRhdGFzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAobSA9PiAoeyAuLi5tLCBzb3VyY2VfdHlwZTogJ2dsb2JhbCcgfSkpXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFx1RDgzRFx1RENFNiBBZGRlZCBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IHJlY29yZHMgJHtpICsgMX1cdTIwMTMke01hdGgubWluKGkgKyBCQVRDSF9TSVpFLCBhbGxJZHMubGVuZ3RoKX1gKTtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlZWRlZCAke2FsbElkcy5sZW5ndGh9IHZlY3RvcnMgaW50byBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoc2Vzc2lvbikge1xuICAgICAgY29uc3QgZG9jc01hcCA9IG5ldyBNYXAoKTtcbiAgICAgIGFsbE1ldGFkYXRhcy5mb3JFYWNoKG1ldGEgPT4ge1xuICAgICAgICBpZiAoIWRvY3NNYXAuaGFzKG1ldGEuZG9jdW1lbnRfaWQpKSB7XG4gICAgICAgICAgZG9jc01hcC5zZXQobWV0YS5kb2N1bWVudF9pZCwge1xuICAgICAgICAgICAgaWQ6IG1ldGEuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBtZXRhLnRvdGFsX3BhZ2VzIHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiAwLFxuICAgICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGRvY3NNYXAuZ2V0KG1ldGEuZG9jdW1lbnRfaWQpLmNodW5rQ291bnQrKztcbiAgICAgIH0pO1xuXG4gICAgICBmb3IgKGNvbnN0IGRvYyBvZiBkb2NzTWFwLnZhbHVlcygpKSB7XG4gICAgICAgIGlmICghc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuaWQgPT09IGRvYy5pZCkpIHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKGRvYyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBVcHNlcnQgYSBkb2N1bWVudCBpbnRvIHRoZSBzZXNzaW9uLlxuICogSWYgYSBkb2Mgd2l0aCB0aGUgc2FtZSBpZCBhbHJlYWR5IGV4aXN0cywgdXBkYXRlIGl0IGluIHBsYWNlIChubyBkdXBsaWNhdGUpLlxuICogU3VwcG9ydHMgcGFydGlhbCB1cGRhdGVzIFx1MjAxNCBvbmx5IHByb3ZpZGVkIGZpZWxkcyBvdmVyd3JpdGUgZXhpc3RpbmcgdmFsdWVzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG5cbiAgY29uc3QgZXhpc3RpbmcgPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kKGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJbmZvLmlkKTtcblxuICBpZiAoZXhpc3RpbmcpIHtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmNodW5rQ291bnQgID0gZG9jdW1lbnRJbmZvLmNodW5rQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5wYWdlQ291bnQgICAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5wYWdlQ291bnQgICA9IGRvY3VtZW50SW5mby5wYWdlQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlU2l6ZSAgICAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlU2l6ZSAgICA9IGRvY3VtZW50SW5mby5maWxlU2l6ZTtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnN0YXR1cyAgICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnN0YXR1cyAgICAgID0gZG9jdW1lbnRJbmZvLnN0YXR1cztcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVuYW1lICAgICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVuYW1lICAgID0gZG9jdW1lbnRJbmZvLmZpbGVuYW1lO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFVwZGF0ZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZXhpc3Rpbmcuc3RhdHVzfSwgY2h1bmtzPSR7ZXhpc3RpbmcuY2h1bmtDb3VudH1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgPz8gMCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgIHN0YXR1czogZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnXG4gIH0pO1xuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWRkZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnfWApO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgLy8gVHJhY2sgZGVsZXRlZCBkb2Mgc28gaXRzIG1lbW9yeSB0dXJucyBhcmUgZXhjbHVkZWQgZnJvbSBmdXR1cmUgcHJvbXB0c1xuICAgIHNlc3Npb24uZGVsZXRlZERvY3VtZW50SWRzLmFkZChkb2N1bWVudElkKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBSZW1vdmVkIGRvYyAke2RvY3VtZW50SWR9LCBhZGRlZCB0byBkZWxldGVkRG9jdW1lbnRJZHNgKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIHJldHVybiBzZXNzaW9uPy5kZWxldGVkRG9jdW1lbnRJZHMgPz8gbmV3IFNldCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gW107XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBzZXNzaW9uRG9jdW1lbnRzOiBbXSwgZ2xvYmFsRG9jdW1lbnRzOiBbXSB9O1xuXG4gIGNvbnN0IG5vcm1hbGl6ZSA9IChkb2MpID0+ICh7XG4gICAgZG9jdW1lbnRfaWQ6IGRvYy5pZCxcbiAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgIGNodW5rX2NvdW50OiBkb2MuY2h1bmtDb3VudCA/PyAwLFxuICAgIHBhZ2VfY291bnQ6IGRvYy5wYWdlQ291bnQgPz8gMCxcbiAgICB1cGxvYWRfdGltZXN0YW1wOiBkb2MudXBsb2FkVGltZXN0YW1wIHx8IG51bGwsXG4gICAgc291cmNlX3R5cGU6IGRvYy5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ3Nlc3Npb25fdXBsb2FkJyA6ICdzZWVkJyxcbiAgICBmaWxlU2l6ZTogZG9jLmZpbGVTaXplIHx8IG51bGwsXG4gICAgc3RhdHVzOiBkb2Muc3RhdHVzID8/IG51bGxcbiAgfSk7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uLmRvY3VtZW50c1xuICAgICAgLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJylcbiAgICAgIC5tYXAobm9ybWFsaXplKSxcbiAgICBnbG9iYWxEb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnZ2xvYmFsJylcbiAgICAgIC5tYXAobm9ybWFsaXplKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgNTtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpOyAvLyBkZXN0cnVjdHVyZVxuICAgIGlmIChjb2xsZWN0aW9uKSBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwIH07XG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gTWF0aC5tYXgoMCwgci5zY29yZSkpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBbcXVlcnlFbWJlZGRpbmcsIHNlc3Npb25Db2xsZWN0aW9uXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIGVtYmVkUXVlcnkocXVlcnkpLFxuICAgICAgc2Vzc2lvbklkID8gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkgOiBQcm9taXNlLnJlc29sdmUobnVsbClcbiAgICBdKTtcblxuICAgIGlmICghc2Vzc2lvbkNvbGxlY3Rpb24pIHtcbiAgICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICBObyBzZXNzaW9uIGNvbGxlY3Rpb24gZm91bmQgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgcmV0dXJuIHsgcmVzdWx0czogW10sIGNvdmVyYWdlOiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwLCBsZXZlbDogJ2xvdycsIHNjb3JlOiAwIH0sIHF1ZXJ5RW1iZWRkaW5nIH07XG4gICAgfVxuXG4gICAgY29uc3QgcmF3UmVzdWx0cyA9IGF3YWl0IHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAuY2F0Y2goKCkgPT4gW10pO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBSYXcgc2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IG11bHRlciBmcm9tICdtdWx0ZXInO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcbmltcG9ydCBwZGYgZnJvbSAncGRmLXBhcnNlJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnOztcbmltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUsIHZhbGlkYXRlUERGRmlsZSwgdmFsaWRhdGVGaWxlU2l6ZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbmltcG9ydCB7XG4gIENvcnJ1cHRlZFBERkVycm9yLFxuICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgRmlsZVRvb0xhcmdlRXJyb3IsXG4gIFRvb01hbnlQREZzRXJyb3IsXG4gIER1cGxpY2F0ZUZpbGVFcnJvclxufSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuaW1wb3J0IHsgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbmltcG9ydCB7IGVtYmVkU2luZ2xlQmF0Y2hHcm91cCB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHtcbiAgZ2V0T3JDcmVhdGVTZXNzaW9uLFxuICBjYW5BY2NlcHRVcGxvYWQsXG4gIGFkZERvY3VtZW50VG9TZXNzaW9uLFxuICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uLFxuICBnZXRBbGxEb2N1bWVudHNcbn0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNsZWFyTWVtb3J5IH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG5jb25zdCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL3NlZWRfZG9jdW1lbnRzJyk7XG5cbmNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHVwbG9hZERpciksXG4gIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG59KTtcblxuY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgc3RvcmFnZSxcbiAgbGltaXRzOiB7IGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0IH0sXG4gIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICBjYihudWxsLCB0cnVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgIH1cbiAgfVxufSk7XG5cbmZ1bmN0aW9uIGNvbnRlbnREaXNwb3NpdGlvbihkaXNwbGF5TmFtZSkge1xuICBjb25zdCBlbmNvZGVkID0gZW5jb2RlVVJJQ29tcG9uZW50KGRpc3BsYXlOYW1lKVxuICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgIC5yZXBsYWNlKC9cXCgvZywgJyUyOCcpXG4gICAgLnJlcGxhY2UoL1xcKS9nLCAnJTI5Jyk7XG4gIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xufVxuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IHRjLml0ZW1zLm1hcChpID0+IGkuc3RyKS5qb2luKCcgJyk7XG4gICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgIGNvbnN0IHBhZ2VNYXAgPSBbXTtcbiAgICBsZXQgY2hhclBvcyA9IDA7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICB9XG5cbiAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRQYWdlTnVtYmVyKGNoYXJTdGFydCwgcGFnZU1hcCkge1xuICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICBpZiAoY2hhclN0YXJ0ID49IGVudHJ5LnN0YXJ0ICYmIGNoYXJTdGFydCA8PSBlbnRyeS5lbmQpIHJldHVybiBlbnRyeS5wYWdlO1xuICB9XG4gIHJldHVybiBwYWdlTWFwW3BhZ2VNYXAubGVuZ3RoIC0gMV0/LnBhZ2UgfHwgMTtcbn1cblxuZnVuY3Rpb24gc3NlRXZlbnQocmVzLCBldmVudCwgZGF0YSkge1xuICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5mbHVzaEhlYWRlcnMoKTtcblxuICBjb25zdCBCQVRDSF9TSVpFICAgICA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19CQVRDSF9NQVhfQ0hVTktTKSB8fCAxMDtcbiAgY29uc3QgUEFSQUxMRUxfQ0FMTFMgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUEFSQUxMRUxfQ0FMTFMpICB8fCAxMDtcbiAgY29uc3QgR1JPVVBfV0FJVF9NUyAgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfR1JPVVBfV0FJVF9NUykgICB8fCA1O1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgICAgID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICBjb25zdCBzZXNzaW9uICAgICAgID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgbWF4UERGcyAgICAgICA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG4gICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgIGlmICh1cGxvYWRlZENvdW50ID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgTWF4aW11bSAke21heFBERnN9IHVwbG9hZHMgcmVhY2hlZGAsIGNvZGU6ICdUT09fTUFOWV9QREZTJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBcIiR7Y2xlYW5GaWxlbmFtZX1cIiBhbHJlYWR5IHVwbG9hZGVkYCwgY29kZTogJ0RVUExJQ0FURV9GSUxFJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgXHUyMDE0IHBhcnNpbmcgJHtjbGVhbkZpbGVuYW1lfSAoJHtmaWxlLnNpemV9IGJ5dGVzKWApO1xuICAgIGNvbnN0IHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfSA9IGF3YWl0IHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGUucGF0aCk7XG5cbiAgICBpZiAoIWZ1bGxUZXh0IHx8IGZ1bGxUZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBleHRyYWN0YWJsZSB0ZXh0IFx1MjAxNCBQREYgbWF5IGJlIHNjYW5uZWQgb3IgaW1hZ2Utb25seScsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBkb2N1bWVudElkID0gdXVpZHY0KCk7XG4gICAgLy8gVXNlIGNodW5rZXIgZGVmYXVsdHMgKFRBUkdFVD02MDAsIE1BWD03NTAsIE9WRVJMQVA9MTAwKSBcdTIwMTQgZG8gTk9UIHBhc3Mgb3ZlcnJpZGVzXG4gICAgY29uc3QgcmF3Q2h1bmtzICA9IGNodW5rVGV4dChmdWxsVGV4dCk7XG5cbiAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiAgICAgICAgIGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiAgICAgICAgIGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogICAgIHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiAgICAgIGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6ICAgICAgdG90YWxQYWdlcyxcbiAgICAgICAgc291cmNlX3R5cGU6ICAgICAgJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiAgICAgICBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiAgICAgICAgIGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiAgICAgIGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9KSk7XG5cbiAgICBjb25zdCB0b3RhbENodW5rcyAgPSBjaHVua3MubGVuZ3RoO1xuICAgIGNvbnN0IHRvdGFsQmF0Y2hlcyA9IE1hdGguY2VpbCh0b3RhbENodW5rcyAvIEJBVENIX1NJWkUpO1xuICAgIGNvbnN0IHRvdGFsU2V0cyAgICA9IE1hdGguY2VpbCh0b3RhbEJhdGNoZXMgLyBQQVJBTExFTF9DQUxMUyk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgc3NlRXZlbnQocmVzLCAndXBsb2FkX2NvbXBsZXRlJywge1xuICAgICAgZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgIH0pO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IDAsIHN0YXR1czogJ2luZGV4aW5nJ1xuICAgIH0pO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgbGV0IHByb2Nlc3NlZENodW5rcyAgPSAwO1xuICAgIGNvbnN0IGFsbEVtYmVkZGluZ3MgID0gW107XG5cbiAgICBjb25zdCBiYXRjaGVzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpKTtcblxuICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IFBBUkFMTEVMX0NBTExTKSBzZXRzLnB1c2goYmF0Y2hlcy5zbGljZShpLCBpICsgUEFSQUxMRUxfQ0FMTFMpKTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAyIHN0YXJ0IFx1MjAxNCAke3NldHMubGVuZ3RofSBzZXRzYCk7XG5cbiAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgIGNvbnN0IGlzTGFzdFNldCAgICAgPSBzZXRJZHggPT09IHNldHMubGVuZ3RoIC0gMTtcbiAgICAgIGNvbnN0IGN1cnJlbnRTZXQgICAgPSBzZXRzW3NldElkeF07XG4gICAgICBjb25zdCBzZXRDaHVua0NvdW50ID0gY3VycmVudFNldC5yZWR1Y2UoKGFjYywgYikgPT4gYWNjICsgYi5sZW5ndGgsIDApO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0vJHtzZXRzLmxlbmd0aH0gXHUyMDE0IGVtYmVkZGluZyAke2N1cnJlbnRTZXQubGVuZ3RofSBiYXRjaCBjYWxsKHMpICgke3NldENodW5rQ291bnR9IGNodW5rcykgaW4gcGFyYWxsZWxgKTtcblxuICAgICAgY29uc3QgZW1iZWRSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgICBjdXJyZW50U2V0Lm1hcChiYXRjaCA9PiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAoYmF0Y2gubWFwKGMgPT4gYy50ZXh0KSkpXG4gICAgICApO1xuXG4gICAgICBjb25zdCBzZXRFbWJlZGRpbmdzID0gW107XG4gICAgICBlbWJlZFJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgICBjb25zdCBiYXRjaCA9IGN1cnJlbnRTZXRbYmF0Y2hJZHhdO1xuICAgICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgICByZXN1bHQudmFsdWUuZm9yRWFjaCgodmVjdG9yLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgICAgaWQ6ICAgICAgICBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEuY2h1bmtfaWQsXG4gICAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yLFxuICAgICAgICAgICAgICBtZXRhZGF0YTogIGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YSxcbiAgICAgICAgICAgICAgdGV4dDogICAgICBiYXRjaFtjaHVua0lkeF0udGV4dFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfSk7XG4gICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gZW1iZWRkZWQgT0sgKCR7YmF0Y2gubGVuZ3RofSBjaHVua3MpYCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBGQUlMRUQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBwcm9jZXNzZWRDaHVua3MgKz0gc2V0RW1iZWRkaW5ncy5sZW5ndGg7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uc2V0RW1iZWRkaW5ncyk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTZXQgJHtzZXRJZHggKyAxfSBlbWJlZGRlZCBcdTIwMTQgJHtwcm9jZXNzZWRDaHVua3N9LyR7dG90YWxDaHVua3N9IGNodW5rcyBzbyBmYXJgKTtcblxuICAgICAgaWYgKCFpc0xhc3RTZXQpIHtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFN0YXJ0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyB0aW1lciArIENocm9tYSB3cml0ZSBjb25jdXJyZW50bHkgZm9yIHNldCAke3NldElkeCArIDF9YCk7XG4gICAgICAgIGNvbnN0IHRpbWVyID0gbmV3IFByb21pc2UociA9PiBzZXRUaW1lb3V0KHIsIEdST1VQX1dBSVRfTVMpKTtcbiAgICAgICAgY29uc3QgY2hyb21hV3JpdGUgPSBhZGRWZWN0b3JzKFxuICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICkudGhlbigoKSA9PiBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApKVxuICAgICAgICAuY2F0Y2goZXJyID0+IGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBGQUlMRUQgZm9yIHNldCAke3NldElkeCArIDF9OmAsIGVyci5tZXNzYWdlKSk7XG5cbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZW1iZWRkaW5nX3Byb2dyZXNzJywge1xuICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgc2V0SW5kZXg6IHNldElkeCArIDEsIHRvdGFsU2V0cyxcbiAgICAgICAgICB3YWl0aW5nTXM6IEdST1VQX1dBSVRfTVMsIGNocm9tYVdyaXRlQ29tcGxldGU6IGZhbHNlXG4gICAgICAgIH0pO1xuXG4gICAgICAgIGF3YWl0IFByb21pc2UuYWxsKFt0aW1lciwgY2hyb21hV3JpdGVdKTtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFRpbWVyICsgQ2hyb21hIGJvdGggZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0sIHByb2NlZWRpbmcgdG8gc2V0ICR7c2V0SWR4ICsgMn1gKTtcblxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIExhc3Qgc2V0ICR7c2V0SWR4ICsgMX0gXHUyMDE0IGF3YWl0aW5nIENocm9tYSB3cml0ZSBkaXJlY3RseWApO1xuICAgICAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgY29tcGxldGUgZm9yIGxhc3Qgc2V0ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiAwLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiB0cnVlXG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCk7XG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLCBzdGF0dXM6ICdyZWFkeSdcbiAgICB9KTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBcdTI3MDUgRG9uZSBcdTIwMTQgJHthbGxFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycyBpbiBDaHJvbWEgZm9yICR7Y2xlYW5GaWxlbmFtZX1gKTtcblxuICAgIHNzZUV2ZW50KHJlcywgJ2RvbmUnLCB7XG4gICAgICBkb2N1bWVudDoge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogYWxsRW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9LFxuICAgICAgc2Vzc2lvbklkXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgdHJ5IHsgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTsgfSBjYXRjaCB7fVxuICAgIH1cbiAgICBjb25zb2xlLmVycm9yKCdbdXBsb2FkXSBVbmhhbmRsZWQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdVcGxvYWQgZmFpbGVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuICB0cnkge1xuICAgIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgfVxuICAgICAgfSBjYXRjaCAoY2hyb21hRXJyKSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gQ2hyb21hIGRlbGV0ZSBmYWlsZWQgZm9yICR7ZG9jdW1lbnRJZH06YCwgY2hyb21hRXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuXG4gICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG5cbiAgICAgIGNsZWFyTWVtb3J5KHNlc3Npb25JZCk7XG4gICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gQ2xlYXJlZCBtZW1vcnkgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgfVxuXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBmaWxlUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKGZpbGVQYXRoKSkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGVQYXRoKTtcbiAgICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIFJlbW92ZWQgZmlsZTogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gRmlsZSBub3QgZm91bmQgb24gZGlzazogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUsIGRvY3VtZW50SWQgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRGVsZXRlIGRvY3VtZW50IGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsIGNvZGU6ICdERUxFVEVfRVJST1InIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudEZpbGUocmVxLCByZXMpIHtcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgdHJ5IHtcbiAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgIGNvbnN0IHVwbG9hZFBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICBjb25zdCBtYXRjaCAgID0gYWxsUGRmcy5maW5kKGYgPT4gZi5pbmNsdWRlcyhwYXRoLnBhcnNlKGZpbGVuYW1lKS5uYW1lKSk7XG4gICAgICAgIGlmIChtYXRjaCkge1xuICAgICAgICAgIGNvbnN0IG1hdGNoUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBtYXRjaCk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24obWF0Y2gpKTtcbiAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShtYXRjaFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbnJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG5yb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG5yb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBMTE1VbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxubGV0IGdlbkFJID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0R2VuQUkoKSB7XG4gIGlmICghZ2VuQUkpIHtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gICAgICB2ZXJ0ZXhhaTogdHJ1ZSxcbiAgICAgIHByb2plY3Q6IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9QUk9KRUNUIHx8ICdwcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAnLFxuICAgICAgbG9jYXRpb246ICdnbG9iYWwnXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIGdlbkFJO1xufVxuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5jb25zdCBGQUxMQkFDS19NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9GQUxMQkFDSyB8fCAnZ2VtaW5pLTIuNS1mbGFzaCc7XG5jb25zdCBGSVJTVF9UT0tFTl9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX0ZJUlNUX1RPS0VOX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDEyMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX1JFUVVFU1RfVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgNDUwMDA7XG5cbmZ1bmN0aW9uIGdldFByaW1hcnlNb2RlbE5hbWUoKSB7XG4gIHJldHVybiBQUklNQVJZX01PREVMO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIEZBTExCQUNLX01PREVMO1xufVxuXG5mdW5jdGlvbiBnZXRUZXh0RnJvbUNodW5rKGNodW5rKSB7XG4gIGlmICh0eXBlb2YgY2h1bms/LnRleHQgPT09ICdzdHJpbmcnKSByZXR1cm4gY2h1bmsudGV4dDtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ2Z1bmN0aW9uJykgcmV0dXJuIGNodW5rLnRleHQoKTtcbiAgcmV0dXJuICcnO1xufVxuXG5mdW5jdGlvbiBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsLCBwcm9tcHQpIHtcbiAgcmV0dXJuIHtcbiAgICBtb2RlbCxcbiAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgIGNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9XG4gIH07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbE5hbWUgPSBnZXRQcmltYXJ5TW9kZWxOYW1lKCk7XG4gIGxldCByZXRyaWVzID0gMDtcbiAgY29uc3QgbWF4UmV0cmllcyA9IDI7XG5cbiAgd2hpbGUgKHJldHJpZXMgPCBtYXhSZXRyaWVzKSB7XG4gICAgbGV0IGZpcnN0VG9rZW5UaW1lb3V0ID0gbnVsbDtcbiAgICBsZXQgcmVxdWVzdFRpbWVvdXRJZCA9IG51bGw7XG4gICAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcblxuICAgIHRyeSB7XG4gICAgICByZXF1ZXN0VGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgICAgIGNvbnN0IHJlc3BvbnNlU3RyZWFtID0gYXdhaXQgZ2V0R2VuQUkoKS5tb2RlbHMuZ2VuZXJhdGVDb250ZW50U3RyZWFtKFxuICAgICAgICBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsTmFtZSwgcHJvbXB0KSxcbiAgICAgICAgeyBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsIH1cbiAgICAgICk7XG5cbiAgICAgIGlmICghcmVzcG9uc2VTdHJlYW0gfHwgdHlwZW9mIHJlc3BvbnNlU3RyZWFtW1N5bWJvbC5hc3luY0l0ZXJhdG9yXSAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYFN0cmVhbWluZyB1bmF2YWlsYWJsZSBmb3IgbW9kZWwgJHttb2RlbE5hbWV9YCk7XG4gICAgICB9XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3BvbnNlU3RyZWFtKSB7XG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdTdHJlYW0gZXhlY3V0aW9uIGFib3J0ZWQgYnkgdGltZW91dCBjb25zdHJhaW50LicpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGdldFRleHRGcm9tQ2h1bmsoY2h1bmspO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuICAgICAgcmV0dXJuO1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcblxuICAgICAgaWYgKGZpcnN0VG9rZW5UaW1lb3V0KSBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgaWYgKHJlcXVlc3RUaW1lb3V0SWQpIGNsZWFyVGltZW91dChyZXF1ZXN0VGltZW91dElkKTtcblxuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIG1vZGVsTmFtZSA9IGdldEZhbGxiYWNrTW9kZWxOYW1lKCk7XG4gICAgfVxuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgcmV0cmlldmVGb3JRdWVyeSwgZ2VuZXJhdGVDaXRhdGlvbnMsIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlIH0gZnJvbSAnLi4vc2VydmljZXMvZ2VtaW5pU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgZ2V0UmVjZW50VHVybnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgZ2V0RGVsZXRlZERvY3VtZW50SWRzIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgT1VUX09GX1NDT1BFX1BBVFRFUk4gPSAvZG9uJ3QgaGF2ZSBpbmZvcm1hdGlvbnxkbyBub3QgaGF2ZSBpbmZvcm1hdGlvbnxub3QgaW4gbXkga25vd2xlZGdlfGNhbid0IGZpbmR8Y2Fubm90IGZpbmR8bm8gaW5mb3JtYXRpb258a25vd2xlZGdlIGJhc2UgZG9lc24ndHxub3QgY292ZXJlZHxvdXRzaWRlLiprbm93bGVkZ2UvaTtcblxuZnVuY3Rpb24gY2xlYW5FeGNlcnB0KHRleHQpIHtcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvKD88IVxcdykoW0EtWmEtel0pXFxzKFtBLVphLXpdKVxccyhbQS1aYS16XSkoXFxzW0EtWmEtel0pKi9nLCAobWF0Y2gpID0+XG4gICAgICBtYXRjaC5yZXBsYWNlKC9cXHMvZywgJycpXG4gICAgKVxuICAgIC5yZXBsYWNlKC9cXHN7Mix9L2csICcgJylcbiAgICAucmVwbGFjZSgvXlxcKlxccyovLCAnJylcbiAgICAudHJpbSgpO1xufVxuXG4vLyBJc3N1ZSA0IGZpeDogcmVtb3ZlIGRvbWFpbkhpbnQgXHUyMDE0IHNob3J0IHF1ZXJpZXMgbm8gbG9uZ2VyIGluaGVyaXQgcHJldmlvdXMgY29udmVyc2F0aW9uIGNvbnRleHRcbmZ1bmN0aW9uIGV4cGFuZFF1ZXJ5KHF1ZXJ5KSB7XG4gIGNvbnN0IHdvcmRzID0gcXVlcnkudHJpbSgpLnNwbGl0KC9cXHMrLyk7XG4gIGlmICh3b3Jkcy5sZW5ndGggPiA0KSByZXR1cm4gcXVlcnk7XG5cbiAgY29uc3QgZXhwYW5zaW9ucyA9IFtcbiAgICAnZGVmaW5pdGlvbicsICdvdmVydmlldycsICdyb2xlJywgJ3Jlc3BvbnNpYmlsaXRpZXMnLFxuICAgICdleGFtcGxlcycsICdrZXkgY29uY2VwdHMnLCAnaG93IGl0IHdvcmtzJywgJ3B1cnBvc2UnXG4gIF07XG5cbiAgcmV0dXJuIGAke3F1ZXJ5fSAke2V4cGFuc2lvbnMuam9pbignICcpfWA7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQsIGNvbnZJZDogcHJvdmlkZWRDb252SWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsIGNvZGU6ICdNSVNTSU5HX1FVRVJZJyB9KTtcbiAgfVxuXG4gIGNvbnN0IHNlc3Npb25JZCA9IHByb3ZpZGVkU2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBjb252SWQgICAgPSBwcm92aWRlZENvbnZJZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgID0gdXVpZHY0KCk7XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ3VzZXInLCBxdWVyeS50cmltKCkpO1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgZXhwYW5kZWRRdWVyeSA9IGV4cGFuZFF1ZXJ5KHF1ZXJ5KTtcbiAgICBjb25zdCB7IHJlc3VsdHMsIGNvdmVyYWdlIH0gPSBhd2FpdCByZXRyaWV2ZUZvclF1ZXJ5KGV4cGFuZGVkUXVlcnksIHNlc3Npb25JZCwgeyB0b3BLOiA1IH0pO1xuXG4gICAgc2VuZEV2ZW50KCdyZXRyaWV2YWwnLCB7XG4gICAgICByZXN1bHRzOiByZXN1bHRzLmxlbmd0aCxcbiAgICAgIGxldmVsOiBjb3ZlcmFnZS5sZXZlbCxcbiAgICAgIHNjb3JlOiBjb3ZlcmFnZS5zY29yZSxcbiAgICAgIHRvcFNjb3JlOiBjb3ZlcmFnZS50b3BTY29yZVxuICAgIH0pO1xuXG4gICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgY29uc3Qgc291cmNlcyA9IHJlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgZmlsZW5hbWU6IHIubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgZXhjZXJwdDogY2xlYW5FeGNlcnB0KHIudGV4dC5zbGljZSgwLCAyMDApKSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICAvLyBHZXQgZGVsZXRlZCBkb2MgSURzIGZvciB0aGlzIHNlc3Npb24gdG8gZmlsdGVyIHN0YWxlIG1lbW9yeSB0dXJuc1xuICAgIGNvbnN0IGRlbGV0ZWREb2NJZHMgPSBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGFsbFJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoY29udklkLCAxMCk7XG5cbiAgICAvLyBGaWx0ZXIgb3V0IGFzc2lzdGFudCB0dXJucyAoYW5kIHRoZWlyIHByZWNlZGluZyB1c2VyIHR1cm5zKSB0aGF0IGNpdGVkIGRlbGV0ZWQgZG9jc1xuICAgIGNvbnN0IGZpbHRlcmVkVHVybnMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbFJlY2VudFR1cm5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCB0dXJuID0gYWxsUmVjZW50VHVybnNbaV07XG4gICAgICBpZiAodHVybi5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgICBjb25zdCBjaXRlc0RlbGV0ZWREb2MgPSB0dXJuLmNpdGF0aW9ucz8uc29tZShjID0+IGRlbGV0ZWREb2NJZHMuaGFzKGMuZG9jdW1lbnRJZCkpO1xuICAgICAgICBpZiAoY2l0ZXNEZWxldGVkRG9jKSB7XG4gICAgICAgICAgLy8gQWxzbyByZW1vdmUgdGhlIHByZWNlZGluZyB1c2VyIHR1cm4gaWYgaXQncyB0aGUgb25lIHRoYXQgcHJvbXB0ZWQgdGhpcyBhbnN3ZXJcbiAgICAgICAgICBpZiAoZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwICYmIGZpbHRlcmVkVHVybnNbZmlsdGVyZWRUdXJucy5sZW5ndGggLSAxXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgICAgICAgIGZpbHRlcmVkVHVybnMucG9wKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnRpbnVlOyAvLyBza2lwIHRoaXMgYXNzaXN0YW50IHR1cm5cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZmlsdGVyZWRUdXJucy5wdXNoKHR1cm4pO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpO1xuICAgIGNvbnN0IGFuc3dlcnMgICA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAnYXNzaXN0YW50Jyk7XG4gICAgY29uc3QgcVNlY3Rpb24gID0gcXVlc3Rpb25zLm1hcCgodCwgaSkgPT4gYFEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgYVNlY3Rpb24gID0gYW5zd2Vycy5tYXAoKHQsIGkpID0+IGBBJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDBcbiAgICAgID8gYFByZXZpb3VzIFF1ZXN0aW9uczpcXG4ke3FTZWN0aW9ufVxcblxcblByZXZpb3VzIEFuc3dlcnM6XFxuJHthU2VjdGlvbn1gXG4gICAgICA6ICcnO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gWW91ciBiZWhhdmlvdXIgZGVwZW5kcyBvbiB0aGUgdHlwZSBvZiBpbnB1dDpcblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMSyAoaGksIGhlbGxvLCBob3cgYXJlIHlvdSwgZG8geW91IGhhdmUgYSBsaWZlLCBqb2tlcywgZ2VuZXJhbCBjaGF0KTpcbiAgIC0gUmVzcG9uZCB3YXJtbHkgYW5kIG5hdHVyYWxseS4gRG8gTk9UIG1lbnRpb24gdGhlIGtub3dsZWRnZSBiYXNlIG9yIGRvY3VtZW50cyBhdCBhbGwuXG4gICAtIERvIE5PVCBhZGQgYW55IGNpdGF0aW9ucy5cblxuMi4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSCBDT05URVhUIChjb250ZXh0IGJlbG93IGlzIHJlbGV2YW50KTpcbiAgIC0gQW5zd2VyIHN0cmljdGx5IHVzaW5nIHRoZSBudW1iZXJlZCBjb250ZXh0IHByb3ZpZGVkLlxuICAgLSBDaXRlIHNvdXJjZXMgaW5saW5lIGFzIFsxXSBbMl0gXHUyMDE0IGFsd2F5cyBzZXBhcmF0ZSBicmFja2V0cywgbmV2ZXIgWzEsIDJdLlxuICAgLSBPbmx5IGNpdGUgbnVtYmVycyB5b3UgYWN0dWFsbHkgdXNlZC5cblxuMy4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSE9VVCBDT05URVhUIChjb250ZXh0IGlzIGVtcHR5IG9yIGlycmVsZXZhbnQpOlxuICAgLSBQb2xpdGVseSBkZWNsaW5lIGluIHlvdXIgb3duIHdvcmRzIFx1MjAxNCB2YXJ5IHlvdXIgcGhyYXNpbmcgbmF0dXJhbGx5LlxuICAgLSBEbyBOT1QgYWRkIGNpdGF0aW9ucy5cbiAgIC0gRG8gTk9UIHVzZSBhIGZpeGVkIHRlbXBsYXRlIG9yIHJvYm90aWMgcmVzcG9uc2UuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0IHx8ICcoTm8gcmVsZXZhbnQgZG9jdW1lbnRzIGZvdW5kIGluIGtub3dsZWRnZSBiYXNlKSd9XG5cbkNPTlZFUlNBVElPTiBISVNUT1JZOlxuJHttZW1vcnlDb250ZXh0IHx8ICcoTm8gcHJldmlvdXMgY29udmVyc2F0aW9uKSd9XG5cbkNVUlJFTlQgUVVFU1RJT046ICR7cXVlcnl9YDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGNpdGVkSW5kaWNlcyA9IFtdO1xuICAgIGNvbnN0IHNlZW4gPSBuZXcgU2V0KCk7XG4gICAgZm9yIChjb25zdCBtYXRjaCBvZiBmdWxsUmVzcG9uc2UubWF0Y2hBbGwoL1xcWyhcXGQrKVxcXS9nKSkge1xuICAgICAgY29uc3QgbnVtID0gcGFyc2VJbnQobWF0Y2hbMV0pO1xuICAgICAgaWYgKCFzZWVuLmhhcyhudW0pKSB7XG4gICAgICAgIHNlZW4uYWRkKG51bSk7XG4gICAgICAgIGNpdGVkSW5kaWNlcy5wdXNoKG51bSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgaXNPdXRPZlNjb3BlID0gT1VUX09GX1NDT1BFX1BBVFRFUk4udGVzdChmdWxsUmVzcG9uc2UpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENpdGF0aW9ucyA9IGNpdGF0aW9ucy5maWx0ZXIoYyA9PiBjaXRlZEluZGljZXMuaW5jbHVkZXMoYy5pbmRleCkpO1xuXG4gICAgY29uc3QgaW5kZXhNYXAgPSBuZXcgTWFwKCk7XG4gICAgY2l0ZWRJbmRpY2VzLmZvckVhY2goKG9sZElkeCwgaSkgPT4ge1xuICAgICAgaW5kZXhNYXAuc2V0KG9sZElkeCwgaSArIDEpO1xuICAgIH0pO1xuXG4gICAgY29uc3QgcmV3cml0dGVuUmVzcG9uc2UgPSBmdWxsUmVzcG9uc2UucmVwbGFjZSgvXFxbKFxcZCspXFxdL2csIChtYXRjaCwgbnVtKSA9PiB7XG4gICAgICBjb25zdCBuZXdJZHggPSBpbmRleE1hcC5nZXQocGFyc2VJbnQobnVtKSk7XG4gICAgICByZXR1cm4gbmV3SWR4ICE9PSB1bmRlZmluZWQgPyBgWyR7bmV3SWR4fV1gIDogbWF0Y2g7XG4gICAgfSk7XG5cbiAgICBjb25zdCBmaW5hbENpdGF0aW9ucyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IG1hdGNoZWRDaXRhdGlvbnNcbiAgICAgICAgICAubWFwKGMgPT4gKHsgLi4uYywgaW5kZXg6IGluZGV4TWFwLmdldChjLmluZGV4KSB9KSlcbiAgICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiBhLmluZGV4IC0gYi5pbmRleCk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2h1bmtJZHMgPSBuZXcgU2V0KG1hdGNoZWRDaXRhdGlvbnMubWFwKGMgPT4gYy5jaHVua0lkKSk7XG5cbiAgICBjb25zdCBmaW5hbFNvdXJjZXMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBzb3VyY2VzXG4gICAgICAgICAgLmZpbHRlcihzID0+IG1hdGNoZWRDaHVua0lkcy5oYXMocy5jaHVua0lkKSlcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4ge1xuICAgICAgICAgICAgY29uc3QgaWR4QSA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGEuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgY29uc3QgaWR4QiA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGIuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgcmV0dXJuIGlkeEEgLSBpZHhCO1xuICAgICAgICAgIH0pO1xuXG4gICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAnYXNzaXN0YW50JywgcmV3cml0dGVuUmVzcG9uc2UsIGZpbmFsQ2l0YXRpb25zLCBjb3ZlcmFnZSwgYW5zd2VySWQpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIGFuc3dlcklkLFxuICAgICAgcmVzcG9uc2U6IHJld3JpdHRlblJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zOiBmaW5hbENpdGF0aW9ucyxcbiAgICAgIGNvdmVyYWdlLFxuICAgICAgc291cmNlczogZmluYWxTb3VyY2VzXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnQW4gZXJyb3Igb2NjdXJyZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNvdXJjZXMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDIwKTtcblxuICBjb25zdCBleGFjdE1hdGNoID0gcmVjZW50VHVybnMuZmluZCh0ID0+IHQuaWQgPT09IGFuc3dlcklkKTtcbiAgaWYgKGV4YWN0TWF0Y2g/LmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGV4YWN0TWF0Y2guY2l0YXRpb25zIH0pO1xuICB9XG5cbiAgY29uc3QgZmFsbGJhY2sgPSBbLi4ucmVjZW50VHVybnNdLnJldmVyc2UoKS5maW5kKHQgPT5cbiAgICB0LnJvbGUgPT09ICdhc3Npc3RhbnQnICYmIHQuY2l0YXRpb25zPy5sZW5ndGggPiAwXG4gICk7XG5cbiAgaWYgKGZhbGxiYWNrKSByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG5cbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kJywgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJyB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXJcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtpbXBvcnQgZXhwcmVzcyBmcm9tICdleHByZXNzJztcbmltcG9ydCBjb3JzIGZyb20gJ2NvcnMnO1xuaW1wb3J0IGRvdGVudiBmcm9tICdkb3RlbnYnO1xuaW1wb3J0IHsgRXZlbnRFbWl0dGVyIH0gZnJvbSAnZXZlbnRzJztcblxuZG90ZW52LmNvbmZpZygpO1xuXG5pbXBvcnQgaGVhbHRoUm91dGVyIGZyb20gJy4vYXBpL2hlYWx0aC5qcyc7XG5pbXBvcnQgZG9jdW1lbnRzUm91dGVyIGZyb20gJy4vYXBpL2RvY3VtZW50cy5qcyc7XG5pbXBvcnQgY2hhdFJvdXRlciBmcm9tICcuL2FwaS9jaGF0LmpzJztcbmltcG9ydCBmZWVkYmFja1JvdXRlciBmcm9tICcuL2FwaS9mZWVkYmFjay5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3MgfSBmcm9tICcuL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBjbGVhck1lbW9yeSB9IGZyb20gJy4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IGFwcCA9IGV4cHJlc3MoKTtcblxuLy8gUHJvZ3Jlc3MgY2FsbGJhY2tzXG5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4vLyBNaWRkbGV3YXJlXG5hcHAudXNlKGNvcnMoe1xuICBvcmlnaW46IFtcbiAgICAnaHR0cDovL2xvY2FsaG9zdDo1MTczJyxcbiAgICAnaHR0cDovL2xvY2FsaG9zdDozMDAwJyxcbiAgICAnaHR0cDovLzEyNy4wLjAuMTo1MTczJ1xuICBdLFxuICBjcmVkZW50aWFsczogdHJ1ZVxufSkpO1xuXG5hcHAudXNlKGV4cHJlc3MuanNvbih7IGxpbWl0OiAnMTBtYicgfSkpO1xuYXBwLnVzZShleHByZXNzLnVybGVuY29kZWQoeyBleHRlbmRlZDogdHJ1ZSwgbGltaXQ6ICcxMG1iJyB9KSk7XG5cbi8vIFJlcXVlc3QgTG9nZ2VyXG5hcHAudXNlKChyZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmxvZyhgJHtyZXEubWV0aG9kfSAke3JlcS5vcmlnaW5hbFVybH1gKTtcbiAgbmV4dCgpO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFRFU1QgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5nZXQoJy9waW5nJywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnNvbGUubG9nKCdcdTI3MDUgUElORyBST1VURSBFWEVDVVRFRCcpO1xuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIElOSVQgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9pbml0JywgYXN5bmMgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXTtcblxuICBpZiAoIXNlc3Npb25JZCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnTWlzc2luZyB4LXNlc3Npb24taWQgaGVhZGVyJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gIH1cblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICB0cnkge1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiB0cnVlLCBzZXNzaW9uSWQgfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignU2Vzc2lvbiBpbml0IHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IGZhbHNlLCBzZXNzaW9uSWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBSRVNUT1JFIE1FTU9SWSBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL3Jlc3RvcmUtbWVtb3J5JywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHsgY29udklkLCBtZXNzYWdlcyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFjb252SWQgfHwgIUFycmF5LmlzQXJyYXkobWVzc2FnZXMpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdjb252SWQgYW5kIG1lc3NhZ2VzIGFyZSByZXF1aXJlZCcsIGNvZGU6ICdCQURfUkVRVUVTVCcgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIC8vIEFsd2F5cyB3aXBlIHRoZSBjb252SWQgbWVtb3J5IGZpcnN0IHNvIHJlcGxheWluZyBuZXZlciBkb3VibGVzIHVwIHR1cm5zXG4gICAgY2xlYXJNZW1vcnkoY29udklkKTtcblxuICAgIGZvciAoY29uc3QgbXNnIG9mIG1lc3NhZ2VzKSB7XG4gICAgICBpZiAoKG1zZy5yb2xlID09PSAndXNlcicgfHwgbXNnLnJvbGUgPT09ICdhc3Npc3RhbnQnKSAmJiB0eXBlb2YgbXNnLmNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgbXNnLnJvbGUsIG1zZy5jb250ZW50KTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmVzLmpzb24oeyBvazogdHJ1ZSwgY29udklkLCByZXN0b3JlZDogbWVzc2FnZXMubGVuZ3RoIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ01lbW9yeSByZXN0b3JlIHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgb2s6IGZhbHNlLCBjb252SWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxtQkFBbUI7QUFDelMsU0FBUyxNQUFNLGNBQWM7QUFRN0IsU0FBUyxpQkFBaUI7QUFDeEIsTUFBSSxDQUFDLGFBQWE7QUFDaEIsVUFBTSxTQUFTLFFBQVEsSUFBSTtBQUMzQixVQUFNLFNBQVMsUUFBUSxJQUFJLGlCQUFpQjtBQUM1QyxVQUFNLFdBQVcsUUFBUSxJQUFJLG1CQUFtQjtBQUNoRCxVQUFNLE9BQU8sUUFBUSxJQUFJLGVBQWU7QUFFeEMsWUFBUSxJQUFJLHFDQUFxQztBQUNqRCxZQUFRLElBQUksZUFBZSxRQUFRLDZCQUE2QjtBQUNoRSxZQUFRLElBQUksZUFBZSxNQUFNO0FBQ2pDLFlBQVEsSUFBSSxlQUFlLFFBQVE7QUFDbkMsWUFBUSxJQUFJLGVBQWUsU0FBUyxtQkFBbUIscUJBQXFCO0FBQzVFLFlBQVEsSUFBSSxxQ0FBcUM7QUFFakQsUUFBSSxDQUFDLFFBQVE7QUFDWCxZQUFNLElBQUk7QUFBQSxRQUNSO0FBQUEsTUFFRjtBQUFBLElBQ0Y7QUFFQSxVQUFNLGdCQUFnQixFQUFFLFFBQVEsUUFBUSxTQUFTO0FBQ2pELFFBQUksS0FBTSxlQUFjLE9BQU87QUFDL0Isa0JBQWMsSUFBSSxZQUFZLGFBQWE7QUFBQSxFQUM3QztBQUNBLFNBQU87QUFDVDtBQUVBLGVBQXNCLHNCQUFzQjtBQUMxQyxNQUFJLENBQUMsa0JBQWtCO0FBQ3JCLFVBQU0sU0FBUyxlQUFlO0FBQzlCLFVBQU0saUJBQWlCLFFBQVEsSUFBSSw0QkFBNEI7QUFDL0QsUUFBSTtBQUNGLHlCQUFtQixNQUFNLE9BQU8sc0JBQXNCO0FBQUEsUUFDcEQsTUFBTTtBQUFBLFFBQ04sVUFBVTtBQUFBLFVBQ1IsYUFBYTtBQUFBLFVBQ2IsTUFBTTtBQUFBLFFBQ1I7QUFBQSxRQUNB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QUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0YsTUFBSyxRQUFRRCxZQUFXLE9BQU87QUFBQSxJQUN4QztBQUFBLEVBQ0o7QUFBQSxFQUNBLFFBQVE7QUFBQSxJQUNKLE1BQU07QUFBQSxFQUNWO0FBQ0osQ0FBQzsiLAogICJuYW1lcyI6IFsidXVpZHY0IiwgImdsb2JhbENvbGxlY3Rpb24iLCAic2Vzc2lvbiIsICJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuQUkiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgIl9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwiLCAiX19kaXJuYW1lIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJhcHAiXQp9Cg==
