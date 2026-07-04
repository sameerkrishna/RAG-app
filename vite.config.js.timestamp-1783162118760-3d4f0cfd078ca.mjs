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
      }
      async consume(tokens) {
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
function isSessionSeeded(sessionId) {
  return seededSessions.has(sessionId);
}
function notifySeedingComplete(sessionId) {
  if (global.seedingListeners && global.seedingListeners.has(`seeding:${sessionId}`)) {
    const eventKey = `seeding:${sessionId}`;
    const listeners = global.seedingListeners.get(eventKey) || [];
    listeners.forEach((response) => {
      try {
        response.write(`event: seeding_complete
data: ${JSON.stringify({ sessionId, seeded: true })}

`);
        response.end();
      } catch (err) {
        console.error(`[notify] Failed to notify listener:`, err.message);
      }
    });
    global.seedingListeners.delete(eventKey);
    console.log(`[notify] Notified ${listeners.length} SSE listeners for session ${sessionId}`);
  }
}
async function initSessionWithGlobalDocs(sessionId) {
  console.log(`\u{1F511} Session init: ${sessionId}`);
  if (seededSessions.has(sessionId)) {
    console.log(`[session] Already seeded ${sessionId}, skipping`);
    notifySeedingComplete(sessionId);
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
      notifySeedingComplete(sessionId);
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
      notifySeedingComplete(sessionId);
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
    notifySeedingComplete(sessionId);
  } catch (error) {
    console.error(`\u274C Failed to seed session ${sessionId}:`, error.message);
    notifySeedingComplete(sessionId);
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
function sseEvent(res, event, data) {
  res.write(`event: ${event}
data: ${JSON.stringify(data)}

`);
}
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
async function handleUpload(req, res) {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  const BATCH_SIZE2 = parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 10;
  const PARALLEL_CALLS = parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 10;
  const GROUP_WAIT_MS = parseInt(process.env.EMBEDDING_GROUP_WAIT_MS) || 1;
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
async function seedingStatusHandler(req, res) {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  if (!sessionId) {
    sseEvent(res, "error", { message: "Missing session ID", code: "MISSING_SESSION" });
    res.end();
    return;
  }
  console.log(`[seeding-status] Client connected for session ${sessionId}`);
  const seeded = isSessionSeeded(sessionId);
  if (seeded) {
    console.log(`[seeding-status] Session ${sessionId} already seeded \u2013 returning immediately`);
    sseEvent(res, "seeding_complete", { sessionId, seeded: true });
    res.end();
    return;
  }
  const eventKey = `seeding:${sessionId}`;
  if (!global.seedingListeners) {
    global.seedingListeners = /* @__PURE__ */ new Map();
  }
  if (!global.seedingListeners.has(eventKey)) {
    global.seedingListeners.set(eventKey, []);
  }
  global.seedingListeners.get(eventKey).push(res);
  req.on("close", () => {
    const listeners = global.seedingListeners.get(eventKey) || [];
    const idx = listeners.indexOf(res);
    if (idx >= 0) {
      listeners.splice(idx, 1);
      console.log(`[seeding-status] Client disconnected for ${sessionId}`);
    }
    if (listeners.length === 0) {
      global.seedingListeners.delete(eventKey);
    }
  });
  try {
    console.log(`[seeding-status] Triggering seeding for ${sessionId}...`);
    await initSessionWithGlobalDocs(sessionId);
  } catch (err) {
    console.error(`[seeding-status] Seeding failed for ${sessionId}:`, err.message);
    const listeners = global.seedingListeners.get(eventKey) || [];
    listeners.forEach((response) => {
      sseEvent(response, "error", { message: err.message, code: "SEED_FAILED" });
      response.end();
    });
    global.seedingListeners.delete(eventKey);
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
    router2.get("/seeding-status", seedingStatusHandler);
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
        var dotenv2, expressApp;
        return __generator(this, function(_a) {
          switch (_a.label) {
            case 0:
              return [4, import("file:///home/project/node_modules/dotenv/lib/main.js")];
            case 1:
              dotenv2 = _a.sent();
              dotenv2.config();
              return [4, Promise.resolve().then(() => (init_app(), app_exports))];
            case 2:
              expressApp = _a.sent().default;
              app2 = expressApp;
              server.middlewares.use("/api", function(req, res, next) {
                var _a2;
                if ((_a2 = req.url) === null || _a2 === void 0 ? void 0 : _a2.startsWith("/chat")) {
                  res.setHeader("X-Accel-Buffering", "no");
                  var originalWrite_1 = res.write.bind(res);
                  res.write = function(chunk) {
                    var result = originalWrite_1(chunk);
                    if (typeof res.flush === "function")
                      res.flush();
                    return result;
                  };
                }
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcbiAgICBjbG91ZENsaWVudCA9IG5ldyBDbG91ZENsaWVudChjbGllbnRPcHRpb25zKTtcbiAgfVxuICByZXR1cm4gY2xvdWRDbGllbnQ7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRHbG9iYWxDb2xsZWN0aW9uKCkge1xuICBpZiAoIWdsb2JhbENvbGxlY3Rpb24pIHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gcHJvY2Vzcy5lbnYuQ0hST01BX0dMT0JBTF9DT0xMRUNUSU9OIHx8ICdkZXYnO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgIGRlc2NyaXB0aW9uOiAnUGVybWFuZW50IHNlZWQgZG9jdW1lbnRzIGZvciBSQUcnLFxuICAgICAgICAgIHR5cGU6ICdnbG9iYWxfa25vd2xlZGdlJ1xuICAgICAgICB9LFxuICAgICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBHbG9iYWwgY29sbGVjdGlvbiByZWFkeTogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGNvbm5lY3QgdG8gZ2xvYmFsIGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG4gIHJldHVybiBnbG9iYWxDb2xsZWN0aW9uO1xufVxuXG4vKipcbiAqIFJldHVybnMgeyBjb2xsZWN0aW9uLCBpc05ldyB9LlxuICogaXNOZXcgPSB0cnVlICBcdTIxOTIgZnJlc2hseSBjcmVhdGVkLCBuZWVkcyBzZWVkaW5nIGZyb20gZ2xvYmFsLlxuICogaXNOZXcgPSBmYWxzZSBcdTIxOTIgYWxyZWFkeSBleGlzdGVkIG9uIENocm9tYSBDbG91ZCwgcmVzcGVjdCBpdHMgY3VycmVudCBzdGF0ZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHsgY29sbGVjdGlvbjogc2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpLCBpc05ldzogZmFsc2UgfTtcbiAgfVxuXG4gIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcblxuICBsZXQgY29sbGVjdGlvbjtcbiAgbGV0IGlzTmV3O1xuXG4gIHRyeSB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICB9KTtcbiAgICBpc05ldyA9IGZhbHNlO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNjdiXFx1ZmUwZiAgU2Vzc2lvbiBjb2xsZWN0aW9uIGV4aXN0cywgcmV1c2luZzogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfSBjYXRjaCB7XG4gICAgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5jcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICBjcmVhdGVkOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIGlzTmV3ID0gdHJ1ZTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY3JlYXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgfVxuXG4gIHNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgcmV0dXJuIHsgY29sbGVjdGlvbiwgaXNOZXcgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBhd2FpdCBjbGllbnQuZGVsZXRlQ29sbGVjdGlvbih7IG5hbWU6IGNvbGxlY3Rpb25OYW1lIH0pO1xuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXFx1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gZGVsZXRlZDogJHtjb2xsZWN0aW9uTmFtZX1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gY29sbGVjdGlvbiAke2NvbGxlY3Rpb25OYW1lfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbi8qKlxuICogQWRkIHZlY3RvcnMgaW4gYmF0Y2hlcyBvZiBCQVRDSF9TSVpFIHRvIGF2b2lkIENocm9tYSBwYXlsb2FkIGxpbWl0cy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGFkZFZlY3RvcnMoY29sbGVjdGlvbiwgdmVjdG9ycywgZW1iZWRkaW5ncywgaWRzKSB7XG4gIHRyeSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBpZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGNvbnN0IGJhdGNoSWRzICAgICAgICA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzICA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyAgPSB2ZWN0b3JzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKS5tYXAodiA9PiB2Lm1ldGFkYXRhKTtcblxuICAgICAgYXdhaXQgY29sbGVjdGlvbi5hZGQoe1xuICAgICAgICBpZHM6ICAgICAgICBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6ICBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiAgYmF0Y2hNZXRhZGF0YXNcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgW2FkZFZlY3RvcnNdIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogYWRkZWQgJHtiYXRjaElkcy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgICB9XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGFkZCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBjb2xsZWN0aW9uLnF1ZXJ5KHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPiBkZWZhdWx0IDEwMCBsaW1pdCkgYXJlIGZ1bGx5IGRlbGV0ZWQuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCB9LFxuICAgICAgICBpbmNsdWRlOiBbXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGFsbElkcyB9KTtcbiAgICB9XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCB2ZWN0b3JzOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG4vKipcbiAqIExpc3QgYWxsIHVuaXF1ZSBkb2N1bWVudHMgaW4gYSBjb2xsZWN0aW9uLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgd2l0aCBCQVRDSF9TSVpFPTMwMCBzbyBjb2xsZWN0aW9ucyBsYXJnZXJcbiAqIHRoYW4gQ2hyb21hJ3MgZGVmYXVsdCBnZXQoKSBsaW1pdCAoMTAwKSBhcmUgZnVsbHkgZW51bWVyYXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcblxuICAgICAgYmF0Y2guaWRzLmZvckVhY2goKGlkLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgbWV0YSAgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2NJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiAgICAgICAgIG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogICAgICAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogICAgICAgbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6ICAgICAgbWV0YS5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIGZpcnN0X2NodW5rX3RleHQ6IGJhdGNoLmRvY3VtZW50c1tpZHhdXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBkb2MgPSBkb2N1bWVudHNNYXAuZ2V0KGRvY0lkKTtcbiAgICAgICAgZG9jLmNodW5rX2NvdW50Kys7XG4gICAgICAgIGRvYy5wYWdlX2NvdW50ID0gTWF0aC5tYXgoZG9jLnBhZ2VfY291bnQsIG1ldGEucGFnZV9udW1iZXIgfHwgMSk7XG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYCAgW2xpc3REb2N1bWVudHNdIG9mZnNldD0ke29mZnNldH0sIGdvdD0ke2JhdGNoLmlkcy5sZW5ndGh9LCB1bmlxdWUgc28gZmFyPSR7ZG9jdW1lbnRzTWFwLnNpemV9YCk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShkb2N1bWVudHNNYXAudmFsdWVzKCkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50czonLCBlcnJvcik7XG4gICAgcmV0dXJuIFtdO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGhDaGVjaygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGhlYXJ0YmVhdCA9IGF3YWl0IGNsaWVudC5oZWFydGJlYXQoKTtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAnaGVhbHRoeScsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIGhlYXJ0YmVhdFxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ3VuaGVhbHRoeScsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gY2xlYW51cFNlc3Npb25Db2xsZWN0aW9ucygpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGNvbnN0IGNvbGxlY3Rpb25zID0gYXdhaXQgY2xpZW50Lmxpc3RDb2xsZWN0aW9ucygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcXHUyNzA1IE5vIHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbnMgZm91bmQuJyk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFxcdWQ4M2VcXHVkZGY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFxcdTI3MDUgRGVsZXRlZDogJHtuYW1lfWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYCAgXFx1MjZhMFxcdWZlMGYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1xcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGNsZWFudXAgY29tcGxldGUuJyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS53YXJuKCdcXHUyNmEwXFx1ZmUwZiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7aW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5cbmNvbnN0IENIQVJTX1BFUl9UT0tFTiAgICAgPSA0O1xuY29uc3QgVEFSR0VUX0NIVU5LX1RPS0VOUyA9IDYwMDsgICAvLyBzb2Z0IHRhcmdldCBwZXIgY2h1bmtcbmNvbnN0IE1BWF9DSFVOS19UT0tFTlMgICAgPSA3NTA7ICAgLy8gaGFyZCBjYXAgYmVmb3JlIGZvcmNlZCBzcGxpdFxuY29uc3QgT1ZFUkxBUF9UT0tFTlMgICAgICA9IDEwMDsgICAvLyBvdmVybGFwIG9ubHkgb24gb3ZlcnNpemVkIHBhcmFncmFwaHNcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyAgICAgPSAxMDA7XG5cbi8vIE1hdGNoZXMgQUxMLUNBUFMgaGVhZGluZ3MsIG1hcmtkb3duIGhlYWRpbmdzLCBvciBudW1iZXJlZCBzZWN0aW9uIGhlYWRpbmdzXG5jb25zdCBIRUFESU5HX1JFID0gL14oPzpbQS1aXVtBLVpcXHNdezIsNjB9JHwjezEsNH1cXHMuK3woPzpcXGQrXFwuKStcXHMuKykvbTtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW5UZXh0KHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuICcnO1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC9cXGYvZywgJ1xcbicpXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJylcbiAgICAucmVwbGFjZSgvWyBcXHRdezIsfS9nLCAnICcpXG4gICAgLnRyaW0oKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDaHVua0lkKHRleHQsIGZpbGVuYW1lKSB7XG4gIHJldHVybiBjcmVhdGVIYXNoKCdtZDUnKVxuICAgIC51cGRhdGUoYCR7ZmlsZW5hbWV9Ojoke3RleHR9YClcbiAgICAuZGlnZXN0KCdoZXgnKVxuICAgIC5zbGljZSgwLCAxNik7XG59XG5cbi8qKlxuICogU3RydWN0dXJlLWF3YXJlIGNodW5raW5nOlxuICogIDEuIFNwbGl0IG9uIGJsYW5rIGxpbmVzIChcXG5cXG4pIGludG8gcGFyYWdyYXBocy5cbiAqICAyLiBBIGxpbmUgbWF0Y2hpbmcgSEVBRElOR19SRSBhbHdheXMgc3RhcnRzIGEgZnJlc2ggY2h1bmsuXG4gKiAgMy4gQWNjdW11bGF0ZSBwYXJhZ3JhcGhzIHVudGlsIHRoZSBzb2Z0IFRBUkdFVCBpcyByZWFjaGVkLCB0aGVuIGZsdXNoLlxuICogIDQuIFBhcmFncmFwaHMgbGFyZ2VyIHRoYW4gTUFYIGFyZSBzcGxpdCB3aXRoIGEgc2xpZGluZyB3aW5kb3cgKyBvdmVybGFwIGFzIGZhbGxiYWNrLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0YXJnZXRUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBUQVJHRVRfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBtYXhUb2tlbnMgICAgPSBvcHRpb25zLm1heENodW5rVG9rZW5zICB8fCBNQVhfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBvdmVybGFwVGsgICAgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgICB8fCBPVkVSTEFQX1RPS0VOUztcblxuICBjb25zdCB0YXJnZXRDaGFycyAgPSB0YXJnZXRUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG1heENoYXJzICAgICA9IG1heFRva2VucyAgICAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRrICAgICogQ0hBUlNfUEVSX1RPS0VOO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICAvLyAxLiBTcGxpdCBpbnRvIHBhcmFncmFwaHNcbiAgY29uc3QgcmF3UGFyYXMgPSB0ZXh0XG4gICAgLnNwbGl0KC9cXG57Mix9LylcbiAgICAubWFwKHAgPT4gcC50cmltKCkpXG4gICAgLmZpbHRlcihwID0+IHAubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUyk7XG5cbiAgY29uc3QgY2h1bmtzICAgICA9IFtdO1xuICBsZXQgICBidWZmZXIgICAgID0gJyc7XG4gIGxldCAgIGJ1ZlN0YXJ0ICAgPSAwO1xuICBsZXQgICBjaHVua0luZGV4ID0gMDtcbiAgbGV0ICAgY2hhckN1cnNvciA9IDA7XG5cbiAgY29uc3QgZmx1c2ggPSAoZm9yY2VUZXh0KSA9PiB7XG4gICAgY29uc3QgY29udGVudCA9IChmb3JjZVRleHQgPz8gYnVmZmVyKS50cmltKCk7XG4gICAgaWYgKGNvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiAgICAgICBjb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2Vucyhjb250ZW50KSxcbiAgICAgICAgY2hhclN0YXJ0OiAgYnVmU3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6ICAgIGJ1ZlN0YXJ0ICsgY29udGVudC5sZW5ndGgsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuICAgIGJ1ZmZlciAgID0gJyc7XG4gICAgYnVmU3RhcnQgPSBjaGFyQ3Vyc29yO1xuICB9O1xuXG4gIGZvciAoY29uc3QgcGFyYSBvZiByYXdQYXJhcykge1xuICAgIGNvbnN0IGlzSGVhZGluZyA9IEhFQURJTkdfUkUudGVzdChwYXJhLnNwbGl0KCdcXG4nKVswXSk7XG5cbiAgICAvLyAyLiBIZWFkaW5nIGFsd2F5cyBzdGFydHMgYSBuZXcgY2h1bmtcbiAgICBpZiAoaXNIZWFkaW5nICYmIGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgaWYgKHBhcmEubGVuZ3RoID4gbWF4Q2hhcnMpIHtcbiAgICAgIC8vIDMuIE92ZXJzaXplZCBwYXJhZ3JhcGggLT4gc2xpZGluZy13aW5kb3cgY2hhciBmYWxsYmFja1xuICAgICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgICBsZXQgcyA9IDA7XG4gICAgICB3aGlsZSAocyA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgIGxldCBlID0gcyArIHRhcmdldENoYXJzO1xuICAgICAgICBpZiAoZSA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgICAgY29uc3Qgc2VhcmNoRnJvbSA9IGUgLSBNYXRoLmZsb29yKHRhcmdldENoYXJzICogMC4yKTtcbiAgICAgICAgICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgICAgICAgICAgY29uc3QgaWR4ID0gcGFyYS5sYXN0SW5kZXhPZihicCwgZSk7XG4gICAgICAgICAgICBpZiAoaWR4ID4gc2VhcmNoRnJvbSkgeyBlID0gaWR4ICsgYnAubGVuZ3RoOyBicmVhazsgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBlID0gTWF0aC5taW4oZSwgcGFyYS5sZW5ndGgpO1xuICAgICAgICBjb25zdCBzbGljZSA9IHBhcmEuc2xpY2UocywgZSkudHJpbSgpO1xuICAgICAgICBpZiAoc2xpY2UubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgICAgIHRleHQ6ICAgICAgIHNsaWNlLFxuICAgICAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoc2xpY2UpLFxuICAgICAgICAgICAgY2hhclN0YXJ0OiAgY2hhckN1cnNvciArIHMsXG4gICAgICAgICAgICBjaGFyRW5kOiAgICBjaGFyQ3Vyc29yICsgZSxcbiAgICAgICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IG5leHQgPSBlIC0gb3ZlcmxhcENoYXJzO1xuICAgICAgICBzID0gbmV4dCA+IHMgPyBuZXh0IDogZTtcbiAgICAgIH1cbiAgICAgIGNoYXJDdXJzb3IgKz0gcGFyYS5sZW5ndGggKyAyO1xuICAgICAgYnVmU3RhcnQgICAgPSBjaGFyQ3Vyc29yO1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgLy8gNC4gTm9ybWFsIHBhcmFncmFwaCBcdTIwMTQgaGFyZCBjYXAgbG9va2FoZWFkIEJFRk9SRSBhY2N1bXVsYXRpbmdcbiAgICBpZiAoYnVmZmVyLmxlbmd0aCA+IDAgJiYgKGJ1ZmZlci5sZW5ndGggKyBwYXJhLmxlbmd0aCArIDIpID4gbWF4Q2hhcnMpIHtcbiAgICAgIGZsdXNoKCk7XG4gICAgfVxuXG4gICAgYnVmZmVyICAgICA9IGJ1ZmZlciA/IGJ1ZmZlciArICdcXG5cXG4nICsgcGFyYSA6IHBhcmE7XG4gICAgY2hhckN1cnNvciArPSBwYXJhLmxlbmd0aCArIDI7XG5cbiAgICAvLyBTb2Z0IGNhcDogZmx1c2ggb25jZSB0YXJnZXQgaXMgcmVhY2hlZFxuICAgIGlmIChidWZmZXIubGVuZ3RoID49IHRhcmdldENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDUuIEZsdXNoIHJlbWFpbmRlclxuICBmbHVzaCgpO1xuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgID0gY2h1bmtUZXh0KGNsZWFuZWRUZXh0LCBvcHRpb25zKTtcbiAgY29uc3QgdG90YWxDaHVua3MgPSB0ZXh0Q2h1bmtzLmxlbmd0aDtcbiAgY29uc3Qgc291cmNlVHlwZSAgPSBvcHRpb25zLnNvdXJjZVR5cGUgfHwgJ3BkZic7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+IHtcbiAgICBjb25zdCBjaHVua0lkID0gZ2VuZXJhdGVDaHVua0lkKGNodW5rLnRleHQsIGZpbGVuYW1lKTtcbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogICAgICAgICBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBjaHVuay5jaHVua0luZGV4LFxuICAgICAgICB0b3RhbF9jaHVua3M6ICAgICB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6ICAgICAgcGFnZU51bWJlciB8fCAxLFxuICAgICAgICB0b3RhbF9wYWdlczogICAgICB0b3RhbFBhZ2VzIHx8IG51bGwsXG4gICAgICAgIHNlY3Rpb25fdGl0bGU6ICAgIGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICAgIHNvdXJjZV90eXBlOiAgICAgIHNvdXJjZVR5cGUsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogICAgICAgY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogICAgICAgICBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogICAgICBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfTtcbiAgfSk7XG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RTZWN0aW9uVGl0bGUodGV4dCkge1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5BSSB9IGZyb20gJ0Bnb29nbGUvZ2VuYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDEuIFNMSURJTkcgV0lORE9XIFJBVEUgTElNSVRFUlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jbGFzcyBTbGlkaW5nV2luZG93UmF0ZUxpbWl0ZXIge1xuICBjb25zdHJ1Y3RvcihsaW1pdFBlck1pbnV0ZSkge1xuICAgIHRoaXMubGltaXRQZXJNaW51dGUgPSBsaW1pdFBlck1pbnV0ZTtcbiAgICB0aGlzLndpbmRvd01zID0gNjAwMDA7XG4gICAgdGhpcy5yZXF1ZXN0cyA9IFtdO1xuICB9XG5cbiAgYXN5bmMgY29uc3VtZSh0b2tlbnMpIHtcbiAgICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICAgIC8vIFJlbW92ZSBlbnRyaWVzIG9sZGVyIHRoYW4gNjAgc2Vjb25kc1xuICAgIHRoaXMucmVxdWVzdHMgPSB0aGlzLnJlcXVlc3RzLmZpbHRlcihyZXEgPT4gcmVxLnRpbWVzdGFtcCA+IG5vdyAtIHRoaXMud2luZG93TXMpO1xuXG4gICAgY29uc3QgY3VycmVudFRvdGFsID0gdGhpcy5yZXF1ZXN0cy5yZWR1Y2UoKHN1bSwgcmVxKSA9PiBzdW0gKyByZXEudG9rZW5zLCAwKTtcblxuICAgIC8vIElmIHdlIGhhdmUgcm9vbSwgY29uc3VtZSBpbnN0YW50bHkgKGJ1cnN0KVxuICAgIGlmIChjdXJyZW50VG90YWwgKyB0b2tlbnMgPD0gdGhpcy5saW1pdFBlck1pbnV0ZSkge1xuICAgICAgdGhpcy5yZXF1ZXN0cy5wdXNoKHsgdGltZXN0YW1wOiBub3csIHRva2VucyB9KTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBPdGhlcndpc2UsIHdhaXQgdW50aWwgdGhlIG9sZGVzdCByZXF1ZXN0IGV4cGlyZXMgKHBsdXMgYSBzbWFsbCBidWZmZXIpXG4gICAgY29uc3QgbmVlZGVkID0gdG9rZW5zIC0gKHRoaXMubGltaXRQZXJNaW51dGUgLSBjdXJyZW50VG90YWwpO1xuICAgIGxldCBhY2N1bXVsYXRlZEV4cGlyZWQgPSAwO1xuICAgIGxldCB3YWl0VW50aWwgPSBub3cgKyB0aGlzLndpbmRvd01zOyAvLyBmYWxsYmFja1xuXG4gICAgY29uc3Qgc29ydGVkID0gWy4uLnRoaXMucmVxdWVzdHNdLnNvcnQoKGEsIGIpID0+IGEudGltZXN0YW1wIC0gYi50aW1lc3RhbXApO1xuICAgIGZvciAoY29uc3QgcmVxIG9mIHNvcnRlZCkge1xuICAgICAgYWNjdW11bGF0ZWRFeHBpcmVkICs9IHJlcS50b2tlbnM7XG4gICAgICBpZiAoYWNjdW11bGF0ZWRFeHBpcmVkID49IG5lZWRlZCkge1xuICAgICAgICAvLyArMTBtcyBidWZmZXIgdG8gc2xpZGUgdGhlIHdpbmRvdyBjbGVhbmx5XG4gICAgICAgIHdhaXRVbnRpbCA9IHJlcS50aW1lc3RhbXAgKyB0aGlzLndpbmRvd01zICsgMTA7XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGRlbGF5ID0gd2FpdFVudGlsIC0gbm93O1xuICAgIGlmIChkZWxheSA+IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKFxuICAgICAgICBgW3JhdGUtbGltaXRdIFdpbmRvdyBmdWxsICgke2N1cnJlbnRUb3RhbH0vJHt0aGlzLmxpbWl0UGVyTWludXRlfSkuIGAgK1xuICAgICAgICBgV2FpdGluZyAkeyhkZWxheSAvIDEwMDApLnRvRml4ZWQoMSl9cyB0byBzZW5kICR7dG9rZW5zfSB0b2tlbnMuLi5gXG4gICAgICApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIGRlbGF5KSk7XG4gICAgfVxuXG4gICAgLy8gUmVjb3JkIHRoZSBjb25zdW1wdGlvbiBhdCB0aGUgbmV3IHRpbWVcbiAgICB0aGlzLnJlcXVlc3RzLnB1c2goeyB0aW1lc3RhbXA6IERhdGUubm93KCksIHRva2VucyB9KTtcbiAgICAvLyBDbGVhbnVwIGFnYWluIGp1c3QgaW4gY2FzZVxuICAgIHRoaXMucmVxdWVzdHMgPSB0aGlzLnJlcXVlc3RzLmZpbHRlcihyZXEgPT4gcmVxLnRpbWVzdGFtcCA+IERhdGUubm93KCkgLSB0aGlzLndpbmRvd01zKTtcbiAgfVxufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDIuIENPTkZJR1VSQVRJT05cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc3QgVFBNX0xJTUlUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19UUE1fTElNSVQpIHx8IDUwMDAwMDtcbmNvbnN0IFJBVEVfTElNSVRFUiA9IG5ldyBTbGlkaW5nV2luZG93UmF0ZUxpbWl0ZXIoVFBNX0xJTUlUKTtcblxuLy8gQkFUQ0hfU0laRTogbnVtYmVyIG9mIGNodW5rcyBwZXIgZW1iZWRDb250ZW50IGNhbGxcbi8vIChrZXB0IGF0IDEwOyBub3RlIHRoZSByZWFsIGNlaWxpbmcgaXMgdGhlIEFQSSdzIH4xMDAtcmVxdWVzdHMtcGVyLWNhbGwgbGltaXQsXG4vLyBub3QgYSBcImNvbnRleHQgd2luZG93XCIgbGltaXQgXHUyMDE0IDEwIGp1c3Qga2VlcHMgYmF0Y2hlcyBzbWFsbCBhbmQgcmV0cnktZnJpZW5kbHkpXG5jb25zdCBCQVRDSF9TSVpFID0gKCkgPT4gMTA7ICAgLy8gMTAgY2h1bmtzIFx1MDBENyA3NTAgdG9rZW5zID0gNyw1MDAgdG9rZW5zIHBlciBBUEkgcmVxdWVzdFxuY29uc3QgUEFSQUxMRUxfQ0FMTFMgPSAoKSA9PiAxMDsgLy8gU2VuZCAxMCBiYXRjaGVzIGNvbmN1cnJlbnRseSB0byBjbGVhciB0aGUgYnVyc3QgZmFzdFxuXG4vLyBSZXRyeSBjb25maWd1cmF0aW9uIChleHBvbmVudGlhbCBiYWNrb2ZmICsgaml0dGVyKVxuY29uc3QgUkVUUllfQkFTRV9ERUxBWV9NUyA9IDIwMDA7ICAgLy8gMiBzZWNvbmRzXG5jb25zdCBSRVRSWV9NQVhfREVMQVlfTVMgPSA2MDAwMDsgICAvLyA2MCBzZWNvbmRzIGNhcFxuY29uc3QgTUFYX1JFVFJZX0FUVEVNUFRTID0gNTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAzLiBBSSBDTElFTlQgKHNpbmdsZSwgcmV1c2FibGUgaW5zdGFuY2UpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnN0IGFpID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgdmVydGV4YWk6IHRydWUsXG4gIHByb2plY3Q6IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9QUk9KRUNUIHx8IHByb2Nlc3MuZW52LkdDUF9QUk9KRUNUIHx8ICdwcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAnLFxuICBsb2NhdGlvbjogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX0xPQ0FUSU9OIHx8ICd1cy1jZW50cmFsMSdcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDQuIFRPS0VOIENBTENVTEFUSU9OICh1c2VzIHN0b3JlZCB0b2tlbl9jb3VudCBpZiBhdmFpbGFibGUpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmZ1bmN0aW9uIGdldFRva2VuQ291bnRGb3JDaHVua3MoY2h1bmtzKSB7XG4gIHJldHVybiBjaHVua3MucmVkdWNlKChzdW0sIGNodW5rKSA9PiB7XG4gICAgLy8gUHJlZmVyIHRoZSBleGFjdCB0b2tlbiBjb3VudCBmcm9tIGNodW5rZXIsIG90aGVyd2lzZSBmYWxsYmFjayB0byByb3VnaCBlc3RpbWF0ZVxuICAgIGNvbnN0IHRva2VuQ291bnQgPSBjaHVuay5tZXRhZGF0YT8udG9rZW5fY291bnQgfHwgTWF0aC5jZWlsKGNodW5rLnRleHQubGVuZ3RoIC8gNCk7XG4gICAgcmV0dXJuIHN1bSArIHRva2VuQ291bnQ7XG4gIH0sIDApO1xufVxuXG4vLyBTYW1lIHJvdWdoIGVzdGltYXRlIGFzIGFib3ZlLCBidXQgZm9yIHJhdyBzdHJpbmdzIHRoYXQgZG9uJ3QgY2FycnkgY2h1bmsgbWV0YWRhdGFcbi8vICh1c2VkIGZvciByZXRyaWVzIGluc2lkZSBlbWJlZEJhdGNoLCBhbmQgZm9yIGVtYmVkUXVlcnkpLlxuZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykge1xuICByZXR1cm4gdGV4dHMucmVkdWNlKChzdW0sIHRleHQpID0+IHN1bSArIE1hdGguY2VpbChTdHJpbmcodGV4dCkubGVuZ3RoIC8gNCksIDApO1xufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDUuIEVNQkVEIEJBVENIICh3aXRoIGV4cG9uZW50aWFsIGJhY2tvZmYgKyBqaXR0ZXIpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFzeW5jIGZ1bmN0aW9uIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIGF0dGVtcHQgPSAxKSB7XG4gIGNvbnN0IG1vZGVsTmFtZSA9IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMDAxJztcbiAgY29uc3Qgb3V0cHV0RGltZW5zaW9uYWxpdHkgPSBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7XG5cbiAgdHJ5IHtcbiAgICAvLyBGSVg6IGBhaS5iYXRjaGVzLmNyZWF0ZUVtYmVkZGluZ3NgIGlzIG5vdCBhIHJlYWwgbWV0aG9kIG9uIHRoZSBAZ29vZ2xlL2dlbmFpIFNESy5cbiAgICAvLyBgYWkuYmF0Y2hlc2AgaXMgZm9yIGFzeW5jIGJhdGNoLXByZWRpY3Rpb24gam9icy4gU3luY2hyb25vdXMgZW1iZWRkaW5nIGNhbGxzIGdvXG4gICAgLy8gdGhyb3VnaCBgYWkubW9kZWxzLmVtYmVkQ29udGVudGAsIHdpdGggb25lIHNoYXJlZCB0YXNrVHlwZS9vdXRwdXREaW1lbnNpb25hbGl0eVxuICAgIC8vIGNvbmZpZyBhcHBsaWVkIGFjcm9zcyBhbGwgYGNvbnRlbnRzYCBpbiB0aGUgY2FsbC5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IGFpLm1vZGVscy5lbWJlZENvbnRlbnQoe1xuICAgICAgbW9kZWw6IG1vZGVsTmFtZSxcbiAgICAgIGNvbnRlbnRzOiB0ZXh0cy5tYXAodGV4dCA9PiAodHlwZW9mIHRleHQgPT09ICdzdHJpbmcnID8gdGV4dCA6IFN0cmluZyh0ZXh0KSkpLFxuICAgICAgY29uZmlnOiB7XG4gICAgICAgIHRhc2tUeXBlOiB0YXNrVHlwZSxcbiAgICAgICAgb3V0cHV0RGltZW5zaW9uYWxpdHk6IG91dHB1dERpbWVuc2lvbmFsaXR5XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjb25zdCBlbWJlZGRpbmdzID0gcmVzcG9uc2U/LmVtYmVkZGluZ3M/Lm1hcChlID0+IGUudmFsdWVzKSB8fCBbXTtcbiAgICBpZiAoZW1iZWRkaW5ncy5sZW5ndGggIT09IHRleHRzLmxlbmd0aCkge1xuICAgICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGBFeHBlY3RlZCAke3RleHRzLmxlbmd0aH0gZW1iZWRkaW5ncywgZ290ICR7ZW1iZWRkaW5ncy5sZW5ndGh9YCk7XG4gICAgfVxuICAgIHJldHVybiBlbWJlZGRpbmdzO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc3QgaXNSZXRyeWFibGUgPSBpczQyOUVycm9yKGVycm9yKSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDIgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMyB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdTZXJ2aWNlIFVuYXZhaWxhYmxlJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnQmFkIEdhdGV3YXknKTtcblxuICAgIGlmIChpc1JldHJ5YWJsZSAmJiBhdHRlbXB0IDwgTUFYX1JFVFJZX0FUVEVNUFRTKSB7XG4gICAgICAvLyBFeHBvbmVudGlhbCBiYWNrb2ZmOiAyXmF0dGVtcHQgKiBiYXNlIChjYXBwZWQpXG4gICAgICBsZXQgZGVsYXkgPSBNYXRoLm1pbihSRVRSWV9NQVhfREVMQVlfTVMsIFJFVFJZX0JBU0VfREVMQVlfTVMgKiBNYXRoLnBvdygyLCBhdHRlbXB0IC0gMSkpO1xuICAgICAgLy8gQWRkIGppdHRlciAoMC44XHUyMDEzMS4yeCkgdG8gYXZvaWQgdGh1bmRlcmluZyBoZXJkXG4gICAgICBjb25zdCBqaXR0ZXIgPSAwLjggKyAoMC40ICogTWF0aC5yYW5kb20oKSk7XG4gICAgICBkZWxheSA9IE1hdGguZmxvb3IoZGVsYXkgKiBqaXR0ZXIpO1xuICAgICAgLy8gUmVzcGVjdCByZXRyeS1hZnRlciBoZWFkZXIgaWYgcHJlc2VudFxuICAgICAgaWYgKGVycm9yLnJldHJ5QWZ0ZXIpIHtcbiAgICAgICAgZGVsYXkgPSBNYXRoLm1heChkZWxheSwgZXJyb3IucmV0cnlBZnRlciAqIDEwMDApO1xuICAgICAgfVxuXG4gICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgYFtlbWJlZGRpbmddIFx1MjNGMyBSZXRyeWFibGUgZXJyb3IgKCR7ZXJyb3I/LnN0YXR1cyB8fCAndW5rbm93bid9KSwgYCArXG4gICAgICAgIGB3YWl0aW5nICR7KGRlbGF5IC8gMTAwMCkudG9GaXhlZCgxKX1zIChhdHRlbXB0ICR7YXR0ZW1wdH0vJHtNQVhfUkVUUllfQVRURU1QVFN9KS4uLmBcbiAgICAgICk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgZGVsYXkpKTtcblxuICAgICAgLy8gRklYOiBhIHJldHJ5IGlzIGEgYnJhbmQgbmV3IEFQSSBjYWxsIGFuZCBjb25zdW1lcyByZWFsIHF1b3RhLCBldmVuIHRob3VnaFxuICAgICAgLy8gdGhlIG9yaWdpbmFsIGNhbGwgZmFpbGVkLiBTa2lwcGluZyBjb25zdW1wdGlvbiBoZXJlIChhcyBiZWZvcmUpIGxldCB0aGUgbG9jYWxcbiAgICAgIC8vIGxpbWl0ZXIgdW5kZXItcmVwb3J0IGFjdHVhbCB1c2FnZSBkdXJpbmcgZXJyb3Igc3Rvcm1zLCB3aGljaCBtZWFudCBpdCBrZXB0XG4gICAgICAvLyB3YXZpbmcgdGhyb3VnaCBuZXcgZ3JvdXBzIHdoaWxlIHJldHJpZXMgd2VyZSBhbHNvIGhpdHRpbmcgdGhlIEFQSSBcdTIwMTQgbWFraW5nXG4gICAgICAvLyA0Mjkgc3Rvcm1zIHdvcnNlIGluc3RlYWQgb2YgYmFja2luZyBvZmYgZnJvbSB0aGVtLlxuICAgICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykpO1xuXG4gICAgICByZXR1cm4gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUsIGF0dGVtcHQgKyAxKTtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnQmF0Y2ggZW1iZWRkaW5nIGZhaWxlZCcpO1xuICB9XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNi4gRVhQT1JURUQgZ2VuZXJhdGVFbWJlZGRpbmdzICh3aXRoIHJhdGUgbGltaXRlciAmIGFjY3VyYXRlIHRva2Vucylcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSA9IEJBVENIX1NJWkUoKTtcbiAgY29uc3QgcGFyYWxsZWxDYWxscyA9IFBBUkFMTEVMX0NBTExTKCk7XG5cbiAgLy8gRml4ZWQtc2l6ZSBhcnJheSB0byBwcmVzZXJ2ZSBjaHJvbm9sb2dpY2FsIG9yZGVyXG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBuZXcgQXJyYXkoY2h1bmtzLmxlbmd0aCk7XG5cbiAgLy8gR3JvdXAgY2h1bmtzIGludG8gYmF0Y2hlcyB3aXRoIHRoZWlyIHN0YXJ0aW5nIGluZGV4XG4gIGNvbnN0IGJhdGNoZXMgPSBbXTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IGJhdGNoU2l6ZSkge1xuICAgIGJhdGNoZXMucHVzaCh7XG4gICAgICBjaHVua3M6IGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSxcbiAgICAgIHN0YXJ0SW5kZXg6IGlcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG5cbiAgICAvLyBDYWxjdWxhdGUgZXhhY3QgdG9rZW5zIHVzaW5nIHN0b3JlZCB0b2tlbl9jb3VudCAob3IgZmFsbGJhY2spXG4gICAgY29uc3QgYWxsQ2h1bmtzSW5Hcm91cCA9IHBhcmFsbGVsQmF0Y2hlcy5mbGF0TWFwKGIgPT4gYi5jaHVua3MpO1xuICAgIGNvbnN0IHRva2Vuc1RvQ29uc3VtZSA9IGdldFRva2VuQ291bnRGb3JDaHVua3MoYWxsQ2h1bmtzSW5Hcm91cCk7XG4gICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUodG9rZW5zVG9Db25zdW1lKTtcblxuICAgIGNvbnNvbGUubG9nKFxuICAgICAgYFtlbWJlZGRpbmddIEdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCBmaXJpbmcgJHtwYXJhbGxlbEJhdGNoZXMubGVuZ3RofSBiYXRjaGVzIGAgK1xuICAgICAgYGluIHBhcmFsbGVsICgke3Rva2Vuc1RvQ29uc3VtZX0gdG9rZW5zKWBcbiAgICApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHBhcmFsbGVsQmF0Y2hlcy5tYXAoYiA9PiBlbWJlZEJhdGNoKGIuY2h1bmtzLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgY29uc3QgZmFpbGVkQmF0Y2hlcyA9IFtdO1xuICAgIHJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgY29uc3QgY3VycmVudEJhdGNoSW5mbyA9IHBhcmFsbGVsQmF0Y2hlc1tiYXRjaElkeF07XG4gICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgY29uc3QgdmVjdG9ycyA9IHJlc3VsdC52YWx1ZTtcbiAgICAgICAgY3VycmVudEJhdGNoSW5mby5jaHVua3MuZm9yRWFjaCgoY2h1bmssIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgY29uc3QgZ2xvYmFsSW5kZXggPSBjdXJyZW50QmF0Y2hJbmZvLnN0YXJ0SW5kZXggKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzW2dsb2JhbEluZGV4XSA9IHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rXyR7Z2xvYmFsSW5kZXh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1tjaHVua0lkeF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfTtcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtlbWJlZGRpbmddIEJhdGNoIHN0YXJ0aW5nIGF0IGluZGV4ICR7Y3VycmVudEJhdGNoSW5mby5zdGFydEluZGV4fSBmYWlsZWQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZEJhdGNoZXMucHVzaChjdXJyZW50QmF0Y2hJbmZvKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIC8vIFJldHJ5IGZhaWxlZCBiYXRjaGVzIGluZGl2aWR1YWxseVxuICAgIGZvciAoY29uc3QgZmFpbGVkQmF0Y2ggb2YgZmFpbGVkQmF0Y2hlcykge1xuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFJldHJ5aW5nIGZhaWxlZCBiYXRjaCBlbGVtZW50cyBzdGFydGluZyBhdCBpbmRleCAke2ZhaWxlZEJhdGNoLnN0YXJ0SW5kZXh9Li4uYCk7XG4gICAgICBmb3IgKGxldCBjaHVua0lkeCA9IDA7IGNodW5rSWR4IDwgZmFpbGVkQmF0Y2guY2h1bmtzLmxlbmd0aDsgY2h1bmtJZHgrKykge1xuICAgICAgICBjb25zdCBjaHVuayA9IGZhaWxlZEJhdGNoLmNodW5rc1tjaHVua0lkeF07XG4gICAgICAgIGNvbnN0IGdsb2JhbEluZGV4ID0gZmFpbGVkQmF0Y2guc3RhcnRJbmRleCArIGNodW5rSWR4O1xuICAgICAgICB0cnkge1xuICAgICAgICAgIC8vIEZJWDogdGhpcyByZXRyeSBpcyBhIGZyZXNoLCByZWFsIEFQSSBjYWxsIFx1MjAxNCB0cmFjayBpdHMgdG9rZW5zIGFnYWluc3RcbiAgICAgICAgICAvLyB0aGUgbGltaXRlciBpbnN0ZWFkIG9mIGFzc3VtaW5nIGl0IHdhcyBcImFscmVhZHkgcGFpZCBmb3JcIi5cbiAgICAgICAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKFtjaHVua10pKTtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzW2dsb2JhbEluZGV4XSA9IHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rX3JldHJ5XyR7Z2xvYmFsSW5kZXh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1swXSxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVuay5tZXRhZGF0YSxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rLnRleHRcbiAgICAgICAgICB9O1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBnbG9iYWxJbmRleH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW2VtYmVkZGluZ10gXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgZ2xvYmFsSW5kZXh9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8vIEZJWDogcGVybWFuZW50bHktZmFpbGVkIGNodW5rcyBhcmUgZHJvcHBlZCBoZXJlLCB3aGljaCBzaGlmdHMgYXJyYXkgaW5kaWNlc1xuICAvLyByZWxhdGl2ZSB0byB0aGUgb3JpZ2luYWwgYGNodW5rc2AgaW5wdXQuIFRoaXMgbG9nIG1ha2VzIHRoYXQgbG9zcyB2aXNpYmxlXG4gIC8vIGluc3RlYWQgb2Ygc2lsZW50OyBjYWxsZXJzIHRoYXQgbmVlZCB0byBrbm93IGV4YWN0bHkgd2hpY2ggY2h1bmtzIHdlcmUgbG9zdFxuICAvLyBjYW4gY29tcGFyZSByZXR1cm5lZCBgaWRgcyBhZ2FpbnN0IHRoZWlyIG9yaWdpbmFsIGNodW5rIGxpc3QuXG4gIGNvbnN0IGZhaWxlZENvdW50ID0gZW1iZWRkaW5ncy5maWx0ZXIoZSA9PiAhZSkubGVuZ3RoO1xuICBpZiAoZmFpbGVkQ291bnQgPiAwKSB7XG4gICAgY29uc29sZS53YXJuKGBbZW1iZWRkaW5nXSAke2ZhaWxlZENvdW50fS8ke2NodW5rcy5sZW5ndGh9IGNodW5rKHMpIHBlcm1hbmVudGx5IGZhaWxlZCB0byBlbWJlZCBhbmQgd2VyZSBkcm9wcGVkLmApO1xuICB9XG5cbiAgLy8gRmlsdGVyIG91dCBhbnkgZWxlbWVudHMgdGhhdCBwZXJtYW5lbnRseSBmYWlsZWRcbiAgcmV0dXJuIGVtYmVkZGluZ3MuZmlsdGVyKEJvb2xlYW4pO1xufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDcuIEVYUE9SVEVEIGVtYmVkUXVlcnlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgLy8gRklYOiB0aGlzIGNhbGwgd2FzIGJ5cGFzc2luZyB0aGUgcmF0ZSBsaW1pdGVyIGVudGlyZWx5LiBJZiBpdCBydW5zIGNvbmN1cnJlbnRseVxuICAvLyB3aXRoIGRvY3VtZW50IGluZ2VzdGlvbiAoZS5nLiBhIHVzZXIgc2VhcmNoZXMgd2hpbGUgYSBiYXRjaCBqb2IgaXMgaW4gZmxpZ2h0KSxcbiAgLy8gaXQgY291bGQgcHVzaCB0b3RhbCB1c2FnZSBvdmVyIHRoZSBjb25maWd1cmVkIFRQTSBidWRnZXQgdW5ub3RpY2VkLlxuICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKFtxdWVyeV0pKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3F1ZXJ5XSwgJ1JFVFJJRVZBTF9RVUVSWScpO1xuICByZXR1cm4gdmVjdG9yc1swXTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJykge1xuICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gZW1iZWRTaW5nbGVCYXRjaEdyb3VwIFx1MjAxNCAke3RleHRzLmxlbmd0aH0gdGV4dHMsIHRhc2tUeXBlPSR7dGFza1R5cGV9YCk7XG4gIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlKTtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgZ290ICR7dmVjdG9ycy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgcmV0dXJuIHZlY3RvcnM7XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7XG4gIGdldEdsb2JhbENvbGxlY3Rpb24sXG4gIGdldFNlc3Npb25Db2xsZWN0aW9uLFxuICBsaXN0RG9jdW1lbnRzLFxuICBhZGRWZWN0b3JzXG59IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTID0gNjA7XG5jb25zdCBzZXNzaW9ucyA9IG5ldyBNYXAoKTtcbmNvbnN0IE1BWF9QREZTX1BFUl9TRVNTSU9OID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04pIHx8IDM7XG5jb25zdCBNQVhfVVBMT0FEX1NJWkVfTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIpIHx8IDU7XG5cbmNvbnN0IHNlZWRlZFNlc3Npb25zID0gbmV3IFNldCgpO1xuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3QgaWQgPSBzZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IHNlc3Npb24gPSB7XG4gICAgaWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIGRlbGV0ZWREb2N1bWVudElkczogbmV3IFNldCgpLFxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuICBzZXNzaW9ucy5zZXQoaWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykgcmV0dXJuIGV4aXN0aW5nO1xuICAgIHJldHVybiBjcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gIH1cbiAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikge1xuICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICBjb25zdCBsYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZShzZXNzaW9uLmxhc3RBY2Nlc3NlZCkuZ2V0VGltZSgpO1xuICBjb25zdCB0aW1lb3V0TXMgPSBzZXNzaW9uLnRpbWVvdXRNaW51dGVzICogNjAgKiAxMDAwO1xuICByZXR1cm4gKG5vdyAtIGxhc3RBY2Nlc3NlZCkgPiB0aW1lb3V0TXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBzZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgc2VlZGVkU2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBDaGVjayBpZiBzZXNzaW9uIGlzIHNlZWRlZCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25TZWVkZWQoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIE5vdGlmeSBTU0UgbGlzdGVuZXJzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZnVuY3Rpb24gbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCkge1xuICBpZiAoZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMgJiYgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuaGFzKGBzZWVkaW5nOiR7c2Vzc2lvbklkfWApKSB7XG4gICAgY29uc3QgZXZlbnRLZXkgPSBgc2VlZGluZzoke3Nlc3Npb25JZH1gO1xuICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgbGlzdGVuZXJzLmZvckVhY2goKHJlc3BvbnNlKSA9PiB7XG4gICAgICB0cnkge1xuICAgICAgICByZXNwb25zZS53cml0ZShgZXZlbnQ6IHNlZWRpbmdfY29tcGxldGVcXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KHsgc2Vzc2lvbklkLCBzZWVkZWQ6IHRydWUgfSl9XFxuXFxuYCk7XG4gICAgICAgIHJlc3BvbnNlLmVuZCgpO1xuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoYFtub3RpZnldIEZhaWxlZCB0byBub3RpZnkgbGlzdGVuZXI6YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgY29uc29sZS5sb2coYFtub3RpZnldIE5vdGlmaWVkICR7bGlzdGVuZXJzLmxlbmd0aH0gU1NFIGxpc3RlbmVycyBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgfVxufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIElmIGNvbGxlY3Rpb24gaXMgTkVXIFx1MjE5MiBzZWVkIGZyb20gZ2xvYmFsIChwYWdpbmF0ZWQsIDMwMC9iYXRjaClcbiAqIC0gSWYgY29sbGVjdGlvbiBFWElTVFMgXHUyMTkyIHNraXAgc2VlZCwgcmVjb25zdHJ1Y3QgaW4tbWVtb3J5IGRvYyBsaXN0IGZyb20gQ2hyb21hXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhgXHVEODNEXHVERDExIFNlc3Npb24gaW5pdDogJHtzZXNzaW9uSWR9YCk7XG4gIGlmIChzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWxyZWFkeSBzZWVkZWQgJHtzZXNzaW9uSWR9LCBza2lwcGluZ2ApO1xuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb246IHNlc3Npb25Db2xsZWN0aW9uLCBpc05ldyB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGlmICghaXNOZXcpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI2N0JcdUZFMEYgIFNlc3Npb24gZXhpc3RzLCByZWNvbnN0cnVjdGluZyBkb2N1bWVudCBsaXN0IGZyb20gQ2hyb21hLi4uYCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKHNlc3Npb25Db2xsZWN0aW9uKTtcbiAgICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgICAgc291cmNlVHlwZTogZG9jLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IGRvY3VtZW50KHMpIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHVEODNDXHVERjMxIE5ldyBzZXNzaW9uIFx1MjAxNCBzZWVkaW5nIGZyb20gZ2xvYmFsIGNvbGxlY3Rpb24uLi5gKTtcblxuICAgIGNvbnN0IEJBVENIX1NJWkUgPSAzMDA7XG4gICAgbGV0IG9mZnNldCA9IDA7XG4gICAgY29uc3QgYWxsSWRzID0gW10sIGFsbEVtYmVkZGluZ3MgPSBbXSwgYWxsRG9jdW1lbnRzID0gW10sIGFsbE1ldGFkYXRhcyA9IFtdO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgZ2xvYmFsQ29sbGVjdGlvbi5nZXQoe1xuICAgICAgICBpbmNsdWRlOiBbJ2VtYmVkZGluZ3MnLCAnZG9jdW1lbnRzJywgJ21ldGFkYXRhcyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcbiAgICAgIGFsbEVtYmVkZGluZ3MucHVzaCguLi5iYXRjaC5lbWJlZGRpbmdzKTtcbiAgICAgIGFsbERvY3VtZW50cy5wdXNoKC4uLmJhdGNoLmRvY3VtZW50cyk7XG4gICAgICBhbGxNZXRhZGF0YXMucHVzaCguLi5iYXRjaC5tZXRhZGF0YXMpO1xuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID09PSAwKSB7XG4gICAgICBjb25zb2xlLmxvZygnXHUyNkEwXHVGRTBGICBHbG9iYWwgY29sbGVjdGlvbiBpcyBlbXB0eSBcdTIwMTQgbm90aGluZyB0byBzZWVkLicpO1xuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbElkcy5sZW5ndGg7IGkgKz0gQkFUQ0hfU0laRSkge1xuICAgICAgYXdhaXQgc2Vzc2lvbkNvbGxlY3Rpb24uYWRkKHtcbiAgICAgICAgaWRzOiBhbGxJZHMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLFxuICAgICAgICBlbWJlZGRpbmdzOiBhbGxFbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSxcbiAgICAgICAgZG9jdW1lbnRzOiBhbGxEb2N1bWVudHMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLFxuICAgICAgICBtZXRhZGF0YXM6IGFsbE1ldGFkYXRhcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKG0gPT4gKHsgLi4ubSwgc291cmNlX3R5cGU6ICdnbG9iYWwnIH0pKVxuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgICBcdUQ4M0RcdURDRTYgQWRkZWQgYmF0Y2ggJHtNYXRoLmZsb29yKGkgLyBCQVRDSF9TSVpFKSArIDF9OiByZWNvcmRzICR7aSArIDF9XHUyMDEzJHtNYXRoLm1pbihpICsgQkFUQ0hfU0laRSwgYWxsSWRzLmxlbmd0aCl9YCk7XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFx1MjcwNSBTZWVkZWQgJHthbGxJZHMubGVuZ3RofSB2ZWN0b3JzIGludG8gc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgICBzZWVkZWRTZXNzaW9ucy5hZGQoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKHNlc3Npb24pIHtcbiAgICAgIGNvbnN0IGRvY3NNYXAgPSBuZXcgTWFwKCk7XG4gICAgICBhbGxNZXRhZGF0YXMuZm9yRWFjaChtZXRhID0+IHtcbiAgICAgICAgaWYgKCFkb2NzTWFwLmhhcyhtZXRhLmRvY3VtZW50X2lkKSkge1xuICAgICAgICAgIGRvY3NNYXAuc2V0KG1ldGEuZG9jdW1lbnRfaWQsIHtcbiAgICAgICAgICAgIGlkOiBtZXRhLmRvY3VtZW50X2lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBmaWxlU2l6ZTogbnVsbCxcbiAgICAgICAgICAgIHBhZ2VDb3VudDogbWV0YS50b3RhbF9wYWdlcyB8fCBudWxsLFxuICAgICAgICAgICAgY2h1bmtDb3VudDogMCxcbiAgICAgICAgICAgIHNvdXJjZVR5cGU6ICdnbG9iYWwnLFxuICAgICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBtZXRhLnVwbG9hZF90aW1lc3RhbXBcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgICBkb2NzTWFwLmdldChtZXRhLmRvY3VtZW50X2lkKS5jaHVua0NvdW50Kys7XG4gICAgICB9KTtcblxuICAgICAgZm9yIChjb25zdCBkb2Mgb2YgZG9jc01hcC52YWx1ZXMoKSkge1xuICAgICAgICBpZiAoIXNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmlkID09PSBkb2MuaWQpKSB7XG4gICAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaChkb2MpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gU3RpbGwgbm90aWZ5IGxpc3RlbmVycyBzbyB0aGV5IGRvbid0IGhhbmcgZm9yZXZlclxuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBEb2N1bWVudCBtYW5hZ2VtZW50IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGZ1bmN0aW9uIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJbmZvKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuXG4gIGNvbnN0IGV4aXN0aW5nID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZChkID0+IGQuaWQgPT09IGRvY3VtZW50SW5mby5pZCk7XG5cbiAgaWYgKGV4aXN0aW5nKSB7XG4gICAgaWYgKGRvY3VtZW50SW5mby5jaHVua0NvdW50ICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmNodW5rQ291bnQgPSBkb2N1bWVudEluZm8uY2h1bmtDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5wYWdlQ291bnQgPSBkb2N1bWVudEluZm8ucGFnZUNvdW50O1xuICAgIGlmIChkb2N1bWVudEluZm8uZmlsZVNpemUgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuZmlsZVNpemUgPSBkb2N1bWVudEluZm8uZmlsZVNpemU7XG4gICAgaWYgKGRvY3VtZW50SW5mby5zdGF0dXMgIT09IHVuZGVmaW5lZCkgZXhpc3Rpbmcuc3RhdHVzID0gZG9jdW1lbnRJbmZvLnN0YXR1cztcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVuYW1lICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVuYW1lID0gZG9jdW1lbnRJbmZvLmZpbGVuYW1lO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFVwZGF0ZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZXhpc3Rpbmcuc3RhdHVzfSwgY2h1bmtzPSR7ZXhpc3RpbmcuY2h1bmtDb3VudH1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgPz8gMCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgIHN0YXR1czogZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnXG4gIH0pO1xuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWRkZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnfWApO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5kZWxldGVkRG9jdW1lbnRJZHMuYWRkKGRvY3VtZW50SWQpO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFJlbW92ZWQgZG9jICR7ZG9jdW1lbnRJZH0sIGFkZGVkIHRvIGRlbGV0ZWREb2N1bWVudElkc2ApO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldERlbGV0ZWREb2N1bWVudElkcyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHNlc3Npb24/LmRlbGV0ZWREb2N1bWVudElkcyA/PyBuZXcgU2V0KCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBbXTtcbiAgcmV0dXJuIHNlc3Npb24uZG9jdW1lbnRzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IHNlc3Npb25Eb2N1bWVudHM6IFtdLCBnbG9iYWxEb2N1bWVudHM6IFtdIH07XG5cbiAgY29uc3Qgbm9ybWFsaXplID0gKGRvYykgPT4gKHtcbiAgICBkb2N1bWVudF9pZDogZG9jLmlkLFxuICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgY2h1bmtfY291bnQ6IGRvYy5jaHVua0NvdW50ID8/IDAsXG4gICAgcGFnZV9jb3VudDogZG9jLnBhZ2VDb3VudCA/PyAwLFxuICAgIHVwbG9hZF90aW1lc3RhbXA6IGRvYy51cGxvYWRUaW1lc3RhbXAgfHwgbnVsbCxcbiAgICBzb3VyY2VfdHlwZTogZG9jLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcgPyAnc2Vzc2lvbl91cGxvYWQnIDogJ3NlZWQnLFxuICAgIGZpbGVTaXplOiBkb2MuZmlsZVNpemUgfHwgbnVsbCxcbiAgICBzdGF0dXM6IGRvYy5zdGF0dXMgPz8gbnVsbFxuICB9KTtcblxuICByZXR1cm4ge1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKVxuICAgICAgLm1hcChub3JtYWxpemUpLFxuICAgIGdsb2JhbERvY3VtZW50czogc2Vzc2lvbi5kb2N1bWVudHNcbiAgICAgIC5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdnbG9iYWwnKVxuICAgICAgLm1hcChub3JtYWxpemUpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIHJldHVybiB7XG4gICAgaWQ6IHNlc3Npb24uaWQsXG4gICAgZG9jdW1lbnRDb3VudDogc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogc2Vzc2lvbi5jcmVhdGVkQXQsXG4gICAgbGFzdEFjY2Vzc2VkOiBzZXNzaW9uLmxhc3RBY2Nlc3NlZCxcbiAgICB0b3RhbFNpemU6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5maWxlU2l6ZSB8fCAwKSwgMCksXG4gICAgdG90YWxDaHVua3M6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdEFjdGl2ZVNlc3Npb25zKCkge1xuICByZXR1cm4gQXJyYXkuZnJvbShzZXNzaW9ucy52YWx1ZXMoKSkuZmlsdGVyKHMgPT4gIWlzU2Vzc2lvbkV4cGlyZWQocykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW51cEV4cGlyZWRTZXNzaW9ucygpIHtcbiAgbGV0IGNsZWFuZWQgPSAwO1xuICBmb3IgKGNvbnN0IFtpZCwgc2Vzc2lvbl0gb2Ygc2Vzc2lvbnMpIHtcbiAgICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgICAgc2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBjbGVhbmVkKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBjbGVhbmVkO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgNTtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpOyAvLyBkZXN0cnVjdHVyZVxuICAgIGlmIChjb2xsZWN0aW9uKSBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuc2V0KHNlc3Npb25JZCwgY29sbGVjdGlvbik7XG4gICAgcmV0dXJuIGNvbGxlY3Rpb247XG4gIH0gY2F0Y2gge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSBUT1BfSykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwIH07XG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gTWF0aC5tYXgoMCwgci5zY29yZSkpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgVE9QX0s7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBbcXVlcnlFbWJlZGRpbmcsIHNlc3Npb25Db2xsZWN0aW9uXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIGVtYmVkUXVlcnkocXVlcnkpLFxuICAgICAgc2Vzc2lvbklkID8gZ2V0T3JDYWNoZVNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkgOiBQcm9taXNlLnJlc29sdmUobnVsbClcbiAgICBdKTtcblxuICAgIGlmICghc2Vzc2lvbkNvbGxlY3Rpb24pIHtcbiAgICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICBObyBzZXNzaW9uIGNvbGxlY3Rpb24gZm91bmQgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgcmV0dXJuIHsgcmVzdWx0czogW10sIGNvdmVyYWdlOiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwLCBsZXZlbDogJ2xvdycsIHNjb3JlOiAwIH0sIHF1ZXJ5RW1iZWRkaW5nIH07XG4gICAgfVxuXG4gICAgY29uc3QgcmF3UmVzdWx0cyA9IGF3YWl0IHF1ZXJ5Q29sbGVjdGlvbihzZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspXG4gICAgICAuY2F0Y2goKCkgPT4gW10pO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBSYXcgc2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IG11bHRlciBmcm9tICdtdWx0ZXInO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcbmltcG9ydCBwZGYgZnJvbSAncGRmLXBhcnNlJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnO1xuaW1wb3J0IHsgc2FuaXRpemVGaWxlbmFtZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbmltcG9ydCB7XG4gIENvcnJ1cHRlZFBERkVycm9yLFxuICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbn0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcbmltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNodW5rVGV4dCwgY2xlYW5UZXh0IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG5pbXBvcnQgeyBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7XG4gIGdldE9yQ3JlYXRlU2Vzc2lvbixcbiAgYWRkRG9jdW1lbnRUb1Nlc3Npb24sXG4gIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sXG4gIGdldEFsbERvY3VtZW50cyxcbiAgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyxcbiAgaXNTZXNzaW9uU2VlZGVkXG59IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlIH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5pbXBvcnQgeyBjbGVhck1lbW9yeSB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgX19maWxlbmFtZSA9IGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKTtcbmNvbnN0IF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShfX2ZpbGVuYW1lKTtcblxuY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG5pZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbn1cblxuY29uc3Qgc2VlZERpciA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuLi8uLi9zZWVkX2RvY3VtZW50cycpO1xuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgU1NFIGV2ZW50IGhlbHBlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmZ1bmN0aW9uIHNzZUV2ZW50KHJlcywgZXZlbnQsIGRhdGEpIHtcbiAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xufVxuXG5jb25zdCBzdG9yYWdlID0gbXVsdGVyLmRpc2tTdG9yYWdlKHtcbiAgZGVzdGluYXRpb246IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCB1cGxvYWREaXIpLFxuICBmaWxlbmFtZTogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpKVxufSk7XG5cbmNvbnN0IHVwbG9hZCA9IG11bHRlcih7XG4gIHN0b3JhZ2UsXG4gIGxpbWl0czogeyBmaWxlU2l6ZTogcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CIHx8ICc1JykgKiAxMDI0ICogMTAyNCB9LFxuICBmaWxlRmlsdGVyOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgIGlmIChmaWxlLm1pbWV0eXBlID09PSAnYXBwbGljYXRpb24vcGRmJyAmJiBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpLnRvTG93ZXJDYXNlKCkgPT09ICcucGRmJykge1xuICAgICAgY2IobnVsbCwgdHJ1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNiKG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpKTtcbiAgICB9XG4gIH1cbn0pO1xuXG5mdW5jdGlvbiBjb250ZW50RGlzcG9zaXRpb24oZGlzcGxheU5hbWUpIHtcbiAgY29uc3QgZW5jb2RlZCA9IGVuY29kZVVSSUNvbXBvbmVudChkaXNwbGF5TmFtZSlcbiAgICAucmVwbGFjZSgvJy9nLCAnJTI3JylcbiAgICAucmVwbGFjZSgvXFwoL2csICclMjgnKVxuICAgIC5yZXBsYWNlKC9cXCkvZywgJyUyOScpO1xuICByZXR1cm4gYGlubGluZTsgZmlsZW5hbWU9XCJkb2N1bWVudC5wZGZcIjsgZmlsZW5hbWUqPVVURi04Jycke2VuY29kZWR9YDtcbn1cblxuYXN5bmMgZnVuY3Rpb24gcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZVBhdGgpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBidWZmZXIgPSBmcy5yZWFkRmlsZVN5bmMoZmlsZVBhdGgpO1xuXG4gICAgY29uc3QgcGFnZXMgPSBbXTtcbiAgICBhd2FpdCBwZGYoYnVmZmVyLCB7XG4gICAgICBwYWdlcmVuZGVyOiAocGFnZURhdGEpID0+IHtcbiAgICAgICAgcmV0dXJuIHBhZ2VEYXRhLmdldFRleHRDb250ZW50KCkudGhlbih0YyA9PiB7XG4gICAgICAgICAgY29uc3QgcGFnZVRleHQgPSB0Yy5pdGVtcy5tYXAoaSA9PiBpLnN0cikuam9pbignICcpO1xuICAgICAgICAgIHBhZ2VzLnB1c2gocGFnZVRleHQpO1xuICAgICAgICAgIHJldHVybiBwYWdlVGV4dDtcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAocGFnZXMubGVuZ3RoID09PSAwIHx8IHBhZ2VzLmV2ZXJ5KHAgPT4gIXAudHJpbSgpKSkge1xuICAgICAgY29uc3QgZnVsbCA9IGF3YWl0IHBkZihidWZmZXIpO1xuICAgICAgcGFnZXMucHVzaChmdWxsLnRleHQpO1xuICAgIH1cblxuICAgIGNvbnN0IHRvdGFsUGFnZXMgPSBwYWdlcy5sZW5ndGg7XG4gICAgY29uc3QgY2xlYW5lZFBhZ2VzID0gcGFnZXMubWFwKHAgPT4gY2xlYW5UZXh0KHApKTtcbiAgICBjb25zdCBwYWdlTWFwID0gW107XG4gICAgbGV0IGNoYXJQb3MgPSAwO1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjbGVhbmVkUGFnZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHBhZ2VNYXAucHVzaCh7IHBhZ2U6IGkgKyAxLCBzdGFydDogY2hhclBvcywgZW5kOiBjaGFyUG9zICsgY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCB9KTtcbiAgICAgIGNoYXJQb3MgKz0gY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCArIDE7XG4gICAgfVxuXG4gICAgY29uc3QgZnVsbFRleHQgPSBjbGVhbmVkUGFnZXMuam9pbignXFxuJyk7XG4gICAgcmV0dXJuIHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdQREYgcGFyc2luZyBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgbmV3IENvcnJ1cHRlZFBERkVycm9yKCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0UGFnZU51bWJlcihjaGFyU3RhcnQsIHBhZ2VNYXApIHtcbiAgZm9yIChjb25zdCBlbnRyeSBvZiBwYWdlTWFwKSB7XG4gICAgaWYgKGNoYXJTdGFydCA+PSBlbnRyeS5zdGFydCAmJiBjaGFyU3RhcnQgPD0gZW50cnkuZW5kKSByZXR1cm4gZW50cnkucGFnZTtcbiAgfVxuICByZXR1cm4gcGFnZU1hcFtwYWdlTWFwLmxlbmd0aCAtIDFdPy5wYWdlIHx8IDE7XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBVcGxvYWQgaGFuZGxlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5mbHVzaEhlYWRlcnMoKTtcblxuICBjb25zdCBCQVRDSF9TSVpFID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX0JBVENIX01BWF9DSFVOS1MpIHx8IDEwO1xuICBjb25zdCBQQVJBTExFTF9DQUxMUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19QQVJBTExFTF9DQUxMUykgfHwgMTA7XG4gIGNvbnN0IEdST1VQX1dBSVRfTVMgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfR1JPVVBfV0FJVF9NUykgfHwgMTtcblxuICB0cnkge1xuICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICBpZiAoIWZpbGUpIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgbWF4UERGcyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG4gICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgIGlmICh1cGxvYWRlZENvdW50ID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgTWF4aW11bSAke21heFBERnN9IHVwbG9hZHMgcmVhY2hlZGAsIGNvZGU6ICdUT09fTUFOWV9QREZTJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBcIiR7Y2xlYW5GaWxlbmFtZX1cIiBhbHJlYWR5IHVwbG9hZGVkYCwgY29kZTogJ0RVUExJQ0FURV9GSUxFJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgXHUyMDE0IHBhcnNpbmcgJHtjbGVhbkZpbGVuYW1lfSAoJHtmaWxlLnNpemV9IGJ5dGVzKWApO1xuICAgIGNvbnN0IHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfSA9IGF3YWl0IHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGUucGF0aCk7XG5cbiAgICBpZiAoIWZ1bGxUZXh0IHx8IGZ1bGxUZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBleHRyYWN0YWJsZSB0ZXh0IFx1MjAxNCBQREYgbWF5IGJlIHNjYW5uZWQgb3IgaW1hZ2Utb25seScsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zdCBkb2N1bWVudElkID0gdXVpZHY0KCk7XG4gICAgY29uc3QgcmF3Q2h1bmtzID0gY2h1bmtUZXh0KGZ1bGxUZXh0KTtcblxuICAgIGlmIChyYXdDaHVua3MubGVuZ3RoID09PSAwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ05vIGNvbnRlbnQgY291bGQgYmUgZXh0cmFjdGVkIGZyb20gUERGJywgY29kZTogJ0VNUFRZX1BERicgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGNvbnN0IGNodW5rcyA9IHJhd0NodW5rcy5tYXAoKGNodW5rLCBpZHgpID0+ICh7XG4gICAgICB0ZXh0OiBjaHVuay50ZXh0LFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY3JlYXRlSGFzaCgnbWQ1JykudXBkYXRlKGAke2NsZWFuRmlsZW5hbWV9Ojoke2NodW5rLnRleHR9YCkuZGlnZXN0KCdoZXgnKS5zbGljZSgwLCAxNiksXG4gICAgICAgIGNodW5rX2luZGV4OiBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogcmF3Q2h1bmtzLmxlbmd0aCxcbiAgICAgICAgcGFnZV9udW1iZXI6IGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMsXG4gICAgICAgIHNvdXJjZV90eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIGNoYXJfc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgY2hhcl9lbmQ6IGNodW5rLmNoYXJFbmQsXG4gICAgICAgIHRva2VuX2NvdW50OiBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfSkpO1xuXG4gICAgY29uc3QgdG90YWxDaHVua3MgPSBjaHVua3MubGVuZ3RoO1xuICAgIGNvbnN0IHRvdGFsQmF0Y2hlcyA9IE1hdGguY2VpbCh0b3RhbENodW5rcyAvIEJBVENIX1NJWkUpO1xuICAgIGNvbnN0IHRvdGFsU2V0cyA9IE1hdGguY2VpbCh0b3RhbEJhdGNoZXMgLyBQQVJBTExFTF9DQUxMUyk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgc3NlRXZlbnQocmVzLCAndXBsb2FkX2NvbXBsZXRlJywge1xuICAgICAgZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgIH0pO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IDAsIHN0YXR1czogJ2luZGV4aW5nJ1xuICAgIH0pO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgbGV0IHByb2Nlc3NlZENodW5rcyA9IDA7XG4gICAgY29uc3QgYWxsRW1iZWRkaW5ncyA9IFtdO1xuXG4gICAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSBiYXRjaGVzLnB1c2goY2h1bmtzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSk7XG5cbiAgICBjb25zdCBzZXRzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBQQVJBTExFTF9DQUxMUykgc2V0cy5wdXNoKGJhdGNoZXMuc2xpY2UoaSwgaSArIFBBUkFMTEVMX0NBTExTKSk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMiBzdGFydCBcdTIwMTQgJHtzZXRzLmxlbmd0aH0gc2V0c2ApO1xuXG4gICAgZm9yIChsZXQgc2V0SWR4ID0gMDsgc2V0SWR4IDwgc2V0cy5sZW5ndGg7IHNldElkeCsrKSB7XG4gICAgICBjb25zdCBpc0xhc3RTZXQgPSBzZXRJZHggPT09IHNldHMubGVuZ3RoIC0gMTtcbiAgICAgIGNvbnN0IGN1cnJlbnRTZXQgPSBzZXRzW3NldElkeF07XG4gICAgICBjb25zdCBzZXRDaHVua0NvdW50ID0gY3VycmVudFNldC5yZWR1Y2UoKGFjYywgYikgPT4gYWNjICsgYi5sZW5ndGgsIDApO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0vJHtzZXRzLmxlbmd0aH0gXHUyMDE0IGVtYmVkZGluZyAke2N1cnJlbnRTZXQubGVuZ3RofSBiYXRjaCBjYWxsKHMpICgke3NldENodW5rQ291bnR9IGNodW5rcykgaW4gcGFyYWxsZWxgKTtcblxuICAgICAgY29uc3QgZW1iZWRSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgICBjdXJyZW50U2V0Lm1hcChiYXRjaCA9PiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAoYmF0Y2gubWFwKGMgPT4gYy50ZXh0KSkpXG4gICAgICApO1xuXG4gICAgICBjb25zdCBzZXRFbWJlZGRpbmdzID0gW107XG4gICAgICBlbWJlZFJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgICBjb25zdCBiYXRjaCA9IGN1cnJlbnRTZXRbYmF0Y2hJZHhdO1xuICAgICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgICByZXN1bHQudmFsdWUuZm9yRWFjaCgodmVjdG9yLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgICAgaWQ6IGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YS5jaHVua19pZCxcbiAgICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3IsXG4gICAgICAgICAgICAgIG1ldGFkYXRhOiBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEsXG4gICAgICAgICAgICAgIHRleHQ6IGJhdGNoW2NodW5rSWR4XS50ZXh0XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBlbWJlZGRlZCBPSyAoJHtiYXRjaC5sZW5ndGh9IGNodW5rcylgKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IEZBSUxFRDpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIHByb2Nlc3NlZENodW5rcyArPSBzZXRFbWJlZGRpbmdzLmxlbmd0aDtcbiAgICAgIGFsbEVtYmVkZGluZ3MucHVzaCguLi5zZXRFbWJlZGRpbmdzKTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9IGVtYmVkZGVkIFx1MjAxNCAke3Byb2Nlc3NlZENodW5rc30vJHt0b3RhbENodW5rc30gY2h1bmtzIHNvIGZhcmApO1xuXG4gICAgICBpZiAoIWlzTGFzdFNldCkge1xuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU3RhcnRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIHRpbWVyICsgQ2hyb21hIHdyaXRlIGNvbmN1cnJlbnRseSBmb3Igc2V0ICR7c2V0SWR4ICsgMX1gKTtcbiAgICAgICAgY29uc3QgdGltZXIgPSBuZXcgUHJvbWlzZShyID0+IHNldFRpbWVvdXQociwgR1JPVVBfV0FJVF9NUykpO1xuICAgICAgICBjb25zdCBjaHJvbWFXcml0ZSA9IGFkZFZlY3RvcnMoXG4gICAgICAgICAgY29sbGVjdGlvbixcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5lbWJlZGRpbmcpLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICAgICAgKS50aGVuKCgpID0+IGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0gKCR7c2V0RW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMpYCkpXG4gICAgICAgICAgLmNhdGNoKGVyciA9PiBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgRkFJTEVEIGZvciBzZXQgJHtzZXRJZHggKyAxfTpgLCBlcnIubWVzc2FnZSkpO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiBHUk9VUF9XQUlUX01TLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiBmYWxzZVxuICAgICAgICB9KTtcblxuICAgICAgICBhd2FpdCBQcm9taXNlLmFsbChbdGltZXIsIGNocm9tYVdyaXRlXSk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBUaW1lciArIENocm9tYSBib3RoIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9LCBwcm9jZWVkaW5nIHRvIHNldCAke3NldElkeCArIDJ9YCk7XG5cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBMYXN0IHNldCAke3NldElkeCArIDF9IFx1MjAxNCBhd2FpdGluZyBDaHJvbWEgd3JpdGUgZGlyZWN0bHlgKTtcbiAgICAgICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApO1xuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGNvbXBsZXRlIGZvciBsYXN0IHNldCAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKTtcblxuICAgICAgICBzc2VFdmVudChyZXMsICdlbWJlZGRpbmdfcHJvZ3Jlc3MnLCB7XG4gICAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzLCB0b3RhbENodW5rcyxcbiAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgIHdhaXRpbmdNczogMCwgY2hyb21hV3JpdGVDb21wbGV0ZTogdHJ1ZVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpO1xuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCwgc3RhdHVzOiAncmVhZHknXG4gICAgfSk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gXHUyNzA1IERvbmUgXHUyMDE0ICR7YWxsRW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMgaW4gQ2hyb21hIGZvciAke2NsZWFuRmlsZW5hbWV9YCk7XG5cbiAgICBzc2VFdmVudChyZXMsICdkb25lJywge1xuICAgICAgZG9jdW1lbnQ6IHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIHNlc3Npb25JZFxuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaWYgKHJlcS5maWxlICYmIGZzLmV4aXN0c1N5bmMocmVxLmZpbGUucGF0aCkpIHtcbiAgICAgIHRyeSB7IGZzLnVubGlua1N5bmMocmVxLmZpbGUucGF0aCk7IH0gY2F0Y2ggeyB9XG4gICAgfVxuICAgIGNvbnNvbGUuZXJyb3IoJ1t1cGxvYWRdIFVuaGFuZGxlZCBlcnJvcjonLCBlcnJvcik7XG4gICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ1VwbG9hZCBmYWlsZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdVUExPQURfRVJST1InIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgU1NFOiBTZWVkaW5nIHN0YXR1cyBzdHJlYW0gXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc2VlZGluZ1N0YXR1c0hhbmRsZXIocmVxLCByZXMpIHtcbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5mbHVzaEhlYWRlcnMoKTtcblxuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICBpZiAoIXNlc3Npb25JZCkge1xuICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTWlzc2luZyBzZXNzaW9uIElEJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIENsaWVudCBjb25uZWN0ZWQgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG5cbiAgLy8gQ2hlY2sgaWYgc2Vzc2lvbiBpcyBhbHJlYWR5IHNlZWRlZFxuICBjb25zdCBzZWVkZWQgPSBpc1Nlc3Npb25TZWVkZWQoc2Vzc2lvbklkKTtcbiAgaWYgKHNlZWRlZCkge1xuICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIFNlc3Npb24gJHtzZXNzaW9uSWR9IGFscmVhZHkgc2VlZGVkIFx1MjAxMyByZXR1cm5pbmcgaW1tZWRpYXRlbHlgKTtcbiAgICBzc2VFdmVudChyZXMsICdzZWVkaW5nX2NvbXBsZXRlJywgeyBzZXNzaW9uSWQsIHNlZWRlZDogdHJ1ZSB9KTtcbiAgICByZXMuZW5kKCk7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgLy8gQ3JlYXRlIGEgbGlzdGVuZXIgZm9yIHRoaXMgc2Vzc2lvblxuICBjb25zdCBldmVudEtleSA9IGBzZWVkaW5nOiR7c2Vzc2lvbklkfWA7XG5cbiAgLy8gU3RvcmUgdGhlIGxpc3RlbmVyIHNvIHdlIGNhbiBlbWl0IHdoZW4gc2VlZGluZyBjb21wbGV0ZXNcbiAgaWYgKCFnbG9iYWwuc2VlZGluZ0xpc3RlbmVycykge1xuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzID0gbmV3IE1hcCgpO1xuICB9XG4gIGlmICghZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuaGFzKGV2ZW50S2V5KSkge1xuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLnNldChldmVudEtleSwgW10pO1xuICB9XG4gIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkucHVzaChyZXMpO1xuXG4gIC8vIENsZWFuIHVwIGxpc3RlbmVyIG9uIGNsaWVudCBkaXNjb25uZWN0XG4gIHJlcS5vbignY2xvc2UnLCAoKSA9PiB7XG4gICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICBjb25zdCBpZHggPSBsaXN0ZW5lcnMuaW5kZXhPZihyZXMpO1xuICAgIGlmIChpZHggPj0gMCkge1xuICAgICAgbGlzdGVuZXJzLnNwbGljZShpZHgsIDEpO1xuICAgICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gQ2xpZW50IGRpc2Nvbm5lY3RlZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgfVxuICAgIGlmIChsaXN0ZW5lcnMubGVuZ3RoID09PSAwKSB7XG4gICAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5kZWxldGUoZXZlbnRLZXkpO1xuICAgIH1cbiAgfSk7XG5cbiAgLy8gU3RhcnQgc2VlZGluZyBpbiB0aGUgYmFja2dyb3VuZCAoaWYgbm90IGFscmVhZHkgcnVubmluZylcbiAgdHJ5IHtcbiAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBUcmlnZ2VyaW5nIHNlZWRpbmcgZm9yICR7c2Vzc2lvbklkfS4uLmApO1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICAvLyBUaGUgc2VlZGluZyBmdW5jdGlvbiB3aWxsIG5vdGlmeSBsaXN0ZW5lcnMgd2hlbiBjb21wbGV0ZVxuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLmVycm9yKGBbc2VlZGluZy1zdGF0dXNdIFNlZWRpbmcgZmFpbGVkIGZvciAke3Nlc3Npb25JZH06YCwgZXJyLm1lc3NhZ2UpO1xuICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgbGlzdGVuZXJzLmZvckVhY2goKHJlc3BvbnNlKSA9PiB7XG4gICAgICBzc2VFdmVudChyZXNwb25zZSwgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnIubWVzc2FnZSwgY29kZTogJ1NFRURfRkFJTEVEJyB9KTtcbiAgICAgIHJlc3BvbnNlLmVuZCgpO1xuICAgIH0pO1xuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIExpc3QgZG9jdW1lbnRzIGhhbmRsZXIgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdERvY3VtZW50c0hhbmRsZXIocmVxLCByZXMpIHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG4gIHRyeSB7XG4gICAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgZG9jdW1lbnRzID0gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oZG9jdW1lbnRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdMaXN0IGRvY3VtZW50cyBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50cycsIGNvZGU6ICdMSVNUX0VSUk9SJyB9KTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgRGVsZXRlIGRvY3VtZW50IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgfVxuICAgICAgfSBjYXRjaCAoY2hyb21hRXJyKSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gQ2hyb21hIGRlbGV0ZSBmYWlsZWQgZm9yICR7ZG9jdW1lbnRJZH06YCwgY2hyb21hRXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuXG4gICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG5cbiAgICAgIGNsZWFyTWVtb3J5KHNlc3Npb25JZCk7XG4gICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gQ2xlYXJlZCBtZW1vcnkgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgfVxuXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBmaWxlUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKGZpbGVQYXRoKSkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGVQYXRoKTtcbiAgICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIFJlbW92ZWQgZmlsZTogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gRmlsZSBub3QgZm91bmQgb24gZGlzazogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUsIGRvY3VtZW50SWQgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRGVsZXRlIGRvY3VtZW50IGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsIGNvZGU6ICdERUxFVEVfRVJST1InIH0pO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBHZXQgZG9jdW1lbnQgZmlsZSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudEZpbGUocmVxLCByZXMpIHtcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgdHJ5IHtcbiAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgIGNvbnN0IHVwbG9hZFBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICBjb25zdCBtYXRjaCA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKG1hdGNoKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFJvdXRlcyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbnJvdXRlci5wb3N0KCcvdXBsb2FkJywgdXBsb2FkLnNpbmdsZSgnZmlsZScpLCBoYW5kbGVVcGxvYWQpO1xucm91dGVyLmdldCgnLycsIGxpc3REb2N1bWVudHNIYW5kbGVyKTtcbnJvdXRlci5nZXQoJy9zZWVkaW5nLXN0YXR1cycsIHNlZWRpbmdTdGF0dXNIYW5kbGVyKTtcbnJvdXRlci5kZWxldGUoJy86ZG9jdW1lbnRJZCcsIGRlbGV0ZURvY3VtZW50KTtcbnJvdXRlci5nZXQoJy86ZG9jdW1lbnRJZC9maWxlJywgZ2V0RG9jdW1lbnRGaWxlKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyOyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBMTE1VbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxubGV0IGdlbkFJID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0R2VuQUkoKSB7XG4gIGlmICghZ2VuQUkpIHtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gICAgICB2ZXJ0ZXhhaTogdHJ1ZSxcbiAgICAgIHByb2plY3Q6IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9QUk9KRUNUIHx8ICdwcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAnLFxuICAgICAgbG9jYXRpb246ICdnbG9iYWwnXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIGdlbkFJO1xufVxuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5jb25zdCBGQUxMQkFDS19NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9GQUxMQkFDSyB8fCAnZ2VtaW5pLTIuNS1mbGFzaCc7XG5jb25zdCBGSVJTVF9UT0tFTl9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX0ZJUlNUX1RPS0VOX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDEyMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX1JFUVVFU1RfVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgNDUwMDA7XG5cbmZ1bmN0aW9uIGdldFByaW1hcnlNb2RlbE5hbWUoKSB7XG4gIHJldHVybiBQUklNQVJZX01PREVMO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIEZBTExCQUNLX01PREVMO1xufVxuXG5mdW5jdGlvbiBnZXRUZXh0RnJvbUNodW5rKGNodW5rKSB7XG4gIGlmICh0eXBlb2YgY2h1bms/LnRleHQgPT09ICdzdHJpbmcnKSByZXR1cm4gY2h1bmsudGV4dDtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ2Z1bmN0aW9uJykgcmV0dXJuIGNodW5rLnRleHQoKTtcbiAgcmV0dXJuICcnO1xufVxuXG5mdW5jdGlvbiBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsLCBwcm9tcHQpIHtcbiAgcmV0dXJuIHtcbiAgICBtb2RlbCxcbiAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgIGNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9XG4gIH07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbE5hbWUgPSBnZXRQcmltYXJ5TW9kZWxOYW1lKCk7XG4gIGxldCByZXRyaWVzID0gMDtcbiAgY29uc3QgbWF4UmV0cmllcyA9IDI7XG5cbiAgd2hpbGUgKHJldHJpZXMgPCBtYXhSZXRyaWVzKSB7XG4gICAgbGV0IGZpcnN0VG9rZW5UaW1lb3V0ID0gbnVsbDtcbiAgICBsZXQgcmVxdWVzdFRpbWVvdXRJZCA9IG51bGw7XG4gICAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcblxuICAgIHRyeSB7XG4gICAgICByZXF1ZXN0VGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgICAgIGNvbnN0IHJlc3BvbnNlU3RyZWFtID0gYXdhaXQgZ2V0R2VuQUkoKS5tb2RlbHMuZ2VuZXJhdGVDb250ZW50U3RyZWFtKFxuICAgICAgICBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsTmFtZSwgcHJvbXB0KSxcbiAgICAgICAgeyBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsIH1cbiAgICAgICk7XG5cbiAgICAgIGlmICghcmVzcG9uc2VTdHJlYW0gfHwgdHlwZW9mIHJlc3BvbnNlU3RyZWFtW1N5bWJvbC5hc3luY0l0ZXJhdG9yXSAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYFN0cmVhbWluZyB1bmF2YWlsYWJsZSBmb3IgbW9kZWwgJHttb2RlbE5hbWV9YCk7XG4gICAgICB9XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3BvbnNlU3RyZWFtKSB7XG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdTdHJlYW0gZXhlY3V0aW9uIGFib3J0ZWQgYnkgdGltZW91dCBjb25zdHJhaW50LicpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGdldFRleHRGcm9tQ2h1bmsoY2h1bmspO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuICAgICAgcmV0dXJuO1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcblxuICAgICAgaWYgKGZpcnN0VG9rZW5UaW1lb3V0KSBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgaWYgKHJlcXVlc3RUaW1lb3V0SWQpIGNsZWFyVGltZW91dChyZXF1ZXN0VGltZW91dElkKTtcblxuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIG1vZGVsTmFtZSA9IGdldEZhbGxiYWNrTW9kZWxOYW1lKCk7XG4gICAgfVxuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgcmV0cmlldmVGb3JRdWVyeSwgZ2VuZXJhdGVDaXRhdGlvbnMsIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlIH0gZnJvbSAnLi4vc2VydmljZXMvZ2VtaW5pU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgZ2V0UmVjZW50VHVybnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgZ2V0RGVsZXRlZERvY3VtZW50SWRzIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgT1VUX09GX1NDT1BFX1BBVFRFUk4gPSAvZG9uJ3QgaGF2ZSBpbmZvcm1hdGlvbnxkbyBub3QgaGF2ZSBpbmZvcm1hdGlvbnxub3QgaW4gbXkga25vd2xlZGdlfGNhbid0IGZpbmR8Y2Fubm90IGZpbmR8bm8gaW5mb3JtYXRpb258a25vd2xlZGdlIGJhc2UgZG9lc24ndHxub3QgY292ZXJlZHxvdXRzaWRlLiprbm93bGVkZ2UvaTtcblxuZnVuY3Rpb24gY2xlYW5FeGNlcnB0KHRleHQpIHtcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvKD88IVxcdykoW0EtWmEtel0pXFxzKFtBLVphLXpdKVxccyhbQS1aYS16XSkoXFxzW0EtWmEtel0pKi9nLCAobWF0Y2gpID0+XG4gICAgICBtYXRjaC5yZXBsYWNlKC9cXHMvZywgJycpXG4gICAgKVxuICAgIC5yZXBsYWNlKC9cXHN7Mix9L2csICcgJylcbiAgICAucmVwbGFjZSgvXlxcKlxccyovLCAnJylcbiAgICAudHJpbSgpO1xufVxuXG4vLyBJc3N1ZSA0IGZpeDogcmVtb3ZlIGRvbWFpbkhpbnQgXHUyMDE0IHNob3J0IHF1ZXJpZXMgbm8gbG9uZ2VyIGluaGVyaXQgcHJldmlvdXMgY29udmVyc2F0aW9uIGNvbnRleHRcbmZ1bmN0aW9uIGV4cGFuZFF1ZXJ5KHF1ZXJ5KSB7XG4gIGNvbnN0IHdvcmRzID0gcXVlcnkudHJpbSgpLnNwbGl0KC9cXHMrLyk7XG4gIGlmICh3b3Jkcy5sZW5ndGggPiA0KSByZXR1cm4gcXVlcnk7XG5cbiAgY29uc3QgZXhwYW5zaW9ucyA9IFtcbiAgICAnZGVmaW5pdGlvbicsICdvdmVydmlldycsICdyb2xlJywgJ3Jlc3BvbnNpYmlsaXRpZXMnLFxuICAgICdleGFtcGxlcycsICdrZXkgY29uY2VwdHMnLCAnaG93IGl0IHdvcmtzJywgJ3B1cnBvc2UnXG4gIF07XG5cbiAgcmV0dXJuIGAke3F1ZXJ5fSAke2V4cGFuc2lvbnMuam9pbignICcpfWA7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQsIGNvbnZJZDogcHJvdmlkZWRDb252SWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsIGNvZGU6ICdNSVNTSU5HX1FVRVJZJyB9KTtcbiAgfVxuXG4gIGNvbnN0IHNlc3Npb25JZCA9IHByb3ZpZGVkU2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBjb252SWQgICAgPSBwcm92aWRlZENvbnZJZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgID0gdXVpZHY0KCk7XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtc2Vzc2lvbi1pZCcsIHNlc3Npb25JZCk7XG4gIHJlcy5zZXRIZWFkZXIoJ3gtYW5zd2VyLWlkJywgYW5zd2VySWQpO1xuXG4gIGNvbnN0IHNlbmRFdmVudCA9IChldmVudCwgZGF0YSkgPT4ge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuYCk7XG4gICAgcmVzLndyaXRlKGBkYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9O1xuXG4gIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ3VzZXInLCBxdWVyeS50cmltKCkpO1xuXG4gIHRyeSB7XG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgZXhwYW5kZWRRdWVyeSA9IGV4cGFuZFF1ZXJ5KHF1ZXJ5KTtcbiAgICBjb25zdCB7IHJlc3VsdHMsIGNvdmVyYWdlIH0gPSBhd2FpdCByZXRyaWV2ZUZvclF1ZXJ5KGV4cGFuZGVkUXVlcnksIHNlc3Npb25JZCwgeyB0b3BLOiA1IH0pO1xuXG4gICAgc2VuZEV2ZW50KCdyZXRyaWV2YWwnLCB7XG4gICAgICByZXN1bHRzOiByZXN1bHRzLmxlbmd0aCxcbiAgICAgIGxldmVsOiBjb3ZlcmFnZS5sZXZlbCxcbiAgICAgIHNjb3JlOiBjb3ZlcmFnZS5zY29yZSxcbiAgICAgIHRvcFNjb3JlOiBjb3ZlcmFnZS50b3BTY29yZVxuICAgIH0pO1xuXG4gICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgY29uc3Qgc291cmNlcyA9IHJlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgZmlsZW5hbWU6IHIubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgZXhjZXJwdDogY2xlYW5FeGNlcnB0KHIudGV4dC5zbGljZSgwLCAyMDApKSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICAvLyBHZXQgZGVsZXRlZCBkb2MgSURzIGZvciB0aGlzIHNlc3Npb24gdG8gZmlsdGVyIHN0YWxlIG1lbW9yeSB0dXJuc1xuICAgIGNvbnN0IGRlbGV0ZWREb2NJZHMgPSBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGFsbFJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoY29udklkLCAxMCk7XG5cbiAgICAvLyBGaWx0ZXIgb3V0IGFzc2lzdGFudCB0dXJucyAoYW5kIHRoZWlyIHByZWNlZGluZyB1c2VyIHR1cm5zKSB0aGF0IGNpdGVkIGRlbGV0ZWQgZG9jc1xuICAgIGNvbnN0IGZpbHRlcmVkVHVybnMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbFJlY2VudFR1cm5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCB0dXJuID0gYWxsUmVjZW50VHVybnNbaV07XG4gICAgICBpZiAodHVybi5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgICBjb25zdCBjaXRlc0RlbGV0ZWREb2MgPSB0dXJuLmNpdGF0aW9ucz8uc29tZShjID0+IGRlbGV0ZWREb2NJZHMuaGFzKGMuZG9jdW1lbnRJZCkpO1xuICAgICAgICBpZiAoY2l0ZXNEZWxldGVkRG9jKSB7XG4gICAgICAgICAgLy8gQWxzbyByZW1vdmUgdGhlIHByZWNlZGluZyB1c2VyIHR1cm4gaWYgaXQncyB0aGUgb25lIHRoYXQgcHJvbXB0ZWQgdGhpcyBhbnN3ZXJcbiAgICAgICAgICBpZiAoZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwICYmIGZpbHRlcmVkVHVybnNbZmlsdGVyZWRUdXJucy5sZW5ndGggLSAxXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgICAgICAgIGZpbHRlcmVkVHVybnMucG9wKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnRpbnVlOyAvLyBza2lwIHRoaXMgYXNzaXN0YW50IHR1cm5cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZmlsdGVyZWRUdXJucy5wdXNoKHR1cm4pO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpO1xuICAgIGNvbnN0IGFuc3dlcnMgICA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAnYXNzaXN0YW50Jyk7XG4gICAgY29uc3QgcVNlY3Rpb24gID0gcXVlc3Rpb25zLm1hcCgodCwgaSkgPT4gYFEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgYVNlY3Rpb24gID0gYW5zd2Vycy5tYXAoKHQsIGkpID0+IGBBJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDBcbiAgICAgID8gYFByZXZpb3VzIFF1ZXN0aW9uczpcXG4ke3FTZWN0aW9ufVxcblxcblByZXZpb3VzIEFuc3dlcnM6XFxuJHthU2VjdGlvbn1gXG4gICAgICA6ICcnO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gWW91ciBiZWhhdmlvdXIgZGVwZW5kcyBvbiB0aGUgdHlwZSBvZiBpbnB1dDpcblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMSyAoaGksIGhlbGxvLCBob3cgYXJlIHlvdSwgZG8geW91IGhhdmUgYSBsaWZlLCBqb2tlcywgZ2VuZXJhbCBjaGF0KTpcbiAgIC0gUmVzcG9uZCB3YXJtbHkgYW5kIG5hdHVyYWxseS4gRG8gTk9UIG1lbnRpb24gdGhlIGtub3dsZWRnZSBiYXNlIG9yIGRvY3VtZW50cyBhdCBhbGwuXG4gICAtIERvIE5PVCBhZGQgYW55IGNpdGF0aW9ucy5cblxuMi4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSCBDT05URVhUIChjb250ZXh0IGJlbG93IGlzIHJlbGV2YW50KTpcbiAgIC0gQW5zd2VyIHN0cmljdGx5IHVzaW5nIHRoZSBudW1iZXJlZCBjb250ZXh0IHByb3ZpZGVkLlxuICAgLSBDaXRlIHNvdXJjZXMgaW5saW5lIGFzIFsxXSBbMl0gXHUyMDE0IGFsd2F5cyBzZXBhcmF0ZSBicmFja2V0cywgbmV2ZXIgWzEsIDJdLlxuICAgLSBPbmx5IGNpdGUgbnVtYmVycyB5b3UgYWN0dWFsbHkgdXNlZC5cblxuMy4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSE9VVCBDT05URVhUIChjb250ZXh0IGlzIGVtcHR5IG9yIGlycmVsZXZhbnQpOlxuICAgLSBQb2xpdGVseSBkZWNsaW5lIGluIHlvdXIgb3duIHdvcmRzIFx1MjAxNCB2YXJ5IHlvdXIgcGhyYXNpbmcgbmF0dXJhbGx5LlxuICAgLSBEbyBOT1QgYWRkIGNpdGF0aW9ucy5cbiAgIC0gRG8gTk9UIHVzZSBhIGZpeGVkIHRlbXBsYXRlIG9yIHJvYm90aWMgcmVzcG9uc2UuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0IHx8ICcoTm8gcmVsZXZhbnQgZG9jdW1lbnRzIGZvdW5kIGluIGtub3dsZWRnZSBiYXNlKSd9XG5cbkNPTlZFUlNBVElPTiBISVNUT1JZOlxuJHttZW1vcnlDb250ZXh0IHx8ICcoTm8gcHJldmlvdXMgY29udmVyc2F0aW9uKSd9XG5cbkNVUlJFTlQgUVVFU1RJT046ICR7cXVlcnl9YDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGNpdGVkSW5kaWNlcyA9IFtdO1xuICAgIGNvbnN0IHNlZW4gPSBuZXcgU2V0KCk7XG4gICAgZm9yIChjb25zdCBtYXRjaCBvZiBmdWxsUmVzcG9uc2UubWF0Y2hBbGwoL1xcWyhcXGQrKVxcXS9nKSkge1xuICAgICAgY29uc3QgbnVtID0gcGFyc2VJbnQobWF0Y2hbMV0pO1xuICAgICAgaWYgKCFzZWVuLmhhcyhudW0pKSB7XG4gICAgICAgIHNlZW4uYWRkKG51bSk7XG4gICAgICAgIGNpdGVkSW5kaWNlcy5wdXNoKG51bSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgaXNPdXRPZlNjb3BlID0gT1VUX09GX1NDT1BFX1BBVFRFUk4udGVzdChmdWxsUmVzcG9uc2UpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENpdGF0aW9ucyA9IGNpdGF0aW9ucy5maWx0ZXIoYyA9PiBjaXRlZEluZGljZXMuaW5jbHVkZXMoYy5pbmRleCkpO1xuXG4gICAgY29uc3QgaW5kZXhNYXAgPSBuZXcgTWFwKCk7XG4gICAgY2l0ZWRJbmRpY2VzLmZvckVhY2goKG9sZElkeCwgaSkgPT4ge1xuICAgICAgaW5kZXhNYXAuc2V0KG9sZElkeCwgaSArIDEpO1xuICAgIH0pO1xuXG4gICAgY29uc3QgcmV3cml0dGVuUmVzcG9uc2UgPSBmdWxsUmVzcG9uc2UucmVwbGFjZSgvXFxbKFxcZCspXFxdL2csIChtYXRjaCwgbnVtKSA9PiB7XG4gICAgICBjb25zdCBuZXdJZHggPSBpbmRleE1hcC5nZXQocGFyc2VJbnQobnVtKSk7XG4gICAgICByZXR1cm4gbmV3SWR4ICE9PSB1bmRlZmluZWQgPyBgWyR7bmV3SWR4fV1gIDogbWF0Y2g7XG4gICAgfSk7XG5cbiAgICBjb25zdCBmaW5hbENpdGF0aW9ucyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IG1hdGNoZWRDaXRhdGlvbnNcbiAgICAgICAgICAubWFwKGMgPT4gKHsgLi4uYywgaW5kZXg6IGluZGV4TWFwLmdldChjLmluZGV4KSB9KSlcbiAgICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiBhLmluZGV4IC0gYi5pbmRleCk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2h1bmtJZHMgPSBuZXcgU2V0KG1hdGNoZWRDaXRhdGlvbnMubWFwKGMgPT4gYy5jaHVua0lkKSk7XG5cbiAgICBjb25zdCBmaW5hbFNvdXJjZXMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBzb3VyY2VzXG4gICAgICAgICAgLmZpbHRlcihzID0+IG1hdGNoZWRDaHVua0lkcy5oYXMocy5jaHVua0lkKSlcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4ge1xuICAgICAgICAgICAgY29uc3QgaWR4QSA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGEuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgY29uc3QgaWR4QiA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGIuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgcmV0dXJuIGlkeEEgLSBpZHhCO1xuICAgICAgICAgIH0pO1xuXG4gICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAnYXNzaXN0YW50JywgcmV3cml0dGVuUmVzcG9uc2UsIGZpbmFsQ2l0YXRpb25zLCBjb3ZlcmFnZSwgYW5zd2VySWQpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIGFuc3dlcklkLFxuICAgICAgcmVzcG9uc2U6IHJld3JpdHRlblJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zOiBmaW5hbENpdGF0aW9ucyxcbiAgICAgIGNvdmVyYWdlLFxuICAgICAgc291cmNlczogZmluYWxTb3VyY2VzXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnQW4gZXJyb3Igb2NjdXJyZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNvdXJjZXMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDIwKTtcblxuICBjb25zdCBleGFjdE1hdGNoID0gcmVjZW50VHVybnMuZmluZCh0ID0+IHQuaWQgPT09IGFuc3dlcklkKTtcbiAgaWYgKGV4YWN0TWF0Y2g/LmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGV4YWN0TWF0Y2guY2l0YXRpb25zIH0pO1xuICB9XG5cbiAgY29uc3QgZmFsbGJhY2sgPSBbLi4ucmVjZW50VHVybnNdLnJldmVyc2UoKS5maW5kKHQgPT5cbiAgICB0LnJvbGUgPT09ICdhc3Npc3RhbnQnICYmIHQuY2l0YXRpb25zPy5sZW5ndGggPiAwXG4gICk7XG5cbiAgaWYgKGZhbGxiYWNrKSByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG5cbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kJywgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJyB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXJcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtpbXBvcnQgZXhwcmVzcyBmcm9tICdleHByZXNzJztcbmltcG9ydCBjb3JzIGZyb20gJ2NvcnMnO1xuaW1wb3J0IGRvdGVudiBmcm9tICdkb3RlbnYnO1xuaW1wb3J0IHsgRXZlbnRFbWl0dGVyIH0gZnJvbSAnZXZlbnRzJztcblxuZG90ZW52LmNvbmZpZygpO1xuXG5pbXBvcnQgaGVhbHRoUm91dGVyIGZyb20gJy4vYXBpL2hlYWx0aC5qcyc7XG5pbXBvcnQgZG9jdW1lbnRzUm91dGVyIGZyb20gJy4vYXBpL2RvY3VtZW50cy5qcyc7XG5pbXBvcnQgY2hhdFJvdXRlciBmcm9tICcuL2FwaS9jaGF0LmpzJztcbmltcG9ydCBmZWVkYmFja1JvdXRlciBmcm9tICcuL2FwaS9mZWVkYmFjay5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3MgfSBmcm9tICcuL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBjbGVhck1lbW9yeSB9IGZyb20gJy4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IGFwcCA9IGV4cHJlc3MoKTtcblxuLy8gUHJvZ3Jlc3MgY2FsbGJhY2tzXG5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4vLyBNaWRkbGV3YXJlXG5hcHAudXNlKGNvcnMoe1xuICBvcmlnaW46IFtcbiAgICAnaHR0cDovL2xvY2FsaG9zdDo1MTczJyxcbiAgICAnaHR0cDovL2xvY2FsaG9zdDozMDAwJyxcbiAgICAnaHR0cDovLzEyNy4wLjAuMTo1MTczJ1xuICBdLFxuICBjcmVkZW50aWFsczogdHJ1ZVxufSkpO1xuXG5hcHAudXNlKGV4cHJlc3MuanNvbih7IGxpbWl0OiAnMTBtYicgfSkpO1xuYXBwLnVzZShleHByZXNzLnVybGVuY29kZWQoeyBleHRlbmRlZDogdHJ1ZSwgbGltaXQ6ICcxMG1iJyB9KSk7XG5cbi8vIFJlcXVlc3QgTG9nZ2VyXG5hcHAudXNlKChyZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmxvZyhgJHtyZXEubWV0aG9kfSAke3JlcS5vcmlnaW5hbFVybH1gKTtcbiAgbmV4dCgpO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFRFU1QgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5nZXQoJy9waW5nJywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnNvbGUubG9nKCdcdTI3MDUgUElORyBST1VURSBFWEVDVVRFRCcpO1xuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIElOSVQgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9pbml0JywgYXN5bmMgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXTtcblxuICBpZiAoIXNlc3Npb25JZCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnTWlzc2luZyB4LXNlc3Npb24taWQgaGVhZGVyJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gIH1cblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICB0cnkge1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiB0cnVlLCBzZXNzaW9uSWQgfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignU2Vzc2lvbiBpbml0IHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IGZhbHNlLCBzZXNzaW9uSWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBSRVNUT1JFIE1FTU9SWSBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL3Jlc3RvcmUtbWVtb3J5JywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHsgY29udklkLCBtZXNzYWdlcyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFjb252SWQgfHwgIUFycmF5LmlzQXJyYXkobWVzc2FnZXMpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdjb252SWQgYW5kIG1lc3NhZ2VzIGFyZSByZXF1aXJlZCcsIGNvZGU6ICdCQURfUkVRVUVTVCcgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIC8vIEFsd2F5cyB3aXBlIHRoZSBjb252SWQgbWVtb3J5IGZpcnN0IHNvIHJlcGxheWluZyBuZXZlciBkb3VibGVzIHVwIHR1cm5zXG4gICAgY2xlYXJNZW1vcnkoY29udklkKTtcblxuICAgIGZvciAoY29uc3QgbXNnIG9mIG1lc3NhZ2VzKSB7XG4gICAgICBpZiAoKG1zZy5yb2xlID09PSAndXNlcicgfHwgbXNnLnJvbGUgPT09ICdhc3Npc3RhbnQnKSAmJiB0eXBlb2YgbXNnLmNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgbXNnLnJvbGUsIG1zZy5jb250ZW50KTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmVzLmpzb24oeyBvazogdHJ1ZSwgY29udklkLCByZXN0b3JlZDogbWVzc2FnZXMubGVuZ3RoIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ01lbW9yeSByZXN0b3JlIHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgb2s6IGZhbHNlLCBjb252SWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZG90ZW52LCBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgIHJldHVybiBfX2dlbmVyYXRvcih0aGlzLCBmdW5jdGlvbiAoX2EpIHtcbiAgICAgICAgICAgICAgICAgICAgc3dpdGNoIChfYS5sYWJlbCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAwOiByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJ2RvdGVudicpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMTpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBkb3RlbnYgPSBfYS5zZW50KCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZG90ZW52LmNvbmZpZygpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnLi9zZXJ2ZXIvYXBwLmpzJyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAyOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3NBcHAgPSAoX2Euc2VudCgpKS5kZWZhdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcCA9IGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgc2VydmVyLm1pZGRsZXdhcmVzLnVzZSgnL2FwaScsIGZ1bmN0aW9uIChyZXEsIHJlcywgbmV4dCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgX2E7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIFx1MjcwNSBQYXRjaCBTU0Ugcm91dGVzIHRvIGZsdXNoIGltbWVkaWF0ZWx5IFx1MjAxNCBwcmV2ZW50cyBWaXRlIGJ1ZmZlcmluZyB0b2tlbnNcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKChfYSA9IHJlcS51cmwpID09PSBudWxsIHx8IF9hID09PSB2b2lkIDAgPyB2b2lkIDAgOiBfYS5zdGFydHNXaXRoKCcvY2hhdCcpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXMuc2V0SGVhZGVyKCdYLUFjY2VsLUJ1ZmZlcmluZycsICdubycpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIG9yaWdpbmFsV3JpdGVfMSA9IHJlcy53cml0ZS5iaW5kKHJlcyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXMud3JpdGUgPSBmdW5jdGlvbiAoY2h1bmspIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgcmVzdWx0ID0gb3JpZ2luYWxXcml0ZV8xKGNodW5rKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZiAodHlwZW9mIHJlcy5mbHVzaCA9PT0gJ2Z1bmN0aW9uJylcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLmZsdXNoKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIHJlc3VsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH07XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwKHJlcSwgcmVzLCBuZXh0KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzIgLypyZXR1cm4qL107XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgIH07XG59XG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcoe1xuICAgIHBsdWdpbnM6IFtyZWFjdCgpLCBleHByZXNzUGx1Z2luKCldLFxuICAgIHJlc29sdmU6IHtcbiAgICAgICAgYWxpYXM6IHtcbiAgICAgICAgICAgICdAJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjJyksXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICBzZXJ2ZXI6IHtcbiAgICAgICAgcG9ydDogNTE3MyxcbiAgICB9LFxufSk7XG4iXSwKICAibWFwcGluZ3MiOiAiOzs7Ozs7Ozs7OztBQUE2USxTQUFTLG1CQUFtQjtBQUN6UyxTQUFTLE1BQU0sY0FBYztBQVE3QixTQUFTLGlCQUFpQjtBQUN4QixNQUFJLENBQUMsYUFBYTtBQUNoQixVQUFNLFNBQVMsUUFBUSxJQUFJO0FBQzNCLFVBQU0sU0FBUyxRQUFRLElBQUksaUJB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QkFBSSxNQUFNO0FBQ2QsMkJBQU87QUFBQSxrQkFDWDtBQUFBLGdCQUNKO0FBQ0EsZ0JBQUFGLEtBQUksS0FBSyxLQUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0YsTUFBSyxRQUFRRCxZQUFXLE9BQU87QUFBQSxJQUN4QztBQUFBLEVBQ0o7QUFBQSxFQUNBLFFBQVE7QUFBQSxJQUNKLE1BQU07QUFBQSxFQUNWO0FBQ0osQ0FBQzsiLAogICJuYW1lcyI6IFsidXVpZHY0IiwgImdsb2JhbENvbGxlY3Rpb24iLCAic2Vzc2lvbiIsICJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuQUkiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgIl9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwiLCAiX19kaXJuYW1lIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJhcHAiLCAiZG90ZW52IiwgIl9hIl0KfQo=
