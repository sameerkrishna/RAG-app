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
import { CloudClient, Schema, SparseVectorIndexConfig, DOCUMENT_KEY, Search, Knn, Rrf } from "file:///home/project/node_modules/chromadb/dist/chromadb.mjs";
import { ChromaBm25EmbeddingFunction } from "file:///home/project/node_modules/@chroma-core/chroma-bm25/dist/chroma-bm25.mjs";
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
    const collectionName = process.env.CHROMA_GLOBAL_COLLECTION || "seed_db";
    try {
      globalCollection = await client.getOrCreateCollection({
        name: collectionName,
        schema: collectionSchema,
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
      schema: collectionSchema,
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
    console.log("=== SEARCH RAW RESPONSE ===");
    console.log(JSON.stringify(results, null, 2));
    console.log("===  RAW RESPONSE ===");
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
async function hybridQueryCollection(collection, queryText, queryEmbedding, topK = 5) {
  return queryCollection(collection, queryEmbedding, topK);
  try {
    const search = new Search().rank(Rrf({
      ranks: [
        Knn({ query: queryEmbedding, returnRank: true, limit: 100 }),
        Knn({ query: queryText, key: "sparse_bm25", returnRank: true, limit: 100 })
      ],
      weights: [0.7, 0.3],
      k: 60
    })).limit(topK);
    const results = await collection.search(search);
    console.log("=== HYBRID SEARCH RAW RESPONSE ===");
    console.log(JSON.stringify(results, null, 2));
    console.log("=== END RAW RESPONSE ===");
    if (!results || !results.ids || results.ids.length === 0) {
      return [];
    }
    return results.ids.map((id, idx) => ({
      id,
      text: results.documents?.[idx] ?? "",
      metadata: results.metadatas?.[idx] ?? {},
      distance: results.distances?.[idx] ?? 0,
      score: results.scores?.[idx] ?? 1 - (results.distances?.[idx] ?? 0)
    }));
  } catch (error) {
    console.error("Hybrid query failed, falling back to dense-only:", error.message);
    return queryCollection(collection, queryEmbedding, topK);
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
var BATCH_SIZE, bm25EmbeddingFunction, collectionSchema, cloudClient, globalCollection, sessionCollections;
var init_chromaService = __esm({
  "server/services/chromaService.js"() {
    "use strict";
    BATCH_SIZE = 300;
    bm25EmbeddingFunction = new ChromaBm25EmbeddingFunction();
    collectionSchema = new Schema().createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: bm25EmbeddingFunction,
        sourceKey: DOCUMENT_KEY,
        bm25: true
      }),
      "sparse_bm25"
    );
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
function calculateCoverage(results, topK = 5) {
  if (!results || results.length === 0) return { confidence: 0, topScore: 0 };
  const scores = results.slice(0, topK).map((r) => Math.max(0, r.score));
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  return {
    confidence: Math.round(avgScore * 100),
    topScore: Math.max(...scores)
  };
}
async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || 5;
  try {
    const [queryEmbedding, sessionCollection] = await Promise.all([
      embedQuery(query),
      sessionId ? getOrCacheSessionCollection(sessionId) : Promise.resolve(null)
    ]);
    if (!sessionCollection) {
      console.warn(`\u26A0\uFE0F  No session collection found for ${sessionId}`);
      return { results: [], coverage: { confidence: 0, topScore: 0, level: "low", score: 0 }, queryEmbedding };
    }
    const rawResults = await hybridQueryCollection(sessionCollection, query, queryEmbedding, topK);
    const results = rawResults.map((r) => ({
      ...r,
      source_type: r.metadata?.source_type || "session"
    }));
    const coverage = calculateCoverage(results, topK);
    const topScore = coverage.topScore;
    const level = topScore >= 0.6 ? "high" : topScore >= 0.3 ? "medium" : "low";
    console.log("\u{1F50D} Query:", query);
    console.log("\u{1F4CA} Coverage:", { ...coverage, level });
    console.log("\u{1F4C8} Scores:", results.map((r) => r.score.toFixed(4)));
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
    TOP_K = parseInt(process.env.TOP_K) || 20;
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQsIFNjaGVtYSwgU3BhcnNlVmVjdG9ySW5kZXhDb25maWcsIERPQ1VNRU5UX0tFWSwgU2VhcmNoLCBLbm4sIFJyZiB9IGZyb20gJ2Nocm9tYWRiJztcbmltcG9ydCB7IENocm9tYUJtMjVFbWJlZGRpbmdGdW5jdGlvbiB9IGZyb20gJ0BjaHJvbWEtY29yZS9jaHJvbWEtYm0yNSc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxuLy8gXHUyNTAwXHUyNTAwIFNoYXJlZCBzY2hlbWE6IGRlbnNlIGVtYmVkZGluZ3MgKG1hbmFnZWQgZXh0ZXJuYWxseSkgKyBCTTI1IHNwYXJzZSBpbmRleCBcdTI1MDBcdTI1MDBcbmNvbnN0IGJtMjVFbWJlZGRpbmdGdW5jdGlvbiA9IG5ldyBDaHJvbWFCbTI1RW1iZWRkaW5nRnVuY3Rpb24oKTtcbmNvbnN0IGNvbGxlY3Rpb25TY2hlbWEgPSBuZXcgU2NoZW1hKCkuY3JlYXRlSW5kZXgoXG4gIG5ldyBTcGFyc2VWZWN0b3JJbmRleENvbmZpZyh7XG4gICAgZW1iZWRkaW5nRnVuY3Rpb246IGJtMjVFbWJlZGRpbmdGdW5jdGlvbixcbiAgICBzb3VyY2VLZXk6IERPQ1VNRU5UX0tFWSxcbiAgICBibTI1OiB0cnVlXG4gIH0pLFxuICAnc3BhcnNlX2JtMjUnXG4pO1xuXG5sZXQgY2xvdWRDbGllbnQgPSBudWxsO1xubGV0IGdsb2JhbENvbGxlY3Rpb24gPSBudWxsO1xuY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5mdW5jdGlvbiBnZXRDbG91ZENsaWVudCgpIHtcbiAgaWYgKCFjbG91ZENsaWVudCkge1xuICAgIGNvbnN0IGFwaUtleSA9IHByb2Nlc3MuZW52LkNIUk9NQV9BUElfS0VZO1xuICAgIGNvbnN0IHRlbmFudCA9IHByb2Nlc3MuZW52LkNIUk9NQV9URU5BTlQgfHwgJ2RlZmF1bHRfdGVuYW50JztcbiAgICBjb25zdCBkYXRhYmFzZSA9IHByb2Nlc3MuZW52LkNIUk9NQV9EQVRBQkFTRSB8fCAnZGVmYXVsdF9kYXRhYmFzZSc7XG4gICAgY29uc3QgaG9zdCA9IHByb2Nlc3MuZW52LkNIUk9NQV9IT1NUIHx8IHVuZGVmaW5lZDtcblxuICAgIGNvbnNvbGUubG9nKFwiLS0tLSBDSFJPTUEgQ09OTkVDVElWSVRZIERFQlVHIC0tLS1cIik7XG4gICAgY29uc29sZS5sb2coXCJIb3N0OiAgICAgIFwiLCBob3N0IHx8IFwiYXBpLnRyeWNocm9tYS5jb20gKGRlZmF1bHQpXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiVGVuYW50OiAgICBcIiwgdGVuYW50KTtcbiAgICBjb25zb2xlLmxvZyhcIkRCIE5hbWU6ICAgXCIsIGRhdGFiYXNlKTtcbiAgICBjb25zb2xlLmxvZyhcIkFQSSBLZXk6ICAgXCIsIGFwaUtleSA/IFwiTE9BREVEIChWQUxJRClcIiA6IFwiTUlTU0lORyAoVU5ERUZJTkVEKVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIi0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXCIpO1xuXG4gICAgaWYgKCFhcGlLZXkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgXCJDUklUSUNBTCBFUlJPUjogQ0hST01BX0FQSV9LRVkgaXMgdW5kZWZpbmVkLiBcIiArXG4gICAgICAgIFwiRW5zdXJlIHlvdXIgZW52aXJvbm1lbnQgdmFyaWFibGVzIGFyZSBjb3JyZWN0bHkgbG9hZGVkIGJlZm9yZSBleGVjdXRpbmcgdGhpcyBmaWxlLlwiXG4gICAgICApO1xuICAgIH1cblxuICAgIGNvbnN0IGNsaWVudE9wdGlvbnMgPSB7IGFwaUtleSwgdGVuYW50LCBkYXRhYmFzZSB9O1xuICAgIGlmIChob3N0KSBjbGllbnRPcHRpb25zLmhvc3QgPSBob3N0O1xuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEdsb2JhbENvbGxlY3Rpb24oKSB7XG4gIGlmICghZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBwcm9jZXNzLmVudi5DSFJPTUFfR0xPQkFMX0NPTExFQ1RJT04gfHwgJ3NlZWRfZGInO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBzY2hlbWE6IGNvbGxlY3Rpb25TY2hlbWEsXG4gICAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgICAgZGVzY3JpcHRpb246ICdQZXJtYW5lbnQgc2VlZCBkb2N1bWVudHMgZm9yIFJBRycsXG4gICAgICAgICAgdHlwZTogJ2dsb2JhbF9rbm93bGVkZ2UnXG4gICAgICAgIH0sXG4gICAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IEdsb2JhbCBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY29ubmVjdCB0byBnbG9iYWwgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGdsb2JhbENvbGxlY3Rpb247XG59XG5cbi8qKlxuICogUmV0dXJucyB7IGNvbGxlY3Rpb24sIGlzTmV3IH0uXG4gKiBpc05ldyA9IHRydWUgIFx1MjE5MiBmcmVzaGx5IGNyZWF0ZWQsIG5lZWRzIHNlZWRpbmcgZnJvbSBnbG9iYWwuXG4gKiBpc05ldyA9IGZhbHNlIFx1MjE5MiBhbHJlYWR5IGV4aXN0ZWQgb24gQ2hyb21hIENsb3VkLCByZXNwZWN0IGl0cyBjdXJyZW50IHN0YXRlLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4geyBjb2xsZWN0aW9uOiBzZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCksIGlzTmV3OiBmYWxzZSB9O1xuICB9XG5cbiAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBgc2Vzc2lvbl8ke3Nlc3Npb25JZH1gO1xuXG4gIGxldCBjb2xsZWN0aW9uO1xuICBsZXQgaXNOZXc7XG5cbiAgdHJ5IHtcbiAgICBjb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldENvbGxlY3Rpb24oe1xuICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICBlbWJlZGRpbmdGdW5jdGlvbjogbnVsbFxuICAgIH0pO1xuICAgIGlzTmV3ID0gZmFsc2U7XG4gICAgY29uc29sZS5sb2coYFxcdTI2N2JcXHVmZTBmICBTZXNzaW9uIGNvbGxlY3Rpb24gZXhpc3RzLCByZXVzaW5nOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICB9IGNhdGNoIHtcbiAgICBjb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmNyZWF0ZUNvbGxlY3Rpb24oe1xuICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICBzY2hlbWE6IGNvbGxlY3Rpb25TY2hlbWEsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICB0eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICBzZXNzaW9uX2lkOiBzZXNzaW9uSWQsXG4gICAgICAgIGNyZWF0ZWQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgfSk7XG4gICAgaXNOZXcgPSB0cnVlO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiBjcmVhdGVkOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICB9XG5cbiAgc2Vzc2lvbkNvbGxlY3Rpb25zLnNldChzZXNzaW9uSWQsIGNvbGxlY3Rpb24pO1xuICByZXR1cm4geyBjb2xsZWN0aW9uLCBpc05ldyB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGF3YWl0IGNsaWVudC5kZWxldGVDb2xsZWN0aW9uKHsgbmFtZTogY29sbGVjdGlvbk5hbWUgfSk7XG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xuICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiBkZWxldGVkOiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBkZWxldGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuLyoqXG4gKiBBZGQgdmVjdG9ycyBpbiBiYXRjaGVzIG9mIEJBVENIX1NJWkUgdG8gYXZvaWQgQ2hyb21hIHBheWxvYWQgbGltaXRzLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gYWRkVmVjdG9ycyhjb2xsZWN0aW9uLCB2ZWN0b3JzLCBlbWJlZGRpbmdzLCBpZHMpIHtcbiAgdHJ5IHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGlkcy5sZW5ndGg7IGkgKz0gQkFUQ0hfU0laRSkge1xuICAgICAgY29uc3QgYmF0Y2hJZHMgPSBpZHMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpO1xuICAgICAgY29uc3QgYmF0Y2hFbWJlZGRpbmdzID0gZW1iZWRkaW5ncy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaERvY3VtZW50cyA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYubWV0YWRhdGEpO1xuXG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmFkZCh7XG4gICAgICAgIGlkczogYmF0Y2hJZHMsXG4gICAgICAgIGVtYmVkZGluZ3M6IGJhdGNoRW1iZWRkaW5ncyxcbiAgICAgICAgZG9jdW1lbnRzOiBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiBiYXRjaE1ldGFkYXRhc1xuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgICBbYWRkVmVjdG9yc10gYmF0Y2ggJHtNYXRoLmZsb29yKGkgLyBCQVRDSF9TSVpFKSArIDF9OiBhZGRlZCAke2JhdGNoSWRzLmxlbmd0aH0gdmVjdG9yc2ApO1xuICAgIH1cbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gYWRkIHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBxdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IGNvbGxlY3Rpb24ucXVlcnkoe1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9KTtcbiAgICBcbiAgICBjb25zb2xlLmxvZygnPT09IFNFQVJDSCBSQVcgUkVTUE9OU0UgPT09Jyk7XG4gICAgY29uc29sZS5sb2coSlNPTi5zdHJpbmdpZnkocmVzdWx0cywgbnVsbCwgMikpO1xuICAgIGNvbnNvbGUubG9nKCc9PT0gIFJBVyBSRVNQT05TRSA9PT0nKTtcbiAgICBcbiAgICBpZiAoIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCB8fCByZXN1bHRzLmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICByZXR1cm4gcmVzdWx0cy5pZHNbMF0ubWFwKChpZCwgaWR4KSA9PiAoe1xuICAgICAgaWQsXG4gICAgICB0ZXh0OiByZXN1bHRzLmRvY3VtZW50c1swXVtpZHhdLFxuICAgICAgbWV0YWRhdGE6IHJlc3VsdHMubWV0YWRhdGFzWzBdW2lkeF0sXG4gICAgICBkaXN0YW5jZTogcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XSxcbiAgICAgIHNjb3JlOiAxIC0gcmVzdWx0cy5kaXN0YW5jZXNbMF1baWR4XVxuICAgIH0pKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gcXVlcnkgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBIeWJyaWQgc2VhcmNoIHVzaW5nIENocm9tYSBDbG91ZCBTZWFyY2ggQVBJIHdpdGggUlJGIChkZW5zZSArIHNwYXJzZSBCTTI1KS5cbiAqIFJldHVybnMgcmVzdWx0cyBpbiB0aGUgc2FtZSBzaGFwZSBhcyBxdWVyeUNvbGxlY3Rpb24oKSBmb3IgYmFja3dhcmQgY29tcGF0aWJpbGl0eS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGh5YnJpZFF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeVRleHQsIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSkge1xuICByZXR1cm4gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLKTtcbiAgdHJ5IHtcbiAgICBjb25zdCBzZWFyY2ggPSBuZXcgU2VhcmNoKClcbiAgICAgIC5yYW5rKFJyZih7XG4gICAgICAgIHJhbmtzOiBbXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5RW1iZWRkaW5nLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMTAwIH0pLFxuICAgICAgICAgIEtubih7IHF1ZXJ5OiBxdWVyeVRleHQsIGtleTogJ3NwYXJzZV9ibTI1JywgcmV0dXJuUmFuazogdHJ1ZSwgbGltaXQ6IDEwMCB9KVxuICAgICAgICBdLFxuICAgICAgICB3ZWlnaHRzOiBbMC43LCAwLjNdLFxuICAgICAgICBrOiA2MFxuICAgICAgfSkpXG4gICAgICAubGltaXQodG9wSyk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5zZWFyY2goc2VhcmNoKTtcblxuICAgIGNvbnNvbGUubG9nKCc9PT0gSFlCUklEIFNFQVJDSCBSQVcgUkVTUE9OU0UgPT09Jyk7XG4gICAgY29uc29sZS5sb2coSlNPTi5zdHJpbmdpZnkocmVzdWx0cywgbnVsbCwgMikpO1xuICAgIGNvbnNvbGUubG9nKCc9PT0gRU5EIFJBVyBSRVNQT05TRSA9PT0nKTtcbiAgICBcbiAgICBpZiAoIXJlc3VsdHMgfHwgIXJlc3VsdHMuaWRzIHx8IHJlc3VsdHMuaWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cblxuICAgIC8vIE1hcCByZXN1bHRzIHRvIHRoZSBzYW1lIHNoYXBlIGFzIHF1ZXJ5Q29sbGVjdGlvbigpXG4gICAgIHJldHVybiByZXN1bHRzLmlkcy5tYXAoKGlkLCBpZHgpID0+ICh7XG4gICAgICAgaWQsXG4gICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHM/LltpZHhdID8/ICcnLFxuICAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhcz8uW2lkeF0gPz8ge30sXG4gICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzPy5baWR4XSA/PyAwLFxuICAgICAgIHNjb3JlOiByZXN1bHRzLnNjb3Jlcz8uW2lkeF0gPz8gKDEgLSAocmVzdWx0cy5kaXN0YW5jZXM/LltpZHhdID8/IDApKVxuICAgIH0pKTtcbiAgICBcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdIeWJyaWQgcXVlcnkgZmFpbGVkLCBmYWxsaW5nIGJhY2sgdG8gZGVuc2Utb25seTonLCBlcnJvci5tZXNzYWdlKTtcbiAgICAvLyBHcmFjZWZ1bCBmYWxsYmFjayB0byBkZW5zZS1vbmx5IHNlYXJjaCBmb3IgYmFja3dhcmQgY29tcGF0aWJpbGl0eVxuICAgIHJldHVybiBxdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEspO1xuICB9XG59XG5cbi8qKlxuICogRGVsZXRlIGFsbCB2ZWN0b3JzIGZvciBhIGdpdmVuIGRvY3VtZW50SWQuXG4gKiBQYWdpbmF0ZXMgY29sbGVjdGlvbi5nZXQoKSBpbiBCQVRDSF9TSVpFIGNodW5rcyBzbyBkb2N1bWVudHMgd2l0aFxuICogbWFueSBjaHVua3MgKD4gZGVmYXVsdCAxMDAgbGltaXQpIGFyZSBmdWxseSBkZWxldGVkLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxJZHMgPSBbXTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgd2hlcmU6IHsgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQgfSxcbiAgICAgICAgaW5jbHVkZTogW10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBhbGxJZHMgfSk7XG4gICAgfVxuICAgIHJldHVybiBhbGxJZHMubGVuZ3RoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuLyoqXG4gKiBMaXN0IGFsbCB1bmlxdWUgZG9jdW1lbnRzIGluIGEgY29sbGVjdGlvbi5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIHdpdGggQkFUQ0hfU0laRT0zMDAgc28gY29sbGVjdGlvbnMgbGFyZ2VyXG4gKiB0aGFuIENocm9tYSdzIGRlZmF1bHQgZ2V0KCkgbGltaXQgKDEwMCkgYXJlIGZ1bGx5IGVudW1lcmF0ZWQuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24pIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBkb2N1bWVudHNNYXAgPSBuZXcgTWFwKCk7XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcblxuICAgICAgaWYgKCFiYXRjaC5pZHMgfHwgYmF0Y2guaWRzLmxlbmd0aCA9PT0gMCkgYnJlYWs7XG5cbiAgICAgIGJhdGNoLmlkcy5mb3JFYWNoKChpZCwgaWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IG1ldGEgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogZG9jSWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2NvdW50OiAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6IG1ldGEuc291cmNlX3R5cGUsXG4gICAgICAgICAgICBmaXJzdF9jaHVua190ZXh0OiBiYXRjaC5kb2N1bWVudHNbaWR4XVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgZG9jID0gZG9jdW1lbnRzTWFwLmdldChkb2NJZCk7XG4gICAgICAgIGRvYy5jaHVua19jb3VudCsrO1xuICAgICAgICBkb2MucGFnZV9jb3VudCA9IE1hdGgubWF4KGRvYy5wYWdlX2NvdW50LCBtZXRhLnBhZ2VfbnVtYmVyIHx8IDEpO1xuICAgICAgfSk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGAgIFtsaXN0RG9jdW1lbnRzXSBvZmZzZXQ9JHtvZmZzZXR9LCBnb3Q9JHtiYXRjaC5pZHMubGVuZ3RofSwgdW5pcXVlIHNvIGZhcj0ke2RvY3VtZW50c01hcC5zaXplfWApO1xuXG4gICAgICBpZiAoYmF0Y2guaWRzLmxlbmd0aCA8IEJBVENIX1NJWkUpIGJyZWFrO1xuICAgICAgb2Zmc2V0ICs9IEJBVENIX1NJWkU7XG4gICAgfVxuXG4gICAgcmV0dXJuIEFycmF5LmZyb20oZG9jdW1lbnRzTWFwLnZhbHVlcygpKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHM6JywgZXJyb3IpO1xuICAgIHJldHVybiBbXTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoQ2hlY2soKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBoZWFydGJlYXQgPSBhd2FpdCBjbGllbnQuaGVhcnRiZWF0KCk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ2hlYWx0aHknLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBoZWFydGJlYXRcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICd1bmhlYWx0aHknLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgIH07XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGNsZWFudXBTZXNzaW9uQ29sbGVjdGlvbnMoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9ucyA9IGF3YWl0IGNsaWVudC5saXN0Q29sbGVjdGlvbnMoKTtcblxuICAgIGNvbnN0IHNlc3Npb25Db2xsZWN0aW9uTmFtZXMgPSBjb2xsZWN0aW9uc1xuICAgICAgLm1hcChjID0+ICh0eXBlb2YgYyA9PT0gJ3N0cmluZycgPyBjIDogYy5uYW1lKSlcbiAgICAgIC5maWx0ZXIobmFtZSA9PiBuYW1lLnN0YXJ0c1dpdGgoJ3Nlc3Npb25fJykpO1xuXG4gICAgaWYgKHNlc3Npb25Db2xsZWN0aW9uTmFtZXMubGVuZ3RoID09PSAwKSB7XG4gICAgICBjb25zb2xlLmxvZygnXFx1MjcwNSBObyBzdGFsZSBzZXNzaW9uIGNvbGxlY3Rpb25zIGZvdW5kLicpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBcXHVkODNlXFx1ZGRmOSBDbGVhbmluZyB1cCAke3Nlc3Npb25Db2xsZWN0aW9uTmFtZXMubGVuZ3RofSBzdGFsZSBzZXNzaW9uIGNvbGxlY3Rpb24ocykuLi5gKTtcblxuICAgIGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHNlc3Npb25Db2xsZWN0aW9uTmFtZXMubWFwKGFzeW5jIG5hbWUgPT4ge1xuICAgICAgICB0cnkge1xuICAgICAgICAgIGF3YWl0IGNsaWVudC5kZWxldGVDb2xsZWN0aW9uKHsgbmFtZSB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgICBcXHUyNzA1IERlbGV0ZWQ6ICR7bmFtZX1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGAgIFxcdTI2YTBcXHVmZTBmIENvdWxkIG5vdCBkZWxldGUgJHtuYW1lfTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH0pXG4gICAgKTtcblxuICAgIHNlc3Npb25Db2xsZWN0aW9ucy5jbGVhcigpO1xuICAgIGNvbnNvbGUubG9nKCdcXHUyNzA1IFNlc3Npb24gY29sbGVjdGlvbiBjbGVhbnVwIGNvbXBsZXRlLicpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUud2FybignXFx1MjZhMFxcdWZlMGYgU2Vzc2lvbiBjbGVhbnVwIGZhaWxlZCAobm9uLWZhdGFsKTonLCBlcnJvci5tZXNzYWdlKTtcbiAgfVxufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoZWFsdGgocmVxLCByZXMpIHtcbiAgY29uc3QgaGVhbHRoU3RhdHVzID0ge1xuICAgIHN0YXR1czogJ29rJyxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICBzZXJ2aWNlczoge31cbiAgfTtcblxuICAvLyBDaGVjayBDaHJvbWFEQlxuICB0cnkge1xuICAgIGNvbnN0IGNocm9tYUhlYWx0aCA9IGF3YWl0IGNocm9tYUhlYWx0aENoZWNrKCk7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0gY2hyb21hSGVhbHRoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IHtcbiAgICAgIHN0YXR1czogJ2Vycm9yJyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlXG4gICAgfTtcbiAgfVxuXG4gIC8vIE92ZXJhbGwgc3RhdHVzXG4gIGNvbnN0IGhhc0Vycm9ycyA9IE9iamVjdC52YWx1ZXMoaGVhbHRoU3RhdHVzLnNlcnZpY2VzKS5zb21lKFxuICAgIHMgPT4gcy5zdGF0dXMgPT09ICdlcnJvcicgfHwgcy5zdGF0dXMgPT09ICd1bmhlYWx0aHknXG4gICk7XG5cbiAgaWYgKGhhc0Vycm9ycykge1xuICAgIGhlYWx0aFN0YXR1cy5zdGF0dXMgPSAnZGVncmFkZWQnO1xuICB9XG5cbiAgcmVzLmpzb24oaGVhbHRoU3RhdHVzKTtcbn1cblxucm91dGVyLmdldCgnLycsIGhlYWx0aCk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtleHBvcnQgY2xhc3MgQXBwRXJyb3IgZXh0ZW5kcyBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUsIHN0YXR1c0NvZGUgPSA1MDApIHtcbiAgICBzdXBlcihtZXNzYWdlKTtcbiAgICB0aGlzLmNvZGUgPSBjb2RlO1xuICAgIHRoaXMuc3RhdHVzQ29kZSA9IHN0YXR1c0NvZGU7XG4gICAgdGhpcy5pc09wZXJhdGlvbmFsID0gdHJ1ZTtcbiAgICBFcnJvci5jYXB0dXJlU3RhY2tUcmFjZSh0aGlzLCB0aGlzLmNvbnN0cnVjdG9yKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVmFsaWRhdGlvbkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1ZBTElEQVRJT05fRVJST1InKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgY29kZSwgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVXBsb2FkTGltaXRFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdVUExPQURfTElNSVRfRVhDRUVERUQnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgY29kZSwgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRmlsZVRvb0xhcmdlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1heFNpemVNQikge1xuICAgIHN1cGVyKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCwgJ0ZJTEVfVE9PX0xBUkdFJywgNDEzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgSW52YWxpZEZpbGVUeXBlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdPbmx5IFBERiBmaWxlcyBhcmUgYWxsb3dlZCcsICdJTlZBTElEX0ZJTEVfVFlQRScsIDQxNSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFRvb01hbnlQREZzRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1heCkge1xuICAgIHN1cGVyKGBNYXhpbXVtICR7bWF4fSBQREZzIGFsbG93ZWQgcGVyIHNlc3Npb25gLCAnVE9PX01BTllfUERGUycsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIER1cGxpY2F0ZUZpbGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoZmlsZW5hbWUpIHtcbiAgICBzdXBlcihgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCwgJ0RVUExJQ0FURV9GSUxFJywgNDA5KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ29ycnVwdGVkUERGRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdGYWlsZWQgdG8gcGFyc2UgUERGIGZpbGUuIEl0IG1heSBiZSBjb3JydXB0ZWQuJywgJ0NPUlJVUFRFRF9QREYnLCA0MjIpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSYXRlTGltaXRFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IocmV0cnlBZnRlciA9IDYwKSB7XG4gICAgc3VwZXIoJ1JhdGUgbGltaXQgZXhjZWVkZWQuIFBsZWFzZSB0cnkgYWdhaW4gbGF0ZXIuJywgJ1JBVEVfTElNSVRfRVhDRUVERUQnLCA0MjkpO1xuICAgIHRoaXMucmV0cnlBZnRlciA9IHJldHJ5QWZ0ZXI7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIExMTVVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdBSSBzZXJ2aWNlIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlLiBQbGVhc2UgdHJ5IGFnYWluLicsICdMTE1fVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBFbWJlZGRpbmdFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSA9ICdGYWlsZWQgdG8gZ2VuZXJhdGUgZW1iZWRkaW5ncycpIHtcbiAgICBzdXBlcihtZXNzYWdlLCAnRU1CRURESU5HX0VSUk9SJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmV0cmlldmFsVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0RvY3VtZW50IHJldHJpZXZhbCBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZScsICdSRVRSSUVWQUxfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9zYW5pdGl6ZS5qc1wiO2ltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgVmFsaWRhdGlvbkVycm9yIH0gZnJvbSAnLi9lcnJvcnMuanMnO1xuXG5jb25zdCBEQU5HRVJPVVNfUEFUVEVSTlMgPSAvWzw+OlwifD8qXFx4MDAtXFx4MWZdL2c7XG5jb25zdCBQQVRIX1RSQVZFUlNBTCA9IC9cXC5cXC4vZztcblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplRmlsZW5hbWUoZmlsZW5hbWUpIHtcbiAgaWYgKCFmaWxlbmFtZSB8fCB0eXBlb2YgZmlsZW5hbWUgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZScpO1xuICB9XG5cbiAgLy8gUmVtb3ZlIHBhdGggY29tcG9uZW50cyBhbmQgZ2V0IGJhc2VuYW1lXG4gIGNvbnN0IGJhc2VuYW1lID0gcGF0aC5iYXNlbmFtZShmaWxlbmFtZSk7XG5cbiAgLy8gUmVtb3ZlIGRhbmdlcm91cyBjaGFyYWN0ZXJzXG4gIGxldCBzYW5pdGl6ZWQgPSBiYXNlbmFtZS5yZXBsYWNlKERBTkdFUk9VU19QQVRURVJOUywgJ18nKTtcblxuICAvLyBSZW1vdmUgcGF0aCB0cmF2ZXJzYWwgYXR0ZW1wdHNcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnJlcGxhY2UoUEFUSF9UUkFWRVJTQUwsICcnKTtcblxuICAvLyBUcmltIHdoaXRlc3BhY2UgYW5kIGxpbWl0IGxlbmd0aFxuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQudHJpbSgpLnNsaWNlKDAsIDI1NSk7XG5cbiAgaWYgKCFzYW5pdGl6ZWQpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lIGFmdGVyIHNhbml0aXphdGlvbicpO1xuICB9XG5cbiAgcmV0dXJuIHNhbml0aXplZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlUERGRmlsZShmaWxlKSB7XG4gIGlmICghZmlsZSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ05vIGZpbGUgcHJvdmlkZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIE1JTUUgdHlwZVxuICBjb25zdCB2YWxpZE1pbWVUeXBlcyA9IFsnYXBwbGljYXRpb24vcGRmJ107XG4gIGlmICghdmFsaWRNaW1lVHlwZXMuaW5jbHVkZXMoZmlsZS5taW1ldHlwZSkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdPbmx5IFBERiBmaWxlcyBhcmUgYWNjZXB0ZWQnKTtcbiAgfVxuXG4gIC8vIENoZWNrIGV4dGVuc2lvblxuICBjb25zdCBleHQgPSBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUgfHwgJycpLnRvTG93ZXJDYXNlKCk7XG4gIGlmIChleHQgIT09ICcucGRmJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ZpbGUgbXVzdCBoYXZlIC5wZGYgZXh0ZW5zaW9uJyk7XG4gIH1cblxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRmlsZVNpemUoc2l6ZUJ5dGVzLCBtYXhTaXplTUIpIHtcbiAgY29uc3QgbWF4Qnl0ZXMgPSBtYXhTaXplTUIgKiAxMDI0ICogMTAyNDtcbiAgaWYgKHNpemVCeXRlcyA+IG1heEJ5dGVzKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmApO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVJbnB1dChpbnB1dCwgbWF4TGVuZ3RoID0gMTAwMDApIHtcbiAgaWYgKCFpbnB1dCB8fCB0eXBlb2YgaW5wdXQgIT09ICdzdHJpbmcnKSB7XG4gICAgcmV0dXJuICcnO1xuICB9XG4gIHJldHVybiBpbnB1dC50cmltKCkuc2xpY2UoMCwgbWF4TGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlRG9jdW1lbnRJZChpZCkge1xuICBpZiAoIWlkIHx8IHR5cGVvZiBpZCAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEJyk7XG4gIH1cbiAgY29uc3QgdXVpZFJlZ2V4ID0gL15bMC05YS1mXXs4fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXs0fS1bMC05YS1mXXsxMn0kL2k7XG4gIGlmICghdXVpZFJlZ2V4LnRlc3QoaWQpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCBmb3JtYXQnKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RUZXh0RnJvbVBERkJ1ZmZlcihidWZmZXIpIHtcbiAgLy8gVGhpcyB3aWxsIGJlIHVzZWQgd2l0aCBwZGYtcGFyc2VcbiAgcmV0dXJuIGJ1ZmZlcjtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2ltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuXG5jb25zdCBDSEFSU19QRVJfVE9LRU4gICAgID0gNDtcbmNvbnN0IFRBUkdFVF9DSFVOS19UT0tFTlMgPSA2MDA7ICAgLy8gc29mdCB0YXJnZXQgcGVyIGNodW5rXG5jb25zdCBNQVhfQ0hVTktfVE9LRU5TICAgID0gNzUwOyAgIC8vIGhhcmQgY2FwIGJlZm9yZSBmb3JjZWQgc3BsaXRcbmNvbnN0IE9WRVJMQVBfVE9LRU5TICAgICAgPSAxMDA7ICAgLy8gb3ZlcmxhcCBvbmx5IG9uIG92ZXJzaXplZCBwYXJhZ3JhcGhzXG5jb25zdCBNSU5fQ0hVTktfQ0hBUlMgICAgID0gMTAwO1xuXG4vLyBNYXRjaGVzIEFMTC1DQVBTIGhlYWRpbmdzLCBtYXJrZG93biBoZWFkaW5ncywgb3IgbnVtYmVyZWQgc2VjdGlvbiBoZWFkaW5nc1xuY29uc3QgSEVBRElOR19SRSA9IC9eKD86W0EtWl1bQS1aXFxzXXsyLDYwfSR8I3sxLDR9XFxzLit8KD86XFxkK1xcLikrXFxzLispL207XG5cbmV4cG9ydCBmdW5jdGlvbiBlc3RpbWF0ZVRva2Vucyh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAwO1xuICByZXR1cm4gTWF0aC5jZWlsKHRleHQubGVuZ3RoIC8gQ0hBUlNfUEVSX1RPS0VOKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFuVGV4dCh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAnJztcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvXFxmL2csICdcXG4nKVxuICAgIC5yZXBsYWNlKC8oXFxzKlxcbil7Myx9L2csICdcXG5cXG4nKVxuICAgIC5yZXBsYWNlKC9eXFxzKlxcZCtcXHMqJC9nbSwgJycpXG4gICAgLnJlcGxhY2UoL1sgXFx0XXsyLH0vZywgJyAnKVxuICAgIC50cmltKCk7XG59XG5cbmZ1bmN0aW9uIGdlbmVyYXRlQ2h1bmtJZCh0ZXh0LCBmaWxlbmFtZSkge1xuICByZXR1cm4gY3JlYXRlSGFzaCgnbWQ1JylcbiAgICAudXBkYXRlKGAke2ZpbGVuYW1lfTo6JHt0ZXh0fWApXG4gICAgLmRpZ2VzdCgnaGV4JylcbiAgICAuc2xpY2UoMCwgMTYpO1xufVxuXG4vKipcbiAqIFN0cnVjdHVyZS1hd2FyZSBjaHVua2luZzpcbiAqICAxLiBTcGxpdCBvbiBibGFuayBsaW5lcyAoXFxuXFxuKSBpbnRvIHBhcmFncmFwaHMuXG4gKiAgMi4gQSBsaW5lIG1hdGNoaW5nIEhFQURJTkdfUkUgYWx3YXlzIHN0YXJ0cyBhIGZyZXNoIGNodW5rLlxuICogIDMuIEFjY3VtdWxhdGUgcGFyYWdyYXBocyB1bnRpbCB0aGUgc29mdCBUQVJHRVQgaXMgcmVhY2hlZCwgdGhlbiBmbHVzaC5cbiAqICA0LiBQYXJhZ3JhcGhzIGxhcmdlciB0aGFuIE1BWCBhcmUgc3BsaXQgd2l0aCBhIHNsaWRpbmcgd2luZG93ICsgb3ZlcmxhcCBhcyBmYWxsYmFjay5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGNodW5rVGV4dCh0ZXh0LCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdGFyZ2V0VG9rZW5zID0gb3B0aW9ucy5jaHVua1NpemVUb2tlbnMgfHwgVEFSR0VUX0NIVU5LX1RPS0VOUztcbiAgY29uc3QgbWF4VG9rZW5zICAgID0gb3B0aW9ucy5tYXhDaHVua1Rva2VucyAgfHwgTUFYX0NIVU5LX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRrICAgID0gb3B0aW9ucy5vdmVybGFwVG9rZW5zICAgfHwgT1ZFUkxBUF9UT0tFTlM7XG5cbiAgY29uc3QgdGFyZ2V0Q2hhcnMgID0gdGFyZ2V0VG9rZW5zICogQ0hBUlNfUEVSX1RPS0VOO1xuICBjb25zdCBtYXhDaGFycyAgICAgPSBtYXhUb2tlbnMgICAgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUayAgICAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgLy8gMS4gU3BsaXQgaW50byBwYXJhZ3JhcGhzXG4gIGNvbnN0IHJhd1BhcmFzID0gdGV4dFxuICAgIC5zcGxpdCgvXFxuezIsfS8pXG4gICAgLm1hcChwID0+IHAudHJpbSgpKVxuICAgIC5maWx0ZXIocCA9PiBwLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpO1xuXG4gIGNvbnN0IGNodW5rcyAgICAgPSBbXTtcbiAgbGV0ICAgYnVmZmVyICAgICA9ICcnO1xuICBsZXQgICBidWZTdGFydCAgID0gMDtcbiAgbGV0ICAgY2h1bmtJbmRleCA9IDA7XG4gIGxldCAgIGNoYXJDdXJzb3IgPSAwO1xuXG4gIGNvbnN0IGZsdXNoID0gKGZvcmNlVGV4dCkgPT4ge1xuICAgIGNvbnN0IGNvbnRlbnQgPSAoZm9yY2VUZXh0ID8/IGJ1ZmZlcikudHJpbSgpO1xuICAgIGlmIChjb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogICAgICAgY29udGVudCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogIGJ1ZlN0YXJ0LFxuICAgICAgICBjaGFyRW5kOiAgICBidWZTdGFydCArIGNvbnRlbnQubGVuZ3RoLFxuICAgICAgICBjaHVua0luZGV4OiBjaHVua0luZGV4KytcbiAgICAgIH0pO1xuICAgIH1cbiAgICBidWZmZXIgICA9ICcnO1xuICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgfTtcblxuICBmb3IgKGNvbnN0IHBhcmEgb2YgcmF3UGFyYXMpIHtcbiAgICBjb25zdCBpc0hlYWRpbmcgPSBIRUFESU5HX1JFLnRlc3QocGFyYS5zcGxpdCgnXFxuJylbMF0pO1xuXG4gICAgLy8gMi4gSGVhZGluZyBhbHdheXMgc3RhcnRzIGEgbmV3IGNodW5rXG4gICAgaWYgKGlzSGVhZGluZyAmJiBidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgIGlmIChwYXJhLmxlbmd0aCA+IG1heENoYXJzKSB7XG4gICAgICAvLyAzLiBPdmVyc2l6ZWQgcGFyYWdyYXBoIC0+IHNsaWRpbmctd2luZG93IGNoYXIgZmFsbGJhY2tcbiAgICAgIGlmIChidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgICAgbGV0IHMgPSAwO1xuICAgICAgd2hpbGUgKHMgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICBsZXQgZSA9IHMgKyB0YXJnZXRDaGFycztcbiAgICAgICAgaWYgKGUgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICAgIGNvbnN0IHNlYXJjaEZyb20gPSBlIC0gTWF0aC5mbG9vcih0YXJnZXRDaGFycyAqIDAuMik7XG4gICAgICAgICAgZm9yIChjb25zdCBicCBvZiBbJy4gJywgJy5cXG4nLCAnPyAnLCAnISAnLCAnXFxuJ10pIHtcbiAgICAgICAgICAgIGNvbnN0IGlkeCA9IHBhcmEubGFzdEluZGV4T2YoYnAsIGUpO1xuICAgICAgICAgICAgaWYgKGlkeCA+IHNlYXJjaEZyb20pIHsgZSA9IGlkeCArIGJwLmxlbmd0aDsgYnJlYWs7IH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgZSA9IE1hdGgubWluKGUsIHBhcmEubGVuZ3RoKTtcbiAgICAgICAgY29uc3Qgc2xpY2UgPSBwYXJhLnNsaWNlKHMsIGUpLnRyaW0oKTtcbiAgICAgICAgaWYgKHNsaWNlLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgICAgICB0ZXh0OiAgICAgICBzbGljZSxcbiAgICAgICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKHNsaWNlKSxcbiAgICAgICAgICAgIGNoYXJTdGFydDogIGNoYXJDdXJzb3IgKyBzLFxuICAgICAgICAgICAgY2hhckVuZDogICAgY2hhckN1cnNvciArIGUsXG4gICAgICAgICAgICBjaHVua0luZGV4OiBjaHVua0luZGV4KytcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCBuZXh0ID0gZSAtIG92ZXJsYXBDaGFycztcbiAgICAgICAgcyA9IG5leHQgPiBzID8gbmV4dCA6IGU7XG4gICAgICB9XG4gICAgICBjaGFyQ3Vyc29yICs9IHBhcmEubGVuZ3RoICsgMjtcbiAgICAgIGJ1ZlN0YXJ0ICAgID0gY2hhckN1cnNvcjtcbiAgICAgIGNvbnRpbnVlO1xuICAgIH1cblxuICAgIC8vIDQuIE5vcm1hbCBwYXJhZ3JhcGggXHUyMDE0IGhhcmQgY2FwIGxvb2thaGVhZCBCRUZPUkUgYWNjdW11bGF0aW5nXG4gICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwICYmIChidWZmZXIubGVuZ3RoICsgcGFyYS5sZW5ndGggKyAyKSA+IG1heENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cblxuICAgIGJ1ZmZlciAgICAgPSBidWZmZXIgPyBidWZmZXIgKyAnXFxuXFxuJyArIHBhcmEgOiBwYXJhO1xuICAgIGNoYXJDdXJzb3IgKz0gcGFyYS5sZW5ndGggKyAyO1xuXG4gICAgLy8gU29mdCBjYXA6IGZsdXNoIG9uY2UgdGFyZ2V0IGlzIHJlYWNoZWRcbiAgICBpZiAoYnVmZmVyLmxlbmd0aCA+PSB0YXJnZXRDaGFycykge1xuICAgICAgZmx1c2goKTtcbiAgICB9XG4gIH1cblxuICAvLyA1LiBGbHVzaCByZW1haW5kZXJcbiAgZmx1c2goKTtcblxuICByZXR1cm4gY2h1bmtzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtQREZDb250ZW50KHBkZkRhdGEsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB7IGZpbGVuYW1lLCBkb2N1bWVudElkLCBwYWdlTnVtYmVyLCB0ZXh0LCB0b3RhbFBhZ2VzIH0gPSBwZGZEYXRhO1xuXG4gIGlmICghdGV4dCB8fCB0ZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICAke2ZpbGVuYW1lfSBwYWdlICR7cGFnZU51bWJlcn06IGV4dHJhY3RlZCB0ZXh0IHRvbyBzaG9ydCBcdTIwMTQgbWF5IGJlIGEgc2Nhbm5lZCBwYWdlLCBza2lwcGluZ2ApO1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGNsZWFuZWRUZXh0ID0gY2xlYW5UZXh0KHRleHQpO1xuICBjb25zdCB0ZXh0Q2h1bmtzICA9IGNodW5rVGV4dChjbGVhbmVkVGV4dCwgb3B0aW9ucyk7XG4gIGNvbnN0IHRvdGFsQ2h1bmtzID0gdGV4dENodW5rcy5sZW5ndGg7XG4gIGNvbnN0IHNvdXJjZVR5cGUgID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogICAgICBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZSxcbiAgICAgICAgY2h1bmtfaWQ6ICAgICAgICAgY2h1bmtJZCxcbiAgICAgICAgY2h1bmtfaW5kZXg6ICAgICAgY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiAgICAgdG90YWxDaHVua3MsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiAgICAgIHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6ICAgICAgdG90YWxQYWdlcyB8fCBudWxsLFxuICAgICAgICBzZWN0aW9uX3RpdGxlOiAgICBleHRyYWN0U2VjdGlvblRpdGxlKGNodW5rLnRleHQpLFxuICAgICAgICBzb3VyY2VfdHlwZTogICAgICBzb3VyY2VUeXBlLFxuICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIGNoYXJfc3RhcnQ6ICAgICAgIGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgY2hhcl9lbmQ6ICAgICAgICAgY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6ICAgICAgY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH07XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0U2VjdGlvblRpdGxlKHRleHQpIHtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IEVtYmVkZGluZ0Vycm9yLCBpczQyOUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAxLiBTTElESU5HIFdJTkRPVyBSQVRFIExJTUlURVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY2xhc3MgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyIHtcbiAgY29uc3RydWN0b3IobGltaXRQZXJNaW51dGUpIHtcbiAgICB0aGlzLmxpbWl0UGVyTWludXRlID0gbGltaXRQZXJNaW51dGU7XG4gICAgdGhpcy53aW5kb3dNcyA9IDYwMDAwO1xuICAgIHRoaXMucmVxdWVzdHMgPSBbXTtcbiAgfVxuXG4gIGFzeW5jIGNvbnN1bWUodG9rZW5zKSB7XG4gICAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgICAvLyBSZW1vdmUgZW50cmllcyBvbGRlciB0aGFuIDYwIHNlY29uZHNcbiAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBub3cgLSB0aGlzLndpbmRvd01zKTtcblxuICAgIGNvbnN0IGN1cnJlbnRUb3RhbCA9IHRoaXMucmVxdWVzdHMucmVkdWNlKChzdW0sIHJlcSkgPT4gc3VtICsgcmVxLnRva2VucywgMCk7XG5cbiAgICAvLyBJZiB3ZSBoYXZlIHJvb20sIGNvbnN1bWUgaW5zdGFudGx5IChidXJzdClcbiAgICBpZiAoY3VycmVudFRvdGFsICsgdG9rZW5zIDw9IHRoaXMubGltaXRQZXJNaW51dGUpIHtcbiAgICAgIHRoaXMucmVxdWVzdHMucHVzaCh7IHRpbWVzdGFtcDogbm93LCB0b2tlbnMgfSk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gT3RoZXJ3aXNlLCB3YWl0IHVudGlsIHRoZSBvbGRlc3QgcmVxdWVzdCBleHBpcmVzIChwbHVzIGEgc21hbGwgYnVmZmVyKVxuICAgIGNvbnN0IG5lZWRlZCA9IHRva2VucyAtICh0aGlzLmxpbWl0UGVyTWludXRlIC0gY3VycmVudFRvdGFsKTtcbiAgICBsZXQgYWNjdW11bGF0ZWRFeHBpcmVkID0gMDtcbiAgICBsZXQgd2FpdFVudGlsID0gbm93ICsgdGhpcy53aW5kb3dNczsgLy8gZmFsbGJhY2tcblxuICAgIGNvbnN0IHNvcnRlZCA9IFsuLi50aGlzLnJlcXVlc3RzXS5zb3J0KChhLCBiKSA9PiBhLnRpbWVzdGFtcCAtIGIudGltZXN0YW1wKTtcbiAgICBmb3IgKGNvbnN0IHJlcSBvZiBzb3J0ZWQpIHtcbiAgICAgIGFjY3VtdWxhdGVkRXhwaXJlZCArPSByZXEudG9rZW5zO1xuICAgICAgaWYgKGFjY3VtdWxhdGVkRXhwaXJlZCA+PSBuZWVkZWQpIHtcbiAgICAgICAgLy8gKzEwbXMgYnVmZmVyIHRvIHNsaWRlIHRoZSB3aW5kb3cgY2xlYW5seVxuICAgICAgICB3YWl0VW50aWwgPSByZXEudGltZXN0YW1wICsgdGhpcy53aW5kb3dNcyArIDEwO1xuICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBkZWxheSA9IHdhaXRVbnRpbCAtIG5vdztcbiAgICBpZiAoZGVsYXkgPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgYFtyYXRlLWxpbWl0XSBXaW5kb3cgZnVsbCAoJHtjdXJyZW50VG90YWx9LyR7dGhpcy5saW1pdFBlck1pbnV0ZX0pLiBgICtcbiAgICAgICAgYFdhaXRpbmcgJHsoZGVsYXkgLyAxMDAwKS50b0ZpeGVkKDEpfXMgdG8gc2VuZCAke3Rva2Vuc30gdG9rZW5zLi4uYFxuICAgICAgKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCBkZWxheSkpO1xuICAgIH1cblxuICAgIC8vIFJlY29yZCB0aGUgY29uc3VtcHRpb24gYXQgdGhlIG5ldyB0aW1lXG4gICAgdGhpcy5yZXF1ZXN0cy5wdXNoKHsgdGltZXN0YW1wOiBEYXRlLm5vdygpLCB0b2tlbnMgfSk7XG4gICAgLy8gQ2xlYW51cCBhZ2FpbiBqdXN0IGluIGNhc2VcbiAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBEYXRlLm5vdygpIC0gdGhpcy53aW5kb3dNcyk7XG4gIH1cbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAyLiBDT05GSUdVUkFUSU9OXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnN0IFRQTV9MSU1JVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfVFBNX0xJTUlUKSB8fCA1MDAwMDA7XG5jb25zdCBSQVRFX0xJTUlURVIgPSBuZXcgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyKFRQTV9MSU1JVCk7XG5cbi8vIEJBVENIX1NJWkU6IG51bWJlciBvZiBjaHVua3MgcGVyIGVtYmVkQ29udGVudCBjYWxsXG4vLyAoa2VwdCBhdCAxMDsgbm90ZSB0aGUgcmVhbCBjZWlsaW5nIGlzIHRoZSBBUEkncyB+MTAwLXJlcXVlc3RzLXBlci1jYWxsIGxpbWl0LFxuLy8gbm90IGEgXCJjb250ZXh0IHdpbmRvd1wiIGxpbWl0IFx1MjAxNCAxMCBqdXN0IGtlZXBzIGJhdGNoZXMgc21hbGwgYW5kIHJldHJ5LWZyaWVuZGx5KVxuY29uc3QgQkFUQ0hfU0laRSA9ICgpID0+IDEwOyAgIC8vIDEwIGNodW5rcyBcdTAwRDcgNzUwIHRva2VucyA9IDcsNTAwIHRva2VucyBwZXIgQVBJIHJlcXVlc3RcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gMTA7IC8vIFNlbmQgMTAgYmF0Y2hlcyBjb25jdXJyZW50bHkgdG8gY2xlYXIgdGhlIGJ1cnN0IGZhc3RcblxuLy8gUmV0cnkgY29uZmlndXJhdGlvbiAoZXhwb25lbnRpYWwgYmFja29mZiArIGppdHRlcilcbmNvbnN0IFJFVFJZX0JBU0VfREVMQVlfTVMgPSAyMDAwOyAgIC8vIDIgc2Vjb25kc1xuY29uc3QgUkVUUllfTUFYX0RFTEFZX01TID0gNjAwMDA7ICAgLy8gNjAgc2Vjb25kcyBjYXBcbmNvbnN0IE1BWF9SRVRSWV9BVFRFTVBUUyA9IDU7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMy4gQUkgQ0xJRU5UIChzaW5nbGUsIHJldXNhYmxlIGluc3RhbmNlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zdCBhaSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gIHZlcnRleGFpOiB0cnVlLFxuICBwcm9qZWN0OiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCBwcm9jZXNzLmVudi5HQ1BfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJyxcbiAgbG9jYXRpb246IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9MT0NBVElPTiB8fCAndXMtY2VudHJhbDEnXG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0LiBUT0tFTiBDQUxDVUxBVElPTiAodXNlcyBzdG9yZWQgdG9rZW5fY291bnQgaWYgYXZhaWxhYmxlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5mdW5jdGlvbiBnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKGNodW5rcykge1xuICByZXR1cm4gY2h1bmtzLnJlZHVjZSgoc3VtLCBjaHVuaykgPT4ge1xuICAgIC8vIFByZWZlciB0aGUgZXhhY3QgdG9rZW4gY291bnQgZnJvbSBjaHVua2VyLCBvdGhlcndpc2UgZmFsbGJhY2sgdG8gcm91Z2ggZXN0aW1hdGVcbiAgICBjb25zdCB0b2tlbkNvdW50ID0gY2h1bmsubWV0YWRhdGE/LnRva2VuX2NvdW50IHx8IE1hdGguY2VpbChjaHVuay50ZXh0Lmxlbmd0aCAvIDQpO1xuICAgIHJldHVybiBzdW0gKyB0b2tlbkNvdW50O1xuICB9LCAwKTtcbn1cblxuLy8gU2FtZSByb3VnaCBlc3RpbWF0ZSBhcyBhYm92ZSwgYnV0IGZvciByYXcgc3RyaW5ncyB0aGF0IGRvbid0IGNhcnJ5IGNodW5rIG1ldGFkYXRhXG4vLyAodXNlZCBmb3IgcmV0cmllcyBpbnNpZGUgZW1iZWRCYXRjaCwgYW5kIGZvciBlbWJlZFF1ZXJ5KS5cbmZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpIHtcbiAgcmV0dXJuIHRleHRzLnJlZHVjZSgoc3VtLCB0ZXh0KSA9PiBzdW0gKyBNYXRoLmNlaWwoU3RyaW5nKHRleHQpLmxlbmd0aCAvIDQpLCAwKTtcbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA1LiBFTUJFRCBCQVRDSCAod2l0aCBleHBvbmVudGlhbCBiYWNrb2ZmICsgaml0dGVyKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hc3luYyBmdW5jdGlvbiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBhdHRlbXB0ID0gMSkge1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG4gIGNvbnN0IG91dHB1dERpbWVuc2lvbmFsaXR5ID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19ESU1FTlNJT05TKSB8fCAzMDcyO1xuXG4gIHRyeSB7XG4gICAgLy8gRklYOiBgYWkuYmF0Y2hlcy5jcmVhdGVFbWJlZGRpbmdzYCBpcyBub3QgYSByZWFsIG1ldGhvZCBvbiB0aGUgQGdvb2dsZS9nZW5haSBTREsuXG4gICAgLy8gYGFpLmJhdGNoZXNgIGlzIGZvciBhc3luYyBiYXRjaC1wcmVkaWN0aW9uIGpvYnMuIFN5bmNocm9ub3VzIGVtYmVkZGluZyBjYWxscyBnb1xuICAgIC8vIHRocm91Z2ggYGFpLm1vZGVscy5lbWJlZENvbnRlbnRgLCB3aXRoIG9uZSBzaGFyZWQgdGFza1R5cGUvb3V0cHV0RGltZW5zaW9uYWxpdHlcbiAgICAvLyBjb25maWcgYXBwbGllZCBhY3Jvc3MgYWxsIGBjb250ZW50c2AgaW4gdGhlIGNhbGwuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCBhaS5tb2RlbHMuZW1iZWRDb250ZW50KHtcbiAgICAgIG1vZGVsOiBtb2RlbE5hbWUsXG4gICAgICBjb250ZW50czogdGV4dHMubWFwKHRleHQgPT4gKHR5cGVvZiB0ZXh0ID09PSAnc3RyaW5nJyA/IHRleHQgOiBTdHJpbmcodGV4dCkpKSxcbiAgICAgIGNvbmZpZzoge1xuICAgICAgICB0YXNrVHlwZTogdGFza1R5cGUsXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBvdXRwdXREaW1lbnNpb25hbGl0eVxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IHJlc3BvbnNlPy5lbWJlZGRpbmdzPy5tYXAoZSA9PiBlLnZhbHVlcykgfHwgW107XG4gICAgaWYgKGVtYmVkZGluZ3MubGVuZ3RoICE9PSB0ZXh0cy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihgRXhwZWN0ZWQgJHt0ZXh0cy5sZW5ndGh9IGVtYmVkZGluZ3MsIGdvdCAke2VtYmVkZGluZ3MubGVuZ3RofWApO1xuICAgIH1cbiAgICByZXR1cm4gZW1iZWRkaW5ncztcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzUmV0cnlhYmxlID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAyIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDMgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnU2VydmljZSBVbmF2YWlsYWJsZScpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ0JhZCBHYXRld2F5Jyk7XG5cbiAgICBpZiAoaXNSZXRyeWFibGUgJiYgYXR0ZW1wdCA8IE1BWF9SRVRSWV9BVFRFTVBUUykge1xuICAgICAgLy8gRXhwb25lbnRpYWwgYmFja29mZjogMl5hdHRlbXB0ICogYmFzZSAoY2FwcGVkKVxuICAgICAgbGV0IGRlbGF5ID0gTWF0aC5taW4oUkVUUllfTUFYX0RFTEFZX01TLCBSRVRSWV9CQVNFX0RFTEFZX01TICogTWF0aC5wb3coMiwgYXR0ZW1wdCAtIDEpKTtcbiAgICAgIC8vIEFkZCBqaXR0ZXIgKDAuOFx1MjAxMzEuMngpIHRvIGF2b2lkIHRodW5kZXJpbmcgaGVyZFxuICAgICAgY29uc3Qgaml0dGVyID0gMC44ICsgKDAuNCAqIE1hdGgucmFuZG9tKCkpO1xuICAgICAgZGVsYXkgPSBNYXRoLmZsb29yKGRlbGF5ICogaml0dGVyKTtcbiAgICAgIC8vIFJlc3BlY3QgcmV0cnktYWZ0ZXIgaGVhZGVyIGlmIHByZXNlbnRcbiAgICAgIGlmIChlcnJvci5yZXRyeUFmdGVyKSB7XG4gICAgICAgIGRlbGF5ID0gTWF0aC5tYXgoZGVsYXksIGVycm9yLnJldHJ5QWZ0ZXIgKiAxMDAwKTtcbiAgICAgIH1cblxuICAgICAgY29uc29sZS5sb2coXG4gICAgICAgIGBbZW1iZWRkaW5nXSBcdTIzRjMgUmV0cnlhYmxlIGVycm9yICgke2Vycm9yPy5zdGF0dXMgfHwgJ3Vua25vd24nfSksIGAgK1xuICAgICAgICBgd2FpdGluZyAkeyhkZWxheSAvIDEwMDApLnRvRml4ZWQoMSl9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7TUFYX1JFVFJZX0FUVEVNUFRTfSkuLi5gXG4gICAgICApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIGRlbGF5KSk7XG5cbiAgICAgIC8vIEZJWDogYSByZXRyeSBpcyBhIGJyYW5kIG5ldyBBUEkgY2FsbCBhbmQgY29uc3VtZXMgcmVhbCBxdW90YSwgZXZlbiB0aG91Z2hcbiAgICAgIC8vIHRoZSBvcmlnaW5hbCBjYWxsIGZhaWxlZC4gU2tpcHBpbmcgY29uc3VtcHRpb24gaGVyZSAoYXMgYmVmb3JlKSBsZXQgdGhlIGxvY2FsXG4gICAgICAvLyBsaW1pdGVyIHVuZGVyLXJlcG9ydCBhY3R1YWwgdXNhZ2UgZHVyaW5nIGVycm9yIHN0b3Jtcywgd2hpY2ggbWVhbnQgaXQga2VwdFxuICAgICAgLy8gd2F2aW5nIHRocm91Z2ggbmV3IGdyb3VwcyB3aGlsZSByZXRyaWVzIHdlcmUgYWxzbyBoaXR0aW5nIHRoZSBBUEkgXHUyMDE0IG1ha2luZ1xuICAgICAgLy8gNDI5IHN0b3JtcyB3b3JzZSBpbnN0ZWFkIG9mIGJhY2tpbmcgb2ZmIGZyb20gdGhlbS5cbiAgICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpKTtcblxuICAgICAgcmV0dXJuIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlLCBhdHRlbXB0ICsgMSk7XG4gICAgfVxuXG4gICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGVycm9yLm1lc3NhZ2UgfHwgJ0JhdGNoIGVtYmVkZGluZyBmYWlsZWQnKTtcbiAgfVxufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDYuIEVYUE9SVEVEIGdlbmVyYXRlRW1iZWRkaW5ncyAod2l0aCByYXRlIGxpbWl0ZXIgJiBhY2N1cmF0ZSB0b2tlbnMpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZUVtYmVkZGluZ3MoY2h1bmtzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBvblByb2dyZXNzKSB7XG4gIGlmICghY2h1bmtzIHx8IGNodW5rcy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICBjb25zdCBiYXRjaFNpemUgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuXG4gIC8vIEZpeGVkLXNpemUgYXJyYXkgdG8gcHJlc2VydmUgY2hyb25vbG9naWNhbCBvcmRlclxuICBjb25zdCBlbWJlZGRpbmdzID0gbmV3IEFycmF5KGNodW5rcy5sZW5ndGgpO1xuXG4gIC8vIEdyb3VwIGNodW5rcyBpbnRvIGJhdGNoZXMgd2l0aCB0aGVpciBzdGFydGluZyBpbmRleFxuICBjb25zdCBiYXRjaGVzID0gW107XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBiYXRjaFNpemUpIHtcbiAgICBiYXRjaGVzLnB1c2goe1xuICAgICAgY2h1bmtzOiBjaHVua3Muc2xpY2UoaSwgaSArIGJhdGNoU2l6ZSksXG4gICAgICBzdGFydEluZGV4OiBpXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB0b3RhbEdyb3VwcyA9IE1hdGguY2VpbChiYXRjaGVzLmxlbmd0aCAvIHBhcmFsbGVsQ2FsbHMpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IHBhcmFsbGVsQmF0Y2hlcyA9IGJhdGNoZXMuc2xpY2UoaSwgaSArIHBhcmFsbGVsQ2FsbHMpO1xuICAgIGNvbnN0IGdyb3VwTnVtID0gTWF0aC5mbG9vcihpIC8gcGFyYWxsZWxDYWxscykgKyAxO1xuXG4gICAgLy8gQ2FsY3VsYXRlIGV4YWN0IHRva2VucyB1c2luZyBzdG9yZWQgdG9rZW5fY291bnQgKG9yIGZhbGxiYWNrKVxuICAgIGNvbnN0IGFsbENodW5rc0luR3JvdXAgPSBwYXJhbGxlbEJhdGNoZXMuZmxhdE1hcChiID0+IGIuY2h1bmtzKTtcbiAgICBjb25zdCB0b2tlbnNUb0NvbnN1bWUgPSBnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKGFsbENodW5rc0luR3JvdXApO1xuICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKHRva2Vuc1RvQ29uc3VtZSk7XG5cbiAgICBjb25zb2xlLmxvZyhcbiAgICAgIGBbZW1iZWRkaW5nXSBHcm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSBcdTIwMTQgZmlyaW5nICR7cGFyYWxsZWxCYXRjaGVzLmxlbmd0aH0gYmF0Y2hlcyBgICtcbiAgICAgIGBpbiBwYXJhbGxlbCAoJHt0b2tlbnNUb0NvbnN1bWV9IHRva2VucylgXG4gICAgKTtcblxuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBwYXJhbGxlbEJhdGNoZXMubWFwKGIgPT4gZW1iZWRCYXRjaChiLmNodW5rcy5tYXAoYyA9PiBjLnRleHQpLCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGN1cnJlbnRCYXRjaEluZm8gPSBwYXJhbGxlbEJhdGNoZXNbYmF0Y2hJZHhdO1xuICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgIGNvbnN0IHZlY3RvcnMgPSByZXN1bHQudmFsdWU7XG4gICAgICAgIGN1cnJlbnRCYXRjaEluZm8uY2h1bmtzLmZvckVhY2goKGNodW5rLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgIGNvbnN0IGdsb2JhbEluZGV4ID0gY3VycmVudEJhdGNoSW5mby5zdGFydEluZGV4ICsgY2h1bmtJZHg7XG4gICAgICAgICAgZW1iZWRkaW5nc1tnbG9iYWxJbmRleF0gPSB7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua18ke2dsb2JhbEluZGV4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH07XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGBbZW1iZWRkaW5nXSBCYXRjaCBzdGFydGluZyBhdCBpbmRleCAke2N1cnJlbnRCYXRjaEluZm8uc3RhcnRJbmRleH0gZmFpbGVkOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICBmYWlsZWRCYXRjaGVzLnB1c2goY3VycmVudEJhdGNoSW5mbyk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAob25Qcm9ncmVzcykge1xuICAgICAgb25Qcm9ncmVzcyh7IGN1cnJlbnRfYmF0Y2g6IGdyb3VwTnVtLCB0b3RhbF9iYXRjaGVzOiB0b3RhbEdyb3VwcyB9KTtcbiAgICB9XG5cbiAgICAvLyBSZXRyeSBmYWlsZWQgYmF0Y2hlcyBpbmRpdmlkdWFsbHlcbiAgICBmb3IgKGNvbnN0IGZhaWxlZEJhdGNoIG9mIGZhaWxlZEJhdGNoZXMpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBSZXRyeWluZyBmYWlsZWQgYmF0Y2ggZWxlbWVudHMgc3RhcnRpbmcgYXQgaW5kZXggJHtmYWlsZWRCYXRjaC5zdGFydEluZGV4fS4uLmApO1xuICAgICAgZm9yIChsZXQgY2h1bmtJZHggPSAwOyBjaHVua0lkeCA8IGZhaWxlZEJhdGNoLmNodW5rcy5sZW5ndGg7IGNodW5rSWR4KyspIHtcbiAgICAgICAgY29uc3QgY2h1bmsgPSBmYWlsZWRCYXRjaC5jaHVua3NbY2h1bmtJZHhdO1xuICAgICAgICBjb25zdCBnbG9iYWxJbmRleCA9IGZhaWxlZEJhdGNoLnN0YXJ0SW5kZXggKyBjaHVua0lkeDtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAvLyBGSVg6IHRoaXMgcmV0cnkgaXMgYSBmcmVzaCwgcmVhbCBBUEkgY2FsbCBcdTIwMTQgdHJhY2sgaXRzIHRva2VucyBhZ2FpbnN0XG4gICAgICAgICAgLy8gdGhlIGxpbWl0ZXIgaW5zdGVhZCBvZiBhc3N1bWluZyBpdCB3YXMgXCJhbHJlYWR5IHBhaWQgZm9yXCIuXG4gICAgICAgICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZ2V0VG9rZW5Db3VudEZvckNodW5rcyhbY2h1bmtdKSk7XG4gICAgICAgICAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW2NodW5rLnRleHRdLCB0YXNrVHlwZSk7XG4gICAgICAgICAgZW1iZWRkaW5nc1tnbG9iYWxJbmRleF0gPSB7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua19yZXRyeV8ke2dsb2JhbEluZGV4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbMF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gXHUyNzA1IFJldHJ5IHN1Y2NlZWRlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgZ2xvYmFsSW5kZXh9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFtlbWJlZGRpbmddIFx1Mjc0QyBSZXRyeSBmYWlsZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGdsb2JhbEluZGV4fTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvLyBGSVg6IHBlcm1hbmVudGx5LWZhaWxlZCBjaHVua3MgYXJlIGRyb3BwZWQgaGVyZSwgd2hpY2ggc2hpZnRzIGFycmF5IGluZGljZXNcbiAgLy8gcmVsYXRpdmUgdG8gdGhlIG9yaWdpbmFsIGBjaHVua3NgIGlucHV0LiBUaGlzIGxvZyBtYWtlcyB0aGF0IGxvc3MgdmlzaWJsZVxuICAvLyBpbnN0ZWFkIG9mIHNpbGVudDsgY2FsbGVycyB0aGF0IG5lZWQgdG8ga25vdyBleGFjdGx5IHdoaWNoIGNodW5rcyB3ZXJlIGxvc3RcbiAgLy8gY2FuIGNvbXBhcmUgcmV0dXJuZWQgYGlkYHMgYWdhaW5zdCB0aGVpciBvcmlnaW5hbCBjaHVuayBsaXN0LlxuICBjb25zdCBmYWlsZWRDb3VudCA9IGVtYmVkZGluZ3MuZmlsdGVyKGUgPT4gIWUpLmxlbmd0aDtcbiAgaWYgKGZhaWxlZENvdW50ID4gMCkge1xuICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gJHtmYWlsZWRDb3VudH0vJHtjaHVua3MubGVuZ3RofSBjaHVuayhzKSBwZXJtYW5lbnRseSBmYWlsZWQgdG8gZW1iZWQgYW5kIHdlcmUgZHJvcHBlZC5gKTtcbiAgfVxuXG4gIC8vIEZpbHRlciBvdXQgYW55IGVsZW1lbnRzIHRoYXQgcGVybWFuZW50bHkgZmFpbGVkXG4gIHJldHVybiBlbWJlZGRpbmdzLmZpbHRlcihCb29sZWFuKTtcbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA3LiBFWFBPUlRFRCBlbWJlZFF1ZXJ5XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFF1ZXJ5KHF1ZXJ5KSB7XG4gIC8vIEZJWDogdGhpcyBjYWxsIHdhcyBieXBhc3NpbmcgdGhlIHJhdGUgbGltaXRlciBlbnRpcmVseS4gSWYgaXQgcnVucyBjb25jdXJyZW50bHlcbiAgLy8gd2l0aCBkb2N1bWVudCBpbmdlc3Rpb24gKGUuZy4gYSB1c2VyIHNlYXJjaGVzIHdoaWxlIGEgYmF0Y2ggam9iIGlzIGluIGZsaWdodCksXG4gIC8vIGl0IGNvdWxkIHB1c2ggdG90YWwgdXNhZ2Ugb3ZlciB0aGUgY29uZmlndXJlZCBUUE0gYnVkZ2V0IHVubm90aWNlZC5cbiAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyhbcXVlcnldKSk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtxdWVyeV0sICdSRVRSSUVWQUxfUVVFUlknKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAodGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcpIHtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgJHt0ZXh0cy5sZW5ndGh9IHRleHRzLCB0YXNrVHlwZT0ke3Rhc2tUeXBlfWApO1xuICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSk7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0IGdvdCAke3ZlY3RvcnMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gIHJldHVybiB2ZWN0b3JzO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQge1xuICBnZXRHbG9iYWxDb2xsZWN0aW9uLFxuICBnZXRTZXNzaW9uQ29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50cyxcbiAgYWRkVmVjdG9yc1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGlkID0gc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICBkZWxldGVkRG9jdW1lbnRJZHM6IG5ldyBTZXQoKSxcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcbiAgc2Vzc2lvbnMuc2V0KGlkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHJldHVybiBleGlzdGluZztcbiAgICByZXR1cm4gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgQ2hlY2sgaWYgc2Vzc2lvbiBpcyBzZWVkZWQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uU2VlZGVkKHNlc3Npb25JZCkge1xuICByZXR1cm4gc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCk7XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBOb3RpZnkgU1NFIGxpc3RlbmVycyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmZ1bmN0aW9uIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpIHtcbiAgaWYgKGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzICYmIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmhhcyhgc2VlZGluZzoke3Nlc3Npb25JZH1gKSkge1xuICAgIGNvbnN0IGV2ZW50S2V5ID0gYHNlZWRpbmc6JHtzZXNzaW9uSWR9YDtcbiAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgIGxpc3RlbmVycy5mb3JFYWNoKChyZXNwb25zZSkgPT4ge1xuICAgICAgdHJ5IHtcbiAgICAgICAgcmVzcG9uc2Uud3JpdGUoYGV2ZW50OiBzZWVkaW5nX2NvbXBsZXRlXFxuZGF0YTogJHtKU09OLnN0cmluZ2lmeSh7IHNlc3Npb25JZCwgc2VlZGVkOiB0cnVlIH0pfVxcblxcbmApO1xuICAgICAgICByZXNwb25zZS5lbmQoKTtcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBjb25zb2xlLmVycm9yKGBbbm90aWZ5XSBGYWlsZWQgdG8gbm90aWZ5IGxpc3RlbmVyOmAsIGVyci5tZXNzYWdlKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5kZWxldGUoZXZlbnRLZXkpO1xuICAgIGNvbnNvbGUubG9nKGBbbm90aWZ5XSBOb3RpZmllZCAke2xpc3RlbmVycy5sZW5ndGh9IFNTRSBsaXN0ZW5lcnMgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gIH1cbn1cblxuLyoqXG4gKiBPbiBzZXNzaW9uIHN0YXJ0OlxuICogLSBJZiBjb2xsZWN0aW9uIGlzIE5FVyBcdTIxOTIgc2VlZCBmcm9tIGdsb2JhbCAocGFnaW5hdGVkLCAzMDAvYmF0Y2gpXG4gKiAtIElmIGNvbGxlY3Rpb24gRVhJU1RTIFx1MjE5MiBza2lwIHNlZWQsIHJlY29uc3RydWN0IGluLW1lbW9yeSBkb2MgbGlzdCBmcm9tIENocm9tYVxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpIHtcbiAgY29uc29sZS5sb2coYFx1RDgzRFx1REQxMSBTZXNzaW9uIGluaXQ6ICR7c2Vzc2lvbklkfWApO1xuICBpZiAoc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIEFscmVhZHkgc2VlZGVkICR7c2Vzc2lvbklkfSwgc2tpcHBpbmdgKTtcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgICByZXR1cm47XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gICAgY29uc3QgeyBjb2xsZWN0aW9uOiBzZXNzaW9uQ29sbGVjdGlvbiwgaXNOZXcgfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG5cbiAgICBpZiAoIWlzTmV3KSB7XG4gICAgICBjb25zb2xlLmxvZyhgXHUyNjdCXHVGRTBGICBTZXNzaW9uIGV4aXN0cywgcmVjb25zdHJ1Y3RpbmcgZG9jdW1lbnQgbGlzdCBmcm9tIENocm9tYS4uLmApO1xuICAgICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICAgIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgICBjb25zdCBkb2NzID0gYXdhaXQgbGlzdERvY3VtZW50cyhzZXNzaW9uQ29sbGVjdGlvbik7XG4gICAgICAgIGRvY3MuZm9yRWFjaChkb2MgPT4ge1xuICAgICAgICAgIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGRvYy5kb2N1bWVudF9pZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgICAgICAgICBmaWxlU2l6ZTogbnVsbCxcbiAgICAgICAgICAgIHBhZ2VDb3VudDogZG9jLnBhZ2VfY291bnQgfHwgbnVsbCxcbiAgICAgICAgICAgIGNodW5rQ291bnQ6IGRvYy5jaHVua19jb3VudCxcbiAgICAgICAgICAgIHNvdXJjZVR5cGU6IGRvYy5zb3VyY2VfdHlwZSxcbiAgICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogZG9jLnVwbG9hZF90aW1lc3RhbXBcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgUmVjb25zdHJ1Y3RlZCAke2RvY3MubGVuZ3RofSBkb2N1bWVudChzKSBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cbiAgICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFx1RDgzQ1x1REYzMSBOZXcgc2Vzc2lvbiBcdTIwMTQgc2VlZGluZyBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uLi4uYCk7XG5cbiAgICBjb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdLCBhbGxFbWJlZGRpbmdzID0gW10sIGFsbERvY3VtZW50cyA9IFtdLCBhbGxNZXRhZGF0YXMgPSBbXTtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGdsb2JhbENvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgaW5jbHVkZTogWydlbWJlZGRpbmdzJywgJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG4gICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uYmF0Y2guZW1iZWRkaW5ncyk7XG4gICAgICBhbGxEb2N1bWVudHMucHVzaCguLi5iYXRjaC5kb2N1bWVudHMpO1xuICAgICAgYWxsTWV0YWRhdGFzLnB1c2goLi4uYmF0Y2gubWV0YWRhdGFzKTtcbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc29sZS5sb2coJ1x1MjZBMFx1RkUwRiAgR2xvYmFsIGNvbGxlY3Rpb24gaXMgZW1wdHkgXHUyMDE0IG5vdGhpbmcgdG8gc2VlZC4nKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhbGxJZHMubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIHtcbiAgICAgIGF3YWl0IHNlc3Npb25Db2xsZWN0aW9uLmFkZCh7XG4gICAgICAgIGlkczogYWxsSWRzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSxcbiAgICAgICAgZW1iZWRkaW5nczogYWxsRW1iZWRkaW5ncy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSksXG4gICAgICAgIGRvY3VtZW50czogYWxsRG9jdW1lbnRzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSxcbiAgICAgICAgbWV0YWRhdGFzOiBhbGxNZXRhZGF0YXMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcChtID0+ICh7IC4uLm0sIHNvdXJjZV90eXBlOiAnZ2xvYmFsJyB9KSlcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYCAgXHVEODNEXHVEQ0U2IEFkZGVkIGJhdGNoICR7TWF0aC5mbG9vcihpIC8gQkFUQ0hfU0laRSkgKyAxfTogcmVjb3JkcyAke2kgKyAxfVx1MjAxMyR7TWF0aC5taW4oaSArIEJBVENIX1NJWkUsIGFsbElkcy5sZW5ndGgpfWApO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2VlZGVkICR7YWxsSWRzLmxlbmd0aH0gdmVjdG9ycyBpbnRvIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG5cbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChzZXNzaW9uKSB7XG4gICAgICBjb25zdCBkb2NzTWFwID0gbmV3IE1hcCgpO1xuICAgICAgYWxsTWV0YWRhdGFzLmZvckVhY2gobWV0YSA9PiB7XG4gICAgICAgIGlmICghZG9jc01hcC5oYXMobWV0YS5kb2N1bWVudF9pZCkpIHtcbiAgICAgICAgICBkb2NzTWFwLnNldChtZXRhLmRvY3VtZW50X2lkLCB7XG4gICAgICAgICAgICBpZDogbWV0YS5kb2N1bWVudF9pZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiBtZXRhLmZpbGVuYW1lLFxuICAgICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgICBwYWdlQ291bnQ6IG1ldGEudG90YWxfcGFnZXMgfHwgbnVsbCxcbiAgICAgICAgICAgIGNodW5rQ291bnQ6IDAsXG4gICAgICAgICAgICBzb3VyY2VUeXBlOiAnZ2xvYmFsJyxcbiAgICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgZG9jc01hcC5nZXQobWV0YS5kb2N1bWVudF9pZCkuY2h1bmtDb3VudCsrO1xuICAgICAgfSk7XG5cbiAgICAgIGZvciAoY29uc3QgZG9jIG9mIGRvY3NNYXAudmFsdWVzKCkpIHtcbiAgICAgICAgaWYgKCFzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5pZCA9PT0gZG9jLmlkKSkge1xuICAgICAgICAgIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goZG9jKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgXHUyNzRDIEZhaWxlZCB0byBzZWVkIHNlc3Npb24gJHtzZXNzaW9uSWR9OmAsIGVycm9yLm1lc3NhZ2UpO1xuICAgIC8vIFN0aWxsIG5vdGlmeSBsaXN0ZW5lcnMgc28gdGhleSBkb24ndCBoYW5nIGZvcmV2ZXJcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgRG9jdW1lbnQgbWFuYWdlbWVudCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBmdW5jdGlvbiBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SW5mbykge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcblxuICBjb25zdCBleGlzdGluZyA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmQoZCA9PiBkLmlkID09PSBkb2N1bWVudEluZm8uaWQpO1xuXG4gIGlmIChleGlzdGluZykge1xuICAgIGlmIChkb2N1bWVudEluZm8uY2h1bmtDb3VudCAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5jaHVua0NvdW50ID0gZG9jdW1lbnRJbmZvLmNodW5rQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5wYWdlQ291bnQgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcucGFnZUNvdW50ID0gZG9jdW1lbnRJbmZvLnBhZ2VDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVTaXplICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVTaXplID0gZG9jdW1lbnRJbmZvLmZpbGVTaXplO1xuICAgIGlmIChkb2N1bWVudEluZm8uc3RhdHVzICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnN0YXR1cyA9IGRvY3VtZW50SW5mby5zdGF0dXM7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlbmFtZSAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlbmFtZSA9IGRvY3VtZW50SW5mby5maWxlbmFtZTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBVcGRhdGVkIGRvYyAke2RvY3VtZW50SW5mby5pZH0gXHUyMDE0IHN0YXR1cz0ke2V4aXN0aW5nLnN0YXR1c30sIGNodW5rcz0ke2V4aXN0aW5nLmNodW5rQ291bnR9YCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICBpZDogZG9jdW1lbnRJbmZvLmlkLFxuICAgIGZpbGVuYW1lOiBkb2N1bWVudEluZm8uZmlsZW5hbWUsXG4gICAgZmlsZVNpemU6IGRvY3VtZW50SW5mby5maWxlU2l6ZSxcbiAgICBwYWdlQ291bnQ6IGRvY3VtZW50SW5mby5wYWdlQ291bnQsXG4gICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIGNodW5rQ291bnQ6IGRvY3VtZW50SW5mby5jaHVua0NvdW50ID8/IDAsXG4gICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICBzdGF0dXM6IGRvY3VtZW50SW5mby5zdGF0dXMgPz8gJ2luZGV4aW5nJ1xuICB9KTtcbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIEFkZGVkIGRvYyAke2RvY3VtZW50SW5mby5pZH0gXHUyMDE0IHN0YXR1cz0ke2RvY3VtZW50SW5mby5zdGF0dXMgPz8gJ2luZGV4aW5nJ31gKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiAnU2Vzc2lvbiBub3QgZm91bmQnIH07XG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgaWYgKHVwbG9hZGVkQ291bnQgPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246IGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gIH07XG4gIH1cbiAgcmV0dXJuIHsgY2FuVXBsb2FkOiB0cnVlIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVVwbG9hZChzZXNzaW9uSWQsIGZpbGUsIGZpbGVuYW1lKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGNvbnN0IGVycm9ycyA9IFtdO1xuXG4gIGlmIChmaWxlLnNpemUgPiBNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIGV4Y2VlZHMgJHtNQVhfVVBMT0FEX1NJWkVfTUJ9TUIgbGltaXRgKTtcbiAgfVxuXG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uXG4gICAgPyBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aFxuICAgIDogMDtcblxuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIGVycm9ycy5wdXNoKGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gKTtcbiAgfVxuXG4gIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBmaWxlbmFtZSkpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCk7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGlzVmFsaWQ6IGVycm9ycy5sZW5ndGggPT09IDAsXG4gICAgZXJyb3JzLFxuICAgIGlzTGFyZ2VGaWxlOiBmaWxlLnNpemUgPiAoTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQgKiAwLjYpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcbiAgY29uc3QgaWR4ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZEluZGV4KGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJZCk7XG4gIGlmIChpZHggPj0gMCkge1xuICAgIHNlc3Npb24uZG9jdW1lbnRzLnNwbGljZShpZHgsIDEpO1xuICAgIHNlc3Npb24uZGVsZXRlZERvY3VtZW50SWRzLmFkZChkb2N1bWVudElkKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBSZW1vdmVkIGRvYyAke2RvY3VtZW50SWR9LCBhZGRlZCB0byBkZWxldGVkRG9jdW1lbnRJZHNgKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIHJldHVybiBzZXNzaW9uPy5kZWxldGVkRG9jdW1lbnRJZHMgPz8gbmV3IFNldCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gW107XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBzZXNzaW9uRG9jdW1lbnRzOiBbXSwgZ2xvYmFsRG9jdW1lbnRzOiBbXSB9O1xuXG4gIGNvbnN0IG5vcm1hbGl6ZSA9IChkb2MpID0+ICh7XG4gICAgZG9jdW1lbnRfaWQ6IGRvYy5pZCxcbiAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgIGNodW5rX2NvdW50OiBkb2MuY2h1bmtDb3VudCA/PyAwLFxuICAgIHBhZ2VfY291bnQ6IGRvYy5wYWdlQ291bnQgPz8gMCxcbiAgICB1cGxvYWRfdGltZXN0YW1wOiBkb2MudXBsb2FkVGltZXN0YW1wIHx8IG51bGwsXG4gICAgc291cmNlX3R5cGU6IGRvYy5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ3Nlc3Npb25fdXBsb2FkJyA6ICdzZWVkJyxcbiAgICBmaWxlU2l6ZTogZG9jLmZpbGVTaXplIHx8IG51bGwsXG4gICAgc3RhdHVzOiBkb2Muc3RhdHVzID8/IG51bGxcbiAgfSk7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uLmRvY3VtZW50c1xuICAgICAgLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJylcbiAgICAgIC5tYXAobm9ybWFsaXplKSxcbiAgICBnbG9iYWxEb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnZ2xvYmFsJylcbiAgICAgIC5tYXAobm9ybWFsaXplKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtpbXBvcnQgeyBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgaHlicmlkUXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDIwO1xuY29uc3QgUkVGVVNBTF9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LlJFRlVTQUxfVEhSRVNIT0xEKSB8fCAwLjA1O1xuXG5jb25zdCBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMgPSBuZXcgTWFwKCk7XG5cbmFzeW5jIGZ1bmN0aW9uIGdldE9yQ2FjaGVTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpIHtcbiAgaWYgKGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIHJldHVybiBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIH1cbiAgdHJ5IHtcbiAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGNvbGxlY3Rpb24pIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IDUpIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4geyBjb25maWRlbmNlOiAwLCB0b3BTY29yZTogMCB9O1xuICBjb25zdCBzY29yZXMgPSByZXN1bHRzLnNsaWNlKDAsIHRvcEspLm1hcChyID0+IE1hdGgubWF4KDAsIHIuc2NvcmUpKTtcbiAgY29uc3QgYXZnU2NvcmUgPSBzY29yZXMucmVkdWNlKChhLCBiKSA9PiBhICsgYiwgMCkgLyBzY29yZXMubGVuZ3RoO1xuICByZXR1cm4ge1xuICAgIGNvbmZpZGVuY2U6IE1hdGgucm91bmQoYXZnU2NvcmUgKiAxMDApLFxuICAgIHRvcFNjb3JlOiBNYXRoLm1heCguLi5zY29yZXMpXG4gIH07XG59XG5cbi8vIFx1MjUwMFx1MjUwMCBNYWluIHJldHJpZXZhbCBmdW5jdGlvbiAoSHlicmlkOiBkZW5zZSArIEJNMjUgdmlhIENocm9tYSBSUkYpIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgNTtcblxuICB0cnkge1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgc2Vzc2lvbkNvbGxlY3Rpb25dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBzZXNzaW9uSWQgPyBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSA6IFByb21pc2UucmVzb2x2ZShudWxsKVxuICAgIF0pO1xuXG4gICAgaWYgKCFzZXNzaW9uQ29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIHNlc3Npb24gY29sbGVjdGlvbiBmb3VuZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcgfTtcbiAgICB9XG5cbiAgICBjb25zdCByYXdSZXN1bHRzID0gYXdhaXQgaHlicmlkUXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeSwgcXVlcnlFbWJlZGRpbmcsIHRvcEspO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBTY29yZXM6JywgcmVzdWx0cy5tYXAociA9PiByLnNjb3JlLnRvRml4ZWQoNCkpKTtcblxuICAgIHJldHVybiB7XG4gICAgICByZXN1bHRzLFxuICAgICAgY292ZXJhZ2U6IHsgLi4uY292ZXJhZ2UsIGxldmVsLCBzY29yZTogdG9wU2NvcmUgfSxcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nXG4gICAgfTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1JldHJpZXZhbCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCkge1xuICBjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMsIG1heFRva2VucyA9IDcwMDApIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgbGV0IHRvdGFsVG9rZW5zID0gMDtcbiAgY29uc3QgY29udGV4dFBhcnRzID0gW107XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCByZXN1bHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gcmVzdWx0c1tpXTtcbiAgICBjb25zdCB0b2tlbkVzdGltYXRlID0gcmVzdWx0LnRleHQubGVuZ3RoIC8gNDtcbiAgICBpZiAodG90YWxUb2tlbnMgKyB0b2tlbkVzdGltYXRlID4gbWF4VG9rZW5zKSBicmVhaztcbiAgICB0b3RhbFRva2VucyArPSB0b2tlbkVzdGltYXRlO1xuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnZ2xvYmFsJyA/ICdbU2VlZCBEb2N1bWVudF0nIDogJ1tTZXNzaW9uIFVwbG9hZF0nO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG4gICAgY29udGV4dFBhcnRzLnB1c2goYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQuc2xpY2UoMCwgMjAwKSArIChyZXN1bHQudGV4dC5sZW5ndGggPiAyMDAgPyAnLi4uJyA6ICcnKSxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hvdWxkU2hvd1JlZnVzYWwoY292ZXJhZ2UpIHtcbiAgcmV0dXJuIGNvdmVyYWdlLnRvcFNjb3JlIDwgUkVGVVNBTF9USFJFU0hPTEQ7XG59XG5cbmV4cG9ydCB7IGNhbGN1bGF0ZUNvdmVyYWdlIH07XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBtZW1vcnlNYXAgPSBuZXcgTWFwKCk7XG5jb25zdCBERUZBVUxUX01FTU9SWV9XSU5ET1cgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCAxMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIGlmICghbWVtb3J5TWFwLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgbWVtb3J5TWFwLnNldChzZXNzaW9uSWQsIHtcbiAgICAgIHR1cm5zOiBbXSxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKVxuICAgIH0pO1xuICB9XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgbWV0YWRhdGEgPSB7fSkge1xuICBjb25zdCBtZW1vcnkgPSBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBtYXhUdXJucyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcblxuICBjb25zdCB0dXJuID0ge1xuICAgIGlkOiBgdHVybl8ke0RhdGUubm93KCl9XyR7TWF0aC5yYW5kb20oKS50b1N0cmluZygzNikuc3Vic3RyKDIsIDkpfWAsXG4gICAgcm9sZSxcbiAgICBjb250ZW50LFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICAuLi5tZXRhZGF0YVxuICB9O1xuXG4gIG1lbW9yeS50dXJucy5wdXNoKHR1cm4pO1xuXG4gIGlmIChtZW1vcnkudHVybnMubGVuZ3RoID4gbWF4VHVybnMpIHtcbiAgICBtZW1vcnkudHVybnMgPSBtZW1vcnkudHVybnMuc2xpY2UoLW1heFR1cm5zKTtcbiAgfVxuXG4gIHJldHVybiB0dXJuO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5KHNlc3Npb25JZCkge1xuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgbWF4VHVybnMgPSBudWxsKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBsaW1pdCA9IG1heFR1cm5zIHx8IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcbiAgcmV0dXJuIG1lbW9yeS50dXJucy5zbGljZSgtbGltaXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q29udmVyc2F0aW9uQ29udGV4dChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICByZXR1cm4gdHVybnMubWFwKHQgPT4gKHtcbiAgICByb2xlOiB0LnJvbGUsXG4gICAgY29udGVudDogdC5jb250ZW50XG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICBpZiAodHVybnMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+IHtcbiAgICBjb25zdCBwcmVmaXggPSB0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyOicgOiAnQXNzaXN0YW50Oic7XG4gICAgcmV0dXJuIGAke3ByZWZpeH0gJHt0LmNvbnRlbnR9YDtcbiAgfSkuam9pbignXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhck1lbW9yeShzZXNzaW9uSWQpIHtcbiAgbWVtb3J5TWFwLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5U3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIHR1cm5Db3VudDogbWVtb3J5LnR1cm5zLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IG1lbW9yeS5jcmVhdGVkQXQsXG4gICAgbGFzdFR1cm5BdDogbWVtb3J5LnR1cm5zLmxlbmd0aCA+IDAgPyBtZW1vcnkudHVybnNbbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDFdLnRpbWVzdGFtcCA6IG51bGxcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgY2l0YXRpb25zID0gW10sIGNvdmVyYWdlID0gbnVsbCwgYW5zd2VySWQgPSBudWxsKSB7XG4gIHJldHVybiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwge1xuICAgIC4uLihhbnN3ZXJJZCAmJiB7IGlkOiBhbnN3ZXJJZCB9KSxcbiAgICBjaXRhdGlvbnMsXG4gICAgY292ZXJhZ2UsXG4gICAgaGFzQ2l0YXRpb25zOiBjaXRhdGlvbnMubGVuZ3RoID4gMFxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RVc2VyTWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICd1c2VyJykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RBc3Npc3RhbnRNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgbXVsdGVyIGZyb20gJ211bHRlcic7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCBmcyBmcm9tICdmcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuaW1wb3J0IHBkZiBmcm9tICdwZGYtcGFyc2UnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG5pbXBvcnQgeyBzYW5pdGl6ZUZpbGVuYW1lIH0gZnJvbSAnLi4vdXRpbHMvc2FuaXRpemUuanMnO1xuaW1wb3J0IHtcbiAgQ29ycnVwdGVkUERGRXJyb3IsXG4gIEludmFsaWRGaWxlVHlwZUVycm9yLFxufSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuaW1wb3J0IHsgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbmltcG9ydCB7IGVtYmVkU2luZ2xlQmF0Y2hHcm91cCB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHtcbiAgZ2V0T3JDcmVhdGVTZXNzaW9uLFxuICBhZGREb2N1bWVudFRvU2Vzc2lvbixcbiAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbixcbiAgZ2V0QWxsRG9jdW1lbnRzLFxuICBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzLFxuICBpc1Nlc3Npb25TZWVkZWRcbn0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNsZWFyTWVtb3J5IH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG5jb25zdCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL3NlZWRfZG9jdW1lbnRzJyk7XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBTU0UgZXZlbnQgaGVscGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZnVuY3Rpb24gc3NlRXZlbnQocmVzLCBldmVudCwgZGF0YSkge1xuICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG59XG5cbmNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHVwbG9hZERpciksXG4gIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG59KTtcblxuY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgc3RvcmFnZSxcbiAgbGltaXRzOiB7IGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0IH0sXG4gIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICBjYihudWxsLCB0cnVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgIH1cbiAgfVxufSk7XG5cbmZ1bmN0aW9uIGNvbnRlbnREaXNwb3NpdGlvbihkaXNwbGF5TmFtZSkge1xuICBjb25zdCBlbmNvZGVkID0gZW5jb2RlVVJJQ29tcG9uZW50KGRpc3BsYXlOYW1lKVxuICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgIC5yZXBsYWNlKC9cXCgvZywgJyUyOCcpXG4gICAgLnJlcGxhY2UoL1xcKS9nLCAnJTI5Jyk7XG4gIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xufVxuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IHRjLml0ZW1zLm1hcChpID0+IGkuc3RyKS5qb2luKCcgJyk7XG4gICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgIGNvbnN0IHBhZ2VNYXAgPSBbXTtcbiAgICBsZXQgY2hhclBvcyA9IDA7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICB9XG5cbiAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRQYWdlTnVtYmVyKGNoYXJTdGFydCwgcGFnZU1hcCkge1xuICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICBpZiAoY2hhclN0YXJ0ID49IGVudHJ5LnN0YXJ0ICYmIGNoYXJTdGFydCA8PSBlbnRyeS5lbmQpIHJldHVybiBlbnRyeS5wYWdlO1xuICB9XG4gIHJldHVybiBwYWdlTWFwW3BhZ2VNYXAubGVuZ3RoIC0gMV0/LnBhZ2UgfHwgMTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFVwbG9hZCBoYW5kbGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gIGNvbnN0IEJBVENIX1NJWkUgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgMTA7XG4gIGNvbnN0IFBBUkFMTEVMX0NBTExTID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCAxMDtcbiAgY29uc3QgR1JPVVBfV0FJVF9NUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19HUk9VUF9XQUlUX01TKSB8fCAxO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLmJvZHkuc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICAgIGNvbnN0IHNlc3Npb24gPSBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBtYXhQREZzID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04gfHwgJzMnKTtcbiAgICBjb25zdCBjbGVhbkZpbGVuYW1lID0gc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSk7XG5cbiAgICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGg7XG4gICAgaWYgKHVwbG9hZGVkQ291bnQgPj0gbWF4UERGcykge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBNYXhpbXVtICR7bWF4UERGc30gdXBsb2FkcyByZWFjaGVkYCwgY29kZTogJ1RPT19NQU5ZX1BERlMnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGNsZWFuRmlsZW5hbWUpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogYFwiJHtjbGVhbkZpbGVuYW1lfVwiIGFscmVhZHkgdXBsb2FkZWRgLCBjb2RlOiAnRFVQTElDQVRFX0ZJTEUnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMSBcdTIwMTQgcGFyc2luZyAke2NsZWFuRmlsZW5hbWV9ICgke2ZpbGUuc2l6ZX0gYnl0ZXMpYCk7XG4gICAgY29uc3QgeyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9ID0gYXdhaXQgcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZS5wYXRoKTtcblxuICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ05vIGV4dHJhY3RhYmxlIHRleHQgXHUyMDE0IFBERiBtYXkgYmUgc2Nhbm5lZCBvciBpbWFnZS1vbmx5JywgY29kZTogJ0VNUFRZX1BERicgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGNvbnN0IGRvY3VtZW50SWQgPSB1dWlkdjQoKTtcbiAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQpO1xuXG4gICAgaWYgKHJhd0NodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc3QgY2h1bmtzID0gcmF3Q2h1bmtzLm1hcCgoY2h1bmssIGlkeCkgPT4gKHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjcmVhdGVIYXNoKCdtZDUnKS51cGRhdGUoYCR7Y2xlYW5GaWxlbmFtZX06OiR7Y2h1bmsudGV4dH1gKS5kaWdlc3QoJ2hleCcpLnNsaWNlKDAsIDE2KSxcbiAgICAgICAgY2h1bmtfaW5kZXg6IGlkeCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiByYXdDaHVua3MubGVuZ3RoLFxuICAgICAgICBwYWdlX251bWJlcjogZ2V0UGFnZU51bWJlcihjaHVuay5jaGFyU3RhcnQsIHBhZ2VNYXApLFxuICAgICAgICB0b3RhbF9wYWdlczogdG90YWxQYWdlcyxcbiAgICAgICAgc291cmNlX3R5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9KSk7XG5cbiAgICBjb25zdCB0b3RhbENodW5rcyA9IGNodW5rcy5sZW5ndGg7XG4gICAgY29uc3QgdG90YWxCYXRjaGVzID0gTWF0aC5jZWlsKHRvdGFsQ2h1bmtzIC8gQkFUQ0hfU0laRSk7XG4gICAgY29uc3QgdG90YWxTZXRzID0gTWF0aC5jZWlsKHRvdGFsQmF0Y2hlcyAvIFBBUkFMTEVMX0NBTExTKTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAke3RvdGFsQ2h1bmtzfSBjaHVua3MgXHUyMTkyICR7dG90YWxCYXRjaGVzfSBBUEkgY2FsbHMgXHUyMTkyICR7dG90YWxTZXRzfSBzZXRzIG9mICR7UEFSQUxMRUxfQ0FMTFN9IHBhcmFsbGVsYCk7XG5cbiAgICBzc2VFdmVudChyZXMsICd1cGxvYWRfY29tcGxldGUnLCB7XG4gICAgICBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgdG90YWxDaHVua3MsIHRvdGFsQmF0Y2hlcywgdG90YWxTZXRzXG4gICAgfSk7XG5cbiAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogMCwgc3RhdHVzOiAnaW5kZXhpbmcnXG4gICAgfSk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMSBkb25lIFx1MjAxNCAke2NsZWFuRmlsZW5hbWV9IGFkZGVkIHRvIHNlc3Npb24gYXMgaW5kZXhpbmdgKTtcblxuICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICBsZXQgcHJvY2Vzc2VkQ2h1bmtzID0gMDtcbiAgICBjb25zdCBhbGxFbWJlZGRpbmdzID0gW107XG5cbiAgICBjb25zdCBiYXRjaGVzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpKTtcblxuICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IFBBUkFMTEVMX0NBTExTKSBzZXRzLnB1c2goYmF0Y2hlcy5zbGljZShpLCBpICsgUEFSQUxMRUxfQ0FMTFMpKTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAyIHN0YXJ0IFx1MjAxNCAke3NldHMubGVuZ3RofSBzZXRzYCk7XG5cbiAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgIGNvbnN0IGlzTGFzdFNldCA9IHNldElkeCA9PT0gc2V0cy5sZW5ndGggLSAxO1xuICAgICAgY29uc3QgY3VycmVudFNldCA9IHNldHNbc2V0SWR4XTtcbiAgICAgIGNvbnN0IHNldENodW5rQ291bnQgPSBjdXJyZW50U2V0LnJlZHVjZSgoYWNjLCBiKSA9PiBhY2MgKyBiLmxlbmd0aCwgMCk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTZXQgJHtzZXRJZHggKyAxfS8ke3NldHMubGVuZ3RofSBcdTIwMTQgZW1iZWRkaW5nICR7Y3VycmVudFNldC5sZW5ndGh9IGJhdGNoIGNhbGwocykgKCR7c2V0Q2h1bmtDb3VudH0gY2h1bmtzKSBpbiBwYXJhbGxlbGApO1xuXG4gICAgICBjb25zdCBlbWJlZFJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICAgIGN1cnJlbnRTZXQubWFwKGJhdGNoID0+IGVtYmVkU2luZ2xlQmF0Y2hHcm91cChiYXRjaC5tYXAoYyA9PiBjLnRleHQpKSlcbiAgICAgICk7XG5cbiAgICAgIGNvbnN0IHNldEVtYmVkZGluZ3MgPSBbXTtcbiAgICAgIGVtYmVkUmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGJhdGNoSWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IGJhdGNoID0gY3VycmVudFNldFtiYXRjaElkeF07XG4gICAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICAgIHJlc3VsdC52YWx1ZS5mb3JFYWNoKCh2ZWN0b3IsIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgICBpZDogYmF0Y2hbY2h1bmtJZHhdLm1ldGFkYXRhLmNodW5rX2lkLFxuICAgICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcixcbiAgICAgICAgICAgICAgbWV0YWRhdGE6IGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YSxcbiAgICAgICAgICAgICAgdGV4dDogYmF0Y2hbY2h1bmtJZHhdLnRleHRcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IGVtYmVkZGVkIE9LICgke2JhdGNoLmxlbmd0aH0gY2h1bmtzKWApO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gRkFJTEVEOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgcHJvY2Vzc2VkQ2h1bmtzICs9IHNldEVtYmVkZGluZ3MubGVuZ3RoO1xuICAgICAgYWxsRW1iZWRkaW5ncy5wdXNoKC4uLnNldEVtYmVkZGluZ3MpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0gZW1iZWRkZWQgXHUyMDE0ICR7cHJvY2Vzc2VkQ2h1bmtzfS8ke3RvdGFsQ2h1bmtzfSBjaHVua3Mgc28gZmFyYCk7XG5cbiAgICAgIGlmICghaXNMYXN0U2V0KSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTdGFydGluZyAke0dST1VQX1dBSVRfTVMgLyAxMDAwfXMgdGltZXIgKyBDaHJvbWEgd3JpdGUgY29uY3VycmVudGx5IGZvciBzZXQgJHtzZXRJZHggKyAxfWApO1xuICAgICAgICBjb25zdCB0aW1lciA9IG5ldyBQcm9taXNlKHIgPT4gc2V0VGltZW91dChyLCBHUk9VUF9XQUlUX01TKSk7XG4gICAgICAgIGNvbnN0IGNocm9tYVdyaXRlID0gYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApLnRoZW4oKCkgPT4gY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBkb25lIGZvciBzZXQgJHtzZXRJZHggKyAxfSAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKSlcbiAgICAgICAgICAuY2F0Y2goZXJyID0+IGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBGQUlMRUQgZm9yIHNldCAke3NldElkeCArIDF9OmAsIGVyci5tZXNzYWdlKSk7XG5cbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZW1iZWRkaW5nX3Byb2dyZXNzJywge1xuICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgc2V0SW5kZXg6IHNldElkeCArIDEsIHRvdGFsU2V0cyxcbiAgICAgICAgICB3YWl0aW5nTXM6IEdST1VQX1dBSVRfTVMsIGNocm9tYVdyaXRlQ29tcGxldGU6IGZhbHNlXG4gICAgICAgIH0pO1xuXG4gICAgICAgIGF3YWl0IFByb21pc2UuYWxsKFt0aW1lciwgY2hyb21hV3JpdGVdKTtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFRpbWVyICsgQ2hyb21hIGJvdGggZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0sIHByb2NlZWRpbmcgdG8gc2V0ICR7c2V0SWR4ICsgMn1gKTtcblxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIExhc3Qgc2V0ICR7c2V0SWR4ICsgMX0gXHUyMDE0IGF3YWl0aW5nIENocm9tYSB3cml0ZSBkaXJlY3RseWApO1xuICAgICAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgY29tcGxldGUgZm9yIGxhc3Qgc2V0ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiAwLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiB0cnVlXG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCk7XG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLCBzdGF0dXM6ICdyZWFkeSdcbiAgICB9KTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBcdTI3MDUgRG9uZSBcdTIwMTQgJHthbGxFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycyBpbiBDaHJvbWEgZm9yICR7Y2xlYW5GaWxlbmFtZX1gKTtcblxuICAgIHNzZUV2ZW50KHJlcywgJ2RvbmUnLCB7XG4gICAgICBkb2N1bWVudDoge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogYWxsRW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9LFxuICAgICAgc2Vzc2lvbklkXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgdHJ5IHsgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTsgfSBjYXRjaCB7IH1cbiAgICB9XG4gICAgY29uc29sZS5lcnJvcignW3VwbG9hZF0gVW5oYW5kbGVkIGVycm9yOicsIGVycm9yKTtcbiAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnVXBsb2FkIGZhaWxlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1VQTE9BRF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBTU0U6IFNlZWRpbmcgc3RhdHVzIHN0cmVhbSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBzZWVkaW5nU3RhdHVzSGFuZGxlcihyZXEsIHJlcykge1xuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGlmICghc2Vzc2lvbklkKSB7XG4gICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdNaXNzaW5nIHNlc3Npb24gSUQnLCBjb2RlOiAnTUlTU0lOR19TRVNTSU9OJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gQ2xpZW50IGNvbm5lY3RlZCBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcblxuICAvLyBDaGVjayBpZiBzZXNzaW9uIGlzIGFscmVhZHkgc2VlZGVkXG4gIGNvbnN0IHNlZWRlZCA9IGlzU2Vzc2lvblNlZWRlZChzZXNzaW9uSWQpO1xuICBpZiAoc2VlZGVkKSB7XG4gICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gU2Vzc2lvbiAke3Nlc3Npb25JZH0gYWxyZWFkeSBzZWVkZWQgXHUyMDEzIHJldHVybmluZyBpbW1lZGlhdGVseWApO1xuICAgIHNzZUV2ZW50KHJlcywgJ3NlZWRpbmdfY29tcGxldGUnLCB7IHNlc3Npb25JZCwgc2VlZGVkOiB0cnVlIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgICByZXR1cm47XG4gIH1cblxuICAvLyBDcmVhdGUgYSBsaXN0ZW5lciBmb3IgdGhpcyBzZXNzaW9uXG4gIGNvbnN0IGV2ZW50S2V5ID0gYHNlZWRpbmc6JHtzZXNzaW9uSWR9YDtcblxuICAvLyBTdG9yZSB0aGUgbGlzdGVuZXIgc28gd2UgY2FuIGVtaXQgd2hlbiBzZWVkaW5nIGNvbXBsZXRlc1xuICBpZiAoIWdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzKSB7XG4gICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMgPSBuZXcgTWFwKCk7XG4gIH1cbiAgaWYgKCFnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5oYXMoZXZlbnRLZXkpKSB7XG4gICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuc2V0KGV2ZW50S2V5LCBbXSk7XG4gIH1cbiAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KS5wdXNoKHJlcyk7XG5cbiAgLy8gQ2xlYW4gdXAgbGlzdGVuZXIgb24gY2xpZW50IGRpc2Nvbm5lY3RcbiAgcmVxLm9uKCdjbG9zZScsICgpID0+IHtcbiAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgIGNvbnN0IGlkeCA9IGxpc3RlbmVycy5pbmRleE9mKHJlcyk7XG4gICAgaWYgKGlkeCA+PSAwKSB7XG4gICAgICBsaXN0ZW5lcnMuc3BsaWNlKGlkeCwgMSk7XG4gICAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBDbGllbnQgZGlzY29ubmVjdGVkIGZvciAke3Nlc3Npb25JZH1gKTtcbiAgICB9XG4gICAgaWYgKGxpc3RlbmVycy5sZW5ndGggPT09IDApIHtcbiAgICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgfVxuICB9KTtcblxuICAvLyBTdGFydCBzZWVkaW5nIGluIHRoZSBiYWNrZ3JvdW5kIChpZiBub3QgYWxyZWFkeSBydW5uaW5nKVxuICB0cnkge1xuICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIFRyaWdnZXJpbmcgc2VlZGluZyBmb3IgJHtzZXNzaW9uSWR9Li4uYCk7XG4gICAgYXdhaXQgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpO1xuICAgIC8vIFRoZSBzZWVkaW5nIGZ1bmN0aW9uIHdpbGwgbm90aWZ5IGxpc3RlbmVycyB3aGVuIGNvbXBsZXRlXG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUuZXJyb3IoYFtzZWVkaW5nLXN0YXR1c10gU2VlZGluZyBmYWlsZWQgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnIubWVzc2FnZSk7XG4gICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICBsaXN0ZW5lcnMuZm9yRWFjaCgocmVzcG9uc2UpID0+IHtcbiAgICAgIHNzZUV2ZW50KHJlc3BvbnNlLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGVyci5tZXNzYWdlLCBjb2RlOiAnU0VFRF9GQUlMRUQnIH0pO1xuICAgICAgcmVzcG9uc2UuZW5kKCk7XG4gICAgfSk7XG4gICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTGlzdCBkb2N1bWVudHMgaGFuZGxlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcbiAgdHJ5IHtcbiAgICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBkb2N1bWVudHMgPSBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0xpc3QgZG9jdW1lbnRzIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJywgY29kZTogJ0xJU1RfRVJST1InIH0pO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBEZWxldGUgZG9jdW1lbnQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnQocmVxLCByZXMpIHtcbiAgY29uc3QgeyBkb2N1bWVudElkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBmaWxlbmFtZSA9IHJlcS5xdWVyeS5maWxlbmFtZTtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICAgICAgYXdhaXQgZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpO1xuICAgICAgICB9XG4gICAgICB9IGNhdGNoIChjaHJvbWFFcnIpIHtcbiAgICAgICAgY29uc29sZS53YXJuKGBbZGVsZXRlXSBDaHJvbWEgZGVsZXRlIGZhaWxlZCBmb3IgJHtkb2N1bWVudElkfTpgLCBjaHJvbWFFcnIubWVzc2FnZSk7XG4gICAgICB9XG5cbiAgICAgIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKTtcblxuICAgICAgY2xlYXJNZW1vcnkoc2Vzc2lvbklkKTtcbiAgICAgIGNvbnNvbGUubG9nKGBbZGVsZXRlXSBDbGVhcmVkIG1lbW9yeSBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgICB9XG5cbiAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgIGNvbnN0IGZpbGVQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoZmlsZVBhdGgpKSB7XG4gICAgICAgIGZzLnVubGlua1N5bmMoZmlsZVBhdGgpO1xuICAgICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gUmVtb3ZlZCBmaWxlOiAke2ZpbGVQYXRofWApO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGBbZGVsZXRlXSBGaWxlIG5vdCBmb3VuZCBvbiBkaXNrOiAke2ZpbGVQYXRofWApO1xuICAgICAgfVxuICAgIH1cblxuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSwgZG9jdW1lbnRJZCB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdEZWxldGUgZG9jdW1lbnQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50JywgY29kZTogJ0RFTEVURV9FUlJPUicgfSk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEdldCBkb2N1bWVudCBmaWxlIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICBjb25zdCBmaWxlbmFtZSA9IHJlcS5xdWVyeS5maWxlbmFtZTtcblxuICB0cnkge1xuICAgIGlmIChmaWxlbmFtZSkge1xuICAgICAgY29uc3QgdXBsb2FkUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHVwbG9hZFBhdGgpKSB7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihmaWxlbmFtZSkpO1xuICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbSh1cGxvYWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHNlZWRQYXRoID0gcGF0aC5qb2luKHNlZWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWRQYXRoKSkge1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0oc2VlZFBhdGgpLnBpcGUocmVzKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZERpcikpIHtcbiAgICAgICAgY29uc3QgYWxsUGRmcyA9IGZzLnJlYWRkaXJTeW5jKHNlZWREaXIpLmZpbHRlcihmID0+IGYuZW5kc1dpdGgoJy5wZGYnKSk7XG4gICAgICAgIGNvbnN0IG1hdGNoID0gYWxsUGRmcy5maW5kKGYgPT4gZi5pbmNsdWRlcyhwYXRoLnBhcnNlKGZpbGVuYW1lKS5uYW1lKSk7XG4gICAgICAgIGlmIChtYXRjaCkge1xuICAgICAgICAgIGNvbnN0IG1hdGNoUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBtYXRjaCk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24obWF0Y2gpKTtcbiAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShtYXRjaFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgUm91dGVzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxucm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG5yb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xucm91dGVyLmdldCgnL3NlZWRpbmctc3RhdHVzJywgc2VlZGluZ1N0YXR1c0hhbmRsZXIpO1xucm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xucm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdDogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCcsXG4gICAgICBsb2NhdGlvbjogJ2dsb2JhbCdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIFBSSU1BUllfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWxOYW1lKCkge1xuICByZXR1cm4gRkFMTEJBQ0tfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tQ2h1bmsoY2h1bmspIHtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ3N0cmluZycpIHJldHVybiBjaHVuay50ZXh0O1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnZnVuY3Rpb24nKSByZXR1cm4gY2h1bmsudGV4dCgpO1xuICByZXR1cm4gJyc7XG59XG5cbmZ1bmN0aW9uIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWwsIHByb21wdCkge1xuICByZXR1cm4ge1xuICAgIG1vZGVsLFxuICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgY29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH1cbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1SZXNwb25zZShwcm9tcHQpIHtcbiAgbGV0IG1vZGVsTmFtZSA9IGdldFByaW1hcnlNb2RlbE5hbWUoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICBsZXQgZmlyc3RUb2tlblRpbWVvdXQgPSBudWxsO1xuICAgIGxldCByZXF1ZXN0VGltZW91dElkID0gbnVsbDtcbiAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgdHJ5IHtcbiAgICAgIHJlcXVlc3RUaW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICAgICAgY29uc3QgcmVzcG9uc2VTdHJlYW0gPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oXG4gICAgICAgIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWxOYW1lLCBwcm9tcHQpLFxuICAgICAgICB7IHNpZ25hbDogY29udHJvbGxlci5zaWduYWwgfVxuICAgICAgKTtcblxuICAgICAgaWYgKCFyZXNwb25zZVN0cmVhbSB8fCB0eXBlb2YgcmVzcG9uc2VTdHJlYW1bU3ltYm9sLmFzeW5jSXRlcmF0b3JdICE9PSAnZnVuY3Rpb24nKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgU3RyZWFtaW5nIHVuYXZhaWxhYmxlIGZvciBtb2RlbCAke21vZGVsTmFtZX1gKTtcbiAgICAgIH1cblxuICAgICAgbGV0IGZpcnN0VG9rZW4gPSB0cnVlO1xuICAgICAgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzcG9uc2VTdHJlYW0pIHtcbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ1N0cmVhbSBleGVjdXRpb24gYWJvcnRlZCBieSB0aW1lb3V0IGNvbnN0cmFpbnQuJyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCB0ZXh0ID0gZ2V0VGV4dEZyb21DaHVuayhjaHVuayk7XG4gICAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgICAgaWYgKGZpcnN0VG9rZW4pIHtcbiAgICAgICAgICAgIGZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBjbGVhclRpbWVvdXQocmVxdWVzdFRpbWVvdXRJZCk7XG4gICAgICByZXR1cm47XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgcmV0cmllcysrO1xuXG4gICAgICBpZiAoZmlyc3RUb2tlblRpbWVvdXQpIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBpZiAocmVxdWVzdFRpbWVvdXRJZCkgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuXG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgbW9kZWxOYW1lID0gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKTtcbiAgICB9XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgZm9ybWF0Q29udGV4dEZvclByb21wdCB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgc3RyZWFtUmVzcG9uc2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBnZXRSZWNlbnRUdXJucyB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBnZXREZWxldGVkRG9jdW1lbnRJZHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBPVVRfT0ZfU0NPUEVfUEFUVEVSTiA9IC9kb24ndCBoYXZlIGluZm9ybWF0aW9ufGRvIG5vdCBoYXZlIGluZm9ybWF0aW9ufG5vdCBpbiBteSBrbm93bGVkZ2V8Y2FuJ3QgZmluZHxjYW5ub3QgZmluZHxubyBpbmZvcm1hdGlvbnxrbm93bGVkZ2UgYmFzZSBkb2Vzbid0fG5vdCBjb3ZlcmVkfG91dHNpZGUuKmtub3dsZWRnZS9pO1xuXG5mdW5jdGlvbiBjbGVhbkV4Y2VycHQodGV4dCkge1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC8oPzwhXFx3KShbQS1aYS16XSlcXHMoW0EtWmEtel0pXFxzKFtBLVphLXpdKShcXHNbQS1aYS16XSkqL2csIChtYXRjaCkgPT5cbiAgICAgIG1hdGNoLnJlcGxhY2UoL1xccy9nLCAnJylcbiAgICApXG4gICAgLnJlcGxhY2UoL1xcc3syLH0vZywgJyAnKVxuICAgIC5yZXBsYWNlKC9eXFwqXFxzKi8sICcnKVxuICAgIC50cmltKCk7XG59XG5cbi8vIElzc3VlIDQgZml4OiByZW1vdmUgZG9tYWluSGludCBcdTIwMTQgc2hvcnQgcXVlcmllcyBubyBsb25nZXIgaW5oZXJpdCBwcmV2aW91cyBjb252ZXJzYXRpb24gY29udGV4dFxuZnVuY3Rpb24gZXhwYW5kUXVlcnkocXVlcnkpIHtcbiAgY29uc3Qgd29yZHMgPSBxdWVyeS50cmltKCkuc3BsaXQoL1xccysvKTtcbiAgaWYgKHdvcmRzLmxlbmd0aCA+IDQpIHJldHVybiBxdWVyeTtcblxuICBjb25zdCBleHBhbnNpb25zID0gW1xuICAgICdkZWZpbml0aW9uJywgJ292ZXJ2aWV3JywgJ3JvbGUnLCAncmVzcG9uc2liaWxpdGllcycsXG4gICAgJ2V4YW1wbGVzJywgJ2tleSBjb25jZXB0cycsICdob3cgaXQgd29ya3MnLCAncHVycG9zZSdcbiAgXTtcblxuICByZXR1cm4gYCR7cXVlcnl9ICR7ZXhwYW5zaW9ucy5qb2luKCcgJyl9YDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZUNoYXRTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSwgc2Vzc2lvbklkOiBwcm92aWRlZFNlc3Npb25JZCwgY29udklkOiBwcm92aWRlZENvbnZJZCB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJywgY29kZTogJ01JU1NJTkdfUVVFUlknIH0pO1xuICB9XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcHJvdmlkZWRTZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGNvbnZJZCAgICA9IHByb3ZpZGVkQ29udklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBhbnN3ZXJJZCAgPSB1dWlkdjQoKTtcblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLnNldEhlYWRlcigneC1zZXNzaW9uLWlkJywgc2Vzc2lvbklkKTtcbiAgcmVzLnNldEhlYWRlcigneC1hbnN3ZXItaWQnLCBhbnN3ZXJJZCk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5gKTtcbiAgICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH07XG5cbiAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCBleHBhbmRlZFF1ZXJ5ID0gZXhwYW5kUXVlcnkocXVlcnkpO1xuICAgIGNvbnN0IHsgcmVzdWx0cywgY292ZXJhZ2UgfSA9IGF3YWl0IHJldHJpZXZlRm9yUXVlcnkoZXhwYW5kZWRRdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgbGV2ZWw6IGNvdmVyYWdlLmxldmVsLFxuICAgICAgc2NvcmU6IGNvdmVyYWdlLnNjb3JlLFxuICAgICAgdG9wU2NvcmU6IGNvdmVyYWdlLnRvcFNjb3JlXG4gICAgfSk7XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBjb25zdCBzb3VyY2VzID0gcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgY2h1bmtJZDogci5pZCxcbiAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgIHBhZ2VOdW1iZXI6IHIubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgICBleGNlcnB0OiBjbGVhbkV4Y2VycHQoci50ZXh0LnNsaWNlKDAsIDIwMCkpLFxuICAgICAgc2NvcmU6IHIuc2NvcmUsXG4gICAgICBzb3VyY2VUeXBlOiByLnNvdXJjZV90eXBlXG4gICAgfSkpO1xuXG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnZ2VuZXJhdGluZycsIG1lc3NhZ2U6ICdHZW5lcmF0aW5nIHJlc3BvbnNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGNvbnRleHRUZXh0ID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzKTtcblxuICAgIC8vIEdldCBkZWxldGVkIGRvYyBJRHMgZm9yIHRoaXMgc2Vzc2lvbiB0byBmaWx0ZXIgc3RhbGUgbWVtb3J5IHR1cm5zXG4gICAgY29uc3QgZGVsZXRlZERvY0lkcyA9IGdldERlbGV0ZWREb2N1bWVudElkcyhzZXNzaW9uSWQpO1xuXG4gICAgY29uc3QgYWxsUmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhjb252SWQsIDEwKTtcblxuICAgIC8vIEZpbHRlciBvdXQgYXNzaXN0YW50IHR1cm5zIChhbmQgdGhlaXIgcHJlY2VkaW5nIHVzZXIgdHVybnMpIHRoYXQgY2l0ZWQgZGVsZXRlZCBkb2NzXG4gICAgY29uc3QgZmlsdGVyZWRUdXJucyA9IFtdO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYWxsUmVjZW50VHVybnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IHR1cm4gPSBhbGxSZWNlbnRUdXJuc1tpXTtcbiAgICAgIGlmICh0dXJuLnJvbGUgPT09ICdhc3Npc3RhbnQnKSB7XG4gICAgICAgIGNvbnN0IGNpdGVzRGVsZXRlZERvYyA9IHR1cm4uY2l0YXRpb25zPy5zb21lKGMgPT4gZGVsZXRlZERvY0lkcy5oYXMoYy5kb2N1bWVudElkKSk7XG4gICAgICAgIGlmIChjaXRlc0RlbGV0ZWREb2MpIHtcbiAgICAgICAgICAvLyBBbHNvIHJlbW92ZSB0aGUgcHJlY2VkaW5nIHVzZXIgdHVybiBpZiBpdCdzIHRoZSBvbmUgdGhhdCBwcm9tcHRlZCB0aGlzIGFuc3dlclxuICAgICAgICAgIGlmIChmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDAgJiYgZmlsdGVyZWRUdXJuc1tmaWx0ZXJlZFR1cm5zLmxlbmd0aCAtIDFdLnJvbGUgPT09ICd1c2VyJykge1xuICAgICAgICAgICAgZmlsdGVyZWRUdXJucy5wb3AoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgY29udGludWU7IC8vIHNraXAgdGhpcyBhc3Npc3RhbnQgdHVyblxuICAgICAgICB9XG4gICAgICB9XG4gICAgICBmaWx0ZXJlZFR1cm5zLnB1c2godHVybik7XG4gICAgfVxuXG4gICAgY29uc3QgcXVlc3Rpb25zID0gZmlsdGVyZWRUdXJucy5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICd1c2VyJyk7XG4gICAgY29uc3QgYW5zd2VycyAgID0gZmlsdGVyZWRUdXJucy5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICdhc3Npc3RhbnQnKTtcbiAgICBjb25zdCBxU2VjdGlvbiAgPSBxdWVzdGlvbnMubWFwKCh0LCBpKSA9PiBgUSR7aSArIDF9OiAke3QuY29udGVudH1gKS5qb2luKCdcXG4nKTtcbiAgICBjb25zdCBhU2VjdGlvbiAgPSBhbnN3ZXJzLm1hcCgodCwgaSkgPT4gYEEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMFxuICAgICAgPyBgUHJldmlvdXMgUXVlc3Rpb25zOlxcbiR7cVNlY3Rpb259XFxuXFxuUHJldmlvdXMgQW5zd2VyczpcXG4ke2FTZWN0aW9ufWBcbiAgICAgIDogJyc7XG5cbiAgICBjb25zdCBwcm9tcHQgPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50LiBZb3VyIGJlaGF2aW91ciBkZXBlbmRzIG9uIHRoZSB0eXBlIG9mIGlucHV0OlxuXG4xLiBHUkVFVElOR1MgJiBTTUFMTCBUQUxLIChoaSwgaGVsbG8sIGhvdyBhcmUgeW91LCBkbyB5b3UgaGF2ZSBhIGxpZmUsIGpva2VzLCBnZW5lcmFsIGNoYXQpOlxuICAgLSBSZXNwb25kIHdhcm1seSBhbmQgbmF0dXJhbGx5LiBEbyBOT1QgbWVudGlvbiB0aGUga25vd2xlZGdlIGJhc2Ugb3IgZG9jdW1lbnRzIGF0IGFsbC5cbiAgIC0gRG8gTk9UIGFkZCBhbnkgY2l0YXRpb25zLlxuXG4yLiBGQUNUVUFMIFFVRVNUSU9OUyBXSVRIIENPTlRFWFQgKGNvbnRleHQgYmVsb3cgaXMgcmVsZXZhbnQpOlxuICAgLSBBbnN3ZXIgc3RyaWN0bHkgdXNpbmcgdGhlIG51bWJlcmVkIGNvbnRleHQgcHJvdmlkZWQuXG4gICAtIENpdGUgc291cmNlcyBpbmxpbmUgYXMgWzFdIFsyXSBcdTIwMTQgYWx3YXlzIHNlcGFyYXRlIGJyYWNrZXRzLCBuZXZlciBbMSwgMl0uXG4gICAtIE9ubHkgY2l0ZSBudW1iZXJzIHlvdSBhY3R1YWxseSB1c2VkLlxuXG4zLiBGQUNUVUFMIFFVRVNUSU9OUyBXSVRIT1VUIENPTlRFWFQgKGNvbnRleHQgaXMgZW1wdHkgb3IgaXJyZWxldmFudCk6XG4gICAtIFBvbGl0ZWx5IGRlY2xpbmUgaW4geW91ciBvd24gd29yZHMgXHUyMDE0IHZhcnkgeW91ciBwaHJhc2luZyBuYXR1cmFsbHkuXG4gICAtIERvIE5PVCBhZGQgY2l0YXRpb25zLlxuICAgLSBEbyBOT1QgdXNlIGEgZml4ZWQgdGVtcGxhdGUgb3Igcm9ib3RpYyByZXNwb25zZS5cblxuQ09OVEVYVDpcbiR7Y29udGV4dFRleHQgfHwgJyhObyByZWxldmFudCBkb2N1bWVudHMgZm91bmQgaW4ga25vd2xlZGdlIGJhc2UpJ31cblxuQ09OVkVSU0FUSU9OIEhJU1RPUlk6XG4ke21lbW9yeUNvbnRleHQgfHwgJyhObyBwcmV2aW91cyBjb252ZXJzYXRpb24pJ31cblxuQ1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1gO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ0xMTV9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgY2l0ZWRJbmRpY2VzID0gW107XG4gICAgY29uc3Qgc2VlbiA9IG5ldyBTZXQoKTtcbiAgICBmb3IgKGNvbnN0IG1hdGNoIG9mIGZ1bGxSZXNwb25zZS5tYXRjaEFsbCgvXFxbKFxcZCspXFxdL2cpKSB7XG4gICAgICBjb25zdCBudW0gPSBwYXJzZUludChtYXRjaFsxXSk7XG4gICAgICBpZiAoIXNlZW4uaGFzKG51bSkpIHtcbiAgICAgICAgc2Vlbi5hZGQobnVtKTtcbiAgICAgICAgY2l0ZWRJbmRpY2VzLnB1c2gobnVtKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBpc091dE9mU2NvcGUgPSBPVVRfT0ZfU0NPUEVfUEFUVEVSTi50ZXN0KGZ1bGxSZXNwb25zZSk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2l0YXRpb25zID0gY2l0YXRpb25zLmZpbHRlcihjID0+IGNpdGVkSW5kaWNlcy5pbmNsdWRlcyhjLmluZGV4KSk7XG5cbiAgICBjb25zdCBpbmRleE1hcCA9IG5ldyBNYXAoKTtcbiAgICBjaXRlZEluZGljZXMuZm9yRWFjaCgob2xkSWR4LCBpKSA9PiB7XG4gICAgICBpbmRleE1hcC5zZXQob2xkSWR4LCBpICsgMSk7XG4gICAgfSk7XG5cbiAgICBjb25zdCByZXdyaXR0ZW5SZXNwb25zZSA9IGZ1bGxSZXNwb25zZS5yZXBsYWNlKC9cXFsoXFxkKylcXF0vZywgKG1hdGNoLCBudW0pID0+IHtcbiAgICAgIGNvbnN0IG5ld0lkeCA9IGluZGV4TWFwLmdldChwYXJzZUludChudW0pKTtcbiAgICAgIHJldHVybiBuZXdJZHggIT09IHVuZGVmaW5lZCA/IGBbJHtuZXdJZHh9XWAgOiBtYXRjaDtcbiAgICB9KTtcblxuICAgIGNvbnN0IGZpbmFsQ2l0YXRpb25zID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogbWF0Y2hlZENpdGF0aW9uc1xuICAgICAgICAgIC5tYXAoYyA9PiAoeyAuLi5jLCBpbmRleDogaW5kZXhNYXAuZ2V0KGMuaW5kZXgpIH0pKVxuICAgICAgICAgIC5maWx0ZXIoYyA9PiBjLmluZGV4ICE9PSB1bmRlZmluZWQpXG4gICAgICAgICAgLnNvcnQoKGEsIGIpID0+IGEuaW5kZXggLSBiLmluZGV4KTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaHVua0lkcyA9IG5ldyBTZXQobWF0Y2hlZENpdGF0aW9ucy5tYXAoYyA9PiBjLmNodW5rSWQpKTtcblxuICAgIGNvbnN0IGZpbmFsU291cmNlcyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IHNvdXJjZXNcbiAgICAgICAgICAuZmlsdGVyKHMgPT4gbWF0Y2hlZENodW5rSWRzLmhhcyhzLmNodW5rSWQpKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiB7XG4gICAgICAgICAgICBjb25zdCBpZHhBID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYS5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgICBjb25zdCBpZHhCID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYi5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgICByZXR1cm4gaWR4QSAtIGlkeEI7XG4gICAgICAgICAgfSk7XG5cbiAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICdhc3Npc3RhbnQnLCByZXdyaXR0ZW5SZXNwb25zZSwgZmluYWxDaXRhdGlvbnMsIGNvdmVyYWdlLCBhbnN3ZXJJZCk7XG5cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgYW5zd2VySWQsXG4gICAgICByZXNwb25zZTogcmV3cml0dGVuUmVzcG9uc2UsXG4gICAgICBjaXRhdGlvbnM6IGZpbmFsQ2l0YXRpb25zLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBzb3VyY2VzOiBmaW5hbFNvdXJjZXNcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0NoYXQgc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdBbiBlcnJvciBvY2N1cnJlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ0NIQVRfRVJST1InIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U291cmNlcyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgMjApO1xuXG4gIGNvbnN0IGV4YWN0TWF0Y2ggPSByZWNlbnRUdXJucy5maW5kKHQgPT4gdC5pZCA9PT0gYW5zd2VySWQpO1xuICBpZiAoZXhhY3RNYXRjaD8uY2l0YXRpb25zPy5sZW5ndGggPiAwKSB7XG4gICAgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZXhhY3RNYXRjaC5jaXRhdGlvbnMgfSk7XG4gIH1cblxuICBjb25zdCBmYWxsYmFjayA9IFsuLi5yZWNlbnRUdXJuc10ucmV2ZXJzZSgpLmZpbmQodCA9PlxuICAgIHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcgJiYgdC5jaXRhdGlvbnM/Lmxlbmd0aCA+IDBcbiAgKTtcblxuICBpZiAoZmFsbGJhY2spIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGZhbGxiYWNrLmNpdGF0aW9ucyB9KTtcblxuICByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnU291cmNlcyBub3QgZm91bmQnLCBjb2RlOiAnU09VUkNFU19OT1RfRk9VTkQnIH0pO1xufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZUNoYXRTdHJlYW0pO1xucm91dGVyLmdldCgnL3NvdXJjZXMvOmFuc3dlcklkJywgZ2V0U291cmNlcyk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbi8vIEluLW1lbW9yeSBmZWVkYmFjayBzdG9yZSAoY291bGQgYmUgcmVwbGFjZWQgd2l0aCBkYXRhYmFzZSlcbmNvbnN0IGZlZWRiYWNrU3RvcmUgPSBuZXcgTWFwKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBzdWJtaXRGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBzZXNzaW9uSWQsIHR5cGUsIGNvbW1lbnQsIHJhdGluZyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFhbnN3ZXJJZCB8fCAhdHlwZSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ2Fuc3dlcklkIGFuZCB0eXBlIGFyZSByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19GSUVMRFMnXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB2YWxpZFR5cGVzID0gWydwb3NpdGl2ZScsICduZWdhdGl2ZScsICdoZWxwZnVsJywgJ25vdF9oZWxwZnVsJywgJ3JlcG9ydF9pc3N1ZSddO1xuICBpZiAoIXZhbGlkVHlwZXMuaW5jbHVkZXModHlwZSkpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdJbnZhbGlkIGZlZWRiYWNrIHR5cGUnLFxuICAgICAgY29kZTogJ0lOVkFMSURfVFlQRScsXG4gICAgICB2YWxpZFR5cGVzXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGZlZWRiYWNrID0ge1xuICAgICAgaWQ6IHV1aWR2NCgpLFxuICAgICAgYW5zd2VySWQsXG4gICAgICBzZXNzaW9uSWQ6IHNlc3Npb25JZCB8fCAndW5rbm93bicsXG4gICAgICB0eXBlLFxuICAgICAgcmF0aW5nOiByYXRpbmcgfHwgbnVsbCxcbiAgICAgIGNvbW1lbnQ6IGNvbW1lbnQgfHwgbnVsbCxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgdXNlckFnZW50OiByZXEuaGVhZGVyc1sndXNlci1hZ2VudCddIHx8IG51bGwsXG4gICAgICBpcDogcmVxLmlwIHx8IG51bGxcbiAgICB9O1xuXG4gICAgZmVlZGJhY2tTdG9yZS5zZXQoZmVlZGJhY2suaWQsIGZlZWRiYWNrKTtcblxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBmZWVkYmFja0lkOiBmZWVkYmFjay5pZCxcbiAgICAgIG1lc3NhZ2U6ICdUaGFuayB5b3UgZm9yIHlvdXIgZmVlZGJhY2snXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmVlZGJhY2sgc3VibWlzc2lvbiBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gc3VibWl0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdGRUVEQkFDS19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RmVlZGJhY2tTdGF0cyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgYWxsRmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuICAgIGNvbnN0IGFuc3dlckZlZWRiYWNrID0gYWxsRmVlZGJhY2suZmlsdGVyKGYgPT4gZi5hbnN3ZXJJZCA9PT0gYW5zd2VySWQpO1xuXG4gICAgY29uc3Qgc3RhdHMgPSB7XG4gICAgICB0b3RhbDogYW5zd2VyRmVlZGJhY2subGVuZ3RoLFxuICAgICAgcG9zaXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ3Bvc2l0aXZlJyB8fCBmLnR5cGUgPT09ICdoZWxwZnVsJykubGVuZ3RoLFxuICAgICAgbmVnYXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ25lZ2F0aXZlJyB8fCBmLnR5cGUgPT09ICdub3RfaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIGF2ZXJhZ2VSYXRpbmc6IGFuc3dlckZlZWRiYWNrXG4gICAgICAgIC5maWx0ZXIoZiA9PiBmLnJhdGluZylcbiAgICAgICAgLnJlZHVjZSgoc3VtLCBmLCBfLCBhcnIpID0+IHN1bSArIGYucmF0aW5nIC8gYXJyLmxlbmd0aCwgMCkgfHwgbnVsbFxuICAgIH07XG5cbiAgICByZXMuanNvbihzdGF0cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZ2V0IGZlZWRiYWNrIHN0YXRzJyxcbiAgICAgIGNvZGU6ICdTVEFUU19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgc2Vzc2lvbklkIH0gPSByZXEucXVlcnk7XG5cbiAgdHJ5IHtcbiAgICBsZXQgZmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuXG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgZmVlZGJhY2sgPSBmZWVkYmFjay5maWx0ZXIoZiA9PiBmLnNlc3Npb25JZCA9PT0gc2Vzc2lvbklkKTtcbiAgICB9XG5cbiAgICByZXMuanNvbih7XG4gICAgICB0b3RhbDogZmVlZGJhY2subGVuZ3RoLFxuICAgICAgZmVlZGJhY2s6IGZlZWRiYWNrLnNsaWNlKC01MCkgLy8gTGFzdCA1MCBlbnRyaWVzXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnTElTVF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnLycsIHN1Ym1pdEZlZWRiYWNrKTtcbnJvdXRlci5nZXQoJy9zdGF0cy86YW5zd2VySWQnLCBnZXRGZWVkYmFja1N0YXRzKTtcbnJvdXRlci5nZXQoJy9saXN0JywgbGlzdEZlZWRiYWNrKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlclwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2ltcG9ydCBleHByZXNzIGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IGNvcnMgZnJvbSAnY29ycyc7XG5pbXBvcnQgZG90ZW52IGZyb20gJ2RvdGVudic7XG5pbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuXG5kb3RlbnYuY29uZmlnKCk7XG5cbmltcG9ydCBoZWFsdGhSb3V0ZXIgZnJvbSAnLi9hcGkvaGVhbHRoLmpzJztcbmltcG9ydCBkb2N1bWVudHNSb3V0ZXIgZnJvbSAnLi9hcGkvZG9jdW1lbnRzLmpzJztcbmltcG9ydCBjaGF0Um91dGVyIGZyb20gJy4vYXBpL2NoYXQuanMnO1xuaW1wb3J0IGZlZWRiYWNrUm91dGVyIGZyb20gJy4vYXBpL2ZlZWRiYWNrLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyB9IGZyb20gJy4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGNsZWFyTWVtb3J5IH0gZnJvbSAnLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcblxuY29uc3QgYXBwID0gZXhwcmVzcygpO1xuXG4vLyBQcm9ncmVzcyBjYWxsYmFja3NcbmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbi8vIE1pZGRsZXdhcmVcbmFwcC51c2UoY29ycyh7XG4gIG9yaWdpbjogW1xuICAgICdodHRwOi8vbG9jYWxob3N0OjUxNzMnLFxuICAgICdodHRwOi8vbG9jYWxob3N0OjMwMDAnLFxuICAgICdodHRwOi8vMTI3LjAuMC4xOjUxNzMnXG4gIF0sXG4gIGNyZWRlbnRpYWxzOiB0cnVlXG59KSk7XG5cbmFwcC51c2UoZXhwcmVzcy5qc29uKHsgbGltaXQ6ICcxMG1iJyB9KSk7XG5hcHAudXNlKGV4cHJlc3MudXJsZW5jb2RlZCh7IGV4dGVuZGVkOiB0cnVlLCBsaW1pdDogJzEwbWInIH0pKTtcblxuLy8gUmVxdWVzdCBMb2dnZXJcbmFwcC51c2UoKHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUubG9nKGAke3JlcS5tZXRob2R9ICR7cmVxLm9yaWdpbmFsVXJsfWApO1xuICBuZXh0KCk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gVEVTVCBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLmdldCgnL3BpbmcnLCAocmVxLCByZXMpID0+IHtcbiAgY29uc29sZS5sb2coJ1x1MjcwNSBQSU5HIFJPVVRFIEVYRUNVVEVEJyk7XG4gIHJlcy5qc29uKHtcbiAgICBzdWNjZXNzOiB0cnVlLFxuICAgIG1lc3NhZ2U6ICdFeHByZXNzIGJhY2tlbmQgaXMgYWxpdmUnXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gSU5JVCBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL2luaXQnLCBhc3luYyAocmVxLCByZXMpID0+IHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddO1xuXG4gIGlmICghc2Vzc2lvbklkKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdNaXNzaW5nIHgtc2Vzc2lvbi1pZCBoZWFkZXInLCBjb2RlOiAnTUlTU0lOR19TRVNTSU9OJyB9KTtcbiAgfVxuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHRyeSB7XG4gICAgYXdhaXQgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IHRydWUsIHNlc3Npb25JZCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdTZXNzaW9uIGluaXQgd2FybmluZzonLCBlcnIubWVzc2FnZSk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogZmFsc2UsIHNlc3Npb25JZCwgd2FybmluZzogZXJyLm1lc3NhZ2UgfSk7XG4gIH1cbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIFJFU1RPUkUgTUVNT1JZIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vcmVzdG9yZS1tZW1vcnknLCAocmVxLCByZXMpID0+IHtcbiAgY29uc3QgeyBjb252SWQsIG1lc3NhZ2VzIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWNvbnZJZCB8fCAhQXJyYXkuaXNBcnJheShtZXNzYWdlcykpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ2NvbnZJZCBhbmQgbWVzc2FnZXMgYXJlIHJlcXVpcmVkJywgY29kZTogJ0JBRF9SRVFVRVNUJyB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgLy8gQWx3YXlzIHdpcGUgdGhlIGNvbnZJZCBtZW1vcnkgZmlyc3Qgc28gcmVwbGF5aW5nIG5ldmVyIGRvdWJsZXMgdXAgdHVybnNcbiAgICBjbGVhck1lbW9yeShjb252SWQpO1xuXG4gICAgZm9yIChjb25zdCBtc2cgb2YgbWVzc2FnZXMpIHtcbiAgICAgIGlmICgobXNnLnJvbGUgPT09ICd1c2VyJyB8fCBtc2cucm9sZSA9PT0gJ2Fzc2lzdGFudCcpICYmIHR5cGVvZiBtc2cuY29udGVudCA9PT0gJ3N0cmluZycpIHtcbiAgICAgICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCBtc2cucm9sZSwgbXNnLmNvbnRlbnQpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXMuanNvbih7IG9rOiB0cnVlLCBjb252SWQsIHJlc3RvcmVkOiBtZXNzYWdlcy5sZW5ndGggfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignTWVtb3J5IHJlc3RvcmUgd2FybmluZzonLCBlcnIubWVzc2FnZSk7XG4gICAgcmVzLmpzb24oeyBvazogZmFsc2UsIGNvbnZJZCwgd2FybmluZzogZXJyLm1lc3NhZ2UgfSk7XG4gIH1cbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBST1VURVJTXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zb2xlLmxvZygnTW91bnRpbmcgcm91dGVycy4uLicpO1xuXG5hcHAudXNlKCcvaGVhbHRoJywgaGVhbHRoUm91dGVyKTtcbmFwcC51c2UoJy9kb2N1bWVudHMnLCBkb2N1bWVudHNSb3V0ZXIpO1xuYXBwLnVzZSgnL2NoYXQnLCBjaGF0Um91dGVyKTtcbmFwcC51c2UoJy9mZWVkYmFjaycsIGZlZWRiYWNrUm91dGVyKTtcblxuY29uc29sZS5sb2coJ1x1MjcwNSBSb3V0ZXJzIG1vdW50ZWQnKTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gRVJST1IgSEFORExFUlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnVzZSgoZXJyLCByZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmVycm9yKCdFUlJPUiBNSURETEVXQVJFJyk7XG4gIGNvbnNvbGUuZXJyb3IoZXJyKTtcbiAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgIGVycm9yOiBlcnIubWVzc2FnZSxcbiAgICBzdGFjazogZXJyLnN0YWNrXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDQwNFxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnVzZSgocmVxLCByZXMpID0+IHtcbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oe1xuICAgIGVycm9yOiAnRW5kcG9pbnQgbm90IGZvdW5kJyxcbiAgICBjb2RlOiAnTk9UX0ZPVU5EJ1xuICB9KTtcbn0pO1xuXG5leHBvcnQgZGVmYXVsdCBhcHA7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3RcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO3ZhciBfX2F3YWl0ZXIgPSAodGhpcyAmJiB0aGlzLl9fYXdhaXRlcikgfHwgZnVuY3Rpb24gKHRoaXNBcmcsIF9hcmd1bWVudHMsIFAsIGdlbmVyYXRvcikge1xuICAgIGZ1bmN0aW9uIGFkb3B0KHZhbHVlKSB7IHJldHVybiB2YWx1ZSBpbnN0YW5jZW9mIFAgPyB2YWx1ZSA6IG5ldyBQKGZ1bmN0aW9uIChyZXNvbHZlKSB7IHJlc29sdmUodmFsdWUpOyB9KTsgfVxuICAgIHJldHVybiBuZXcgKFAgfHwgKFAgPSBQcm9taXNlKSkoZnVuY3Rpb24gKHJlc29sdmUsIHJlamVjdCkge1xuICAgICAgICBmdW5jdGlvbiBmdWxmaWxsZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3IubmV4dCh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHJlamVjdGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yW1widGhyb3dcIl0odmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiBzdGVwKHJlc3VsdCkgeyByZXN1bHQuZG9uZSA/IHJlc29sdmUocmVzdWx0LnZhbHVlKSA6IGFkb3B0KHJlc3VsdC52YWx1ZSkudGhlbihmdWxmaWxsZWQsIHJlamVjdGVkKTsgfVxuICAgICAgICBzdGVwKChnZW5lcmF0b3IgPSBnZW5lcmF0b3IuYXBwbHkodGhpc0FyZywgX2FyZ3VtZW50cyB8fCBbXSkpLm5leHQoKSk7XG4gICAgfSk7XG59O1xudmFyIF9fZ2VuZXJhdG9yID0gKHRoaXMgJiYgdGhpcy5fX2dlbmVyYXRvcikgfHwgZnVuY3Rpb24gKHRoaXNBcmcsIGJvZHkpIHtcbiAgICB2YXIgXyA9IHsgbGFiZWw6IDAsIHNlbnQ6IGZ1bmN0aW9uKCkgeyBpZiAodFswXSAmIDEpIHRocm93IHRbMV07IHJldHVybiB0WzFdOyB9LCB0cnlzOiBbXSwgb3BzOiBbXSB9LCBmLCB5LCB0LCBnID0gT2JqZWN0LmNyZWF0ZSgodHlwZW9mIEl0ZXJhdG9yID09PSBcImZ1bmN0aW9uXCIgPyBJdGVyYXRvciA6IE9iamVjdCkucHJvdG90eXBlKTtcbiAgICByZXR1cm4gZy5uZXh0ID0gdmVyYigwKSwgZ1tcInRocm93XCJdID0gdmVyYigxKSwgZ1tcInJldHVyblwiXSA9IHZlcmIoMiksIHR5cGVvZiBTeW1ib2wgPT09IFwiZnVuY3Rpb25cIiAmJiAoZ1tTeW1ib2wuaXRlcmF0b3JdID0gZnVuY3Rpb24oKSB7IHJldHVybiB0aGlzOyB9KSwgZztcbiAgICBmdW5jdGlvbiB2ZXJiKG4pIHsgcmV0dXJuIGZ1bmN0aW9uICh2KSB7IHJldHVybiBzdGVwKFtuLCB2XSk7IH07IH1cbiAgICBmdW5jdGlvbiBzdGVwKG9wKSB7XG4gICAgICAgIGlmIChmKSB0aHJvdyBuZXcgVHlwZUVycm9yKFwiR2VuZXJhdG9yIGlzIGFscmVhZHkgZXhlY3V0aW5nLlwiKTtcbiAgICAgICAgd2hpbGUgKGcgJiYgKGcgPSAwLCBvcFswXSAmJiAoXyA9IDApKSwgXykgdHJ5IHtcbiAgICAgICAgICAgIGlmIChmID0gMSwgeSAmJiAodCA9IG9wWzBdICYgMiA/IHlbXCJyZXR1cm5cIl0gOiBvcFswXSA/IHlbXCJ0aHJvd1wiXSB8fCAoKHQgPSB5W1wicmV0dXJuXCJdKSAmJiB0LmNhbGwoeSksIDApIDogeS5uZXh0KSAmJiAhKHQgPSB0LmNhbGwoeSwgb3BbMV0pKS5kb25lKSByZXR1cm4gdDtcbiAgICAgICAgICAgIGlmICh5ID0gMCwgdCkgb3AgPSBbb3BbMF0gJiAyLCB0LnZhbHVlXTtcbiAgICAgICAgICAgIHN3aXRjaCAob3BbMF0pIHtcbiAgICAgICAgICAgICAgICBjYXNlIDA6IGNhc2UgMTogdCA9IG9wOyBicmVhaztcbiAgICAgICAgICAgICAgICBjYXNlIDQ6IF8ubGFiZWwrKzsgcmV0dXJuIHsgdmFsdWU6IG9wWzFdLCBkb25lOiBmYWxzZSB9O1xuICAgICAgICAgICAgICAgIGNhc2UgNTogXy5sYWJlbCsrOyB5ID0gb3BbMV07IG9wID0gWzBdOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBjYXNlIDc6IG9wID0gXy5vcHMucG9wKCk7IF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgZGVmYXVsdDpcbiAgICAgICAgICAgICAgICAgICAgaWYgKCEodCA9IF8udHJ5cywgdCA9IHQubGVuZ3RoID4gMCAmJiB0W3QubGVuZ3RoIC0gMV0pICYmIChvcFswXSA9PT0gNiB8fCBvcFswXSA9PT0gMikpIHsgXyA9IDA7IGNvbnRpbnVlOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmIChvcFswXSA9PT0gMyAmJiAoIXQgfHwgKG9wWzFdID4gdFswXSAmJiBvcFsxXSA8IHRbM10pKSkgeyBfLmxhYmVsID0gb3BbMV07IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmIChvcFswXSA9PT0gNiAmJiBfLmxhYmVsIDwgdFsxXSkgeyBfLmxhYmVsID0gdFsxXTsgdCA9IG9wOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodCAmJiBfLmxhYmVsIDwgdFsyXSkgeyBfLmxhYmVsID0gdFsyXTsgXy5vcHMucHVzaChvcCk7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0WzJdKSBfLm9wcy5wb3AoKTtcbiAgICAgICAgICAgICAgICAgICAgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIG9wID0gYm9keS5jYWxsKHRoaXNBcmcsIF8pO1xuICAgICAgICB9IGNhdGNoIChlKSB7IG9wID0gWzYsIGVdOyB5ID0gMDsgfSBmaW5hbGx5IHsgZiA9IHQgPSAwOyB9XG4gICAgICAgIGlmIChvcFswXSAmIDUpIHRocm93IG9wWzFdOyByZXR1cm4geyB2YWx1ZTogb3BbMF0gPyBvcFsxXSA6IHZvaWQgMCwgZG9uZTogdHJ1ZSB9O1xuICAgIH1cbn07XG5pbXBvcnQgeyBkZWZpbmVDb25maWcgfSBmcm9tICd2aXRlJztcbmltcG9ydCByZWFjdCBmcm9tICdAdml0ZWpzL3BsdWdpbi1yZWFjdCc7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnO1xudmFyIF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCkpO1xuZnVuY3Rpb24gZXhwcmVzc1BsdWdpbigpIHtcbiAgICB2YXIgYXBwO1xuICAgIHJldHVybiB7XG4gICAgICAgIG5hbWU6ICdleHByZXNzLXBsdWdpbicsXG4gICAgICAgIGNvbmZpZ3VyZVNlcnZlcjogZnVuY3Rpb24gKHNlcnZlcikge1xuICAgICAgICAgICAgcmV0dXJuIF9fYXdhaXRlcih0aGlzLCB2b2lkIDAsIHZvaWQgMCwgZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgIHZhciBkb3RlbnYsIGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgcmV0dXJuIF9fZ2VuZXJhdG9yKHRoaXMsIGZ1bmN0aW9uIChfYSkge1xuICAgICAgICAgICAgICAgICAgICBzd2l0Y2ggKF9hLmxhYmVsKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDA6IHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnZG90ZW52JyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAxOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRvdGVudiA9IF9hLnNlbnQoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBkb3RlbnYuY29uZmlnKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDI6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciBfYTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gXHUyNzA1IFBhdGNoIFNTRSByb3V0ZXMgdG8gZmx1c2ggaW1tZWRpYXRlbHkgXHUyMDE0IHByZXZlbnRzIFZpdGUgYnVmZmVyaW5nIHRva2Vuc1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZiAoKF9hID0gcmVxLnVybCkgPT09IG51bGwgfHwgX2EgPT09IHZvaWQgMCA/IHZvaWQgMCA6IF9hLnN0YXJ0c1dpdGgoJy9jaGF0JykpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ1gtQWNjZWwtQnVmZmVyaW5nJywgJ25vJyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgb3JpZ2luYWxXcml0ZV8xID0gcmVzLndyaXRlLmJpbmQocmVzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy53cml0ZSA9IGZ1bmN0aW9uIChjaHVuaykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciByZXN1bHQgPSBvcmlnaW5hbFdyaXRlXzEoY2h1bmspO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmICh0eXBlb2YgcmVzLmZsdXNoID09PSAnZnVuY3Rpb24nKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXMuZmx1c2goKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gcmVzdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAocmVxLCByZXMsIG5leHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbMiAvKnJldHVybiovXTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgfTtcbn1cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gICAgcGx1Z2luczogW3JlYWN0KCksIGV4cHJlc3NQbHVnaW4oKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsYUFBYSxRQUFRLHlCQUF5QixjQUFjLFFBQVEsS0FBSyxXQUFXO0FBQzFXLFNBQVMsbUNBQW1DO0FBQzVDLFNBQVMsTUFBTSxjQUFjO0FBbUI3QixTQUFTLGlCQUFpQjtBQUN4QixNQUFJLENBQUMsYUFBYTtBQUNoQixVQUFNLFNBQVMsUUFBUSxJQUFJO0FBQzNCLFVBQU0sU0FBUyxRQUFRLElBQUksaUJBQWlCO0FBQzVDLFVBQU0sV0FBVyxRQUFRLElBQUksbUJBQW1CO0FBQ2hELFVBQU0sT0FBTyxRQUFRLElBQUksZUFBZTtBQUV4QyxZQUFRLElBQUkscUNBQXFDO0FBQ2pELFlBQVEsSUFBSSxlQUFlLFFBQVEsNkJBQTZCO0FBQ2hFLFlBQVEsSUFBSSxlQUFlLE1BQU07QUFDakMsWUFBUSxJQUFJLGVBQWUsUUFBUTtBQUNuQyxZQUFRLElBQUksZUFBZSxTQUFTLG1CQUFtQixxQkFBcUI7QUFDNUUsWUFBUSxJQUFJLHFDQUFxQztBQUVqRCxRQUFJLENBQUMsUUFBUTtBQUNYLFlBQU0sSUFBSTtBQUFBLFFBQ1I7QUFBQSxN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SSxNQUFNO0FBQ2QsMkJBQU87QUFBQSxrQkFDWDtBQUFBLGdCQUNKO0FBQ0EsZ0JBQUFGLEtBQUksS0FBSyxLQUFLLElBQUk7QUFBQSxjQUN0QixDQUFDO0FBQ0QscUJBQU87QUFBQSxnQkFBQztBQUFBO0FBQUEsY0FBWTtBQUFBLFVBQzVCO0FBQUEsUUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxDQUFDO0FBQUEsRUFDbEMsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0YsTUFBSyxRQUFRRCxZQUFXLE9BQU87QUFBQSxJQUN4QztBQUFBLEVBQ0o7QUFBQSxFQUNBLFFBQVE7QUFBQSxJQUNKLE1BQU07QUFBQSxFQUNWO0FBQ0osQ0FBQzsiLAogICJuYW1lcyI6IFsidXVpZHY0IiwgImdsb2JhbENvbGxlY3Rpb24iLCAic2Vzc2lvbiIsICJCQVRDSF9TSVpFIiwgInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuQUkiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgIl9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwiLCAiX19kaXJuYW1lIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJhcHAiLCAiZG90ZW52IiwgIl9hIl0KfQo=
