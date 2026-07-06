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
async function getCollection() {
  const collection = await getGlobalCollection();
  return { collection, isNew: false };
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
async function queryCollection(collection, queryEmbedding, topK = 5, where = void 0) {
  try {
    const queryOpts = {
      queryEmbeddings: [queryEmbedding],
      nResults: topK,
      include: ["documents", "metadatas", "distances"]
    };
    if (where) queryOpts.where = where;
    const results = await collection.query(queryOpts);
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
async function hybridQueryCollection(collection, queryText, queryEmbedding, topK = 5, where = void 0) {
  try {
    let search = new Search().rank(Rrf({
      ranks: [
        Knn({ query: queryEmbedding, returnRank: true, limit: 100 }),
        Knn({ query: queryText, key: "sparse_bm25", returnRank: true, limit: 100 })
      ],
      weights: [0.9, 0.1],
      k: 60
    })).where(where).select("#document", "#metadata", "#score").limit(topK);
    const raw = await collection.search(search);
    if (!raw.ids || !raw.ids[0] || raw.ids[0].length === 0) {
      return [];
    }
    const ids = raw.ids[0];
    const docs = raw.documents?.[0] ?? [];
    const metas = raw.metadatas?.[0] ?? [];
    const scores = raw.scores?.[0] ?? [];
    const MAX_RRF = 1 / 61;
    const MIN_RRF = 1 / 160;
    return ids.map((id, idx) => {
      const rawRRF = Math.abs(scores[idx] ?? MIN_RRF);
      let normalizedScore = (rawRRF - MIN_RRF) / (MAX_RRF - MIN_RRF);
      normalizedScore = Math.max(0, Math.min(1, normalizedScore));
      return {
        id,
        text: docs[idx] ?? "",
        metadata: metas[idx] ?? {},
        distance: 1 - normalizedScore,
        score: normalizedScore
      };
    });
  } catch (error) {
    console.error("Hybrid query failed, falling back to dense-only:", error.message);
    return queryCollection(collection, queryEmbedding, topK, where);
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
async function listDocuments(collection, where = void 0) {
  try {
    const documentsMap = /* @__PURE__ */ new Map();
    let offset = 0;
    while (true) {
      const getOpts = {
        include: ["metadatas", "documents"],
        limit: BATCH_SIZE,
        offset
      };
      if (where) getOpts.where = where;
      const batch = await collection.get(getOpts);
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
var BATCH_SIZE, bm25EmbeddingFunction, collectionSchema, cloudClient, globalCollection;
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
    const collection = await getGlobalCollection();
    if (!globalDataInitialized) {
      try {
        const globalDocs = await listDocuments(collection, { session_id: "global" });
        globalDocumentsCache = globalDocs.map((doc) => ({
          id: doc.document_id,
          filename: doc.filename,
          fileSize: null,
          pageCount: doc.page_count || null,
          chunkCount: doc.chunk_count,
          sourceType: "global",
          uploadTimestamp: doc.upload_timestamp
        }));
        globalDataInitialized = true;
        console.log(`\u2705 Global documents cache loaded: ${globalDocumentsCache.length} document(s)`);
      } catch (err) {
        console.error("\u274C Failed to initialize global data:", err.message);
      }
    }
    const session = getSession(sessionId);
    if (session && session.documents.length === 0) {
      const docs = await listDocuments(collection, { session_id: sessionId });
      docs.forEach((doc) => {
        session.documents.push({
          id: doc.document_id,
          filename: doc.filename,
          fileSize: null,
          pageCount: doc.page_count || null,
          chunkCount: doc.chunk_count,
          sourceType: "session_upload",
          uploadTimestamp: doc.upload_timestamp
        });
      });
      if (docs.length > 0) {
        console.log(`\u267B\uFE0F  Reconstructed ${docs.length} session document(s) for ${sessionId}`);
      }
    }
    seededSessions.add(sessionId);
    console.log(`\u2705 Session ${sessionId} ready (no vector copying needed)`);
    notifySeedingComplete(sessionId);
  } catch (error) {
    console.error(`\u274C Failed to init session ${sessionId}:`, error.message);
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
    globalDocuments: globalDocumentsCache.map(normalize)
  };
}
var DEFAULT_TIMEOUT_MINUTES, sessions, MAX_PDFS_PER_SESSION, MAX_UPLOAD_SIZE_MB, seededSessions, globalDocumentsCache, globalDataInitialized;
var init_sessionService = __esm({
  "server/services/sessionService.js"() {
    "use strict";
    init_chromaService();
    DEFAULT_TIMEOUT_MINUTES = 60;
    sessions = /* @__PURE__ */ new Map();
    MAX_PDFS_PER_SESSION = parseInt(process.env.MAX_PDFS_PER_SESSION) || 3;
    MAX_UPLOAD_SIZE_MB = parseInt(process.env.MAX_UPLOAD_SIZE_MB) || 5;
    seededSessions = /* @__PURE__ */ new Set();
    globalDocumentsCache = [];
    globalDataInitialized = false;
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
import { v4 as uuidv43 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
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
    const sessionId = req.headers["x-session-id"] || req.body.sessionId || uuidv43();
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
    const documentId = uuidv43();
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
        session_id: sessionId,
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
    const { collection } = await getCollection();
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
        const { collection } = await getCollection();
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

// server/services/retrievalService.js
import { v4 as uuidv44 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
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
    const [queryEmbedding, { collection }] = await Promise.all([
      embedQuery(query),
      getCollection()
    ]);
    if (!collection) {
      console.warn(`\u26A0\uFE0F  No collection available`);
      return { results: [], coverage: { confidence: 0, topScore: 0, level: "low", score: 0 }, queryEmbedding };
    }
    const where = sessionId ? { session_id: { "$in": ["global", sessionId] } } : { session_id: "global" };
    const rawResults = await hybridQueryCollection(collection, query, queryEmbedding, topK, where);
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
function formatContextForPrompt(results, maxTokens = 7e3) {
  if (!results || results.length === 0) return "";
  let totalTokens = 0;
  const contextParts = [];
  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    const tokenEstimate = result.text.length / 4;
    if (totalTokens + tokenEstimate > maxTokens) break;
    totalTokens += tokenEstimate;
    const sourceLabel = result.source_type === "session_upload" ? "[Session Upload]" : "[Seed Document]";
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
    excerpt: result.text,
    score: result.score,
    sourceType: result.source_type,
    chunkId: result.id
  }));
}
var TOP_K, REFUSAL_THRESHOLD;
var init_retrievalService = __esm({
  "server/services/retrievalService.js"() {
    "use strict";
    init_chromaService();
    init_embeddingService();
    TOP_K = parseInt(process.env.TOP_K) || 20;
    REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;
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
      excerpt: cleanExcerpt(r.text),
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQsIFNjaGVtYSwgU3BhcnNlVmVjdG9ySW5kZXhDb25maWcsIERPQ1VNRU5UX0tFWSwgU2VhcmNoLCBLbm4sIFJyZiB9IGZyb20gJ2Nocm9tYWRiJztcbmltcG9ydCB7IENocm9tYUJtMjVFbWJlZGRpbmdGdW5jdGlvbiB9IGZyb20gJ0BjaHJvbWEtY29yZS9jaHJvbWEtYm0yNSc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgQkFUQ0hfU0laRSA9IDMwMDtcblxuLy8gXHUyNTAwXHUyNTAwIFNoYXJlZCBzY2hlbWE6IGRlbnNlIGVtYmVkZGluZ3MgKG1hbmFnZWQgZXh0ZXJuYWxseSkgKyBCTTI1IHNwYXJzZSBpbmRleCBcdTI1MDBcdTI1MDBcbmNvbnN0IGJtMjVFbWJlZGRpbmdGdW5jdGlvbiA9IG5ldyBDaHJvbWFCbTI1RW1iZWRkaW5nRnVuY3Rpb24oKTtcbmNvbnN0IGNvbGxlY3Rpb25TY2hlbWEgPSBuZXcgU2NoZW1hKCkuY3JlYXRlSW5kZXgoXG4gIG5ldyBTcGFyc2VWZWN0b3JJbmRleENvbmZpZyh7XG4gICAgZW1iZWRkaW5nRnVuY3Rpb246IGJtMjVFbWJlZGRpbmdGdW5jdGlvbixcbiAgICBzb3VyY2VLZXk6IERPQ1VNRU5UX0tFWSxcbiAgICBibTI1OiB0cnVlXG4gIH0pLFxuICAnc3BhcnNlX2JtMjUnXG4pO1xuXG5sZXQgY2xvdWRDbGllbnQgPSBudWxsO1xubGV0IGdsb2JhbENvbGxlY3Rpb24gPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRDbG91ZENsaWVudCgpIHtcbiAgaWYgKCFjbG91ZENsaWVudCkge1xuICAgIGNvbnN0IGFwaUtleSA9IHByb2Nlc3MuZW52LkNIUk9NQV9BUElfS0VZO1xuICAgIGNvbnN0IHRlbmFudCA9IHByb2Nlc3MuZW52LkNIUk9NQV9URU5BTlQgfHwgJ2RlZmF1bHRfdGVuYW50JztcbiAgICBjb25zdCBkYXRhYmFzZSA9IHByb2Nlc3MuZW52LkNIUk9NQV9EQVRBQkFTRSB8fCAnZGVmYXVsdF9kYXRhYmFzZSc7XG4gICAgY29uc3QgaG9zdCA9IHByb2Nlc3MuZW52LkNIUk9NQV9IT1NUIHx8IHVuZGVmaW5lZDtcblxuICAgIGNvbnNvbGUubG9nKFwiLS0tLSBDSFJPTUEgQ09OTkVDVElWSVRZIERFQlVHIC0tLS1cIik7XG4gICAgY29uc29sZS5sb2coXCJIb3N0OiAgICAgIFwiLCBob3N0IHx8IFwiYXBpLnRyeWNocm9tYS5jb20gKGRlZmF1bHQpXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiVGVuYW50OiAgICBcIiwgdGVuYW50KTtcbiAgICBjb25zb2xlLmxvZyhcIkRCIE5hbWU6ICAgXCIsIGRhdGFiYXNlKTtcbiAgICBjb25zb2xlLmxvZyhcIkFQSSBLZXk6ICAgXCIsIGFwaUtleSA/IFwiTE9BREVEIChWQUxJRClcIiA6IFwiTUlTU0lORyAoVU5ERUZJTkVEKVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIi0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXCIpO1xuXG4gICAgaWYgKCFhcGlLZXkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgXCJDUklUSUNBTCBFUlJPUjogQ0hST01BX0FQSV9LRVkgaXMgdW5kZWZpbmVkLiBcIiArXG4gICAgICAgIFwiRW5zdXJlIHlvdXIgZW52aXJvbm1lbnQgdmFyaWFibGVzIGFyZSBjb3JyZWN0bHkgbG9hZGVkIGJlZm9yZSBleGVjdXRpbmcgdGhpcyBmaWxlLlwiXG4gICAgICApO1xuICAgIH1cblxuICAgIGNvbnN0IGNsaWVudE9wdGlvbnMgPSB7IGFwaUtleSwgdGVuYW50LCBkYXRhYmFzZSB9O1xuICAgIGlmIChob3N0KSBjbGllbnRPcHRpb25zLmhvc3QgPSBob3N0O1xuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEdsb2JhbENvbGxlY3Rpb24oKSB7XG4gIGlmICghZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBwcm9jZXNzLmVudi5DSFJPTUFfR0xPQkFMX0NPTExFQ1RJT04gfHwgJ3NlZWRfZGInO1xuICAgIHRyeSB7XG4gICAgICBnbG9iYWxDb2xsZWN0aW9uID0gYXdhaXQgY2xpZW50LmdldE9yQ3JlYXRlQ29sbGVjdGlvbih7XG4gICAgICAgIG5hbWU6IGNvbGxlY3Rpb25OYW1lLFxuICAgICAgICBzY2hlbWE6IGNvbGxlY3Rpb25TY2hlbWEsXG4gICAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgICAgZGVzY3JpcHRpb246ICdQZXJtYW5lbnQgc2VlZCBkb2N1bWVudHMgZm9yIFJBRycsXG4gICAgICAgICAgdHlwZTogJ2dsb2JhbF9rbm93bGVkZ2UnXG4gICAgICAgIH0sXG4gICAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IEdsb2JhbCBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gY29ubmVjdCB0byBnbG9iYWwgY29sbGVjdGlvbjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGdsb2JhbENvbGxlY3Rpb247XG59XG5cbi8qKlxuICogUmV0dXJucyB0aGUgc2luZ2xlIHNoYXJlZCBjb2xsZWN0aW9uLlxuICogRHJvcC1pbiByZXBsYWNlbWVudCBmb3IgdGhlIG9sZCBnZXRTZXNzaW9uQ29sbGVjdGlvbiBcdTIwMTQgY2FsbGVycyB0aGF0XG4gKiBwcmV2aW91c2x5IGRlc3RydWN0dXJlZCB7IGNvbGxlY3Rpb24gfSB3aWxsIHN0aWxsIHdvcmsuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRDb2xsZWN0aW9uKCkge1xuICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICByZXR1cm4geyBjb2xsZWN0aW9uLCBpc05ldzogZmFsc2UgfTtcbn1cblxuLyoqXG4gKiBBZGQgdmVjdG9ycyBpbiBiYXRjaGVzIG9mIEJBVENIX1NJWkUgdG8gYXZvaWQgQ2hyb21hIHBheWxvYWQgbGltaXRzLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gYWRkVmVjdG9ycyhjb2xsZWN0aW9uLCB2ZWN0b3JzLCBlbWJlZGRpbmdzLCBpZHMpIHtcbiAgdHJ5IHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGlkcy5sZW5ndGg7IGkgKz0gQkFUQ0hfU0laRSkge1xuICAgICAgY29uc3QgYmF0Y2hJZHMgPSBpZHMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpO1xuICAgICAgY29uc3QgYmF0Y2hFbWJlZGRpbmdzID0gZW1iZWRkaW5ncy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaERvY3VtZW50cyA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYudGV4dCk7XG4gICAgICBjb25zdCBiYXRjaE1ldGFkYXRhcyA9IHZlY3RvcnMuc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpLm1hcCh2ID0+IHYubWV0YWRhdGEpO1xuXG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmFkZCh7XG4gICAgICAgIGlkczogYmF0Y2hJZHMsXG4gICAgICAgIGVtYmVkZGluZ3M6IGJhdGNoRW1iZWRkaW5ncyxcbiAgICAgICAgZG9jdW1lbnRzOiBiYXRjaERvY3VtZW50cyxcbiAgICAgICAgbWV0YWRhdGFzOiBiYXRjaE1ldGFkYXRhc1xuICAgICAgfSk7XG4gICAgICBjb25zb2xlLmxvZyhgICBbYWRkVmVjdG9yc10gYmF0Y2ggJHtNYXRoLmZsb29yKGkgLyBCQVRDSF9TSVpFKSArIDF9OiBhZGRlZCAke2JhdGNoSWRzLmxlbmd0aH0gdmVjdG9yc2ApO1xuICAgIH1cbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gYWRkIHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBxdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1LCB3aGVyZSA9IHVuZGVmaW5lZCkge1xuICB0cnkge1xuICAgIGNvbnN0IHF1ZXJ5T3B0cyA9IHtcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nczogW3F1ZXJ5RW1iZWRkaW5nXSxcbiAgICAgIG5SZXN1bHRzOiB0b3BLLFxuICAgICAgaW5jbHVkZTogWydkb2N1bWVudHMnLCAnbWV0YWRhdGFzJywgJ2Rpc3RhbmNlcyddXG4gICAgfTtcbiAgICBpZiAod2hlcmUpIHF1ZXJ5T3B0cy53aGVyZSA9IHdoZXJlO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IGNvbGxlY3Rpb24ucXVlcnkocXVlcnlPcHRzKTtcblxuICAgIGlmICghcmVzdWx0cy5pZHMgfHwgcmVzdWx0cy5pZHMubGVuZ3RoID09PSAwIHx8IHJlc3VsdHMuaWRzWzBdLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHRzLmlkc1swXS5tYXAoKGlkLCBpZHgpID0+ICh7XG4gICAgICBpZCxcbiAgICAgIHRleHQ6IHJlc3VsdHMuZG9jdW1lbnRzWzBdW2lkeF0sXG4gICAgICBtZXRhZGF0YTogcmVzdWx0cy5tZXRhZGF0YXNbMF1baWR4XSxcbiAgICAgIGRpc3RhbmNlOiByZXN1bHRzLmRpc3RhbmNlc1swXVtpZHhdLFxuICAgICAgc2NvcmU6IDEgLSByZXN1bHRzLmRpc3RhbmNlc1swXVtpZHhdXG4gICAgfSkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBxdWVyeSBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG4vKipcbiAqIEh5YnJpZCBzZWFyY2ggdXNpbmcgQ2hyb21hIENsb3VkIFNlYXJjaCBBUEkgd2l0aCBSUkYgKGRlbnNlICsgc3BhcnNlIEJNMjUpLlxuICogUmV0dXJucyByZXN1bHRzIGluIHRoZSBzYW1lIHNoYXBlIGFzIHF1ZXJ5Q29sbGVjdGlvbigpIGZvciBiYWNrd2FyZCBjb21wYXRpYmlsaXR5LlxuICogQWNjZXB0cyBhbiBvcHRpb25hbCBgd2hlcmVgIGNsYXVzZSBmb3IgbWV0YWRhdGEgZmlsdGVyaW5nIChlLmcuIHNlc3Npb25faWQgJGluKS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGh5YnJpZFF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeVRleHQsIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLID0gNSwgd2hlcmUgPSB1bmRlZmluZWQpIHtcbiAgdHJ5IHtcbiAgICBsZXQgc2VhcmNoID0gbmV3IFNlYXJjaCgpXG4gICAgICAucmFuayhScmYoe1xuICAgICAgICByYW5rczogW1xuICAgICAgICAgIEtubih7IHF1ZXJ5OiBxdWVyeUVtYmVkZGluZywgcmV0dXJuUmFuazogdHJ1ZSwgbGltaXQ6IDEwMCB9KSxcbiAgICAgICAgICBLbm4oeyBxdWVyeTogcXVlcnlUZXh0LCBrZXk6ICdzcGFyc2VfYm0yNScsIHJldHVyblJhbms6IHRydWUsIGxpbWl0OiAxMDAgfSlcbiAgICAgICAgXSxcbiAgICAgICAgd2VpZ2h0czogWzAuOSwgMC4xXSxcbiAgICAgICAgazogNjBcbiAgICAgIH0pKVxuICAgICAgLndoZXJlKHdoZXJlKVxuICAgICAgLnNlbGVjdChcIiNkb2N1bWVudFwiLCBcIiNtZXRhZGF0YVwiLCBcIiNzY29yZVwiKVxuICAgICAgLmxpbWl0KHRvcEspO1xuXG4gICAgY29uc3QgcmF3ID0gYXdhaXQgY29sbGVjdGlvbi5zZWFyY2goc2VhcmNoKTtcblxuICAgIC8vIFBhcmFsbGVsXHUyMDExYXJyYXkgc3RydWN0dXJlOiBpZHNbMF0sIGRvY3VtZW50c1swXSwgbWV0YWRhdGFzWzBdLCBzY29yZXNbMF1cbiAgICBpZiAoIXJhdy5pZHMgfHwgIXJhdy5pZHNbMF0gfHwgcmF3Lmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICBjb25zdCBpZHMgPSByYXcuaWRzWzBdO1xuICAgIGNvbnN0IGRvY3MgPSByYXcuZG9jdW1lbnRzPy5bMF0gPz8gW107XG4gICAgY29uc3QgbWV0YXMgPSByYXcubWV0YWRhdGFzPy5bMF0gPz8gW107XG4gICAgY29uc3Qgc2NvcmVzID0gcmF3LnNjb3Jlcz8uWzBdID8/IFtdO1xuXG4gICAgLy8gMS4gRGVmaW5lIGdsb2JhbCBSUkYgYm91bmRzIGJhc2VkIG9uIHlvdXIgd2VpZ2h0cyBbMC43LCAwLjNdIGFuZCBsaW1pdHMgKDEwMClcbiAgICAvLyBNYXggcG9zc2libGUgcmF3IFJSRjogMSAvICg2MCArIDEpID0gMC4wMTYzOTM0XG4gICAgLy8gTWluIHBvc3NpYmxlIHJhdyBSUkY6IDEgLyAoNjAgKyAxMDApID0gMC4wMDYyNTAwXG4gICAgY29uc3QgTUFYX1JSRiA9IDEgLyA2MTtcbiAgICBjb25zdCBNSU5fUlJGID0gMSAvIDE2MDtcblxuICAgIHJldHVybiBpZHMubWFwKChpZCwgaWR4KSA9PiB7XG4gICAgICAvLyBDaHJvbWEgcmV0dXJucyBuZWdhdGl2ZSB2YWx1ZXMgKGUuZy4gLTAuMDE2MzkpLCBjb252ZXJ0IHRvIHBvc2l0aXZlIHJhdyBSUkZcbiAgICAgIGNvbnN0IHJhd1JSRiA9IE1hdGguYWJzKHNjb3Jlc1tpZHhdID8/IE1JTl9SUkYpO1xuXG4gICAgICAvLyAyLiBMaW5lYXIgbWluLW1heCBub3JtYWxpemF0aW9uIHRvIGZpdCBwZXJmZWN0bHkgYmV0d2VlbiAwLjAgYW5kIDEuMFxuICAgICAgbGV0IG5vcm1hbGl6ZWRTY29yZSA9IChyYXdSUkYgLSBNSU5fUlJGKSAvIChNQVhfUlJGIC0gTUlOX1JSRik7XG5cbiAgICAgIC8vIEJvdW5kYXJ5IHByb3RlY3Rpb25cbiAgICAgIG5vcm1hbGl6ZWRTY29yZSA9IE1hdGgubWF4KDAsIE1hdGgubWluKDEsIG5vcm1hbGl6ZWRTY29yZSkpO1xuXG4gICAgICAvL2NvbnN0IGZpbmFsU2NvcmUgPSBNYXRoLnJvdW5kKG5vcm1hbGl6ZWRTY29yZSAqIDEwMCkgLyAxMDA7XG5cbiAgICAgIHJldHVybiB7XG4gICAgICAgIGlkLFxuICAgICAgICB0ZXh0OiBkb2NzW2lkeF0gPz8gJycsXG4gICAgICAgIG1ldGFkYXRhOiBtZXRhc1tpZHhdID8/IHt9LFxuICAgICAgICBkaXN0YW5jZTogMSAtIG5vcm1hbGl6ZWRTY29yZSxcbiAgICAgICAgc2NvcmU6IG5vcm1hbGl6ZWRTY29yZVxuICAgICAgfTtcbiAgICB9KTtcblxuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignSHlicmlkIHF1ZXJ5IGZhaWxlZCwgZmFsbGluZyBiYWNrIHRvIGRlbnNlLW9ubHk6JywgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gR3JhY2VmdWwgZmFsbGJhY2sgdG8gZGVuc2Utb25seSBzZWFyY2ggZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICByZXR1cm4gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLLCB3aGVyZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPmRlZmF1bHQgMTAwIGxpbWl0KSBhcmUgZnVsbHkgZGVsZXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYWxsSWRzID0gW107XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICAgIHdoZXJlOiB7IGRvY3VtZW50X2lkOiBkb2N1bWVudElkIH0sXG4gICAgICAgIGluY2x1ZGU6IFtdLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcblxuICAgICAgaWYgKCFiYXRjaC5pZHMgfHwgYmF0Y2guaWRzLmxlbmd0aCA9PT0gMCkgYnJlYWs7XG4gICAgICBhbGxJZHMucHVzaCguLi5iYXRjaC5pZHMpO1xuXG4gICAgICBpZiAoYmF0Y2guaWRzLmxlbmd0aCA8IEJBVENIX1NJWkUpIGJyZWFrO1xuICAgICAgb2Zmc2V0ICs9IEJBVENIX1NJWkU7XG4gICAgfVxuXG4gICAgaWYgKGFsbElkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7IGlkczogYWxsSWRzIH0pO1xuICAgIH1cbiAgICByZXR1cm4gYWxsSWRzLmxlbmd0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogRGVsZXRlIGFsbCB2ZWN0b3JzIGJlbG9uZ2luZyB0byBhIHNwZWNpZmljIHNlc3Npb24uXG4gKiBVc2VzIHNlc3Npb25faWQgbWV0YWRhdGEgZmlsdGVyIHRvIGZpbmQgYW5kIHJlbW92ZSB0aGVtIGluIGJhdGNoZXMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVTZXNzaW9uVmVjdG9ycyhzZXNzaW9uSWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBzZXNzaW9uX2lkOiBzZXNzaW9uSWQgfSxcbiAgICAgICAgaW5jbHVkZTogW10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBhbGxJZHMgfSk7XG4gICAgfVxuICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IERlbGV0ZWQgJHthbGxJZHMubGVuZ3RofSBzZXNzaW9uIHZlY3RvcnMgZm9yIHNlc3Npb25faWQ9JHtzZXNzaW9uSWR9YCk7XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGRlbGV0ZSBzZXNzaW9uIHZlY3RvcnMgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuLyoqXG4gKiBMaXN0IGFsbCB1bmlxdWUgZG9jdW1lbnRzIGluIGEgY29sbGVjdGlvbi5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIHdpdGggQkFUQ0hfU0laRT0zMDAgc28gY29sbGVjdGlvbnMgbGFyZ2VyXG4gKiB0aGFuIENocm9tYSdzIGRlZmF1bHQgZ2V0KCkgbGltaXQgKDEwMCkgYXJlIGZ1bGx5IGVudW1lcmF0ZWQuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZG9jdW1lbnRzTWFwID0gbmV3IE1hcCgpO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGdldE9wdHMgPSB7XG4gICAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9O1xuICAgICAgaWYgKHdoZXJlKSBnZXRPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoZ2V0T3B0cyk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuXG4gICAgICBiYXRjaC5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYmF0Y2gubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYmF0Y2guZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuXG4gICAgICBjb25zb2xlLmxvZyhgICBbbGlzdERvY3VtZW50c10gb2Zmc2V0PSR7b2Zmc2V0fSwgZ290PSR7YmF0Y2guaWRzLmxlbmd0aH0sIHVuaXF1ZSBzbyBmYXI9JHtkb2N1bWVudHNNYXAuc2l6ZX1gKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7aW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5cbmNvbnN0IENIQVJTX1BFUl9UT0tFTiAgICAgPSA0O1xuY29uc3QgVEFSR0VUX0NIVU5LX1RPS0VOUyA9IDYwMDsgICAvLyBzb2Z0IHRhcmdldCBwZXIgY2h1bmtcbmNvbnN0IE1BWF9DSFVOS19UT0tFTlMgICAgPSA3NTA7ICAgLy8gaGFyZCBjYXAgYmVmb3JlIGZvcmNlZCBzcGxpdFxuY29uc3QgT1ZFUkxBUF9UT0tFTlMgICAgICA9IDEwMDsgICAvLyBvdmVybGFwIG9ubHkgb24gb3ZlcnNpemVkIHBhcmFncmFwaHNcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyAgICAgPSAxMDA7XG5cbi8vIE1hdGNoZXMgQUxMLUNBUFMgaGVhZGluZ3MsIG1hcmtkb3duIGhlYWRpbmdzLCBvciBudW1iZXJlZCBzZWN0aW9uIGhlYWRpbmdzXG5jb25zdCBIRUFESU5HX1JFID0gL14oPzpbQS1aXVtBLVpcXHNdezIsNjB9JHwjezEsNH1cXHMuK3woPzpcXGQrXFwuKStcXHMuKykvbTtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW5UZXh0KHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuICcnO1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC9cXGYvZywgJ1xcbicpXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJylcbiAgICAucmVwbGFjZSgvWyBcXHRdezIsfS9nLCAnICcpXG4gICAgLnRyaW0oKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDaHVua0lkKHRleHQsIGZpbGVuYW1lKSB7XG4gIHJldHVybiBjcmVhdGVIYXNoKCdtZDUnKVxuICAgIC51cGRhdGUoYCR7ZmlsZW5hbWV9Ojoke3RleHR9YClcbiAgICAuZGlnZXN0KCdoZXgnKVxuICAgIC5zbGljZSgwLCAxNik7XG59XG5cbi8qKlxuICogU3RydWN0dXJlLWF3YXJlIGNodW5raW5nOlxuICogIDEuIFNwbGl0IG9uIGJsYW5rIGxpbmVzIChcXG5cXG4pIGludG8gcGFyYWdyYXBocy5cbiAqICAyLiBBIGxpbmUgbWF0Y2hpbmcgSEVBRElOR19SRSBhbHdheXMgc3RhcnRzIGEgZnJlc2ggY2h1bmsuXG4gKiAgMy4gQWNjdW11bGF0ZSBwYXJhZ3JhcGhzIHVudGlsIHRoZSBzb2Z0IFRBUkdFVCBpcyByZWFjaGVkLCB0aGVuIGZsdXNoLlxuICogIDQuIFBhcmFncmFwaHMgbGFyZ2VyIHRoYW4gTUFYIGFyZSBzcGxpdCB3aXRoIGEgc2xpZGluZyB3aW5kb3cgKyBvdmVybGFwIGFzIGZhbGxiYWNrLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0YXJnZXRUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBUQVJHRVRfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBtYXhUb2tlbnMgICAgPSBvcHRpb25zLm1heENodW5rVG9rZW5zICB8fCBNQVhfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBvdmVybGFwVGsgICAgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgICB8fCBPVkVSTEFQX1RPS0VOUztcblxuICBjb25zdCB0YXJnZXRDaGFycyAgPSB0YXJnZXRUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG1heENoYXJzICAgICA9IG1heFRva2VucyAgICAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRrICAgICogQ0hBUlNfUEVSX1RPS0VOO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICAvLyAxLiBTcGxpdCBpbnRvIHBhcmFncmFwaHNcbiAgY29uc3QgcmF3UGFyYXMgPSB0ZXh0XG4gICAgLnNwbGl0KC9cXG57Mix9LylcbiAgICAubWFwKHAgPT4gcC50cmltKCkpXG4gICAgLmZpbHRlcihwID0+IHAubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUyk7XG5cbiAgY29uc3QgY2h1bmtzICAgICA9IFtdO1xuICBsZXQgICBidWZmZXIgICAgID0gJyc7XG4gIGxldCAgIGJ1ZlN0YXJ0ICAgPSAwO1xuICBsZXQgICBjaHVua0luZGV4ID0gMDtcbiAgbGV0ICAgY2hhckN1cnNvciA9IDA7XG5cbiAgY29uc3QgZmx1c2ggPSAoZm9yY2VUZXh0KSA9PiB7XG4gICAgY29uc3QgY29udGVudCA9IChmb3JjZVRleHQgPz8gYnVmZmVyKS50cmltKCk7XG4gICAgaWYgKGNvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiAgICAgICBjb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2Vucyhjb250ZW50KSxcbiAgICAgICAgY2hhclN0YXJ0OiAgYnVmU3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6ICAgIGJ1ZlN0YXJ0ICsgY29udGVudC5sZW5ndGgsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuICAgIGJ1ZmZlciAgID0gJyc7XG4gICAgYnVmU3RhcnQgPSBjaGFyQ3Vyc29yO1xuICB9O1xuXG4gIGZvciAoY29uc3QgcGFyYSBvZiByYXdQYXJhcykge1xuICAgIGNvbnN0IGlzSGVhZGluZyA9IEhFQURJTkdfUkUudGVzdChwYXJhLnNwbGl0KCdcXG4nKVswXSk7XG5cbiAgICAvLyAyLiBIZWFkaW5nIGFsd2F5cyBzdGFydHMgYSBuZXcgY2h1bmtcbiAgICBpZiAoaXNIZWFkaW5nICYmIGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgaWYgKHBhcmEubGVuZ3RoID4gbWF4Q2hhcnMpIHtcbiAgICAgIC8vIDMuIE92ZXJzaXplZCBwYXJhZ3JhcGggLT4gc2xpZGluZy13aW5kb3cgY2hhciBmYWxsYmFja1xuICAgICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgICBsZXQgcyA9IDA7XG4gICAgICB3aGlsZSAocyA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgIGxldCBlID0gcyArIHRhcmdldENoYXJzO1xuICAgICAgICBpZiAoZSA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgICAgY29uc3Qgc2VhcmNoRnJvbSA9IGUgLSBNYXRoLmZsb29yKHRhcmdldENoYXJzICogMC4yKTtcbiAgICAgICAgICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgICAgICAgICAgY29uc3QgaWR4ID0gcGFyYS5sYXN0SW5kZXhPZihicCwgZSk7XG4gICAgICAgICAgICBpZiAoaWR4ID4gc2VhcmNoRnJvbSkgeyBlID0gaWR4ICsgYnAubGVuZ3RoOyBicmVhazsgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBlID0gTWF0aC5taW4oZSwgcGFyYS5sZW5ndGgpO1xuICAgICAgICBjb25zdCBzbGljZSA9IHBhcmEuc2xpY2UocywgZSkudHJpbSgpO1xuICAgICAgICBpZiAoc2xpY2UubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgICAgIHRleHQ6ICAgICAgIHNsaWNlLFxuICAgICAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoc2xpY2UpLFxuICAgICAgICAgICAgY2hhclN0YXJ0OiAgY2hhckN1cnNvciArIHMsXG4gICAgICAgICAgICBjaGFyRW5kOiAgICBjaGFyQ3Vyc29yICsgZSxcbiAgICAgICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IG5leHQgPSBlIC0gb3ZlcmxhcENoYXJzO1xuICAgICAgICBzID0gbmV4dCA+IHMgPyBuZXh0IDogZTtcbiAgICAgIH1cbiAgICAgIGNoYXJDdXJzb3IgKz0gcGFyYS5sZW5ndGggKyAyO1xuICAgICAgYnVmU3RhcnQgICAgPSBjaGFyQ3Vyc29yO1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgLy8gNC4gTm9ybWFsIHBhcmFncmFwaCBcdTIwMTQgaGFyZCBjYXAgbG9va2FoZWFkIEJFRk9SRSBhY2N1bXVsYXRpbmdcbiAgICBpZiAoYnVmZmVyLmxlbmd0aCA+IDAgJiYgKGJ1ZmZlci5sZW5ndGggKyBwYXJhLmxlbmd0aCArIDIpID4gbWF4Q2hhcnMpIHtcbiAgICAgIGZsdXNoKCk7XG4gICAgfVxuXG4gICAgYnVmZmVyICAgICA9IGJ1ZmZlciA/IGJ1ZmZlciArICdcXG5cXG4nICsgcGFyYSA6IHBhcmE7XG4gICAgY2hhckN1cnNvciArPSBwYXJhLmxlbmd0aCArIDI7XG5cbiAgICAvLyBTb2Z0IGNhcDogZmx1c2ggb25jZSB0YXJnZXQgaXMgcmVhY2hlZFxuICAgIGlmIChidWZmZXIubGVuZ3RoID49IHRhcmdldENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDUuIEZsdXNoIHJlbWFpbmRlclxuICBmbHVzaCgpO1xuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgID0gY2h1bmtUZXh0KGNsZWFuZWRUZXh0LCBvcHRpb25zKTtcbiAgY29uc3QgdG90YWxDaHVua3MgPSB0ZXh0Q2h1bmtzLmxlbmd0aDtcbiAgY29uc3Qgc291cmNlVHlwZSAgPSBvcHRpb25zLnNvdXJjZVR5cGUgfHwgJ3BkZic7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+IHtcbiAgICBjb25zdCBjaHVua0lkID0gZ2VuZXJhdGVDaHVua0lkKGNodW5rLnRleHQsIGZpbGVuYW1lKTtcbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogICAgICAgICBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogICAgICBjaHVuay5jaHVua0luZGV4LFxuICAgICAgICB0b3RhbF9jaHVua3M6ICAgICB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6ICAgICAgcGFnZU51bWJlciB8fCAxLFxuICAgICAgICB0b3RhbF9wYWdlczogICAgICB0b3RhbFBhZ2VzIHx8IG51bGwsXG4gICAgICAgIHNlY3Rpb25fdGl0bGU6ICAgIGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICAgIHNvdXJjZV90eXBlOiAgICAgIHNvdXJjZVR5cGUsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogICAgICAgY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogICAgICAgICBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogICAgICBjaHVuay50b2tlbkNvdW50XG4gICAgICB9XG4gICAgfTtcbiAgfSk7XG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RTZWN0aW9uVGl0bGUodGV4dCkge1xuICBjb25zdCBsaW5lcyA9IHRleHQuc3BsaXQoJ1xcbicpLmZpbHRlcihsID0+IGwudHJpbSgpKTtcbiAgaWYgKGxpbmVzLmxlbmd0aCA+IDApIHtcbiAgICBjb25zdCBmaXJzdExpbmUgPSBsaW5lc1swXS50cmltKCk7XG4gICAgaWYgKGZpcnN0TGluZS5sZW5ndGggPCAxMDAgJiYgIWZpcnN0TGluZS5lbmRzV2l0aCgnLicpKSB7XG4gICAgICByZXR1cm4gZmlyc3RMaW5lLnNsaWNlKDAsIDUwKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5BSSB9IGZyb20gJ0Bnb29nbGUvZ2VuYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDEuIFNMSURJTkcgV0lORE9XIFJBVEUgTElNSVRFUlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jbGFzcyBTbGlkaW5nV2luZG93UmF0ZUxpbWl0ZXIge1xuICBjb25zdHJ1Y3RvcihsaW1pdFBlck1pbnV0ZSkge1xuICAgIHRoaXMubGltaXRQZXJNaW51dGUgPSBsaW1pdFBlck1pbnV0ZTtcbiAgICB0aGlzLndpbmRvd01zID0gNjAwMDA7XG4gICAgdGhpcy5yZXF1ZXN0cyA9IFtdO1xuICB9XG5cbiAgYXN5bmMgY29uc3VtZSh0b2tlbnMpIHtcbiAgICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICAgIC8vIFJlbW92ZSBlbnRyaWVzIG9sZGVyIHRoYW4gNjAgc2Vjb25kc1xuICAgIHRoaXMucmVxdWVzdHMgPSB0aGlzLnJlcXVlc3RzLmZpbHRlcihyZXEgPT4gcmVxLnRpbWVzdGFtcCA+IG5vdyAtIHRoaXMud2luZG93TXMpO1xuXG4gICAgY29uc3QgY3VycmVudFRvdGFsID0gdGhpcy5yZXF1ZXN0cy5yZWR1Y2UoKHN1bSwgcmVxKSA9PiBzdW0gKyByZXEudG9rZW5zLCAwKTtcblxuICAgIC8vIElmIHdlIGhhdmUgcm9vbSwgY29uc3VtZSBpbnN0YW50bHkgKGJ1cnN0KVxuICAgIGlmIChjdXJyZW50VG90YWwgKyB0b2tlbnMgPD0gdGhpcy5saW1pdFBlck1pbnV0ZSkge1xuICAgICAgdGhpcy5yZXF1ZXN0cy5wdXNoKHsgdGltZXN0YW1wOiBub3csIHRva2VucyB9KTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBPdGhlcndpc2UsIHdhaXQgdW50aWwgdGhlIG9sZGVzdCByZXF1ZXN0IGV4cGlyZXMgKHBsdXMgYSBzbWFsbCBidWZmZXIpXG4gICAgY29uc3QgbmVlZGVkID0gdG9rZW5zIC0gKHRoaXMubGltaXRQZXJNaW51dGUgLSBjdXJyZW50VG90YWwpO1xuICAgIGxldCBhY2N1bXVsYXRlZEV4cGlyZWQgPSAwO1xuICAgIGxldCB3YWl0VW50aWwgPSBub3cgKyB0aGlzLndpbmRvd01zOyAvLyBmYWxsYmFja1xuXG4gICAgY29uc3Qgc29ydGVkID0gWy4uLnRoaXMucmVxdWVzdHNdLnNvcnQoKGEsIGIpID0+IGEudGltZXN0YW1wIC0gYi50aW1lc3RhbXApO1xuICAgIGZvciAoY29uc3QgcmVxIG9mIHNvcnRlZCkge1xuICAgICAgYWNjdW11bGF0ZWRFeHBpcmVkICs9IHJlcS50b2tlbnM7XG4gICAgICBpZiAoYWNjdW11bGF0ZWRFeHBpcmVkID49IG5lZWRlZCkge1xuICAgICAgICAvLyArMTBtcyBidWZmZXIgdG8gc2xpZGUgdGhlIHdpbmRvdyBjbGVhbmx5XG4gICAgICAgIHdhaXRVbnRpbCA9IHJlcS50aW1lc3RhbXAgKyB0aGlzLndpbmRvd01zICsgMTA7XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGRlbGF5ID0gd2FpdFVudGlsIC0gbm93O1xuICAgIGlmIChkZWxheSA+IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKFxuICAgICAgICBgW3JhdGUtbGltaXRdIFdpbmRvdyBmdWxsICgke2N1cnJlbnRUb3RhbH0vJHt0aGlzLmxpbWl0UGVyTWludXRlfSkuIGAgK1xuICAgICAgICBgV2FpdGluZyAkeyhkZWxheSAvIDEwMDApLnRvRml4ZWQoMSl9cyB0byBzZW5kICR7dG9rZW5zfSB0b2tlbnMuLi5gXG4gICAgICApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIGRlbGF5KSk7XG4gICAgfVxuXG4gICAgLy8gUmVjb3JkIHRoZSBjb25zdW1wdGlvbiBhdCB0aGUgbmV3IHRpbWVcbiAgICB0aGlzLnJlcXVlc3RzLnB1c2goeyB0aW1lc3RhbXA6IERhdGUubm93KCksIHRva2VucyB9KTtcbiAgICAvLyBDbGVhbnVwIGFnYWluIGp1c3QgaW4gY2FzZVxuICAgIHRoaXMucmVxdWVzdHMgPSB0aGlzLnJlcXVlc3RzLmZpbHRlcihyZXEgPT4gcmVxLnRpbWVzdGFtcCA+IERhdGUubm93KCkgLSB0aGlzLndpbmRvd01zKTtcbiAgfVxufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDIuIENPTkZJR1VSQVRJT05cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc3QgVFBNX0xJTUlUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19UUE1fTElNSVQpIHx8IDUwMDAwMDtcbmNvbnN0IFJBVEVfTElNSVRFUiA9IG5ldyBTbGlkaW5nV2luZG93UmF0ZUxpbWl0ZXIoVFBNX0xJTUlUKTtcblxuLy8gQkFUQ0hfU0laRTogbnVtYmVyIG9mIGNodW5rcyBwZXIgZW1iZWRDb250ZW50IGNhbGxcbi8vIChrZXB0IGF0IDEwOyBub3RlIHRoZSByZWFsIGNlaWxpbmcgaXMgdGhlIEFQSSdzIH4xMDAtcmVxdWVzdHMtcGVyLWNhbGwgbGltaXQsXG4vLyBub3QgYSBcImNvbnRleHQgd2luZG93XCIgbGltaXQgXHUyMDE0IDEwIGp1c3Qga2VlcHMgYmF0Y2hlcyBzbWFsbCBhbmQgcmV0cnktZnJpZW5kbHkpXG5jb25zdCBCQVRDSF9TSVpFID0gKCkgPT4gMTA7ICAgLy8gMTAgY2h1bmtzIFx1MDBENyA3NTAgdG9rZW5zID0gNyw1MDAgdG9rZW5zIHBlciBBUEkgcmVxdWVzdFxuY29uc3QgUEFSQUxMRUxfQ0FMTFMgPSAoKSA9PiAxMDsgLy8gU2VuZCAxMCBiYXRjaGVzIGNvbmN1cnJlbnRseSB0byBjbGVhciB0aGUgYnVyc3QgZmFzdFxuXG4vLyBSZXRyeSBjb25maWd1cmF0aW9uIChleHBvbmVudGlhbCBiYWNrb2ZmICsgaml0dGVyKVxuY29uc3QgUkVUUllfQkFTRV9ERUxBWV9NUyA9IDIwMDA7ICAgLy8gMiBzZWNvbmRzXG5jb25zdCBSRVRSWV9NQVhfREVMQVlfTVMgPSA2MDAwMDsgICAvLyA2MCBzZWNvbmRzIGNhcFxuY29uc3QgTUFYX1JFVFJZX0FUVEVNUFRTID0gNTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAzLiBBSSBDTElFTlQgKHNpbmdsZSwgcmV1c2FibGUgaW5zdGFuY2UpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnN0IGFpID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgdmVydGV4YWk6IHRydWUsXG4gIHByb2plY3Q6IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9QUk9KRUNUIHx8IHByb2Nlc3MuZW52LkdDUF9QUk9KRUNUIHx8ICdwcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAnLFxuICBsb2NhdGlvbjogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX0xPQ0FUSU9OIHx8ICd1cy1jZW50cmFsMSdcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDQuIFRPS0VOIENBTENVTEFUSU9OICh1c2VzIHN0b3JlZCB0b2tlbl9jb3VudCBpZiBhdmFpbGFibGUpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmZ1bmN0aW9uIGdldFRva2VuQ291bnRGb3JDaHVua3MoY2h1bmtzKSB7XG4gIHJldHVybiBjaHVua3MucmVkdWNlKChzdW0sIGNodW5rKSA9PiB7XG4gICAgLy8gUHJlZmVyIHRoZSBleGFjdCB0b2tlbiBjb3VudCBmcm9tIGNodW5rZXIsIG90aGVyd2lzZSBmYWxsYmFjayB0byByb3VnaCBlc3RpbWF0ZVxuICAgIGNvbnN0IHRva2VuQ291bnQgPSBjaHVuay5tZXRhZGF0YT8udG9rZW5fY291bnQgfHwgTWF0aC5jZWlsKGNodW5rLnRleHQubGVuZ3RoIC8gNCk7XG4gICAgcmV0dXJuIHN1bSArIHRva2VuQ291bnQ7XG4gIH0sIDApO1xufVxuXG4vLyBTYW1lIHJvdWdoIGVzdGltYXRlIGFzIGFib3ZlLCBidXQgZm9yIHJhdyBzdHJpbmdzIHRoYXQgZG9uJ3QgY2FycnkgY2h1bmsgbWV0YWRhdGFcbi8vICh1c2VkIGZvciByZXRyaWVzIGluc2lkZSBlbWJlZEJhdGNoLCBhbmQgZm9yIGVtYmVkUXVlcnkpLlxuZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykge1xuICByZXR1cm4gdGV4dHMucmVkdWNlKChzdW0sIHRleHQpID0+IHN1bSArIE1hdGguY2VpbChTdHJpbmcodGV4dCkubGVuZ3RoIC8gNCksIDApO1xufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDUuIEVNQkVEIEJBVENIICh3aXRoIGV4cG9uZW50aWFsIGJhY2tvZmYgKyBqaXR0ZXIpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFzeW5jIGZ1bmN0aW9uIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIGF0dGVtcHQgPSAxKSB7XG4gIGNvbnN0IG1vZGVsTmFtZSA9IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMDAxJztcbiAgY29uc3Qgb3V0cHV0RGltZW5zaW9uYWxpdHkgPSBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7XG5cbiAgdHJ5IHtcbiAgICAvLyBGSVg6IGBhaS5iYXRjaGVzLmNyZWF0ZUVtYmVkZGluZ3NgIGlzIG5vdCBhIHJlYWwgbWV0aG9kIG9uIHRoZSBAZ29vZ2xlL2dlbmFpIFNESy5cbiAgICAvLyBgYWkuYmF0Y2hlc2AgaXMgZm9yIGFzeW5jIGJhdGNoLXByZWRpY3Rpb24gam9icy4gU3luY2hyb25vdXMgZW1iZWRkaW5nIGNhbGxzIGdvXG4gICAgLy8gdGhyb3VnaCBgYWkubW9kZWxzLmVtYmVkQ29udGVudGAsIHdpdGggb25lIHNoYXJlZCB0YXNrVHlwZS9vdXRwdXREaW1lbnNpb25hbGl0eVxuICAgIC8vIGNvbmZpZyBhcHBsaWVkIGFjcm9zcyBhbGwgYGNvbnRlbnRzYCBpbiB0aGUgY2FsbC5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IGFpLm1vZGVscy5lbWJlZENvbnRlbnQoe1xuICAgICAgbW9kZWw6IG1vZGVsTmFtZSxcbiAgICAgIGNvbnRlbnRzOiB0ZXh0cy5tYXAodGV4dCA9PiAodHlwZW9mIHRleHQgPT09ICdzdHJpbmcnID8gdGV4dCA6IFN0cmluZyh0ZXh0KSkpLFxuICAgICAgY29uZmlnOiB7XG4gICAgICAgIHRhc2tUeXBlOiB0YXNrVHlwZSxcbiAgICAgICAgb3V0cHV0RGltZW5zaW9uYWxpdHk6IG91dHB1dERpbWVuc2lvbmFsaXR5XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjb25zdCBlbWJlZGRpbmdzID0gcmVzcG9uc2U/LmVtYmVkZGluZ3M/Lm1hcChlID0+IGUudmFsdWVzKSB8fCBbXTtcbiAgICBpZiAoZW1iZWRkaW5ncy5sZW5ndGggIT09IHRleHRzLmxlbmd0aCkge1xuICAgICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGBFeHBlY3RlZCAke3RleHRzLmxlbmd0aH0gZW1iZWRkaW5ncywgZ290ICR7ZW1iZWRkaW5ncy5sZW5ndGh9YCk7XG4gICAgfVxuICAgIHJldHVybiBlbWJlZGRpbmdzO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc3QgaXNSZXRyeWFibGUgPSBpczQyOUVycm9yKGVycm9yKSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDIgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMyB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdTZXJ2aWNlIFVuYXZhaWxhYmxlJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnQmFkIEdhdGV3YXknKTtcblxuICAgIGlmIChpc1JldHJ5YWJsZSAmJiBhdHRlbXB0IDwgTUFYX1JFVFJZX0FUVEVNUFRTKSB7XG4gICAgICAvLyBFeHBvbmVudGlhbCBiYWNrb2ZmOiAyXmF0dGVtcHQgKiBiYXNlIChjYXBwZWQpXG4gICAgICBsZXQgZGVsYXkgPSBNYXRoLm1pbihSRVRSWV9NQVhfREVMQVlfTVMsIFJFVFJZX0JBU0VfREVMQVlfTVMgKiBNYXRoLnBvdygyLCBhdHRlbXB0IC0gMSkpO1xuICAgICAgLy8gQWRkIGppdHRlciAoMC44XHUyMDEzMS4yeCkgdG8gYXZvaWQgdGh1bmRlcmluZyBoZXJkXG4gICAgICBjb25zdCBqaXR0ZXIgPSAwLjggKyAoMC40ICogTWF0aC5yYW5kb20oKSk7XG4gICAgICBkZWxheSA9IE1hdGguZmxvb3IoZGVsYXkgKiBqaXR0ZXIpO1xuICAgICAgLy8gUmVzcGVjdCByZXRyeS1hZnRlciBoZWFkZXIgaWYgcHJlc2VudFxuICAgICAgaWYgKGVycm9yLnJldHJ5QWZ0ZXIpIHtcbiAgICAgICAgZGVsYXkgPSBNYXRoLm1heChkZWxheSwgZXJyb3IucmV0cnlBZnRlciAqIDEwMDApO1xuICAgICAgfVxuXG4gICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgYFtlbWJlZGRpbmddIFx1MjNGMyBSZXRyeWFibGUgZXJyb3IgKCR7ZXJyb3I/LnN0YXR1cyB8fCAndW5rbm93bid9KSwgYCArXG4gICAgICAgIGB3YWl0aW5nICR7KGRlbGF5IC8gMTAwMCkudG9GaXhlZCgxKX1zIChhdHRlbXB0ICR7YXR0ZW1wdH0vJHtNQVhfUkVUUllfQVRURU1QVFN9KS4uLmBcbiAgICAgICk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgZGVsYXkpKTtcblxuICAgICAgLy8gRklYOiBhIHJldHJ5IGlzIGEgYnJhbmQgbmV3IEFQSSBjYWxsIGFuZCBjb25zdW1lcyByZWFsIHF1b3RhLCBldmVuIHRob3VnaFxuICAgICAgLy8gdGhlIG9yaWdpbmFsIGNhbGwgZmFpbGVkLiBTa2lwcGluZyBjb25zdW1wdGlvbiBoZXJlIChhcyBiZWZvcmUpIGxldCB0aGUgbG9jYWxcbiAgICAgIC8vIGxpbWl0ZXIgdW5kZXItcmVwb3J0IGFjdHVhbCB1c2FnZSBkdXJpbmcgZXJyb3Igc3Rvcm1zLCB3aGljaCBtZWFudCBpdCBrZXB0XG4gICAgICAvLyB3YXZpbmcgdGhyb3VnaCBuZXcgZ3JvdXBzIHdoaWxlIHJldHJpZXMgd2VyZSBhbHNvIGhpdHRpbmcgdGhlIEFQSSBcdTIwMTQgbWFraW5nXG4gICAgICAvLyA0Mjkgc3Rvcm1zIHdvcnNlIGluc3RlYWQgb2YgYmFja2luZyBvZmYgZnJvbSB0aGVtLlxuICAgICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykpO1xuXG4gICAgICByZXR1cm4gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUsIGF0dGVtcHQgKyAxKTtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnQmF0Y2ggZW1iZWRkaW5nIGZhaWxlZCcpO1xuICB9XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNi4gRVhQT1JURUQgZ2VuZXJhdGVFbWJlZGRpbmdzICh3aXRoIHJhdGUgbGltaXRlciAmIGFjY3VyYXRlIHRva2Vucylcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSA9IEJBVENIX1NJWkUoKTtcbiAgY29uc3QgcGFyYWxsZWxDYWxscyA9IFBBUkFMTEVMX0NBTExTKCk7XG5cbiAgLy8gRml4ZWQtc2l6ZSBhcnJheSB0byBwcmVzZXJ2ZSBjaHJvbm9sb2dpY2FsIG9yZGVyXG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBuZXcgQXJyYXkoY2h1bmtzLmxlbmd0aCk7XG5cbiAgLy8gR3JvdXAgY2h1bmtzIGludG8gYmF0Y2hlcyB3aXRoIHRoZWlyIHN0YXJ0aW5nIGluZGV4XG4gIGNvbnN0IGJhdGNoZXMgPSBbXTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IGJhdGNoU2l6ZSkge1xuICAgIGJhdGNoZXMucHVzaCh7XG4gICAgICBjaHVua3M6IGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSxcbiAgICAgIHN0YXJ0SW5kZXg6IGlcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG5cbiAgICAvLyBDYWxjdWxhdGUgZXhhY3QgdG9rZW5zIHVzaW5nIHN0b3JlZCB0b2tlbl9jb3VudCAob3IgZmFsbGJhY2spXG4gICAgY29uc3QgYWxsQ2h1bmtzSW5Hcm91cCA9IHBhcmFsbGVsQmF0Y2hlcy5mbGF0TWFwKGIgPT4gYi5jaHVua3MpO1xuICAgIGNvbnN0IHRva2Vuc1RvQ29uc3VtZSA9IGdldFRva2VuQ291bnRGb3JDaHVua3MoYWxsQ2h1bmtzSW5Hcm91cCk7XG4gICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUodG9rZW5zVG9Db25zdW1lKTtcblxuICAgIGNvbnNvbGUubG9nKFxuICAgICAgYFtlbWJlZGRpbmddIEdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCBmaXJpbmcgJHtwYXJhbGxlbEJhdGNoZXMubGVuZ3RofSBiYXRjaGVzIGAgK1xuICAgICAgYGluIHBhcmFsbGVsICgke3Rva2Vuc1RvQ29uc3VtZX0gdG9rZW5zKWBcbiAgICApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHBhcmFsbGVsQmF0Y2hlcy5tYXAoYiA9PiBlbWJlZEJhdGNoKGIuY2h1bmtzLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgY29uc3QgZmFpbGVkQmF0Y2hlcyA9IFtdO1xuICAgIHJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgY29uc3QgY3VycmVudEJhdGNoSW5mbyA9IHBhcmFsbGVsQmF0Y2hlc1tiYXRjaElkeF07XG4gICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgY29uc3QgdmVjdG9ycyA9IHJlc3VsdC52YWx1ZTtcbiAgICAgICAgY3VycmVudEJhdGNoSW5mby5jaHVua3MuZm9yRWFjaCgoY2h1bmssIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgY29uc3QgZ2xvYmFsSW5kZXggPSBjdXJyZW50QmF0Y2hJbmZvLnN0YXJ0SW5kZXggKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzW2dsb2JhbEluZGV4XSA9IHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rXyR7Z2xvYmFsSW5kZXh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1tjaHVua0lkeF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfTtcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtlbWJlZGRpbmddIEJhdGNoIHN0YXJ0aW5nIGF0IGluZGV4ICR7Y3VycmVudEJhdGNoSW5mby5zdGFydEluZGV4fSBmYWlsZWQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZEJhdGNoZXMucHVzaChjdXJyZW50QmF0Y2hJbmZvKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIC8vIFJldHJ5IGZhaWxlZCBiYXRjaGVzIGluZGl2aWR1YWxseVxuICAgIGZvciAoY29uc3QgZmFpbGVkQmF0Y2ggb2YgZmFpbGVkQmF0Y2hlcykge1xuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFJldHJ5aW5nIGZhaWxlZCBiYXRjaCBlbGVtZW50cyBzdGFydGluZyBhdCBpbmRleCAke2ZhaWxlZEJhdGNoLnN0YXJ0SW5kZXh9Li4uYCk7XG4gICAgICBmb3IgKGxldCBjaHVua0lkeCA9IDA7IGNodW5rSWR4IDwgZmFpbGVkQmF0Y2guY2h1bmtzLmxlbmd0aDsgY2h1bmtJZHgrKykge1xuICAgICAgICBjb25zdCBjaHVuayA9IGZhaWxlZEJhdGNoLmNodW5rc1tjaHVua0lkeF07XG4gICAgICAgIGNvbnN0IGdsb2JhbEluZGV4ID0gZmFpbGVkQmF0Y2guc3RhcnRJbmRleCArIGNodW5rSWR4O1xuICAgICAgICB0cnkge1xuICAgICAgICAgIC8vIEZJWDogdGhpcyByZXRyeSBpcyBhIGZyZXNoLCByZWFsIEFQSSBjYWxsIFx1MjAxNCB0cmFjayBpdHMgdG9rZW5zIGFnYWluc3RcbiAgICAgICAgICAvLyB0aGUgbGltaXRlciBpbnN0ZWFkIG9mIGFzc3VtaW5nIGl0IHdhcyBcImFscmVhZHkgcGFpZCBmb3JcIi5cbiAgICAgICAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKFtjaHVua10pKTtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzW2dsb2JhbEluZGV4XSA9IHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rX3JldHJ5XyR7Z2xvYmFsSW5kZXh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1swXSxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVuay5tZXRhZGF0YSxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rLnRleHRcbiAgICAgICAgICB9O1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBnbG9iYWxJbmRleH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW2VtYmVkZGluZ10gXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgZ2xvYmFsSW5kZXh9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8vIEZJWDogcGVybWFuZW50bHktZmFpbGVkIGNodW5rcyBhcmUgZHJvcHBlZCBoZXJlLCB3aGljaCBzaGlmdHMgYXJyYXkgaW5kaWNlc1xuICAvLyByZWxhdGl2ZSB0byB0aGUgb3JpZ2luYWwgYGNodW5rc2AgaW5wdXQuIFRoaXMgbG9nIG1ha2VzIHRoYXQgbG9zcyB2aXNpYmxlXG4gIC8vIGluc3RlYWQgb2Ygc2lsZW50OyBjYWxsZXJzIHRoYXQgbmVlZCB0byBrbm93IGV4YWN0bHkgd2hpY2ggY2h1bmtzIHdlcmUgbG9zdFxuICAvLyBjYW4gY29tcGFyZSByZXR1cm5lZCBgaWRgcyBhZ2FpbnN0IHRoZWlyIG9yaWdpbmFsIGNodW5rIGxpc3QuXG4gIGNvbnN0IGZhaWxlZENvdW50ID0gZW1iZWRkaW5ncy5maWx0ZXIoZSA9PiAhZSkubGVuZ3RoO1xuICBpZiAoZmFpbGVkQ291bnQgPiAwKSB7XG4gICAgY29uc29sZS53YXJuKGBbZW1iZWRkaW5nXSAke2ZhaWxlZENvdW50fS8ke2NodW5rcy5sZW5ndGh9IGNodW5rKHMpIHBlcm1hbmVudGx5IGZhaWxlZCB0byBlbWJlZCBhbmQgd2VyZSBkcm9wcGVkLmApO1xuICB9XG5cbiAgLy8gRmlsdGVyIG91dCBhbnkgZWxlbWVudHMgdGhhdCBwZXJtYW5lbnRseSBmYWlsZWRcbiAgcmV0dXJuIGVtYmVkZGluZ3MuZmlsdGVyKEJvb2xlYW4pO1xufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDcuIEVYUE9SVEVEIGVtYmVkUXVlcnlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgLy8gRklYOiB0aGlzIGNhbGwgd2FzIGJ5cGFzc2luZyB0aGUgcmF0ZSBsaW1pdGVyIGVudGlyZWx5LiBJZiBpdCBydW5zIGNvbmN1cnJlbnRseVxuICAvLyB3aXRoIGRvY3VtZW50IGluZ2VzdGlvbiAoZS5nLiBhIHVzZXIgc2VhcmNoZXMgd2hpbGUgYSBiYXRjaCBqb2IgaXMgaW4gZmxpZ2h0KSxcbiAgLy8gaXQgY291bGQgcHVzaCB0b3RhbCB1c2FnZSBvdmVyIHRoZSBjb25maWd1cmVkIFRQTSBidWRnZXQgdW5ub3RpY2VkLlxuICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKFtxdWVyeV0pKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3F1ZXJ5XSwgJ1JFVFJJRVZBTF9RVUVSWScpO1xuICByZXR1cm4gdmVjdG9yc1swXTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJykge1xuICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gZW1iZWRTaW5nbGVCYXRjaEdyb3VwIFx1MjAxNCAke3RleHRzLmxlbmd0aH0gdGV4dHMsIHRhc2tUeXBlPSR7dGFza1R5cGV9YCk7XG4gIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlKTtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgZ290ICR7dmVjdG9ycy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgcmV0dXJuIHZlY3RvcnM7XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7XG4gIGdldEdsb2JhbENvbGxlY3Rpb24sXG4gIGdldENvbGxlY3Rpb24sXG4gIGxpc3REb2N1bWVudHNcbn0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcblxuY29uc3QgREVGQVVMVF9USU1FT1VUX01JTlVURVMgPSA2MDtcbmNvbnN0IHNlc3Npb25zID0gbmV3IE1hcCgpO1xuY29uc3QgTUFYX1BERlNfUEVSX1NFU1NJT04gPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTikgfHwgMztcbmNvbnN0IE1BWF9VUExPQURfU0laRV9NQiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQikgfHwgNTtcblxuY29uc3Qgc2VlZGVkU2Vzc2lvbnMgPSBuZXcgU2V0KCk7XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBHbG9iYWwgZG9jdW1lbnRzIGNhY2hlIChwb3B1bGF0ZWQgb25jZSBvbiBmaXJzdCBzZXNzaW9uIGluaXQpIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxubGV0IGdsb2JhbERvY3VtZW50c0NhY2hlID0gW107XG5sZXQgZ2xvYmFsRGF0YUluaXRpYWxpemVkID0gZmFsc2U7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRHbG9iYWxEb2N1bWVudHNDYWNoZSgpIHtcbiAgcmV0dXJuIGdsb2JhbERvY3VtZW50c0NhY2hlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3QgaWQgPSBzZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IHNlc3Npb24gPSB7XG4gICAgaWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIGRlbGV0ZWREb2N1bWVudElkczogbmV3IFNldCgpLFxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuICBzZXNzaW9ucy5zZXQoaWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykgcmV0dXJuIGV4aXN0aW5nO1xuICAgIHJldHVybiBjcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gIH1cbiAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikge1xuICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICBjb25zdCBsYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZShzZXNzaW9uLmxhc3RBY2Nlc3NlZCkuZ2V0VGltZSgpO1xuICBjb25zdCB0aW1lb3V0TXMgPSBzZXNzaW9uLnRpbWVvdXRNaW51dGVzICogNjAgKiAxMDAwO1xuICByZXR1cm4gKG5vdyAtIGxhc3RBY2Nlc3NlZCkgPiB0aW1lb3V0TXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBzZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgc2VlZGVkU2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBDaGVjayBpZiBzZXNzaW9uIGlzIHNlZWRlZCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25TZWVkZWQoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIE5vdGlmeSBTU0UgbGlzdGVuZXJzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZnVuY3Rpb24gbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCkge1xuICBpZiAoZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMgJiYgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuaGFzKGBzZWVkaW5nOiR7c2Vzc2lvbklkfWApKSB7XG4gICAgY29uc3QgZXZlbnRLZXkgPSBgc2VlZGluZzoke3Nlc3Npb25JZH1gO1xuICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgbGlzdGVuZXJzLmZvckVhY2goKHJlc3BvbnNlKSA9PiB7XG4gICAgICB0cnkge1xuICAgICAgICByZXNwb25zZS53cml0ZShgZXZlbnQ6IHNlZWRpbmdfY29tcGxldGVcXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KHsgc2Vzc2lvbklkLCBzZWVkZWQ6IHRydWUgfSl9XFxuXFxuYCk7XG4gICAgICAgIHJlc3BvbnNlLmVuZCgpO1xuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoYFtub3RpZnldIEZhaWxlZCB0byBub3RpZnkgbGlzdGVuZXI6YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgY29uc29sZS5sb2coYFtub3RpZnldIE5vdGlmaWVkICR7bGlzdGVuZXJzLmxlbmd0aH0gU1NFIGxpc3RlbmVycyBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgfVxufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIFJlY29uc3RydWN0IGluLW1lbW9yeSBzZXNzaW9uIGRvYyBsaXN0IGZyb20gdGhlIHNpbmdsZSBjb2xsZWN0aW9uXG4gKiAgIGJ5IGZpbHRlcmluZyBvbiBzZXNzaW9uX2lkIG1ldGFkYXRhLlxuICogLSBObyB2ZWN0b3IgY29weWluZyBpcyBwZXJmb3JtZWQgXHUyMDE0IGdsb2JhbCBkb2NzIGFyZSBzZXJ2ZWQgZnJvbSBjYWNoZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKSB7XG4gIGNvbnNvbGUubG9nKGBcdUQ4M0RcdUREMTEgU2Vzc2lvbiBpbml0OiAke3Nlc3Npb25JZH1gKTtcbiAgaWYgKHNlZWRlZFNlc3Npb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBBbHJlYWR5IHNlZWRlZCAke3Nlc3Npb25JZH0sIHNraXBwaW5nYCk7XG4gICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuXG4gICAgLy8gXHUyNTAwXHUyNTAwIExhenkgb25lLXRpbWUgZ2xvYmFsIGNhY2hlIGluaXQgKHJ1bnMgb24gZmlyc3Qgc2Vzc2lvbiBpbml0KSBcdTI1MDBcdTI1MDBcbiAgICBpZiAoIWdsb2JhbERhdGFJbml0aWFsaXplZCkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgZ2xvYmFsRG9jcyA9IGF3YWl0IGxpc3REb2N1bWVudHMoY29sbGVjdGlvbiwgeyBzZXNzaW9uX2lkOiAnZ2xvYmFsJyB9KTtcbiAgICAgICAgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUgPSBnbG9iYWxEb2NzLm1hcChkb2MgPT4gKHtcbiAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgIGNodW5rQ291bnQ6IGRvYy5jaHVua19jb3VudCxcbiAgICAgICAgICBzb3VyY2VUeXBlOiAnZ2xvYmFsJyxcbiAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IGRvYy51cGxvYWRfdGltZXN0YW1wXG4gICAgICAgIH0pKTtcbiAgICAgICAgZ2xvYmFsRGF0YUluaXRpYWxpemVkID0gdHJ1ZTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBHbG9iYWwgZG9jdW1lbnRzIGNhY2hlIGxvYWRlZDogJHtnbG9iYWxEb2N1bWVudHNDYWNoZS5sZW5ndGh9IGRvY3VtZW50KHMpYCk7XG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcignXHUyNzRDIEZhaWxlZCB0byBpbml0aWFsaXplIGdsb2JhbCBkYXRhOicsIGVyci5tZXNzYWdlKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcblxuICAgIC8vIFJlY29uc3RydWN0IHNlc3Npb24tc3BlY2lmaWMgZG9jcyAodXNlciB1cGxvYWRzKSBmcm9tIHRoZSBjb2xsZWN0aW9uXG4gICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICBjb25zdCBkb2NzID0gYXdhaXQgbGlzdERvY3VtZW50cyhjb2xsZWN0aW9uLCB7IHNlc3Npb25faWQ6IHNlc3Npb25JZCB9KTtcbiAgICAgIGRvY3MuZm9yRWFjaChkb2MgPT4ge1xuICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgIGNodW5rQ291bnQ6IGRvYy5jaHVua19jb3VudCxcbiAgICAgICAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogZG9jLnVwbG9hZF90aW1lc3RhbXBcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICAgIGlmIChkb2NzLmxlbmd0aCA+IDApIHtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjY3Qlx1RkUwRiAgUmVjb25zdHJ1Y3RlZCAke2RvY3MubGVuZ3RofSBzZXNzaW9uIGRvY3VtZW50KHMpIGZvciAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cbiAgICB9XG4gICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgY29uc29sZS5sb2coYFx1MjcwNSBTZXNzaW9uICR7c2Vzc2lvbklkfSByZWFkeSAobm8gdmVjdG9yIGNvcHlpbmcgbmVlZGVkKWApO1xuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgXHUyNzRDIEZhaWxlZCB0byBpbml0IHNlc3Npb24gJHtzZXNzaW9uSWR9OmAsIGVycm9yLm1lc3NhZ2UpO1xuICAgIC8vIFN0aWxsIG5vdGlmeSBsaXN0ZW5lcnMgc28gdGhleSBkb24ndCBoYW5nIGZvcmV2ZXJcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgRG9jdW1lbnQgbWFuYWdlbWVudCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBmdW5jdGlvbiBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SW5mbykge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcblxuICBjb25zdCBleGlzdGluZyA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmQoZCA9PiBkLmlkID09PSBkb2N1bWVudEluZm8uaWQpO1xuXG4gIGlmIChleGlzdGluZykge1xuICAgIGlmIChkb2N1bWVudEluZm8uY2h1bmtDb3VudCAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5jaHVua0NvdW50ID0gZG9jdW1lbnRJbmZvLmNodW5rQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5wYWdlQ291bnQgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcucGFnZUNvdW50ID0gZG9jdW1lbnRJbmZvLnBhZ2VDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVTaXplICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVTaXplID0gZG9jdW1lbnRJbmZvLmZpbGVTaXplO1xuICAgIGlmIChkb2N1bWVudEluZm8uc3RhdHVzICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnN0YXR1cyA9IGRvY3VtZW50SW5mby5zdGF0dXM7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlbmFtZSAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlbmFtZSA9IGRvY3VtZW50SW5mby5maWxlbmFtZTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBVcGRhdGVkIGRvYyAke2RvY3VtZW50SW5mby5pZH0gXHUyMDE0IHN0YXR1cz0ke2V4aXN0aW5nLnN0YXR1c30sIGNodW5rcz0ke2V4aXN0aW5nLmNodW5rQ291bnR9YCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICBpZDogZG9jdW1lbnRJbmZvLmlkLFxuICAgIGZpbGVuYW1lOiBkb2N1bWVudEluZm8uZmlsZW5hbWUsXG4gICAgZmlsZVNpemU6IGRvY3VtZW50SW5mby5maWxlU2l6ZSxcbiAgICBwYWdlQ291bnQ6IGRvY3VtZW50SW5mby5wYWdlQ291bnQsXG4gICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIGNodW5rQ291bnQ6IGRvY3VtZW50SW5mby5jaHVua0NvdW50ID8/IDAsXG4gICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICBzdGF0dXM6IGRvY3VtZW50SW5mby5zdGF0dXMgPz8gJ2luZGV4aW5nJ1xuICB9KTtcbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIEFkZGVkIGRvYyAke2RvY3VtZW50SW5mby5pZH0gXHUyMDE0IHN0YXR1cz0ke2RvY3VtZW50SW5mby5zdGF0dXMgPz8gJ2luZGV4aW5nJ31gKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiAnU2Vzc2lvbiBub3QgZm91bmQnIH07XG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgaWYgKHVwbG9hZGVkQ291bnQgPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246IGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gIH07XG4gIH1cbiAgcmV0dXJuIHsgY2FuVXBsb2FkOiB0cnVlIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVVwbG9hZChzZXNzaW9uSWQsIGZpbGUsIGZpbGVuYW1lKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGNvbnN0IGVycm9ycyA9IFtdO1xuXG4gIGlmIChmaWxlLnNpemUgPiBNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIGV4Y2VlZHMgJHtNQVhfVVBMT0FEX1NJWkVfTUJ9TUIgbGltaXRgKTtcbiAgfVxuXG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uXG4gICAgPyBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aFxuICAgIDogMDtcblxuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIGVycm9ycy5wdXNoKGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gKTtcbiAgfVxuXG4gIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBmaWxlbmFtZSkpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCk7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGlzVmFsaWQ6IGVycm9ycy5sZW5ndGggPT09IDAsXG4gICAgZXJyb3JzLFxuICAgIGlzTGFyZ2VGaWxlOiBmaWxlLnNpemUgPiAoTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQgKiAwLjYpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcbiAgY29uc3QgaWR4ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZEluZGV4KGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJZCk7XG4gIGlmIChpZHggPj0gMCkge1xuICAgIHNlc3Npb24uZG9jdW1lbnRzLnNwbGljZShpZHgsIDEpO1xuICAgIHNlc3Npb24uZGVsZXRlZERvY3VtZW50SWRzLmFkZChkb2N1bWVudElkKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBSZW1vdmVkIGRvYyAke2RvY3VtZW50SWR9LCBhZGRlZCB0byBkZWxldGVkRG9jdW1lbnRJZHNgKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIHJldHVybiBzZXNzaW9uPy5kZWxldGVkRG9jdW1lbnRJZHMgPz8gbmV3IFNldCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gW107XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBzZXNzaW9uRG9jdW1lbnRzOiBbXSwgZ2xvYmFsRG9jdW1lbnRzOiBbXSB9O1xuXG4gIGNvbnN0IG5vcm1hbGl6ZSA9IChkb2MpID0+ICh7XG4gICAgZG9jdW1lbnRfaWQ6IGRvYy5pZCxcbiAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgIGNodW5rX2NvdW50OiBkb2MuY2h1bmtDb3VudCA/PyAwLFxuICAgIHBhZ2VfY291bnQ6IGRvYy5wYWdlQ291bnQgPz8gMCxcbiAgICB1cGxvYWRfdGltZXN0YW1wOiBkb2MudXBsb2FkVGltZXN0YW1wIHx8IG51bGwsXG4gICAgc291cmNlX3R5cGU6IGRvYy5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ3Nlc3Npb25fdXBsb2FkJyA6ICdzZWVkJyxcbiAgICBmaWxlU2l6ZTogZG9jLmZpbGVTaXplIHx8IG51bGwsXG4gICAgc3RhdHVzOiBkb2Muc3RhdHVzID8/IG51bGxcbiAgfSk7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uLmRvY3VtZW50c1xuICAgICAgLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJylcbiAgICAgIC5tYXAobm9ybWFsaXplKSxcbiAgICBnbG9iYWxEb2N1bWVudHM6IGdsb2JhbERvY3VtZW50c0NhY2hlXG4gICAgICAubWFwKG5vcm1hbGl6ZSlcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25TdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgcmV0dXJuIHtcbiAgICBpZDogc2Vzc2lvbi5pZCxcbiAgICBkb2N1bWVudENvdW50OiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggKyBnbG9iYWxEb2N1bWVudHNDYWNoZS5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBzZXNzaW9uLmNyZWF0ZWRBdCxcbiAgICBsYXN0QWNjZXNzZWQ6IHNlc3Npb24ubGFzdEFjY2Vzc2VkLFxuICAgIHRvdGFsU2l6ZTogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmZpbGVTaXplIHx8IDApLCAwKSxcbiAgICB0b3RhbENodW5rczogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gICAgICArIGdsb2JhbERvY3VtZW50c0NhY2hlLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdEFjdGl2ZVNlc3Npb25zKCkge1xuICByZXR1cm4gQXJyYXkuZnJvbShzZXNzaW9ucy52YWx1ZXMoKSkuZmlsdGVyKHMgPT4gIWlzU2Vzc2lvbkV4cGlyZWQocykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW51cEV4cGlyZWRTZXNzaW9ucygpIHtcbiAgbGV0IGNsZWFuZWQgPSAwO1xuICBmb3IgKGNvbnN0IFtpZCwgc2Vzc2lvbl0gb2Ygc2Vzc2lvbnMpIHtcbiAgICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgICAgc2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBjbGVhbmVkKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBjbGVhbmVkO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IG1lbW9yeU1hcCA9IG5ldyBNYXAoKTtcbmNvbnN0IERFRkFVTFRfTUVNT1JZX1dJTkRPVyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IDEwO1xuXG5leHBvcnQgZnVuY3Rpb24gaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpIHtcbiAgaWYgKCFtZW1vcnlNYXAuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBtZW1vcnlNYXAuc2V0KHNlc3Npb25JZCwge1xuICAgICAgdHVybnM6IFtdLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBtZXRhZGF0YSA9IHt9KSB7XG4gIGNvbnN0IG1lbW9yeSA9IG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IG1heFR1cm5zID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuXG4gIGNvbnN0IHR1cm4gPSB7XG4gICAgaWQ6IGB0dXJuXyR7RGF0ZS5ub3coKX1fJHtNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zdWJzdHIoMiwgOSl9YCxcbiAgICByb2xlLFxuICAgIGNvbnRlbnQsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIC4uLm1ldGFkYXRhXG4gIH07XG5cbiAgbWVtb3J5LnR1cm5zLnB1c2godHVybik7XG5cbiAgaWYgKG1lbW9yeS50dXJucy5sZW5ndGggPiBtYXhUdXJucykge1xuICAgIG1lbW9yeS50dXJucyA9IG1lbW9yeS50dXJucy5zbGljZSgtbWF4VHVybnMpO1xuICB9XG5cbiAgcmV0dXJuIHR1cm47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCBtYXhUdXJucyA9IG51bGwpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IGxpbWl0ID0gbWF4VHVybnMgfHwgcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuICByZXR1cm4gbWVtb3J5LnR1cm5zLnNsaWNlKC1saW1pdCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb252ZXJzYXRpb25Db250ZXh0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIHJldHVybiB0dXJucy5tYXAodCA9PiAoe1xuICAgIHJvbGU6IHQucm9sZSxcbiAgICBjb250ZW50OiB0LmNvbnRlbnRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIGlmICh0dXJucy5sZW5ndGggPT09IDApIHJldHVybiAnJztcblxuICByZXR1cm4gdHVybnMubWFwKHQgPT4ge1xuICAgIGNvbnN0IHByZWZpeCA9IHQucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXI6JyA6ICdBc3Npc3RhbnQ6JztcbiAgICByZXR1cm4gYCR7cHJlZml4fSAke3QuY29udGVudH1gO1xuICB9KS5qb2luKCdcXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFyTWVtb3J5KHNlc3Npb25JZCkge1xuICBtZW1vcnlNYXAuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnlTdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIHJldHVybiB7XG4gICAgdHVybkNvdW50OiBtZW1vcnkudHVybnMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogbWVtb3J5LmNyZWF0ZWRBdCxcbiAgICBsYXN0VHVybkF0OiBtZW1vcnkudHVybnMubGVuZ3RoID4gMCA/IG1lbW9yeS50dXJuc1ttZW1vcnkudHVybnMubGVuZ3RoIC0gMV0udGltZXN0YW1wIDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBjaXRhdGlvbnMgPSBbXSwgY292ZXJhZ2UgPSBudWxsLCBhbnN3ZXJJZCA9IG51bGwpIHtcbiAgcmV0dXJuIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCB7XG4gICAgLi4uKGFuc3dlcklkICYmIHsgaWQ6IGFuc3dlcklkIH0pLFxuICAgIGNpdGF0aW9ucyxcbiAgICBjb3ZlcmFnZSxcbiAgICBoYXNDaXRhdGlvbnM6IGNpdGF0aW9ucy5sZW5ndGggPiAwXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdFVzZXJNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ3VzZXInKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50JykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IGZzIGZyb20gJ2ZzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5pbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbmltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUgfSBmcm9tICcuLi91dGlscy9zYW5pdGl6ZS5qcyc7XG5pbXBvcnQge1xuICBDb3JydXB0ZWRQREZFcnJvcixcbiAgSW52YWxpZEZpbGVUeXBlRXJyb3IsXG59IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5pbXBvcnQgeyBnZXRDb2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNodW5rVGV4dCwgY2xlYW5UZXh0IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG5pbXBvcnQgeyBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7XG4gIGdldE9yQ3JlYXRlU2Vzc2lvbixcbiAgYWRkRG9jdW1lbnRUb1Nlc3Npb24sXG4gIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sXG4gIGdldEFsbERvY3VtZW50cyxcbiAgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyxcbiAgaXNTZXNzaW9uU2VlZGVkXG59IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNsZWFyTWVtb3J5IH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG5jb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbmlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xufVxuXG5jb25zdCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL3NlZWRfZG9jdW1lbnRzJyk7XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBTU0UgZXZlbnQgaGVscGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZnVuY3Rpb24gc3NlRXZlbnQocmVzLCBldmVudCwgZGF0YSkge1xuICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG59XG5cbmNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHVwbG9hZERpciksXG4gIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG59KTtcblxuY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgc3RvcmFnZSxcbiAgbGltaXRzOiB7IGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0IH0sXG4gIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICBjYihudWxsLCB0cnVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgIH1cbiAgfVxufSk7XG5cbmZ1bmN0aW9uIGNvbnRlbnREaXNwb3NpdGlvbihkaXNwbGF5TmFtZSkge1xuICBjb25zdCBlbmNvZGVkID0gZW5jb2RlVVJJQ29tcG9uZW50KGRpc3BsYXlOYW1lKVxuICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgIC5yZXBsYWNlKC9cXCgvZywgJyUyOCcpXG4gICAgLnJlcGxhY2UoL1xcKS9nLCAnJTI5Jyk7XG4gIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xufVxuXG5hc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICB0cnkge1xuICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IHRjLml0ZW1zLm1hcChpID0+IGkuc3RyKS5qb2luKCcgJyk7XG4gICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgIGNvbnN0IHBhZ2VNYXAgPSBbXTtcbiAgICBsZXQgY2hhclBvcyA9IDA7XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICB9XG5cbiAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXRQYWdlTnVtYmVyKGNoYXJTdGFydCwgcGFnZU1hcCkge1xuICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICBpZiAoY2hhclN0YXJ0ID49IGVudHJ5LnN0YXJ0ICYmIGNoYXJTdGFydCA8PSBlbnRyeS5lbmQpIHJldHVybiBlbnRyeS5wYWdlO1xuICB9XG4gIHJldHVybiBwYWdlTWFwW3BhZ2VNYXAubGVuZ3RoIC0gMV0/LnBhZ2UgfHwgMTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFVwbG9hZCBoYW5kbGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gIGNvbnN0IEJBVENIX1NJWkUgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgMTA7XG4gIGNvbnN0IFBBUkFMTEVMX0NBTExTID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCAxMDtcbiAgY29uc3QgR1JPVVBfV0FJVF9NUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19HUk9VUF9XQUlUX01TKSB8fCAxO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLmJvZHkuc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICAgIGNvbnN0IHNlc3Npb24gPSBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBjb25zdCBtYXhQREZzID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04gfHwgJzMnKTtcbiAgICBjb25zdCBjbGVhbkZpbGVuYW1lID0gc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSk7XG5cbiAgICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGg7XG4gICAgaWYgKHVwbG9hZGVkQ291bnQgPj0gbWF4UERGcykge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBNYXhpbXVtICR7bWF4UERGc30gdXBsb2FkcyByZWFjaGVkYCwgY29kZTogJ1RPT19NQU5ZX1BERlMnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGNsZWFuRmlsZW5hbWUpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogYFwiJHtjbGVhbkZpbGVuYW1lfVwiIGFscmVhZHkgdXBsb2FkZWRgLCBjb2RlOiAnRFVQTElDQVRFX0ZJTEUnIH0pO1xuICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMSBcdTIwMTQgcGFyc2luZyAke2NsZWFuRmlsZW5hbWV9ICgke2ZpbGUuc2l6ZX0gYnl0ZXMpYCk7XG4gICAgY29uc3QgeyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9ID0gYXdhaXQgcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZS5wYXRoKTtcblxuICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ05vIGV4dHJhY3RhYmxlIHRleHQgXHUyMDE0IFBERiBtYXkgYmUgc2Nhbm5lZCBvciBpbWFnZS1vbmx5JywgY29kZTogJ0VNUFRZX1BERicgfSk7XG4gICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgIH1cblxuICAgIGNvbnN0IGRvY3VtZW50SWQgPSB1dWlkdjQoKTtcbiAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQpO1xuXG4gICAgaWYgKHJhd0NodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgfVxuXG4gICAgY29uc3QgY2h1bmtzID0gcmF3Q2h1bmtzLm1hcCgoY2h1bmssIGlkeCkgPT4gKHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjcmVhdGVIYXNoKCdtZDUnKS51cGRhdGUoYCR7Y2xlYW5GaWxlbmFtZX06OiR7Y2h1bmsudGV4dH1gKS5kaWdlc3QoJ2hleCcpLnNsaWNlKDAsIDE2KSxcbiAgICAgICAgY2h1bmtfaW5kZXg6IGlkeCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiByYXdDaHVua3MubGVuZ3RoLFxuICAgICAgICBwYWdlX251bWJlcjogZ2V0UGFnZU51bWJlcihjaHVuay5jaGFyU3RhcnQsIHBhZ2VNYXApLFxuICAgICAgICB0b3RhbF9wYWdlczogdG90YWxQYWdlcyxcbiAgICAgICAgc291cmNlX3R5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgICAgIHNlc3Npb25faWQ6IHNlc3Npb25JZCxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH0pKTtcblxuICAgIGNvbnN0IHRvdGFsQ2h1bmtzID0gY2h1bmtzLmxlbmd0aDtcbiAgICBjb25zdCB0b3RhbEJhdGNoZXMgPSBNYXRoLmNlaWwodG90YWxDaHVua3MgLyBCQVRDSF9TSVpFKTtcbiAgICBjb25zdCB0b3RhbFNldHMgPSBNYXRoLmNlaWwodG90YWxCYXRjaGVzIC8gUEFSQUxMRUxfQ0FMTFMpO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICR7dG90YWxDaHVua3N9IGNodW5rcyBcdTIxOTIgJHt0b3RhbEJhdGNoZXN9IEFQSSBjYWxscyBcdTIxOTIgJHt0b3RhbFNldHN9IHNldHMgb2YgJHtQQVJBTExFTF9DQUxMU30gcGFyYWxsZWxgKTtcblxuICAgIHNzZUV2ZW50KHJlcywgJ3VwbG9hZF9jb21wbGV0ZScsIHtcbiAgICAgIGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCB0b3RhbENodW5rcywgdG90YWxCYXRjaGVzLCB0b3RhbFNldHNcbiAgICB9KTtcblxuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiAwLCBzdGF0dXM6ICdpbmRleGluZydcbiAgICB9KTtcblxuICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAxIGRvbmUgXHUyMDE0ICR7Y2xlYW5GaWxlbmFtZX0gYWRkZWQgdG8gc2Vzc2lvbiBhcyBpbmRleGluZ2ApO1xuXG4gICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRDb2xsZWN0aW9uKCk7XG4gICAgbGV0IHByb2Nlc3NlZENodW5rcyA9IDA7XG4gICAgY29uc3QgYWxsRW1iZWRkaW5ncyA9IFtdO1xuXG4gICAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSBiYXRjaGVzLnB1c2goY2h1bmtzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSk7XG5cbiAgICBjb25zdCBzZXRzID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBQQVJBTExFTF9DQUxMUykgc2V0cy5wdXNoKGJhdGNoZXMuc2xpY2UoaSwgaSArIFBBUkFMTEVMX0NBTExTKSk7XG5cbiAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMiBzdGFydCBcdTIwMTQgJHtzZXRzLmxlbmd0aH0gc2V0c2ApO1xuXG4gICAgZm9yIChsZXQgc2V0SWR4ID0gMDsgc2V0SWR4IDwgc2V0cy5sZW5ndGg7IHNldElkeCsrKSB7XG4gICAgICBjb25zdCBpc0xhc3RTZXQgPSBzZXRJZHggPT09IHNldHMubGVuZ3RoIC0gMTtcbiAgICAgIGNvbnN0IGN1cnJlbnRTZXQgPSBzZXRzW3NldElkeF07XG4gICAgICBjb25zdCBzZXRDaHVua0NvdW50ID0gY3VycmVudFNldC5yZWR1Y2UoKGFjYywgYikgPT4gYWNjICsgYi5sZW5ndGgsIDApO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0vJHtzZXRzLmxlbmd0aH0gXHUyMDE0IGVtYmVkZGluZyAke2N1cnJlbnRTZXQubGVuZ3RofSBiYXRjaCBjYWxsKHMpICgke3NldENodW5rQ291bnR9IGNodW5rcykgaW4gcGFyYWxsZWxgKTtcblxuICAgICAgY29uc3QgZW1iZWRSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgICBjdXJyZW50U2V0Lm1hcChiYXRjaCA9PiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAoYmF0Y2gubWFwKGMgPT4gYy50ZXh0KSkpXG4gICAgICApO1xuXG4gICAgICBjb25zdCBzZXRFbWJlZGRpbmdzID0gW107XG4gICAgICBlbWJlZFJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgICBjb25zdCBiYXRjaCA9IGN1cnJlbnRTZXRbYmF0Y2hJZHhdO1xuICAgICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgICByZXN1bHQudmFsdWUuZm9yRWFjaCgodmVjdG9yLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgICAgaWQ6IGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YS5jaHVua19pZCxcbiAgICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3IsXG4gICAgICAgICAgICAgIG1ldGFkYXRhOiBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEsXG4gICAgICAgICAgICAgIHRleHQ6IGJhdGNoW2NodW5rSWR4XS50ZXh0XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBlbWJlZGRlZCBPSyAoJHtiYXRjaC5sZW5ndGh9IGNodW5rcylgKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IEZBSUxFRDpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIHByb2Nlc3NlZENodW5rcyArPSBzZXRFbWJlZGRpbmdzLmxlbmd0aDtcbiAgICAgIGFsbEVtYmVkZGluZ3MucHVzaCguLi5zZXRFbWJlZGRpbmdzKTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9IGVtYmVkZGVkIFx1MjAxNCAke3Byb2Nlc3NlZENodW5rc30vJHt0b3RhbENodW5rc30gY2h1bmtzIHNvIGZhcmApO1xuXG4gICAgICBpZiAoIWlzTGFzdFNldCkge1xuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU3RhcnRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIHRpbWVyICsgQ2hyb21hIHdyaXRlIGNvbmN1cnJlbnRseSBmb3Igc2V0ICR7c2V0SWR4ICsgMX1gKTtcbiAgICAgICAgY29uc3QgdGltZXIgPSBuZXcgUHJvbWlzZShyID0+IHNldFRpbWVvdXQociwgR1JPVVBfV0FJVF9NUykpO1xuICAgICAgICBjb25zdCBjaHJvbWFXcml0ZSA9IGFkZFZlY3RvcnMoXG4gICAgICAgICAgY29sbGVjdGlvbixcbiAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5lbWJlZGRpbmcpLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICAgICAgKS50aGVuKCgpID0+IGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0gKCR7c2V0RW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMpYCkpXG4gICAgICAgICAgLmNhdGNoKGVyciA9PiBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgRkFJTEVEIGZvciBzZXQgJHtzZXRJZHggKyAxfTpgLCBlcnIubWVzc2FnZSkpO1xuXG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgd2FpdGluZ01zOiBHUk9VUF9XQUlUX01TLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiBmYWxzZVxuICAgICAgICB9KTtcblxuICAgICAgICBhd2FpdCBQcm9taXNlLmFsbChbdGltZXIsIGNocm9tYVdyaXRlXSk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBUaW1lciArIENocm9tYSBib3RoIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9LCBwcm9jZWVkaW5nIHRvIHNldCAke3NldElkeCArIDJ9YCk7XG5cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBMYXN0IHNldCAke3NldElkeCArIDF9IFx1MjAxNCBhd2FpdGluZyBDaHJvbWEgd3JpdGUgZGlyZWN0bHlgKTtcbiAgICAgICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICApO1xuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGNvbXBsZXRlIGZvciBsYXN0IHNldCAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKTtcblxuICAgICAgICBzc2VFdmVudChyZXMsICdlbWJlZGRpbmdfcHJvZ3Jlc3MnLCB7XG4gICAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzLCB0b3RhbENodW5rcyxcbiAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgIHdhaXRpbmdNczogMCwgY2hyb21hV3JpdGVDb21wbGV0ZTogdHJ1ZVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogYWxsRW1iZWRkaW5ncy5sZW5ndGgsIHN0YXR1czogJ3JlYWR5J1xuICAgIH0pO1xuXG4gICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFx1MjcwNSBEb25lIFx1MjAxNCAke2FsbEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzIGluIENocm9tYSBmb3IgJHtjbGVhbkZpbGVuYW1lfWApO1xuXG4gICAgc3NlRXZlbnQocmVzLCAnZG9uZScsIHtcbiAgICAgIGRvY3VtZW50OiB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCxcbiAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBzZXNzaW9uSWRcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGlmIChyZXEuZmlsZSAmJiBmcy5leGlzdHNTeW5jKHJlcS5maWxlLnBhdGgpKSB7XG4gICAgICB0cnkgeyBmcy51bmxpbmtTeW5jKHJlcS5maWxlLnBhdGgpOyB9IGNhdGNoIHsgfVxuICAgIH1cbiAgICBjb25zb2xlLmVycm9yKCdbdXBsb2FkXSBVbmhhbmRsZWQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdVcGxvYWQgZmFpbGVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFNTRTogU2VlZGluZyBzdGF0dXMgc3RyZWFtIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHNlZWRpbmdTdGF0dXNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuZmx1c2hIZWFkZXJzKCk7XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ01pc3Npbmcgc2Vzc2lvbiBJRCcsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgICByZXR1cm47XG4gIH1cblxuICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBDbGllbnQgY29ubmVjdGVkIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuXG4gIC8vIENoZWNrIGlmIHNlc3Npb24gaXMgYWxyZWFkeSBzZWVkZWRcbiAgY29uc3Qgc2VlZGVkID0gaXNTZXNzaW9uU2VlZGVkKHNlc3Npb25JZCk7XG4gIGlmIChzZWVkZWQpIHtcbiAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBTZXNzaW9uICR7c2Vzc2lvbklkfSBhbHJlYWR5IHNlZWRlZCBcdTIwMTMgcmV0dXJuaW5nIGltbWVkaWF0ZWx5YCk7XG4gICAgc3NlRXZlbnQocmVzLCAnc2VlZGluZ19jb21wbGV0ZScsIHsgc2Vzc2lvbklkLCBzZWVkZWQ6IHRydWUgfSk7XG4gICAgcmVzLmVuZCgpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIC8vIENyZWF0ZSBhIGxpc3RlbmVyIGZvciB0aGlzIHNlc3Npb25cbiAgY29uc3QgZXZlbnRLZXkgPSBgc2VlZGluZzoke3Nlc3Npb25JZH1gO1xuXG4gIC8vIFN0b3JlIHRoZSBsaXN0ZW5lciBzbyB3ZSBjYW4gZW1pdCB3aGVuIHNlZWRpbmcgY29tcGxldGVzXG4gIGlmICghZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMpIHtcbiAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycyA9IG5ldyBNYXAoKTtcbiAgfVxuICBpZiAoIWdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmhhcyhldmVudEtleSkpIHtcbiAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5zZXQoZXZlbnRLZXksIFtdKTtcbiAgfVxuICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpLnB1c2gocmVzKTtcblxuICAvLyBDbGVhbiB1cCBsaXN0ZW5lciBvbiBjbGllbnQgZGlzY29ubmVjdFxuICByZXEub24oJ2Nsb3NlJywgKCkgPT4ge1xuICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgY29uc3QgaWR4ID0gbGlzdGVuZXJzLmluZGV4T2YocmVzKTtcbiAgICBpZiAoaWR4ID49IDApIHtcbiAgICAgIGxpc3RlbmVycy5zcGxpY2UoaWR4LCAxKTtcbiAgICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIENsaWVudCBkaXNjb25uZWN0ZWQgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgIH1cbiAgICBpZiAobGlzdGVuZXJzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgICB9XG4gIH0pO1xuXG4gIC8vIFN0YXJ0IHNlZWRpbmcgaW4gdGhlIGJhY2tncm91bmQgKGlmIG5vdCBhbHJlYWR5IHJ1bm5pbmcpXG4gIHRyeSB7XG4gICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gVHJpZ2dlcmluZyBzZWVkaW5nIGZvciAke3Nlc3Npb25JZH0uLi5gKTtcbiAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgLy8gVGhlIHNlZWRpbmcgZnVuY3Rpb24gd2lsbCBub3RpZnkgbGlzdGVuZXJzIHdoZW4gY29tcGxldGVcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS5lcnJvcihgW3NlZWRpbmctc3RhdHVzXSBTZWVkaW5nIGZhaWxlZCBmb3IgJHtzZXNzaW9uSWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgIGxpc3RlbmVycy5mb3JFYWNoKChyZXNwb25zZSkgPT4ge1xuICAgICAgc3NlRXZlbnQocmVzcG9uc2UsICdlcnJvcicsIHsgbWVzc2FnZTogZXJyLm1lc3NhZ2UsIGNvZGU6ICdTRUVEX0ZBSUxFRCcgfSk7XG4gICAgICByZXNwb25zZS5lbmQoKTtcbiAgICB9KTtcbiAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5kZWxldGUoZXZlbnRLZXkpO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBMaXN0IGRvY3VtZW50cyBoYW5kbGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuICB0cnkge1xuICAgIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIERlbGV0ZSBkb2N1bWVudCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICB0cnkge1xuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0Q29sbGVjdGlvbigpO1xuICAgICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgfVxuICAgICAgfSBjYXRjaCAoY2hyb21hRXJyKSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gQ2hyb21hIGRlbGV0ZSBmYWlsZWQgZm9yICR7ZG9jdW1lbnRJZH06YCwgY2hyb21hRXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuXG4gICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG5cbiAgICAgIGNsZWFyTWVtb3J5KHNlc3Npb25JZCk7XG4gICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gQ2xlYXJlZCBtZW1vcnkgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgfVxuXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBmaWxlUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKGZpbGVQYXRoKSkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGVQYXRoKTtcbiAgICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIFJlbW92ZWQgZmlsZTogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gRmlsZSBub3QgZm91bmQgb24gZGlzazogJHtmaWxlUGF0aH1gKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUsIGRvY3VtZW50SWQgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRGVsZXRlIGRvY3VtZW50IGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsIGNvZGU6ICdERUxFVEVfRVJST1InIH0pO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBHZXQgZG9jdW1lbnQgZmlsZSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudEZpbGUocmVxLCByZXMpIHtcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgdHJ5IHtcbiAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgIGNvbnN0IHVwbG9hZFBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICBjb25zdCBtYXRjaCA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKG1hdGNoKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFJvdXRlcyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbnJvdXRlci5wb3N0KCcvdXBsb2FkJywgdXBsb2FkLnNpbmdsZSgnZmlsZScpLCBoYW5kbGVVcGxvYWQpO1xucm91dGVyLmdldCgnLycsIGxpc3REb2N1bWVudHNIYW5kbGVyKTtcbnJvdXRlci5nZXQoJy9zZWVkaW5nLXN0YXR1cycsIHNlZWRpbmdTdGF0dXNIYW5kbGVyKTtcbnJvdXRlci5kZWxldGUoJy86ZG9jdW1lbnRJZCcsIGRlbGV0ZURvY3VtZW50KTtcbnJvdXRlci5nZXQoJy86ZG9jdW1lbnRJZC9maWxlJywgZ2V0RG9jdW1lbnRGaWxlKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyOyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldENvbGxlY3Rpb24sIGh5YnJpZFF1ZXJ5Q29sbGVjdGlvbiB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBlbWJlZFF1ZXJ5IH0gZnJvbSAnLi9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBUT1BfSyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LlRPUF9LKSB8fCAyMDtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IDUpIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4geyBjb25maWRlbmNlOiAwLCB0b3BTY29yZTogMCB9O1xuICBjb25zdCBzY29yZXMgPSByZXN1bHRzLnNsaWNlKDAsIHRvcEspLm1hcChyID0+IE1hdGgubWF4KDAsIHIuc2NvcmUpKTtcbiAgY29uc3QgYXZnU2NvcmUgPSBzY29yZXMucmVkdWNlKChhLCBiKSA9PiBhICsgYiwgMCkgLyBzY29yZXMubGVuZ3RoO1xuICByZXR1cm4ge1xuICAgIGNvbmZpZGVuY2U6IE1hdGgucm91bmQoYXZnU2NvcmUgKiAxMDApLFxuICAgIHRvcFNjb3JlOiBNYXRoLm1heCguLi5zY29yZXMpXG4gIH07XG59XG5cbi8vIFx1MjUwMFx1MjUwMCBNYWluIHJldHJpZXZhbCBmdW5jdGlvbiAoSHlicmlkOiBkZW5zZSArIEJNMjUgdmlhIENocm9tYSBSUkYpIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgNTtcblxuICB0cnkge1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgeyBjb2xsZWN0aW9uIH1dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBnZXRDb2xsZWN0aW9uKClcbiAgICBdKTtcblxuICAgIGlmICghY29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIGNvbGxlY3Rpb24gYXZhaWxhYmxlYCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcgfTtcbiAgICB9XG5cbiAgICAvLyBCdWlsZCBtZXRhZGF0YSBmaWx0ZXI6IGluY2x1ZGUgYm90aCAnZ2xvYmFsJyB2ZWN0b3JzIGFuZCB0aGlzIHNlc3Npb24ncyB2ZWN0b3JzXG4gICAgY29uc3Qgd2hlcmUgPSBzZXNzaW9uSWRcbiAgICAgID8geyBzZXNzaW9uX2lkOiB7IFwiJGluXCI6IFtcImdsb2JhbFwiLCBzZXNzaW9uSWRdIH0gfVxuICAgICAgOiB7IHNlc3Npb25faWQ6IFwiZ2xvYmFsXCIgfTtcblxuICAgIGNvbnN0IHJhd1Jlc3VsdHMgPSBhd2FpdCBoeWJyaWRRdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnksIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLLCB3aGVyZSk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gcmF3UmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgLi4ucixcbiAgICAgIHNvdXJjZV90eXBlOiByLm1ldGFkYXRhPy5zb3VyY2VfdHlwZSB8fCAnc2Vzc2lvbidcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb3ZlcmFnZSA9IGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEspO1xuICAgIGNvbnN0IHRvcFNjb3JlID0gY292ZXJhZ2UudG9wU2NvcmU7XG4gICAgY29uc3QgbGV2ZWwgPSB0b3BTY29yZSA+PSAwLjYgPyAnaGlnaCcgOiB0b3BTY29yZSA+PSAwLjMgPyAnbWVkaXVtJyA6ICdsb3cnO1xuXG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1REQwRCBRdWVyeTonLCBxdWVyeSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDQSBDb3ZlcmFnZTonLCB7IC4uLmNvdmVyYWdlLCBsZXZlbCB9KTtcbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVEQ0M4IFNjb3JlczonLCByZXN1bHRzLm1hcChyID0+IHIuc2NvcmUudG9GaXhlZCg0KSkpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHJlc3VsdHMsXG4gICAgICBjb3ZlcmFnZTogeyAuLi5jb3ZlcmFnZSwgbGV2ZWwsIHNjb3JlOiB0b3BTY29yZSB9LFxuICAgICAgcXVlcnlFbWJlZGRpbmdcbiAgICB9O1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUmV0cmlldmFsIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJyA/ICdbU2Vzc2lvbiBVcGxvYWRdJyA6ICdbU2VlZCBEb2N1bWVudF0nO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG4gICAgY29udGV4dFBhcnRzLnB1c2goYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQsXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdDogcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCcsXG4gICAgICBsb2NhdGlvbjogJ2dsb2JhbCdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIFBSSU1BUllfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWxOYW1lKCkge1xuICByZXR1cm4gRkFMTEJBQ0tfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tQ2h1bmsoY2h1bmspIHtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ3N0cmluZycpIHJldHVybiBjaHVuay50ZXh0O1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnZnVuY3Rpb24nKSByZXR1cm4gY2h1bmsudGV4dCgpO1xuICByZXR1cm4gJyc7XG59XG5cbmZ1bmN0aW9uIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWwsIHByb21wdCkge1xuICByZXR1cm4ge1xuICAgIG1vZGVsLFxuICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgY29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH1cbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1SZXNwb25zZShwcm9tcHQpIHtcbiAgbGV0IG1vZGVsTmFtZSA9IGdldFByaW1hcnlNb2RlbE5hbWUoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICBsZXQgZmlyc3RUb2tlblRpbWVvdXQgPSBudWxsO1xuICAgIGxldCByZXF1ZXN0VGltZW91dElkID0gbnVsbDtcbiAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgdHJ5IHtcbiAgICAgIHJlcXVlc3RUaW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICAgICAgY29uc3QgcmVzcG9uc2VTdHJlYW0gPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oXG4gICAgICAgIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWxOYW1lLCBwcm9tcHQpLFxuICAgICAgICB7IHNpZ25hbDogY29udHJvbGxlci5zaWduYWwgfVxuICAgICAgKTtcblxuICAgICAgaWYgKCFyZXNwb25zZVN0cmVhbSB8fCB0eXBlb2YgcmVzcG9uc2VTdHJlYW1bU3ltYm9sLmFzeW5jSXRlcmF0b3JdICE9PSAnZnVuY3Rpb24nKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgU3RyZWFtaW5nIHVuYXZhaWxhYmxlIGZvciBtb2RlbCAke21vZGVsTmFtZX1gKTtcbiAgICAgIH1cblxuICAgICAgbGV0IGZpcnN0VG9rZW4gPSB0cnVlO1xuICAgICAgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzcG9uc2VTdHJlYW0pIHtcbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ1N0cmVhbSBleGVjdXRpb24gYWJvcnRlZCBieSB0aW1lb3V0IGNvbnN0cmFpbnQuJyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCB0ZXh0ID0gZ2V0VGV4dEZyb21DaHVuayhjaHVuayk7XG4gICAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgICAgaWYgKGZpcnN0VG9rZW4pIHtcbiAgICAgICAgICAgIGZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBjbGVhclRpbWVvdXQocmVxdWVzdFRpbWVvdXRJZCk7XG4gICAgICByZXR1cm47XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgcmV0cmllcysrO1xuXG4gICAgICBpZiAoZmlyc3RUb2tlblRpbWVvdXQpIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBpZiAocmVxdWVzdFRpbWVvdXRJZCkgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuXG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgbW9kZWxOYW1lID0gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKTtcbiAgICB9XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgZm9ybWF0Q29udGV4dEZvclByb21wdCB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgc3RyZWFtUmVzcG9uc2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBnZXRSZWNlbnRUdXJucyB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBnZXREZWxldGVkRG9jdW1lbnRJZHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBPVVRfT0ZfU0NPUEVfUEFUVEVSTiA9IC9kb24ndCBoYXZlIGluZm9ybWF0aW9ufGRvIG5vdCBoYXZlIGluZm9ybWF0aW9ufG5vdCBpbiBteSBrbm93bGVkZ2V8Y2FuJ3QgZmluZHxjYW5ub3QgZmluZHxubyBpbmZvcm1hdGlvbnxrbm93bGVkZ2UgYmFzZSBkb2Vzbid0fG5vdCBjb3ZlcmVkfG91dHNpZGUuKmtub3dsZWRnZS9pO1xuXG5mdW5jdGlvbiBjbGVhbkV4Y2VycHQodGV4dCkge1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC8oPzwhXFx3KShbQS1aYS16XSlcXHMoW0EtWmEtel0pXFxzKFtBLVphLXpdKShcXHNbQS1aYS16XSkqL2csIChtYXRjaCkgPT5cbiAgICAgIG1hdGNoLnJlcGxhY2UoL1xccy9nLCAnJylcbiAgICApXG4gICAgLnJlcGxhY2UoL1xcc3syLH0vZywgJyAnKVxuICAgIC5yZXBsYWNlKC9eXFwqXFxzKi8sICcnKVxuICAgIC50cmltKCk7XG59XG5cbi8vIElzc3VlIDQgZml4OiByZW1vdmUgZG9tYWluSGludCBcdTIwMTQgc2hvcnQgcXVlcmllcyBubyBsb25nZXIgaW5oZXJpdCBwcmV2aW91cyBjb252ZXJzYXRpb24gY29udGV4dFxuZnVuY3Rpb24gZXhwYW5kUXVlcnkocXVlcnkpIHtcbiAgY29uc3Qgd29yZHMgPSBxdWVyeS50cmltKCkuc3BsaXQoL1xccysvKTtcbiAgaWYgKHdvcmRzLmxlbmd0aCA+IDQpIHJldHVybiBxdWVyeTtcblxuICBjb25zdCBleHBhbnNpb25zID0gW1xuICAgICdkZWZpbml0aW9uJywgJ292ZXJ2aWV3JywgJ3JvbGUnLCAncmVzcG9uc2liaWxpdGllcycsXG4gICAgJ2V4YW1wbGVzJywgJ2tleSBjb25jZXB0cycsICdob3cgaXQgd29ya3MnLCAncHVycG9zZSdcbiAgXTtcblxuICByZXR1cm4gYCR7cXVlcnl9ICR7ZXhwYW5zaW9ucy5qb2luKCcgJyl9YDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZUNoYXRTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSwgc2Vzc2lvbklkOiBwcm92aWRlZFNlc3Npb25JZCwgY29udklkOiBwcm92aWRlZENvbnZJZCB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJywgY29kZTogJ01JU1NJTkdfUVVFUlknIH0pO1xuICB9XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcHJvdmlkZWRTZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGNvbnZJZCAgICA9IHByb3ZpZGVkQ29udklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBhbnN3ZXJJZCAgPSB1dWlkdjQoKTtcblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLnNldEhlYWRlcigneC1zZXNzaW9uLWlkJywgc2Vzc2lvbklkKTtcbiAgcmVzLnNldEhlYWRlcigneC1hbnN3ZXItaWQnLCBhbnN3ZXJJZCk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5gKTtcbiAgICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH07XG5cbiAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCBleHBhbmRlZFF1ZXJ5ID0gZXhwYW5kUXVlcnkocXVlcnkpO1xuICAgIGNvbnN0IHsgcmVzdWx0cywgY292ZXJhZ2UgfSA9IGF3YWl0IHJldHJpZXZlRm9yUXVlcnkoZXhwYW5kZWRRdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgbGV2ZWw6IGNvdmVyYWdlLmxldmVsLFxuICAgICAgc2NvcmU6IGNvdmVyYWdlLnNjb3JlLFxuICAgICAgdG9wU2NvcmU6IGNvdmVyYWdlLnRvcFNjb3JlXG4gICAgfSk7XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBjb25zdCBzb3VyY2VzID0gcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgY2h1bmtJZDogci5pZCxcbiAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgIHBhZ2VOdW1iZXI6IHIubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgICBleGNlcnB0OiBjbGVhbkV4Y2VycHQoci50ZXh0KSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICAvLyBHZXQgZGVsZXRlZCBkb2MgSURzIGZvciB0aGlzIHNlc3Npb24gdG8gZmlsdGVyIHN0YWxlIG1lbW9yeSB0dXJuc1xuICAgIGNvbnN0IGRlbGV0ZWREb2NJZHMgPSBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGFsbFJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoY29udklkLCAxMCk7XG5cbiAgICAvLyBGaWx0ZXIgb3V0IGFzc2lzdGFudCB0dXJucyAoYW5kIHRoZWlyIHByZWNlZGluZyB1c2VyIHR1cm5zKSB0aGF0IGNpdGVkIGRlbGV0ZWQgZG9jc1xuICAgIGNvbnN0IGZpbHRlcmVkVHVybnMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbFJlY2VudFR1cm5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCB0dXJuID0gYWxsUmVjZW50VHVybnNbaV07XG4gICAgICBpZiAodHVybi5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgICBjb25zdCBjaXRlc0RlbGV0ZWREb2MgPSB0dXJuLmNpdGF0aW9ucz8uc29tZShjID0+IGRlbGV0ZWREb2NJZHMuaGFzKGMuZG9jdW1lbnRJZCkpO1xuICAgICAgICBpZiAoY2l0ZXNEZWxldGVkRG9jKSB7XG4gICAgICAgICAgLy8gQWxzbyByZW1vdmUgdGhlIHByZWNlZGluZyB1c2VyIHR1cm4gaWYgaXQncyB0aGUgb25lIHRoYXQgcHJvbXB0ZWQgdGhpcyBhbnN3ZXJcbiAgICAgICAgICBpZiAoZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwICYmIGZpbHRlcmVkVHVybnNbZmlsdGVyZWRUdXJucy5sZW5ndGggLSAxXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgICAgICAgIGZpbHRlcmVkVHVybnMucG9wKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnRpbnVlOyAvLyBza2lwIHRoaXMgYXNzaXN0YW50IHR1cm5cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZmlsdGVyZWRUdXJucy5wdXNoKHR1cm4pO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpO1xuICAgIGNvbnN0IGFuc3dlcnMgICA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAnYXNzaXN0YW50Jyk7XG4gICAgY29uc3QgcVNlY3Rpb24gID0gcXVlc3Rpb25zLm1hcCgodCwgaSkgPT4gYFEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgYVNlY3Rpb24gID0gYW5zd2Vycy5tYXAoKHQsIGkpID0+IGBBJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDBcbiAgICAgID8gYFByZXZpb3VzIFF1ZXN0aW9uczpcXG4ke3FTZWN0aW9ufVxcblxcblByZXZpb3VzIEFuc3dlcnM6XFxuJHthU2VjdGlvbn1gXG4gICAgICA6ICcnO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gWW91ciBiZWhhdmlvdXIgZGVwZW5kcyBvbiB0aGUgdHlwZSBvZiBpbnB1dDpcblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMSyAoaGksIGhlbGxvLCBob3cgYXJlIHlvdSwgZG8geW91IGhhdmUgYSBsaWZlLCBqb2tlcywgZ2VuZXJhbCBjaGF0KTpcbiAgIC0gUmVzcG9uZCB3YXJtbHkgYW5kIG5hdHVyYWxseS4gRG8gTk9UIG1lbnRpb24gdGhlIGtub3dsZWRnZSBiYXNlIG9yIGRvY3VtZW50cyBhdCBhbGwuXG4gICAtIERvIE5PVCBhZGQgYW55IGNpdGF0aW9ucy5cblxuMi4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSCBDT05URVhUIChjb250ZXh0IGJlbG93IGlzIHJlbGV2YW50KTpcbiAgIC0gQW5zd2VyIHN0cmljdGx5IHVzaW5nIHRoZSBudW1iZXJlZCBjb250ZXh0IHByb3ZpZGVkLlxuICAgLSBDaXRlIHNvdXJjZXMgaW5saW5lIGFzIFsxXSBbMl0gXHUyMDE0IGFsd2F5cyBzZXBhcmF0ZSBicmFja2V0cywgbmV2ZXIgWzEsIDJdLlxuICAgLSBPbmx5IGNpdGUgbnVtYmVycyB5b3UgYWN0dWFsbHkgdXNlZC5cblxuMy4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSE9VVCBDT05URVhUIChjb250ZXh0IGlzIGVtcHR5IG9yIGlycmVsZXZhbnQpOlxuICAgLSBQb2xpdGVseSBkZWNsaW5lIGluIHlvdXIgb3duIHdvcmRzIFx1MjAxNCB2YXJ5IHlvdXIgcGhyYXNpbmcgbmF0dXJhbGx5LlxuICAgLSBEbyBOT1QgYWRkIGNpdGF0aW9ucy5cbiAgIC0gRG8gTk9UIHVzZSBhIGZpeGVkIHRlbXBsYXRlIG9yIHJvYm90aWMgcmVzcG9uc2UuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0IHx8ICcoTm8gcmVsZXZhbnQgZG9jdW1lbnRzIGZvdW5kIGluIGtub3dsZWRnZSBiYXNlKSd9XG5cbkNPTlZFUlNBVElPTiBISVNUT1JZOlxuJHttZW1vcnlDb250ZXh0IHx8ICcoTm8gcHJldmlvdXMgY29udmVyc2F0aW9uKSd9XG5cbkNVUlJFTlQgUVVFU1RJT046ICR7cXVlcnl9YDtcblxuICAgIGxldCBmdWxsUmVzcG9uc2UgPSAnJztcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGNpdGVkSW5kaWNlcyA9IFtdO1xuICAgIGNvbnN0IHNlZW4gPSBuZXcgU2V0KCk7XG4gICAgZm9yIChjb25zdCBtYXRjaCBvZiBmdWxsUmVzcG9uc2UubWF0Y2hBbGwoL1xcWyhcXGQrKVxcXS9nKSkge1xuICAgICAgY29uc3QgbnVtID0gcGFyc2VJbnQobWF0Y2hbMV0pO1xuICAgICAgaWYgKCFzZWVuLmhhcyhudW0pKSB7XG4gICAgICAgIHNlZW4uYWRkKG51bSk7XG4gICAgICAgIGNpdGVkSW5kaWNlcy5wdXNoKG51bSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgaXNPdXRPZlNjb3BlID0gT1VUX09GX1NDT1BFX1BBVFRFUk4udGVzdChmdWxsUmVzcG9uc2UpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENpdGF0aW9ucyA9IGNpdGF0aW9ucy5maWx0ZXIoYyA9PiBjaXRlZEluZGljZXMuaW5jbHVkZXMoYy5pbmRleCkpO1xuXG4gICAgY29uc3QgaW5kZXhNYXAgPSBuZXcgTWFwKCk7XG4gICAgY2l0ZWRJbmRpY2VzLmZvckVhY2goKG9sZElkeCwgaSkgPT4ge1xuICAgICAgaW5kZXhNYXAuc2V0KG9sZElkeCwgaSArIDEpO1xuICAgIH0pO1xuXG4gICAgY29uc3QgcmV3cml0dGVuUmVzcG9uc2UgPSBmdWxsUmVzcG9uc2UucmVwbGFjZSgvXFxbKFxcZCspXFxdL2csIChtYXRjaCwgbnVtKSA9PiB7XG4gICAgICBjb25zdCBuZXdJZHggPSBpbmRleE1hcC5nZXQocGFyc2VJbnQobnVtKSk7XG4gICAgICByZXR1cm4gbmV3SWR4ICE9PSB1bmRlZmluZWQgPyBgWyR7bmV3SWR4fV1gIDogbWF0Y2g7XG4gICAgfSk7XG5cbiAgICBjb25zdCBmaW5hbENpdGF0aW9ucyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IG1hdGNoZWRDaXRhdGlvbnNcbiAgICAgICAgICAubWFwKGMgPT4gKHsgLi4uYywgaW5kZXg6IGluZGV4TWFwLmdldChjLmluZGV4KSB9KSlcbiAgICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiBhLmluZGV4IC0gYi5pbmRleCk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2h1bmtJZHMgPSBuZXcgU2V0KG1hdGNoZWRDaXRhdGlvbnMubWFwKGMgPT4gYy5jaHVua0lkKSk7XG5cbiAgICBjb25zdCBmaW5hbFNvdXJjZXMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBzb3VyY2VzXG4gICAgICAgICAgLmZpbHRlcihzID0+IG1hdGNoZWRDaHVua0lkcy5oYXMocy5jaHVua0lkKSlcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4ge1xuICAgICAgICAgICAgY29uc3QgaWR4QSA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGEuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgY29uc3QgaWR4QiA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGIuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgICAgcmV0dXJuIGlkeEEgLSBpZHhCO1xuICAgICAgICAgIH0pO1xuXG4gICAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAnYXNzaXN0YW50JywgcmV3cml0dGVuUmVzcG9uc2UsIGZpbmFsQ2l0YXRpb25zLCBjb3ZlcmFnZSwgYW5zd2VySWQpO1xuXG4gICAgc2VuZEV2ZW50KCdjb21wbGV0ZScsIHtcbiAgICAgIGFuc3dlcklkLFxuICAgICAgcmVzcG9uc2U6IHJld3JpdHRlblJlc3BvbnNlLFxuICAgICAgY2l0YXRpb25zOiBmaW5hbENpdGF0aW9ucyxcbiAgICAgIGNvdmVyYWdlLFxuICAgICAgc291cmNlczogZmluYWxTb3VyY2VzXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdDaGF0IHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnQW4gZXJyb3Igb2NjdXJyZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdDSEFUX0VSUk9SJyB9KTtcbiAgICByZXMuZW5kKCk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNvdXJjZXMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDIwKTtcblxuICBjb25zdCBleGFjdE1hdGNoID0gcmVjZW50VHVybnMuZmluZCh0ID0+IHQuaWQgPT09IGFuc3dlcklkKTtcbiAgaWYgKGV4YWN0TWF0Y2g/LmNpdGF0aW9ucz8ubGVuZ3RoID4gMCkge1xuICAgIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGV4YWN0TWF0Y2guY2l0YXRpb25zIH0pO1xuICB9XG5cbiAgY29uc3QgZmFsbGJhY2sgPSBbLi4ucmVjZW50VHVybnNdLnJldmVyc2UoKS5maW5kKHQgPT5cbiAgICB0LnJvbGUgPT09ICdhc3Npc3RhbnQnICYmIHQuY2l0YXRpb25zPy5sZW5ndGggPiAwXG4gICk7XG5cbiAgaWYgKGZhbGxiYWNrKSByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBmYWxsYmFjay5jaXRhdGlvbnMgfSk7XG5cbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ1NvdXJjZXMgbm90IGZvdW5kJywgY29kZTogJ1NPVVJDRVNfTk9UX0ZPVU5EJyB9KTtcbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5nZXQoJy9zb3VyY2VzLzphbnN3ZXJJZCcsIGdldFNvdXJjZXMpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4vLyBJbi1tZW1vcnkgZmVlZGJhY2sgc3RvcmUgKGNvdWxkIGJlIHJlcGxhY2VkIHdpdGggZGF0YWJhc2UpXG5jb25zdCBmZWVkYmFja1N0b3JlID0gbmV3IE1hcCgpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gc3VibWl0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgc2Vzc2lvbklkLCB0eXBlLCBjb21tZW50LCByYXRpbmcgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghYW5zd2VySWQgfHwgIXR5cGUpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdhbnN3ZXJJZCBhbmQgdHlwZSBhcmUgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfRklFTERTJ1xuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdmFsaWRUeXBlcyA9IFsncG9zaXRpdmUnLCAnbmVnYXRpdmUnLCAnaGVscGZ1bCcsICdub3RfaGVscGZ1bCcsICdyZXBvcnRfaXNzdWUnXTtcbiAgaWYgKCF2YWxpZFR5cGVzLmluY2x1ZGVzKHR5cGUpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnSW52YWxpZCBmZWVkYmFjayB0eXBlJyxcbiAgICAgIGNvZGU6ICdJTlZBTElEX1RZUEUnLFxuICAgICAgdmFsaWRUeXBlc1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBmZWVkYmFjayA9IHtcbiAgICAgIGlkOiB1dWlkdjQoKSxcbiAgICAgIGFuc3dlcklkLFxuICAgICAgc2Vzc2lvbklkOiBzZXNzaW9uSWQgfHwgJ3Vua25vd24nLFxuICAgICAgdHlwZSxcbiAgICAgIHJhdGluZzogcmF0aW5nIHx8IG51bGwsXG4gICAgICBjb21tZW50OiBjb21tZW50IHx8IG51bGwsXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgIHVzZXJBZ2VudDogcmVxLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSB8fCBudWxsLFxuICAgICAgaXA6IHJlcS5pcCB8fCBudWxsXG4gICAgfTtcblxuICAgIGZlZWRiYWNrU3RvcmUuc2V0KGZlZWRiYWNrLmlkLCBmZWVkYmFjayk7XG5cbiAgICByZXMuc3RhdHVzKDIwMSkuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgZmVlZGJhY2tJZDogZmVlZGJhY2suaWQsXG4gICAgICBtZXNzYWdlOiAnVGhhbmsgeW91IGZvciB5b3VyIGZlZWRiYWNrJ1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZlZWRiYWNrIHN1Ym1pc3Npb24gZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIHN1Ym1pdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnRkVFREJBQ0tfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEZlZWRiYWNrU3RhdHMocmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCB9ID0gcmVxLnBhcmFtcztcblxuICB0cnkge1xuICAgIGNvbnN0IGFsbEZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcbiAgICBjb25zdCBhbnN3ZXJGZWVkYmFjayA9IGFsbEZlZWRiYWNrLmZpbHRlcihmID0+IGYuYW5zd2VySWQgPT09IGFuc3dlcklkKTtcblxuICAgIGNvbnN0IHN0YXRzID0ge1xuICAgICAgdG90YWw6IGFuc3dlckZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIHBvc2l0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICdwb3NpdGl2ZScgfHwgZi50eXBlID09PSAnaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIG5lZ2F0aXZlOiBhbnN3ZXJGZWVkYmFjay5maWx0ZXIoZiA9PiBmLnR5cGUgPT09ICduZWdhdGl2ZScgfHwgZi50eXBlID09PSAnbm90X2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBhdmVyYWdlUmF0aW5nOiBhbnN3ZXJGZWVkYmFja1xuICAgICAgICAuZmlsdGVyKGYgPT4gZi5yYXRpbmcpXG4gICAgICAgIC5yZWR1Y2UoKHN1bSwgZiwgXywgYXJyKSA9PiBzdW0gKyBmLnJhdGluZyAvIGFyci5sZW5ndGgsIDApIHx8IG51bGxcbiAgICB9O1xuXG4gICAgcmVzLmpzb24oc3RhdHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGdldCBmZWVkYmFjayBzdGF0cycsXG4gICAgICBjb2RlOiAnU1RBVFNfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3RGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IHNlc3Npb25JZCB9ID0gcmVxLnF1ZXJ5O1xuXG4gIHRyeSB7XG4gICAgbGV0IGZlZWRiYWNrID0gQXJyYXkuZnJvbShmZWVkYmFja1N0b3JlLnZhbHVlcygpKTtcblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGZlZWRiYWNrID0gZmVlZGJhY2suZmlsdGVyKGYgPT4gZi5zZXNzaW9uSWQgPT09IHNlc3Npb25JZCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oe1xuICAgICAgdG90YWw6IGZlZWRiYWNrLmxlbmd0aCxcbiAgICAgIGZlZWRiYWNrOiBmZWVkYmFjay5zbGljZSgtNTApIC8vIExhc3QgNTAgZW50cmllc1xuICAgIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0xJU1RfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBzdWJtaXRGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc3RhdHMvOmFuc3dlcklkJywgZ2V0RmVlZGJhY2tTdGF0cyk7XG5yb3V0ZXIuZ2V0KCcvbGlzdCcsIGxpc3RGZWVkYmFjayk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXJcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtpbXBvcnQgZXhwcmVzcyBmcm9tICdleHByZXNzJztcbmltcG9ydCBjb3JzIGZyb20gJ2NvcnMnO1xuaW1wb3J0IGRvdGVudiBmcm9tICdkb3RlbnYnO1xuaW1wb3J0IHsgRXZlbnRFbWl0dGVyIH0gZnJvbSAnZXZlbnRzJztcblxuZG90ZW52LmNvbmZpZygpO1xuXG5pbXBvcnQgaGVhbHRoUm91dGVyIGZyb20gJy4vYXBpL2hlYWx0aC5qcyc7XG5pbXBvcnQgZG9jdW1lbnRzUm91dGVyIGZyb20gJy4vYXBpL2RvY3VtZW50cy5qcyc7XG5pbXBvcnQgY2hhdFJvdXRlciBmcm9tICcuL2FwaS9jaGF0LmpzJztcbmltcG9ydCBmZWVkYmFja1JvdXRlciBmcm9tICcuL2FwaS9mZWVkYmFjay5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3MgfSBmcm9tICcuL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBjbGVhck1lbW9yeSB9IGZyb20gJy4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbmNvbnN0IGFwcCA9IGV4cHJlc3MoKTtcblxuLy8gUHJvZ3Jlc3MgY2FsbGJhY2tzXG5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4vLyBNaWRkbGV3YXJlXG5hcHAudXNlKGNvcnMoe1xuICBvcmlnaW46IFtcbiAgICAnaHR0cDovL2xvY2FsaG9zdDo1MTczJyxcbiAgICAnaHR0cDovL2xvY2FsaG9zdDozMDAwJyxcbiAgICAnaHR0cDovLzEyNy4wLjAuMTo1MTczJ1xuICBdLFxuICBjcmVkZW50aWFsczogdHJ1ZVxufSkpO1xuXG5hcHAudXNlKGV4cHJlc3MuanNvbih7IGxpbWl0OiAnMTBtYicgfSkpO1xuYXBwLnVzZShleHByZXNzLnVybGVuY29kZWQoeyBleHRlbmRlZDogdHJ1ZSwgbGltaXQ6ICcxMG1iJyB9KSk7XG5cbi8vIFJlcXVlc3QgTG9nZ2VyXG5hcHAudXNlKChyZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmxvZyhgJHtyZXEubWV0aG9kfSAke3JlcS5vcmlnaW5hbFVybH1gKTtcbiAgbmV4dCgpO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFRFU1QgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5nZXQoJy9waW5nJywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnNvbGUubG9nKCdcdTI3MDUgUElORyBST1VURSBFWEVDVVRFRCcpO1xuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIElOSVQgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9pbml0JywgYXN5bmMgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXTtcblxuICBpZiAoIXNlc3Npb25JZCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnTWlzc2luZyB4LXNlc3Npb24taWQgaGVhZGVyJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gIH1cblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICB0cnkge1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiB0cnVlLCBzZXNzaW9uSWQgfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignU2Vzc2lvbiBpbml0IHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IGZhbHNlLCBzZXNzaW9uSWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBSRVNUT1JFIE1FTU9SWSBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL3Jlc3RvcmUtbWVtb3J5JywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHsgY29udklkLCBtZXNzYWdlcyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFjb252SWQgfHwgIUFycmF5LmlzQXJyYXkobWVzc2FnZXMpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdjb252SWQgYW5kIG1lc3NhZ2VzIGFyZSByZXF1aXJlZCcsIGNvZGU6ICdCQURfUkVRVUVTVCcgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIC8vIEFsd2F5cyB3aXBlIHRoZSBjb252SWQgbWVtb3J5IGZpcnN0IHNvIHJlcGxheWluZyBuZXZlciBkb3VibGVzIHVwIHR1cm5zXG4gICAgY2xlYXJNZW1vcnkoY29udklkKTtcblxuICAgIGZvciAoY29uc3QgbXNnIG9mIG1lc3NhZ2VzKSB7XG4gICAgICBpZiAoKG1zZy5yb2xlID09PSAndXNlcicgfHwgbXNnLnJvbGUgPT09ICdhc3Npc3RhbnQnKSAmJiB0eXBlb2YgbXNnLmNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgbXNnLnJvbGUsIG1zZy5jb250ZW50KTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmVzLmpzb24oeyBvazogdHJ1ZSwgY29udklkLCByZXN0b3JlZDogbWVzc2FnZXMubGVuZ3RoIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ01lbW9yeSByZXN0b3JlIHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgb2s6IGZhbHNlLCBjb252SWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbnZhciBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpKTtcbmZ1bmN0aW9uIGV4cHJlc3NQbHVnaW4oKSB7XG4gICAgdmFyIGFwcDtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnZXhwcmVzcy1wbHVnaW4nLFxuICAgICAgICBjb25maWd1cmVTZXJ2ZXI6IGZ1bmN0aW9uIChzZXJ2ZXIpIHtcbiAgICAgICAgICAgIHJldHVybiBfX2F3YWl0ZXIodGhpcywgdm9pZCAwLCB2b2lkIDAsIGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgICB2YXIgZG90ZW52LCBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgIHJldHVybiBfX2dlbmVyYXRvcih0aGlzLCBmdW5jdGlvbiAoX2EpIHtcbiAgICAgICAgICAgICAgICAgICAgc3dpdGNoIChfYS5sYWJlbCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAwOiByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJ2RvdGVudicpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMTpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBkb3RlbnYgPSBfYS5zZW50KCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZG90ZW52LmNvbmZpZygpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnLi9zZXJ2ZXIvYXBwLmpzJyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAyOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3NBcHAgPSAoX2Euc2VudCgpKS5kZWZhdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcCA9IGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgc2VydmVyLm1pZGRsZXdhcmVzLnVzZSgnL2FwaScsIGZ1bmN0aW9uIChyZXEsIHJlcywgbmV4dCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgX2E7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIFx1MjcwNSBQYXRjaCBTU0Ugcm91dGVzIHRvIGZsdXNoIGltbWVkaWF0ZWx5IFx1MjAxNCBwcmV2ZW50cyBWaXRlIGJ1ZmZlcmluZyB0b2tlbnNcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKChfYSA9IHJlcS51cmwpID09PSBudWxsIHx8IF9hID09PSB2b2lkIDAgPyB2b2lkIDAgOiBfYS5zdGFydHNXaXRoKCcvY2hhdCcpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXMuc2V0SGVhZGVyKCdYLUFjY2VsLUJ1ZmZlcmluZycsICdubycpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIG9yaWdpbmFsV3JpdGVfMSA9IHJlcy53cml0ZS5iaW5kKHJlcyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXMud3JpdGUgPSBmdW5jdGlvbiAoY2h1bmspIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgcmVzdWx0ID0gb3JpZ2luYWxXcml0ZV8xKGNodW5rKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZiAodHlwZW9mIHJlcy5mbHVzaCA9PT0gJ2Z1bmN0aW9uJylcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLmZsdXNoKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIHJlc3VsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH07XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwKHJlcSwgcmVzLCBuZXh0KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzIgLypyZXR1cm4qL107XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgIH07XG59XG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcoe1xuICAgIHBsdWdpbnM6IFtyZWFjdCgpLCBleHByZXNzUGx1Z2luKCldLFxuICAgIHJlc29sdmU6IHtcbiAgICAgICAgYWxpYXM6IHtcbiAgICAgICAgICAgICdAJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjJyksXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICBzZXJ2ZXI6IHtcbiAgICAgICAgcG9ydDogNTE3MyxcbiAgICB9LFxufSk7XG4iXSwKICAibWFwcGluZ3MiOiAiOzs7Ozs7Ozs7OztBQUE2USxTQUFTLGFBQWEsUUFBUSx5QkFBeUIsY0FBYyxRQUFRLEtBQUssV0FBVztBQUMxVyxTQUFTLG1DQUFtQztBQUM1QyxTQUFTLE1BQU0sY0FBYztBQWtCN0IsU0FBUyxpQkFBaUI7QUFDeEIsTUFBSSxDQUFDLGFBQWE7QUFDaEIsVUFBTSxTQUFTLFFBQVEsSUFBSTtBQUMzQixVQUFNLFNBQVMsUUFBUSxJQUFJLGlCQUFpQjtBQUM1QyxVQUFNLFdBQVcsUUFBUSxJQUFJLG1CQUFtQjtBQUNoRCxVQUFNLE9BQU8sUUFBUSxJQUFJLGVBQWU7QUFFeEMsWUFBUSxJQUFJLHFDQUFxQztBQUNqRCxZQUFRLElBQUksZUFBZSxRQUFRLDZCQUE2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d0JBQUksT0FBTyxJQUFJLFVBQVU7QUFDckIsMEJBQUksTUFBTTtBQUNkLDJCQUFPO0FBQUEsa0JBQ1g7QUFBQSxnQkFDSjtBQUNBLGdCQUFBRixLQUFJLEtBQUssS0FBSyxJQUFJO0FBQUEsY0FDdEIsQ0FBQztBQUNELHFCQUFPO0FBQUEsZ0JBQUM7QUFBQTtBQUFBLGNBQVk7QUFBQSxVQUM1QjtBQUFBLFFBQ0osQ0FBQztBQUFBLE1BQ0wsQ0FBQztBQUFBLElBQ0w7QUFBQSxFQUNKO0FBQ0o7QUFDQSxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUN4QixTQUFTLENBQUMsTUFBTSxHQUFHLGNBQWMsQ0FBQztBQUFBLEVBQ2xDLFNBQVM7QUFBQSxJQUNMLE9BQU87QUFBQSxNQUNILEtBQUtGLE1BQUssUUFBUUQsWUFBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJyb3V0ZXIiLCAidXVpZHY0IiwgIkdvb2dsZUdlbkFJIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIiwgImRvdGVudiIsICJfYSJdCn0K
