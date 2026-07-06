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
function snapToBoundary(text, rawOffset, hardLimit) {
  if (rawOffset <= 0) return 0;
  const searchWindowEnd = Math.min(rawOffset + 80, hardLimit);
  for (const bp of [". ", ".\n", "? ", "! ", "\n"]) {
    const idx = text.indexOf(bp, rawOffset);
    if (idx !== -1 && idx < searchWindowEnd) {
      return idx + bp.length;
    }
  }
  const spaceIdx = text.indexOf(" ", rawOffset);
  if (spaceIdx !== -1 && spaceIdx < searchWindowEnd) {
    return spaceIdx + 1;
  }
  let i = rawOffset;
  while (i > 0 && !/\s/.test(text[i - 1])) i--;
  return i > 0 ? i : rawOffset;
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
        if (e >= para.length) break;
        const rawNext = e - overlapChars;
        s = rawNext > s ? snapToBoundary(para, rawNext, e) : e;
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
function joinTextItems(items) {
  let out = "";
  let prevItem = null;
  for (const item of items) {
    const str = item.str;
    if (str === void 0) {
      prevItem = item;
      continue;
    }
    if (str === "") {
      if (!/\n$/.test(out)) out += "\n";
      prevItem = null;
      continue;
    }
    if (prevItem && prevItem.str) {
      const prevEnd = prevItem.transform[4] + (prevItem.width || 0);
      const curStart = item.transform[4];
      const gap = curStart - prevEnd;
      const fontH = Math.abs(item.transform[3]) || 10;
      const spaceThreshold = fontH * 0.25;
      const alreadySpaced = /\s$/.test(out) || /^\s/.test(str);
      if (!alreadySpaced && gap > spaceThreshold) {
        out += " ";
      }
    }
    out += str;
    prevItem = item;
  }
  return out;
}
async function parsePDFWithBoundaryMap(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);
    const pages = [];
    await pdf(buffer, {
      pagerender: (pageData) => {
        return pageData.getTextContent().then((tc) => {
          const pageText = joinTextItems(tc.items);
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
function getPageRange(charStart, charEnd, pageMap) {
  let startPage = null;
  let endPage = null;
  let bestPage = null;
  let maxOverlap = -1;
  for (const entry of pageMap) {
    const overlapStart = Math.max(charStart, entry.start);
    const overlapEnd = Math.min(charEnd, entry.end);
    const overlap = overlapEnd - overlapStart;
    if (overlap <= 0) continue;
    if (startPage === null) startPage = entry.page;
    endPage = entry.page;
    if (overlap > maxOverlap) {
      maxOverlap = overlap;
      bestPage = entry.page;
    }
  }
  if (startPage === null) {
    const lastPage = pageMap[pageMap.length - 1]?.page || 1;
    return { page: lastPage, pageStart: lastPage, pageEnd: lastPage };
  }
  return { page: bestPage, pageStart: startPage, pageEnd: endPage };
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
    const chunks = rawChunks.map((chunk, idx) => {
      const { page, pageStart, pageEnd } = getPageRange(chunk.charStart, chunk.charEnd, pageMap);
      return {
        text: chunk.text,
        metadata: {
          document_id: documentId,
          filename: cleanFilename,
          chunk_id: createHash("md5").update(`${cleanFilename}::${chunk.text}`).digest("hex").slice(0, 16),
          chunk_index: idx,
          total_chunks: rawChunks.length,
          page_number: page,
          // majority page — kept for backward compatibility
          page_start: pageStart,
          // new: first page this chunk overlaps
          page_end: pageEnd,
          // new: last page this chunk overlaps
          total_pages: totalPages,
          source_type: "session_upload",
          session_id: sessionId,
          upload_timestamp: (/* @__PURE__ */ new Date()).toISOString(),
          char_start: chunk.charStart,
          char_end: chunk.charEnd,
          token_count: chunk.tokenCount
        }
      };
    });
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

// server/services/supabaseService.js
import { createClient } from "file:///home/project/node_modules/@supabase/supabase-js/dist/index.mjs";
function insertConversationAsync(sessionId, data) {
  const previousPromise = sessionInsertPromises.get(sessionId) || Promise.resolve();
  const nextPromise = previousPromise.then(async () => {
    console.log(`[Supabase] Inserting conversation for session ${sessionId}, answer_key: ${data.answer_key}`);
    const { error } = await supabase.from("Conversation_History").insert(data);
    if (error) {
    } else {
      console.log(`[Supabase] Successfully inserted conversation for session ${sessionId}`);
    }
  }).catch((err) => {
  });
  sessionInsertPromises.set(sessionId, nextPromise);
  nextPromise.finally(() => {
    if (sessionInsertPromises.get(sessionId) === nextPromise) {
      sessionInsertPromises.delete(sessionId);
    }
  });
  return nextPromise;
}
async function updateFeedbackAsync(answerKey, feedback, retries = 2) {
  try {
    const { error } = await supabase.from("Conversation_History").update({ feedback }).eq("answer_key", answerKey);
    if (error) {
      throw error;
    } else {
      console.log(`[Supabase] Successfully updated feedback for answer_key: ${answerKey}`);
    }
  } catch (error) {
    const isNetworkError = error.message && error.message.includes("fetch failed");
    if (isNetworkError && retries > 0) {
      await new Promise((res) => setTimeout(res, 500));
      return updateFeedbackAsync(answerKey, feedback, retries - 1);
    }
    throw error;
  }
}
var supabaseUrl, supabaseKey, supabase, sessionInsertPromises;
var init_supabaseService = __esm({
  "server/services/supabaseService.js"() {
    "use strict";
    supabaseUrl = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
    supabaseKey = process.env.VITE_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY;
    if (!supabaseUrl || !supabaseKey) {
      console.warn("Supabase URL or Key is missing. Database operations will not work properly.");
    }
    supabase = createClient(
      supabaseUrl || "http://localhost",
      supabaseKey || "public-anon-key"
    );
    sessionInsertPromises = /* @__PURE__ */ new Map();
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
  const { query, sessionId: providedSessionId, convId: providedConvId, messageId } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({ error: "Query is required", code: "MISSING_QUERY" });
  }
  const sessionId = providedSessionId || uuidv45();
  const convId = providedConvId || uuidv45();
  const answerId = messageId || uuidv45();
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
    const prompt = `You are an AI Knowledge Assistant for PERSONAL FINANCE EDUCATION ONLY.
    
Explain financial concepts, terms, metrics, and frameworks only using the provided context. You MUST NOT provide financial, investment, legal, tax, or insurance advice, and you MUST NOT recommend, endorse, rate, compare, or judge the suitability of any stock, fund, ETF, index, insurance product, strategy, timing decision, buy/sell/hold/switch/redeem action, or allocation \u2014 under any framing, including hypothetical or "just your opinion".

GLOBAL RULES
- Never say whether to buy/sell/hold/switch/redeem/invest in anything specific, predict returns/prices/market direction, or judge suitability.
- Never evaluate a security or fund the user names \u2014 explain the general category, concept, or metric instead, if supported by the provided context.
- If a question mixes personal details (a return %, fund name, amount) with a decision request, refuse the decision and explain only the general framework \u2014 never reason about the user's specific numbers, holdings, or product.
- Treat reframed/hypothetical/"casual opinion" versions of advice requests as still seeking advice; hold the same boundary.
- Don't let explanations imply a recommendation. Don't ask questions that edge toward personalization. Note that a qualified financial advisor can help with personal decisions, where relevant.
- If the provided context is absent, weak, or not directly relevant, do not answer from prior knowledge.

1. GREETINGS & SMALL TALK
- Respond warmly and naturally.
- Do not mention the knowledge base or documents.
- Do not add citations.

2. EDUCATIONAL QUESTIONS WITH CONTEXT
- Answer fully using only the numbered context.
- Connecting related concepts is encouraged if they are supported by the context.
- Stay neutral \u2014 explain, never recommend.
- Cite as [1] [2], never [1, 2].
- Cite only the numbers actually used.

3. ADVICE / RECOMMENDATION / PERSONAL-DECISION QUESTIONS
Examples: Should I invest now? Is this a good fund? Should I sell?
- Refuse politely, in natural language each time \u2014 no fixed template.
- State plainly that you provide education, not financial or investment advice.
- Do not mention or analyze the user's named fund, stock, return, NAV, or holding except to restate that you cannot advise on it.
- Pivot to explaining the concept or how that category is evaluated generally \u2014 without referencing the user's specific numbers, holdings, or decision.
- No citations.

4. NO USABLE CONTEXT
4a. Finance-related but uncovered
Includes finance questions not covered by the provided material, and requests for current prices, NAVs, ratios, returns, or performance figures that require live data.
- Decline politely, in natural language each time \u2014 no fixed template.
- State that you do not have material covering that specific topic, or that the request needs current/live data you do not have.
- State that you can answer only from the available educational content.
- No citations.
4b. Unrelated to finance / out of scope
Includes general knowledge, coding, writing, math, task completion, and any request outside the role of a personal finance education assistant.
- Decline politely, in natural language each time \u2014 no fixed template.
- State plainly that you are a personal finance education assistant and that this request falls outside that scope.
- Do not attempt the task, even partially, even if you know the answer.
- No citations.

5. STYLE
- Clear, calm, and non-promotional.
- Prefer phrases like \u201CThis means\u2026\u201D, \u201CIn general\u2026\u201D, and \u201CAccording to the provided material\u2026\u201D
- Never say:
  - \u201CYou should invest\u2026\u201D
  - \u201CThis is a good fund\u2026\u201D
  - \u201CI recommend\u2026\u201D
  - \u201CYou can buy\u2026\u201D
  - \u201CThis stock will\u2026\u201D
  - \u201CYou should continue/sell/redeem\u2026\u201D

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
    const chunksList = finalSources.map((s, i) => ({
      [`chunk${i + 1}`]: s.excerpt || s.text || ""
    }));
    const conversationJson = {
      session_id: sessionId,
      query,
      chunks: chunksList,
      llm_response: rewrittenResponse
    };
    insertConversationAsync(sessionId, {
      answer_key: answerId,
      feedback: "none",
      conversation: conversationJson
    });
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
async function handleFeedback(req, res) {
  const { answerId, feedback } = req.body;
  if (!answerId || !feedback) {
    return res.status(400).json({ error: "Missing answerId or feedback" });
  }
  try {
    await updateFeedbackAsync(answerId, feedback);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message || "Error updating feedback" });
  }
}
var router3, OUT_OF_SCOPE_PATTERN, chat_default;
var init_chat = __esm({
  "server/api/chat.js"() {
    "use strict";
    init_retrievalService();
    init_geminiService();
    init_memoryService();
    init_sessionService();
    init_supabaseService();
    router3 = Router3();
    OUT_OF_SCOPE_PATTERN = /don't have information|do not have information|not in my knowledge|can't find|cannot find|no information|knowledge base doesn't|not covered|outside.*knowledge/i;
    router3.post("/", handleChatStream);
    router3.post("/feedback", handleFeedback);
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9jaGF0LmpzIiwgInNlcnZlci9hcGkvZmVlZGJhY2suanMiLCAic2VydmVyL2FwcC5qcyIsICJ2aXRlLmNvbmZpZy5qcyJdLAogICJzb3VyY2VzQ29udGVudCI6IFsiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanNcIjtpbXBvcnQgeyBDbG91ZENsaWVudCwgU2NoZW1hLCBTcGFyc2VWZWN0b3JJbmRleENvbmZpZywgRE9DVU1FTlRfS0VZLCBTZWFyY2gsIEtubiwgUnJmIH0gZnJvbSAnY2hyb21hZGInO1xuaW1wb3J0IHsgQ2hyb21hQm0yNUVtYmVkZGluZ0Z1bmN0aW9uIH0gZnJvbSAnQGNocm9tYS1jb3JlL2Nocm9tYS1ibTI1JztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuXG4vLyBcdTI1MDBcdTI1MDAgU2hhcmVkIHNjaGVtYTogZGVuc2UgZW1iZWRkaW5ncyAobWFuYWdlZCBleHRlcm5hbGx5KSArIEJNMjUgc3BhcnNlIGluZGV4IFx1MjUwMFx1MjUwMFxuY29uc3QgYm0yNUVtYmVkZGluZ0Z1bmN0aW9uID0gbmV3IENocm9tYUJtMjVFbWJlZGRpbmdGdW5jdGlvbigpO1xuY29uc3QgY29sbGVjdGlvblNjaGVtYSA9IG5ldyBTY2hlbWEoKS5jcmVhdGVJbmRleChcbiAgbmV3IFNwYXJzZVZlY3RvckluZGV4Q29uZmlnKHtcbiAgICBlbWJlZGRpbmdGdW5jdGlvbjogYm0yNUVtYmVkZGluZ0Z1bmN0aW9uLFxuICAgIHNvdXJjZUtleTogRE9DVU1FTlRfS0VZLFxuICAgIGJtMjU6IHRydWVcbiAgfSksXG4gICdzcGFyc2VfYm0yNSdcbik7XG5cbmxldCBjbG91ZENsaWVudCA9IG51bGw7XG5sZXQgZ2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldENsb3VkQ2xpZW50KCkge1xuICBpZiAoIWNsb3VkQ2xpZW50KSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0FQSV9LRVk7XG4gICAgY29uc3QgdGVuYW50ID0gcHJvY2Vzcy5lbnYuQ0hST01BX1RFTkFOVCB8fCAnZGVmYXVsdF90ZW5hbnQnO1xuICAgIGNvbnN0IGRhdGFiYXNlID0gcHJvY2Vzcy5lbnYuQ0hST01BX0RBVEFCQVNFIHx8ICdkZWZhdWx0X2RhdGFiYXNlJztcbiAgICBjb25zdCBob3N0ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0hPU1QgfHwgdW5kZWZpbmVkO1xuXG4gICAgY29uc29sZS5sb2coXCItLS0tIENIUk9NQSBDT05ORUNUSVZJVFkgREVCVUcgLS0tLVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIkhvc3Q6ICAgICAgXCIsIGhvc3QgfHwgXCJhcGkudHJ5Y2hyb21hLmNvbSAoZGVmYXVsdClcIik7XG4gICAgY29uc29sZS5sb2coXCJUZW5hbnQ6ICAgIFwiLCB0ZW5hbnQpO1xuICAgIGNvbnNvbGUubG9nKFwiREIgTmFtZTogICBcIiwgZGF0YWJhc2UpO1xuICAgIGNvbnNvbGUubG9nKFwiQVBJIEtleTogICBcIiwgYXBpS2V5ID8gXCJMT0FERUQgKFZBTElEKVwiIDogXCJNSVNTSU5HIChVTkRFRklORUQpXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cIik7XG5cbiAgICBpZiAoIWFwaUtleSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBcIkNSSVRJQ0FMIEVSUk9SOiBDSFJPTUFfQVBJX0tFWSBpcyB1bmRlZmluZWQuIFwiICtcbiAgICAgICAgXCJFbnN1cmUgeW91ciBlbnZpcm9ubWVudCB2YXJpYWJsZXMgYXJlIGNvcnJlY3RseSBsb2FkZWQgYmVmb3JlIGV4ZWN1dGluZyB0aGlzIGZpbGUuXCJcbiAgICAgICk7XG4gICAgfVxuXG4gICAgY29uc3QgY2xpZW50T3B0aW9ucyA9IHsgYXBpS2V5LCB0ZW5hbnQsIGRhdGFiYXNlIH07XG4gICAgaWYgKGhvc3QpIGNsaWVudE9wdGlvbnMuaG9zdCA9IGhvc3Q7XG4gICAgY2xvdWRDbGllbnQgPSBuZXcgQ2xvdWRDbGllbnQoY2xpZW50T3B0aW9ucyk7XG4gIH1cbiAgcmV0dXJuIGNsb3VkQ2xpZW50O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0R2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFnbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IHByb2Nlc3MuZW52LkNIUk9NQV9HTE9CQUxfQ09MTEVDVElPTiB8fCAnc2VlZF9kYic7XG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIHNjaGVtYTogY29sbGVjdGlvblNjaGVtYSxcbiAgICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgICBkZXNjcmlwdGlvbjogJ1Blcm1hbmVudCBzZWVkIGRvY3VtZW50cyBmb3IgUkFHJyxcbiAgICAgICAgICB0eXBlOiAnZ2xvYmFsX2tub3dsZWRnZSdcbiAgICAgICAgfSxcbiAgICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYFxcdTI3MDUgR2xvYmFsIGNvbGxlY3Rpb24gcmVhZHk6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjb25uZWN0IHRvIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHRoZSBzaW5nbGUgc2hhcmVkIGNvbGxlY3Rpb24uXG4gKiBEcm9wLWluIHJlcGxhY2VtZW50IGZvciB0aGUgb2xkIGdldFNlc3Npb25Db2xsZWN0aW9uIFx1MjAxNCBjYWxsZXJzIHRoYXRcbiAqIHByZXZpb3VzbHkgZGVzdHJ1Y3R1cmVkIHsgY29sbGVjdGlvbiB9IHdpbGwgc3RpbGwgd29yay5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldENvbGxlY3Rpb24oKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIHJldHVybiB7IGNvbGxlY3Rpb24sIGlzTmV3OiBmYWxzZSB9O1xufVxuXG4vKipcbiAqIEFkZCB2ZWN0b3JzIGluIGJhdGNoZXMgb2YgQkFUQ0hfU0laRSB0byBhdm9pZCBDaHJvbWEgcGF5bG9hZCBsaW1pdHMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBhZGRWZWN0b3JzKGNvbGxlY3Rpb24sIHZlY3RvcnMsIGVtYmVkZGluZ3MsIGlkcykge1xuICB0cnkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgaWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBjb25zdCBiYXRjaElkcyA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi50ZXh0KTtcbiAgICAgIGNvbnN0IGJhdGNoTWV0YWRhdGFzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi5tZXRhZGF0YSk7XG5cbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uYWRkKHtcbiAgICAgICAgaWRzOiBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6IGJhdGNoRG9jdW1lbnRzLFxuICAgICAgICBtZXRhZGF0YXM6IGJhdGNoTWV0YWRhdGFzXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFthZGRWZWN0b3JzXSBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IGFkZGVkICR7YmF0Y2hJZHMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gICAgfVxuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBhZGQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSyA9IDUsIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcXVlcnlPcHRzID0ge1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9O1xuICAgIGlmICh3aGVyZSkgcXVlcnlPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5xdWVyeShxdWVyeU9wdHMpO1xuXG4gICAgaWYgKCFyZXN1bHRzLmlkcyB8fCByZXN1bHRzLmlkcy5sZW5ndGggPT09IDAgfHwgcmVzdWx0cy5pZHNbMF0ubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdHMuaWRzWzBdLm1hcCgoaWQsIGlkeCkgPT4gKHtcbiAgICAgIGlkLFxuICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHNbMF1baWR4XSxcbiAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhc1swXVtpZHhdLFxuICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF0sXG4gICAgICBzY29yZTogMSAtIHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF1cbiAgICB9KSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIHF1ZXJ5IGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogSHlicmlkIHNlYXJjaCB1c2luZyBDaHJvbWEgQ2xvdWQgU2VhcmNoIEFQSSB3aXRoIFJSRiAoZGVuc2UgKyBzcGFyc2UgQk0yNSkuXG4gKiBSZXR1cm5zIHJlc3VsdHMgaW4gdGhlIHNhbWUgc2hhcGUgYXMgcXVlcnlDb2xsZWN0aW9uKCkgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHkuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcgKGUuZy4gc2Vzc2lvbl9pZCAkaW4pLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaHlicmlkUXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5VGV4dCwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1LCB3aGVyZSA9IHVuZGVmaW5lZCkge1xuICB0cnkge1xuICAgIGxldCBzZWFyY2ggPSBuZXcgU2VhcmNoKClcbiAgICAgIC5yYW5rKFJyZih7XG4gICAgICAgIHJhbmtzOiBbXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5RW1iZWRkaW5nLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMTAwIH0pLFxuICAgICAgICAgIEtubih7IHF1ZXJ5OiBxdWVyeVRleHQsIGtleTogJ3NwYXJzZV9ibTI1JywgcmV0dXJuUmFuazogdHJ1ZSwgbGltaXQ6IDEwMCB9KVxuICAgICAgICBdLFxuICAgICAgICB3ZWlnaHRzOiBbMC45LCAwLjFdLFxuICAgICAgICBrOiA2MFxuICAgICAgfSkpXG4gICAgICAud2hlcmUod2hlcmUpXG4gICAgICAuc2VsZWN0KFwiI2RvY3VtZW50XCIsIFwiI21ldGFkYXRhXCIsIFwiI3Njb3JlXCIpXG4gICAgICAubGltaXQodG9wSyk7XG5cbiAgICBjb25zdCByYXcgPSBhd2FpdCBjb2xsZWN0aW9uLnNlYXJjaChzZWFyY2gpO1xuXG4gICAgLy8gUGFyYWxsZWxcdTIwMTFhcnJheSBzdHJ1Y3R1cmU6IGlkc1swXSwgZG9jdW1lbnRzWzBdLCBtZXRhZGF0YXNbMF0sIHNjb3Jlc1swXVxuICAgIGlmICghcmF3LmlkcyB8fCAhcmF3Lmlkc1swXSB8fCByYXcuaWRzWzBdLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cblxuICAgIGNvbnN0IGlkcyA9IHJhdy5pZHNbMF07XG4gICAgY29uc3QgZG9jcyA9IHJhdy5kb2N1bWVudHM/LlswXSA/PyBbXTtcbiAgICBjb25zdCBtZXRhcyA9IHJhdy5tZXRhZGF0YXM/LlswXSA/PyBbXTtcbiAgICBjb25zdCBzY29yZXMgPSByYXcuc2NvcmVzPy5bMF0gPz8gW107XG5cbiAgICAvLyAxLiBEZWZpbmUgZ2xvYmFsIFJSRiBib3VuZHMgYmFzZWQgb24geW91ciB3ZWlnaHRzIFswLjcsIDAuM10gYW5kIGxpbWl0cyAoMTAwKVxuICAgIC8vIE1heCBwb3NzaWJsZSByYXcgUlJGOiAxIC8gKDYwICsgMSkgPSAwLjAxNjM5MzRcbiAgICAvLyBNaW4gcG9zc2libGUgcmF3IFJSRjogMSAvICg2MCArIDEwMCkgPSAwLjAwNjI1MDBcbiAgICBjb25zdCBNQVhfUlJGID0gMSAvIDYxO1xuICAgIGNvbnN0IE1JTl9SUkYgPSAxIC8gMTYwO1xuXG4gICAgcmV0dXJuIGlkcy5tYXAoKGlkLCBpZHgpID0+IHtcbiAgICAgIC8vIENocm9tYSByZXR1cm5zIG5lZ2F0aXZlIHZhbHVlcyAoZS5nLiAtMC4wMTYzOSksIGNvbnZlcnQgdG8gcG9zaXRpdmUgcmF3IFJSRlxuICAgICAgY29uc3QgcmF3UlJGID0gTWF0aC5hYnMoc2NvcmVzW2lkeF0gPz8gTUlOX1JSRik7XG5cbiAgICAgIC8vIDIuIExpbmVhciBtaW4tbWF4IG5vcm1hbGl6YXRpb24gdG8gZml0IHBlcmZlY3RseSBiZXR3ZWVuIDAuMCBhbmQgMS4wXG4gICAgICBsZXQgbm9ybWFsaXplZFNjb3JlID0gKHJhd1JSRiAtIE1JTl9SUkYpIC8gKE1BWF9SUkYgLSBNSU5fUlJGKTtcblxuICAgICAgLy8gQm91bmRhcnkgcHJvdGVjdGlvblxuICAgICAgbm9ybWFsaXplZFNjb3JlID0gTWF0aC5tYXgoMCwgTWF0aC5taW4oMSwgbm9ybWFsaXplZFNjb3JlKSk7XG5cbiAgICAgIC8vY29uc3QgZmluYWxTY29yZSA9IE1hdGgucm91bmQobm9ybWFsaXplZFNjb3JlICogMTAwKSAvIDEwMDtcblxuICAgICAgcmV0dXJuIHtcbiAgICAgICAgaWQsXG4gICAgICAgIHRleHQ6IGRvY3NbaWR4XSA/PyAnJyxcbiAgICAgICAgbWV0YWRhdGE6IG1ldGFzW2lkeF0gPz8ge30sXG4gICAgICAgIGRpc3RhbmNlOiAxIC0gbm9ybWFsaXplZFNjb3JlLFxuICAgICAgICBzY29yZTogbm9ybWFsaXplZFNjb3JlXG4gICAgICB9O1xuICAgIH0pO1xuXG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdIeWJyaWQgcXVlcnkgZmFpbGVkLCBmYWxsaW5nIGJhY2sgdG8gZGVuc2Utb25seTonLCBlcnJvci5tZXNzYWdlKTtcbiAgICAvLyBHcmFjZWZ1bCBmYWxsYmFjayB0byBkZW5zZS1vbmx5IHNlYXJjaCBmb3IgYmFja3dhcmQgY29tcGF0aWJpbGl0eVxuICAgIHJldHVybiBxdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEssIHdoZXJlKTtcbiAgfVxufVxuXG4vKipcbiAqIERlbGV0ZSBhbGwgdmVjdG9ycyBmb3IgYSBnaXZlbiBkb2N1bWVudElkLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgaW4gQkFUQ0hfU0laRSBjaHVua3Mgc28gZG9jdW1lbnRzIHdpdGhcbiAqIG1hbnkgY2h1bmtzICg+ZGVmYXVsdCAxMDAgbGltaXQpIGFyZSBmdWxseSBkZWxldGVkLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxJZHMgPSBbXTtcbiAgICBsZXQgb2Zmc2V0ID0gMDtcblxuICAgIHdoaWxlICh0cnVlKSB7XG4gICAgICBjb25zdCBiYXRjaCA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgICAgd2hlcmU6IHsgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQgfSxcbiAgICAgICAgaW5jbHVkZTogW10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBhbGxJZHMgfSk7XG4gICAgfVxuICAgIHJldHVybiBhbGxJZHMubGVuZ3RoO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgYmVsb25naW5nIHRvIGEgc3BlY2lmaWMgc2Vzc2lvbi5cbiAqIFVzZXMgc2Vzc2lvbl9pZCBtZXRhZGF0YSBmaWx0ZXIgdG8gZmluZCBhbmQgcmVtb3ZlIHRoZW0gaW4gYmF0Y2hlcy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb25WZWN0b3JzKHNlc3Npb25JZCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gICAgY29uc3QgYWxsSWRzID0gW107XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICAgIHdoZXJlOiB7IHNlc3Npb25faWQ6IHNlc3Npb25JZCB9LFxuICAgICAgICBpbmNsdWRlOiBbXSxcbiAgICAgICAgbGltaXQ6IEJBVENIX1NJWkUsXG4gICAgICAgIG9mZnNldFxuICAgICAgfSk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuICAgICAgYWxsSWRzLnB1c2goLi4uYmF0Y2guaWRzKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIGlmIChhbGxJZHMubGVuZ3RoID4gMCkge1xuICAgICAgYXdhaXQgY29sbGVjdGlvbi5kZWxldGUoeyBpZHM6IGFsbElkcyB9KTtcbiAgICB9XG4gICAgY29uc29sZS5sb2coYFxcdTI3MDUgRGVsZXRlZCAke2FsbElkcy5sZW5ndGh9IHNlc3Npb24gdmVjdG9ycyBmb3Igc2Vzc2lvbl9pZD0ke3Nlc3Npb25JZH1gKTtcbiAgICByZXR1cm4gYWxsSWRzLmxlbmd0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBGYWlsZWQgdG8gZGVsZXRlIHNlc3Npb24gdmVjdG9ycyBmb3IgJHtzZXNzaW9uSWR9OmAsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRDb3VudChjb2xsZWN0aW9uKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGF3YWl0IGNvbGxlY3Rpb24uY291bnQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZ2V0IGRvY3VtZW50IGNvdW50OicsIGVycm9yKTtcbiAgICByZXR1cm4gMDtcbiAgfVxufVxuXG4vKipcbiAqIExpc3QgYWxsIHVuaXF1ZSBkb2N1bWVudHMgaW4gYSBjb2xsZWN0aW9uLlxuICogUGFnaW5hdGVzIGNvbGxlY3Rpb24uZ2V0KCkgd2l0aCBCQVRDSF9TSVpFPTMwMCBzbyBjb2xsZWN0aW9ucyBsYXJnZXJcbiAqIHRoYW4gQ2hyb21hJ3MgZGVmYXVsdCBnZXQoKSBsaW1pdCAoMTAwKSBhcmUgZnVsbHkgZW51bWVyYXRlZC5cbiAqIEFjY2VwdHMgYW4gb3B0aW9uYWwgYHdoZXJlYCBjbGF1c2UgZm9yIG1ldGFkYXRhIGZpbHRlcmluZy5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbiwgd2hlcmUgPSB1bmRlZmluZWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBkb2N1bWVudHNNYXAgPSBuZXcgTWFwKCk7XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgZ2V0T3B0cyA9IHtcbiAgICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH07XG4gICAgICBpZiAod2hlcmUpIGdldE9wdHMud2hlcmUgPSB3aGVyZTtcblxuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldChnZXRPcHRzKTtcblxuICAgICAgaWYgKCFiYXRjaC5pZHMgfHwgYmF0Y2guaWRzLmxlbmd0aCA9PT0gMCkgYnJlYWs7XG5cbiAgICAgIGJhdGNoLmlkcy5mb3JFYWNoKChpZCwgaWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IG1ldGEgPSBiYXRjaC5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogZG9jSWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2NvdW50OiAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6IG1ldGEuc291cmNlX3R5cGUsXG4gICAgICAgICAgICBmaXJzdF9jaHVua190ZXh0OiBiYXRjaC5kb2N1bWVudHNbaWR4XVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgZG9jID0gZG9jdW1lbnRzTWFwLmdldChkb2NJZCk7XG4gICAgICAgIGRvYy5jaHVua19jb3VudCsrO1xuICAgICAgICBkb2MucGFnZV9jb3VudCA9IE1hdGgubWF4KGRvYy5wYWdlX2NvdW50LCBtZXRhLnBhZ2VfbnVtYmVyIHx8IDEpO1xuICAgICAgfSk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGAgIFtsaXN0RG9jdW1lbnRzXSBvZmZzZXQ9JHtvZmZzZXR9LCBnb3Q9JHtiYXRjaC5pZHMubGVuZ3RofSwgdW5pcXVlIHNvIGZhcj0ke2RvY3VtZW50c01hcC5zaXplfWApO1xuXG4gICAgICBpZiAoYmF0Y2guaWRzLmxlbmd0aCA8IEJBVENIX1NJWkUpIGJyZWFrO1xuICAgICAgb2Zmc2V0ICs9IEJBVENIX1NJWkU7XG4gICAgfVxuXG4gICAgcmV0dXJuIEFycmF5LmZyb20oZG9jdW1lbnRzTWFwLnZhbHVlcygpKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHM6JywgZXJyb3IpO1xuICAgIHJldHVybiBbXTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoQ2hlY2soKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBoZWFydGJlYXQgPSBhd2FpdCBjbGllbnQuaGVhcnRiZWF0KCk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ2hlYWx0aHknLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBoZWFydGJlYXRcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICd1bmhlYWx0aHknLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgIH07XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IGhlYWx0aENoZWNrIGFzIGNocm9tYUhlYWx0aENoZWNrIH0gZnJvbSAnLi4vc2VydmljZXMvY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBPdmVyYWxsIHN0YXR1c1xuICBjb25zdCBoYXNFcnJvcnMgPSBPYmplY3QudmFsdWVzKGhlYWx0aFN0YXR1cy5zZXJ2aWNlcykuc29tZShcbiAgICBzID0+IHMuc3RhdHVzID09PSAnZXJyb3InIHx8IHMuc3RhdHVzID09PSAndW5oZWFsdGh5J1xuICApO1xuXG4gIGlmIChoYXNFcnJvcnMpIHtcbiAgICBoZWFsdGhTdGF0dXMuc3RhdHVzID0gJ2RlZ3JhZGVkJztcbiAgfVxuXG4gIHJlcy5qc29uKGhlYWx0aFN0YXR1cyk7XG59XG5cbnJvdXRlci5nZXQoJy8nLCBoZWFsdGgpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9lcnJvcnMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7ZXhwb3J0IGNsYXNzIEFwcEVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlLCBzdGF0dXNDb2RlID0gNTAwKSB7XG4gICAgc3VwZXIobWVzc2FnZSk7XG4gICAgdGhpcy5jb2RlID0gY29kZTtcbiAgICB0aGlzLnN0YXR1c0NvZGUgPSBzdGF0dXNDb2RlO1xuICAgIHRoaXMuaXNPcGVyYXRpb25hbCA9IHRydWU7XG4gICAgRXJyb3IuY2FwdHVyZVN0YWNrVHJhY2UodGhpcywgdGhpcy5jb25zdHJ1Y3Rvcik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFZhbGlkYXRpb25FcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSA9ICdWQUxJREFUSU9OX0VSUk9SJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFVwbG9hZExpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVVBMT0FEX0xJTUlUX0VYQ0VFREVEJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsIGNvZGUsIDQwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEZpbGVUb29MYXJnZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXhTaXplTUIpIHtcbiAgICBzdXBlcihgRmlsZSBleGNlZWRzIG1heGltdW0gc2l6ZSBvZiAke21heFNpemVNQn1NQmAsICdGSUxFX1RPT19MQVJHRScsIDQxMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEludmFsaWRGaWxlVHlwZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignT25seSBQREYgZmlsZXMgYXJlIGFsbG93ZWQnLCAnSU5WQUxJRF9GSUxFX1RZUEUnLCA0MTUpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBUb29NYW55UERGc0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtYXgpIHtcbiAgICBzdXBlcihgTWF4aW11bSAke21heH0gUERGcyBhbGxvd2VkIHBlciBzZXNzaW9uYCwgJ1RPT19NQU5ZX1BERlMnLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBEdXBsaWNhdGVGaWxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKGZpbGVuYW1lKSB7XG4gICAgc3VwZXIoYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmAsICdEVVBMSUNBVEVfRklMRScsIDQwOSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvcnJ1cHRlZFBERkVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRmFpbGVkIHRvIHBhcnNlIFBERiBmaWxlLiBJdCBtYXkgYmUgY29ycnVwdGVkLicsICdDT1JSVVBURURfUERGJywgNDIyKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUmF0ZUxpbWl0RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKHJldHJ5QWZ0ZXIgPSA2MCkge1xuICAgIHN1cGVyKCdSYXRlIGxpbWl0IGV4Y2VlZGVkLiBQbGVhc2UgdHJ5IGFnYWluIGxhdGVyLicsICdSQVRFX0xJTUlUX0VYQ0VFREVEJywgNDI5KTtcbiAgICB0aGlzLnJldHJ5QWZ0ZXIgPSByZXRyeUFmdGVyO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBMTE1VbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignQUkgc2VydmljZSBpcyB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZS4gUGxlYXNlIHRyeSBhZ2Fpbi4nLCAnTExNX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRW1iZWRkaW5nRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UgPSAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnKSB7XG4gICAgc3VwZXIobWVzc2FnZSwgJ0VNQkVERElOR19FUlJPUicsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJldHJpZXZhbFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdEb2N1bWVudCByZXRyaWV2YWwgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUnLCAnUkVUUklFVkFMX1VOQVZBSUxBQkxFJywgNTAzKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ292ZXJhZ2VUb29Mb3dFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0luc3VmZmljaWVudCBpbmZvcm1hdGlvbiBpbiBrbm93bGVkZ2UgYmFzZScsICdDT1ZFUkFHRV9UT09fTE9XJywgMjAwKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNSZXRyeWFibGVFcnJvcihlcnJvcikge1xuICBjb25zdCByZXRyeWFibGVDb2RlcyA9IFsnUkFURV9MSU1JVF9FWENFRURFRCcsICdFTUJFRERJTkdfRVJST1InLCAnTExNX1VOQVZBSUxBQkxFJ107XG4gIHJldHVybiByZXRyeWFibGVDb2Rlcy5pbmNsdWRlcyhlcnJvci5jb2RlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzNDI5RXJyb3IoZXJyb3IpIHtcbiAgcmV0dXJuIGVycm9yPy5jb2RlID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCc0MjknKSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdUb28gTWFueSBSZXF1ZXN0cycpO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcblxuY29uc3QgQ0hBUlNfUEVSX1RPS0VOID0gNDtcbmNvbnN0IFRBUkdFVF9DSFVOS19UT0tFTlMgPSA2MDA7ICAgLy8gc29mdCB0YXJnZXQgcGVyIGNodW5rXG5jb25zdCBNQVhfQ0hVTktfVE9LRU5TID0gNzUwOyAgIC8vIGhhcmQgY2FwIGJlZm9yZSBmb3JjZWQgc3BsaXRcbmNvbnN0IE9WRVJMQVBfVE9LRU5TID0gMTAwOyAgIC8vIG92ZXJsYXAgb25seSBvbiBvdmVyc2l6ZWQgcGFyYWdyYXBoc1xuY29uc3QgTUlOX0NIVU5LX0NIQVJTID0gMTAwO1xuXG4vLyBNYXRjaGVzIEFMTC1DQVBTIGhlYWRpbmdzLCBtYXJrZG93biBoZWFkaW5ncywgb3IgbnVtYmVyZWQgc2VjdGlvbiBoZWFkaW5nc1xuY29uc3QgSEVBRElOR19SRSA9IC9eKD86W0EtWl1bQS1aXFxzXXsyLDYwfSR8I3sxLDR9XFxzLit8KD86XFxkK1xcLikrXFxzLispL207XG5cbmV4cG9ydCBmdW5jdGlvbiBlc3RpbWF0ZVRva2Vucyh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAwO1xuICByZXR1cm4gTWF0aC5jZWlsKHRleHQubGVuZ3RoIC8gQ0hBUlNfUEVSX1RPS0VOKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFuVGV4dCh0ZXh0KSB7XG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiAnJztcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvXFxmL2csICdcXG4nKVxuICAgIC5yZXBsYWNlKC8oXFxzKlxcbil7Myx9L2csICdcXG5cXG4nKVxuICAgIC5yZXBsYWNlKC9eXFxzKlxcZCtcXHMqJC9nbSwgJycpXG4gICAgLnJlcGxhY2UoL1sgXFx0XXsyLH0vZywgJyAnKVxuICAgIC50cmltKCk7XG59XG5cbmZ1bmN0aW9uIGdlbmVyYXRlQ2h1bmtJZCh0ZXh0LCBmaWxlbmFtZSkge1xuICByZXR1cm4gY3JlYXRlSGFzaCgnbWQ1JylcbiAgICAudXBkYXRlKGAke2ZpbGVuYW1lfTo6JHt0ZXh0fWApXG4gICAgLmRpZ2VzdCgnaGV4JylcbiAgICAuc2xpY2UoMCwgMTYpO1xufVxuXG4vKipcbiAqIEdpdmVuIGEgcmF3IChwb3NzaWJseSBtaWQtd29yZCkgb2Zmc2V0LCBzbmFwIGZvcndhcmQgdG8gdGhlIG5lYXJlc3RcbiAqIGNsZWFuIHNlbnRlbmNlIHN0YXJ0LCBmYWxsaW5nIGJhY2sgdG8gdGhlIG5lYXJlc3Qgd29yZCBib3VuZGFyeSxcbiAqIHNvIG92ZXJsYXBwZWQgY2h1bmtzIG5ldmVyIGJlZ2luIG1pZC1zZW50ZW5jZSBvciBtaWQtd29yZC5cbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gdGV4dCAgICAgICB0aGUgcGFyYWdyYXBoL3RleHQgYmVpbmcgd2luZG93ZWRcbiAqIEBwYXJhbSB7bnVtYmVyfSByYXdPZmZzZXQgIHRoZSByYXcgKHVuc25hcHBlZCkgc3RhcnQgb2Zmc2V0IGZvciB0aGUgbmV4dCB3aW5kb3dcbiAqIEBwYXJhbSB7bnVtYmVyfSBoYXJkTGltaXQgIGRvbid0IHNlYXJjaCBwYXN0IHRoaXMgb2Zmc2V0IChlbmQgb2YgcHJldmlvdXMgd2luZG93KVxuICovXG5mdW5jdGlvbiBzbmFwVG9Cb3VuZGFyeSh0ZXh0LCByYXdPZmZzZXQsIGhhcmRMaW1pdCkge1xuICBpZiAocmF3T2Zmc2V0IDw9IDApIHJldHVybiAwO1xuXG4gIC8vIFByZWZlciBhIHJlYWwgc2VudGVuY2UgYm91bmRhcnkgd2l0aGluIGEgc21hbGwgZm9yd2FyZCB3aW5kb3dcbiAgY29uc3Qgc2VhcmNoV2luZG93RW5kID0gTWF0aC5taW4ocmF3T2Zmc2V0ICsgODAsIGhhcmRMaW1pdCk7IC8vIH44MCBjaGFycyBcdTIyNDggb25lIHNlbnRlbmNlXG4gIGZvciAoY29uc3QgYnAgb2YgWycuICcsICcuXFxuJywgJz8gJywgJyEgJywgJ1xcbiddKSB7XG4gICAgY29uc3QgaWR4ID0gdGV4dC5pbmRleE9mKGJwLCByYXdPZmZzZXQpO1xuICAgIGlmIChpZHggIT09IC0xICYmIGlkeCA8IHNlYXJjaFdpbmRvd0VuZCkge1xuICAgICAgcmV0dXJuIGlkeCArIGJwLmxlbmd0aDtcbiAgICB9XG4gIH1cblxuICAvLyBGYWxsIGJhY2s6IHNuYXAgdG8gdGhlIG5leHQgd29yZCBib3VuZGFyeSBzbyB3ZSBhdCBsZWFzdCBkb24ndFxuICAvLyBzcGxpdCBhIHdvcmQgaW4gaGFsZlxuICBjb25zdCBzcGFjZUlkeCA9IHRleHQuaW5kZXhPZignICcsIHJhd09mZnNldCk7XG4gIGlmIChzcGFjZUlkeCAhPT0gLTEgJiYgc3BhY2VJZHggPCBzZWFyY2hXaW5kb3dFbmQpIHtcbiAgICByZXR1cm4gc3BhY2VJZHggKyAxO1xuICB9XG5cbiAgLy8gTGFzdCByZXNvcnQ6IGlmIHRoZSBjdXJyZW50IHBvc2l0aW9uIGlzIGFscmVhZHkgbWlkLXdvcmQsXG4gIC8vIHdhbGsgYmFja3dhcmQgdG8gdGhlIGxhc3Qgc3BhY2UgYmVmb3JlIGl0XG4gIGxldCBpID0gcmF3T2Zmc2V0O1xuICB3aGlsZSAoaSA+IDAgJiYgIS9cXHMvLnRlc3QodGV4dFtpIC0gMV0pKSBpLS07XG4gIHJldHVybiBpID4gMCA/IGkgOiByYXdPZmZzZXQ7XG59XG5cbi8qKlxuICogU3RydWN0dXJlLWF3YXJlIGNodW5raW5nOlxuICogIDEuIFNwbGl0IG9uIGJsYW5rIGxpbmVzIChcXG5cXG4pIGludG8gcGFyYWdyYXBocy5cbiAqICAyLiBBIGxpbmUgbWF0Y2hpbmcgSEVBRElOR19SRSBhbHdheXMgc3RhcnRzIGEgZnJlc2ggY2h1bmsuXG4gKiAgMy4gQWNjdW11bGF0ZSBwYXJhZ3JhcGhzIHVudGlsIHRoZSBzb2Z0IFRBUkdFVCBpcyByZWFjaGVkLCB0aGVuIGZsdXNoLlxuICogIDQuIFBhcmFncmFwaHMgbGFyZ2VyIHRoYW4gTUFYIGFyZSBzcGxpdCB3aXRoIGEgc2xpZGluZyB3aW5kb3cgKyBvdmVybGFwIGFzIGZhbGxiYWNrLlxuICogICAgIEJvdGggd2luZG93IGVuZHMgQU5EIHdpbmRvdyBzdGFydHMgYXJlIHNuYXBwZWQgdG8gc2VudGVuY2Uvd29yZCBib3VuZGFyaWVzXG4gKiAgICAgc28gbm8gY2h1bmsgYmVnaW5zIG9yIGVuZHMgbWlkLXdvcmQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1RleHQodGV4dCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRhcmdldFRva2VucyA9IG9wdGlvbnMuY2h1bmtTaXplVG9rZW5zIHx8IFRBUkdFVF9DSFVOS19UT0tFTlM7XG4gIGNvbnN0IG1heFRva2VucyA9IG9wdGlvbnMubWF4Q2h1bmtUb2tlbnMgfHwgTUFYX0NIVU5LX1RPS0VOUztcbiAgY29uc3Qgb3ZlcmxhcFRrID0gb3B0aW9ucy5vdmVybGFwVG9rZW5zIHx8IE9WRVJMQVBfVE9LRU5TO1xuXG4gIGNvbnN0IHRhcmdldENoYXJzID0gdGFyZ2V0VG9rZW5zICogQ0hBUlNfUEVSX1RPS0VOO1xuICBjb25zdCBtYXhDaGFycyA9IG1heFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3Qgb3ZlcmxhcENoYXJzID0gb3ZlcmxhcFRrICogQ0hBUlNfUEVSX1RPS0VOO1xuXG4gIGlmICghdGV4dCB8fCB0eXBlb2YgdGV4dCAhPT0gJ3N0cmluZycpIHJldHVybiBbXTtcblxuICAvLyAxLiBTcGxpdCBpbnRvIHBhcmFncmFwaHNcbiAgY29uc3QgcmF3UGFyYXMgPSB0ZXh0XG4gICAgLnNwbGl0KC9cXG57Mix9LylcbiAgICAubWFwKHAgPT4gcC50cmltKCkpXG4gICAgLmZpbHRlcihwID0+IHAubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUyk7XG5cbiAgY29uc3QgY2h1bmtzID0gW107XG4gIGxldCBidWZmZXIgPSAnJztcbiAgbGV0IGJ1ZlN0YXJ0ID0gMDtcbiAgbGV0IGNodW5rSW5kZXggPSAwO1xuICBsZXQgY2hhckN1cnNvciA9IDA7XG5cbiAgY29uc3QgZmx1c2ggPSAoZm9yY2VUZXh0KSA9PiB7XG4gICAgY29uc3QgY29udGVudCA9IChmb3JjZVRleHQgPz8gYnVmZmVyKS50cmltKCk7XG4gICAgaWYgKGNvbnRlbnQubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgY2h1bmtzLnB1c2goe1xuICAgICAgICB0ZXh0OiBjb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2Vucyhjb250ZW50KSxcbiAgICAgICAgY2hhclN0YXJ0OiBidWZTdGFydCxcbiAgICAgICAgY2hhckVuZDogYnVmU3RhcnQgKyBjb250ZW50Lmxlbmd0aCxcbiAgICAgICAgY2h1bmtJbmRleDogY2h1bmtJbmRleCsrXG4gICAgICB9KTtcbiAgICB9XG4gICAgYnVmZmVyID0gJyc7XG4gICAgYnVmU3RhcnQgPSBjaGFyQ3Vyc29yO1xuICB9O1xuXG4gIGZvciAoY29uc3QgcGFyYSBvZiByYXdQYXJhcykge1xuICAgIGNvbnN0IGlzSGVhZGluZyA9IEhFQURJTkdfUkUudGVzdChwYXJhLnNwbGl0KCdcXG4nKVswXSk7XG5cbiAgICAvLyAyLiBIZWFkaW5nIGFsd2F5cyBzdGFydHMgYSBuZXcgY2h1bmtcbiAgICBpZiAoaXNIZWFkaW5nICYmIGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgaWYgKHBhcmEubGVuZ3RoID4gbWF4Q2hhcnMpIHtcbiAgICAgIC8vIDMuIE92ZXJzaXplZCBwYXJhZ3JhcGggLT4gc2xpZGluZy13aW5kb3cgY2hhciBmYWxsYmFja1xuICAgICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwKSBmbHVzaCgpO1xuXG4gICAgICBsZXQgcyA9IDA7XG4gICAgICB3aGlsZSAocyA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgIGxldCBlID0gcyArIHRhcmdldENoYXJzO1xuICAgICAgICBpZiAoZSA8IHBhcmEubGVuZ3RoKSB7XG4gICAgICAgICAgY29uc3Qgc2VhcmNoRnJvbSA9IGUgLSBNYXRoLmZsb29yKHRhcmdldENoYXJzICogMC4yKTtcbiAgICAgICAgICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgICAgICAgICAgY29uc3QgaWR4ID0gcGFyYS5sYXN0SW5kZXhPZihicCwgZSk7XG4gICAgICAgICAgICBpZiAoaWR4ID4gc2VhcmNoRnJvbSkgeyBlID0gaWR4ICsgYnAubGVuZ3RoOyBicmVhazsgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBlID0gTWF0aC5taW4oZSwgcGFyYS5sZW5ndGgpO1xuICAgICAgICBjb25zdCBzbGljZSA9IHBhcmEuc2xpY2UocywgZSkudHJpbSgpO1xuICAgICAgICBpZiAoc2xpY2UubGVuZ3RoID49IE1JTl9DSFVOS19DSEFSUykge1xuICAgICAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgICAgIHRleHQ6IHNsaWNlLFxuICAgICAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoc2xpY2UpLFxuICAgICAgICAgICAgY2hhclN0YXJ0OiBjaGFyQ3Vyc29yICsgcyxcbiAgICAgICAgICAgIGNoYXJFbmQ6IGNoYXJDdXJzb3IgKyBlLFxuICAgICAgICAgICAgY2h1bmtJbmRleDogY2h1bmtJbmRleCsrXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cblxuICAgICAgICBpZiAoZSA+PSBwYXJhLmxlbmd0aCkgYnJlYWs7XG5cbiAgICAgICAgLy8gU25hcCB0aGUgb3ZlcmxhcHBlZCBzdGFydCBmb3J3YXJkIHRvIGEgY2xlYW4gc2VudGVuY2Uvd29yZFxuICAgICAgICAvLyBib3VuZGFyeSBpbnN0ZWFkIG9mIHVzaW5nIHRoZSByYXcgb2Zmc2V0LCB3aGljaCBjb3VsZCBsYW5kXG4gICAgICAgIC8vIG1pZC13b3JkIChlLmcuIFwicyB0aGF0IGFuIEVURi4uLlwiKS5cbiAgICAgICAgY29uc3QgcmF3TmV4dCA9IGUgLSBvdmVybGFwQ2hhcnM7XG4gICAgICAgIHMgPSByYXdOZXh0ID4gcyA/IHNuYXBUb0JvdW5kYXJ5KHBhcmEsIHJhd05leHQsIGUpIDogZTtcbiAgICAgIH1cbiAgICAgIGNoYXJDdXJzb3IgKz0gcGFyYS5sZW5ndGggKyAyO1xuICAgICAgYnVmU3RhcnQgPSBjaGFyQ3Vyc29yO1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgLy8gNC4gTm9ybWFsIHBhcmFncmFwaCBcdTIwMTQgaGFyZCBjYXAgbG9va2FoZWFkIEJFRk9SRSBhY2N1bXVsYXRpbmdcbiAgICBpZiAoYnVmZmVyLmxlbmd0aCA+IDAgJiYgKGJ1ZmZlci5sZW5ndGggKyBwYXJhLmxlbmd0aCArIDIpID4gbWF4Q2hhcnMpIHtcbiAgICAgIGZsdXNoKCk7XG4gICAgfVxuXG4gICAgYnVmZmVyID0gYnVmZmVyID8gYnVmZmVyICsgJ1xcblxcbicgKyBwYXJhIDogcGFyYTtcbiAgICBjaGFyQ3Vyc29yICs9IHBhcmEubGVuZ3RoICsgMjtcblxuICAgIC8vIFNvZnQgY2FwOiBmbHVzaCBvbmNlIHRhcmdldCBpcyByZWFjaGVkXG4gICAgaWYgKGJ1ZmZlci5sZW5ndGggPj0gdGFyZ2V0Q2hhcnMpIHtcbiAgICAgIGZsdXNoKCk7XG4gICAgfVxuICB9XG5cbiAgLy8gNS4gRmx1c2ggcmVtYWluZGVyXG4gIGZsdXNoKCk7XG5cbiAgcmV0dXJuIGNodW5rcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNodW5rUERGQ29udGVudChwZGZEYXRhLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgeyBmaWxlbmFtZSwgZG9jdW1lbnRJZCwgcGFnZU51bWJlciwgdGV4dCwgdG90YWxQYWdlcyB9ID0gcGRmRGF0YTtcblxuICBpZiAoIXRleHQgfHwgdGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICBjb25zb2xlLndhcm4oYFx1MjZBMFx1RkUwRiAgJHtmaWxlbmFtZX0gcGFnZSAke3BhZ2VOdW1iZXJ9OiBleHRyYWN0ZWQgdGV4dCB0b28gc2hvcnQgXHUyMDE0IG1heSBiZSBhIHNjYW5uZWQgcGFnZSwgc2tpcHBpbmdgKTtcbiAgICByZXR1cm4gW107XG4gIH1cblxuICBjb25zdCBjbGVhbmVkVGV4dCA9IGNsZWFuVGV4dCh0ZXh0KTtcbiAgY29uc3QgdGV4dENodW5rcyA9IGNodW5rVGV4dChjbGVhbmVkVGV4dCwgb3B0aW9ucyk7XG4gIGNvbnN0IHRvdGFsQ2h1bmtzID0gdGV4dENodW5rcy5sZW5ndGg7XG4gIGNvbnN0IHNvdXJjZVR5cGUgPSBvcHRpb25zLnNvdXJjZVR5cGUgfHwgJ3BkZic7XG5cbiAgcmV0dXJuIHRleHRDaHVua3MubWFwKGNodW5rID0+IHtcbiAgICBjb25zdCBjaHVua0lkID0gZ2VuZXJhdGVDaHVua0lkKGNodW5rLnRleHQsIGZpbGVuYW1lKTtcbiAgICByZXR1cm4ge1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZSxcbiAgICAgICAgY2h1bmtfaWQ6IGNodW5rSWQsXG4gICAgICAgIGNodW5rX2luZGV4OiBjaHVuay5jaHVua0luZGV4LFxuICAgICAgICB0b3RhbF9jaHVua3M6IHRvdGFsQ2h1bmtzLFxuICAgICAgICBwYWdlX251bWJlcjogcGFnZU51bWJlciB8fCAxLFxuICAgICAgICB0b3RhbF9wYWdlczogdG90YWxQYWdlcyB8fCBudWxsLFxuICAgICAgICBzZWN0aW9uX3RpdGxlOiBleHRyYWN0U2VjdGlvblRpdGxlKGNodW5rLnRleHQpLFxuICAgICAgICBzb3VyY2VfdHlwZTogc291cmNlVHlwZSxcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH07XG4gIH0pO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0U2VjdGlvblRpdGxlKHRleHQpIHtcbiAgY29uc3QgbGluZXMgPSB0ZXh0LnNwbGl0KCdcXG4nKS5maWx0ZXIobCA9PiBsLnRyaW0oKSk7XG4gIGlmIChsaW5lcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgZmlyc3RMaW5lID0gbGluZXNbMF0udHJpbSgpO1xuICAgIGlmIChmaXJzdExpbmUubGVuZ3RoIDwgMTAwICYmICFmaXJzdExpbmUuZW5kc1dpdGgoJy4nKSkge1xuICAgICAgcmV0dXJuIGZpcnN0TGluZS5zbGljZSgwLCA1MCk7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IEVtYmVkZGluZ0Vycm9yLCBpczQyOUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAxLiBTTElESU5HIFdJTkRPVyBSQVRFIExJTUlURVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY2xhc3MgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyIHtcbiAgY29uc3RydWN0b3IobGltaXRQZXJNaW51dGUpIHtcbiAgICB0aGlzLmxpbWl0UGVyTWludXRlID0gbGltaXRQZXJNaW51dGU7XG4gICAgdGhpcy53aW5kb3dNcyA9IDYwMDAwO1xuICAgIHRoaXMucmVxdWVzdHMgPSBbXTtcbiAgfVxuXG4gIGFzeW5jIGNvbnN1bWUodG9rZW5zKSB7XG4gICAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgICAvLyBSZW1vdmUgZW50cmllcyBvbGRlciB0aGFuIDYwIHNlY29uZHNcbiAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBub3cgLSB0aGlzLndpbmRvd01zKTtcblxuICAgIGNvbnN0IGN1cnJlbnRUb3RhbCA9IHRoaXMucmVxdWVzdHMucmVkdWNlKChzdW0sIHJlcSkgPT4gc3VtICsgcmVxLnRva2VucywgMCk7XG5cbiAgICAvLyBJZiB3ZSBoYXZlIHJvb20sIGNvbnN1bWUgaW5zdGFudGx5IChidXJzdClcbiAgICBpZiAoY3VycmVudFRvdGFsICsgdG9rZW5zIDw9IHRoaXMubGltaXRQZXJNaW51dGUpIHtcbiAgICAgIHRoaXMucmVxdWVzdHMucHVzaCh7IHRpbWVzdGFtcDogbm93LCB0b2tlbnMgfSk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gT3RoZXJ3aXNlLCB3YWl0IHVudGlsIHRoZSBvbGRlc3QgcmVxdWVzdCBleHBpcmVzIChwbHVzIGEgc21hbGwgYnVmZmVyKVxuICAgIGNvbnN0IG5lZWRlZCA9IHRva2VucyAtICh0aGlzLmxpbWl0UGVyTWludXRlIC0gY3VycmVudFRvdGFsKTtcbiAgICBsZXQgYWNjdW11bGF0ZWRFeHBpcmVkID0gMDtcbiAgICBsZXQgd2FpdFVudGlsID0gbm93ICsgdGhpcy53aW5kb3dNczsgLy8gZmFsbGJhY2tcblxuICAgIGNvbnN0IHNvcnRlZCA9IFsuLi50aGlzLnJlcXVlc3RzXS5zb3J0KChhLCBiKSA9PiBhLnRpbWVzdGFtcCAtIGIudGltZXN0YW1wKTtcbiAgICBmb3IgKGNvbnN0IHJlcSBvZiBzb3J0ZWQpIHtcbiAgICAgIGFjY3VtdWxhdGVkRXhwaXJlZCArPSByZXEudG9rZW5zO1xuICAgICAgaWYgKGFjY3VtdWxhdGVkRXhwaXJlZCA+PSBuZWVkZWQpIHtcbiAgICAgICAgLy8gKzEwbXMgYnVmZmVyIHRvIHNsaWRlIHRoZSB3aW5kb3cgY2xlYW5seVxuICAgICAgICB3YWl0VW50aWwgPSByZXEudGltZXN0YW1wICsgdGhpcy53aW5kb3dNcyArIDEwO1xuICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBkZWxheSA9IHdhaXRVbnRpbCAtIG5vdztcbiAgICBpZiAoZGVsYXkgPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgYFtyYXRlLWxpbWl0XSBXaW5kb3cgZnVsbCAoJHtjdXJyZW50VG90YWx9LyR7dGhpcy5saW1pdFBlck1pbnV0ZX0pLiBgICtcbiAgICAgICAgYFdhaXRpbmcgJHsoZGVsYXkgLyAxMDAwKS50b0ZpeGVkKDEpfXMgdG8gc2VuZCAke3Rva2Vuc30gdG9rZW5zLi4uYFxuICAgICAgKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCBkZWxheSkpO1xuICAgIH1cblxuICAgIC8vIFJlY29yZCB0aGUgY29uc3VtcHRpb24gYXQgdGhlIG5ldyB0aW1lXG4gICAgdGhpcy5yZXF1ZXN0cy5wdXNoKHsgdGltZXN0YW1wOiBEYXRlLm5vdygpLCB0b2tlbnMgfSk7XG4gICAgLy8gQ2xlYW51cCBhZ2FpbiBqdXN0IGluIGNhc2VcbiAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBEYXRlLm5vdygpIC0gdGhpcy53aW5kb3dNcyk7XG4gIH1cbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAyLiBDT05GSUdVUkFUSU9OXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnN0IFRQTV9MSU1JVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfVFBNX0xJTUlUKSB8fCA1MDAwMDA7XG5jb25zdCBSQVRFX0xJTUlURVIgPSBuZXcgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyKFRQTV9MSU1JVCk7XG5cbi8vIEJBVENIX1NJWkU6IG51bWJlciBvZiBjaHVua3MgcGVyIGVtYmVkQ29udGVudCBjYWxsXG4vLyAoa2VwdCBhdCAxMDsgbm90ZSB0aGUgcmVhbCBjZWlsaW5nIGlzIHRoZSBBUEkncyB+MTAwLXJlcXVlc3RzLXBlci1jYWxsIGxpbWl0LFxuLy8gbm90IGEgXCJjb250ZXh0IHdpbmRvd1wiIGxpbWl0IFx1MjAxNCAxMCBqdXN0IGtlZXBzIGJhdGNoZXMgc21hbGwgYW5kIHJldHJ5LWZyaWVuZGx5KVxuY29uc3QgQkFUQ0hfU0laRSA9ICgpID0+IDEwOyAgIC8vIDEwIGNodW5rcyBcdTAwRDcgNzUwIHRva2VucyA9IDcsNTAwIHRva2VucyBwZXIgQVBJIHJlcXVlc3RcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gMTA7IC8vIFNlbmQgMTAgYmF0Y2hlcyBjb25jdXJyZW50bHkgdG8gY2xlYXIgdGhlIGJ1cnN0IGZhc3RcblxuLy8gUmV0cnkgY29uZmlndXJhdGlvbiAoZXhwb25lbnRpYWwgYmFja29mZiArIGppdHRlcilcbmNvbnN0IFJFVFJZX0JBU0VfREVMQVlfTVMgPSAyMDAwOyAgIC8vIDIgc2Vjb25kc1xuY29uc3QgUkVUUllfTUFYX0RFTEFZX01TID0gNjAwMDA7ICAgLy8gNjAgc2Vjb25kcyBjYXBcbmNvbnN0IE1BWF9SRVRSWV9BVFRFTVBUUyA9IDU7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMy4gQUkgQ0xJRU5UIChzaW5nbGUsIHJldXNhYmxlIGluc3RhbmNlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zdCBhaSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gIHZlcnRleGFpOiB0cnVlLFxuICBwcm9qZWN0OiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCBwcm9jZXNzLmVudi5HQ1BfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJyxcbiAgbG9jYXRpb246IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9MT0NBVElPTiB8fCAndXMtY2VudHJhbDEnXG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0LiBUT0tFTiBDQUxDVUxBVElPTiAodXNlcyBzdG9yZWQgdG9rZW5fY291bnQgaWYgYXZhaWxhYmxlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5mdW5jdGlvbiBnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKGNodW5rcykge1xuICByZXR1cm4gY2h1bmtzLnJlZHVjZSgoc3VtLCBjaHVuaykgPT4ge1xuICAgIC8vIFByZWZlciB0aGUgZXhhY3QgdG9rZW4gY291bnQgZnJvbSBjaHVua2VyLCBvdGhlcndpc2UgZmFsbGJhY2sgdG8gcm91Z2ggZXN0aW1hdGVcbiAgICBjb25zdCB0b2tlbkNvdW50ID0gY2h1bmsubWV0YWRhdGE/LnRva2VuX2NvdW50IHx8IE1hdGguY2VpbChjaHVuay50ZXh0Lmxlbmd0aCAvIDQpO1xuICAgIHJldHVybiBzdW0gKyB0b2tlbkNvdW50O1xuICB9LCAwKTtcbn1cblxuLy8gU2FtZSByb3VnaCBlc3RpbWF0ZSBhcyBhYm92ZSwgYnV0IGZvciByYXcgc3RyaW5ncyB0aGF0IGRvbid0IGNhcnJ5IGNodW5rIG1ldGFkYXRhXG4vLyAodXNlZCBmb3IgcmV0cmllcyBpbnNpZGUgZW1iZWRCYXRjaCwgYW5kIGZvciBlbWJlZFF1ZXJ5KS5cbmZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpIHtcbiAgcmV0dXJuIHRleHRzLnJlZHVjZSgoc3VtLCB0ZXh0KSA9PiBzdW0gKyBNYXRoLmNlaWwoU3RyaW5nKHRleHQpLmxlbmd0aCAvIDQpLCAwKTtcbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA1LiBFTUJFRCBCQVRDSCAod2l0aCBleHBvbmVudGlhbCBiYWNrb2ZmICsgaml0dGVyKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hc3luYyBmdW5jdGlvbiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBhdHRlbXB0ID0gMSkge1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG4gIGNvbnN0IG91dHB1dERpbWVuc2lvbmFsaXR5ID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19ESU1FTlNJT05TKSB8fCAzMDcyO1xuXG4gIHRyeSB7XG4gICAgLy8gRklYOiBgYWkuYmF0Y2hlcy5jcmVhdGVFbWJlZGRpbmdzYCBpcyBub3QgYSByZWFsIG1ldGhvZCBvbiB0aGUgQGdvb2dsZS9nZW5haSBTREsuXG4gICAgLy8gYGFpLmJhdGNoZXNgIGlzIGZvciBhc3luYyBiYXRjaC1wcmVkaWN0aW9uIGpvYnMuIFN5bmNocm9ub3VzIGVtYmVkZGluZyBjYWxscyBnb1xuICAgIC8vIHRocm91Z2ggYGFpLm1vZGVscy5lbWJlZENvbnRlbnRgLCB3aXRoIG9uZSBzaGFyZWQgdGFza1R5cGUvb3V0cHV0RGltZW5zaW9uYWxpdHlcbiAgICAvLyBjb25maWcgYXBwbGllZCBhY3Jvc3MgYWxsIGBjb250ZW50c2AgaW4gdGhlIGNhbGwuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCBhaS5tb2RlbHMuZW1iZWRDb250ZW50KHtcbiAgICAgIG1vZGVsOiBtb2RlbE5hbWUsXG4gICAgICBjb250ZW50czogdGV4dHMubWFwKHRleHQgPT4gKHR5cGVvZiB0ZXh0ID09PSAnc3RyaW5nJyA/IHRleHQgOiBTdHJpbmcodGV4dCkpKSxcbiAgICAgIGNvbmZpZzoge1xuICAgICAgICB0YXNrVHlwZTogdGFza1R5cGUsXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBvdXRwdXREaW1lbnNpb25hbGl0eVxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IHJlc3BvbnNlPy5lbWJlZGRpbmdzPy5tYXAoZSA9PiBlLnZhbHVlcykgfHwgW107XG4gICAgaWYgKGVtYmVkZGluZ3MubGVuZ3RoICE9PSB0ZXh0cy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihgRXhwZWN0ZWQgJHt0ZXh0cy5sZW5ndGh9IGVtYmVkZGluZ3MsIGdvdCAke2VtYmVkZGluZ3MubGVuZ3RofWApO1xuICAgIH1cbiAgICByZXR1cm4gZW1iZWRkaW5ncztcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzUmV0cnlhYmxlID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAyIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDMgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnU2VydmljZSBVbmF2YWlsYWJsZScpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ0JhZCBHYXRld2F5Jyk7XG5cbiAgICBpZiAoaXNSZXRyeWFibGUgJiYgYXR0ZW1wdCA8IE1BWF9SRVRSWV9BVFRFTVBUUykge1xuICAgICAgLy8gRXhwb25lbnRpYWwgYmFja29mZjogMl5hdHRlbXB0ICogYmFzZSAoY2FwcGVkKVxuICAgICAgbGV0IGRlbGF5ID0gTWF0aC5taW4oUkVUUllfTUFYX0RFTEFZX01TLCBSRVRSWV9CQVNFX0RFTEFZX01TICogTWF0aC5wb3coMiwgYXR0ZW1wdCAtIDEpKTtcbiAgICAgIC8vIEFkZCBqaXR0ZXIgKDAuOFx1MjAxMzEuMngpIHRvIGF2b2lkIHRodW5kZXJpbmcgaGVyZFxuICAgICAgY29uc3Qgaml0dGVyID0gMC44ICsgKDAuNCAqIE1hdGgucmFuZG9tKCkpO1xuICAgICAgZGVsYXkgPSBNYXRoLmZsb29yKGRlbGF5ICogaml0dGVyKTtcbiAgICAgIC8vIFJlc3BlY3QgcmV0cnktYWZ0ZXIgaGVhZGVyIGlmIHByZXNlbnRcbiAgICAgIGlmIChlcnJvci5yZXRyeUFmdGVyKSB7XG4gICAgICAgIGRlbGF5ID0gTWF0aC5tYXgoZGVsYXksIGVycm9yLnJldHJ5QWZ0ZXIgKiAxMDAwKTtcbiAgICAgIH1cblxuICAgICAgY29uc29sZS5sb2coXG4gICAgICAgIGBbZW1iZWRkaW5nXSBcdTIzRjMgUmV0cnlhYmxlIGVycm9yICgke2Vycm9yPy5zdGF0dXMgfHwgJ3Vua25vd24nfSksIGAgK1xuICAgICAgICBgd2FpdGluZyAkeyhkZWxheSAvIDEwMDApLnRvRml4ZWQoMSl9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7TUFYX1JFVFJZX0FUVEVNUFRTfSkuLi5gXG4gICAgICApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIGRlbGF5KSk7XG5cbiAgICAgIC8vIEZJWDogYSByZXRyeSBpcyBhIGJyYW5kIG5ldyBBUEkgY2FsbCBhbmQgY29uc3VtZXMgcmVhbCBxdW90YSwgZXZlbiB0aG91Z2hcbiAgICAgIC8vIHRoZSBvcmlnaW5hbCBjYWxsIGZhaWxlZC4gU2tpcHBpbmcgY29uc3VtcHRpb24gaGVyZSAoYXMgYmVmb3JlKSBsZXQgdGhlIGxvY2FsXG4gICAgICAvLyBsaW1pdGVyIHVuZGVyLXJlcG9ydCBhY3R1YWwgdXNhZ2UgZHVyaW5nIGVycm9yIHN0b3Jtcywgd2hpY2ggbWVhbnQgaXQga2VwdFxuICAgICAgLy8gd2F2aW5nIHRocm91Z2ggbmV3IGdyb3VwcyB3aGlsZSByZXRyaWVzIHdlcmUgYWxzbyBoaXR0aW5nIHRoZSBBUEkgXHUyMDE0IG1ha2luZ1xuICAgICAgLy8gNDI5IHN0b3JtcyB3b3JzZSBpbnN0ZWFkIG9mIGJhY2tpbmcgb2ZmIGZyb20gdGhlbS5cbiAgICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpKTtcblxuICAgICAgcmV0dXJuIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlLCBhdHRlbXB0ICsgMSk7XG4gICAgfVxuXG4gICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGVycm9yLm1lc3NhZ2UgfHwgJ0JhdGNoIGVtYmVkZGluZyBmYWlsZWQnKTtcbiAgfVxufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDYuIEVYUE9SVEVEIGdlbmVyYXRlRW1iZWRkaW5ncyAod2l0aCByYXRlIGxpbWl0ZXIgJiBhY2N1cmF0ZSB0b2tlbnMpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZUVtYmVkZGluZ3MoY2h1bmtzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnLCBvblByb2dyZXNzKSB7XG4gIGlmICghY2h1bmtzIHx8IGNodW5rcy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcblxuICBjb25zdCBiYXRjaFNpemUgPSBCQVRDSF9TSVpFKCk7XG4gIGNvbnN0IHBhcmFsbGVsQ2FsbHMgPSBQQVJBTExFTF9DQUxMUygpO1xuXG4gIC8vIEZpeGVkLXNpemUgYXJyYXkgdG8gcHJlc2VydmUgY2hyb25vbG9naWNhbCBvcmRlclxuICBjb25zdCBlbWJlZGRpbmdzID0gbmV3IEFycmF5KGNodW5rcy5sZW5ndGgpO1xuXG4gIC8vIEdyb3VwIGNodW5rcyBpbnRvIGJhdGNoZXMgd2l0aCB0aGVpciBzdGFydGluZyBpbmRleFxuICBjb25zdCBiYXRjaGVzID0gW107XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBiYXRjaFNpemUpIHtcbiAgICBiYXRjaGVzLnB1c2goe1xuICAgICAgY2h1bmtzOiBjaHVua3Muc2xpY2UoaSwgaSArIGJhdGNoU2l6ZSksXG4gICAgICBzdGFydEluZGV4OiBpXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB0b3RhbEdyb3VwcyA9IE1hdGguY2VpbChiYXRjaGVzLmxlbmd0aCAvIHBhcmFsbGVsQ2FsbHMpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gcGFyYWxsZWxDYWxscykge1xuICAgIGNvbnN0IHBhcmFsbGVsQmF0Y2hlcyA9IGJhdGNoZXMuc2xpY2UoaSwgaSArIHBhcmFsbGVsQ2FsbHMpO1xuICAgIGNvbnN0IGdyb3VwTnVtID0gTWF0aC5mbG9vcihpIC8gcGFyYWxsZWxDYWxscykgKyAxO1xuXG4gICAgLy8gQ2FsY3VsYXRlIGV4YWN0IHRva2VucyB1c2luZyBzdG9yZWQgdG9rZW5fY291bnQgKG9yIGZhbGxiYWNrKVxuICAgIGNvbnN0IGFsbENodW5rc0luR3JvdXAgPSBwYXJhbGxlbEJhdGNoZXMuZmxhdE1hcChiID0+IGIuY2h1bmtzKTtcbiAgICBjb25zdCB0b2tlbnNUb0NvbnN1bWUgPSBnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKGFsbENodW5rc0luR3JvdXApO1xuICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKHRva2Vuc1RvQ29uc3VtZSk7XG5cbiAgICBjb25zb2xlLmxvZyhcbiAgICAgIGBbZW1iZWRkaW5nXSBHcm91cCAke2dyb3VwTnVtfS8ke3RvdGFsR3JvdXBzfSBcdTIwMTQgZmlyaW5nICR7cGFyYWxsZWxCYXRjaGVzLmxlbmd0aH0gYmF0Y2hlcyBgICtcbiAgICAgIGBpbiBwYXJhbGxlbCAoJHt0b2tlbnNUb0NvbnN1bWV9IHRva2VucylgXG4gICAgKTtcblxuICAgIGNvbnN0IHJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICBwYXJhbGxlbEJhdGNoZXMubWFwKGIgPT4gZW1iZWRCYXRjaChiLmNodW5rcy5tYXAoYyA9PiBjLnRleHQpLCB0YXNrVHlwZSkpXG4gICAgKTtcblxuICAgIGNvbnN0IGZhaWxlZEJhdGNoZXMgPSBbXTtcbiAgICByZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgIGNvbnN0IGN1cnJlbnRCYXRjaEluZm8gPSBwYXJhbGxlbEJhdGNoZXNbYmF0Y2hJZHhdO1xuICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgIGNvbnN0IHZlY3RvcnMgPSByZXN1bHQudmFsdWU7XG4gICAgICAgIGN1cnJlbnRCYXRjaEluZm8uY2h1bmtzLmZvckVhY2goKGNodW5rLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgIGNvbnN0IGdsb2JhbEluZGV4ID0gY3VycmVudEJhdGNoSW5mby5zdGFydEluZGV4ICsgY2h1bmtJZHg7XG4gICAgICAgICAgZW1iZWRkaW5nc1tnbG9iYWxJbmRleF0gPSB7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua18ke2dsb2JhbEluZGV4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH07XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKGBbZW1iZWRkaW5nXSBCYXRjaCBzdGFydGluZyBhdCBpbmRleCAke2N1cnJlbnRCYXRjaEluZm8uc3RhcnRJbmRleH0gZmFpbGVkOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICBmYWlsZWRCYXRjaGVzLnB1c2goY3VycmVudEJhdGNoSW5mbyk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAob25Qcm9ncmVzcykge1xuICAgICAgb25Qcm9ncmVzcyh7IGN1cnJlbnRfYmF0Y2g6IGdyb3VwTnVtLCB0b3RhbF9iYXRjaGVzOiB0b3RhbEdyb3VwcyB9KTtcbiAgICB9XG5cbiAgICAvLyBSZXRyeSBmYWlsZWQgYmF0Y2hlcyBpbmRpdmlkdWFsbHlcbiAgICBmb3IgKGNvbnN0IGZhaWxlZEJhdGNoIG9mIGZhaWxlZEJhdGNoZXMpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBSZXRyeWluZyBmYWlsZWQgYmF0Y2ggZWxlbWVudHMgc3RhcnRpbmcgYXQgaW5kZXggJHtmYWlsZWRCYXRjaC5zdGFydEluZGV4fS4uLmApO1xuICAgICAgZm9yIChsZXQgY2h1bmtJZHggPSAwOyBjaHVua0lkeCA8IGZhaWxlZEJhdGNoLmNodW5rcy5sZW5ndGg7IGNodW5rSWR4KyspIHtcbiAgICAgICAgY29uc3QgY2h1bmsgPSBmYWlsZWRCYXRjaC5jaHVua3NbY2h1bmtJZHhdO1xuICAgICAgICBjb25zdCBnbG9iYWxJbmRleCA9IGZhaWxlZEJhdGNoLnN0YXJ0SW5kZXggKyBjaHVua0lkeDtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAvLyBGSVg6IHRoaXMgcmV0cnkgaXMgYSBmcmVzaCwgcmVhbCBBUEkgY2FsbCBcdTIwMTQgdHJhY2sgaXRzIHRva2VucyBhZ2FpbnN0XG4gICAgICAgICAgLy8gdGhlIGxpbWl0ZXIgaW5zdGVhZCBvZiBhc3N1bWluZyBpdCB3YXMgXCJhbHJlYWR5IHBhaWQgZm9yXCIuXG4gICAgICAgICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZ2V0VG9rZW5Db3VudEZvckNodW5rcyhbY2h1bmtdKSk7XG4gICAgICAgICAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW2NodW5rLnRleHRdLCB0YXNrVHlwZSk7XG4gICAgICAgICAgZW1iZWRkaW5nc1tnbG9iYWxJbmRleF0gPSB7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua19yZXRyeV8ke2dsb2JhbEluZGV4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbMF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gXHUyNzA1IFJldHJ5IHN1Y2NlZWRlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgZ2xvYmFsSW5kZXh9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFtlbWJlZGRpbmddIFx1Mjc0QyBSZXRyeSBmYWlsZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGdsb2JhbEluZGV4fTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvLyBGSVg6IHBlcm1hbmVudGx5LWZhaWxlZCBjaHVua3MgYXJlIGRyb3BwZWQgaGVyZSwgd2hpY2ggc2hpZnRzIGFycmF5IGluZGljZXNcbiAgLy8gcmVsYXRpdmUgdG8gdGhlIG9yaWdpbmFsIGBjaHVua3NgIGlucHV0LiBUaGlzIGxvZyBtYWtlcyB0aGF0IGxvc3MgdmlzaWJsZVxuICAvLyBpbnN0ZWFkIG9mIHNpbGVudDsgY2FsbGVycyB0aGF0IG5lZWQgdG8ga25vdyBleGFjdGx5IHdoaWNoIGNodW5rcyB3ZXJlIGxvc3RcbiAgLy8gY2FuIGNvbXBhcmUgcmV0dXJuZWQgYGlkYHMgYWdhaW5zdCB0aGVpciBvcmlnaW5hbCBjaHVuayBsaXN0LlxuICBjb25zdCBmYWlsZWRDb3VudCA9IGVtYmVkZGluZ3MuZmlsdGVyKGUgPT4gIWUpLmxlbmd0aDtcbiAgaWYgKGZhaWxlZENvdW50ID4gMCkge1xuICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gJHtmYWlsZWRDb3VudH0vJHtjaHVua3MubGVuZ3RofSBjaHVuayhzKSBwZXJtYW5lbnRseSBmYWlsZWQgdG8gZW1iZWQgYW5kIHdlcmUgZHJvcHBlZC5gKTtcbiAgfVxuXG4gIC8vIEZpbHRlciBvdXQgYW55IGVsZW1lbnRzIHRoYXQgcGVybWFuZW50bHkgZmFpbGVkXG4gIHJldHVybiBlbWJlZGRpbmdzLmZpbHRlcihCb29sZWFuKTtcbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA3LiBFWFBPUlRFRCBlbWJlZFF1ZXJ5XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFF1ZXJ5KHF1ZXJ5KSB7XG4gIC8vIEZJWDogdGhpcyBjYWxsIHdhcyBieXBhc3NpbmcgdGhlIHJhdGUgbGltaXRlciBlbnRpcmVseS4gSWYgaXQgcnVucyBjb25jdXJyZW50bHlcbiAgLy8gd2l0aCBkb2N1bWVudCBpbmdlc3Rpb24gKGUuZy4gYSB1c2VyIHNlYXJjaGVzIHdoaWxlIGEgYmF0Y2ggam9iIGlzIGluIGZsaWdodCksXG4gIC8vIGl0IGNvdWxkIHB1c2ggdG90YWwgdXNhZ2Ugb3ZlciB0aGUgY29uZmlndXJlZCBUUE0gYnVkZ2V0IHVubm90aWNlZC5cbiAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyhbcXVlcnldKSk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtxdWVyeV0sICdSRVRSSUVWQUxfUVVFUlknKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAodGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcpIHtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgJHt0ZXh0cy5sZW5ndGh9IHRleHRzLCB0YXNrVHlwZT0ke3Rhc2tUeXBlfWApO1xuICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSk7XG4gIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSk7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0IGdvdCAke3ZlY3RvcnMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gIHJldHVybiB2ZWN0b3JzO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzXCI7aW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQge1xuICBnZXRHbG9iYWxDb2xsZWN0aW9uLFxuICBnZXRDb2xsZWN0aW9uLFxuICBsaXN0RG9jdW1lbnRzXG59IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5cbmNvbnN0IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTID0gNjA7XG5jb25zdCBzZXNzaW9ucyA9IG5ldyBNYXAoKTtcbmNvbnN0IE1BWF9QREZTX1BFUl9TRVNTSU9OID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04pIHx8IDM7XG5jb25zdCBNQVhfVVBMT0FEX1NJWkVfTUIgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIpIHx8IDU7XG5cbmNvbnN0IHNlZWRlZFNlc3Npb25zID0gbmV3IFNldCgpO1xuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgR2xvYmFsIGRvY3VtZW50cyBjYWNoZSAocG9wdWxhdGVkIG9uY2Ugb24gZmlyc3Qgc2Vzc2lvbiBpbml0KSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmxldCBnbG9iYWxEb2N1bWVudHNDYWNoZSA9IFtdO1xubGV0IGdsb2JhbERhdGFJbml0aWFsaXplZCA9IGZhbHNlO1xuXG5leHBvcnQgZnVuY3Rpb24gZ2V0R2xvYmFsRG9jdW1lbnRzQ2FjaGUoKSB7XG4gIHJldHVybiBnbG9iYWxEb2N1bWVudHNDYWNoZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGlkID0gc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkLFxuICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKSxcbiAgICBsYXN0QWNjZXNzZWQ6IG5ldyBEYXRlKCksXG4gICAgZG9jdW1lbnRzOiBbXSxcbiAgICBkZWxldGVkRG9jdW1lbnRJZHM6IG5ldyBTZXQoKSxcbiAgICB0aW1lb3V0TWludXRlczogREVGQVVMVF9USU1FT1VUX01JTlVURVNcbiAgfTtcbiAgc2Vzc2lvbnMuc2V0KGlkLCBzZXNzaW9uKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gc2Vzc2lvbnMuZ2V0KHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgcmV0dXJuIHNlc3Npb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGlmIChzZXNzaW9uSWQpIHtcbiAgICBjb25zdCBleGlzdGluZyA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICBpZiAoZXhpc3RpbmcpIHJldHVybiBleGlzdGluZztcbiAgICByZXR1cm4gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgQ2hlY2sgaWYgc2Vzc2lvbiBpcyBzZWVkZWQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uU2VlZGVkKHNlc3Npb25JZCkge1xuICByZXR1cm4gc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCk7XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBOb3RpZnkgU1NFIGxpc3RlbmVycyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmZ1bmN0aW9uIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpIHtcbiAgaWYgKGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzICYmIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmhhcyhgc2VlZGluZzoke3Nlc3Npb25JZH1gKSkge1xuICAgIGNvbnN0IGV2ZW50S2V5ID0gYHNlZWRpbmc6JHtzZXNzaW9uSWR9YDtcbiAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgIGxpc3RlbmVycy5mb3JFYWNoKChyZXNwb25zZSkgPT4ge1xuICAgICAgdHJ5IHtcbiAgICAgICAgcmVzcG9uc2Uud3JpdGUoYGV2ZW50OiBzZWVkaW5nX2NvbXBsZXRlXFxuZGF0YTogJHtKU09OLnN0cmluZ2lmeSh7IHNlc3Npb25JZCwgc2VlZGVkOiB0cnVlIH0pfVxcblxcbmApO1xuICAgICAgICByZXNwb25zZS5lbmQoKTtcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBjb25zb2xlLmVycm9yKGBbbm90aWZ5XSBGYWlsZWQgdG8gbm90aWZ5IGxpc3RlbmVyOmAsIGVyci5tZXNzYWdlKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5kZWxldGUoZXZlbnRLZXkpO1xuICAgIGNvbnNvbGUubG9nKGBbbm90aWZ5XSBOb3RpZmllZCAke2xpc3RlbmVycy5sZW5ndGh9IFNTRSBsaXN0ZW5lcnMgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gIH1cbn1cblxuLyoqXG4gKiBPbiBzZXNzaW9uIHN0YXJ0OlxuICogLSBSZWNvbnN0cnVjdCBpbi1tZW1vcnkgc2Vzc2lvbiBkb2MgbGlzdCBmcm9tIHRoZSBzaW5nbGUgY29sbGVjdGlvblxuICogICBieSBmaWx0ZXJpbmcgb24gc2Vzc2lvbl9pZCBtZXRhZGF0YS5cbiAqIC0gTm8gdmVjdG9yIGNvcHlpbmcgaXMgcGVyZm9ybWVkIFx1MjAxNCBnbG9iYWwgZG9jcyBhcmUgc2VydmVkIGZyb20gY2FjaGUuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhgXHVEODNEXHVERDExIFNlc3Npb24gaW5pdDogJHtzZXNzaW9uSWR9YCk7XG4gIGlmIChzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKSkge1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWxyZWFkeSBzZWVkZWQgJHtzZXNzaW9uSWR9LCBza2lwcGluZ2ApO1xuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcblxuICAgIC8vIFx1MjUwMFx1MjUwMCBMYXp5IG9uZS10aW1lIGdsb2JhbCBjYWNoZSBpbml0IChydW5zIG9uIGZpcnN0IHNlc3Npb24gaW5pdCkgXHUyNTAwXHUyNTAwXG4gICAgaWYgKCFnbG9iYWxEYXRhSW5pdGlhbGl6ZWQpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGdsb2JhbERvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHsgc2Vzc2lvbl9pZDogJ2dsb2JhbCcgfSk7XG4gICAgICAgIGdsb2JhbERvY3VtZW50c0NhY2hlID0gZ2xvYmFsRG9jcy5tYXAoZG9jID0+ICh7XG4gICAgICAgICAgaWQ6IGRvYy5kb2N1bWVudF9pZCxcbiAgICAgICAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgIHBhZ2VDb3VudDogZG9jLnBhZ2VfY291bnQgfHwgbnVsbCxcbiAgICAgICAgICBjaHVua0NvdW50OiBkb2MuY2h1bmtfY291bnQsXG4gICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICB9KSk7XG4gICAgICAgIGdsb2JhbERhdGFJbml0aWFsaXplZCA9IHRydWU7XG4gICAgICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgR2xvYmFsIGRvY3VtZW50cyBjYWNoZSBsb2FkZWQ6ICR7Z2xvYmFsRG9jdW1lbnRzQ2FjaGUubGVuZ3RofSBkb2N1bWVudChzKWApO1xuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoJ1x1Mjc0QyBGYWlsZWQgdG8gaW5pdGlhbGl6ZSBnbG9iYWwgZGF0YTonLCBlcnIubWVzc2FnZSk7XG4gICAgICB9XG4gICAgfVxuICAgIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgICAvLyBSZWNvbnN0cnVjdCBzZXNzaW9uLXNwZWNpZmljIGRvY3MgKHVzZXIgdXBsb2FkcykgZnJvbSB0aGUgY29sbGVjdGlvblxuICAgIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc3QgZG9jcyA9IGF3YWl0IGxpc3REb2N1bWVudHMoY29sbGVjdGlvbiwgeyBzZXNzaW9uX2lkOiBzZXNzaW9uSWQgfSk7XG4gICAgICBkb2NzLmZvckVhY2goZG9jID0+IHtcbiAgICAgICAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgICAgICAgaWQ6IGRvYy5kb2N1bWVudF9pZCxcbiAgICAgICAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgIHBhZ2VDb3VudDogZG9jLnBhZ2VfY291bnQgfHwgbnVsbCxcbiAgICAgICAgICBjaHVua0NvdW50OiBkb2MuY2h1bmtfY291bnQsXG4gICAgICAgICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IGRvYy51cGxvYWRfdGltZXN0YW1wXG4gICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgICBpZiAoZG9jcy5sZW5ndGggPiAwKSB7XG4gICAgICAgIGNvbnNvbGUubG9nKGBcdTI2N0JcdUZFMEYgIFJlY29uc3RydWN0ZWQgJHtkb2NzLmxlbmd0aH0gc2Vzc2lvbiBkb2N1bWVudChzKSBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICB9XG4gICAgfVxuICAgIHNlZWRlZFNlc3Npb25zLmFkZChzZXNzaW9uSWQpO1xuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiAke3Nlc3Npb25JZH0gcmVhZHkgKG5vIHZlY3RvciBjb3B5aW5nIG5lZWRlZClgKTtcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYFx1Mjc0QyBGYWlsZWQgdG8gaW5pdCBzZXNzaW9uICR7c2Vzc2lvbklkfTpgLCBlcnJvci5tZXNzYWdlKTtcbiAgICAvLyBTdGlsbCBub3RpZnkgbGlzdGVuZXJzIHNvIHRoZXkgZG9uJ3QgaGFuZyBmb3JldmVyXG4gICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG4gIH1cbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIERvY3VtZW50IG1hbmFnZW1lbnQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG5cbiAgY29uc3QgZXhpc3RpbmcgPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kKGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJbmZvLmlkKTtcblxuICBpZiAoZXhpc3RpbmcpIHtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuY2h1bmtDb3VudCA9IGRvY3VtZW50SW5mby5jaHVua0NvdW50O1xuICAgIGlmIChkb2N1bWVudEluZm8ucGFnZUNvdW50ICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnBhZ2VDb3VudCA9IGRvY3VtZW50SW5mby5wYWdlQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlU2l6ZSAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlU2l6ZSA9IGRvY3VtZW50SW5mby5maWxlU2l6ZTtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnN0YXR1cyAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5zdGF0dXMgPSBkb2N1bWVudEluZm8uc3RhdHVzO1xuICAgIGlmIChkb2N1bWVudEluZm8uZmlsZW5hbWUgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuZmlsZW5hbWUgPSBkb2N1bWVudEluZm8uZmlsZW5hbWU7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gVXBkYXRlZCBkb2MgJHtkb2N1bWVudEluZm8uaWR9IFx1MjAxNCBzdGF0dXM9JHtleGlzdGluZy5zdGF0dXN9LCBjaHVua3M9JHtleGlzdGluZy5jaHVua0NvdW50fWApO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgc2Vzc2lvbi5kb2N1bWVudHMucHVzaCh7XG4gICAgaWQ6IGRvY3VtZW50SW5mby5pZCxcbiAgICBmaWxlbmFtZTogZG9jdW1lbnRJbmZvLmZpbGVuYW1lLFxuICAgIGZpbGVTaXplOiBkb2N1bWVudEluZm8uZmlsZVNpemUsXG4gICAgcGFnZUNvdW50OiBkb2N1bWVudEluZm8ucGFnZUNvdW50LFxuICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICBjaHVua0NvdW50OiBkb2N1bWVudEluZm8uY2h1bmtDb3VudCA/PyAwLFxuICAgIHNvdXJjZVR5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgc3RhdHVzOiBkb2N1bWVudEluZm8uc3RhdHVzID8/ICdpbmRleGluZydcbiAgfSk7XG4gIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBBZGRlZCBkb2MgJHtkb2N1bWVudEluZm8uaWR9IFx1MjAxNCBzdGF0dXM9JHtkb2N1bWVudEluZm8uc3RhdHVzID8/ICdpbmRleGluZyd9YCk7XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2FuQWNjZXB0VXBsb2FkKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogJ1Nlc3Npb24gbm90IGZvdW5kJyB9O1xuICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGg7XG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiBgTWF4aW11bSAke01BWF9QREZTX1BFUl9TRVNTSU9OfSBQREZzIHBlciBzZXNzaW9uYCB9O1xuICB9XG4gIHJldHVybiB7IGNhblVwbG9hZDogdHJ1ZSB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVVcGxvYWQoc2Vzc2lvbklkLCBmaWxlLCBmaWxlbmFtZSkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBjb25zdCBlcnJvcnMgPSBbXTtcblxuICBpZiAoZmlsZS5zaXplID4gTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBleGNlZWRzICR7TUFYX1VQTE9BRF9TSVpFX01CfU1CIGxpbWl0YCk7XG4gIH1cblxuICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvblxuICAgID8gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGhcbiAgICA6IDA7XG5cbiAgaWYgKHVwbG9hZGVkQ291bnQgPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICBlcnJvcnMucHVzaChgTWF4aW11bSAke01BWF9QREZTX1BFUl9TRVNTSU9OfSBQREZzIHBlciBzZXNzaW9uYCk7XG4gIH1cblxuICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gZmlsZW5hbWUpKSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgXCIke2ZpbGVuYW1lfVwiIGFscmVhZHkgZXhpc3RzIGluIHRoaXMgc2Vzc2lvbmApO1xuICB9XG5cbiAgcmV0dXJuIHtcbiAgICBpc1ZhbGlkOiBlcnJvcnMubGVuZ3RoID09PSAwLFxuICAgIGVycm9ycyxcbiAgICBpc0xhcmdlRmlsZTogZmlsZS5zaXplID4gKE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0ICogMC42KVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG4gIGNvbnN0IGlkeCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmRJbmRleChkID0+IGQuaWQgPT09IGRvY3VtZW50SWQpO1xuICBpZiAoaWR4ID49IDApIHtcbiAgICBzZXNzaW9uLmRvY3VtZW50cy5zcGxpY2UoaWR4LCAxKTtcbiAgICBzZXNzaW9uLmRlbGV0ZWREb2N1bWVudElkcy5hZGQoZG9jdW1lbnRJZCk7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gUmVtb3ZlZCBkb2MgJHtkb2N1bWVudElkfSwgYWRkZWQgdG8gZGVsZXRlZERvY3VtZW50SWRzYCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cbiAgcmV0dXJuIGZhbHNlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0RGVsZXRlZERvY3VtZW50SWRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICByZXR1cm4gc2Vzc2lvbj8uZGVsZXRlZERvY3VtZW50SWRzID8/IG5ldyBTZXQoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25Eb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIFtdO1xuICByZXR1cm4gc2Vzc2lvbi5kb2N1bWVudHM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIHsgc2Vzc2lvbkRvY3VtZW50czogW10sIGdsb2JhbERvY3VtZW50czogW10gfTtcblxuICBjb25zdCBub3JtYWxpemUgPSAoZG9jKSA9PiAoe1xuICAgIGRvY3VtZW50X2lkOiBkb2MuaWQsXG4gICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICBjaHVua19jb3VudDogZG9jLmNodW5rQ291bnQgPz8gMCxcbiAgICBwYWdlX2NvdW50OiBkb2MucGFnZUNvdW50ID8/IDAsXG4gICAgdXBsb2FkX3RpbWVzdGFtcDogZG9jLnVwbG9hZFRpbWVzdGFtcCB8fCBudWxsLFxuICAgIHNvdXJjZV90eXBlOiBkb2Muc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJyA/ICdzZXNzaW9uX3VwbG9hZCcgOiAnc2VlZCcsXG4gICAgZmlsZVNpemU6IGRvYy5maWxlU2l6ZSB8fCBudWxsLFxuICAgIHN0YXR1czogZG9jLnN0YXR1cyA/PyBudWxsXG4gIH0pO1xuXG4gIHJldHVybiB7XG4gICAgc2Vzc2lvbkRvY3VtZW50czogc2Vzc2lvbi5kb2N1bWVudHNcbiAgICAgIC5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpXG4gICAgICAubWFwKG5vcm1hbGl6ZSksXG4gICAgZ2xvYmFsRG9jdW1lbnRzOiBnbG9iYWxEb2N1bWVudHNDYWNoZVxuICAgICAgLm1hcChub3JtYWxpemUpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uU3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIG51bGw7XG4gIHJldHVybiB7XG4gICAgaWQ6IHNlc3Npb24uaWQsXG4gICAgZG9jdW1lbnRDb3VudDogc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoICsgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogc2Vzc2lvbi5jcmVhdGVkQXQsXG4gICAgbGFzdEFjY2Vzc2VkOiBzZXNzaW9uLmxhc3RBY2Nlc3NlZCxcbiAgICB0b3RhbFNpemU6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5maWxlU2l6ZSB8fCAwKSwgMCksXG4gICAgdG90YWxDaHVua3M6IHNlc3Npb24uZG9jdW1lbnRzLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICAgICAgKyBnbG9iYWxEb2N1bWVudHNDYWNoZS5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBtZW1vcnlNYXAgPSBuZXcgTWFwKCk7XG5jb25zdCBERUZBVUxUX01FTU9SWV9XSU5ET1cgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCAxMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIGlmICghbWVtb3J5TWFwLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgbWVtb3J5TWFwLnNldChzZXNzaW9uSWQsIHtcbiAgICAgIHR1cm5zOiBbXSxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKVxuICAgIH0pO1xuICB9XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgbWV0YWRhdGEgPSB7fSkge1xuICBjb25zdCBtZW1vcnkgPSBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBtYXhUdXJucyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcblxuICBjb25zdCB0dXJuID0ge1xuICAgIGlkOiBgdHVybl8ke0RhdGUubm93KCl9XyR7TWF0aC5yYW5kb20oKS50b1N0cmluZygzNikuc3Vic3RyKDIsIDkpfWAsXG4gICAgcm9sZSxcbiAgICBjb250ZW50LFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICAuLi5tZXRhZGF0YVxuICB9O1xuXG4gIG1lbW9yeS50dXJucy5wdXNoKHR1cm4pO1xuXG4gIGlmIChtZW1vcnkudHVybnMubGVuZ3RoID4gbWF4VHVybnMpIHtcbiAgICBtZW1vcnkudHVybnMgPSBtZW1vcnkudHVybnMuc2xpY2UoLW1heFR1cm5zKTtcbiAgfVxuXG4gIHJldHVybiB0dXJuO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5KHNlc3Npb25JZCkge1xuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgbWF4VHVybnMgPSBudWxsKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBsaW1pdCA9IG1heFR1cm5zIHx8IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcbiAgcmV0dXJuIG1lbW9yeS50dXJucy5zbGljZSgtbGltaXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q29udmVyc2F0aW9uQ29udGV4dChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICByZXR1cm4gdHVybnMubWFwKHQgPT4gKHtcbiAgICByb2xlOiB0LnJvbGUsXG4gICAgY29udGVudDogdC5jb250ZW50XG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICBpZiAodHVybnMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+IHtcbiAgICBjb25zdCBwcmVmaXggPSB0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyOicgOiAnQXNzaXN0YW50Oic7XG4gICAgcmV0dXJuIGAke3ByZWZpeH0gJHt0LmNvbnRlbnR9YDtcbiAgfSkuam9pbignXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhck1lbW9yeShzZXNzaW9uSWQpIHtcbiAgbWVtb3J5TWFwLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5U3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIHR1cm5Db3VudDogbWVtb3J5LnR1cm5zLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IG1lbW9yeS5jcmVhdGVkQXQsXG4gICAgbGFzdFR1cm5BdDogbWVtb3J5LnR1cm5zLmxlbmd0aCA+IDAgPyBtZW1vcnkudHVybnNbbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDFdLnRpbWVzdGFtcCA6IG51bGxcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgY2l0YXRpb25zID0gW10sIGNvdmVyYWdlID0gbnVsbCwgYW5zd2VySWQgPSBudWxsKSB7XG4gIHJldHVybiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwge1xuICAgIC4uLihhbnN3ZXJJZCAmJiB7IGlkOiBhbnN3ZXJJZCB9KSxcbiAgICBjaXRhdGlvbnMsXG4gICAgY292ZXJhZ2UsXG4gICAgaGFzQ2l0YXRpb25zOiBjaXRhdGlvbnMubGVuZ3RoID4gMFxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RVc2VyTWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICd1c2VyJykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RBc3Npc3RhbnRNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7ICBpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbiAgaW1wb3J0IG11bHRlciBmcm9tICdtdWx0ZXInO1xuICBpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbiAgaW1wb3J0IGZzIGZyb20gJ2ZzJztcbiAgaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG4gIGltcG9ydCB7IGNyZWF0ZUhhc2ggfSBmcm9tICdjcnlwdG8nO1xuICBpbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG4gIGltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnO1xuICBpbXBvcnQgeyBzYW5pdGl6ZUZpbGVuYW1lIH0gZnJvbSAnLi4vdXRpbHMvc2FuaXRpemUuanMnO1xuICBpbXBvcnQge1xuICAgIENvcnJ1cHRlZFBERkVycm9yLFxuICAgIEludmFsaWRGaWxlVHlwZUVycm9yLFxuICB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG4gIGltcG9ydCB7IGdldENvbGxlY3Rpb24sIGFkZFZlY3RvcnMsIGRlbGV0ZURvY3VtZW50VmVjdG9ycyB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuICBpbXBvcnQgeyBjaHVua1RleHQsIGNsZWFuVGV4dCB9IGZyb20gJy4uL3V0aWxzL2NodW5rZXIuanMnO1xuICBpbXBvcnQgeyBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbiAgaW1wb3J0IHtcbiAgICBnZXRPckNyZWF0ZVNlc3Npb24sXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24sXG4gICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbixcbiAgICBnZXRBbGxEb2N1bWVudHMsXG4gICAgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyxcbiAgICBpc1Nlc3Npb25TZWVkZWRcbiAgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG4gIGltcG9ydCB7IGNsZWFyTWVtb3J5IH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5cbiAgY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbiAgY29uc3QgX19maWxlbmFtZSA9IGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKTtcbiAgY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG4gIGNvbnN0IHVwbG9hZERpciA9ICcvdG1wL3VwbG9hZHMnO1xuICBpZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICAgIGZzLm1rZGlyU3luYyh1cGxvYWREaXIsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xuICB9XG5cbiAgY29uc3Qgc2VlZERpciA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuLi8uLi9zZWVkX2RvY3VtZW50cycpO1xuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBTU0UgZXZlbnQgaGVscGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBmdW5jdGlvbiBzc2VFdmVudChyZXMsIGV2ZW50LCBkYXRhKSB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9XG5cbiAgY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gICAgZGVzdGluYXRpb246IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCB1cGxvYWREaXIpLFxuICAgIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG4gIH0pO1xuXG4gIGNvbnN0IHVwbG9hZCA9IG11bHRlcih7XG4gICAgc3RvcmFnZSxcbiAgICBsaW1pdHM6IHsgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjQgfSxcbiAgICBmaWxlRmlsdGVyOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgICAgfVxuICAgIH1cbiAgfSk7XG5cbiAgZnVuY3Rpb24gY29udGVudERpc3Bvc2l0aW9uKGRpc3BsYXlOYW1lKSB7XG4gICAgY29uc3QgZW5jb2RlZCA9IGVuY29kZVVSSUNvbXBvbmVudChkaXNwbGF5TmFtZSlcbiAgICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgICAgLnJlcGxhY2UoL1xcKC9nLCAnJTI4JylcbiAgICAgIC5yZXBsYWNlKC9cXCkvZywgJyUyOScpO1xuICAgIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xuICB9XG5cbiAgLyoqXG4gICAqIEpvaW4gcGRmLmpzIHRleHQtY29udGVudCBpdGVtcyBpbnRvIGEgc2luZ2xlIHN0cmluZyB1c2luZyBlYWNoIGl0ZW0nc1xuICAgKiB4LXBvc2l0aW9uICh0cmFuc2Zvcm1bNF0pIGFuZCB3aWR0aCB0byBkZWNpZGUgd2hldGhlciBhIHNwYWNlIGJlbG9uZ3NcbiAgICogYmV0d2VlbiB0d28gaXRlbXMsIGluc3RlYWQgb2YgYWx3YXlzIGpvaW5pbmcgd2l0aCBhIHNpbmdsZSBzcGFjZS5cbiAgICpcbiAgICogVGhpcyBhdm9pZHMgdHdvIGNvbW1vbiBhcnRpZmFjdHMgZnJvbSBuYWl2ZSBgLmpvaW4oJyAnKWA6XG4gICAqICAtIHdvcmRzIHNwbGl0IGFjcm9zcyBhZGphY2VudCB0ZXh0IHJ1bnMgZ2V0dGluZyBhIHBoYW50b20gc3BhY2VcbiAgICogICAgaW5zZXJ0ZWQgaW4gdGhlIG1pZGRsZSAoZS5nLiBcIlNhdiBpbmdzXCIpXG4gICAqICAtIGFkamFjZW50IHdvcmRzIHdpdGggbm8gc3BhY2UgaW4gdGhlIFBERidzIGludGVybmFsIHJ1bnMgZ2V0dGluZ1xuICAgKiAgICBnbHVlZCB0b2dldGhlciAoZS5nLiBcInRoZSByZXBvcnRcIiAtPiBcInRoZXJlcG9ydFwiKVxuICAgKlxuICAgKiBFbXB0eS1zdHJpbmcgaXRlbXMgYXJlIHBkZi5qcydzIHNpZ25hbCBmb3IgYSBsaW5lIGJyZWFrLCB3aGljaCB3ZVxuICAgKiBjb252ZXJ0IHRvIGEgbmV3bGluZSBzbyBwYXJhZ3JhcGggc3RydWN0dXJlIGlzbid0IGxvc3QuXG4gICAqL1xuICBmdW5jdGlvbiBqb2luVGV4dEl0ZW1zKGl0ZW1zKSB7XG4gICAgbGV0IG91dCA9ICcnO1xuICAgIGxldCBwcmV2SXRlbSA9IG51bGw7XG5cbiAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgaXRlbXMpIHtcbiAgICAgIGNvbnN0IHN0ciA9IGl0ZW0uc3RyO1xuICAgICAgaWYgKHN0ciA9PT0gdW5kZWZpbmVkKSB7IHByZXZJdGVtID0gaXRlbTsgY29udGludWU7IH1cblxuICAgICAgaWYgKHN0ciA9PT0gJycpIHtcbiAgICAgICAgLy8gcGRmLmpzIGVtaXRzIGVtcHR5IGl0ZW1zIHRvIHNpZ25hbCBsaW5lIGJyZWFrc1xuICAgICAgICBpZiAoIS9cXG4kLy50ZXN0KG91dCkpIG91dCArPSAnXFxuJztcbiAgICAgICAgcHJldkl0ZW0gPSBudWxsO1xuICAgICAgICBjb250aW51ZTtcbiAgICAgIH1cblxuICAgICAgaWYgKHByZXZJdGVtICYmIHByZXZJdGVtLnN0cikge1xuICAgICAgICBjb25zdCBwcmV2RW5kID0gcHJldkl0ZW0udHJhbnNmb3JtWzRdICsgKHByZXZJdGVtLndpZHRoIHx8IDApO1xuICAgICAgICBjb25zdCBjdXJTdGFydCA9IGl0ZW0udHJhbnNmb3JtWzRdO1xuICAgICAgICBjb25zdCBnYXAgPSBjdXJTdGFydCAtIHByZXZFbmQ7XG4gICAgICAgIGNvbnN0IGZvbnRIID0gTWF0aC5hYnMoaXRlbS50cmFuc2Zvcm1bM10pIHx8IDEwO1xuICAgICAgICBjb25zdCBzcGFjZVRocmVzaG9sZCA9IGZvbnRIICogMC4yNTtcblxuICAgICAgICBjb25zdCBhbHJlYWR5U3BhY2VkID0gL1xccyQvLnRlc3Qob3V0KSB8fCAvXlxccy8udGVzdChzdHIpO1xuICAgICAgICBpZiAoIWFscmVhZHlTcGFjZWQgJiYgZ2FwID4gc3BhY2VUaHJlc2hvbGQpIHtcbiAgICAgICAgICBvdXQgKz0gJyAnO1xuICAgICAgICB9XG4gICAgICAgIC8vIGVsc2U6IGl0ZW1zIGFyZSB0b3VjaGluZy9vdmVybGFwcGluZyAtPiBzYW1lIHdvcmQsIG5vIHNwYWNlIGluc2VydGVkXG4gICAgICB9XG5cbiAgICAgIG91dCArPSBzdHI7XG4gICAgICBwcmV2SXRlbSA9IGl0ZW07XG4gICAgfVxuXG4gICAgcmV0dXJuIG91dDtcbiAgfVxuXG4gIGFzeW5jIGZ1bmN0aW9uIHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGVQYXRoKSB7XG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICAgIGNvbnN0IHBhZ2VzID0gW107XG4gICAgICBhd2FpdCBwZGYoYnVmZmVyLCB7XG4gICAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICAgIHJldHVybiBwYWdlRGF0YS5nZXRUZXh0Q29udGVudCgpLnRoZW4odGMgPT4ge1xuICAgICAgICAgICAgY29uc3QgcGFnZVRleHQgPSBqb2luVGV4dEl0ZW1zKHRjLml0ZW1zKTtcbiAgICAgICAgICAgIHBhZ2VzLnB1c2gocGFnZVRleHQpO1xuICAgICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgaWYgKHBhZ2VzLmxlbmd0aCA9PT0gMCB8fCBwYWdlcy5ldmVyeShwID0+ICFwLnRyaW0oKSkpIHtcbiAgICAgICAgY29uc3QgZnVsbCA9IGF3YWl0IHBkZihidWZmZXIpO1xuICAgICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHRvdGFsUGFnZXMgPSBwYWdlcy5sZW5ndGg7XG4gICAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgICAgY29uc3QgcGFnZU1hcCA9IFtdO1xuICAgICAgbGV0IGNoYXJQb3MgPSAwO1xuXG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgICBwYWdlTWFwLnB1c2goeyBwYWdlOiBpICsgMSwgc3RhcnQ6IGNoYXJQb3MsIGVuZDogY2hhclBvcyArIGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggfSk7XG4gICAgICAgIGNoYXJQb3MgKz0gY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCArIDE7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGZ1bGxUZXh0ID0gY2xlYW5lZFBhZ2VzLmpvaW4oJ1xcbicpO1xuICAgICAgcmV0dXJuIHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignUERGIHBhcnNpbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgICAgdGhyb3cgbmV3IENvcnJ1cHRlZFBERkVycm9yKCk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEdpdmVuIGEgY2h1bmsncyBbY2hhclN0YXJ0LCBjaGFyRW5kKSByYW5nZSwgZmluZCB3aGljaCBwYWdlKHMpIGl0XG4gICAqIG92ZXJsYXBzLiBSZXR1cm5zIHRoZSBtYWpvcml0eSBwYWdlIChtb3N0IG92ZXJsYXBwaW5nIGNoYXJzLCB1c2VkXG4gICAqIGZvciBgcGFnZV9udW1iZXJgIGZvciBiYWNrd2FyZCBjb21wYXRpYmlsaXR5KSBwbHVzIHRoZSB0cnVlIHN0YXJ0L2VuZFxuICAgKiBwYWdlcyBzbyBjaHVua3Mgc3Bhbm5pbmcgYSBwYWdlIGJyZWFrIGFyZW4ndCBzaWxlbnRseSBtaXNsYWJlbGVkIHdpdGhcbiAgICoganVzdCB0aGUgZmlyc3QgcGFnZS5cbiAgICovXG4gIGZ1bmN0aW9uIGdldFBhZ2VSYW5nZShjaGFyU3RhcnQsIGNoYXJFbmQsIHBhZ2VNYXApIHtcbiAgICBsZXQgc3RhcnRQYWdlID0gbnVsbDtcbiAgICBsZXQgZW5kUGFnZSA9IG51bGw7XG4gICAgbGV0IGJlc3RQYWdlID0gbnVsbDtcbiAgICBsZXQgbWF4T3ZlcmxhcCA9IC0xO1xuXG4gICAgZm9yIChjb25zdCBlbnRyeSBvZiBwYWdlTWFwKSB7XG4gICAgICBjb25zdCBvdmVybGFwU3RhcnQgPSBNYXRoLm1heChjaGFyU3RhcnQsIGVudHJ5LnN0YXJ0KTtcbiAgICAgIGNvbnN0IG92ZXJsYXBFbmQgPSBNYXRoLm1pbihjaGFyRW5kLCBlbnRyeS5lbmQpO1xuICAgICAgY29uc3Qgb3ZlcmxhcCA9IG92ZXJsYXBFbmQgLSBvdmVybGFwU3RhcnQ7XG4gICAgICBpZiAob3ZlcmxhcCA8PSAwKSBjb250aW51ZTtcblxuICAgICAgaWYgKHN0YXJ0UGFnZSA9PT0gbnVsbCkgc3RhcnRQYWdlID0gZW50cnkucGFnZTtcbiAgICAgIGVuZFBhZ2UgPSBlbnRyeS5wYWdlO1xuXG4gICAgICBpZiAob3ZlcmxhcCA+IG1heE92ZXJsYXApIHtcbiAgICAgICAgbWF4T3ZlcmxhcCA9IG92ZXJsYXA7XG4gICAgICAgIGJlc3RQYWdlID0gZW50cnkucGFnZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoc3RhcnRQYWdlID09PSBudWxsKSB7XG4gICAgICBjb25zdCBsYXN0UGFnZSA9IHBhZ2VNYXBbcGFnZU1hcC5sZW5ndGggLSAxXT8ucGFnZSB8fCAxO1xuICAgICAgcmV0dXJuIHsgcGFnZTogbGFzdFBhZ2UsIHBhZ2VTdGFydDogbGFzdFBhZ2UsIHBhZ2VFbmQ6IGxhc3RQYWdlIH07XG4gICAgfVxuXG4gICAgcmV0dXJuIHsgcGFnZTogYmVzdFBhZ2UsIHBhZ2VTdGFydDogc3RhcnRQYWdlLCBwYWdlRW5kOiBlbmRQYWdlIH07XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgVXBsb2FkIGhhbmRsZXIgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gICAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gICAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gICAgY29uc3QgQkFUQ0hfU0laRSA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19CQVRDSF9NQVhfQ0hVTktTKSB8fCAxMDtcbiAgICBjb25zdCBQQVJBTExFTF9DQUxMUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19QQVJBTExFTF9DQUxMUykgfHwgMTA7XG4gICAgY29uc3QgR1JPVVBfV0FJVF9NUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19HUk9VUF9XQUlUX01TKSB8fCAxO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgICBjb25zdCBtYXhQREZzID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04gfHwgJzMnKTtcbiAgICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgICAgaWYgKHVwbG9hZGVkQ291bnQgPj0gbWF4UERGcykge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgTWF4aW11bSAke21heFBERnN9IHVwbG9hZHMgcmVhY2hlZGAsIGNvZGU6ICdUT09fTUFOWV9QREZTJyB9KTtcbiAgICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICAgIH1cblxuICAgICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgXCIke2NsZWFuRmlsZW5hbWV9XCIgYWxyZWFkeSB1cGxvYWRlZGAsIGNvZGU6ICdEVVBMSUNBVEVfRklMRScgfSk7XG4gICAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAxIFx1MjAxNCBwYXJzaW5nICR7Y2xlYW5GaWxlbmFtZX0gKCR7ZmlsZS5zaXplfSBieXRlcylgKTtcbiAgICAgIGNvbnN0IHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfSA9IGF3YWl0IHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGUucGF0aCk7XG5cbiAgICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBleHRyYWN0YWJsZSB0ZXh0IFx1MjAxNCBQREYgbWF5IGJlIHNjYW5uZWQgb3IgaW1hZ2Utb25seScsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBkb2N1bWVudElkID0gdXVpZHY0KCk7XG4gICAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQpO1xuXG4gICAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgY2h1bmtzID0gcmF3Q2h1bmtzLm1hcCgoY2h1bmssIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCB7IHBhZ2UsIHBhZ2VTdGFydCwgcGFnZUVuZCB9ID0gZ2V0UGFnZVJhbmdlKGNodW5rLmNoYXJTdGFydCwgY2h1bmsuY2hhckVuZCwgcGFnZU1hcCk7XG4gICAgICAgIHJldHVybiB7XG4gICAgICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2lkOiBjcmVhdGVIYXNoKCdtZDUnKS51cGRhdGUoYCR7Y2xlYW5GaWxlbmFtZX06OiR7Y2h1bmsudGV4dH1gKS5kaWdlc3QoJ2hleCcpLnNsaWNlKDAsIDE2KSxcbiAgICAgICAgICAgIGNodW5rX2luZGV4OiBpZHgsXG4gICAgICAgICAgICB0b3RhbF9jaHVua3M6IHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgICAgICBwYWdlX251bWJlcjogcGFnZSwgICAgICAgLy8gbWFqb3JpdHkgcGFnZSBcdTIwMTQga2VwdCBmb3IgYmFja3dhcmQgY29tcGF0aWJpbGl0eVxuICAgICAgICAgICAgcGFnZV9zdGFydDogcGFnZVN0YXJ0LCAgIC8vIG5ldzogZmlyc3QgcGFnZSB0aGlzIGNodW5rIG92ZXJsYXBzXG4gICAgICAgICAgICBwYWdlX2VuZDogcGFnZUVuZCwgICAgICAgLy8gbmV3OiBsYXN0IHBhZ2UgdGhpcyBjaHVuayBvdmVybGFwc1xuICAgICAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMsXG4gICAgICAgICAgICBzb3VyY2VfdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgICAgIHNlc3Npb25faWQ6IHNlc3Npb25JZCxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgICAgIGNoYXJfc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgICAgICB9XG4gICAgICAgIH07XG4gICAgICB9KTtcblxuICAgICAgY29uc3QgdG90YWxDaHVua3MgPSBjaHVua3MubGVuZ3RoO1xuICAgICAgY29uc3QgdG90YWxCYXRjaGVzID0gTWF0aC5jZWlsKHRvdGFsQ2h1bmtzIC8gQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCB0b3RhbFNldHMgPSBNYXRoLmNlaWwodG90YWxCYXRjaGVzIC8gUEFSQUxMRUxfQ0FMTFMpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgICBzc2VFdmVudChyZXMsICd1cGxvYWRfY29tcGxldGUnLCB7XG4gICAgICAgIGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgICAgfSk7XG5cbiAgICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogMCwgc3RhdHVzOiAnaW5kZXhpbmcnXG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0Q29sbGVjdGlvbigpO1xuICAgICAgbGV0IHByb2Nlc3NlZENodW5rcyA9IDA7XG4gICAgICBjb25zdCBhbGxFbWJlZGRpbmdzID0gW107XG5cbiAgICAgIGNvbnN0IGJhdGNoZXMgPSBbXTtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSBiYXRjaGVzLnB1c2goY2h1bmtzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSk7XG5cbiAgICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gUEFSQUxMRUxfQ0FMTFMpIHNldHMucHVzaChiYXRjaGVzLnNsaWNlKGksIGkgKyBQQVJBTExFTF9DQUxMUykpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMiBzdGFydCBcdTIwMTQgJHtzZXRzLmxlbmd0aH0gc2V0c2ApO1xuXG4gICAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgICAgY29uc3QgaXNMYXN0U2V0ID0gc2V0SWR4ID09PSBzZXRzLmxlbmd0aCAtIDE7XG4gICAgICAgIGNvbnN0IGN1cnJlbnRTZXQgPSBzZXRzW3NldElkeF07XG4gICAgICAgIGNvbnN0IHNldENodW5rQ291bnQgPSBjdXJyZW50U2V0LnJlZHVjZSgoYWNjLCBiKSA9PiBhY2MgKyBiLmxlbmd0aCwgMCk7XG5cbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9LyR7c2V0cy5sZW5ndGh9IFx1MjAxNCBlbWJlZGRpbmcgJHtjdXJyZW50U2V0Lmxlbmd0aH0gYmF0Y2ggY2FsbChzKSAoJHtzZXRDaHVua0NvdW50fSBjaHVua3MpIGluIHBhcmFsbGVsYCk7XG5cbiAgICAgICAgY29uc3QgZW1iZWRSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgICAgIGN1cnJlbnRTZXQubWFwKGJhdGNoID0+IGVtYmVkU2luZ2xlQmF0Y2hHcm91cChiYXRjaC5tYXAoYyA9PiBjLnRleHQpKSlcbiAgICAgICAgKTtcblxuICAgICAgICBjb25zdCBzZXRFbWJlZGRpbmdzID0gW107XG4gICAgICAgIGVtYmVkUmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGJhdGNoSWR4KSA9PiB7XG4gICAgICAgICAgY29uc3QgYmF0Y2ggPSBjdXJyZW50U2V0W2JhdGNoSWR4XTtcbiAgICAgICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgICAgIHJlc3VsdC52YWx1ZS5mb3JFYWNoKCh2ZWN0b3IsIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgICAgIHNldEVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICAgICAgaWQ6IGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YS5jaHVua19pZCxcbiAgICAgICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcixcbiAgICAgICAgICAgICAgICBtZXRhZGF0YTogYmF0Y2hbY2h1bmtJZHhdLm1ldGFkYXRhLFxuICAgICAgICAgICAgICAgIHRleHQ6IGJhdGNoW2NodW5rSWR4XS50ZXh0XG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBlbWJlZGRlZCBPSyAoJHtiYXRjaC5sZW5ndGh9IGNodW5rcylgKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcihgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBGQUlMRUQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICBwcm9jZXNzZWRDaHVua3MgKz0gc2V0RW1iZWRkaW5ncy5sZW5ndGg7XG4gICAgICAgIGFsbEVtYmVkZGluZ3MucHVzaCguLi5zZXRFbWJlZGRpbmdzKTtcblxuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0gZW1iZWRkZWQgXHUyMDE0ICR7cHJvY2Vzc2VkQ2h1bmtzfS8ke3RvdGFsQ2h1bmtzfSBjaHVua3Mgc28gZmFyYCk7XG5cbiAgICAgICAgaWYgKCFpc0xhc3RTZXQpIHtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU3RhcnRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIHRpbWVyICsgQ2hyb21hIHdyaXRlIGNvbmN1cnJlbnRseSBmb3Igc2V0ICR7c2V0SWR4ICsgMX1gKTtcbiAgICAgICAgICBjb25zdCB0aW1lciA9IG5ldyBQcm9taXNlKHIgPT4gc2V0VGltZW91dChyLCBHUk9VUF9XQUlUX01TKSk7XG4gICAgICAgICAgY29uc3QgY2hyb21hV3JpdGUgPSBhZGRWZWN0b3JzKFxuICAgICAgICAgICAgY29sbGVjdGlvbixcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICAgICAgICApLnRoZW4oKCkgPT4gY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBkb25lIGZvciBzZXQgJHtzZXRJZHggKyAxfSAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKSlcbiAgICAgICAgICAgIC5jYXRjaChlcnIgPT4gY29uc29sZS5lcnJvcihgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIEZBSUxFRCBmb3Igc2V0ICR7c2V0SWR4ICsgMX06YCwgZXJyLm1lc3NhZ2UpKTtcblxuICAgICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgICAgd2FpdGluZ01zOiBHUk9VUF9XQUlUX01TLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiBmYWxzZVxuICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgYXdhaXQgUHJvbWlzZS5hbGwoW3RpbWVyLCBjaHJvbWFXcml0ZV0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBUaW1lciArIENocm9tYSBib3RoIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9LCBwcm9jZWVkaW5nIHRvIHNldCAke3NldElkeCArIDJ9YCk7XG5cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gTGFzdCBzZXQgJHtzZXRJZHggKyAxfSBcdTIwMTQgYXdhaXRpbmcgQ2hyb21hIHdyaXRlIGRpcmVjdGx5YCk7XG4gICAgICAgICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICAgKTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGNvbXBsZXRlIGZvciBsYXN0IHNldCAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKTtcblxuICAgICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgICAgd2FpdGluZ01zOiAwLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiB0cnVlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCwgc3RhdHVzOiAncmVhZHknXG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFx1MjcwNSBEb25lIFx1MjAxNCAke2FsbEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzIGluIENocm9tYSBmb3IgJHtjbGVhbkZpbGVuYW1lfWApO1xuXG4gICAgICBzc2VFdmVudChyZXMsICdkb25lJywge1xuICAgICAgICBkb2N1bWVudDoge1xuICAgICAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICAgIH0sXG4gICAgICAgIHNlc3Npb25JZFxuICAgICAgfSk7XG5cbiAgICAgIHJlcy5lbmQoKTtcblxuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgICB0cnkgeyBmcy51bmxpbmtTeW5jKHJlcS5maWxlLnBhdGgpOyB9IGNhdGNoIHsgfVxuICAgICAgfVxuICAgICAgY29uc29sZS5lcnJvcignW3VwbG9hZF0gVW5oYW5kbGVkIGVycm9yOicsIGVycm9yKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdVcGxvYWQgZmFpbGVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJyB9KTtcbiAgICAgIHJlcy5lbmQoKTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgU1NFOiBTZWVkaW5nIHN0YXR1cyBzdHJlYW0gXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBzZWVkaW5nU3RhdHVzSGFuZGxlcihyZXEsIHJlcykge1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgICByZXMuZmx1c2hIZWFkZXJzKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICAgIGlmICghc2Vzc2lvbklkKSB7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ01pc3Npbmcgc2Vzc2lvbiBJRCcsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICAgICAgcmVzLmVuZCgpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIENsaWVudCBjb25uZWN0ZWQgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG5cbiAgICAvLyBDaGVjayBpZiBzZXNzaW9uIGlzIGFscmVhZHkgc2VlZGVkXG4gICAgY29uc3Qgc2VlZGVkID0gaXNTZXNzaW9uU2VlZGVkKHNlc3Npb25JZCk7XG4gICAgaWYgKHNlZWRlZCkge1xuICAgICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gU2Vzc2lvbiAke3Nlc3Npb25JZH0gYWxyZWFkeSBzZWVkZWQgXHUyMDEzIHJldHVybmluZyBpbW1lZGlhdGVseWApO1xuICAgICAgc3NlRXZlbnQocmVzLCAnc2VlZGluZ19jb21wbGV0ZScsIHsgc2Vzc2lvbklkLCBzZWVkZWQ6IHRydWUgfSk7XG4gICAgICByZXMuZW5kKCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gQ3JlYXRlIGEgbGlzdGVuZXIgZm9yIHRoaXMgc2Vzc2lvblxuICAgIGNvbnN0IGV2ZW50S2V5ID0gYHNlZWRpbmc6JHtzZXNzaW9uSWR9YDtcblxuICAgIC8vIFN0b3JlIHRoZSBsaXN0ZW5lciBzbyB3ZSBjYW4gZW1pdCB3aGVuIHNlZWRpbmcgY29tcGxldGVzXG4gICAgaWYgKCFnbG9iYWwuc2VlZGluZ0xpc3RlbmVycykge1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMgPSBuZXcgTWFwKCk7XG4gICAgfVxuICAgIGlmICghZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuaGFzKGV2ZW50S2V5KSkge1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuc2V0KGV2ZW50S2V5LCBbXSk7XG4gICAgfVxuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkucHVzaChyZXMpO1xuXG4gICAgLy8gQ2xlYW4gdXAgbGlzdGVuZXIgb24gY2xpZW50IGRpc2Nvbm5lY3RcbiAgICByZXEub24oJ2Nsb3NlJywgKCkgPT4ge1xuICAgICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICAgIGNvbnN0IGlkeCA9IGxpc3RlbmVycy5pbmRleE9mKHJlcyk7XG4gICAgICBpZiAoaWR4ID49IDApIHtcbiAgICAgICAgbGlzdGVuZXJzLnNwbGljZShpZHgsIDEpO1xuICAgICAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBDbGllbnQgZGlzY29ubmVjdGVkIGZvciAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cbiAgICAgIGlmIChsaXN0ZW5lcnMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICAvLyBTdGFydCBzZWVkaW5nIGluIHRoZSBiYWNrZ3JvdW5kIChpZiBub3QgYWxyZWFkeSBydW5uaW5nKVxuICAgIHRyeSB7XG4gICAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBUcmlnZ2VyaW5nIHNlZWRpbmcgZm9yICR7c2Vzc2lvbklkfS4uLmApO1xuICAgICAgYXdhaXQgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpO1xuICAgICAgLy8gVGhlIHNlZWRpbmcgZnVuY3Rpb24gd2lsbCBub3RpZnkgbGlzdGVuZXJzIHdoZW4gY29tcGxldGVcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoYFtzZWVkaW5nLXN0YXR1c10gU2VlZGluZyBmYWlsZWQgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgICAgbGlzdGVuZXJzLmZvckVhY2goKHJlc3BvbnNlKSA9PiB7XG4gICAgICAgIHNzZUV2ZW50KHJlc3BvbnNlLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGVyci5tZXNzYWdlLCBjb2RlOiAnU0VFRF9GQUlMRUQnIH0pO1xuICAgICAgICByZXNwb25zZS5lbmQoKTtcbiAgICAgIH0pO1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgTGlzdCBkb2N1bWVudHMgaGFuZGxlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG4gICAgdHJ5IHtcbiAgICAgIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgY29uc3QgZG9jdW1lbnRzID0gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gICAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdMaXN0IGRvY3VtZW50cyBlcnJvcjonLCBlcnJvcik7XG4gICAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJywgY29kZTogJ0xJU1RfRVJST1InIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBEZWxldGUgZG9jdW1lbnQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICAgIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgICBjb25zdCBmaWxlbmFtZSA9IHJlcS5xdWVyeS5maWxlbmFtZTtcbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICAgIHRyeSB7XG4gICAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRDb2xsZWN0aW9uKCk7XG4gICAgICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gY2F0Y2ggKGNocm9tYUVycikge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gQ2hyb21hIGRlbGV0ZSBmYWlsZWQgZm9yICR7ZG9jdW1lbnRJZH06YCwgY2hyb21hRXJyLm1lc3NhZ2UpO1xuICAgICAgICB9XG5cbiAgICAgICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpO1xuXG4gICAgICAgIGNsZWFyTWVtb3J5KHNlc3Npb25JZCk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbZGVsZXRlXSBDbGVhcmVkIG1lbW9yeSBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICAgIGNvbnN0IGZpbGVQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhmaWxlUGF0aCkpIHtcbiAgICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGVQYXRoKTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gUmVtb3ZlZCBmaWxlOiAke2ZpbGVQYXRofWApO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gRmlsZSBub3QgZm91bmQgb24gZGlzazogJHtmaWxlUGF0aH1gKTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUsIGRvY3VtZW50SWQgfSk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsIGNvZGU6ICdERUxFVEVfRVJST1InIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBHZXQgZG9jdW1lbnQgZmlsZSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICAgIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuXG4gICAgdHJ5IHtcbiAgICAgIGlmIChmaWxlbmFtZSkge1xuICAgICAgICBjb25zdCB1cGxvYWRQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3Qgc2VlZFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgZmlsZW5hbWUpO1xuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihmaWxlbmFtZSkpO1xuICAgICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cblxuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkRGlyKSkge1xuICAgICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICAgIGNvbnN0IG1hdGNoID0gYWxsUGRmcy5maW5kKGYgPT4gZi5pbmNsdWRlcyhwYXRoLnBhcnNlKGZpbGVuYW1lKS5uYW1lKSk7XG4gICAgICAgICAgaWYgKG1hdGNoKSB7XG4gICAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihtYXRjaCkpO1xuICAgICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgUm91dGVzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICByb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbiAgcm91dGVyLmdldCgnLycsIGxpc3REb2N1bWVudHNIYW5kbGVyKTtcbiAgcm91dGVyLmdldCgnL3NlZWRpbmctc3RhdHVzJywgc2VlZGluZ1N0YXR1c0hhbmRsZXIpO1xuICByb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG4gIHJvdXRlci5nZXQoJy86ZG9jdW1lbnRJZC9maWxlJywgZ2V0RG9jdW1lbnRGaWxlKTtcblxuICBleHBvcnQgZGVmYXVsdCByb3V0ZXI7IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZ2V0Q29sbGVjdGlvbiwgaHlicmlkUXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDIwO1xuY29uc3QgUkVGVVNBTF9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LlJFRlVTQUxfVEhSRVNIT0xEKSB8fCAwLjA1O1xuXG5mdW5jdGlvbiBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLID0gNSkge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwIH07XG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gTWF0aC5tYXgoMCwgci5zY29yZSkpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwIE1haW4gcmV0cmlldmFsIGZ1bmN0aW9uIChIeWJyaWQ6IGRlbnNlICsgQk0yNSB2aWEgQ2hyb21hIFJSRikgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdG9wSyA9IG9wdGlvbnMudG9wSyB8fCA1O1xuXG4gIHRyeSB7XG4gICAgY29uc3QgW3F1ZXJ5RW1iZWRkaW5nLCB7IGNvbGxlY3Rpb24gfV0gPSBhd2FpdCBQcm9taXNlLmFsbChbXG4gICAgICBlbWJlZFF1ZXJ5KHF1ZXJ5KSxcbiAgICAgIGdldENvbGxlY3Rpb24oKVxuICAgIF0pO1xuXG4gICAgaWYgKCFjb2xsZWN0aW9uKSB7XG4gICAgICBjb25zb2xlLndhcm4oYFx1MjZBMFx1RkUwRiAgTm8gY29sbGVjdGlvbiBhdmFpbGFibGVgKTtcbiAgICAgIHJldHVybiB7IHJlc3VsdHM6IFtdLCBjb3ZlcmFnZTogeyBjb25maWRlbmNlOiAwLCB0b3BTY29yZTogMCwgbGV2ZWw6ICdsb3cnLCBzY29yZTogMCB9LCBxdWVyeUVtYmVkZGluZyB9O1xuICAgIH1cblxuICAgIC8vIEJ1aWxkIG1ldGFkYXRhIGZpbHRlcjogaW5jbHVkZSBib3RoICdnbG9iYWwnIHZlY3RvcnMgYW5kIHRoaXMgc2Vzc2lvbidzIHZlY3RvcnNcbiAgICBjb25zdCB3aGVyZSA9IHNlc3Npb25JZFxuICAgICAgPyB7IHNlc3Npb25faWQ6IHsgXCIkaW5cIjogW1wiZ2xvYmFsXCIsIHNlc3Npb25JZF0gfSB9XG4gICAgICA6IHsgc2Vzc2lvbl9pZDogXCJnbG9iYWxcIiB9O1xuXG4gICAgY29uc3QgcmF3UmVzdWx0cyA9IGF3YWl0IGh5YnJpZFF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeSwgcXVlcnlFbWJlZGRpbmcsIHRvcEssIHdoZXJlKTtcblxuICAgIGNvbnN0IHJlc3VsdHMgPSByYXdSZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICAuLi5yLFxuICAgICAgc291cmNlX3R5cGU6IHIubWV0YWRhdGE/LnNvdXJjZV90eXBlIHx8ICdzZXNzaW9uJ1xuICAgIH0pKTtcblxuICAgIGNvbnN0IGNvdmVyYWdlID0gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyk7XG4gICAgY29uc3QgdG9wU2NvcmUgPSBjb3ZlcmFnZS50b3BTY29yZTtcbiAgICBjb25zdCBsZXZlbCA9IHRvcFNjb3JlID49IDAuNiA/ICdoaWdoJyA6IHRvcFNjb3JlID49IDAuMyA/ICdtZWRpdW0nIDogJ2xvdyc7XG5cbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVERDBEIFF1ZXJ5OicsIHF1ZXJ5KTtcbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVEQ0NBIENvdmVyYWdlOicsIHsgLi4uY292ZXJhZ2UsIGxldmVsIH0pO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQzggU2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMsIG1heFRva2VucyA9IDcwMDApIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgbGV0IHRvdGFsVG9rZW5zID0gMDtcbiAgY29uc3QgY29udGV4dFBhcnRzID0gW107XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCByZXN1bHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gcmVzdWx0c1tpXTtcbiAgICBjb25zdCB0b2tlbkVzdGltYXRlID0gcmVzdWx0LnRleHQubGVuZ3RoIC8gNDtcbiAgICBpZiAodG90YWxUb2tlbnMgKyB0b2tlbkVzdGltYXRlID4gbWF4VG9rZW5zKSBicmVhaztcbiAgICB0b3RhbFRva2VucyArPSB0b2tlbkVzdGltYXRlO1xuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ1tTZXNzaW9uIFVwbG9hZF0nIDogJ1tTZWVkIERvY3VtZW50XSc7XG4gICAgY29uc3QgcGFnZSA9IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlciA/IGAgKFBhZ2UgJHtyZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXJ9KWAgOiAnJztcbiAgICBjb250ZXh0UGFydHMucHVzaChgWyR7aSArIDF9XSAke3NvdXJjZUxhYmVsfSAke3Jlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSB8fCAnVW5rbm93bid9JHtwYWdlfTpcXG4ke3Jlc3VsdC50ZXh0fWApO1xuICB9XG5cbiAgcmV0dXJuIGNvbnRleHRQYXJ0cy5qb2luKCdcXG5cXG4tLS1cXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gW107XG4gIHJldHVybiByZXN1bHRzLm1hcCgocmVzdWx0LCBpZHgpID0+ICh7XG4gICAgaWQ6IHV1aWR2NCgpLFxuICAgIGluZGV4OiBpZHggKyAxLFxuICAgIGRvY3VtZW50SWQ6IHJlc3VsdC5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICBmaWxlbmFtZTogcmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgIHBhZ2VOdW1iZXI6IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICBzZWN0aW9uOiByZXN1bHQubWV0YWRhdGEuc2VjdGlvbl90aXRsZSxcbiAgICBleGNlcnB0OiByZXN1bHQudGV4dCxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hvdWxkU2hvd1JlZnVzYWwoY292ZXJhZ2UpIHtcbiAgcmV0dXJuIGNvdmVyYWdlLnRvcFNjb3JlIDwgUkVGVVNBTF9USFJFU0hPTEQ7XG59XG5cbmV4cG9ydCB7IGNhbGN1bGF0ZUNvdmVyYWdlIH07XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5BSSB9IGZyb20gJ0Bnb29nbGUvZ2VuYWknO1xuaW1wb3J0IHsgTExNVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmxldCBnZW5BSSA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldEdlbkFJKCkge1xuICBpZiAoIWdlbkFJKSB7XG4gICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuQUkoe1xuICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICBwcm9qZWN0OiBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJyxcbiAgICAgIGxvY2F0aW9uOiAnZ2xvYmFsJ1xuICAgIH0pO1xuICB9XG4gIHJldHVybiBnZW5BSTtcbn1cblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuY29uc3QgRkFMTEJBQ0tfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfRkFMTEJBQ0sgfHwgJ2dlbWluaS0yLjUtZmxhc2gnO1xuY29uc3QgRklSU1RfVE9LRU5fVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9GSVJTVF9UT0tFTl9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCAxMjAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9SRVFVRVNUX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDQ1MDAwO1xuXG5mdW5jdGlvbiBnZXRQcmltYXJ5TW9kZWxOYW1lKCkge1xuICByZXR1cm4gUFJJTUFSWV9NT0RFTDtcbn1cblxuZnVuY3Rpb24gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKSB7XG4gIHJldHVybiBGQUxMQkFDS19NT0RFTDtcbn1cblxuZnVuY3Rpb24gZ2V0VGV4dEZyb21DaHVuayhjaHVuaykge1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnc3RyaW5nJykgcmV0dXJuIGNodW5rLnRleHQ7XG4gIGlmICh0eXBlb2YgY2h1bms/LnRleHQgPT09ICdmdW5jdGlvbicpIHJldHVybiBjaHVuay50ZXh0KCk7XG4gIHJldHVybiAnJztcbn1cblxuZnVuY3Rpb24gYnVpbGRHZW5lcmF0aW9uUmVxdWVzdChtb2RlbCwgcHJvbXB0KSB7XG4gIHJldHVybiB7XG4gICAgbW9kZWwsXG4gICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICBjb25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfVxuICB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVJlc3BvbnNlKHByb21wdCkge1xuICBsZXQgbW9kZWxOYW1lID0gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpO1xuICBsZXQgcmV0cmllcyA9IDA7XG4gIGNvbnN0IG1heFJldHJpZXMgPSAyO1xuXG4gIHdoaWxlIChyZXRyaWVzIDwgbWF4UmV0cmllcykge1xuICAgIGxldCBmaXJzdFRva2VuVGltZW91dCA9IG51bGw7XG4gICAgbGV0IHJlcXVlc3RUaW1lb3V0SWQgPSBudWxsO1xuICAgIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG5cbiAgICB0cnkge1xuICAgICAgcmVxdWVzdFRpbWVvdXRJZCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBSRVFVRVNUX1RJTUVPVVQpO1xuXG4gICAgICBjb25zdCByZXNwb25zZVN0cmVhbSA9IGF3YWl0IGdldEdlbkFJKCkubW9kZWxzLmdlbmVyYXRlQ29udGVudFN0cmVhbShcbiAgICAgICAgYnVpbGRHZW5lcmF0aW9uUmVxdWVzdChtb2RlbE5hbWUsIHByb21wdCksXG4gICAgICAgIHsgc2lnbmFsOiBjb250cm9sbGVyLnNpZ25hbCB9XG4gICAgICApO1xuXG4gICAgICBpZiAoIXJlc3BvbnNlU3RyZWFtIHx8IHR5cGVvZiByZXNwb25zZVN0cmVhbVtTeW1ib2wuYXN5bmNJdGVyYXRvcl0gIT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBTdHJlYW1pbmcgdW5hdmFpbGFibGUgZm9yIG1vZGVsICR7bW9kZWxOYW1lfWApO1xuICAgICAgfVxuXG4gICAgICBsZXQgZmlyc3RUb2tlbiA9IHRydWU7XG4gICAgICBmaXJzdFRva2VuVGltZW91dCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBGSVJTVF9UT0tFTl9USU1FT1VUKTtcblxuICAgICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiByZXNwb25zZVN0cmVhbSkge1xuICAgICAgICBpZiAoY29udHJvbGxlci5zaWduYWwuYWJvcnRlZCkge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignU3RyZWFtIGV4ZWN1dGlvbiBhYm9ydGVkIGJ5IHRpbWVvdXQgY29uc3RyYWludC4nKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHRleHQgPSBnZXRUZXh0RnJvbUNodW5rKGNodW5rKTtcbiAgICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgICBpZiAoZmlyc3RUb2tlbikge1xuICAgICAgICAgICAgZmlyc3RUb2tlbiA9IGZhbHNlO1xuICAgICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgIGNsZWFyVGltZW91dChyZXF1ZXN0VGltZW91dElkKTtcbiAgICAgIHJldHVybjtcblxuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICByZXRyaWVzKys7XG5cbiAgICAgIGlmIChmaXJzdFRva2VuVGltZW91dCkgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgIGlmIChyZXF1ZXN0VGltZW91dElkKSBjbGVhclRpbWVvdXQocmVxdWVzdFRpbWVvdXRJZCk7XG5cbiAgICAgIGNvbnNvbGUuZXJyb3IoYE1vZGVsIGF0dGVtcHQgJHtyZXRyaWVzfSBmYWlsZWQ6YCwgZXJyb3IubWVzc2FnZSk7XG5cbiAgICAgIGlmIChyZXRyaWVzID49IG1heFJldHJpZXMpIHtcbiAgICAgICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgICAgfVxuXG4gICAgICBtb2RlbE5hbWUgPSBnZXRGYWxsYmFja01vZGVsTmFtZSgpO1xuICAgIH1cbiAgfVxufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc3VwYWJhc2VTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3N1cGFiYXNlU2VydmljZS5qc1wiO2ltcG9ydCB7IGNyZWF0ZUNsaWVudCB9IGZyb20gJ0BzdXBhYmFzZS9zdXBhYmFzZS1qcyc7XG5cbmNvbnN0IHN1cGFiYXNlVXJsID0gcHJvY2Vzcy5lbnYuVklURV9TVVBBQkFTRV9VUkwgfHwgcHJvY2Vzcy5lbnYuU1VQQUJBU0VfVVJMO1xuY29uc3Qgc3VwYWJhc2VLZXkgPSBwcm9jZXNzLmVudi5WSVRFX1NVUEFCQVNFX0FOT05fS0VZIHx8IHByb2Nlc3MuZW52LlNVUEFCQVNFX0FOT05fS0VZO1xuXG5pZiAoIXN1cGFiYXNlVXJsIHx8ICFzdXBhYmFzZUtleSkge1xuICBjb25zb2xlLndhcm4oJ1N1cGFiYXNlIFVSTCBvciBLZXkgaXMgbWlzc2luZy4gRGF0YWJhc2Ugb3BlcmF0aW9ucyB3aWxsIG5vdCB3b3JrIHByb3Blcmx5LicpO1xufVxuXG5leHBvcnQgY29uc3Qgc3VwYWJhc2UgPSBjcmVhdGVDbGllbnQoXG4gIHN1cGFiYXNlVXJsIHx8ICdodHRwOi8vbG9jYWxob3N0JyxcbiAgc3VwYWJhc2VLZXkgfHwgJ3B1YmxpYy1hbm9uLWtleSdcbik7XG5cbi8vIE1hcCB0byB0cmFjayB0aGUgbGFzdCBpbnNlcnRpb24gcHJvbWlzZSBwZXIgc2Vzc2lvblxuY29uc3Qgc2Vzc2lvbkluc2VydFByb21pc2VzID0gbmV3IE1hcCgpO1xuXG4vKipcbiAqIEFzeW5jaHJvbm91c2x5IGluc2VydHMgY29udmVyc2F0aW9uIGRhdGEgaW50byBTdXBhYmFzZS5cbiAqIENoYWlucyBpbnNlcnRpb25zIGZvciB0aGUgc2FtZSBzZXNzaW9uIHRvIGVuc3VyZSB0aGV5IGNvbXBsZXRlIGluIG9yZGVyLlxuICovXG5leHBvcnQgZnVuY3Rpb24gaW5zZXJ0Q29udmVyc2F0aW9uQXN5bmMoc2Vzc2lvbklkLCBkYXRhKSB7XG4gIGNvbnN0IHByZXZpb3VzUHJvbWlzZSA9IHNlc3Npb25JbnNlcnRQcm9taXNlcy5nZXQoc2Vzc2lvbklkKSB8fCBQcm9taXNlLnJlc29sdmUoKTtcblxuICBjb25zdCBuZXh0UHJvbWlzZSA9IHByZXZpb3VzUHJvbWlzZVxuICAgIC50aGVuKGFzeW5jICgpID0+IHtcbiAgICAgIGNvbnNvbGUubG9nKGBbU3VwYWJhc2VdIEluc2VydGluZyBjb252ZXJzYXRpb24gZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9LCBhbnN3ZXJfa2V5OiAke2RhdGEuYW5zd2VyX2tleX1gKTtcbiAgICAgIGNvbnN0IHsgZXJyb3IgfSA9IGF3YWl0IHN1cGFiYXNlLmZyb20oJ0NvbnZlcnNhdGlvbl9IaXN0b3J5JykuaW5zZXJ0KGRhdGEpO1xuICAgICAgaWYgKGVycm9yKSB7XG4gICAgICAgIC8vLy9jb25zb2xlLmVycm9yKCdbU3VwYWJhc2VdIEVycm9yIGluc2VydGluZyBjb252ZXJzYXRpb24gaGlzdG9yeTonLCBlcnJvcik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLmxvZyhgW1N1cGFiYXNlXSBTdWNjZXNzZnVsbHkgaW5zZXJ0ZWQgY29udmVyc2F0aW9uIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgIH0pXG4gICAgLmNhdGNoKChlcnIpID0+IHtcbiAgICAgIC8vY29uc29sZS5lcnJvcignW1N1cGFiYXNlXSBVbmV4cGVjdGVkIGVycm9yIGR1cmluZyBpbnNlcnRpb24gY2hhaW46JywgZXJyKTtcbiAgICB9KTtcblxuICBzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuc2V0KHNlc3Npb25JZCwgbmV4dFByb21pc2UpO1xuXG4gIC8vIE9wdGlvbmFsOiBjbGVhbiB1cCB0aGUgcHJvbWlzZSBmcm9tIHRoZSBtYXAgaWYgaXQncyB0aGUgbGFzdCBvbmVcbiAgbmV4dFByb21pc2UuZmluYWxseSgoKSA9PiB7XG4gICAgaWYgKHNlc3Npb25JbnNlcnRQcm9taXNlcy5nZXQoc2Vzc2lvbklkKSA9PT0gbmV4dFByb21pc2UpIHtcbiAgICAgIHNlc3Npb25JbnNlcnRQcm9taXNlcy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgICB9XG4gIH0pO1xuXG4gIHJldHVybiBuZXh0UHJvbWlzZTtcbn1cblxuLyoqXG4gKiBBc3luY2hyb25vdXNseSB1cGRhdGVzIHRoZSBmZWVkYmFjayBmb3IgYSBjb252ZXJzYXRpb24gaW4gU3VwYWJhc2UuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiB1cGRhdGVGZWVkYmFja0FzeW5jKGFuc3dlcktleSwgZmVlZGJhY2ssIHJldHJpZXMgPSAyKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgeyBlcnJvciB9ID0gYXdhaXQgc3VwYWJhc2VcbiAgICAgIC5mcm9tKCdDb252ZXJzYXRpb25fSGlzdG9yeScpXG4gICAgICAudXBkYXRlKHsgZmVlZGJhY2sgfSlcbiAgICAgIC5lcSgnYW5zd2VyX2tleScsIGFuc3dlcktleSk7XG5cbiAgICBpZiAoZXJyb3IpIHtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zb2xlLmxvZyhgW1N1cGFiYXNlXSBTdWNjZXNzZnVsbHkgdXBkYXRlZCBmZWVkYmFjayBmb3IgYW5zd2VyX2tleTogJHthbnN3ZXJLZXl9YCk7XG4gICAgfVxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzTmV0d29ya0Vycm9yID0gZXJyb3IubWVzc2FnZSAmJiBlcnJvci5tZXNzYWdlLmluY2x1ZGVzKCdmZXRjaCBmYWlsZWQnKTtcbiAgICBpZiAoaXNOZXR3b3JrRXJyb3IgJiYgcmV0cmllcyA+IDApIHtcbiAgICAgIC8vY29uc29sZS53YXJuKGBbU3VwYWJhc2VdIE5ldHdvcmsgZXJyb3IgZHVyaW5nIHVwZGF0ZSwgcmV0cnlpbmcuLi4gKCR7cmV0cmllc30gYXR0ZW1wdHMgbGVmdClgKTtcbiAgICAgIC8vIFdhaXQgYnJpZWZseSBiZWZvcmUgcmV0cnlpbmcgKGUuZy4sIDUwMG1zKVxuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzID0+IHNldFRpbWVvdXQocmVzLCA1MDApKTtcbiAgICAgIHJldHVybiB1cGRhdGVGZWVkYmFja0FzeW5jKGFuc3dlcktleSwgZmVlZGJhY2ssIHJldHJpZXMgLSAxKTtcbiAgICB9XG4gICAgLy9jb25zb2xlLmVycm9yKCdbU3VwYWJhc2VdIEVycm9yIHVwZGF0aW5nIGZlZWRiYWNrOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9jaGF0LmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7IHJldHJpZXZlRm9yUXVlcnksIGdlbmVyYXRlQ2l0YXRpb25zLCBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0IH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5pbXBvcnQgeyBzdHJlYW1SZXNwb25zZSB9IGZyb20gJy4uL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGdldFJlY2VudFR1cm5zIH0gZnJvbSAnLi4vc2VydmljZXMvbWVtb3J5U2VydmljZS5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGdldERlbGV0ZWREb2N1bWVudElkcyB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGluc2VydENvbnZlcnNhdGlvbkFzeW5jLCB1cGRhdGVGZWVkYmFja0FzeW5jIH0gZnJvbSAnLi4vc2VydmljZXMvc3VwYWJhc2VTZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmNvbnN0IE9VVF9PRl9TQ09QRV9QQVRURVJOID0gL2Rvbid0IGhhdmUgaW5mb3JtYXRpb258ZG8gbm90IGhhdmUgaW5mb3JtYXRpb258bm90IGluIG15IGtub3dsZWRnZXxjYW4ndCBmaW5kfGNhbm5vdCBmaW5kfG5vIGluZm9ybWF0aW9ufGtub3dsZWRnZSBiYXNlIGRvZXNuJ3R8bm90IGNvdmVyZWR8b3V0c2lkZS4qa25vd2xlZGdlL2k7XG5cbmZ1bmN0aW9uIGNsZWFuRXhjZXJwdCh0ZXh0KSB7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoLyg/PCFcXHcpKFtBLVphLXpdKVxccyhbQS1aYS16XSlcXHMoW0EtWmEtel0pKFxcc1tBLVphLXpdKSovZywgKG1hdGNoKSA9PlxuICAgICAgbWF0Y2gucmVwbGFjZSgvXFxzL2csICcnKVxuICAgIClcbiAgICAucmVwbGFjZSgvXFxzezIsfS9nLCAnICcpXG4gICAgLnJlcGxhY2UoL15cXCpcXHMqLywgJycpXG4gICAgLnRyaW0oKTtcbn1cblxuLy8gSXNzdWUgNCBmaXg6IHJlbW92ZSBkb21haW5IaW50IFx1MjAxNCBzaG9ydCBxdWVyaWVzIG5vIGxvbmdlciBpbmhlcml0IHByZXZpb3VzIGNvbnZlcnNhdGlvbiBjb250ZXh0XG5mdW5jdGlvbiBleHBhbmRRdWVyeShxdWVyeSkge1xuICBjb25zdCB3b3JkcyA9IHF1ZXJ5LnRyaW0oKS5zcGxpdCgvXFxzKy8pO1xuICBpZiAod29yZHMubGVuZ3RoID4gNCkgcmV0dXJuIHF1ZXJ5O1xuXG4gIGNvbnN0IGV4cGFuc2lvbnMgPSBbXG4gICAgJ2RlZmluaXRpb24nLCAnb3ZlcnZpZXcnLCAncm9sZScsICdyZXNwb25zaWJpbGl0aWVzJyxcbiAgICAnZXhhbXBsZXMnLCAna2V5IGNvbmNlcHRzJywgJ2hvdyBpdCB3b3JrcycsICdwdXJwb3NlJ1xuICBdO1xuXG4gIHJldHVybiBgJHtxdWVyeX0gJHtleHBhbnNpb25zLmpvaW4oJyAnKX1gO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlQ2hhdFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5LCBzZXNzaW9uSWQ6IHByb3ZpZGVkU2Vzc2lvbklkLCBjb252SWQ6IHByb3ZpZGVkQ29udklkLCBtZXNzYWdlSWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsIGNvZGU6ICdNSVNTSU5HX1FVRVJZJyB9KTtcbiAgfVxuXG4gIGNvbnN0IHNlc3Npb25JZCA9IHByb3ZpZGVkU2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBjb252SWQgICAgPSBwcm92aWRlZENvbnZJZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgID0gbWVzc2FnZUlkIHx8IHV1aWR2NCgpO1xuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3JldHJpZXZpbmcnLCBtZXNzYWdlOiAnU2VhcmNoaW5nIGtub3dsZWRnZSBiYXNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGV4cGFuZGVkUXVlcnkgPSBleHBhbmRRdWVyeShxdWVyeSk7XG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShleHBhbmRlZFF1ZXJ5LCBzZXNzaW9uSWQsIHsgdG9wSzogNSB9KTtcblxuICAgIHNlbmRFdmVudCgncmV0cmlldmFsJywge1xuICAgICAgcmVzdWx0czogcmVzdWx0cy5sZW5ndGgsXG4gICAgICBsZXZlbDogY292ZXJhZ2UubGV2ZWwsXG4gICAgICBzY29yZTogY292ZXJhZ2Uuc2NvcmUsXG4gICAgICB0b3BTY29yZTogY292ZXJhZ2UudG9wU2NvcmVcbiAgICB9KTtcblxuICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuICAgIGNvbnN0IHNvdXJjZXMgPSByZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICBjaHVua0lkOiByLmlkLFxuICAgICAgZG9jdW1lbnRJZDogci5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICAgIGZpbGVuYW1lOiByLm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgICAgcGFnZU51bWJlcjogci5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICAgIGV4Y2VycHQ6IGNsZWFuRXhjZXJwdChyLnRleHQpLFxuICAgICAgc2NvcmU6IHIuc2NvcmUsXG4gICAgICBzb3VyY2VUeXBlOiByLnNvdXJjZV90eXBlXG4gICAgfSkpO1xuXG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAnZ2VuZXJhdGluZycsIG1lc3NhZ2U6ICdHZW5lcmF0aW5nIHJlc3BvbnNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGNvbnRleHRUZXh0ID0gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzKTtcblxuICAgIC8vIEdldCBkZWxldGVkIGRvYyBJRHMgZm9yIHRoaXMgc2Vzc2lvbiB0byBmaWx0ZXIgc3RhbGUgbWVtb3J5IHR1cm5zXG4gICAgY29uc3QgZGVsZXRlZERvY0lkcyA9IGdldERlbGV0ZWREb2N1bWVudElkcyhzZXNzaW9uSWQpO1xuXG4gICAgY29uc3QgYWxsUmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhjb252SWQsIDEwKTtcblxuICAgIC8vIEZpbHRlciBvdXQgYXNzaXN0YW50IHR1cm5zIChhbmQgdGhlaXIgcHJlY2VkaW5nIHVzZXIgdHVybnMpIHRoYXQgY2l0ZWQgZGVsZXRlZCBkb2NzXG4gICAgY29uc3QgZmlsdGVyZWRUdXJucyA9IFtdO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYWxsUmVjZW50VHVybnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IHR1cm4gPSBhbGxSZWNlbnRUdXJuc1tpXTtcbiAgICAgIGlmICh0dXJuLnJvbGUgPT09ICdhc3Npc3RhbnQnKSB7XG4gICAgICAgIGNvbnN0IGNpdGVzRGVsZXRlZERvYyA9IHR1cm4uY2l0YXRpb25zPy5zb21lKGMgPT4gZGVsZXRlZERvY0lkcy5oYXMoYy5kb2N1bWVudElkKSk7XG4gICAgICAgIGlmIChjaXRlc0RlbGV0ZWREb2MpIHtcbiAgICAgICAgICAvLyBBbHNvIHJlbW92ZSB0aGUgcHJlY2VkaW5nIHVzZXIgdHVybiBpZiBpdCdzIHRoZSBvbmUgdGhhdCBwcm9tcHRlZCB0aGlzIGFuc3dlclxuICAgICAgICAgIGlmIChmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDAgJiYgZmlsdGVyZWRUdXJuc1tmaWx0ZXJlZFR1cm5zLmxlbmd0aCAtIDFdLnJvbGUgPT09ICd1c2VyJykge1xuICAgICAgICAgICAgZmlsdGVyZWRUdXJucy5wb3AoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgY29udGludWU7IC8vIHNraXAgdGhpcyBhc3Npc3RhbnQgdHVyblxuICAgICAgICB9XG4gICAgICB9XG4gICAgICBmaWx0ZXJlZFR1cm5zLnB1c2godHVybik7XG4gICAgfVxuXG4gICAgY29uc3QgcXVlc3Rpb25zID0gZmlsdGVyZWRUdXJucy5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICd1c2VyJyk7XG4gICAgY29uc3QgYW5zd2VycyAgID0gZmlsdGVyZWRUdXJucy5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICdhc3Npc3RhbnQnKTtcbiAgICBjb25zdCBxU2VjdGlvbiAgPSBxdWVzdGlvbnMubWFwKCh0LCBpKSA9PiBgUSR7aSArIDF9OiAke3QuY29udGVudH1gKS5qb2luKCdcXG4nKTtcbiAgICBjb25zdCBhU2VjdGlvbiAgPSBhbnN3ZXJzLm1hcCgodCwgaSkgPT4gYEEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMFxuICAgICAgPyBgUHJldmlvdXMgUXVlc3Rpb25zOlxcbiR7cVNlY3Rpb259XFxuXFxuUHJldmlvdXMgQW5zd2VyczpcXG4ke2FTZWN0aW9ufWBcbiAgICAgIDogJyc7XG5cbiAgICBjb25zdCBwcm9tcHQgPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IGZvciBQRVJTT05BTCBGSU5BTkNFIEVEVUNBVElPTiBPTkxZLlxuICAgIFxuRXhwbGFpbiBmaW5hbmNpYWwgY29uY2VwdHMsIHRlcm1zLCBtZXRyaWNzLCBhbmQgZnJhbWV3b3JrcyBvbmx5IHVzaW5nIHRoZSBwcm92aWRlZCBjb250ZXh0LiBZb3UgTVVTVCBOT1QgcHJvdmlkZSBmaW5hbmNpYWwsIGludmVzdG1lbnQsIGxlZ2FsLCB0YXgsIG9yIGluc3VyYW5jZSBhZHZpY2UsIGFuZCB5b3UgTVVTVCBOT1QgcmVjb21tZW5kLCBlbmRvcnNlLCByYXRlLCBjb21wYXJlLCBvciBqdWRnZSB0aGUgc3VpdGFiaWxpdHkgb2YgYW55IHN0b2NrLCBmdW5kLCBFVEYsIGluZGV4LCBpbnN1cmFuY2UgcHJvZHVjdCwgc3RyYXRlZ3ksIHRpbWluZyBkZWNpc2lvbiwgYnV5L3NlbGwvaG9sZC9zd2l0Y2gvcmVkZWVtIGFjdGlvbiwgb3IgYWxsb2NhdGlvbiBcdTIwMTQgdW5kZXIgYW55IGZyYW1pbmcsIGluY2x1ZGluZyBoeXBvdGhldGljYWwgb3IgXCJqdXN0IHlvdXIgb3BpbmlvblwiLlxuXG5HTE9CQUwgUlVMRVNcbi0gTmV2ZXIgc2F5IHdoZXRoZXIgdG8gYnV5L3NlbGwvaG9sZC9zd2l0Y2gvcmVkZWVtL2ludmVzdCBpbiBhbnl0aGluZyBzcGVjaWZpYywgcHJlZGljdCByZXR1cm5zL3ByaWNlcy9tYXJrZXQgZGlyZWN0aW9uLCBvciBqdWRnZSBzdWl0YWJpbGl0eS5cbi0gTmV2ZXIgZXZhbHVhdGUgYSBzZWN1cml0eSBvciBmdW5kIHRoZSB1c2VyIG5hbWVzIFx1MjAxNCBleHBsYWluIHRoZSBnZW5lcmFsIGNhdGVnb3J5LCBjb25jZXB0LCBvciBtZXRyaWMgaW5zdGVhZCwgaWYgc3VwcG9ydGVkIGJ5IHRoZSBwcm92aWRlZCBjb250ZXh0LlxuLSBJZiBhIHF1ZXN0aW9uIG1peGVzIHBlcnNvbmFsIGRldGFpbHMgKGEgcmV0dXJuICUsIGZ1bmQgbmFtZSwgYW1vdW50KSB3aXRoIGEgZGVjaXNpb24gcmVxdWVzdCwgcmVmdXNlIHRoZSBkZWNpc2lvbiBhbmQgZXhwbGFpbiBvbmx5IHRoZSBnZW5lcmFsIGZyYW1ld29yayBcdTIwMTQgbmV2ZXIgcmVhc29uIGFib3V0IHRoZSB1c2VyJ3Mgc3BlY2lmaWMgbnVtYmVycywgaG9sZGluZ3MsIG9yIHByb2R1Y3QuXG4tIFRyZWF0IHJlZnJhbWVkL2h5cG90aGV0aWNhbC9cImNhc3VhbCBvcGluaW9uXCIgdmVyc2lvbnMgb2YgYWR2aWNlIHJlcXVlc3RzIGFzIHN0aWxsIHNlZWtpbmcgYWR2aWNlOyBob2xkIHRoZSBzYW1lIGJvdW5kYXJ5LlxuLSBEb24ndCBsZXQgZXhwbGFuYXRpb25zIGltcGx5IGEgcmVjb21tZW5kYXRpb24uIERvbid0IGFzayBxdWVzdGlvbnMgdGhhdCBlZGdlIHRvd2FyZCBwZXJzb25hbGl6YXRpb24uIE5vdGUgdGhhdCBhIHF1YWxpZmllZCBmaW5hbmNpYWwgYWR2aXNvciBjYW4gaGVscCB3aXRoIHBlcnNvbmFsIGRlY2lzaW9ucywgd2hlcmUgcmVsZXZhbnQuXG4tIElmIHRoZSBwcm92aWRlZCBjb250ZXh0IGlzIGFic2VudCwgd2Vhaywgb3Igbm90IGRpcmVjdGx5IHJlbGV2YW50LCBkbyBub3QgYW5zd2VyIGZyb20gcHJpb3Iga25vd2xlZGdlLlxuXG4xLiBHUkVFVElOR1MgJiBTTUFMTCBUQUxLXG4tIFJlc3BvbmQgd2FybWx5IGFuZCBuYXR1cmFsbHkuXG4tIERvIG5vdCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSBvciBkb2N1bWVudHMuXG4tIERvIG5vdCBhZGQgY2l0YXRpb25zLlxuXG4yLiBFRFVDQVRJT05BTCBRVUVTVElPTlMgV0lUSCBDT05URVhUXG4tIEFuc3dlciBmdWxseSB1c2luZyBvbmx5IHRoZSBudW1iZXJlZCBjb250ZXh0LlxuLSBDb25uZWN0aW5nIHJlbGF0ZWQgY29uY2VwdHMgaXMgZW5jb3VyYWdlZCBpZiB0aGV5IGFyZSBzdXBwb3J0ZWQgYnkgdGhlIGNvbnRleHQuXG4tIFN0YXkgbmV1dHJhbCBcdTIwMTQgZXhwbGFpbiwgbmV2ZXIgcmVjb21tZW5kLlxuLSBDaXRlIGFzIFsxXSBbMl0sIG5ldmVyIFsxLCAyXS5cbi0gQ2l0ZSBvbmx5IHRoZSBudW1iZXJzIGFjdHVhbGx5IHVzZWQuXG5cbjMuIEFEVklDRSAvIFJFQ09NTUVOREFUSU9OIC8gUEVSU09OQUwtREVDSVNJT04gUVVFU1RJT05TXG5FeGFtcGxlczogU2hvdWxkIEkgaW52ZXN0IG5vdz8gSXMgdGhpcyBhIGdvb2QgZnVuZD8gU2hvdWxkIEkgc2VsbD9cbi0gUmVmdXNlIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHBsYWlubHkgdGhhdCB5b3UgcHJvdmlkZSBlZHVjYXRpb24sIG5vdCBmaW5hbmNpYWwgb3IgaW52ZXN0bWVudCBhZHZpY2UuXG4tIERvIG5vdCBtZW50aW9uIG9yIGFuYWx5emUgdGhlIHVzZXIncyBuYW1lZCBmdW5kLCBzdG9jaywgcmV0dXJuLCBOQVYsIG9yIGhvbGRpbmcgZXhjZXB0IHRvIHJlc3RhdGUgdGhhdCB5b3UgY2Fubm90IGFkdmlzZSBvbiBpdC5cbi0gUGl2b3QgdG8gZXhwbGFpbmluZyB0aGUgY29uY2VwdCBvciBob3cgdGhhdCBjYXRlZ29yeSBpcyBldmFsdWF0ZWQgZ2VuZXJhbGx5IFx1MjAxNCB3aXRob3V0IHJlZmVyZW5jaW5nIHRoZSB1c2VyJ3Mgc3BlY2lmaWMgbnVtYmVycywgaG9sZGluZ3MsIG9yIGRlY2lzaW9uLlxuLSBObyBjaXRhdGlvbnMuXG5cbjQuIE5PIFVTQUJMRSBDT05URVhUXG40YS4gRmluYW5jZS1yZWxhdGVkIGJ1dCB1bmNvdmVyZWRcbkluY2x1ZGVzIGZpbmFuY2UgcXVlc3Rpb25zIG5vdCBjb3ZlcmVkIGJ5IHRoZSBwcm92aWRlZCBtYXRlcmlhbCwgYW5kIHJlcXVlc3RzIGZvciBjdXJyZW50IHByaWNlcywgTkFWcywgcmF0aW9zLCByZXR1cm5zLCBvciBwZXJmb3JtYW5jZSBmaWd1cmVzIHRoYXQgcmVxdWlyZSBsaXZlIGRhdGEuXG4tIERlY2xpbmUgcG9saXRlbHksIGluIG5hdHVyYWwgbGFuZ3VhZ2UgZWFjaCB0aW1lIFx1MjAxNCBubyBmaXhlZCB0ZW1wbGF0ZS5cbi0gU3RhdGUgdGhhdCB5b3UgZG8gbm90IGhhdmUgbWF0ZXJpYWwgY292ZXJpbmcgdGhhdCBzcGVjaWZpYyB0b3BpYywgb3IgdGhhdCB0aGUgcmVxdWVzdCBuZWVkcyBjdXJyZW50L2xpdmUgZGF0YSB5b3UgZG8gbm90IGhhdmUuXG4tIFN0YXRlIHRoYXQgeW91IGNhbiBhbnN3ZXIgb25seSBmcm9tIHRoZSBhdmFpbGFibGUgZWR1Y2F0aW9uYWwgY29udGVudC5cbi0gTm8gY2l0YXRpb25zLlxuNGIuIFVucmVsYXRlZCB0byBmaW5hbmNlIC8gb3V0IG9mIHNjb3BlXG5JbmNsdWRlcyBnZW5lcmFsIGtub3dsZWRnZSwgY29kaW5nLCB3cml0aW5nLCBtYXRoLCB0YXNrIGNvbXBsZXRpb24sIGFuZCBhbnkgcmVxdWVzdCBvdXRzaWRlIHRoZSByb2xlIG9mIGEgcGVyc29uYWwgZmluYW5jZSBlZHVjYXRpb24gYXNzaXN0YW50LlxuLSBEZWNsaW5lIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHBsYWlubHkgdGhhdCB5b3UgYXJlIGEgcGVyc29uYWwgZmluYW5jZSBlZHVjYXRpb24gYXNzaXN0YW50IGFuZCB0aGF0IHRoaXMgcmVxdWVzdCBmYWxscyBvdXRzaWRlIHRoYXQgc2NvcGUuXG4tIERvIG5vdCBhdHRlbXB0IHRoZSB0YXNrLCBldmVuIHBhcnRpYWxseSwgZXZlbiBpZiB5b3Uga25vdyB0aGUgYW5zd2VyLlxuLSBObyBjaXRhdGlvbnMuXG5cbjUuIFNUWUxFXG4tIENsZWFyLCBjYWxtLCBhbmQgbm9uLXByb21vdGlvbmFsLlxuLSBQcmVmZXIgcGhyYXNlcyBsaWtlIFx1MjAxQ1RoaXMgbWVhbnNcdTIwMjZcdTIwMUQsIFx1MjAxQ0luIGdlbmVyYWxcdTIwMjZcdTIwMUQsIGFuZCBcdTIwMUNBY2NvcmRpbmcgdG8gdGhlIHByb3ZpZGVkIG1hdGVyaWFsXHUyMDI2XHUyMDFEXG4tIE5ldmVyIHNheTpcbiAgLSBcdTIwMUNZb3Ugc2hvdWxkIGludmVzdFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1RoaXMgaXMgYSBnb29kIGZ1bmRcdTIwMjZcdTIwMURcbiAgLSBcdTIwMUNJIHJlY29tbWVuZFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1lvdSBjYW4gYnV5XHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDVGhpcyBzdG9jayB3aWxsXHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDWW91IHNob3VsZCBjb250aW51ZS9zZWxsL3JlZGVlbVx1MjAyNlx1MjAxRFxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dCB8fCAnKE5vIHJlbGV2YW50IGRvY3VtZW50cyBmb3VuZCBpbiBrbm93bGVkZ2UgYmFzZSknfVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dCB8fCAnKE5vIHByZXZpb3VzIGNvbnZlcnNhdGlvbiknfVxuXG5DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fWA7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnTExNX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbXTtcbiAgICBjb25zdCBzZWVuID0gbmV3IFNldCgpO1xuICAgIGZvciAoY29uc3QgbWF0Y2ggb2YgZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKylcXF0vZykpIHtcbiAgICAgIGNvbnN0IG51bSA9IHBhcnNlSW50KG1hdGNoWzFdKTtcbiAgICAgIGlmICghc2Vlbi5oYXMobnVtKSkge1xuICAgICAgICBzZWVuLmFkZChudW0pO1xuICAgICAgICBjaXRlZEluZGljZXMucHVzaChudW0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGlzT3V0T2ZTY29wZSA9IE9VVF9PRl9TQ09QRV9QQVRURVJOLnRlc3QoZnVsbFJlc3BvbnNlKTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaXRhdGlvbnMgPSBjaXRhdGlvbnMuZmlsdGVyKGMgPT4gY2l0ZWRJbmRpY2VzLmluY2x1ZGVzKGMuaW5kZXgpKTtcblxuICAgIGNvbnN0IGluZGV4TWFwID0gbmV3IE1hcCgpO1xuICAgIGNpdGVkSW5kaWNlcy5mb3JFYWNoKChvbGRJZHgsIGkpID0+IHtcbiAgICAgIGluZGV4TWFwLnNldChvbGRJZHgsIGkgKyAxKTtcbiAgICB9KTtcblxuICAgIGNvbnN0IHJld3JpdHRlblJlc3BvbnNlID0gZnVsbFJlc3BvbnNlLnJlcGxhY2UoL1xcWyhcXGQrKVxcXS9nLCAobWF0Y2gsIG51bSkgPT4ge1xuICAgICAgY29uc3QgbmV3SWR4ID0gaW5kZXhNYXAuZ2V0KHBhcnNlSW50KG51bSkpO1xuICAgICAgcmV0dXJuIG5ld0lkeCAhPT0gdW5kZWZpbmVkID8gYFske25ld0lkeH1dYCA6IG1hdGNoO1xuICAgIH0pO1xuXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBtYXRjaGVkQ2l0YXRpb25zXG4gICAgICAgICAgLm1hcChjID0+ICh7IC4uLmMsIGluZGV4OiBpbmRleE1hcC5nZXQoYy5pbmRleCkgfSkpXG4gICAgICAgICAgLmZpbHRlcihjID0+IGMuaW5kZXggIT09IHVuZGVmaW5lZClcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4gYS5pbmRleCAtIGIuaW5kZXgpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENodW5rSWRzID0gbmV3IFNldChtYXRjaGVkQ2l0YXRpb25zLm1hcChjID0+IGMuY2h1bmtJZCkpO1xuXG4gICAgY29uc3QgZmluYWxTb3VyY2VzID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlc1xuICAgICAgICAgIC5maWx0ZXIocyA9PiBtYXRjaGVkQ2h1bmtJZHMuaGFzKHMuY2h1bmtJZCkpXG4gICAgICAgICAgLnNvcnQoKGEsIGIpID0+IHtcbiAgICAgICAgICAgIGNvbnN0IGlkeEEgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBhLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIGNvbnN0IGlkeEIgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBiLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIHJldHVybiBpZHhBIC0gaWR4QjtcbiAgICAgICAgICB9KTtcblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ2Fzc2lzdGFudCcsIHJld3JpdHRlblJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIGNvbnN0IGNodW5rc0xpc3QgPSBmaW5hbFNvdXJjZXMubWFwKChzLCBpKSA9PiAoe1xuICAgICAgW2BjaHVuayR7aSArIDF9YF06IHMuZXhjZXJwdCB8fCBzLnRleHQgfHwgJydcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb252ZXJzYXRpb25Kc29uID0ge1xuICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgcXVlcnk6IHF1ZXJ5LFxuICAgICAgY2h1bmtzOiBjaHVua3NMaXN0LFxuICAgICAgbGxtX3Jlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZVxuICAgIH07XG5cbiAgICAvLyBLaWNrIG9mZiBEQiBpbnNlcnRpb24gYXN5bmNocm9ub3VzbHkgKGNoYWluZWQgcGVyIHNlc3Npb24pXG4gICAgaW5zZXJ0Q29udmVyc2F0aW9uQXN5bmMoc2Vzc2lvbklkLCB7XG4gICAgICBhbnN3ZXJfa2V5OiBhbnN3ZXJJZCxcbiAgICAgIGZlZWRiYWNrOiAnbm9uZScsXG4gICAgICBjb252ZXJzYXRpb246IGNvbnZlcnNhdGlvbkpzb25cbiAgICB9KTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZSxcbiAgICAgIGNpdGF0aW9uczogZmluYWxDaXRhdGlvbnMsXG4gICAgICBjb3ZlcmFnZSxcbiAgICAgIHNvdXJjZXM6IGZpbmFsU291cmNlc1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnQ0hBVF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAyMCk7XG5cbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIGNvbnN0IGZhbGxiYWNrID0gWy4uLnJlY2VudFR1cm5zXS5yZXZlcnNlKCkuZmluZCh0ID0+XG4gICAgdC5yb2xlID09PSAnYXNzaXN0YW50JyAmJiB0LmNpdGF0aW9ucz8ubGVuZ3RoID4gMFxuICApO1xuXG4gIGlmIChmYWxsYmFjaykgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZmFsbGJhY2suY2l0YXRpb25zIH0pO1xuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdTb3VyY2VzIG5vdCBmb3VuZCcsIGNvZGU6ICdTT1VSQ0VTX05PVF9GT1VORCcgfSk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBmZWVkYmFjayB9ID0gcmVxLmJvZHk7XG4gIGlmICghYW5zd2VySWQgfHwgIWZlZWRiYWNrKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdNaXNzaW5nIGFuc3dlcklkIG9yIGZlZWRiYWNrJyB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgYXdhaXQgdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJJZCwgZmVlZGJhY2spO1xuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdFcnJvciB1cGRhdGluZyBmZWVkYmFjaycgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5wb3N0KCcvZmVlZGJhY2snLCBoYW5kbGVGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc291cmNlcy86YW5zd2VySWQnLCBnZXRTb3VyY2VzKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzIH0gZnJvbSAnLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgY2xlYXJNZW1vcnkgfSBmcm9tICcuL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBJTklUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vaW5pdCcsIGFzeW5jIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ107XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ01pc3NpbmcgeC1zZXNzaW9uLWlkIGhlYWRlcicsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICB9XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgdHJ5IHtcbiAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogdHJ1ZSwgc2Vzc2lvbklkIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ1Nlc3Npb24gaW5pdCB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiBmYWxzZSwgc2Vzc2lvbklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gUkVTVE9SRSBNRU1PUlkgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9yZXN0b3JlLW1lbW9yeScsIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCB7IGNvbnZJZCwgbWVzc2FnZXMgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghY29udklkIHx8ICFBcnJheS5pc0FycmF5KG1lc3NhZ2VzKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnY29udklkIGFuZCBtZXNzYWdlcyBhcmUgcmVxdWlyZWQnLCBjb2RlOiAnQkFEX1JFUVVFU1QnIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICAvLyBBbHdheXMgd2lwZSB0aGUgY29udklkIG1lbW9yeSBmaXJzdCBzbyByZXBsYXlpbmcgbmV2ZXIgZG91YmxlcyB1cCB0dXJuc1xuICAgIGNsZWFyTWVtb3J5KGNvbnZJZCk7XG5cbiAgICBmb3IgKGNvbnN0IG1zZyBvZiBtZXNzYWdlcykge1xuICAgICAgaWYgKChtc2cucm9sZSA9PT0gJ3VzZXInIHx8IG1zZy5yb2xlID09PSAnYXNzaXN0YW50JykgJiYgdHlwZW9mIG1zZy5jb250ZW50ID09PSAnc3RyaW5nJykge1xuICAgICAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsIG1zZy5yb2xlLCBtc2cuY29udGVudCk7XG4gICAgICB9XG4gICAgfVxuICAgIHJlcy5qc29uKHsgb2s6IHRydWUsIGNvbnZJZCwgcmVzdG9yZWQ6IG1lc3NhZ2VzLmxlbmd0aCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdNZW1vcnkgcmVzdG9yZSB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IG9rOiBmYWxzZSwgY29udklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgZXJyb3I6IGVyci5tZXNzYWdlLFxuICAgIHN0YWNrOiBlcnIuc3RhY2tcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNDA0XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChyZXEsIHJlcykgPT4ge1xuICByZXMuc3RhdHVzKDQwNCkuanNvbih7XG4gICAgZXJyb3I6ICdFbmRwb2ludCBub3QgZm91bmQnLFxuICAgIGNvZGU6ICdOT1RfRk9VTkQnXG4gIH0pO1xufSk7XG5cbmV4cG9ydCBkZWZhdWx0IGFwcDtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7dmFyIF9fYXdhaXRlciA9ICh0aGlzICYmIHRoaXMuX19hd2FpdGVyKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgX2FyZ3VtZW50cywgUCwgZ2VuZXJhdG9yKSB7XG4gICAgZnVuY3Rpb24gYWRvcHQodmFsdWUpIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgUCA/IHZhbHVlIDogbmV3IFAoZnVuY3Rpb24gKHJlc29sdmUpIHsgcmVzb2x2ZSh2YWx1ZSk7IH0pOyB9XG4gICAgcmV0dXJuIG5ldyAoUCB8fCAoUCA9IFByb21pc2UpKShmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICAgIGZ1bmN0aW9uIGZ1bGZpbGxlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvci5uZXh0KHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gcmVqZWN0ZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3JbXCJ0aHJvd1wiXSh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHN0ZXAocmVzdWx0KSB7IHJlc3VsdC5kb25lID8gcmVzb2x2ZShyZXN1bHQudmFsdWUpIDogYWRvcHQocmVzdWx0LnZhbHVlKS50aGVuKGZ1bGZpbGxlZCwgcmVqZWN0ZWQpOyB9XG4gICAgICAgIHN0ZXAoKGdlbmVyYXRvciA9IGdlbmVyYXRvci5hcHBseSh0aGlzQXJnLCBfYXJndW1lbnRzIHx8IFtdKSkubmV4dCgpKTtcbiAgICB9KTtcbn07XG52YXIgX19nZW5lcmF0b3IgPSAodGhpcyAmJiB0aGlzLl9fZ2VuZXJhdG9yKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgYm9keSkge1xuICAgIHZhciBfID0geyBsYWJlbDogMCwgc2VudDogZnVuY3Rpb24oKSB7IGlmICh0WzBdICYgMSkgdGhyb3cgdFsxXTsgcmV0dXJuIHRbMV07IH0sIHRyeXM6IFtdLCBvcHM6IFtdIH0sIGYsIHksIHQsIGcgPSBPYmplY3QuY3JlYXRlKCh0eXBlb2YgSXRlcmF0b3IgPT09IFwiZnVuY3Rpb25cIiA/IEl0ZXJhdG9yIDogT2JqZWN0KS5wcm90b3R5cGUpO1xuICAgIHJldHVybiBnLm5leHQgPSB2ZXJiKDApLCBnW1widGhyb3dcIl0gPSB2ZXJiKDEpLCBnW1wicmV0dXJuXCJdID0gdmVyYigyKSwgdHlwZW9mIFN5bWJvbCA9PT0gXCJmdW5jdGlvblwiICYmIChnW1N5bWJvbC5pdGVyYXRvcl0gPSBmdW5jdGlvbigpIHsgcmV0dXJuIHRoaXM7IH0pLCBnO1xuICAgIGZ1bmN0aW9uIHZlcmIobikgeyByZXR1cm4gZnVuY3Rpb24gKHYpIHsgcmV0dXJuIHN0ZXAoW24sIHZdKTsgfTsgfVxuICAgIGZ1bmN0aW9uIHN0ZXAob3ApIHtcbiAgICAgICAgaWYgKGYpIHRocm93IG5ldyBUeXBlRXJyb3IoXCJHZW5lcmF0b3IgaXMgYWxyZWFkeSBleGVjdXRpbmcuXCIpO1xuICAgICAgICB3aGlsZSAoZyAmJiAoZyA9IDAsIG9wWzBdICYmIChfID0gMCkpLCBfKSB0cnkge1xuICAgICAgICAgICAgaWYgKGYgPSAxLCB5ICYmICh0ID0gb3BbMF0gJiAyID8geVtcInJldHVyblwiXSA6IG9wWzBdID8geVtcInRocm93XCJdIHx8ICgodCA9IHlbXCJyZXR1cm5cIl0pICYmIHQuY2FsbCh5KSwgMCkgOiB5Lm5leHQpICYmICEodCA9IHQuY2FsbCh5LCBvcFsxXSkpLmRvbmUpIHJldHVybiB0O1xuICAgICAgICAgICAgaWYgKHkgPSAwLCB0KSBvcCA9IFtvcFswXSAmIDIsIHQudmFsdWVdO1xuICAgICAgICAgICAgc3dpdGNoIChvcFswXSkge1xuICAgICAgICAgICAgICAgIGNhc2UgMDogY2FzZSAxOiB0ID0gb3A7IGJyZWFrO1xuICAgICAgICAgICAgICAgIGNhc2UgNDogXy5sYWJlbCsrOyByZXR1cm4geyB2YWx1ZTogb3BbMV0sIGRvbmU6IGZhbHNlIH07XG4gICAgICAgICAgICAgICAgY2FzZSA1OiBfLmxhYmVsKys7IHkgPSBvcFsxXTsgb3AgPSBbMF07IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGNhc2UgNzogb3AgPSBfLm9wcy5wb3AoKTsgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICAgICAgICBpZiAoISh0ID0gXy50cnlzLCB0ID0gdC5sZW5ndGggPiAwICYmIHRbdC5sZW5ndGggLSAxXSkgJiYgKG9wWzBdID09PSA2IHx8IG9wWzBdID09PSAyKSkgeyBfID0gMDsgY29udGludWU7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSAzICYmICghdCB8fCAob3BbMV0gPiB0WzBdICYmIG9wWzFdIDwgdFszXSkpKSB7IF8ubGFiZWwgPSBvcFsxXTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSA2ICYmIF8ubGFiZWwgPCB0WzFdKSB7IF8ubGFiZWwgPSB0WzFdOyB0ID0gb3A7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0ICYmIF8ubGFiZWwgPCB0WzJdKSB7IF8ubGFiZWwgPSB0WzJdOyBfLm9wcy5wdXNoKG9wKTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHRbMl0pIF8ub3BzLnBvcCgpO1xuICAgICAgICAgICAgICAgICAgICBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgb3AgPSBib2R5LmNhbGwodGhpc0FyZywgXyk7XG4gICAgICAgIH0gY2F0Y2ggKGUpIHsgb3AgPSBbNiwgZV07IHkgPSAwOyB9IGZpbmFsbHkgeyBmID0gdCA9IDA7IH1cbiAgICAgICAgaWYgKG9wWzBdICYgNSkgdGhyb3cgb3BbMV07IHJldHVybiB7IHZhbHVlOiBvcFswXSA/IG9wWzFdIDogdm9pZCAwLCBkb25lOiB0cnVlIH07XG4gICAgfVxufTtcbmltcG9ydCB7IGRlZmluZUNvbmZpZyB9IGZyb20gJ3ZpdGUnO1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0JztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGRvdGVudiwgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCdkb3RlbnYnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZG90ZW52ID0gX2Euc2VudCgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRvdGVudi5jb25maWcoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJy4vc2VydmVyL2FwcC5qcycpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMjpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBleHByZXNzQXBwID0gKF9hLnNlbnQoKSkuZGVmYXVsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAgPSBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNlcnZlci5taWRkbGV3YXJlcy51c2UoJy9hcGknLCBmdW5jdGlvbiAocmVxLCByZXMsIG5leHQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIF9hO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBcdTI3MDUgUGF0Y2ggU1NFIHJvdXRlcyB0byBmbHVzaCBpbW1lZGlhdGVseSBcdTIwMTQgcHJldmVudHMgVml0ZSBidWZmZXJpbmcgdG9rZW5zXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmICgoX2EgPSByZXEudXJsKSA9PT0gbnVsbCB8fCBfYSA9PT0gdm9pZCAwID8gdm9pZCAwIDogX2Euc3RhcnRzV2l0aCgnL2NoYXQnKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLnNldEhlYWRlcignWC1BY2NlbC1CdWZmZXJpbmcnLCAnbm8nKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciBvcmlnaW5hbFdyaXRlXzEgPSByZXMud3JpdGUuYmluZChyZXMpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLndyaXRlID0gZnVuY3Rpb24gKGNodW5rKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIHJlc3VsdCA9IG9yaWdpbmFsV3JpdGVfMShjaHVuayk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKHR5cGVvZiByZXMuZmx1c2ggPT09ICdmdW5jdGlvbicpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy5mbHVzaCgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiByZXN1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcbiAgICBwbHVnaW5zOiBbcmVhY3QoKSwgZXhwcmVzc1BsdWdpbigpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxhQUFhLFFBQVEseUJBQXlCLGNBQWMsUUFBUSxLQUFLLFdBQVc7QUFDMVcsU0FBUyxtQ0FBbUM7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Q2xELEtBQUs7QUFDRCwyQkFBYyxHQUFHLEtBQUssRUFBRztBQUN6QixjQUFBRCxPQUFNO0FBQ04scUJBQU8sWUFBWSxJQUFJLFFBQVEsU0FBVSxLQUFLLEtBQUssTUFBTTtBQUNyRCxvQkFBSUU7QUFFSixxQkFBS0EsTUFBSyxJQUFJLFNBQVMsUUFBUUEsUUFBTyxTQUFTLFNBQVNBLElBQUcsV0FBVyxPQUFPLEdBQUc7QUFDNUUsc0JBQUksVUFBVSxxQkFBcUIsSUFBSTtBQUN2QyxzQkFBSSxrQkFBa0IsSUFBSSxNQUFNLEtBQUssR0FBRztBQUN4QyxzQkFBSSxRQUFRLFNBQVUsT0FBTztBQUN6Qix3QkFBSSxTQUFTLGdCQUFnQixLQUFLO0FBQ2xDLHdCQUFJLE9BQU8sSUFBSSxVQUFVO0FBQ3JCLDBCQUFJLE1BQU07QUFDZCwyQkFBTztBQUFBLGtCQUNYO0FBQUEsZ0JBQ0o7QUFDQSxnQkFBQUYsS0FBSSxLQUFLLEtBQUssSUFBSTtBQUFBLGNBQ3RCLENBQUM7QUFDRCxxQkFBTztBQUFBLGdCQUFDO0FBQUE7QUFBQSxjQUFZO0FBQUEsVUFDNUI7QUFBQSxRQUNKLENBQUM7QUFBQSxNQUNMLENBQUM7QUFBQSxJQUNMO0FBQUEsRUFDSjtBQUNKO0FBQ0EsSUFBTyxzQkFBUSxhQUFhO0FBQUEsRUFDeEIsU0FBUyxDQUFDLE1BQU0sR0FBRyxjQUFjLENBQUM7QUFBQSxFQUNsQyxTQUFTO0FBQUEsSUFDTCxPQUFPO0FBQUEsTUFDSCxLQUFLRixNQUFLLFFBQVFELFlBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJ1dWlkdjQiLCAiUm91dGVyIiwgInBhdGgiLCAidXVpZHY0IiwgIkJBVENIX1NJWkUiLCAicm91dGVyIiwgInV1aWR2NCIsICJHb29nbGVHZW5BSSIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCIsICJfX2Rpcm5hbWUiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgImFwcCIsICJkb3RlbnYiLCAiX2EiXQp9Cg==
