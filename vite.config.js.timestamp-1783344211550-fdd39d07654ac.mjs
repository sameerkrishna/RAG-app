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
        Knn({ query: queryEmbedding, returnRank: true, limit: 20 }),
        Knn({ query: queryText, key: "sparse_bm25", returnRank: true, limit: 20 })
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
function createAIClient() {
  const project = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || "project-d48e2f39-2685-4746-aa0";
  const location = process.env.GOOGLE_CLOUD_LOCATION || "us-central1";
  const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;
  if (credentialsJson) {
    try {
      const credentials = JSON.parse(credentialsJson);
      return new GoogleGenAI({
        vertexai: true,
        project,
        location,
        credentials
      });
    } catch (e) {
      console.warn("Failed to parse GOOGLE_CREDENTIALS_JSON, falling back to default auth");
    }
  }
  return new GoogleGenAI({
    vertexai: true,
    project,
    location
  });
}
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
    ai = createAIClient();
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
    const tEmbedStart = performance.now();
    let tEmbedEnd;
    const [queryEmbedding, { collection }] = await Promise.all([
      embedQuery(query).then((result) => {
        tEmbedEnd = performance.now();
        return result;
      }),
      getCollection()
    ]);
    const embeddingMs = tEmbedEnd - tEmbedStart;
    if (!collection) {
      console.warn(`\u26A0\uFE0F  No collection available`);
      return { results: [], coverage: { confidence: 0, topScore: 0, level: "low", score: 0 }, queryEmbedding, timings: { embeddingMs, retrievalMs: 0 } };
    }
    const where = sessionId ? { session_id: { "$in": ["global", sessionId] } } : { session_id: "global" };
    const tRetrievalStart = performance.now();
    const rawResults = await hybridQueryCollection(collection, query, queryEmbedding, topK, where);
    const retrievalMs = performance.now() - tRetrievalStart;
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
      queryEmbedding,
      timings: { embeddingMs, retrievalMs }
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
    const project = process.env.GOOGLE_CLOUD_PROJECT || "project-d48e2f39-2685-4746-aa0";
    const location = "global";
    const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;
    if (credentialsJson) {
      try {
        const credentials = JSON.parse(credentialsJson);
        genAI = new GoogleGenAI2({
          vertexai: true,
          project,
          location,
          credentials
        });
        return genAI;
      } catch (e) {
        console.warn("Failed to parse GOOGLE_CREDENTIALS_JSON, falling back to default auth");
      }
    }
    genAI = new GoogleGenAI2({
      vertexai: true,
      project,
      location
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
function sanitizeNullBytes(val) {
  if (typeof val === "string") {
    return val.replace(/\u0000/g, "");
  }
  if (Array.isArray(val)) {
    return val.map(sanitizeNullBytes);
  }
  if (val !== null && typeof val === "object") {
    const cleanObj = {};
    for (const key of Object.keys(val)) {
      cleanObj[key] = sanitizeNullBytes(val[key]);
    }
    return cleanObj;
  }
  return val;
}
function insertConversationAsync(sessionId, data) {
  const previousPromise = sessionInsertPromises.get(sessionId) || Promise.resolve();
  const nextPromise = previousPromise.then(async () => {
    const cleanData = sanitizeNullBytes(data);
    console.log(`[Supabase] Inserting conversation for session ${sessionId}, answer_key: ${cleanData.answer_key}`);
    const { error } = await supabase.from("Conversation_History").insert(cleanData);
    if (error) {
      console.error("[Supabase] Error inserting conversation history:", error);
    } else {
      console.log(`[Supabase] Successfully inserted conversation for session ${sessionId}`);
    }
  }).catch((err) => {
    console.error("[Supabase] Unexpected error during insertion chain:", err);
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
    const tQueryStart = performance.now();
    sendEvent("status", { stage: "retrieving", message: "Searching knowledge base..." });
    const expandedQuery = expandQuery(query);
    const { results, coverage, timings } = await retrieveForQuery(expandedQuery, sessionId, { topK: 5 });
    const tChunksReceived = performance.now();
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
    let isFirstToken = true;
    let tFirstToken;
    const tLlmStart = performance.now();
    for await (const chunk of streamResponse(prompt)) {
      if (chunk.type === "token") {
        if (isFirstToken) {
          tFirstToken = performance.now();
          isFirstToken = false;
        }
        fullResponse += chunk.text;
        sendEvent("token", { text: chunk.text });
      } else if (chunk.type === "error") {
        sendEvent("error", { message: chunk.error, code: "LLM_ERROR" });
      } else if (chunk.type === "complete") {
        fullResponse = chunk.response;
      }
    }
    const metric1_queryToEmbedding = tChunksReceived - tQueryStart - (timings?.retrievalMs || 0);
    const metric2_embeddingToChunks = timings?.retrievalMs || 0;
    const metric3_chunksToFirstToken = tFirstToken ? tFirstToken - tChunksReceived : -1;
    const metric4_promptToFirstToken = tFirstToken ? tFirstToken - tLlmStart : -1;
    const metric5_queryToFirstToken = tFirstToken ? tFirstToken - tQueryStart : -1;
    console.log("\n\u250C\u2500\u2500\u2500 \u23F1  Performance Metrics \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510");
    console.log(`\u2502  1. Query \u2192 Embedding response  : ${metric1_queryToEmbedding.toFixed(0)} ms`);
    console.log(`\u2502  2. Embedding \u2192 Chunks retrieved: ${metric2_embeddingToChunks.toFixed(0)} ms`);
    console.log(`\u2502  3. Chunks \u2192 First LLM token    : ${metric3_chunksToFirstToken >= 0 ? metric3_chunksToFirstToken.toFixed(0) + " ms" : "N/A"}`);
    console.log(`\u2502  4. API Call                    : ${metric4_promptToFirstToken >= 0 ? metric4_promptToFirstToken.toFixed(0) + " ms" : "N/A"}`);
    console.log(`\u2502  5. Query sent \u2192 First token    : ${metric5_queryToFirstToken >= 0 ? metric5_queryToFirstToken.toFixed(0) + " ms" : "N/A"}`);
    console.log("\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n");
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
      origin: true,
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
import fs2 from "fs";
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
function copyNetlifyFiles() {
  return {
    name: "copy-netlify-files",
    closeBundle: function() {
      var redirectsSrc = path3.resolve(__dirname2, "dist/_redirects");
      if (fs2.existsSync(redirectsSrc)) {
        console.log("\u2705 _redirects exists in dist");
      }
      var netlifyToml = path3.resolve(__dirname2, "netlify.toml");
      var netlifyTomlDest = path3.resolve(__dirname2, "dist/netlify.toml");
      if (fs2.existsSync(netlifyToml)) {
        fs2.copyFileSync(netlifyToml, netlifyTomlDest);
        console.log("\u2705 netlify.toml copied to dist");
      }
    }
  };
}
var vite_config_default = defineConfig({
  plugins: [react(), expressPlugin(), copyNetlifyFiles()],
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9jaGF0LmpzIiwgInNlcnZlci9hcGkvZmVlZGJhY2suanMiLCAic2VydmVyL2FwcC5qcyIsICJ2aXRlLmNvbmZpZy5qcyJdLAogICJzb3VyY2VzQ29udGVudCI6IFsiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanNcIjtpbXBvcnQgeyBDbG91ZENsaWVudCwgU2NoZW1hLCBTcGFyc2VWZWN0b3JJbmRleENvbmZpZywgRE9DVU1FTlRfS0VZLCBTZWFyY2gsIEtubiwgUnJmIH0gZnJvbSAnY2hyb21hZGInO1xuaW1wb3J0IHsgQ2hyb21hQm0yNUVtYmVkZGluZ0Z1bmN0aW9uIH0gZnJvbSAnQGNocm9tYS1jb3JlL2Nocm9tYS1ibTI1JztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuXG4vLyBcdTI1MDBcdTI1MDAgU2hhcmVkIHNjaGVtYTogZGVuc2UgZW1iZWRkaW5ncyAobWFuYWdlZCBleHRlcm5hbGx5KSArIEJNMjUgc3BhcnNlIGluZGV4IFx1MjUwMFx1MjUwMFxuY29uc3QgYm0yNUVtYmVkZGluZ0Z1bmN0aW9uID0gbmV3IENocm9tYUJtMjVFbWJlZGRpbmdGdW5jdGlvbigpO1xuY29uc3QgY29sbGVjdGlvblNjaGVtYSA9IG5ldyBTY2hlbWEoKS5jcmVhdGVJbmRleChcbiAgbmV3IFNwYXJzZVZlY3RvckluZGV4Q29uZmlnKHtcbiAgICBlbWJlZGRpbmdGdW5jdGlvbjogYm0yNUVtYmVkZGluZ0Z1bmN0aW9uLFxuICAgIHNvdXJjZUtleTogRE9DVU1FTlRfS0VZLFxuICAgIGJtMjU6IHRydWVcbiAgfSksXG4gICdzcGFyc2VfYm0yNSdcbik7XG5cbmxldCBjbG91ZENsaWVudCA9IG51bGw7XG5sZXQgZ2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldENsb3VkQ2xpZW50KCkge1xuICBpZiAoIWNsb3VkQ2xpZW50KSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0FQSV9LRVk7XG4gICAgY29uc3QgdGVuYW50ID0gcHJvY2Vzcy5lbnYuQ0hST01BX1RFTkFOVCB8fCAnZGVmYXVsdF90ZW5hbnQnO1xuICAgIGNvbnN0IGRhdGFiYXNlID0gcHJvY2Vzcy5lbnYuQ0hST01BX0RBVEFCQVNFIHx8ICdkZWZhdWx0X2RhdGFiYXNlJztcbiAgICBjb25zdCBob3N0ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0hPU1QgfHwgdW5kZWZpbmVkO1xuXG4gICAgY29uc29sZS5sb2coXCItLS0tIENIUk9NQSBDT05ORUNUSVZJVFkgREVCVUcgLS0tLVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIkhvc3Q6ICAgICAgXCIsIGhvc3QgfHwgXCJhcGkudHJ5Y2hyb21hLmNvbSAoZGVmYXVsdClcIik7XG4gICAgY29uc29sZS5sb2coXCJUZW5hbnQ6ICAgIFwiLCB0ZW5hbnQpO1xuICAgIGNvbnNvbGUubG9nKFwiREIgTmFtZTogICBcIiwgZGF0YWJhc2UpO1xuICAgIGNvbnNvbGUubG9nKFwiQVBJIEtleTogICBcIiwgYXBpS2V5ID8gXCJMT0FERUQgKFZBTElEKVwiIDogXCJNSVNTSU5HIChVTkRFRklORUQpXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cIik7XG5cbiAgICBpZiAoIWFwaUtleSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBcIkNSSVRJQ0FMIEVSUk9SOiBDSFJPTUFfQVBJX0tFWSBpcyB1bmRlZmluZWQuIFwiICtcbiAgICAgICAgXCJFbnN1cmUgeW91ciBlbnZpcm9ubWVudCB2YXJpYWJsZXMgYXJlIGNvcnJlY3RseSBsb2FkZWQgYmVmb3JlIGV4ZWN1dGluZyB0aGlzIGZpbGUuXCJcbiAgICAgICk7XG4gICAgfVxuXG4gICAgY29uc3QgY2xpZW50T3B0aW9ucyA9IHsgYXBpS2V5LCB0ZW5hbnQsIGRhdGFiYXNlIH07XG4gICAgaWYgKGhvc3QpIGNsaWVudE9wdGlvbnMuaG9zdCA9IGhvc3Q7XG4gICAgY2xvdWRDbGllbnQgPSBuZXcgQ2xvdWRDbGllbnQoY2xpZW50T3B0aW9ucyk7XG4gIH1cbiAgcmV0dXJuIGNsb3VkQ2xpZW50O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0R2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFnbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IHByb2Nlc3MuZW52LkNIUk9NQV9HTE9CQUxfQ09MTEVDVElPTiB8fCAnc2VlZF9kYic7XG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIHNjaGVtYTogY29sbGVjdGlvblNjaGVtYSxcbiAgICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgICBkZXNjcmlwdGlvbjogJ1Blcm1hbmVudCBzZWVkIGRvY3VtZW50cyBmb3IgUkFHJyxcbiAgICAgICAgICB0eXBlOiAnZ2xvYmFsX2tub3dsZWRnZSdcbiAgICAgICAgfSxcbiAgICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYFxcdTI3MDUgR2xvYmFsIGNvbGxlY3Rpb24gcmVhZHk6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjb25uZWN0IHRvIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHRoZSBzaW5nbGUgc2hhcmVkIGNvbGxlY3Rpb24uXG4gKiBEcm9wLWluIHJlcGxhY2VtZW50IGZvciB0aGUgb2xkIGdldFNlc3Npb25Db2xsZWN0aW9uIFx1MjAxNCBjYWxsZXJzIHRoYXRcbiAqIHByZXZpb3VzbHkgZGVzdHJ1Y3R1cmVkIHsgY29sbGVjdGlvbiB9IHdpbGwgc3RpbGwgd29yay5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldENvbGxlY3Rpb24oKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIHJldHVybiB7IGNvbGxlY3Rpb24sIGlzTmV3OiBmYWxzZSB9O1xufVxuXG4vKipcbiAqIEFkZCB2ZWN0b3JzIGluIGJhdGNoZXMgb2YgQkFUQ0hfU0laRSB0byBhdm9pZCBDaHJvbWEgcGF5bG9hZCBsaW1pdHMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBhZGRWZWN0b3JzKGNvbGxlY3Rpb24sIHZlY3RvcnMsIGVtYmVkZGluZ3MsIGlkcykge1xuICB0cnkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgaWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBjb25zdCBiYXRjaElkcyA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi50ZXh0KTtcbiAgICAgIGNvbnN0IGJhdGNoTWV0YWRhdGFzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi5tZXRhZGF0YSk7XG5cbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uYWRkKHtcbiAgICAgICAgaWRzOiBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6IGJhdGNoRG9jdW1lbnRzLFxuICAgICAgICBtZXRhZGF0YXM6IGJhdGNoTWV0YWRhdGFzXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFthZGRWZWN0b3JzXSBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IGFkZGVkICR7YmF0Y2hJZHMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gICAgfVxuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBhZGQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSyA9IDUsIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcXVlcnlPcHRzID0ge1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9O1xuICAgIGlmICh3aGVyZSkgcXVlcnlPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5xdWVyeShxdWVyeU9wdHMpO1xuXG4gICAgaWYgKCFyZXN1bHRzLmlkcyB8fCByZXN1bHRzLmlkcy5sZW5ndGggPT09IDAgfHwgcmVzdWx0cy5pZHNbMF0ubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdHMuaWRzWzBdLm1hcCgoaWQsIGlkeCkgPT4gKHtcbiAgICAgIGlkLFxuICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHNbMF1baWR4XSxcbiAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhc1swXVtpZHhdLFxuICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF0sXG4gICAgICBzY29yZTogMSAtIHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF1cbiAgICB9KSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIHF1ZXJ5IGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogSHlicmlkIHNlYXJjaCB1c2luZyBDaHJvbWEgQ2xvdWQgU2VhcmNoIEFQSSB3aXRoIFJSRiAoZGVuc2UgKyBzcGFyc2UgQk0yNSkuXG4gKiBSZXR1cm5zIHJlc3VsdHMgaW4gdGhlIHNhbWUgc2hhcGUgYXMgcXVlcnlDb2xsZWN0aW9uKCkgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHkuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcgKGUuZy4gc2Vzc2lvbl9pZCAkaW4pLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaHlicmlkUXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5VGV4dCwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1LCB3aGVyZSA9IHVuZGVmaW5lZCkge1xuICB0cnkge1xuICAgIGxldCBzZWFyY2ggPSBuZXcgU2VhcmNoKClcbiAgICAgIC5yYW5rKFJyZih7XG4gICAgICAgIHJhbmtzOiBbXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5RW1iZWRkaW5nLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMjAgfSksXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5VGV4dCwga2V5OiAnc3BhcnNlX2JtMjUnLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMjAgfSlcbiAgICAgICAgXSxcbiAgICAgICAgd2VpZ2h0czogWzAuOSwgMC4xXSxcbiAgICAgICAgazogNjBcbiAgICAgIH0pKVxuICAgICAgLndoZXJlKHdoZXJlKVxuICAgICAgLnNlbGVjdChcIiNkb2N1bWVudFwiLCBcIiNtZXRhZGF0YVwiLCBcIiNzY29yZVwiKVxuICAgICAgLmxpbWl0KHRvcEspO1xuXG4gICAgY29uc3QgcmF3ID0gYXdhaXQgY29sbGVjdGlvbi5zZWFyY2goc2VhcmNoKTtcblxuICAgIC8vIFBhcmFsbGVsXHUyMDExYXJyYXkgc3RydWN0dXJlOiBpZHNbMF0sIGRvY3VtZW50c1swXSwgbWV0YWRhdGFzWzBdLCBzY29yZXNbMF1cbiAgICBpZiAoIXJhdy5pZHMgfHwgIXJhdy5pZHNbMF0gfHwgcmF3Lmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICBjb25zdCBpZHMgPSByYXcuaWRzWzBdO1xuICAgIGNvbnN0IGRvY3MgPSByYXcuZG9jdW1lbnRzPy5bMF0gPz8gW107XG4gICAgY29uc3QgbWV0YXMgPSByYXcubWV0YWRhdGFzPy5bMF0gPz8gW107XG4gICAgY29uc3Qgc2NvcmVzID0gcmF3LnNjb3Jlcz8uWzBdID8/IFtdO1xuXG4gICAgLy8gMS4gRGVmaW5lIGdsb2JhbCBSUkYgYm91bmRzIGJhc2VkIG9uIHlvdXIgd2VpZ2h0cyBbMC43LCAwLjNdIGFuZCBsaW1pdHMgKDEwMClcbiAgICAvLyBNYXggcG9zc2libGUgcmF3IFJSRjogMSAvICg2MCArIDEpID0gMC4wMTYzOTM0XG4gICAgLy8gTWluIHBvc3NpYmxlIHJhdyBSUkY6IDEgLyAoNjAgKyAxMDApID0gMC4wMDYyNTAwXG4gICAgY29uc3QgTUFYX1JSRiA9IDEgLyA2MTtcbiAgICBjb25zdCBNSU5fUlJGID0gMSAvIDE2MDtcblxuICAgIHJldHVybiBpZHMubWFwKChpZCwgaWR4KSA9PiB7XG4gICAgICAvLyBDaHJvbWEgcmV0dXJucyBuZWdhdGl2ZSB2YWx1ZXMgKGUuZy4gLTAuMDE2MzkpLCBjb252ZXJ0IHRvIHBvc2l0aXZlIHJhdyBSUkZcbiAgICAgIGNvbnN0IHJhd1JSRiA9IE1hdGguYWJzKHNjb3Jlc1tpZHhdID8/IE1JTl9SUkYpO1xuXG4gICAgICAvLyAyLiBMaW5lYXIgbWluLW1heCBub3JtYWxpemF0aW9uIHRvIGZpdCBwZXJmZWN0bHkgYmV0d2VlbiAwLjAgYW5kIDEuMFxuICAgICAgbGV0IG5vcm1hbGl6ZWRTY29yZSA9IChyYXdSUkYgLSBNSU5fUlJGKSAvIChNQVhfUlJGIC0gTUlOX1JSRik7XG5cbiAgICAgIC8vIEJvdW5kYXJ5IHByb3RlY3Rpb25cbiAgICAgIG5vcm1hbGl6ZWRTY29yZSA9IE1hdGgubWF4KDAsIE1hdGgubWluKDEsIG5vcm1hbGl6ZWRTY29yZSkpO1xuXG4gICAgICAvL2NvbnN0IGZpbmFsU2NvcmUgPSBNYXRoLnJvdW5kKG5vcm1hbGl6ZWRTY29yZSAqIDEwMCkgLyAxMDA7XG5cbiAgICAgIHJldHVybiB7XG4gICAgICAgIGlkLFxuICAgICAgICB0ZXh0OiBkb2NzW2lkeF0gPz8gJycsXG4gICAgICAgIG1ldGFkYXRhOiBtZXRhc1tpZHhdID8/IHt9LFxuICAgICAgICBkaXN0YW5jZTogMSAtIG5vcm1hbGl6ZWRTY29yZSxcbiAgICAgICAgc2NvcmU6IG5vcm1hbGl6ZWRTY29yZVxuICAgICAgfTtcbiAgICB9KTtcblxuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignSHlicmlkIHF1ZXJ5IGZhaWxlZCwgZmFsbGluZyBiYWNrIHRvIGRlbnNlLW9ubHk6JywgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gR3JhY2VmdWwgZmFsbGJhY2sgdG8gZGVuc2Utb25seSBzZWFyY2ggZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICByZXR1cm4gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLLCB3aGVyZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPmRlZmF1bHQgMTAwIGxpbWl0KSBhcmUgZnVsbHkgZGVsZXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYWxsSWRzID0gW107XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICAgIHdoZXJlOiB7IGRvY3VtZW50X2lkOiBkb2N1bWVudElkIH0sXG4gICAgICAgIGluY2x1ZGU6IFtdLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcblxuICAgICAgaWYgKCFiYXRjaC5pZHMgfHwgYmF0Y2guaWRzLmxlbmd0aCA9PT0gMCkgYnJlYWs7XG4gICAgICBhbGxJZHMucHVzaCguLi5iYXRjaC5pZHMpO1xuXG4gICAgICBpZiAoYmF0Y2guaWRzLmxlbmd0aCA8IEJBVENIX1NJWkUpIGJyZWFrO1xuICAgICAgb2Zmc2V0ICs9IEJBVENIX1NJWkU7XG4gICAgfVxuXG4gICAgaWYgKGFsbElkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7IGlkczogYWxsSWRzIH0pO1xuICAgIH1cbiAgICByZXR1cm4gYWxsSWRzLmxlbmd0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogRGVsZXRlIGFsbCB2ZWN0b3JzIGJlbG9uZ2luZyB0byBhIHNwZWNpZmljIHNlc3Npb24uXG4gKiBVc2VzIHNlc3Npb25faWQgbWV0YWRhdGEgZmlsdGVyIHRvIGZpbmQgYW5kIHJlbW92ZSB0aGVtIGluIGJhdGNoZXMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVTZXNzaW9uVmVjdG9ycyhzZXNzaW9uSWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBzZXNzaW9uX2lkOiBzZXNzaW9uSWQgfSxcbiAgICAgICAgaW5jbHVkZTogW10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBhbGxJZHMgfSk7XG4gICAgfVxuICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IERlbGV0ZWQgJHthbGxJZHMubGVuZ3RofSBzZXNzaW9uIHZlY3RvcnMgZm9yIHNlc3Npb25faWQ9JHtzZXNzaW9uSWR9YCk7XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGRlbGV0ZSBzZXNzaW9uIHZlY3RvcnMgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuLyoqXG4gKiBMaXN0IGFsbCB1bmlxdWUgZG9jdW1lbnRzIGluIGEgY29sbGVjdGlvbi5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIHdpdGggQkFUQ0hfU0laRT0zMDAgc28gY29sbGVjdGlvbnMgbGFyZ2VyXG4gKiB0aGFuIENocm9tYSdzIGRlZmF1bHQgZ2V0KCkgbGltaXQgKDEwMCkgYXJlIGZ1bGx5IGVudW1lcmF0ZWQuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZG9jdW1lbnRzTWFwID0gbmV3IE1hcCgpO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGdldE9wdHMgPSB7XG4gICAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9O1xuICAgICAgaWYgKHdoZXJlKSBnZXRPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoZ2V0T3B0cyk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuXG4gICAgICBiYXRjaC5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYmF0Y2gubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYmF0Y2guZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuXG4gICAgICBjb25zb2xlLmxvZyhgICBbbGlzdERvY3VtZW50c10gb2Zmc2V0PSR7b2Zmc2V0fSwgZ290PSR7YmF0Y2guaWRzLmxlbmd0aH0sIHVuaXF1ZSBzbyBmYXI9JHtkb2N1bWVudHNNYXAuc2l6ZX1gKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7aW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5cbmNvbnN0IENIQVJTX1BFUl9UT0tFTiA9IDQ7XG5jb25zdCBUQVJHRVRfQ0hVTktfVE9LRU5TID0gNjAwOyAgIC8vIHNvZnQgdGFyZ2V0IHBlciBjaHVua1xuY29uc3QgTUFYX0NIVU5LX1RPS0VOUyA9IDc1MDsgICAvLyBoYXJkIGNhcCBiZWZvcmUgZm9yY2VkIHNwbGl0XG5jb25zdCBPVkVSTEFQX1RPS0VOUyA9IDEwMDsgICAvLyBvdmVybGFwIG9ubHkgb24gb3ZlcnNpemVkIHBhcmFncmFwaHNcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuLy8gTWF0Y2hlcyBBTEwtQ0FQUyBoZWFkaW5ncywgbWFya2Rvd24gaGVhZGluZ3MsIG9yIG51bWJlcmVkIHNlY3Rpb24gaGVhZGluZ3NcbmNvbnN0IEhFQURJTkdfUkUgPSAvXig/OltBLVpdW0EtWlxcc117Miw2MH0kfCN7MSw0fVxccy4rfCg/OlxcZCtcXC4pK1xccy4rKS9tO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblRleHQodGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gJyc7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoL1xcZi9nLCAnXFxuJylcbiAgICAucmVwbGFjZSgvKFxccypcXG4pezMsfS9nLCAnXFxuXFxuJylcbiAgICAucmVwbGFjZSgvXlxccypcXGQrXFxzKiQvZ20sICcnKVxuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuLyoqXG4gKiBHaXZlbiBhIHJhdyAocG9zc2libHkgbWlkLXdvcmQpIG9mZnNldCwgc25hcCBmb3J3YXJkIHRvIHRoZSBuZWFyZXN0XG4gKiBjbGVhbiBzZW50ZW5jZSBzdGFydCwgZmFsbGluZyBiYWNrIHRvIHRoZSBuZWFyZXN0IHdvcmQgYm91bmRhcnksXG4gKiBzbyBvdmVybGFwcGVkIGNodW5rcyBuZXZlciBiZWdpbiBtaWQtc2VudGVuY2Ugb3IgbWlkLXdvcmQuXG4gKlxuICogQHBhcmFtIHtzdHJpbmd9IHRleHQgICAgICAgdGhlIHBhcmFncmFwaC90ZXh0IGJlaW5nIHdpbmRvd2VkXG4gKiBAcGFyYW0ge251bWJlcn0gcmF3T2Zmc2V0ICB0aGUgcmF3ICh1bnNuYXBwZWQpIHN0YXJ0IG9mZnNldCBmb3IgdGhlIG5leHQgd2luZG93XG4gKiBAcGFyYW0ge251bWJlcn0gaGFyZExpbWl0ICBkb24ndCBzZWFyY2ggcGFzdCB0aGlzIG9mZnNldCAoZW5kIG9mIHByZXZpb3VzIHdpbmRvdylcbiAqL1xuZnVuY3Rpb24gc25hcFRvQm91bmRhcnkodGV4dCwgcmF3T2Zmc2V0LCBoYXJkTGltaXQpIHtcbiAgaWYgKHJhd09mZnNldCA8PSAwKSByZXR1cm4gMDtcblxuICAvLyBQcmVmZXIgYSByZWFsIHNlbnRlbmNlIGJvdW5kYXJ5IHdpdGhpbiBhIHNtYWxsIGZvcndhcmQgd2luZG93XG4gIGNvbnN0IHNlYXJjaFdpbmRvd0VuZCA9IE1hdGgubWluKHJhd09mZnNldCArIDgwLCBoYXJkTGltaXQpOyAvLyB+ODAgY2hhcnMgXHUyMjQ4IG9uZSBzZW50ZW5jZVxuICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgIGNvbnN0IGlkeCA9IHRleHQuaW5kZXhPZihicCwgcmF3T2Zmc2V0KTtcbiAgICBpZiAoaWR4ICE9PSAtMSAmJiBpZHggPCBzZWFyY2hXaW5kb3dFbmQpIHtcbiAgICAgIHJldHVybiBpZHggKyBicC5sZW5ndGg7XG4gICAgfVxuICB9XG5cbiAgLy8gRmFsbCBiYWNrOiBzbmFwIHRvIHRoZSBuZXh0IHdvcmQgYm91bmRhcnkgc28gd2UgYXQgbGVhc3QgZG9uJ3RcbiAgLy8gc3BsaXQgYSB3b3JkIGluIGhhbGZcbiAgY29uc3Qgc3BhY2VJZHggPSB0ZXh0LmluZGV4T2YoJyAnLCByYXdPZmZzZXQpO1xuICBpZiAoc3BhY2VJZHggIT09IC0xICYmIHNwYWNlSWR4IDwgc2VhcmNoV2luZG93RW5kKSB7XG4gICAgcmV0dXJuIHNwYWNlSWR4ICsgMTtcbiAgfVxuXG4gIC8vIExhc3QgcmVzb3J0OiBpZiB0aGUgY3VycmVudCBwb3NpdGlvbiBpcyBhbHJlYWR5IG1pZC13b3JkLFxuICAvLyB3YWxrIGJhY2t3YXJkIHRvIHRoZSBsYXN0IHNwYWNlIGJlZm9yZSBpdFxuICBsZXQgaSA9IHJhd09mZnNldDtcbiAgd2hpbGUgKGkgPiAwICYmICEvXFxzLy50ZXN0KHRleHRbaSAtIDFdKSkgaS0tO1xuICByZXR1cm4gaSA+IDAgPyBpIDogcmF3T2Zmc2V0O1xufVxuXG4vKipcbiAqIFN0cnVjdHVyZS1hd2FyZSBjaHVua2luZzpcbiAqICAxLiBTcGxpdCBvbiBibGFuayBsaW5lcyAoXFxuXFxuKSBpbnRvIHBhcmFncmFwaHMuXG4gKiAgMi4gQSBsaW5lIG1hdGNoaW5nIEhFQURJTkdfUkUgYWx3YXlzIHN0YXJ0cyBhIGZyZXNoIGNodW5rLlxuICogIDMuIEFjY3VtdWxhdGUgcGFyYWdyYXBocyB1bnRpbCB0aGUgc29mdCBUQVJHRVQgaXMgcmVhY2hlZCwgdGhlbiBmbHVzaC5cbiAqICA0LiBQYXJhZ3JhcGhzIGxhcmdlciB0aGFuIE1BWCBhcmUgc3BsaXQgd2l0aCBhIHNsaWRpbmcgd2luZG93ICsgb3ZlcmxhcCBhcyBmYWxsYmFjay5cbiAqICAgICBCb3RoIHdpbmRvdyBlbmRzIEFORCB3aW5kb3cgc3RhcnRzIGFyZSBzbmFwcGVkIHRvIHNlbnRlbmNlL3dvcmQgYm91bmRhcmllc1xuICogICAgIHNvIG5vIGNodW5rIGJlZ2lucyBvciBlbmRzIG1pZC13b3JkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0YXJnZXRUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBUQVJHRVRfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBtYXhUb2tlbnMgPSBvcHRpb25zLm1heENodW5rVG9rZW5zIHx8IE1BWF9DSFVOS19UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUayA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBPVkVSTEFQX1RPS0VOUztcblxuICBjb25zdCB0YXJnZXRDaGFycyA9IHRhcmdldFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3QgbWF4Q2hhcnMgPSBtYXhUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUayAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgLy8gMS4gU3BsaXQgaW50byBwYXJhZ3JhcGhzXG4gIGNvbnN0IHJhd1BhcmFzID0gdGV4dFxuICAgIC5zcGxpdCgvXFxuezIsfS8pXG4gICAgLm1hcChwID0+IHAudHJpbSgpKVxuICAgIC5maWx0ZXIocCA9PiBwLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpO1xuXG4gIGNvbnN0IGNodW5rcyA9IFtdO1xuICBsZXQgYnVmZmVyID0gJyc7XG4gIGxldCBidWZTdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcbiAgbGV0IGNoYXJDdXJzb3IgPSAwO1xuXG4gIGNvbnN0IGZsdXNoID0gKGZvcmNlVGV4dCkgPT4ge1xuICAgIGNvbnN0IGNvbnRlbnQgPSAoZm9yY2VUZXh0ID8/IGJ1ZmZlcikudHJpbSgpO1xuICAgIGlmIChjb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY29udGVudCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogYnVmU3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGJ1ZlN0YXJ0ICsgY29udGVudC5sZW5ndGgsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuICAgIGJ1ZmZlciA9ICcnO1xuICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgfTtcblxuICBmb3IgKGNvbnN0IHBhcmEgb2YgcmF3UGFyYXMpIHtcbiAgICBjb25zdCBpc0hlYWRpbmcgPSBIRUFESU5HX1JFLnRlc3QocGFyYS5zcGxpdCgnXFxuJylbMF0pO1xuXG4gICAgLy8gMi4gSGVhZGluZyBhbHdheXMgc3RhcnRzIGEgbmV3IGNodW5rXG4gICAgaWYgKGlzSGVhZGluZyAmJiBidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgIGlmIChwYXJhLmxlbmd0aCA+IG1heENoYXJzKSB7XG4gICAgICAvLyAzLiBPdmVyc2l6ZWQgcGFyYWdyYXBoIC0+IHNsaWRpbmctd2luZG93IGNoYXIgZmFsbGJhY2tcbiAgICAgIGlmIChidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgICAgbGV0IHMgPSAwO1xuICAgICAgd2hpbGUgKHMgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICBsZXQgZSA9IHMgKyB0YXJnZXRDaGFycztcbiAgICAgICAgaWYgKGUgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICAgIGNvbnN0IHNlYXJjaEZyb20gPSBlIC0gTWF0aC5mbG9vcih0YXJnZXRDaGFycyAqIDAuMik7XG4gICAgICAgICAgZm9yIChjb25zdCBicCBvZiBbJy4gJywgJy5cXG4nLCAnPyAnLCAnISAnLCAnXFxuJ10pIHtcbiAgICAgICAgICAgIGNvbnN0IGlkeCA9IHBhcmEubGFzdEluZGV4T2YoYnAsIGUpO1xuICAgICAgICAgICAgaWYgKGlkeCA+IHNlYXJjaEZyb20pIHsgZSA9IGlkeCArIGJwLmxlbmd0aDsgYnJlYWs7IH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgZSA9IE1hdGgubWluKGUsIHBhcmEubGVuZ3RoKTtcbiAgICAgICAgY29uc3Qgc2xpY2UgPSBwYXJhLnNsaWNlKHMsIGUpLnRyaW0oKTtcbiAgICAgICAgaWYgKHNsaWNlLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgICAgICB0ZXh0OiBzbGljZSxcbiAgICAgICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKHNsaWNlKSxcbiAgICAgICAgICAgIGNoYXJTdGFydDogY2hhckN1cnNvciArIHMsXG4gICAgICAgICAgICBjaGFyRW5kOiBjaGFyQ3Vyc29yICsgZSxcbiAgICAgICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKGUgPj0gcGFyYS5sZW5ndGgpIGJyZWFrO1xuXG4gICAgICAgIC8vIFNuYXAgdGhlIG92ZXJsYXBwZWQgc3RhcnQgZm9yd2FyZCB0byBhIGNsZWFuIHNlbnRlbmNlL3dvcmRcbiAgICAgICAgLy8gYm91bmRhcnkgaW5zdGVhZCBvZiB1c2luZyB0aGUgcmF3IG9mZnNldCwgd2hpY2ggY291bGQgbGFuZFxuICAgICAgICAvLyBtaWQtd29yZCAoZS5nLiBcInMgdGhhdCBhbiBFVEYuLi5cIikuXG4gICAgICAgIGNvbnN0IHJhd05leHQgPSBlIC0gb3ZlcmxhcENoYXJzO1xuICAgICAgICBzID0gcmF3TmV4dCA+IHMgPyBzbmFwVG9Cb3VuZGFyeShwYXJhLCByYXdOZXh0LCBlKSA6IGU7XG4gICAgICB9XG4gICAgICBjaGFyQ3Vyc29yICs9IHBhcmEubGVuZ3RoICsgMjtcbiAgICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgICAgIGNvbnRpbnVlO1xuICAgIH1cblxuICAgIC8vIDQuIE5vcm1hbCBwYXJhZ3JhcGggXHUyMDE0IGhhcmQgY2FwIGxvb2thaGVhZCBCRUZPUkUgYWNjdW11bGF0aW5nXG4gICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwICYmIChidWZmZXIubGVuZ3RoICsgcGFyYS5sZW5ndGggKyAyKSA+IG1heENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cblxuICAgIGJ1ZmZlciA9IGJ1ZmZlciA/IGJ1ZmZlciArICdcXG5cXG4nICsgcGFyYSA6IHBhcmE7XG4gICAgY2hhckN1cnNvciArPSBwYXJhLmxlbmd0aCArIDI7XG5cbiAgICAvLyBTb2Z0IGNhcDogZmx1c2ggb25jZSB0YXJnZXQgaXMgcmVhY2hlZFxuICAgIGlmIChidWZmZXIubGVuZ3RoID49IHRhcmdldENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDUuIEZsdXNoIHJlbWFpbmRlclxuICBmbHVzaCgpO1xuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQoY2xlYW5lZFRleHQsIG9wdGlvbnMpO1xuICBjb25zdCB0b3RhbENodW5rcyA9IHRleHRDaHVua3MubGVuZ3RoO1xuICBjb25zdCBzb3VyY2VUeXBlID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6IHNvdXJjZVR5cGUsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9O1xuICB9KTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIGNvbnN0IGxpbmVzID0gdGV4dC5zcGxpdCgnXFxuJykuZmlsdGVyKGwgPT4gbC50cmltKCkpO1xuICBpZiAobGluZXMubGVuZ3RoID4gMCkge1xuICAgIGNvbnN0IGZpcnN0TGluZSA9IGxpbmVzWzBdLnRyaW0oKTtcbiAgICBpZiAoZmlyc3RMaW5lLmxlbmd0aCA8IDEwMCAmJiAhZmlyc3RMaW5lLmVuZHNXaXRoKCcuJykpIHtcbiAgICAgIHJldHVybiBmaXJzdExpbmUuc2xpY2UoMCwgNTApO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBFbWJlZGRpbmdFcnJvciwgaXM0MjlFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMS4gU0xJRElORyBXSU5ET1cgUkFURSBMSU1JVEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNsYXNzIFNsaWRpbmdXaW5kb3dSYXRlTGltaXRlciB7XG4gIGNvbnN0cnVjdG9yKGxpbWl0UGVyTWludXRlKSB7XG4gICAgdGhpcy5saW1pdFBlck1pbnV0ZSA9IGxpbWl0UGVyTWludXRlO1xuICAgIHRoaXMud2luZG93TXMgPSA2MDAwMDtcbiAgICB0aGlzLnJlcXVlc3RzID0gW107XG4gIH1cblxuICBhc3luYyBjb25zdW1lKHRva2Vucykge1xuICAgIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gICAgLy8gUmVtb3ZlIGVudHJpZXMgb2xkZXIgdGhhbiA2MCBzZWNvbmRzXG4gICAgdGhpcy5yZXF1ZXN0cyA9IHRoaXMucmVxdWVzdHMuZmlsdGVyKHJlcSA9PiByZXEudGltZXN0YW1wID4gbm93IC0gdGhpcy53aW5kb3dNcyk7XG5cbiAgICBjb25zdCBjdXJyZW50VG90YWwgPSB0aGlzLnJlcXVlc3RzLnJlZHVjZSgoc3VtLCByZXEpID0+IHN1bSArIHJlcS50b2tlbnMsIDApO1xuXG4gICAgLy8gSWYgd2UgaGF2ZSByb29tLCBjb25zdW1lIGluc3RhbnRseSAoYnVyc3QpXG4gICAgaWYgKGN1cnJlbnRUb3RhbCArIHRva2VucyA8PSB0aGlzLmxpbWl0UGVyTWludXRlKSB7XG4gICAgICB0aGlzLnJlcXVlc3RzLnB1c2goeyB0aW1lc3RhbXA6IG5vdywgdG9rZW5zIH0pO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIC8vIE90aGVyd2lzZSwgd2FpdCB1bnRpbCB0aGUgb2xkZXN0IHJlcXVlc3QgZXhwaXJlcyAocGx1cyBhIHNtYWxsIGJ1ZmZlcilcbiAgICBjb25zdCBuZWVkZWQgPSB0b2tlbnMgLSAodGhpcy5saW1pdFBlck1pbnV0ZSAtIGN1cnJlbnRUb3RhbCk7XG4gICAgbGV0IGFjY3VtdWxhdGVkRXhwaXJlZCA9IDA7XG4gICAgbGV0IHdhaXRVbnRpbCA9IG5vdyArIHRoaXMud2luZG93TXM7IC8vIGZhbGxiYWNrXG5cbiAgICBjb25zdCBzb3J0ZWQgPSBbLi4udGhpcy5yZXF1ZXN0c10uc29ydCgoYSwgYikgPT4gYS50aW1lc3RhbXAgLSBiLnRpbWVzdGFtcCk7XG4gICAgZm9yIChjb25zdCByZXEgb2Ygc29ydGVkKSB7XG4gICAgICBhY2N1bXVsYXRlZEV4cGlyZWQgKz0gcmVxLnRva2VucztcbiAgICAgIGlmIChhY2N1bXVsYXRlZEV4cGlyZWQgPj0gbmVlZGVkKSB7XG4gICAgICAgIC8vICsxMG1zIGJ1ZmZlciB0byBzbGlkZSB0aGUgd2luZG93IGNsZWFubHlcbiAgICAgICAgd2FpdFVudGlsID0gcmVxLnRpbWVzdGFtcCArIHRoaXMud2luZG93TXMgKyAxMDtcbiAgICAgICAgYnJlYWs7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgZGVsYXkgPSB3YWl0VW50aWwgLSBub3c7XG4gICAgaWYgKGRlbGF5ID4gMCkge1xuICAgICAgY29uc29sZS5sb2coXG4gICAgICAgIGBbcmF0ZS1saW1pdF0gV2luZG93IGZ1bGwgKCR7Y3VycmVudFRvdGFsfS8ke3RoaXMubGltaXRQZXJNaW51dGV9KS4gYCArXG4gICAgICAgIGBXYWl0aW5nICR7KGRlbGF5IC8gMTAwMCkudG9GaXhlZCgxKX1zIHRvIHNlbmQgJHt0b2tlbnN9IHRva2Vucy4uLmBcbiAgICAgICk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgZGVsYXkpKTtcbiAgICB9XG5cbiAgICAvLyBSZWNvcmQgdGhlIGNvbnN1bXB0aW9uIGF0IHRoZSBuZXcgdGltZVxuICAgIHRoaXMucmVxdWVzdHMucHVzaCh7IHRpbWVzdGFtcDogRGF0ZS5ub3coKSwgdG9rZW5zIH0pO1xuICAgIC8vIENsZWFudXAgYWdhaW4ganVzdCBpbiBjYXNlXG4gICAgdGhpcy5yZXF1ZXN0cyA9IHRoaXMucmVxdWVzdHMuZmlsdGVyKHJlcSA9PiByZXEudGltZXN0YW1wID4gRGF0ZS5ub3coKSAtIHRoaXMud2luZG93TXMpO1xuICB9XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMi4gQ09ORklHVVJBVElPTlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zdCBUUE1fTElNSVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX1RQTV9MSU1JVCkgfHwgNTAwMDAwO1xuY29uc3QgUkFURV9MSU1JVEVSID0gbmV3IFNsaWRpbmdXaW5kb3dSYXRlTGltaXRlcihUUE1fTElNSVQpO1xuXG4vLyBCQVRDSF9TSVpFOiBudW1iZXIgb2YgY2h1bmtzIHBlciBlbWJlZENvbnRlbnQgY2FsbFxuLy8gKGtlcHQgYXQgMTA7IG5vdGUgdGhlIHJlYWwgY2VpbGluZyBpcyB0aGUgQVBJJ3MgfjEwMC1yZXF1ZXN0cy1wZXItY2FsbCBsaW1pdCxcbi8vIG5vdCBhIFwiY29udGV4dCB3aW5kb3dcIiBsaW1pdCBcdTIwMTQgMTAganVzdCBrZWVwcyBiYXRjaGVzIHNtYWxsIGFuZCByZXRyeS1mcmllbmRseSlcbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiAxMDsgICAvLyAxMCBjaHVua3MgXHUwMEQ3IDc1MCB0b2tlbnMgPSA3LDUwMCB0b2tlbnMgcGVyIEFQSSByZXF1ZXN0XG5jb25zdCBQQVJBTExFTF9DQUxMUyA9ICgpID0+IDEwOyAvLyBTZW5kIDEwIGJhdGNoZXMgY29uY3VycmVudGx5IHRvIGNsZWFyIHRoZSBidXJzdCBmYXN0XG5cbi8vIFJldHJ5IGNvbmZpZ3VyYXRpb24gKGV4cG9uZW50aWFsIGJhY2tvZmYgKyBqaXR0ZXIpXG5jb25zdCBSRVRSWV9CQVNFX0RFTEFZX01TID0gMjAwMDsgICAvLyAyIHNlY29uZHNcbmNvbnN0IFJFVFJZX01BWF9ERUxBWV9NUyA9IDYwMDAwOyAgIC8vIDYwIHNlY29uZHMgY2FwXG5jb25zdCBNQVhfUkVUUllfQVRURU1QVFMgPSA1O1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDMuIEFJIENMSUVOVCAoc2luZ2xlLCByZXVzYWJsZSBpbnN0YW5jZSlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZnVuY3Rpb24gY3JlYXRlQUlDbGllbnQoKSB7XG4gIGNvbnN0IHByb2plY3QgPSBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCBwcm9jZXNzLmVudi5HQ1BfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJztcbiAgY29uc3QgbG9jYXRpb24gPSBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfTE9DQVRJT04gfHwgJ3VzLWNlbnRyYWwxJztcblxuICAvLyBTdXBwb3J0IGNyZWRlbnRpYWxzIGZyb20gZW52IHZhciAoZm9yIHNlcnZlcmxlc3MpIG9yIGZpbGUgKGZvciBsb2NhbCBkZXYpXG4gIGNvbnN0IGNyZWRlbnRpYWxzSnNvbiA9IHByb2Nlc3MuZW52LkdPT0dMRV9DUkVERU5USUFMU19KU09OO1xuXG4gIGlmIChjcmVkZW50aWFsc0pzb24pIHtcbiAgICB0cnkge1xuICAgICAgY29uc3QgY3JlZGVudGlhbHMgPSBKU09OLnBhcnNlKGNyZWRlbnRpYWxzSnNvbik7XG4gICAgICByZXR1cm4gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICAgIHByb2plY3QsXG4gICAgICAgIGxvY2F0aW9uLFxuICAgICAgICBjcmVkZW50aWFsc1xuICAgICAgfSk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgY29uc29sZS53YXJuKCdGYWlsZWQgdG8gcGFyc2UgR09PR0xFX0NSRURFTlRJQUxTX0pTT04sIGZhbGxpbmcgYmFjayB0byBkZWZhdWx0IGF1dGgnKTtcbiAgICB9XG4gIH1cblxuICByZXR1cm4gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICB2ZXJ0ZXhhaTogdHJ1ZSxcbiAgICBwcm9qZWN0LFxuICAgIGxvY2F0aW9uXG4gIH0pO1xufVxuXG5jb25zdCBhaSA9IGNyZWF0ZUFJQ2xpZW50KCk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNC4gVE9LRU4gQ0FMQ1VMQVRJT04gKHVzZXMgc3RvcmVkIHRva2VuX2NvdW50IGlmIGF2YWlsYWJsZSlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZnVuY3Rpb24gZ2V0VG9rZW5Db3VudEZvckNodW5rcyhjaHVua3MpIHtcbiAgcmV0dXJuIGNodW5rcy5yZWR1Y2UoKHN1bSwgY2h1bmspID0+IHtcbiAgICAvLyBQcmVmZXIgdGhlIGV4YWN0IHRva2VuIGNvdW50IGZyb20gY2h1bmtlciwgb3RoZXJ3aXNlIGZhbGxiYWNrIHRvIHJvdWdoIGVzdGltYXRlXG4gICAgY29uc3QgdG9rZW5Db3VudCA9IGNodW5rLm1ldGFkYXRhPy50b2tlbl9jb3VudCB8fCBNYXRoLmNlaWwoY2h1bmsudGV4dC5sZW5ndGggLyA0KTtcbiAgICByZXR1cm4gc3VtICsgdG9rZW5Db3VudDtcbiAgfSwgMCk7XG59XG5cbi8vIFNhbWUgcm91Z2ggZXN0aW1hdGUgYXMgYWJvdmUsIGJ1dCBmb3IgcmF3IHN0cmluZ3MgdGhhdCBkb24ndCBjYXJyeSBjaHVuayBtZXRhZGF0YVxuLy8gKHVzZWQgZm9yIHJldHJpZXMgaW5zaWRlIGVtYmVkQmF0Y2gsIGFuZCBmb3IgZW1iZWRRdWVyeSkuXG5mdW5jdGlvbiBlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSB7XG4gIHJldHVybiB0ZXh0cy5yZWR1Y2UoKHN1bSwgdGV4dCkgPT4gc3VtICsgTWF0aC5jZWlsKFN0cmluZyh0ZXh0KS5sZW5ndGggLyA0KSwgMCk7XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNS4gRU1CRUQgQkFUQ0ggKHdpdGggZXhwb25lbnRpYWwgYmFja29mZiArIGppdHRlcilcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbW9kZWxOYW1lID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19NT0RFTCB8fCAnZ2VtaW5pLWVtYmVkZGluZy0wMDEnO1xuICBjb25zdCBvdXRwdXREaW1lbnNpb25hbGl0eSA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfRElNRU5TSU9OUykgfHwgMzA3MjtcblxuICB0cnkge1xuICAgIC8vIEZJWDogYGFpLmJhdGNoZXMuY3JlYXRlRW1iZWRkaW5nc2AgaXMgbm90IGEgcmVhbCBtZXRob2Qgb24gdGhlIEBnb29nbGUvZ2VuYWkgU0RLLlxuICAgIC8vIGBhaS5iYXRjaGVzYCBpcyBmb3IgYXN5bmMgYmF0Y2gtcHJlZGljdGlvbiBqb2JzLiBTeW5jaHJvbm91cyBlbWJlZGRpbmcgY2FsbHMgZ29cbiAgICAvLyB0aHJvdWdoIGBhaS5tb2RlbHMuZW1iZWRDb250ZW50YCwgd2l0aCBvbmUgc2hhcmVkIHRhc2tUeXBlL291dHB1dERpbWVuc2lvbmFsaXR5XG4gICAgLy8gY29uZmlnIGFwcGxpZWQgYWNyb3NzIGFsbCBgY29udGVudHNgIGluIHRoZSBjYWxsLlxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgYWkubW9kZWxzLmVtYmVkQ29udGVudCh7XG4gICAgICBtb2RlbDogbW9kZWxOYW1lLFxuICAgICAgY29udGVudHM6IHRleHRzLm1hcCh0ZXh0ID0+ICh0eXBlb2YgdGV4dCA9PT0gJ3N0cmluZycgPyB0ZXh0IDogU3RyaW5nKHRleHQpKSksXG4gICAgICBjb25maWc6IHtcbiAgICAgICAgdGFza1R5cGU6IHRhc2tUeXBlLFxuICAgICAgICBvdXRwdXREaW1lbnNpb25hbGl0eTogb3V0cHV0RGltZW5zaW9uYWxpdHlcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGNvbnN0IGVtYmVkZGluZ3MgPSByZXNwb25zZT8uZW1iZWRkaW5ncz8ubWFwKGUgPT4gZS52YWx1ZXMpIHx8IFtdO1xuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtlbWJlZGRpbmdzLmxlbmd0aH1gKTtcbiAgICB9XG4gICAgcmV0dXJuIGVtYmVkZGluZ3M7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zdCBpc1JldHJ5YWJsZSA9IGlzNDI5RXJyb3IoZXJyb3IpIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMiB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAzIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1NlcnZpY2UgVW5hdmFpbGFibGUnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdCYWQgR2F0ZXdheScpO1xuXG4gICAgaWYgKGlzUmV0cnlhYmxlICYmIGF0dGVtcHQgPCBNQVhfUkVUUllfQVRURU1QVFMpIHtcbiAgICAgIC8vIEV4cG9uZW50aWFsIGJhY2tvZmY6IDJeYXR0ZW1wdCAqIGJhc2UgKGNhcHBlZClcbiAgICAgIGxldCBkZWxheSA9IE1hdGgubWluKFJFVFJZX01BWF9ERUxBWV9NUywgUkVUUllfQkFTRV9ERUxBWV9NUyAqIE1hdGgucG93KDIsIGF0dGVtcHQgLSAxKSk7XG4gICAgICAvLyBBZGQgaml0dGVyICgwLjhcdTIwMTMxLjJ4KSB0byBhdm9pZCB0aHVuZGVyaW5nIGhlcmRcbiAgICAgIGNvbnN0IGppdHRlciA9IDAuOCArICgwLjQgKiBNYXRoLnJhbmRvbSgpKTtcbiAgICAgIGRlbGF5ID0gTWF0aC5mbG9vcihkZWxheSAqIGppdHRlcik7XG4gICAgICAvLyBSZXNwZWN0IHJldHJ5LWFmdGVyIGhlYWRlciBpZiBwcmVzZW50XG4gICAgICBpZiAoZXJyb3IucmV0cnlBZnRlcikge1xuICAgICAgICBkZWxheSA9IE1hdGgubWF4KGRlbGF5LCBlcnJvci5yZXRyeUFmdGVyICogMTAwMCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnNvbGUubG9nKFxuICAgICAgICBgW2VtYmVkZGluZ10gXHUyM0YzIFJldHJ5YWJsZSBlcnJvciAoJHtlcnJvcj8uc3RhdHVzIHx8ICd1bmtub3duJ30pLCBgICtcbiAgICAgICAgYHdhaXRpbmcgJHsoZGVsYXkgLyAxMDAwKS50b0ZpeGVkKDEpfXMgKGF0dGVtcHQgJHthdHRlbXB0fS8ke01BWF9SRVRSWV9BVFRFTVBUU30pLi4uYFxuICAgICAgKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCBkZWxheSkpO1xuXG4gICAgICAvLyBGSVg6IGEgcmV0cnkgaXMgYSBicmFuZCBuZXcgQVBJIGNhbGwgYW5kIGNvbnN1bWVzIHJlYWwgcXVvdGEsIGV2ZW4gdGhvdWdoXG4gICAgICAvLyB0aGUgb3JpZ2luYWwgY2FsbCBmYWlsZWQuIFNraXBwaW5nIGNvbnN1bXB0aW9uIGhlcmUgKGFzIGJlZm9yZSkgbGV0IHRoZSBsb2NhbFxuICAgICAgLy8gbGltaXRlciB1bmRlci1yZXBvcnQgYWN0dWFsIHVzYWdlIGR1cmluZyBlcnJvciBzdG9ybXMsIHdoaWNoIG1lYW50IGl0IGtlcHRcbiAgICAgIC8vIHdhdmluZyB0aHJvdWdoIG5ldyBncm91cHMgd2hpbGUgcmV0cmllcyB3ZXJlIGFsc28gaGl0dGluZyB0aGUgQVBJIFx1MjAxNCBtYWtpbmdcbiAgICAgIC8vIDQyOSBzdG9ybXMgd29yc2UgaW5zdGVhZCBvZiBiYWNraW5nIG9mZiBmcm9tIHRoZW0uXG4gICAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSk7XG5cbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA2LiBFWFBPUlRFRCBnZW5lcmF0ZUVtYmVkZGluZ3MgKHdpdGggcmF0ZSBsaW1pdGVyICYgYWNjdXJhdGUgdG9rZW5zKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVFbWJlZGRpbmdzKGNodW5rcywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgb25Qcm9ncmVzcykge1xuICBpZiAoIWNodW5rcyB8fCBjaHVua3MubGVuZ3RoID09PSAwKSByZXR1cm4gW107XG5cbiAgY29uc3QgYmF0Y2hTaXplID0gQkFUQ0hfU0laRSgpO1xuICBjb25zdCBwYXJhbGxlbENhbGxzID0gUEFSQUxMRUxfQ0FMTFMoKTtcblxuICAvLyBGaXhlZC1zaXplIGFycmF5IHRvIHByZXNlcnZlIGNocm9ub2xvZ2ljYWwgb3JkZXJcbiAgY29uc3QgZW1iZWRkaW5ncyA9IG5ldyBBcnJheShjaHVua3MubGVuZ3RoKTtcblxuICAvLyBHcm91cCBjaHVua3MgaW50byBiYXRjaGVzIHdpdGggdGhlaXIgc3RhcnRpbmcgaW5kZXhcbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKHtcbiAgICAgIGNodW5rczogY2h1bmtzLnNsaWNlKGksIGkgKyBiYXRjaFNpemUpLFxuICAgICAgc3RhcnRJbmRleDogaVxuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdG90YWxHcm91cHMgPSBNYXRoLmNlaWwoYmF0Y2hlcy5sZW5ndGggLyBwYXJhbGxlbENhbGxzKTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IHBhcmFsbGVsQ2FsbHMpIHtcbiAgICBjb25zdCBwYXJhbGxlbEJhdGNoZXMgPSBiYXRjaGVzLnNsaWNlKGksIGkgKyBwYXJhbGxlbENhbGxzKTtcbiAgICBjb25zdCBncm91cE51bSA9IE1hdGguZmxvb3IoaSAvIHBhcmFsbGVsQ2FsbHMpICsgMTtcblxuICAgIC8vIENhbGN1bGF0ZSBleGFjdCB0b2tlbnMgdXNpbmcgc3RvcmVkIHRva2VuX2NvdW50IChvciBmYWxsYmFjaylcbiAgICBjb25zdCBhbGxDaHVua3NJbkdyb3VwID0gcGFyYWxsZWxCYXRjaGVzLmZsYXRNYXAoYiA9PiBiLmNodW5rcyk7XG4gICAgY29uc3QgdG9rZW5zVG9Db25zdW1lID0gZ2V0VG9rZW5Db3VudEZvckNodW5rcyhhbGxDaHVua3NJbkdyb3VwKTtcbiAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZSh0b2tlbnNUb0NvbnN1bWUpO1xuXG4gICAgY29uc29sZS5sb2coXG4gICAgICBgW2VtYmVkZGluZ10gR3JvdXAgJHtncm91cE51bX0vJHt0b3RhbEdyb3Vwc30gXHUyMDE0IGZpcmluZyAke3BhcmFsbGVsQmF0Y2hlcy5sZW5ndGh9IGJhdGNoZXMgYCArXG4gICAgICBgaW4gcGFyYWxsZWwgKCR7dG9rZW5zVG9Db25zdW1lfSB0b2tlbnMpYFxuICAgICk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgcGFyYWxsZWxCYXRjaGVzLm1hcChiID0+IGVtYmVkQmF0Y2goYi5jaHVua3MubWFwKGMgPT4gYy50ZXh0KSwgdGFza1R5cGUpKVxuICAgICk7XG5cbiAgICBjb25zdCBmYWlsZWRCYXRjaGVzID0gW107XG4gICAgcmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGJhdGNoSWR4KSA9PiB7XG4gICAgICBjb25zdCBjdXJyZW50QmF0Y2hJbmZvID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlO1xuICAgICAgICBjdXJyZW50QmF0Y2hJbmZvLmNodW5rcy5mb3JFYWNoKChjaHVuaywgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBnbG9iYWxJbmRleCA9IGN1cnJlbnRCYXRjaEluZm8uc3RhcnRJbmRleCArIGNodW5rSWR4O1xuICAgICAgICAgIGVtYmVkZGluZ3NbZ2xvYmFsSW5kZXhdID0ge1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHtnbG9iYWxJbmRleH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzW2NodW5rSWR4XSxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVuay5tZXRhZGF0YSxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rLnRleHRcbiAgICAgICAgICB9O1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gQmF0Y2ggc3RhcnRpbmcgYXQgaW5kZXggJHtjdXJyZW50QmF0Y2hJbmZvLnN0YXJ0SW5kZXh9IGZhaWxlZDpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgZmFpbGVkQmF0Y2hlcy5wdXNoKGN1cnJlbnRCYXRjaEluZm8pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKG9uUHJvZ3Jlc3MpIHtcbiAgICAgIG9uUHJvZ3Jlc3MoeyBjdXJyZW50X2JhdGNoOiBncm91cE51bSwgdG90YWxfYmF0Y2hlczogdG90YWxHcm91cHMgfSk7XG4gICAgfVxuXG4gICAgLy8gUmV0cnkgZmFpbGVkIGJhdGNoZXMgaW5kaXZpZHVhbGx5XG4gICAgZm9yIChjb25zdCBmYWlsZWRCYXRjaCBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gUmV0cnlpbmcgZmFpbGVkIGJhdGNoIGVsZW1lbnRzIHN0YXJ0aW5nIGF0IGluZGV4ICR7ZmFpbGVkQmF0Y2guc3RhcnRJbmRleH0uLi5gKTtcbiAgICAgIGZvciAobGV0IGNodW5rSWR4ID0gMDsgY2h1bmtJZHggPCBmYWlsZWRCYXRjaC5jaHVua3MubGVuZ3RoOyBjaHVua0lkeCsrKSB7XG4gICAgICAgIGNvbnN0IGNodW5rID0gZmFpbGVkQmF0Y2guY2h1bmtzW2NodW5rSWR4XTtcbiAgICAgICAgY29uc3QgZ2xvYmFsSW5kZXggPSBmYWlsZWRCYXRjaC5zdGFydEluZGV4ICsgY2h1bmtJZHg7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgLy8gRklYOiB0aGlzIHJldHJ5IGlzIGEgZnJlc2gsIHJlYWwgQVBJIGNhbGwgXHUyMDE0IHRyYWNrIGl0cyB0b2tlbnMgYWdhaW5zdFxuICAgICAgICAgIC8vIHRoZSBsaW1pdGVyIGluc3RlYWQgb2YgYXNzdW1pbmcgaXQgd2FzIFwiYWxyZWFkeSBwYWlkIGZvclwiLlxuICAgICAgICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGdldFRva2VuQ291bnRGb3JDaHVua3MoW2NodW5rXSkpO1xuICAgICAgICAgIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtjaHVuay50ZXh0XSwgdGFza1R5cGUpO1xuICAgICAgICAgIGVtYmVkZGluZ3NbZ2xvYmFsSW5kZXhdID0ge1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtnbG9iYWxJbmRleH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH07XG4gICAgICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFx1MjcwNSBSZXRyeSBzdWNjZWVkZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGdsb2JhbEluZGV4fWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLmVycm9yKGBbZW1iZWRkaW5nXSBcdTI3NEMgUmV0cnkgZmFpbGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBnbG9iYWxJbmRleH06YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgLy8gRklYOiBwZXJtYW5lbnRseS1mYWlsZWQgY2h1bmtzIGFyZSBkcm9wcGVkIGhlcmUsIHdoaWNoIHNoaWZ0cyBhcnJheSBpbmRpY2VzXG4gIC8vIHJlbGF0aXZlIHRvIHRoZSBvcmlnaW5hbCBgY2h1bmtzYCBpbnB1dC4gVGhpcyBsb2cgbWFrZXMgdGhhdCBsb3NzIHZpc2libGVcbiAgLy8gaW5zdGVhZCBvZiBzaWxlbnQ7IGNhbGxlcnMgdGhhdCBuZWVkIHRvIGtub3cgZXhhY3RseSB3aGljaCBjaHVua3Mgd2VyZSBsb3N0XG4gIC8vIGNhbiBjb21wYXJlIHJldHVybmVkIGBpZGBzIGFnYWluc3QgdGhlaXIgb3JpZ2luYWwgY2h1bmsgbGlzdC5cbiAgY29uc3QgZmFpbGVkQ291bnQgPSBlbWJlZGRpbmdzLmZpbHRlcihlID0+ICFlKS5sZW5ndGg7XG4gIGlmIChmYWlsZWRDb3VudCA+IDApIHtcbiAgICBjb25zb2xlLndhcm4oYFtlbWJlZGRpbmddICR7ZmFpbGVkQ291bnR9LyR7Y2h1bmtzLmxlbmd0aH0gY2h1bmsocykgcGVybWFuZW50bHkgZmFpbGVkIHRvIGVtYmVkIGFuZCB3ZXJlIGRyb3BwZWQuYCk7XG4gIH1cblxuICAvLyBGaWx0ZXIgb3V0IGFueSBlbGVtZW50cyB0aGF0IHBlcm1hbmVudGx5IGZhaWxlZFxuICByZXR1cm4gZW1iZWRkaW5ncy5maWx0ZXIoQm9vbGVhbik7XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNy4gRVhQT1JURUQgZW1iZWRRdWVyeVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICAvLyBGSVg6IHRoaXMgY2FsbCB3YXMgYnlwYXNzaW5nIHRoZSByYXRlIGxpbWl0ZXIgZW50aXJlbHkuIElmIGl0IHJ1bnMgY29uY3VycmVudGx5XG4gIC8vIHdpdGggZG9jdW1lbnQgaW5nZXN0aW9uIChlLmcuIGEgdXNlciBzZWFyY2hlcyB3aGlsZSBhIGJhdGNoIGpvYiBpcyBpbiBmbGlnaHQpLFxuICAvLyBpdCBjb3VsZCBwdXNoIHRvdGFsIHVzYWdlIG92ZXIgdGhlIGNvbmZpZ3VyZWQgVFBNIGJ1ZGdldCB1bm5vdGljZWQuXG4gIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHMoW3F1ZXJ5XSkpO1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnKSB7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0ICR7dGV4dHMubGVuZ3RofSB0ZXh0cywgdGFza1R5cGU9JHt0YXNrVHlwZX1gKTtcbiAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykpO1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUpO1xuICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gZW1iZWRTaW5nbGVCYXRjaEdyb3VwIFx1MjAxNCBnb3QgJHt2ZWN0b3JzLmxlbmd0aH0gdmVjdG9yc2ApO1xuICByZXR1cm4gdmVjdG9ycztcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2ltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHtcbiAgZ2V0R2xvYmFsQ29sbGVjdGlvbixcbiAgZ2V0Q29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50c1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEdsb2JhbCBkb2N1bWVudHMgY2FjaGUgKHBvcHVsYXRlZCBvbmNlIG9uIGZpcnN0IHNlc3Npb24gaW5pdCkgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5sZXQgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUgPSBbXTtcbmxldCBnbG9iYWxEYXRhSW5pdGlhbGl6ZWQgPSBmYWxzZTtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldEdsb2JhbERvY3VtZW50c0NhY2hlKCkge1xuICByZXR1cm4gZ2xvYmFsRG9jdW1lbnRzQ2FjaGU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBpZCA9IHNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3Qgc2Vzc2lvbiA9IHtcbiAgICBpZCxcbiAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCksXG4gICAgbGFzdEFjY2Vzc2VkOiBuZXcgRGF0ZSgpLFxuICAgIGRvY3VtZW50czogW10sXG4gICAgZGVsZXRlZERvY3VtZW50SWRzOiBuZXcgU2V0KCksXG4gICAgdGltZW91dE1pbnV0ZXM6IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTXG4gIH07XG4gIHNlc3Npb25zLnNldChpZCwgc2Vzc2lvbik7XG4gIHJldHVybiBzZXNzaW9uO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IHNlc3Npb25zLmdldChzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIHJldHVybiBzZXNzaW9uO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGV4aXN0aW5nKSByZXR1cm4gZXhpc3Rpbmc7XG4gICAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgfVxuICByZXR1cm4gY3JlYXRlU2Vzc2lvbigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSB7XG4gIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gIGNvbnN0IGxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKHNlc3Npb24ubGFzdEFjY2Vzc2VkKS5nZXRUaW1lKCk7XG4gIGNvbnN0IHRpbWVvdXRNcyA9IHNlc3Npb24udGltZW91dE1pbnV0ZXMgKiA2MCAqIDEwMDA7XG4gIHJldHVybiAobm93IC0gbGFzdEFjY2Vzc2VkKSA+IHRpbWVvdXRNcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIHNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xuICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIENoZWNrIGlmIHNlc3Npb24gaXMgc2VlZGVkIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvblNlZWRlZChzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIHNlZWRlZFNlc3Npb25zLmhhcyhzZXNzaW9uSWQpO1xufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTm90aWZ5IFNTRSBsaXN0ZW5lcnMgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5mdW5jdGlvbiBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKSB7XG4gIGlmIChnbG9iYWwuc2VlZGluZ0xpc3RlbmVycyAmJiBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5oYXMoYHNlZWRpbmc6JHtzZXNzaW9uSWR9YCkpIHtcbiAgICBjb25zdCBldmVudEtleSA9IGBzZWVkaW5nOiR7c2Vzc2lvbklkfWA7XG4gICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICBsaXN0ZW5lcnMuZm9yRWFjaCgocmVzcG9uc2UpID0+IHtcbiAgICAgIHRyeSB7XG4gICAgICAgIHJlc3BvbnNlLndyaXRlKGBldmVudDogc2VlZGluZ19jb21wbGV0ZVxcbmRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoeyBzZXNzaW9uSWQsIHNlZWRlZDogdHJ1ZSB9KX1cXG5cXG5gKTtcbiAgICAgICAgcmVzcG9uc2UuZW5kKCk7XG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcihgW25vdGlmeV0gRmFpbGVkIHRvIG5vdGlmeSBsaXN0ZW5lcjpgLCBlcnIubWVzc2FnZSk7XG4gICAgICB9XG4gICAgfSk7XG4gICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgICBjb25zb2xlLmxvZyhgW25vdGlmeV0gTm90aWZpZWQgJHtsaXN0ZW5lcnMubGVuZ3RofSBTU0UgbGlzdGVuZXJzIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICB9XG59XG5cbi8qKlxuICogT24gc2Vzc2lvbiBzdGFydDpcbiAqIC0gUmVjb25zdHJ1Y3QgaW4tbWVtb3J5IHNlc3Npb24gZG9jIGxpc3QgZnJvbSB0aGUgc2luZ2xlIGNvbGxlY3Rpb25cbiAqICAgYnkgZmlsdGVyaW5nIG9uIHNlc3Npb25faWQgbWV0YWRhdGEuXG4gKiAtIE5vIHZlY3RvciBjb3B5aW5nIGlzIHBlcmZvcm1lZCBcdTIwMTQgZ2xvYmFsIGRvY3MgYXJlIHNlcnZlZCBmcm9tIGNhY2hlLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpIHtcbiAgY29uc29sZS5sb2coYFx1RDgzRFx1REQxMSBTZXNzaW9uIGluaXQ6ICR7c2Vzc2lvbklkfWApO1xuICBpZiAoc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIEFscmVhZHkgc2VlZGVkICR7c2Vzc2lvbklkfSwgc2tpcHBpbmdgKTtcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgICByZXR1cm47XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG5cbiAgICAvLyBcdTI1MDBcdTI1MDAgTGF6eSBvbmUtdGltZSBnbG9iYWwgY2FjaGUgaW5pdCAocnVucyBvbiBmaXJzdCBzZXNzaW9uIGluaXQpIFx1MjUwMFx1MjUwMFxuICAgIGlmICghZ2xvYmFsRGF0YUluaXRpYWxpemVkKSB7XG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCBnbG9iYWxEb2NzID0gYXdhaXQgbGlzdERvY3VtZW50cyhjb2xsZWN0aW9uLCB7IHNlc3Npb25faWQ6ICdnbG9iYWwnIH0pO1xuICAgICAgICBnbG9iYWxEb2N1bWVudHNDYWNoZSA9IGdsb2JhbERvY3MubWFwKGRvYyA9PiAoe1xuICAgICAgICAgIGlkOiBkb2MuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICBmaWxlU2l6ZTogbnVsbCxcbiAgICAgICAgICBwYWdlQ291bnQ6IGRvYy5wYWdlX2NvdW50IHx8IG51bGwsXG4gICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgIHNvdXJjZVR5cGU6ICdnbG9iYWwnLFxuICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogZG9jLnVwbG9hZF90aW1lc3RhbXBcbiAgICAgICAgfSkpO1xuICAgICAgICBnbG9iYWxEYXRhSW5pdGlhbGl6ZWQgPSB0cnVlO1xuICAgICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IEdsb2JhbCBkb2N1bWVudHMgY2FjaGUgbG9hZGVkOiAke2dsb2JhbERvY3VtZW50c0NhY2hlLmxlbmd0aH0gZG9jdW1lbnQocylgKTtcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdcdTI3NEMgRmFpbGVkIHRvIGluaXRpYWxpemUgZ2xvYmFsIGRhdGE6JywgZXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gICAgLy8gUmVjb25zdHJ1Y3Qgc2Vzc2lvbi1zcGVjaWZpYyBkb2NzICh1c2VyIHVwbG9hZHMpIGZyb20gdGhlIGNvbGxlY3Rpb25cbiAgICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHsgc2Vzc2lvbl9pZDogc2Vzc2lvbklkIH0pO1xuICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgICAgICAgIGlkOiBkb2MuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICBmaWxlU2l6ZTogbnVsbCxcbiAgICAgICAgICBwYWdlQ291bnQ6IGRvYy5wYWdlX2NvdW50IHx8IG51bGwsXG4gICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgIHNvdXJjZVR5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgICAgaWYgKGRvY3MubGVuZ3RoID4gMCkge1xuICAgICAgICBjb25zb2xlLmxvZyhgXHUyNjdCXHVGRTBGICBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IHNlc3Npb24gZG9jdW1lbnQocykgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgIH1cbiAgICBzZWVkZWRTZXNzaW9ucy5hZGQoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gJHtzZXNzaW9uSWR9IHJlYWR5IChubyB2ZWN0b3IgY29weWluZyBuZWVkZWQpYCk7XG4gICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIGluaXQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gU3RpbGwgbm90aWZ5IGxpc3RlbmVycyBzbyB0aGV5IGRvbid0IGhhbmcgZm9yZXZlclxuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBEb2N1bWVudCBtYW5hZ2VtZW50IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGZ1bmN0aW9uIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJbmZvKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuXG4gIGNvbnN0IGV4aXN0aW5nID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZChkID0+IGQuaWQgPT09IGRvY3VtZW50SW5mby5pZCk7XG5cbiAgaWYgKGV4aXN0aW5nKSB7XG4gICAgaWYgKGRvY3VtZW50SW5mby5jaHVua0NvdW50ICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmNodW5rQ291bnQgPSBkb2N1bWVudEluZm8uY2h1bmtDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5wYWdlQ291bnQgPSBkb2N1bWVudEluZm8ucGFnZUNvdW50O1xuICAgIGlmIChkb2N1bWVudEluZm8uZmlsZVNpemUgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuZmlsZVNpemUgPSBkb2N1bWVudEluZm8uZmlsZVNpemU7XG4gICAgaWYgKGRvY3VtZW50SW5mby5zdGF0dXMgIT09IHVuZGVmaW5lZCkgZXhpc3Rpbmcuc3RhdHVzID0gZG9jdW1lbnRJbmZvLnN0YXR1cztcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVuYW1lICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVuYW1lID0gZG9jdW1lbnRJbmZvLmZpbGVuYW1lO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFVwZGF0ZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZXhpc3Rpbmcuc3RhdHVzfSwgY2h1bmtzPSR7ZXhpc3RpbmcuY2h1bmtDb3VudH1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgPz8gMCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgIHN0YXR1czogZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnXG4gIH0pO1xuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWRkZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnfWApO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5kZWxldGVkRG9jdW1lbnRJZHMuYWRkKGRvY3VtZW50SWQpO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFJlbW92ZWQgZG9jICR7ZG9jdW1lbnRJZH0sIGFkZGVkIHRvIGRlbGV0ZWREb2N1bWVudElkc2ApO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldERlbGV0ZWREb2N1bWVudElkcyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHNlc3Npb24/LmRlbGV0ZWREb2N1bWVudElkcyA/PyBuZXcgU2V0KCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBbXTtcbiAgcmV0dXJuIHNlc3Npb24uZG9jdW1lbnRzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IHNlc3Npb25Eb2N1bWVudHM6IFtdLCBnbG9iYWxEb2N1bWVudHM6IFtdIH07XG5cbiAgY29uc3Qgbm9ybWFsaXplID0gKGRvYykgPT4gKHtcbiAgICBkb2N1bWVudF9pZDogZG9jLmlkLFxuICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgY2h1bmtfY291bnQ6IGRvYy5jaHVua0NvdW50ID8/IDAsXG4gICAgcGFnZV9jb3VudDogZG9jLnBhZ2VDb3VudCA/PyAwLFxuICAgIHVwbG9hZF90aW1lc3RhbXA6IGRvYy51cGxvYWRUaW1lc3RhbXAgfHwgbnVsbCxcbiAgICBzb3VyY2VfdHlwZTogZG9jLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcgPyAnc2Vzc2lvbl91cGxvYWQnIDogJ3NlZWQnLFxuICAgIGZpbGVTaXplOiBkb2MuZmlsZVNpemUgfHwgbnVsbCxcbiAgICBzdGF0dXM6IGRvYy5zdGF0dXMgPz8gbnVsbFxuICB9KTtcblxuICByZXR1cm4ge1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKVxuICAgICAgLm1hcChub3JtYWxpemUpLFxuICAgIGdsb2JhbERvY3VtZW50czogZ2xvYmFsRG9jdW1lbnRzQ2FjaGVcbiAgICAgIC5tYXAobm9ybWFsaXplKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCArIGdsb2JhbERvY3VtZW50c0NhY2hlLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgICAgICsgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsaXN0QWN0aXZlU2Vzc2lvbnMoKSB7XG4gIHJldHVybiBBcnJheS5mcm9tKHNlc3Npb25zLnZhbHVlcygpKS5maWx0ZXIocyA9PiAhaXNTZXNzaW9uRXhwaXJlZChzKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhbnVwRXhwaXJlZFNlc3Npb25zKCkge1xuICBsZXQgY2xlYW5lZCA9IDA7XG4gIGZvciAoY29uc3QgW2lkLCBzZXNzaW9uXSBvZiBzZXNzaW9ucykge1xuICAgIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgICBzZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgc2VlZGVkU2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIGNsZWFuZWQrKztcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGNsZWFuZWQ7XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiOyAgaW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG4gIGltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbiAgaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG4gIGltcG9ydCBmcyBmcm9tICdmcyc7XG4gIGltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuICBpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcbiAgaW1wb3J0IHBkZiBmcm9tICdwZGYtcGFyc2UnO1xuICBpbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbiAgaW1wb3J0IHsgc2FuaXRpemVGaWxlbmFtZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbiAgaW1wb3J0IHtcbiAgICBDb3JydXB0ZWRQREZFcnJvcixcbiAgICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuICBpbXBvcnQgeyBnZXRDb2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbiAgaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbiAgaW1wb3J0IHsgZW1iZWRTaW5nbGVCYXRjaEdyb3VwIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG4gIGltcG9ydCB7XG4gICAgZ2V0T3JDcmVhdGVTZXNzaW9uLFxuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uLFxuICAgIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sXG4gICAgZ2V0QWxsRG9jdW1lbnRzLFxuICAgIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3MsXG4gICAgaXNTZXNzaW9uU2VlZGVkXG4gIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuICBpbXBvcnQgeyBjbGVhck1lbW9yeSB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG4gIGNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4gIGNvbnN0IF9fZmlsZW5hbWUgPSBmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCk7XG4gIGNvbnN0IF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShfX2ZpbGVuYW1lKTtcblxuICBjb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbiAgaWYgKCFmcy5leGlzdHNTeW5jKHVwbG9hZERpcikpIHtcbiAgICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbiAgfVxuXG4gIGNvbnN0IHNlZWREaXIgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi4vLi4vc2VlZF9kb2N1bWVudHMnKTtcblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgU1NFIGV2ZW50IGhlbHBlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZnVuY3Rpb24gc3NlRXZlbnQocmVzLCBldmVudCwgZGF0YSkge1xuICAgIHJlcy53cml0ZShgZXZlbnQ6ICR7ZXZlbnR9XFxuZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfVxuXG4gIGNvbnN0IHN0b3JhZ2UgPSBtdWx0ZXIuZGlza1N0b3JhZ2Uoe1xuICAgIGRlc3RpbmF0aW9uOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgdXBsb2FkRGlyKSxcbiAgICBmaWxlbmFtZTogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpKVxuICB9KTtcblxuICBjb25zdCB1cGxvYWQgPSBtdWx0ZXIoe1xuICAgIHN0b3JhZ2UsXG4gICAgbGltaXRzOiB7IGZpbGVTaXplOiBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfVVBMT0FEX1NJWkVfTUIgfHwgJzUnKSAqIDEwMjQgKiAxMDI0IH0sXG4gICAgZmlsZUZpbHRlcjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICAgIGlmIChmaWxlLm1pbWV0eXBlID09PSAnYXBwbGljYXRpb24vcGRmJyAmJiBwYXRoLmV4dG5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpLnRvTG93ZXJDYXNlKCkgPT09ICcucGRmJykge1xuICAgICAgICBjYihudWxsLCB0cnVlKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNiKG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpKTtcbiAgICAgIH1cbiAgICB9XG4gIH0pO1xuXG4gIGZ1bmN0aW9uIGNvbnRlbnREaXNwb3NpdGlvbihkaXNwbGF5TmFtZSkge1xuICAgIGNvbnN0IGVuY29kZWQgPSBlbmNvZGVVUklDb21wb25lbnQoZGlzcGxheU5hbWUpXG4gICAgICAucmVwbGFjZSgvJy9nLCAnJTI3JylcbiAgICAgIC5yZXBsYWNlKC9cXCgvZywgJyUyOCcpXG4gICAgICAucmVwbGFjZSgvXFwpL2csICclMjknKTtcbiAgICByZXR1cm4gYGlubGluZTsgZmlsZW5hbWU9XCJkb2N1bWVudC5wZGZcIjsgZmlsZW5hbWUqPVVURi04Jycke2VuY29kZWR9YDtcbiAgfVxuXG4gIC8qKlxuICAgKiBKb2luIHBkZi5qcyB0ZXh0LWNvbnRlbnQgaXRlbXMgaW50byBhIHNpbmdsZSBzdHJpbmcgdXNpbmcgZWFjaCBpdGVtJ3NcbiAgICogeC1wb3NpdGlvbiAodHJhbnNmb3JtWzRdKSBhbmQgd2lkdGggdG8gZGVjaWRlIHdoZXRoZXIgYSBzcGFjZSBiZWxvbmdzXG4gICAqIGJldHdlZW4gdHdvIGl0ZW1zLCBpbnN0ZWFkIG9mIGFsd2F5cyBqb2luaW5nIHdpdGggYSBzaW5nbGUgc3BhY2UuXG4gICAqXG4gICAqIFRoaXMgYXZvaWRzIHR3byBjb21tb24gYXJ0aWZhY3RzIGZyb20gbmFpdmUgYC5qb2luKCcgJylgOlxuICAgKiAgLSB3b3JkcyBzcGxpdCBhY3Jvc3MgYWRqYWNlbnQgdGV4dCBydW5zIGdldHRpbmcgYSBwaGFudG9tIHNwYWNlXG4gICAqICAgIGluc2VydGVkIGluIHRoZSBtaWRkbGUgKGUuZy4gXCJTYXYgaW5nc1wiKVxuICAgKiAgLSBhZGphY2VudCB3b3JkcyB3aXRoIG5vIHNwYWNlIGluIHRoZSBQREYncyBpbnRlcm5hbCBydW5zIGdldHRpbmdcbiAgICogICAgZ2x1ZWQgdG9nZXRoZXIgKGUuZy4gXCJ0aGUgcmVwb3J0XCIgLT4gXCJ0aGVyZXBvcnRcIilcbiAgICpcbiAgICogRW1wdHktc3RyaW5nIGl0ZW1zIGFyZSBwZGYuanMncyBzaWduYWwgZm9yIGEgbGluZSBicmVhaywgd2hpY2ggd2VcbiAgICogY29udmVydCB0byBhIG5ld2xpbmUgc28gcGFyYWdyYXBoIHN0cnVjdHVyZSBpc24ndCBsb3N0LlxuICAgKi9cbiAgZnVuY3Rpb24gam9pblRleHRJdGVtcyhpdGVtcykge1xuICAgIGxldCBvdXQgPSAnJztcbiAgICBsZXQgcHJldkl0ZW0gPSBudWxsO1xuXG4gICAgZm9yIChjb25zdCBpdGVtIG9mIGl0ZW1zKSB7XG4gICAgICBjb25zdCBzdHIgPSBpdGVtLnN0cjtcbiAgICAgIGlmIChzdHIgPT09IHVuZGVmaW5lZCkgeyBwcmV2SXRlbSA9IGl0ZW07IGNvbnRpbnVlOyB9XG5cbiAgICAgIGlmIChzdHIgPT09ICcnKSB7XG4gICAgICAgIC8vIHBkZi5qcyBlbWl0cyBlbXB0eSBpdGVtcyB0byBzaWduYWwgbGluZSBicmVha3NcbiAgICAgICAgaWYgKCEvXFxuJC8udGVzdChvdXQpKSBvdXQgKz0gJ1xcbic7XG4gICAgICAgIHByZXZJdGVtID0gbnVsbDtcbiAgICAgICAgY29udGludWU7XG4gICAgICB9XG5cbiAgICAgIGlmIChwcmV2SXRlbSAmJiBwcmV2SXRlbS5zdHIpIHtcbiAgICAgICAgY29uc3QgcHJldkVuZCA9IHByZXZJdGVtLnRyYW5zZm9ybVs0XSArIChwcmV2SXRlbS53aWR0aCB8fCAwKTtcbiAgICAgICAgY29uc3QgY3VyU3RhcnQgPSBpdGVtLnRyYW5zZm9ybVs0XTtcbiAgICAgICAgY29uc3QgZ2FwID0gY3VyU3RhcnQgLSBwcmV2RW5kO1xuICAgICAgICBjb25zdCBmb250SCA9IE1hdGguYWJzKGl0ZW0udHJhbnNmb3JtWzNdKSB8fCAxMDtcbiAgICAgICAgY29uc3Qgc3BhY2VUaHJlc2hvbGQgPSBmb250SCAqIDAuMjU7XG5cbiAgICAgICAgY29uc3QgYWxyZWFkeVNwYWNlZCA9IC9cXHMkLy50ZXN0KG91dCkgfHwgL15cXHMvLnRlc3Qoc3RyKTtcbiAgICAgICAgaWYgKCFhbHJlYWR5U3BhY2VkICYmIGdhcCA+IHNwYWNlVGhyZXNob2xkKSB7XG4gICAgICAgICAgb3V0ICs9ICcgJztcbiAgICAgICAgfVxuICAgICAgICAvLyBlbHNlOiBpdGVtcyBhcmUgdG91Y2hpbmcvb3ZlcmxhcHBpbmcgLT4gc2FtZSB3b3JkLCBubyBzcGFjZSBpbnNlcnRlZFxuICAgICAgfVxuXG4gICAgICBvdXQgKz0gc3RyO1xuICAgICAgcHJldkl0ZW0gPSBpdGVtO1xuICAgIH1cblxuICAgIHJldHVybiBvdXQ7XG4gIH1cblxuICBhc3luYyBmdW5jdGlvbiBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlUGF0aCkge1xuICAgIHRyeSB7XG4gICAgICBjb25zdCBidWZmZXIgPSBmcy5yZWFkRmlsZVN5bmMoZmlsZVBhdGgpO1xuXG4gICAgICBjb25zdCBwYWdlcyA9IFtdO1xuICAgICAgYXdhaXQgcGRmKGJ1ZmZlciwge1xuICAgICAgICBwYWdlcmVuZGVyOiAocGFnZURhdGEpID0+IHtcbiAgICAgICAgICByZXR1cm4gcGFnZURhdGEuZ2V0VGV4dENvbnRlbnQoKS50aGVuKHRjID0+IHtcbiAgICAgICAgICAgIGNvbnN0IHBhZ2VUZXh0ID0gam9pblRleHRJdGVtcyh0Yy5pdGVtcyk7XG4gICAgICAgICAgICBwYWdlcy5wdXNoKHBhZ2VUZXh0KTtcbiAgICAgICAgICAgIHJldHVybiBwYWdlVGV4dDtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGlmIChwYWdlcy5sZW5ndGggPT09IDAgfHwgcGFnZXMuZXZlcnkocCA9PiAhcC50cmltKCkpKSB7XG4gICAgICAgIGNvbnN0IGZ1bGwgPSBhd2FpdCBwZGYoYnVmZmVyKTtcbiAgICAgICAgcGFnZXMucHVzaChmdWxsLnRleHQpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCB0b3RhbFBhZ2VzID0gcGFnZXMubGVuZ3RoO1xuICAgICAgY29uc3QgY2xlYW5lZFBhZ2VzID0gcGFnZXMubWFwKHAgPT4gY2xlYW5UZXh0KHApKTtcbiAgICAgIGNvbnN0IHBhZ2VNYXAgPSBbXTtcbiAgICAgIGxldCBjaGFyUG9zID0gMDtcblxuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjbGVhbmVkUGFnZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgcGFnZU1hcC5wdXNoKHsgcGFnZTogaSArIDEsIHN0YXJ0OiBjaGFyUG9zLCBlbmQ6IGNoYXJQb3MgKyBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoIH0pO1xuICAgICAgICBjaGFyUG9zICs9IGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggKyAxO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBmdWxsVGV4dCA9IGNsZWFuZWRQYWdlcy5qb2luKCdcXG4nKTtcbiAgICAgIHJldHVybiB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH07XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ1BERiBwYXJzaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICAgIHRocm93IG5ldyBDb3JydXB0ZWRQREZFcnJvcigpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBHaXZlbiBhIGNodW5rJ3MgW2NoYXJTdGFydCwgY2hhckVuZCkgcmFuZ2UsIGZpbmQgd2hpY2ggcGFnZShzKSBpdFxuICAgKiBvdmVybGFwcy4gUmV0dXJucyB0aGUgbWFqb3JpdHkgcGFnZSAobW9zdCBvdmVybGFwcGluZyBjaGFycywgdXNlZFxuICAgKiBmb3IgYHBhZ2VfbnVtYmVyYCBmb3IgYmFja3dhcmQgY29tcGF0aWJpbGl0eSkgcGx1cyB0aGUgdHJ1ZSBzdGFydC9lbmRcbiAgICogcGFnZXMgc28gY2h1bmtzIHNwYW5uaW5nIGEgcGFnZSBicmVhayBhcmVuJ3Qgc2lsZW50bHkgbWlzbGFiZWxlZCB3aXRoXG4gICAqIGp1c3QgdGhlIGZpcnN0IHBhZ2UuXG4gICAqL1xuICBmdW5jdGlvbiBnZXRQYWdlUmFuZ2UoY2hhclN0YXJ0LCBjaGFyRW5kLCBwYWdlTWFwKSB7XG4gICAgbGV0IHN0YXJ0UGFnZSA9IG51bGw7XG4gICAgbGV0IGVuZFBhZ2UgPSBudWxsO1xuICAgIGxldCBiZXN0UGFnZSA9IG51bGw7XG4gICAgbGV0IG1heE92ZXJsYXAgPSAtMTtcblxuICAgIGZvciAoY29uc3QgZW50cnkgb2YgcGFnZU1hcCkge1xuICAgICAgY29uc3Qgb3ZlcmxhcFN0YXJ0ID0gTWF0aC5tYXgoY2hhclN0YXJ0LCBlbnRyeS5zdGFydCk7XG4gICAgICBjb25zdCBvdmVybGFwRW5kID0gTWF0aC5taW4oY2hhckVuZCwgZW50cnkuZW5kKTtcbiAgICAgIGNvbnN0IG92ZXJsYXAgPSBvdmVybGFwRW5kIC0gb3ZlcmxhcFN0YXJ0O1xuICAgICAgaWYgKG92ZXJsYXAgPD0gMCkgY29udGludWU7XG5cbiAgICAgIGlmIChzdGFydFBhZ2UgPT09IG51bGwpIHN0YXJ0UGFnZSA9IGVudHJ5LnBhZ2U7XG4gICAgICBlbmRQYWdlID0gZW50cnkucGFnZTtcblxuICAgICAgaWYgKG92ZXJsYXAgPiBtYXhPdmVybGFwKSB7XG4gICAgICAgIG1heE92ZXJsYXAgPSBvdmVybGFwO1xuICAgICAgICBiZXN0UGFnZSA9IGVudHJ5LnBhZ2U7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHN0YXJ0UGFnZSA9PT0gbnVsbCkge1xuICAgICAgY29uc3QgbGFzdFBhZ2UgPSBwYWdlTWFwW3BhZ2VNYXAubGVuZ3RoIC0gMV0/LnBhZ2UgfHwgMTtcbiAgICAgIHJldHVybiB7IHBhZ2U6IGxhc3RQYWdlLCBwYWdlU3RhcnQ6IGxhc3RQYWdlLCBwYWdlRW5kOiBsYXN0UGFnZSB9O1xuICAgIH1cblxuICAgIHJldHVybiB7IHBhZ2U6IGJlc3RQYWdlLCBwYWdlU3RhcnQ6IHN0YXJ0UGFnZSwgcGFnZUVuZDogZW5kUGFnZSB9O1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFVwbG9hZCBoYW5kbGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBleHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlVXBsb2FkKHJlcSwgcmVzKSB7XG4gICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gICAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICAgIHJlcy5mbHVzaEhlYWRlcnMoKTtcblxuICAgIGNvbnN0IEJBVENIX1NJWkUgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgMTA7XG4gICAgY29uc3QgUEFSQUxMRUxfQ0FMTFMgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfUEFSQUxMRUxfQ0FMTFMpIHx8IDEwO1xuICAgIGNvbnN0IEdST1VQX1dBSVRfTVMgPSBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfR1JPVVBfV0FJVF9NUykgfHwgMTtcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBmaWxlID0gcmVxLmZpbGU7XG4gICAgICBpZiAoIWZpbGUpIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuXG4gICAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLmJvZHkuc2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICAgICAgY29uc3Qgc2Vzc2lvbiA9IGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgY29uc3QgbWF4UERGcyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG4gICAgICBjb25zdCBjbGVhbkZpbGVuYW1lID0gc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSk7XG5cbiAgICAgIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgICAgIGlmICh1cGxvYWRlZENvdW50ID49IG1heFBERnMpIHtcbiAgICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogYE1heGltdW0gJHttYXhQREZzfSB1cGxvYWRzIHJlYWNoZWRgLCBjb2RlOiAnVE9PX01BTllfUERGUycgfSk7XG4gICAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgICB9XG5cbiAgICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogYFwiJHtjbGVhbkZpbGVuYW1lfVwiIGFscmVhZHkgdXBsb2FkZWRgLCBjb2RlOiAnRFVQTElDQVRFX0ZJTEUnIH0pO1xuICAgICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgICAgfVxuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMSBcdTIwMTQgcGFyc2luZyAke2NsZWFuRmlsZW5hbWV9ICgke2ZpbGUuc2l6ZX0gYnl0ZXMpYCk7XG4gICAgICBjb25zdCB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH0gPSBhd2FpdCBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlLnBhdGgpO1xuXG4gICAgICBpZiAoIWZ1bGxUZXh0IHx8IGZ1bGxUZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gZXh0cmFjdGFibGUgdGV4dCBcdTIwMTQgUERGIG1heSBiZSBzY2FubmVkIG9yIGltYWdlLW9ubHknLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgZG9jdW1lbnRJZCA9IHV1aWR2NCgpO1xuICAgICAgY29uc3QgcmF3Q2h1bmtzID0gY2h1bmtUZXh0KGZ1bGxUZXh0KTtcblxuICAgICAgaWYgKHJhd0NodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ05vIGNvbnRlbnQgY291bGQgYmUgZXh0cmFjdGVkIGZyb20gUERGJywgY29kZTogJ0VNUFRZX1BERicgfSk7XG4gICAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGNodW5rcyA9IHJhd0NodW5rcy5tYXAoKGNodW5rLCBpZHgpID0+IHtcbiAgICAgICAgY29uc3QgeyBwYWdlLCBwYWdlU3RhcnQsIHBhZ2VFbmQgfSA9IGdldFBhZ2VSYW5nZShjaHVuay5jaGFyU3RhcnQsIGNodW5rLmNoYXJFbmQsIHBhZ2VNYXApO1xuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19pZDogY3JlYXRlSGFzaCgnbWQ1JykudXBkYXRlKGAke2NsZWFuRmlsZW5hbWV9Ojoke2NodW5rLnRleHR9YCkuZGlnZXN0KCdoZXgnKS5zbGljZSgwLCAxNiksXG4gICAgICAgICAgICBjaHVua19pbmRleDogaWR4LFxuICAgICAgICAgICAgdG90YWxfY2h1bmtzOiByYXdDaHVua3MubGVuZ3RoLFxuICAgICAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2UsICAgICAgIC8vIG1ham9yaXR5IHBhZ2UgXHUyMDE0IGtlcHQgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICAgICAgICAgIHBhZ2Vfc3RhcnQ6IHBhZ2VTdGFydCwgICAvLyBuZXc6IGZpcnN0IHBhZ2UgdGhpcyBjaHVuayBvdmVybGFwc1xuICAgICAgICAgICAgcGFnZV9lbmQ6IHBhZ2VFbmQsICAgICAgIC8vIG5ldzogbGFzdCBwYWdlIHRoaXMgY2h1bmsgb3ZlcmxhcHNcbiAgICAgICAgICAgIHRvdGFsX3BhZ2VzOiB0b3RhbFBhZ2VzLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgICAgICAgICBzZXNzaW9uX2lkOiBzZXNzaW9uSWQsXG4gICAgICAgICAgICB1cGxvYWRfdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgICAgIHRva2VuX2NvdW50OiBjaHVuay50b2tlbkNvdW50XG4gICAgICAgICAgfVxuICAgICAgICB9O1xuICAgICAgfSk7XG5cbiAgICAgIGNvbnN0IHRvdGFsQ2h1bmtzID0gY2h1bmtzLmxlbmd0aDtcbiAgICAgIGNvbnN0IHRvdGFsQmF0Y2hlcyA9IE1hdGguY2VpbCh0b3RhbENodW5rcyAvIEJBVENIX1NJWkUpO1xuICAgICAgY29uc3QgdG90YWxTZXRzID0gTWF0aC5jZWlsKHRvdGFsQmF0Y2hlcyAvIFBBUkFMTEVMX0NBTExTKTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICR7dG90YWxDaHVua3N9IGNodW5rcyBcdTIxOTIgJHt0b3RhbEJhdGNoZXN9IEFQSSBjYWxscyBcdTIxOTIgJHt0b3RhbFNldHN9IHNldHMgb2YgJHtQQVJBTExFTF9DQUxMU30gcGFyYWxsZWxgKTtcblxuICAgICAgc3NlRXZlbnQocmVzLCAndXBsb2FkX2NvbXBsZXRlJywge1xuICAgICAgICBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCB0b3RhbENodW5rcywgdG90YWxCYXRjaGVzLCB0b3RhbFNldHNcbiAgICAgIH0pO1xuXG4gICAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IDAsIHN0YXR1czogJ2luZGV4aW5nJ1xuICAgICAgfSk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAxIGRvbmUgXHUyMDE0ICR7Y2xlYW5GaWxlbmFtZX0gYWRkZWQgdG8gc2Vzc2lvbiBhcyBpbmRleGluZ2ApO1xuXG4gICAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldENvbGxlY3Rpb24oKTtcbiAgICAgIGxldCBwcm9jZXNzZWRDaHVua3MgPSAwO1xuICAgICAgY29uc3QgYWxsRW1iZWRkaW5ncyA9IFtdO1xuXG4gICAgICBjb25zdCBiYXRjaGVzID0gW107XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gQkFUQ0hfU0laRSkgYmF0Y2hlcy5wdXNoKGNodW5rcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkpO1xuXG4gICAgICBjb25zdCBzZXRzID0gW107XG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IFBBUkFMTEVMX0NBTExTKSBzZXRzLnB1c2goYmF0Y2hlcy5zbGljZShpLCBpICsgUEFSQUxMRUxfQ0FMTFMpKTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDIgc3RhcnQgXHUyMDE0ICR7c2V0cy5sZW5ndGh9IHNldHNgKTtcblxuICAgICAgZm9yIChsZXQgc2V0SWR4ID0gMDsgc2V0SWR4IDwgc2V0cy5sZW5ndGg7IHNldElkeCsrKSB7XG4gICAgICAgIGNvbnN0IGlzTGFzdFNldCA9IHNldElkeCA9PT0gc2V0cy5sZW5ndGggLSAxO1xuICAgICAgICBjb25zdCBjdXJyZW50U2V0ID0gc2V0c1tzZXRJZHhdO1xuICAgICAgICBjb25zdCBzZXRDaHVua0NvdW50ID0gY3VycmVudFNldC5yZWR1Y2UoKGFjYywgYikgPT4gYWNjICsgYi5sZW5ndGgsIDApO1xuXG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTZXQgJHtzZXRJZHggKyAxfS8ke3NldHMubGVuZ3RofSBcdTIwMTQgZW1iZWRkaW5nICR7Y3VycmVudFNldC5sZW5ndGh9IGJhdGNoIGNhbGwocykgKCR7c2V0Q2h1bmtDb3VudH0gY2h1bmtzKSBpbiBwYXJhbGxlbGApO1xuXG4gICAgICAgIGNvbnN0IGVtYmVkUmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgICAgICBjdXJyZW50U2V0Lm1hcChiYXRjaCA9PiBlbWJlZFNpbmdsZUJhdGNoR3JvdXAoYmF0Y2gubWFwKGMgPT4gYy50ZXh0KSkpXG4gICAgICAgICk7XG5cbiAgICAgICAgY29uc3Qgc2V0RW1iZWRkaW5ncyA9IFtdO1xuICAgICAgICBlbWJlZFJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgICAgIGNvbnN0IGJhdGNoID0gY3VycmVudFNldFtiYXRjaElkeF07XG4gICAgICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgICAgICByZXN1bHQudmFsdWUuZm9yRWFjaCgodmVjdG9yLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgICAgICBzZXRFbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgICAgIGlkOiBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEuY2h1bmtfaWQsXG4gICAgICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3IsXG4gICAgICAgICAgICAgICAgbWV0YWRhdGE6IGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YSxcbiAgICAgICAgICAgICAgICB0ZXh0OiBiYXRjaFtjaHVua0lkeF0udGV4dFxuICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gZW1iZWRkZWQgT0sgKCR7YmF0Y2gubGVuZ3RofSBjaHVua3MpYCk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dICAgQmF0Y2ggJHtzZXRJZHggKiBQQVJBTExFTF9DQUxMUyArIGJhdGNoSWR4ICsgMX0gRkFJTEVEOmAsIHJlc3VsdC5yZWFzb24/Lm1lc3NhZ2UpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG5cbiAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzICs9IHNldEVtYmVkZGluZ3MubGVuZ3RoO1xuICAgICAgICBhbGxFbWJlZGRpbmdzLnB1c2goLi4uc2V0RW1iZWRkaW5ncyk7XG5cbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9IGVtYmVkZGVkIFx1MjAxNCAke3Byb2Nlc3NlZENodW5rc30vJHt0b3RhbENodW5rc30gY2h1bmtzIHNvIGZhcmApO1xuXG4gICAgICAgIGlmICghaXNMYXN0U2V0KSB7XG4gICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFN0YXJ0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyB0aW1lciArIENocm9tYSB3cml0ZSBjb25jdXJyZW50bHkgZm9yIHNldCAke3NldElkeCArIDF9YCk7XG4gICAgICAgICAgY29uc3QgdGltZXIgPSBuZXcgUHJvbWlzZShyID0+IHNldFRpbWVvdXQociwgR1JPVVBfV0FJVF9NUykpO1xuICAgICAgICAgIGNvbnN0IGNocm9tYVdyaXRlID0gYWRkVmVjdG9ycyhcbiAgICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICAgKS50aGVuKCgpID0+IGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0gKCR7c2V0RW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMpYCkpXG4gICAgICAgICAgICAuY2F0Y2goZXJyID0+IGNvbnNvbGUuZXJyb3IoYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBGQUlMRUQgZm9yIHNldCAke3NldElkeCArIDF9OmAsIGVyci5tZXNzYWdlKSk7XG5cbiAgICAgICAgICBzc2VFdmVudChyZXMsICdlbWJlZGRpbmdfcHJvZ3Jlc3MnLCB7XG4gICAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgICAgc2V0SW5kZXg6IHNldElkeCArIDEsIHRvdGFsU2V0cyxcbiAgICAgICAgICAgIHdhaXRpbmdNczogR1JPVVBfV0FJVF9NUywgY2hyb21hV3JpdGVDb21wbGV0ZTogZmFsc2VcbiAgICAgICAgICB9KTtcblxuICAgICAgICAgIGF3YWl0IFByb21pc2UuYWxsKFt0aW1lciwgY2hyb21hV3JpdGVdKTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gVGltZXIgKyBDaHJvbWEgYm90aCBkb25lIGZvciBzZXQgJHtzZXRJZHggKyAxfSwgcHJvY2VlZGluZyB0byBzZXQgJHtzZXRJZHggKyAyfWApO1xuXG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIExhc3Qgc2V0ICR7c2V0SWR4ICsgMX0gXHUyMDE0IGF3YWl0aW5nIENocm9tYSB3cml0ZSBkaXJlY3RseWApO1xuICAgICAgICAgIGF3YWl0IGFkZFZlY3RvcnMoXG4gICAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5lbWJlZGRpbmcpLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICAgICk7XG4gICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBjb21wbGV0ZSBmb3IgbGFzdCBzZXQgKCR7c2V0RW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMpYCk7XG5cbiAgICAgICAgICBzc2VFdmVudChyZXMsICdlbWJlZGRpbmdfcHJvZ3Jlc3MnLCB7XG4gICAgICAgICAgICBwcm9jZXNzZWRDaHVua3MsIHRvdGFsQ2h1bmtzLFxuICAgICAgICAgICAgc2V0SW5kZXg6IHNldElkeCArIDEsIHRvdGFsU2V0cyxcbiAgICAgICAgICAgIHdhaXRpbmdNczogMCwgY2hyb21hV3JpdGVDb21wbGV0ZTogdHJ1ZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogYWxsRW1iZWRkaW5ncy5sZW5ndGgsIHN0YXR1czogJ3JlYWR5J1xuICAgICAgfSk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBcdTI3MDUgRG9uZSBcdTIwMTQgJHthbGxFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycyBpbiBDaHJvbWEgZm9yICR7Y2xlYW5GaWxlbmFtZX1gKTtcblxuICAgICAgc3NlRXZlbnQocmVzLCAnZG9uZScsIHtcbiAgICAgICAgZG9jdW1lbnQ6IHtcbiAgICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCxcbiAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgICB9LFxuICAgICAgICBzZXNzaW9uSWRcbiAgICAgIH0pO1xuXG4gICAgICByZXMuZW5kKCk7XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgaWYgKHJlcS5maWxlICYmIGZzLmV4aXN0c1N5bmMocmVxLmZpbGUucGF0aCkpIHtcbiAgICAgICAgdHJ5IHsgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTsgfSBjYXRjaCB7IH1cbiAgICAgIH1cbiAgICAgIGNvbnNvbGUuZXJyb3IoJ1t1cGxvYWRdIFVuaGFuZGxlZCBlcnJvcjonLCBlcnJvcik7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogZXJyb3IubWVzc2FnZSB8fCAnVXBsb2FkIGZhaWxlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1VQTE9BRF9FUlJPUicgfSk7XG4gICAgICByZXMuZW5kKCk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFNTRTogU2VlZGluZyBzdGF0dXMgc3RyZWFtIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBleHBvcnQgYXN5bmMgZnVuY3Rpb24gc2VlZGluZ1N0YXR1c0hhbmRsZXIocmVxLCByZXMpIHtcbiAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gICAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gICAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgICBpZiAoIXNlc3Npb25JZCkge1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdNaXNzaW5nIHNlc3Npb24gSUQnLCBjb2RlOiAnTUlTU0lOR19TRVNTSU9OJyB9KTtcbiAgICAgIHJlcy5lbmQoKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBDbGllbnQgY29ubmVjdGVkIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuXG4gICAgLy8gQ2hlY2sgaWYgc2Vzc2lvbiBpcyBhbHJlYWR5IHNlZWRlZFxuICAgIGNvbnN0IHNlZWRlZCA9IGlzU2Vzc2lvblNlZWRlZChzZXNzaW9uSWQpO1xuICAgIGlmIChzZWVkZWQpIHtcbiAgICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIFNlc3Npb24gJHtzZXNzaW9uSWR9IGFscmVhZHkgc2VlZGVkIFx1MjAxMyByZXR1cm5pbmcgaW1tZWRpYXRlbHlgKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ3NlZWRpbmdfY29tcGxldGUnLCB7IHNlc3Npb25JZCwgc2VlZGVkOiB0cnVlIH0pO1xuICAgICAgcmVzLmVuZCgpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIC8vIENyZWF0ZSBhIGxpc3RlbmVyIGZvciB0aGlzIHNlc3Npb25cbiAgICBjb25zdCBldmVudEtleSA9IGBzZWVkaW5nOiR7c2Vzc2lvbklkfWA7XG5cbiAgICAvLyBTdG9yZSB0aGUgbGlzdGVuZXIgc28gd2UgY2FuIGVtaXQgd2hlbiBzZWVkaW5nIGNvbXBsZXRlc1xuICAgIGlmICghZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMpIHtcbiAgICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzID0gbmV3IE1hcCgpO1xuICAgIH1cbiAgICBpZiAoIWdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmhhcyhldmVudEtleSkpIHtcbiAgICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLnNldChldmVudEtleSwgW10pO1xuICAgIH1cbiAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpLnB1c2gocmVzKTtcblxuICAgIC8vIENsZWFuIHVwIGxpc3RlbmVyIG9uIGNsaWVudCBkaXNjb25uZWN0XG4gICAgcmVxLm9uKCdjbG9zZScsICgpID0+IHtcbiAgICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgICBjb25zdCBpZHggPSBsaXN0ZW5lcnMuaW5kZXhPZihyZXMpO1xuICAgICAgaWYgKGlkeCA+PSAwKSB7XG4gICAgICAgIGxpc3RlbmVycy5zcGxpY2UoaWR4LCAxKTtcbiAgICAgICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gQ2xpZW50IGRpc2Nvbm5lY3RlZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICB9XG4gICAgICBpZiAobGlzdGVuZXJzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5kZWxldGUoZXZlbnRLZXkpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgLy8gU3RhcnQgc2VlZGluZyBpbiB0aGUgYmFja2dyb3VuZCAoaWYgbm90IGFscmVhZHkgcnVubmluZylcbiAgICB0cnkge1xuICAgICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gVHJpZ2dlcmluZyBzZWVkaW5nIGZvciAke3Nlc3Npb25JZH0uLi5gKTtcbiAgICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICAgIC8vIFRoZSBzZWVkaW5nIGZ1bmN0aW9uIHdpbGwgbm90aWZ5IGxpc3RlbmVycyB3aGVuIGNvbXBsZXRlXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBjb25zb2xlLmVycm9yKGBbc2VlZGluZy1zdGF0dXNdIFNlZWRpbmcgZmFpbGVkIGZvciAke3Nlc3Npb25JZH06YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICAgIGxpc3RlbmVycy5mb3JFYWNoKChyZXNwb25zZSkgPT4ge1xuICAgICAgICBzc2VFdmVudChyZXNwb25zZSwgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnIubWVzc2FnZSwgY29kZTogJ1NFRURfRkFJTEVEJyB9KTtcbiAgICAgICAgcmVzcG9uc2UuZW5kKCk7XG4gICAgICB9KTtcbiAgICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIExpc3QgZG9jdW1lbnRzIGhhbmRsZXIgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuICAgIHRyeSB7XG4gICAgICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICAgIGNvbnN0IGRvY3VtZW50cyA9IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgICAgcmVzLmpzb24oZG9jdW1lbnRzKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGRvY3VtZW50cycsIGNvZGU6ICdMSVNUX0VSUk9SJyB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgRGVsZXRlIGRvY3VtZW50IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBleHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnQocmVxLCByZXMpIHtcbiAgICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gICAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgICB0cnkge1xuICAgICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgICB0cnkge1xuICAgICAgICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0Q29sbGVjdGlvbigpO1xuICAgICAgICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICAgICAgICBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgICAgICAgfVxuICAgICAgICB9IGNhdGNoIChjaHJvbWFFcnIpIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYFtkZWxldGVdIENocm9tYSBkZWxldGUgZmFpbGVkIGZvciAke2RvY3VtZW50SWR9OmAsIGNocm9tYUVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuXG4gICAgICAgIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKTtcblxuICAgICAgICBjbGVhck1lbW9yeShzZXNzaW9uSWQpO1xuICAgICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gQ2xlYXJlZCBtZW1vcnkgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgICB9XG5cbiAgICAgIGlmIChmaWxlbmFtZSkge1xuICAgICAgICBjb25zdCBmaWxlUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoZmlsZVBhdGgpKSB7XG4gICAgICAgICAgZnMudW5saW5rU3luYyhmaWxlUGF0aCk7XG4gICAgICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIFJlbW92ZWQgZmlsZTogJHtmaWxlUGF0aH1gKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zb2xlLndhcm4oYFtkZWxldGVdIEZpbGUgbm90IGZvdW5kIG9uIGRpc2s6ICR7ZmlsZVBhdGh9YCk7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgcmVzLmpzb24oeyBzdWNjZXNzOiB0cnVlLCBkb2N1bWVudElkIH0pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdEZWxldGUgZG9jdW1lbnQgZXJyb3I6JywgZXJyb3IpO1xuICAgICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQnLCBjb2RlOiAnREVMRVRFX0VSUk9SJyB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgR2V0IGRvY3VtZW50IGZpbGUgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXREb2N1bWVudEZpbGUocmVxLCByZXMpIHtcbiAgICBjb25zdCBmaWxlbmFtZSA9IHJlcS5xdWVyeS5maWxlbmFtZTtcblxuICAgIHRyeSB7XG4gICAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgICAgY29uc3QgdXBsb2FkUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGZpbGVuYW1lKTtcbiAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmModXBsb2FkUGF0aCkpIHtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihmaWxlbmFtZSkpO1xuICAgICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHVwbG9hZFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHNlZWRQYXRoID0gcGF0aC5qb2luKHNlZWREaXIsIGZpbGVuYW1lKTtcbiAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZFBhdGgpKSB7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShzZWVkUGF0aCkucGlwZShyZXMpO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZERpcikpIHtcbiAgICAgICAgICBjb25zdCBhbGxQZGZzID0gZnMucmVhZGRpclN5bmMoc2VlZERpcikuZmlsdGVyKGYgPT4gZi5lbmRzV2l0aCgnLnBkZicpKTtcbiAgICAgICAgICBjb25zdCBtYXRjaCA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICAgIGlmIChtYXRjaCkge1xuICAgICAgICAgICAgY29uc3QgbWF0Y2hQYXRoID0gcGF0aC5qb2luKHNlZWREaXIsIG1hdGNoKTtcbiAgICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24obWF0Y2gpKTtcbiAgICAgICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKG1hdGNoUGF0aCkucGlwZShyZXMpO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignR2V0IGRvY3VtZW50IGZpbGUgZXJyb3I6JywgZXJyb3IpO1xuICAgICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFJvdXRlcyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgcm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG4gIHJvdXRlci5nZXQoJy8nLCBsaXN0RG9jdW1lbnRzSGFuZGxlcik7XG4gIHJvdXRlci5nZXQoJy9zZWVkaW5nLXN0YXR1cycsIHNlZWRpbmdTdGF0dXNIYW5kbGVyKTtcbiAgcm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xuICByb3V0ZXIuZ2V0KCcvOmRvY3VtZW50SWQvZmlsZScsIGdldERvY3VtZW50RmlsZSk7XG5cbiAgZXhwb3J0IGRlZmF1bHQgcm91dGVyOyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2ltcG9ydCB7IGdldENvbGxlY3Rpb24sIGh5YnJpZFF1ZXJ5Q29sbGVjdGlvbiB9IGZyb20gJy4vY2hyb21hU2VydmljZS5qcyc7XG5pbXBvcnQgeyBlbWJlZFF1ZXJ5IH0gZnJvbSAnLi9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBUT1BfSyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LlRPUF9LKSB8fCAyMDtcbmNvbnN0IFJFRlVTQUxfVEhSRVNIT0xEID0gcGFyc2VGbG9hdChwcm9jZXNzLmVudi5SRUZVU0FMX1RIUkVTSE9MRCkgfHwgMC4wNTtcblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IDUpIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4geyBjb25maWRlbmNlOiAwLCB0b3BTY29yZTogMCB9O1xuICBjb25zdCBzY29yZXMgPSByZXN1bHRzLnNsaWNlKDAsIHRvcEspLm1hcChyID0+IE1hdGgubWF4KDAsIHIuc2NvcmUpKTtcbiAgY29uc3QgYXZnU2NvcmUgPSBzY29yZXMucmVkdWNlKChhLCBiKSA9PiBhICsgYiwgMCkgLyBzY29yZXMubGVuZ3RoO1xuICByZXR1cm4ge1xuICAgIGNvbmZpZGVuY2U6IE1hdGgucm91bmQoYXZnU2NvcmUgKiAxMDApLFxuICAgIHRvcFNjb3JlOiBNYXRoLm1heCguLi5zY29yZXMpXG4gIH07XG59XG5cbi8vIFx1MjUwMFx1MjUwMCBNYWluIHJldHJpZXZhbCBmdW5jdGlvbiAoSHlicmlkOiBkZW5zZSArIEJNMjUgdmlhIENocm9tYSBSUkYpIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJldHJpZXZlRm9yUXVlcnkocXVlcnksIHNlc3Npb25JZCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHRvcEsgPSBvcHRpb25zLnRvcEsgfHwgNTtcblxuICB0cnkge1xuICAgIC8vIFx1MjUwMFx1MjUwMCBUaW1pbmc6IGVtYmVkZGluZyBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgICBjb25zdCB0RW1iZWRTdGFydCA9IHBlcmZvcm1hbmNlLm5vdygpO1xuICAgIGxldCB0RW1iZWRFbmQ7XG4gICAgY29uc3QgW3F1ZXJ5RW1iZWRkaW5nLCB7IGNvbGxlY3Rpb24gfV0gPSBhd2FpdCBQcm9taXNlLmFsbChbXG4gICAgICBlbWJlZFF1ZXJ5KHF1ZXJ5KS50aGVuKHJlc3VsdCA9PiB7IHRFbWJlZEVuZCA9IHBlcmZvcm1hbmNlLm5vdygpOyByZXR1cm4gcmVzdWx0OyB9KSxcbiAgICAgIGdldENvbGxlY3Rpb24oKVxuICAgIF0pO1xuICAgIGNvbnN0IGVtYmVkZGluZ01zID0gdEVtYmVkRW5kIC0gdEVtYmVkU3RhcnQ7XG5cbiAgICBpZiAoIWNvbGxlY3Rpb24pIHtcbiAgICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICBObyBjb2xsZWN0aW9uIGF2YWlsYWJsZWApO1xuICAgICAgcmV0dXJuIHsgcmVzdWx0czogW10sIGNvdmVyYWdlOiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwLCBsZXZlbDogJ2xvdycsIHNjb3JlOiAwIH0sIHF1ZXJ5RW1iZWRkaW5nLCB0aW1pbmdzOiB7IGVtYmVkZGluZ01zLCByZXRyaWV2YWxNczogMCB9IH07XG4gICAgfVxuXG4gICAgLy8gQnVpbGQgbWV0YWRhdGEgZmlsdGVyOiBpbmNsdWRlIGJvdGggJ2dsb2JhbCcgdmVjdG9ycyBhbmQgdGhpcyBzZXNzaW9uJ3MgdmVjdG9yc1xuICAgIGNvbnN0IHdoZXJlID0gc2Vzc2lvbklkXG4gICAgICA/IHsgc2Vzc2lvbl9pZDogeyBcIiRpblwiOiBbXCJnbG9iYWxcIiwgc2Vzc2lvbklkXSB9IH1cbiAgICAgIDogeyBzZXNzaW9uX2lkOiBcImdsb2JhbFwiIH07XG5cbiAgICAvLyBcdTI1MDBcdTI1MDAgVGltaW5nOiByZXRyaWV2YWwgKENocm9tYSBzZWFyY2gpIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICAgIGNvbnN0IHRSZXRyaWV2YWxTdGFydCA9IHBlcmZvcm1hbmNlLm5vdygpO1xuICAgIGNvbnN0IHJhd1Jlc3VsdHMgPSBhd2FpdCBoeWJyaWRRdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnksIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLLCB3aGVyZSk7XG4gICAgY29uc3QgcmV0cmlldmFsTXMgPSBwZXJmb3JtYW5jZS5ub3coKSAtIHRSZXRyaWV2YWxTdGFydDtcblxuICAgIGNvbnN0IHJlc3VsdHMgPSByYXdSZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICAuLi5yLFxuICAgICAgc291cmNlX3R5cGU6IHIubWV0YWRhdGE/LnNvdXJjZV90eXBlIHx8ICdzZXNzaW9uJ1xuICAgIH0pKTtcblxuICAgIGNvbnN0IGNvdmVyYWdlID0gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyk7XG4gICAgY29uc3QgdG9wU2NvcmUgPSBjb3ZlcmFnZS50b3BTY29yZTtcbiAgICBjb25zdCBsZXZlbCA9IHRvcFNjb3JlID49IDAuNiA/ICdoaWdoJyA6IHRvcFNjb3JlID49IDAuMyA/ICdtZWRpdW0nIDogJ2xvdyc7XG5cbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVERDBEIFF1ZXJ5OicsIHF1ZXJ5KTtcbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVEQ0NBIENvdmVyYWdlOicsIHsgLi4uY292ZXJhZ2UsIGxldmVsIH0pO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQzggU2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZyxcbiAgICAgIHRpbWluZ3M6IHsgZW1iZWRkaW5nTXMsIHJldHJpZXZhbE1zIH1cbiAgICB9O1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUmV0cmlldmFsIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJyA/ICdbU2Vzc2lvbiBVcGxvYWRdJyA6ICdbU2VlZCBEb2N1bWVudF0nO1xuICAgIGNvbnN0IHBhZ2UgPSByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIgPyBgIChQYWdlICR7cmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyfSlgIDogJyc7XG4gICAgY29udGV4dFBhcnRzLnB1c2goYFske2kgKyAxfV0gJHtzb3VyY2VMYWJlbH0gJHtyZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfSR7cGFnZX06XFxuJHtyZXN1bHQudGV4dH1gKTtcbiAgfVxuXG4gIHJldHVybiBjb250ZXh0UGFydHMuam9pbignXFxuXFxuLS0tXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuICByZXR1cm4gcmVzdWx0cy5tYXAoKHJlc3VsdCwgaWR4KSA9PiAoe1xuICAgIGlkOiB1dWlkdjQoKSxcbiAgICBpbmRleDogaWR4ICsgMSxcbiAgICBkb2N1bWVudElkOiByZXN1bHQubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgZmlsZW5hbWU6IHJlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICBwYWdlTnVtYmVyOiByZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgc2VjdGlvbjogcmVzdWx0Lm1ldGFkYXRhLnNlY3Rpb25fdGl0bGUsXG4gICAgZXhjZXJwdDogcmVzdWx0LnRleHQsXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9O1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmFpJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGNvbnN0IHByb2plY3QgPSBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJztcbiAgICBjb25zdCBsb2NhdGlvbiA9ICdnbG9iYWwnO1xuXG4gICAgLy8gU3VwcG9ydCBjcmVkZW50aWFscyBmcm9tIGVudiB2YXIgKGZvciBzZXJ2ZXJsZXNzKSBvciBmaWxlIChmb3IgbG9jYWwgZGV2KVxuICAgIGNvbnN0IGNyZWRlbnRpYWxzSnNvbiA9IHByb2Nlc3MuZW52LkdPT0dMRV9DUkVERU5USUFMU19KU09OO1xuXG4gICAgaWYgKGNyZWRlbnRpYWxzSnNvbikge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgY3JlZGVudGlhbHMgPSBKU09OLnBhcnNlKGNyZWRlbnRpYWxzSnNvbik7XG4gICAgICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgICAgICB2ZXJ0ZXhhaTogdHJ1ZSxcbiAgICAgICAgICBwcm9qZWN0LFxuICAgICAgICAgIGxvY2F0aW9uLFxuICAgICAgICAgIGNyZWRlbnRpYWxzXG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm4gZ2VuQUk7XG4gICAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgIGNvbnNvbGUud2FybignRmFpbGVkIHRvIHBhcnNlIEdPT0dMRV9DUkVERU5USUFMU19KU09OLCBmYWxsaW5nIGJhY2sgdG8gZGVmYXVsdCBhdXRoJyk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuQUkoe1xuICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICBwcm9qZWN0LFxuICAgICAgbG9jYXRpb25cbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIFBSSU1BUllfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWxOYW1lKCkge1xuICByZXR1cm4gRkFMTEJBQ0tfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tQ2h1bmsoY2h1bmspIHtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ3N0cmluZycpIHJldHVybiBjaHVuay50ZXh0O1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnZnVuY3Rpb24nKSByZXR1cm4gY2h1bmsudGV4dCgpO1xuICByZXR1cm4gJyc7XG59XG5cbmZ1bmN0aW9uIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWwsIHByb21wdCkge1xuICByZXR1cm4ge1xuICAgIG1vZGVsLFxuICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgY29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH1cbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1SZXNwb25zZShwcm9tcHQpIHtcbiAgbGV0IG1vZGVsTmFtZSA9IGdldFByaW1hcnlNb2RlbE5hbWUoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICBsZXQgZmlyc3RUb2tlblRpbWVvdXQgPSBudWxsO1xuICAgIGxldCByZXF1ZXN0VGltZW91dElkID0gbnVsbDtcbiAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgdHJ5IHtcbiAgICAgIHJlcXVlc3RUaW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICAgICAgY29uc3QgcmVzcG9uc2VTdHJlYW0gPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oXG4gICAgICAgIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWxOYW1lLCBwcm9tcHQpLFxuICAgICAgICB7IHNpZ25hbDogY29udHJvbGxlci5zaWduYWwgfVxuICAgICAgKTtcblxuICAgICAgaWYgKCFyZXNwb25zZVN0cmVhbSB8fCB0eXBlb2YgcmVzcG9uc2VTdHJlYW1bU3ltYm9sLmFzeW5jSXRlcmF0b3JdICE9PSAnZnVuY3Rpb24nKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgU3RyZWFtaW5nIHVuYXZhaWxhYmxlIGZvciBtb2RlbCAke21vZGVsTmFtZX1gKTtcbiAgICAgIH1cblxuICAgICAgbGV0IGZpcnN0VG9rZW4gPSB0cnVlO1xuICAgICAgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzcG9uc2VTdHJlYW0pIHtcbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ1N0cmVhbSBleGVjdXRpb24gYWJvcnRlZCBieSB0aW1lb3V0IGNvbnN0cmFpbnQuJyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCB0ZXh0ID0gZ2V0VGV4dEZyb21DaHVuayhjaHVuayk7XG4gICAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgICAgaWYgKGZpcnN0VG9rZW4pIHtcbiAgICAgICAgICAgIGZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBjbGVhclRpbWVvdXQocmVxdWVzdFRpbWVvdXRJZCk7XG4gICAgICByZXR1cm47XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgcmV0cmllcysrO1xuXG4gICAgICBpZiAoZmlyc3RUb2tlblRpbWVvdXQpIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBpZiAocmVxdWVzdFRpbWVvdXRJZCkgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuXG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgbW9kZWxOYW1lID0gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKTtcbiAgICB9XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3N1cGFiYXNlU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanNcIjtpbXBvcnQgeyBjcmVhdGVDbGllbnQgfSBmcm9tICdAc3VwYWJhc2Uvc3VwYWJhc2UtanMnO1xuXG5jb25zdCBzdXBhYmFzZVVybCA9IHByb2Nlc3MuZW52LlZJVEVfU1VQQUJBU0VfVVJMIHx8IHByb2Nlc3MuZW52LlNVUEFCQVNFX1VSTDtcbmNvbnN0IHN1cGFiYXNlS2V5ID0gcHJvY2Vzcy5lbnYuVklURV9TVVBBQkFTRV9BTk9OX0tFWSB8fCBwcm9jZXNzLmVudi5TVVBBQkFTRV9BTk9OX0tFWTtcblxuaWYgKCFzdXBhYmFzZVVybCB8fCAhc3VwYWJhc2VLZXkpIHtcbiAgY29uc29sZS53YXJuKCdTdXBhYmFzZSBVUkwgb3IgS2V5IGlzIG1pc3NpbmcuIERhdGFiYXNlIG9wZXJhdGlvbnMgd2lsbCBub3Qgd29yayBwcm9wZXJseS4nKTtcbn1cblxuZXhwb3J0IGNvbnN0IHN1cGFiYXNlID0gY3JlYXRlQ2xpZW50KFxuICBzdXBhYmFzZVVybCB8fCAnaHR0cDovL2xvY2FsaG9zdCcsXG4gIHN1cGFiYXNlS2V5IHx8ICdwdWJsaWMtYW5vbi1rZXknXG4pO1xuXG4vLyBNYXAgdG8gdHJhY2sgdGhlIGxhc3QgaW5zZXJ0aW9uIHByb21pc2UgcGVyIHNlc3Npb25cbmNvbnN0IHNlc3Npb25JbnNlcnRQcm9taXNlcyA9IG5ldyBNYXAoKTtcblxuLyoqXG4gKiBSZWN1cnNpdmVseSByZW1vdmVzIG51bGwgYnl0ZXMgKFxcdTAwMDApIGZyb20gc3RyaW5ncywgYXJyYXlzLCBvciBvYmplY3RzLlxuICogUG9zdGdyZVNRTCAoU3VwYWJhc2UpIGRvZXMgbm90IHN1cHBvcnQgXFx1MDAwMCBpbiB0ZXh0L2pzb25iIGZpZWxkcy5cbiAqL1xuZnVuY3Rpb24gc2FuaXRpemVOdWxsQnl0ZXModmFsKSB7XG4gIGlmICh0eXBlb2YgdmFsID09PSAnc3RyaW5nJykge1xuICAgIHJldHVybiB2YWwucmVwbGFjZSgvXFx1MDAwMC9nLCAnJyk7XG4gIH1cbiAgaWYgKEFycmF5LmlzQXJyYXkodmFsKSkge1xuICAgIHJldHVybiB2YWwubWFwKHNhbml0aXplTnVsbEJ5dGVzKTtcbiAgfVxuICBpZiAodmFsICE9PSBudWxsICYmIHR5cGVvZiB2YWwgPT09ICdvYmplY3QnKSB7XG4gICAgY29uc3QgY2xlYW5PYmogPSB7fTtcbiAgICBmb3IgKGNvbnN0IGtleSBvZiBPYmplY3Qua2V5cyh2YWwpKSB7XG4gICAgICBjbGVhbk9ialtrZXldID0gc2FuaXRpemVOdWxsQnl0ZXModmFsW2tleV0pO1xuICAgIH1cbiAgICByZXR1cm4gY2xlYW5PYmo7XG4gIH1cbiAgcmV0dXJuIHZhbDtcbn1cblxuLyoqXG4gKiBBc3luY2hyb25vdXNseSBpbnNlcnRzIGNvbnZlcnNhdGlvbiBkYXRhIGludG8gU3VwYWJhc2UuXG4gKiBDaGFpbnMgaW5zZXJ0aW9ucyBmb3IgdGhlIHNhbWUgc2Vzc2lvbiB0byBlbnN1cmUgdGhleSBjb21wbGV0ZSBpbiBvcmRlci5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGluc2VydENvbnZlcnNhdGlvbkFzeW5jKHNlc3Npb25JZCwgZGF0YSkge1xuICBjb25zdCBwcmV2aW91c1Byb21pc2UgPSBzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuZ2V0KHNlc3Npb25JZCkgfHwgUHJvbWlzZS5yZXNvbHZlKCk7XG5cbiAgY29uc3QgbmV4dFByb21pc2UgPSBwcmV2aW91c1Byb21pc2VcbiAgICAudGhlbihhc3luYyAoKSA9PiB7XG4gICAgICBjb25zdCBjbGVhbkRhdGEgPSBzYW5pdGl6ZU51bGxCeXRlcyhkYXRhKTtcbiAgICAgIGNvbnNvbGUubG9nKGBbU3VwYWJhc2VdIEluc2VydGluZyBjb252ZXJzYXRpb24gZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9LCBhbnN3ZXJfa2V5OiAke2NsZWFuRGF0YS5hbnN3ZXJfa2V5fWApO1xuICAgICAgY29uc3QgeyBlcnJvciB9ID0gYXdhaXQgc3VwYWJhc2UuZnJvbSgnQ29udmVyc2F0aW9uX0hpc3RvcnknKS5pbnNlcnQoY2xlYW5EYXRhKTtcbiAgICAgIGlmIChlcnJvcikge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdbU3VwYWJhc2VdIEVycm9yIGluc2VydGluZyBjb252ZXJzYXRpb24gaGlzdG9yeTonLCBlcnJvcik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLmxvZyhgW1N1cGFiYXNlXSBTdWNjZXNzZnVsbHkgaW5zZXJ0ZWQgY29udmVyc2F0aW9uIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgIH0pXG4gICAgLmNhdGNoKChlcnIpID0+IHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ1tTdXBhYmFzZV0gVW5leHBlY3RlZCBlcnJvciBkdXJpbmcgaW5zZXJ0aW9uIGNoYWluOicsIGVycik7XG4gICAgfSk7XG5cbiAgc2Vzc2lvbkluc2VydFByb21pc2VzLnNldChzZXNzaW9uSWQsIG5leHRQcm9taXNlKTtcblxuICAvLyBPcHRpb25hbDogY2xlYW4gdXAgdGhlIHByb21pc2UgZnJvbSB0aGUgbWFwIGlmIGl0J3MgdGhlIGxhc3Qgb25lXG4gIG5leHRQcm9taXNlLmZpbmFsbHkoKCkgPT4ge1xuICAgIGlmIChzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuZ2V0KHNlc3Npb25JZCkgPT09IG5leHRQcm9taXNlKSB7XG4gICAgICBzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuZGVsZXRlKHNlc3Npb25JZCk7XG4gICAgfVxuICB9KTtcblxuICByZXR1cm4gbmV4dFByb21pc2U7XG59XG5cbi8qKlxuICogQXN5bmNocm9ub3VzbHkgdXBkYXRlcyB0aGUgZmVlZGJhY2sgZm9yIGEgY29udmVyc2F0aW9uIGluIFN1cGFiYXNlLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJLZXksIGZlZWRiYWNrLCByZXRyaWVzID0gMikge1xuICB0cnkge1xuICAgIGNvbnN0IHsgZXJyb3IgfSA9IGF3YWl0IHN1cGFiYXNlXG4gICAgICAuZnJvbSgnQ29udmVyc2F0aW9uX0hpc3RvcnknKVxuICAgICAgLnVwZGF0ZSh7IGZlZWRiYWNrIH0pXG4gICAgICAuZXEoJ2Fuc3dlcl9rZXknLCBhbnN3ZXJLZXkpO1xuXG4gICAgaWYgKGVycm9yKSB7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc29sZS5sb2coYFtTdXBhYmFzZV0gU3VjY2Vzc2Z1bGx5IHVwZGF0ZWQgZmVlZGJhY2sgZm9yIGFuc3dlcl9rZXk6ICR7YW5zd2VyS2V5fWApO1xuICAgIH1cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zdCBpc05ldHdvcmtFcnJvciA9IGVycm9yLm1lc3NhZ2UgJiYgZXJyb3IubWVzc2FnZS5pbmNsdWRlcygnZmV0Y2ggZmFpbGVkJyk7XG4gICAgaWYgKGlzTmV0d29ya0Vycm9yICYmIHJldHJpZXMgPiAwKSB7XG4gICAgICAvL2NvbnNvbGUud2FybihgW1N1cGFiYXNlXSBOZXR3b3JrIGVycm9yIGR1cmluZyB1cGRhdGUsIHJldHJ5aW5nLi4uICgke3JldHJpZXN9IGF0dGVtcHRzIGxlZnQpYCk7XG4gICAgICAvLyBXYWl0IGJyaWVmbHkgYmVmb3JlIHJldHJ5aW5nIChlLmcuLCA1MDBtcylcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlcyA9PiBzZXRUaW1lb3V0KHJlcywgNTAwKSk7XG4gICAgICByZXR1cm4gdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJLZXksIGZlZWRiYWNrLCByZXRyaWVzIC0gMSk7XG4gICAgfVxuICAgIC8vY29uc29sZS5lcnJvcignW1N1cGFiYXNlXSBFcnJvciB1cGRhdGluZyBmZWVkYmFjazonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgZm9ybWF0Q29udGV4dEZvclByb21wdCB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgc3RyZWFtUmVzcG9uc2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBnZXRSZWNlbnRUdXJucyB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBnZXREZWxldGVkRG9jdW1lbnRJZHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBpbnNlcnRDb252ZXJzYXRpb25Bc3luYywgdXBkYXRlRmVlZGJhY2tBc3luYyB9IGZyb20gJy4uL3NlcnZpY2VzL3N1cGFiYXNlU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBPVVRfT0ZfU0NPUEVfUEFUVEVSTiA9IC9kb24ndCBoYXZlIGluZm9ybWF0aW9ufGRvIG5vdCBoYXZlIGluZm9ybWF0aW9ufG5vdCBpbiBteSBrbm93bGVkZ2V8Y2FuJ3QgZmluZHxjYW5ub3QgZmluZHxubyBpbmZvcm1hdGlvbnxrbm93bGVkZ2UgYmFzZSBkb2Vzbid0fG5vdCBjb3ZlcmVkfG91dHNpZGUuKmtub3dsZWRnZS9pO1xuXG5mdW5jdGlvbiBjbGVhbkV4Y2VycHQodGV4dCkge1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC8oPzwhXFx3KShbQS1aYS16XSlcXHMoW0EtWmEtel0pXFxzKFtBLVphLXpdKShcXHNbQS1aYS16XSkqL2csIChtYXRjaCkgPT5cbiAgICAgIG1hdGNoLnJlcGxhY2UoL1xccy9nLCAnJylcbiAgICApXG4gICAgLnJlcGxhY2UoL1xcc3syLH0vZywgJyAnKVxuICAgIC5yZXBsYWNlKC9eXFwqXFxzKi8sICcnKVxuICAgIC50cmltKCk7XG59XG5cbi8vIElzc3VlIDQgZml4OiByZW1vdmUgZG9tYWluSGludCBcdTIwMTQgc2hvcnQgcXVlcmllcyBubyBsb25nZXIgaW5oZXJpdCBwcmV2aW91cyBjb252ZXJzYXRpb24gY29udGV4dFxuZnVuY3Rpb24gZXhwYW5kUXVlcnkocXVlcnkpIHtcbiAgY29uc3Qgd29yZHMgPSBxdWVyeS50cmltKCkuc3BsaXQoL1xccysvKTtcbiAgaWYgKHdvcmRzLmxlbmd0aCA+IDQpIHJldHVybiBxdWVyeTtcblxuICBjb25zdCBleHBhbnNpb25zID0gW1xuICAgICdkZWZpbml0aW9uJywgJ292ZXJ2aWV3JywgJ3JvbGUnLCAncmVzcG9uc2liaWxpdGllcycsXG4gICAgJ2V4YW1wbGVzJywgJ2tleSBjb25jZXB0cycsICdob3cgaXQgd29ya3MnLCAncHVycG9zZSdcbiAgXTtcblxuICByZXR1cm4gYCR7cXVlcnl9ICR7ZXhwYW5zaW9ucy5qb2luKCcgJyl9YDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZUNoYXRTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSwgc2Vzc2lvbklkOiBwcm92aWRlZFNlc3Npb25JZCwgY29udklkOiBwcm92aWRlZENvbnZJZCwgbWVzc2FnZUlkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLCBjb2RlOiAnTUlTU0lOR19RVUVSWScgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgY29udklkID0gcHJvdmlkZWRDb252SWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkID0gbWVzc2FnZUlkIHx8IHV1aWR2NCgpO1xuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICB0cnkge1xuICAgIGNvbnN0IHRRdWVyeVN0YXJ0ID0gcGVyZm9ybWFuY2Uubm93KCk7XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCBleHBhbmRlZFF1ZXJ5ID0gZXhwYW5kUXVlcnkocXVlcnkpO1xuICAgIGNvbnN0IHsgcmVzdWx0cywgY292ZXJhZ2UsIHRpbWluZ3MgfSA9IGF3YWl0IHJldHJpZXZlRm9yUXVlcnkoZXhwYW5kZWRRdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG4gICAgY29uc3QgdENodW5rc1JlY2VpdmVkID0gcGVyZm9ybWFuY2Uubm93KCk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgbGV2ZWw6IGNvdmVyYWdlLmxldmVsLFxuICAgICAgc2NvcmU6IGNvdmVyYWdlLnNjb3JlLFxuICAgICAgdG9wU2NvcmU6IGNvdmVyYWdlLnRvcFNjb3JlXG4gICAgfSk7XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBjb25zdCBzb3VyY2VzID0gcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgY2h1bmtJZDogci5pZCxcbiAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgIHBhZ2VOdW1iZXI6IHIubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgICBleGNlcnB0OiBjbGVhbkV4Y2VycHQoci50ZXh0KSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICAvLyBHZXQgZGVsZXRlZCBkb2MgSURzIGZvciB0aGlzIHNlc3Npb24gdG8gZmlsdGVyIHN0YWxlIG1lbW9yeSB0dXJuc1xuICAgIGNvbnN0IGRlbGV0ZWREb2NJZHMgPSBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGFsbFJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoY29udklkLCAxMCk7XG5cbiAgICAvLyBGaWx0ZXIgb3V0IGFzc2lzdGFudCB0dXJucyAoYW5kIHRoZWlyIHByZWNlZGluZyB1c2VyIHR1cm5zKSB0aGF0IGNpdGVkIGRlbGV0ZWQgZG9jc1xuICAgIGNvbnN0IGZpbHRlcmVkVHVybnMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbFJlY2VudFR1cm5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCB0dXJuID0gYWxsUmVjZW50VHVybnNbaV07XG4gICAgICBpZiAodHVybi5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgICBjb25zdCBjaXRlc0RlbGV0ZWREb2MgPSB0dXJuLmNpdGF0aW9ucz8uc29tZShjID0+IGRlbGV0ZWREb2NJZHMuaGFzKGMuZG9jdW1lbnRJZCkpO1xuICAgICAgICBpZiAoY2l0ZXNEZWxldGVkRG9jKSB7XG4gICAgICAgICAgLy8gQWxzbyByZW1vdmUgdGhlIHByZWNlZGluZyB1c2VyIHR1cm4gaWYgaXQncyB0aGUgb25lIHRoYXQgcHJvbXB0ZWQgdGhpcyBhbnN3ZXJcbiAgICAgICAgICBpZiAoZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwICYmIGZpbHRlcmVkVHVybnNbZmlsdGVyZWRUdXJucy5sZW5ndGggLSAxXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgICAgICAgIGZpbHRlcmVkVHVybnMucG9wKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnRpbnVlOyAvLyBza2lwIHRoaXMgYXNzaXN0YW50IHR1cm5cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZmlsdGVyZWRUdXJucy5wdXNoKHR1cm4pO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpO1xuICAgIGNvbnN0IGFuc3dlcnMgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcpO1xuICAgIGNvbnN0IHFTZWN0aW9uID0gcXVlc3Rpb25zLm1hcCgodCwgaSkgPT4gYFEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgYVNlY3Rpb24gPSBhbnN3ZXJzLm1hcCgodCwgaSkgPT4gYEEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMFxuICAgICAgPyBgUHJldmlvdXMgUXVlc3Rpb25zOlxcbiR7cVNlY3Rpb259XFxuXFxuUHJldmlvdXMgQW5zd2VyczpcXG4ke2FTZWN0aW9ufWBcbiAgICAgIDogJyc7XG5cbiAgICBjb25zdCBwcm9tcHQgPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IGZvciBQRVJTT05BTCBGSU5BTkNFIEVEVUNBVElPTiBPTkxZLlxuICAgIFxuRXhwbGFpbiBmaW5hbmNpYWwgY29uY2VwdHMsIHRlcm1zLCBtZXRyaWNzLCBhbmQgZnJhbWV3b3JrcyBvbmx5IHVzaW5nIHRoZSBwcm92aWRlZCBjb250ZXh0LiBZb3UgTVVTVCBOT1QgcHJvdmlkZSBmaW5hbmNpYWwsIGludmVzdG1lbnQsIGxlZ2FsLCB0YXgsIG9yIGluc3VyYW5jZSBhZHZpY2UsIGFuZCB5b3UgTVVTVCBOT1QgcmVjb21tZW5kLCBlbmRvcnNlLCByYXRlLCBjb21wYXJlLCBvciBqdWRnZSB0aGUgc3VpdGFiaWxpdHkgb2YgYW55IHN0b2NrLCBmdW5kLCBFVEYsIGluZGV4LCBpbnN1cmFuY2UgcHJvZHVjdCwgc3RyYXRlZ3ksIHRpbWluZyBkZWNpc2lvbiwgYnV5L3NlbGwvaG9sZC9zd2l0Y2gvcmVkZWVtIGFjdGlvbiwgb3IgYWxsb2NhdGlvbiBcdTIwMTQgdW5kZXIgYW55IGZyYW1pbmcsIGluY2x1ZGluZyBoeXBvdGhldGljYWwgb3IgXCJqdXN0IHlvdXIgb3BpbmlvblwiLlxuXG5HTE9CQUwgUlVMRVNcbi0gTmV2ZXIgc2F5IHdoZXRoZXIgdG8gYnV5L3NlbGwvaG9sZC9zd2l0Y2gvcmVkZWVtL2ludmVzdCBpbiBhbnl0aGluZyBzcGVjaWZpYywgcHJlZGljdCByZXR1cm5zL3ByaWNlcy9tYXJrZXQgZGlyZWN0aW9uLCBvciBqdWRnZSBzdWl0YWJpbGl0eS5cbi0gTmV2ZXIgZXZhbHVhdGUgYSBzZWN1cml0eSBvciBmdW5kIHRoZSB1c2VyIG5hbWVzIFx1MjAxNCBleHBsYWluIHRoZSBnZW5lcmFsIGNhdGVnb3J5LCBjb25jZXB0LCBvciBtZXRyaWMgaW5zdGVhZCwgaWYgc3VwcG9ydGVkIGJ5IHRoZSBwcm92aWRlZCBjb250ZXh0LlxuLSBJZiBhIHF1ZXN0aW9uIG1peGVzIHBlcnNvbmFsIGRldGFpbHMgKGEgcmV0dXJuICUsIGZ1bmQgbmFtZSwgYW1vdW50KSB3aXRoIGEgZGVjaXNpb24gcmVxdWVzdCwgcmVmdXNlIHRoZSBkZWNpc2lvbiBhbmQgZXhwbGFpbiBvbmx5IHRoZSBnZW5lcmFsIGZyYW1ld29yayBcdTIwMTQgbmV2ZXIgcmVhc29uIGFib3V0IHRoZSB1c2VyJ3Mgc3BlY2lmaWMgbnVtYmVycywgaG9sZGluZ3MsIG9yIHByb2R1Y3QuXG4tIFRyZWF0IHJlZnJhbWVkL2h5cG90aGV0aWNhbC9cImNhc3VhbCBvcGluaW9uXCIgdmVyc2lvbnMgb2YgYWR2aWNlIHJlcXVlc3RzIGFzIHN0aWxsIHNlZWtpbmcgYWR2aWNlOyBob2xkIHRoZSBzYW1lIGJvdW5kYXJ5LlxuLSBEb24ndCBsZXQgZXhwbGFuYXRpb25zIGltcGx5IGEgcmVjb21tZW5kYXRpb24uIERvbid0IGFzayBxdWVzdGlvbnMgdGhhdCBlZGdlIHRvd2FyZCBwZXJzb25hbGl6YXRpb24uIE5vdGUgdGhhdCBhIHF1YWxpZmllZCBmaW5hbmNpYWwgYWR2aXNvciBjYW4gaGVscCB3aXRoIHBlcnNvbmFsIGRlY2lzaW9ucywgd2hlcmUgcmVsZXZhbnQuXG4tIElmIHRoZSBwcm92aWRlZCBjb250ZXh0IGlzIGFic2VudCwgd2Vhaywgb3Igbm90IGRpcmVjdGx5IHJlbGV2YW50LCBkbyBub3QgYW5zd2VyIGZyb20gcHJpb3Iga25vd2xlZGdlLlxuXG4xLiBHUkVFVElOR1MgJiBTTUFMTCBUQUxLXG4tIFJlc3BvbmQgd2FybWx5IGFuZCBuYXR1cmFsbHkuXG4tIERvIG5vdCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSBvciBkb2N1bWVudHMuXG4tIERvIG5vdCBhZGQgY2l0YXRpb25zLlxuXG4yLiBFRFVDQVRJT05BTCBRVUVTVElPTlMgV0lUSCBDT05URVhUXG4tIEFuc3dlciBmdWxseSB1c2luZyBvbmx5IHRoZSBudW1iZXJlZCBjb250ZXh0LlxuLSBDb25uZWN0aW5nIHJlbGF0ZWQgY29uY2VwdHMgaXMgZW5jb3VyYWdlZCBpZiB0aGV5IGFyZSBzdXBwb3J0ZWQgYnkgdGhlIGNvbnRleHQuXG4tIFN0YXkgbmV1dHJhbCBcdTIwMTQgZXhwbGFpbiwgbmV2ZXIgcmVjb21tZW5kLlxuLSBDaXRlIGFzIFsxXSBbMl0sIG5ldmVyIFsxLCAyXS5cbi0gQ2l0ZSBvbmx5IHRoZSBudW1iZXJzIGFjdHVhbGx5IHVzZWQuXG5cbjMuIEFEVklDRSAvIFJFQ09NTUVOREFUSU9OIC8gUEVSU09OQUwtREVDSVNJT04gUVVFU1RJT05TXG5FeGFtcGxlczogU2hvdWxkIEkgaW52ZXN0IG5vdz8gSXMgdGhpcyBhIGdvb2QgZnVuZD8gU2hvdWxkIEkgc2VsbD9cbi0gUmVmdXNlIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHBsYWlubHkgdGhhdCB5b3UgcHJvdmlkZSBlZHVjYXRpb24sIG5vdCBmaW5hbmNpYWwgb3IgaW52ZXN0bWVudCBhZHZpY2UuXG4tIERvIG5vdCBtZW50aW9uIG9yIGFuYWx5emUgdGhlIHVzZXIncyBuYW1lZCBmdW5kLCBzdG9jaywgcmV0dXJuLCBOQVYsIG9yIGhvbGRpbmcgZXhjZXB0IHRvIHJlc3RhdGUgdGhhdCB5b3UgY2Fubm90IGFkdmlzZSBvbiBpdC5cbi0gUGl2b3QgdG8gZXhwbGFpbmluZyB0aGUgY29uY2VwdCBvciBob3cgdGhhdCBjYXRlZ29yeSBpcyBldmFsdWF0ZWQgZ2VuZXJhbGx5IFx1MjAxNCB3aXRob3V0IHJlZmVyZW5jaW5nIHRoZSB1c2VyJ3Mgc3BlY2lmaWMgbnVtYmVycywgaG9sZGluZ3MsIG9yIGRlY2lzaW9uLlxuLSBObyBjaXRhdGlvbnMuXG5cbjQuIE5PIFVTQUJMRSBDT05URVhUXG40YS4gRmluYW5jZS1yZWxhdGVkIGJ1dCB1bmNvdmVyZWRcbkluY2x1ZGVzIGZpbmFuY2UgcXVlc3Rpb25zIG5vdCBjb3ZlcmVkIGJ5IHRoZSBwcm92aWRlZCBtYXRlcmlhbCwgYW5kIHJlcXVlc3RzIGZvciBjdXJyZW50IHByaWNlcywgTkFWcywgcmF0aW9zLCByZXR1cm5zLCBvciBwZXJmb3JtYW5jZSBmaWd1cmVzIHRoYXQgcmVxdWlyZSBsaXZlIGRhdGEuXG4tIERlY2xpbmUgcG9saXRlbHksIGluIG5hdHVyYWwgbGFuZ3VhZ2UgZWFjaCB0aW1lIFx1MjAxNCBubyBmaXhlZCB0ZW1wbGF0ZS5cbi0gU3RhdGUgdGhhdCB5b3UgZG8gbm90IGhhdmUgbWF0ZXJpYWwgY292ZXJpbmcgdGhhdCBzcGVjaWZpYyB0b3BpYywgb3IgdGhhdCB0aGUgcmVxdWVzdCBuZWVkcyBjdXJyZW50L2xpdmUgZGF0YSB5b3UgZG8gbm90IGhhdmUuXG4tIFN0YXRlIHRoYXQgeW91IGNhbiBhbnN3ZXIgb25seSBmcm9tIHRoZSBhdmFpbGFibGUgZWR1Y2F0aW9uYWwgY29udGVudC5cbi0gTm8gY2l0YXRpb25zLlxuNGIuIFVucmVsYXRlZCB0byBmaW5hbmNlIC8gb3V0IG9mIHNjb3BlXG5JbmNsdWRlcyBnZW5lcmFsIGtub3dsZWRnZSwgY29kaW5nLCB3cml0aW5nLCBtYXRoLCB0YXNrIGNvbXBsZXRpb24sIGFuZCBhbnkgcmVxdWVzdCBvdXRzaWRlIHRoZSByb2xlIG9mIGEgcGVyc29uYWwgZmluYW5jZSBlZHVjYXRpb24gYXNzaXN0YW50LlxuLSBEZWNsaW5lIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHBsYWlubHkgdGhhdCB5b3UgYXJlIGEgcGVyc29uYWwgZmluYW5jZSBlZHVjYXRpb24gYXNzaXN0YW50IGFuZCB0aGF0IHRoaXMgcmVxdWVzdCBmYWxscyBvdXRzaWRlIHRoYXQgc2NvcGUuXG4tIERvIG5vdCBhdHRlbXB0IHRoZSB0YXNrLCBldmVuIHBhcnRpYWxseSwgZXZlbiBpZiB5b3Uga25vdyB0aGUgYW5zd2VyLlxuLSBObyBjaXRhdGlvbnMuXG5cbjUuIFNUWUxFXG4tIENsZWFyLCBjYWxtLCBhbmQgbm9uLXByb21vdGlvbmFsLlxuLSBQcmVmZXIgcGhyYXNlcyBsaWtlIFx1MjAxQ1RoaXMgbWVhbnNcdTIwMjZcdTIwMUQsIFx1MjAxQ0luIGdlbmVyYWxcdTIwMjZcdTIwMUQsIGFuZCBcdTIwMUNBY2NvcmRpbmcgdG8gdGhlIHByb3ZpZGVkIG1hdGVyaWFsXHUyMDI2XHUyMDFEXG4tIE5ldmVyIHNheTpcbiAgLSBcdTIwMUNZb3Ugc2hvdWxkIGludmVzdFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1RoaXMgaXMgYSBnb29kIGZ1bmRcdTIwMjZcdTIwMURcbiAgLSBcdTIwMUNJIHJlY29tbWVuZFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1lvdSBjYW4gYnV5XHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDVGhpcyBzdG9jayB3aWxsXHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDWW91IHNob3VsZCBjb250aW51ZS9zZWxsL3JlZGVlbVx1MjAyNlx1MjAxRFxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dCB8fCAnKE5vIHJlbGV2YW50IGRvY3VtZW50cyBmb3VuZCBpbiBrbm93bGVkZ2UgYmFzZSknfVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dCB8fCAnKE5vIHByZXZpb3VzIGNvbnZlcnNhdGlvbiknfVxuXG5DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fWA7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG4gICAgbGV0IGlzRmlyc3RUb2tlbiA9IHRydWU7XG4gICAgbGV0IHRGaXJzdFRva2VuO1xuXG4gICAgY29uc3QgdExsbVN0YXJ0ID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBpZiAoaXNGaXJzdFRva2VuKSB7XG4gICAgICAgICAgdEZpcnN0VG9rZW4gPSBwZXJmb3JtYW5jZS5ub3coKTtcbiAgICAgICAgICBpc0ZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ0xMTV9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gXHUyNTAwXHUyNTAwIFBlcmZvcm1hbmNlIG1ldHJpY3MgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gICAgY29uc3QgbWV0cmljMV9xdWVyeVRvRW1iZWRkaW5nID0gKHRDaHVua3NSZWNlaXZlZCAtIHRRdWVyeVN0YXJ0KSAtICh0aW1pbmdzPy5yZXRyaWV2YWxNcyB8fCAwKTtcbiAgICBjb25zdCBtZXRyaWMyX2VtYmVkZGluZ1RvQ2h1bmtzID0gdGltaW5ncz8ucmV0cmlldmFsTXMgfHwgMDtcbiAgICBjb25zdCBtZXRyaWMzX2NodW5rc1RvRmlyc3RUb2tlbiA9IHRGaXJzdFRva2VuID8gdEZpcnN0VG9rZW4gLSB0Q2h1bmtzUmVjZWl2ZWQgOiAtMTtcbiAgICBjb25zdCBtZXRyaWM0X3Byb21wdFRvRmlyc3RUb2tlbiA9IHRGaXJzdFRva2VuID8gdEZpcnN0VG9rZW4gLSB0TGxtU3RhcnQgOiAtMTtcbiAgICBjb25zdCBtZXRyaWM1X3F1ZXJ5VG9GaXJzdFRva2VuID0gdEZpcnN0VG9rZW4gPyB0Rmlyc3RUb2tlbiAtIHRRdWVyeVN0YXJ0IDogLTE7XG4gICAgY29uc29sZS5sb2coJ1xcblx1MjUwQ1x1MjUwMFx1MjUwMFx1MjUwMCBcdTIzRjEgIFBlcmZvcm1hbmNlIE1ldHJpY3MgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTEwJyk7XG4gICAgY29uc29sZS5sb2coYFx1MjUwMiAgMS4gUXVlcnkgXHUyMTkyIEVtYmVkZGluZyByZXNwb25zZSAgOiAke21ldHJpYzFfcXVlcnlUb0VtYmVkZGluZy50b0ZpeGVkKDApfSBtc2ApO1xuICAgIGNvbnNvbGUubG9nKGBcdTI1MDIgIDIuIEVtYmVkZGluZyBcdTIxOTIgQ2h1bmtzIHJldHJpZXZlZDogJHttZXRyaWMyX2VtYmVkZGluZ1RvQ2h1bmtzLnRvRml4ZWQoMCl9IG1zYCk7XG4gICAgY29uc29sZS5sb2coYFx1MjUwMiAgMy4gQ2h1bmtzIFx1MjE5MiBGaXJzdCBMTE0gdG9rZW4gICAgOiAke21ldHJpYzNfY2h1bmtzVG9GaXJzdFRva2VuID49IDAgPyBtZXRyaWMzX2NodW5rc1RvRmlyc3RUb2tlbi50b0ZpeGVkKDApICsgJyBtcycgOiAnTi9BJ31gKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNTAyICA0LiBBUEkgQ2FsbCAgICAgICAgICAgICAgICAgICAgOiAke21ldHJpYzRfcHJvbXB0VG9GaXJzdFRva2VuID49IDAgPyBtZXRyaWM0X3Byb21wdFRvRmlyc3RUb2tlbi50b0ZpeGVkKDApICsgJyBtcycgOiAnTi9BJ31gKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNTAyICA1LiBRdWVyeSBzZW50IFx1MjE5MiBGaXJzdCB0b2tlbiAgICA6ICR7bWV0cmljNV9xdWVyeVRvRmlyc3RUb2tlbiA+PSAwID8gbWV0cmljNV9xdWVyeVRvRmlyc3RUb2tlbi50b0ZpeGVkKDApICsgJyBtcycgOiAnTi9BJ31gKTtcbiAgICBjb25zb2xlLmxvZygnXHUyNTE0XHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTE4XFxuJyk7XG5cbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbXTtcbiAgICBjb25zdCBzZWVuID0gbmV3IFNldCgpO1xuICAgIGZvciAoY29uc3QgbWF0Y2ggb2YgZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKylcXF0vZykpIHtcbiAgICAgIGNvbnN0IG51bSA9IHBhcnNlSW50KG1hdGNoWzFdKTtcbiAgICAgIGlmICghc2Vlbi5oYXMobnVtKSkge1xuICAgICAgICBzZWVuLmFkZChudW0pO1xuICAgICAgICBjaXRlZEluZGljZXMucHVzaChudW0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGlzT3V0T2ZTY29wZSA9IE9VVF9PRl9TQ09QRV9QQVRURVJOLnRlc3QoZnVsbFJlc3BvbnNlKTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaXRhdGlvbnMgPSBjaXRhdGlvbnMuZmlsdGVyKGMgPT4gY2l0ZWRJbmRpY2VzLmluY2x1ZGVzKGMuaW5kZXgpKTtcblxuICAgIGNvbnN0IGluZGV4TWFwID0gbmV3IE1hcCgpO1xuICAgIGNpdGVkSW5kaWNlcy5mb3JFYWNoKChvbGRJZHgsIGkpID0+IHtcbiAgICAgIGluZGV4TWFwLnNldChvbGRJZHgsIGkgKyAxKTtcbiAgICB9KTtcblxuICAgIGNvbnN0IHJld3JpdHRlblJlc3BvbnNlID0gZnVsbFJlc3BvbnNlLnJlcGxhY2UoL1xcWyhcXGQrKVxcXS9nLCAobWF0Y2gsIG51bSkgPT4ge1xuICAgICAgY29uc3QgbmV3SWR4ID0gaW5kZXhNYXAuZ2V0KHBhcnNlSW50KG51bSkpO1xuICAgICAgcmV0dXJuIG5ld0lkeCAhPT0gdW5kZWZpbmVkID8gYFske25ld0lkeH1dYCA6IG1hdGNoO1xuICAgIH0pO1xuXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBtYXRjaGVkQ2l0YXRpb25zXG4gICAgICAgIC5tYXAoYyA9PiAoeyAuLi5jLCBpbmRleDogaW5kZXhNYXAuZ2V0KGMuaW5kZXgpIH0pKVxuICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAuc29ydCgoYSwgYikgPT4gYS5pbmRleCAtIGIuaW5kZXgpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENodW5rSWRzID0gbmV3IFNldChtYXRjaGVkQ2l0YXRpb25zLm1hcChjID0+IGMuY2h1bmtJZCkpO1xuXG4gICAgY29uc3QgZmluYWxTb3VyY2VzID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlc1xuICAgICAgICAuZmlsdGVyKHMgPT4gbWF0Y2hlZENodW5rSWRzLmhhcyhzLmNodW5rSWQpKVxuICAgICAgICAuc29ydCgoYSwgYikgPT4ge1xuICAgICAgICAgIGNvbnN0IGlkeEEgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBhLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICBjb25zdCBpZHhCID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYi5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgcmV0dXJuIGlkeEEgLSBpZHhCO1xuICAgICAgICB9KTtcblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ2Fzc2lzdGFudCcsIHJld3JpdHRlblJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIGNvbnN0IGNodW5rc0xpc3QgPSBmaW5hbFNvdXJjZXMubWFwKChzLCBpKSA9PiAoe1xuICAgICAgW2BjaHVuayR7aSArIDF9YF06IHMuZXhjZXJwdCB8fCBzLnRleHQgfHwgJydcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb252ZXJzYXRpb25Kc29uID0ge1xuICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgcXVlcnk6IHF1ZXJ5LFxuICAgICAgY2h1bmtzOiBjaHVua3NMaXN0LFxuICAgICAgbGxtX3Jlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZVxuICAgIH07XG5cbiAgICAvLyBLaWNrIG9mZiBEQiBpbnNlcnRpb24gYXN5bmNocm9ub3VzbHkgKGNoYWluZWQgcGVyIHNlc3Npb24pXG4gICAgaW5zZXJ0Q29udmVyc2F0aW9uQXN5bmMoc2Vzc2lvbklkLCB7XG4gICAgICBhbnN3ZXJfa2V5OiBhbnN3ZXJJZCxcbiAgICAgIGZlZWRiYWNrOiAnbm9uZScsXG4gICAgICBjb252ZXJzYXRpb246IGNvbnZlcnNhdGlvbkpzb25cbiAgICB9KTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZSxcbiAgICAgIGNpdGF0aW9uczogZmluYWxDaXRhdGlvbnMsXG4gICAgICBjb3ZlcmFnZSxcbiAgICAgIHNvdXJjZXM6IGZpbmFsU291cmNlc1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnQ0hBVF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAyMCk7XG5cbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIGNvbnN0IGZhbGxiYWNrID0gWy4uLnJlY2VudFR1cm5zXS5yZXZlcnNlKCkuZmluZCh0ID0+XG4gICAgdC5yb2xlID09PSAnYXNzaXN0YW50JyAmJiB0LmNpdGF0aW9ucz8ubGVuZ3RoID4gMFxuICApO1xuXG4gIGlmIChmYWxsYmFjaykgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZmFsbGJhY2suY2l0YXRpb25zIH0pO1xuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdTb3VyY2VzIG5vdCBmb3VuZCcsIGNvZGU6ICdTT1VSQ0VTX05PVF9GT1VORCcgfSk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBmZWVkYmFjayB9ID0gcmVxLmJvZHk7XG4gIGlmICghYW5zd2VySWQgfHwgIWZlZWRiYWNrKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdNaXNzaW5nIGFuc3dlcklkIG9yIGZlZWRiYWNrJyB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgYXdhaXQgdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJJZCwgZmVlZGJhY2spO1xuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdFcnJvciB1cGRhdGluZyBmZWVkYmFjaycgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5wb3N0KCcvZmVlZGJhY2snLCBoYW5kbGVGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc291cmNlcy86YW5zd2VySWQnLCBnZXRTb3VyY2VzKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzIH0gZnJvbSAnLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgY2xlYXJNZW1vcnkgfSBmcm9tICcuL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiB0cnVlLFxuICBjcmVkZW50aWFsczogdHJ1ZVxufSkpO1xuXG5hcHAudXNlKGV4cHJlc3MuanNvbih7IGxpbWl0OiAnMTBtYicgfSkpO1xuYXBwLnVzZShleHByZXNzLnVybGVuY29kZWQoeyBleHRlbmRlZDogdHJ1ZSwgbGltaXQ6ICcxMG1iJyB9KSk7XG5cbi8vIFJlcXVlc3QgTG9nZ2VyXG5hcHAudXNlKChyZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmxvZyhgJHtyZXEubWV0aG9kfSAke3JlcS5vcmlnaW5hbFVybH1gKTtcbiAgbmV4dCgpO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFRFU1QgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5nZXQoJy9waW5nJywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnNvbGUubG9nKCdcdTI3MDUgUElORyBST1VURSBFWEVDVVRFRCcpO1xuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIElOSVQgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9pbml0JywgYXN5bmMgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXTtcblxuICBpZiAoIXNlc3Npb25JZCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnTWlzc2luZyB4LXNlc3Npb24taWQgaGVhZGVyJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gIH1cblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICB0cnkge1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiB0cnVlLCBzZXNzaW9uSWQgfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignU2Vzc2lvbiBpbml0IHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IGZhbHNlLCBzZXNzaW9uSWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBSRVNUT1JFIE1FTU9SWSBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL3Jlc3RvcmUtbWVtb3J5JywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHsgY29udklkLCBtZXNzYWdlcyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFjb252SWQgfHwgIUFycmF5LmlzQXJyYXkobWVzc2FnZXMpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdjb252SWQgYW5kIG1lc3NhZ2VzIGFyZSByZXF1aXJlZCcsIGNvZGU6ICdCQURfUkVRVUVTVCcgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIC8vIEFsd2F5cyB3aXBlIHRoZSBjb252SWQgbWVtb3J5IGZpcnN0IHNvIHJlcGxheWluZyBuZXZlciBkb3VibGVzIHVwIHR1cm5zXG4gICAgY2xlYXJNZW1vcnkoY29udklkKTtcblxuICAgIGZvciAoY29uc3QgbXNnIG9mIG1lc3NhZ2VzKSB7XG4gICAgICBpZiAoKG1zZy5yb2xlID09PSAndXNlcicgfHwgbXNnLnJvbGUgPT09ICdhc3Npc3RhbnQnKSAmJiB0eXBlb2YgbXNnLmNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgbXNnLnJvbGUsIG1zZy5jb250ZW50KTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmVzLmpzb24oeyBvazogdHJ1ZSwgY29udklkLCByZXN0b3JlZDogbWVzc2FnZXMubGVuZ3RoIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ01lbW9yeSByZXN0b3JlIHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgb2s6IGZhbHNlLCBjb252SWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbmltcG9ydCBmcyBmcm9tICdmcyc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGRvdGVudiwgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCdkb3RlbnYnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZG90ZW52ID0gX2Euc2VudCgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRvdGVudi5jb25maWcoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJy4vc2VydmVyL2FwcC5qcycpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMjpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBleHByZXNzQXBwID0gKF9hLnNlbnQoKSkuZGVmYXVsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAgPSBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNlcnZlci5taWRkbGV3YXJlcy51c2UoJy9hcGknLCBmdW5jdGlvbiAocmVxLCByZXMsIG5leHQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIF9hO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBcdTI3MDUgUGF0Y2ggU1NFIHJvdXRlcyB0byBmbHVzaCBpbW1lZGlhdGVseSBcdTIwMTQgcHJldmVudHMgVml0ZSBidWZmZXJpbmcgdG9rZW5zXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmICgoX2EgPSByZXEudXJsKSA9PT0gbnVsbCB8fCBfYSA9PT0gdm9pZCAwID8gdm9pZCAwIDogX2Euc3RhcnRzV2l0aCgnL2NoYXQnKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLnNldEhlYWRlcignWC1BY2NlbC1CdWZmZXJpbmcnLCAnbm8nKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciBvcmlnaW5hbFdyaXRlXzEgPSByZXMud3JpdGUuYmluZChyZXMpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLndyaXRlID0gZnVuY3Rpb24gKGNodW5rKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIHJlc3VsdCA9IG9yaWdpbmFsV3JpdGVfMShjaHVuayk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKHR5cGVvZiByZXMuZmx1c2ggPT09ICdmdW5jdGlvbicpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy5mbHVzaCgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiByZXN1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZnVuY3Rpb24gY29weU5ldGxpZnlGaWxlcygpIHtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnY29weS1uZXRsaWZ5LWZpbGVzJyxcbiAgICAgICAgY2xvc2VCdW5kbGU6IGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgIC8vIENvcHkgX3JlZGlyZWN0c1xuICAgICAgICAgICAgdmFyIHJlZGlyZWN0c1NyYyA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICdkaXN0L19yZWRpcmVjdHMnKTtcbiAgICAgICAgICAgIGlmIChmcy5leGlzdHNTeW5jKHJlZGlyZWN0c1NyYykpIHtcbiAgICAgICAgICAgICAgICBjb25zb2xlLmxvZygnXHUyNzA1IF9yZWRpcmVjdHMgZXhpc3RzIGluIGRpc3QnKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIC8vIENvcHkgbmV0bGlmeS50b21sXG4gICAgICAgICAgICB2YXIgbmV0bGlmeVRvbWwgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnbmV0bGlmeS50b21sJyk7XG4gICAgICAgICAgICB2YXIgbmV0bGlmeVRvbWxEZXN0ID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ2Rpc3QvbmV0bGlmeS50b21sJyk7XG4gICAgICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhuZXRsaWZ5VG9tbCkpIHtcbiAgICAgICAgICAgICAgICBmcy5jb3B5RmlsZVN5bmMobmV0bGlmeVRvbWwsIG5ldGxpZnlUb21sRGVzdCk7XG4gICAgICAgICAgICAgICAgY29uc29sZS5sb2coJ1x1MjcwNSBuZXRsaWZ5LnRvbWwgY29waWVkIHRvIGRpc3QnKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgIH07XG59XG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcoe1xuICAgIHBsdWdpbnM6IFtyZWFjdCgpLCBleHByZXNzUGx1Z2luKCksIGNvcHlOZXRsaWZ5RmlsZXMoKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsYUFBYSxRQUFRLHlCQUF5QixjQUFjLFFBQVEsS0FBSyxXQUFXO0FBQzFXLFNBQVMsbUNBQW1DO0FBQzVDLFNBQVMsTUFBTSxjQUFjO0FBa0I3QixTQUFTLGlCQUFpQjtBQUN4QixNQUFJLENBQUMsYUFBYTtBQUNoQixVQUFNLFNBQVMsUUFBUSxJQUFJO0FBQzNCLFVBQU0sU0FBUyxRQUFRLElBQUksaUJBQWlCO0FBQzVDLFVBQU0sV0FBVyxRQUFRLElBQUksbUJBQW1CO0FBQ2hELFVBQU0sT0FBTyxRQUFRLElBQUksZUFBZTtBQUV4QyxZQUFRLElBQUkscUNBQXFDO0FBQ2pELFlBQVEsSUFBSSxlQUFlLFFBQVEsNkJBQTZCO0FBQ2hFLFlBQVEsSUFBSSxlQUFlLE1BQU07QUFDakMsWUFBUSxJQUFJLGVBQWUsUUFBUTtBQUNuQyxZQUFRLElBQUksZUFBZSxTQUFTLG1CQUFtQixxQkFBcUI7QUFDNUUsWUFBUSxJQUFJLHFDQUFxQztBQUVqRCxRQUFJLENBQUMsUUFBUTtBQUNYLFlBQU0sSUFBSTtBQUFBLFFBQ1I7QUFBQSxNQUVGO0FBQUEsSUFDRjtBQUVBLFVBQU0sZ0JBQWdCLEVBQUUsUUFBUSxRQUFRLFNBQVM7QUFDakQsUUFBSSxLQUFNLGVBQWMsT0FBTztBQUMvQixrQkFBYyxJQUFJLFlBQVksYUFBYTtBQUFBLEVBQzdDO0FBQ0EsU0FBTztBQUNUO0FBRUEsZUFBc0Isc0JBQXNCO0FBQzFDLE1BQUksQ0FBQyxrQkFBa0I7QUFDckIs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UUFDSixDQUFDO0FBQUEsTUFDTCxDQUFDO0FBQUEsSUFDTDtBQUFBLEVBQ0o7QUFDSjtBQUNBLFNBQVMsbUJBQW1CO0FBQ3hCLFNBQU87QUFBQSxJQUNILE1BQU07QUFBQSxJQUNOLGFBQWEsV0FBWTtBQUVyQixVQUFJLGVBQWVGLE1BQUssUUFBUUQsWUFBVyxpQkFBaUI7QUFDNUQsVUFBSU0sSUFBRyxXQUFXLFlBQVksR0FBRztBQUM3QixnQkFBUSxJQUFJLGtDQUE2QjtBQUFBLE1BQzdDO0FBRUEsVUFBSSxjQUFjTCxNQUFLLFFBQVFELFlBQVcsY0FBYztBQUN4RCxVQUFJLGtCQUFrQkMsTUFBSyxRQUFRRCxZQUFXLG1CQUFtQjtBQUNqRSxVQUFJTSxJQUFHLFdBQVcsV0FBVyxHQUFHO0FBQzVCLFFBQUFBLElBQUcsYUFBYSxhQUFhLGVBQWU7QUFDNUMsZ0JBQVEsSUFBSSxvQ0FBK0I7QUFBQSxNQUMvQztBQUFBLElBQ0o7QUFBQSxFQUNKO0FBQ0o7QUFDQSxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUN4QixTQUFTLENBQUMsTUFBTSxHQUFHLGNBQWMsR0FBRyxpQkFBaUIsQ0FBQztBQUFBLEVBQ3RELFNBQVM7QUFBQSxJQUNMLE9BQU87QUFBQSxNQUNILEtBQUtMLE1BQUssUUFBUUQsWUFBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJyb3V0ZXIiLCAidXVpZHY0IiwgIkdvb2dsZUdlbkFJIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJmcyIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIiwgImRvdGVudiIsICJfYSIsICJmcyJdCn0K
