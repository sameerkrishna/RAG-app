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
    if (!fs.existsSync(seedDir)) {
      seedDir = path2.resolve(process.cwd(), "seed_documents");
    }
    if (!fs.existsSync(seedDir)) {
      seedDir = path2.resolve(process.cwd(), "dist/seed_documents");
    }
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
      var seedSrc = path3.resolve(__dirname2, "seed_documents");
      var seedDest = path3.resolve(__dirname2, "dist/seed_documents");
      if (fs2.existsSync(seedSrc)) {
        fs2.mkdirSync(seedDest, { recursive: true });
        var files = fs2.readdirSync(seedSrc);
        files.forEach(function(file) {
          var srcFile = path3.join(seedSrc, file);
          var destFile = path3.join(seedDest, file);
          if (fs2.statSync(srcFile).isFile()) {
            fs2.copyFileSync(srcFile, destFile);
          }
        });
        console.log("\u2705 seed_documents copied to dist (".concat(files.length, " files)"));
      }
      var credsSrc = path3.resolve(__dirname2, "google_credentials");
      var credsDest = path3.resolve(__dirname2, "dist/google_credentials");
      if (fs2.existsSync(credsSrc)) {
        fs2.mkdirSync(credsDest, { recursive: true });
        var files = fs2.readdirSync(credsSrc);
        files.forEach(function(file) {
          var srcFile = path3.join(credsSrc, file);
          var destFile = path3.join(credsDest, file);
          if (fs2.statSync(srcFile).isFile()) {
            fs2.copyFileSync(srcFile, destFile);
          }
        });
        console.log("\u2705 google_credentials copied to dist (".concat(files.length, " files)"));
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9jaGF0LmpzIiwgInNlcnZlci9hcGkvZmVlZGJhY2suanMiLCAic2VydmVyL2FwcC5qcyIsICJ2aXRlLmNvbmZpZy5qcyJdLAogICJzb3VyY2VzQ29udGVudCI6IFsiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanNcIjtpbXBvcnQgeyBDbG91ZENsaWVudCwgU2NoZW1hLCBTcGFyc2VWZWN0b3JJbmRleENvbmZpZywgRE9DVU1FTlRfS0VZLCBTZWFyY2gsIEtubiwgUnJmIH0gZnJvbSAnY2hyb21hZGInO1xuaW1wb3J0IHsgQ2hyb21hQm0yNUVtYmVkZGluZ0Z1bmN0aW9uIH0gZnJvbSAnQGNocm9tYS1jb3JlL2Nocm9tYS1ibTI1JztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuXG4vLyBcdTI1MDBcdTI1MDAgU2hhcmVkIHNjaGVtYTogZGVuc2UgZW1iZWRkaW5ncyAobWFuYWdlZCBleHRlcm5hbGx5KSArIEJNMjUgc3BhcnNlIGluZGV4IFx1MjUwMFx1MjUwMFxuY29uc3QgYm0yNUVtYmVkZGluZ0Z1bmN0aW9uID0gbmV3IENocm9tYUJtMjVFbWJlZGRpbmdGdW5jdGlvbigpO1xuY29uc3QgY29sbGVjdGlvblNjaGVtYSA9IG5ldyBTY2hlbWEoKS5jcmVhdGVJbmRleChcbiAgbmV3IFNwYXJzZVZlY3RvckluZGV4Q29uZmlnKHtcbiAgICBlbWJlZGRpbmdGdW5jdGlvbjogYm0yNUVtYmVkZGluZ0Z1bmN0aW9uLFxuICAgIHNvdXJjZUtleTogRE9DVU1FTlRfS0VZLFxuICAgIGJtMjU6IHRydWVcbiAgfSksXG4gICdzcGFyc2VfYm0yNSdcbik7XG5cbmxldCBjbG91ZENsaWVudCA9IG51bGw7XG5sZXQgZ2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldENsb3VkQ2xpZW50KCkge1xuICBpZiAoIWNsb3VkQ2xpZW50KSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0FQSV9LRVk7XG4gICAgY29uc3QgdGVuYW50ID0gcHJvY2Vzcy5lbnYuQ0hST01BX1RFTkFOVCB8fCAnZGVmYXVsdF90ZW5hbnQnO1xuICAgIGNvbnN0IGRhdGFiYXNlID0gcHJvY2Vzcy5lbnYuQ0hST01BX0RBVEFCQVNFIHx8ICdkZWZhdWx0X2RhdGFiYXNlJztcbiAgICBjb25zdCBob3N0ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0hPU1QgfHwgdW5kZWZpbmVkO1xuXG4gICAgY29uc29sZS5sb2coXCItLS0tIENIUk9NQSBDT05ORUNUSVZJVFkgREVCVUcgLS0tLVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIkhvc3Q6ICAgICAgXCIsIGhvc3QgfHwgXCJhcGkudHJ5Y2hyb21hLmNvbSAoZGVmYXVsdClcIik7XG4gICAgY29uc29sZS5sb2coXCJUZW5hbnQ6ICAgIFwiLCB0ZW5hbnQpO1xuICAgIGNvbnNvbGUubG9nKFwiREIgTmFtZTogICBcIiwgZGF0YWJhc2UpO1xuICAgIGNvbnNvbGUubG9nKFwiQVBJIEtleTogICBcIiwgYXBpS2V5ID8gXCJMT0FERUQgKFZBTElEKVwiIDogXCJNSVNTSU5HIChVTkRFRklORUQpXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cIik7XG5cbiAgICBpZiAoIWFwaUtleSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBcIkNSSVRJQ0FMIEVSUk9SOiBDSFJPTUFfQVBJX0tFWSBpcyB1bmRlZmluZWQuIFwiICtcbiAgICAgICAgXCJFbnN1cmUgeW91ciBlbnZpcm9ubWVudCB2YXJpYWJsZXMgYXJlIGNvcnJlY3RseSBsb2FkZWQgYmVmb3JlIGV4ZWN1dGluZyB0aGlzIGZpbGUuXCJcbiAgICAgICk7XG4gICAgfVxuXG4gICAgY29uc3QgY2xpZW50T3B0aW9ucyA9IHsgYXBpS2V5LCB0ZW5hbnQsIGRhdGFiYXNlIH07XG4gICAgaWYgKGhvc3QpIGNsaWVudE9wdGlvbnMuaG9zdCA9IGhvc3Q7XG4gICAgY2xvdWRDbGllbnQgPSBuZXcgQ2xvdWRDbGllbnQoY2xpZW50T3B0aW9ucyk7XG4gIH1cbiAgcmV0dXJuIGNsb3VkQ2xpZW50O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0R2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFnbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IHByb2Nlc3MuZW52LkNIUk9NQV9HTE9CQUxfQ09MTEVDVElPTiB8fCAnc2VlZF9kYic7XG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIHNjaGVtYTogY29sbGVjdGlvblNjaGVtYSxcbiAgICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgICBkZXNjcmlwdGlvbjogJ1Blcm1hbmVudCBzZWVkIGRvY3VtZW50cyBmb3IgUkFHJyxcbiAgICAgICAgICB0eXBlOiAnZ2xvYmFsX2tub3dsZWRnZSdcbiAgICAgICAgfSxcbiAgICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYFxcdTI3MDUgR2xvYmFsIGNvbGxlY3Rpb24gcmVhZHk6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjb25uZWN0IHRvIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHRoZSBzaW5nbGUgc2hhcmVkIGNvbGxlY3Rpb24uXG4gKiBEcm9wLWluIHJlcGxhY2VtZW50IGZvciB0aGUgb2xkIGdldFNlc3Npb25Db2xsZWN0aW9uIFx1MjAxNCBjYWxsZXJzIHRoYXRcbiAqIHByZXZpb3VzbHkgZGVzdHJ1Y3R1cmVkIHsgY29sbGVjdGlvbiB9IHdpbGwgc3RpbGwgd29yay5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldENvbGxlY3Rpb24oKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIHJldHVybiB7IGNvbGxlY3Rpb24sIGlzTmV3OiBmYWxzZSB9O1xufVxuXG4vKipcbiAqIEFkZCB2ZWN0b3JzIGluIGJhdGNoZXMgb2YgQkFUQ0hfU0laRSB0byBhdm9pZCBDaHJvbWEgcGF5bG9hZCBsaW1pdHMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBhZGRWZWN0b3JzKGNvbGxlY3Rpb24sIHZlY3RvcnMsIGVtYmVkZGluZ3MsIGlkcykge1xuICB0cnkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgaWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBjb25zdCBiYXRjaElkcyA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi50ZXh0KTtcbiAgICAgIGNvbnN0IGJhdGNoTWV0YWRhdGFzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi5tZXRhZGF0YSk7XG5cbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uYWRkKHtcbiAgICAgICAgaWRzOiBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6IGJhdGNoRG9jdW1lbnRzLFxuICAgICAgICBtZXRhZGF0YXM6IGJhdGNoTWV0YWRhdGFzXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFthZGRWZWN0b3JzXSBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IGFkZGVkICR7YmF0Y2hJZHMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gICAgfVxuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBhZGQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSyA9IDUsIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcXVlcnlPcHRzID0ge1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9O1xuICAgIGlmICh3aGVyZSkgcXVlcnlPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5xdWVyeShxdWVyeU9wdHMpO1xuXG4gICAgaWYgKCFyZXN1bHRzLmlkcyB8fCByZXN1bHRzLmlkcy5sZW5ndGggPT09IDAgfHwgcmVzdWx0cy5pZHNbMF0ubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdHMuaWRzWzBdLm1hcCgoaWQsIGlkeCkgPT4gKHtcbiAgICAgIGlkLFxuICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHNbMF1baWR4XSxcbiAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhc1swXVtpZHhdLFxuICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF0sXG4gICAgICBzY29yZTogMSAtIHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF1cbiAgICB9KSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIHF1ZXJ5IGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogSHlicmlkIHNlYXJjaCB1c2luZyBDaHJvbWEgQ2xvdWQgU2VhcmNoIEFQSSB3aXRoIFJSRiAoZGVuc2UgKyBzcGFyc2UgQk0yNSkuXG4gKiBSZXR1cm5zIHJlc3VsdHMgaW4gdGhlIHNhbWUgc2hhcGUgYXMgcXVlcnlDb2xsZWN0aW9uKCkgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHkuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcgKGUuZy4gc2Vzc2lvbl9pZCAkaW4pLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaHlicmlkUXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5VGV4dCwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1LCB3aGVyZSA9IHVuZGVmaW5lZCkge1xuICB0cnkge1xuICAgIGxldCBzZWFyY2ggPSBuZXcgU2VhcmNoKClcbiAgICAgIC5yYW5rKFJyZih7XG4gICAgICAgIHJhbmtzOiBbXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5RW1iZWRkaW5nLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMjAgfSksXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5VGV4dCwga2V5OiAnc3BhcnNlX2JtMjUnLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMjAgfSlcbiAgICAgICAgXSxcbiAgICAgICAgd2VpZ2h0czogWzAuOSwgMC4xXSxcbiAgICAgICAgazogNjBcbiAgICAgIH0pKVxuICAgICAgLndoZXJlKHdoZXJlKVxuICAgICAgLnNlbGVjdChcIiNkb2N1bWVudFwiLCBcIiNtZXRhZGF0YVwiLCBcIiNzY29yZVwiKVxuICAgICAgLmxpbWl0KHRvcEspO1xuXG4gICAgY29uc3QgcmF3ID0gYXdhaXQgY29sbGVjdGlvbi5zZWFyY2goc2VhcmNoKTtcblxuICAgIC8vIFBhcmFsbGVsXHUyMDExYXJyYXkgc3RydWN0dXJlOiBpZHNbMF0sIGRvY3VtZW50c1swXSwgbWV0YWRhdGFzWzBdLCBzY29yZXNbMF1cbiAgICBpZiAoIXJhdy5pZHMgfHwgIXJhdy5pZHNbMF0gfHwgcmF3Lmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICBjb25zdCBpZHMgPSByYXcuaWRzWzBdO1xuICAgIGNvbnN0IGRvY3MgPSByYXcuZG9jdW1lbnRzPy5bMF0gPz8gW107XG4gICAgY29uc3QgbWV0YXMgPSByYXcubWV0YWRhdGFzPy5bMF0gPz8gW107XG4gICAgY29uc3Qgc2NvcmVzID0gcmF3LnNjb3Jlcz8uWzBdID8/IFtdO1xuXG4gICAgLy8gMS4gRGVmaW5lIGdsb2JhbCBSUkYgYm91bmRzIGJhc2VkIG9uIHlvdXIgd2VpZ2h0cyBbMC43LCAwLjNdIGFuZCBsaW1pdHMgKDEwMClcbiAgICAvLyBNYXggcG9zc2libGUgcmF3IFJSRjogMSAvICg2MCArIDEpID0gMC4wMTYzOTM0XG4gICAgLy8gTWluIHBvc3NpYmxlIHJhdyBSUkY6IDEgLyAoNjAgKyAxMDApID0gMC4wMDYyNTAwXG4gICAgY29uc3QgTUFYX1JSRiA9IDEgLyA2MTtcbiAgICBjb25zdCBNSU5fUlJGID0gMSAvIDE2MDtcblxuICAgIHJldHVybiBpZHMubWFwKChpZCwgaWR4KSA9PiB7XG4gICAgICAvLyBDaHJvbWEgcmV0dXJucyBuZWdhdGl2ZSB2YWx1ZXMgKGUuZy4gLTAuMDE2MzkpLCBjb252ZXJ0IHRvIHBvc2l0aXZlIHJhdyBSUkZcbiAgICAgIGNvbnN0IHJhd1JSRiA9IE1hdGguYWJzKHNjb3Jlc1tpZHhdID8/IE1JTl9SUkYpO1xuXG4gICAgICAvLyAyLiBMaW5lYXIgbWluLW1heCBub3JtYWxpemF0aW9uIHRvIGZpdCBwZXJmZWN0bHkgYmV0d2VlbiAwLjAgYW5kIDEuMFxuICAgICAgbGV0IG5vcm1hbGl6ZWRTY29yZSA9IChyYXdSUkYgLSBNSU5fUlJGKSAvIChNQVhfUlJGIC0gTUlOX1JSRik7XG5cbiAgICAgIC8vIEJvdW5kYXJ5IHByb3RlY3Rpb25cbiAgICAgIG5vcm1hbGl6ZWRTY29yZSA9IE1hdGgubWF4KDAsIE1hdGgubWluKDEsIG5vcm1hbGl6ZWRTY29yZSkpO1xuXG4gICAgICAvL2NvbnN0IGZpbmFsU2NvcmUgPSBNYXRoLnJvdW5kKG5vcm1hbGl6ZWRTY29yZSAqIDEwMCkgLyAxMDA7XG5cbiAgICAgIHJldHVybiB7XG4gICAgICAgIGlkLFxuICAgICAgICB0ZXh0OiBkb2NzW2lkeF0gPz8gJycsXG4gICAgICAgIG1ldGFkYXRhOiBtZXRhc1tpZHhdID8/IHt9LFxuICAgICAgICBkaXN0YW5jZTogMSAtIG5vcm1hbGl6ZWRTY29yZSxcbiAgICAgICAgc2NvcmU6IG5vcm1hbGl6ZWRTY29yZVxuICAgICAgfTtcbiAgICB9KTtcblxuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignSHlicmlkIHF1ZXJ5IGZhaWxlZCwgZmFsbGluZyBiYWNrIHRvIGRlbnNlLW9ubHk6JywgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gR3JhY2VmdWwgZmFsbGJhY2sgdG8gZGVuc2Utb25seSBzZWFyY2ggZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICByZXR1cm4gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLLCB3aGVyZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPmRlZmF1bHQgMTAwIGxpbWl0KSBhcmUgZnVsbHkgZGVsZXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYWxsSWRzID0gW107XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICAgIHdoZXJlOiB7IGRvY3VtZW50X2lkOiBkb2N1bWVudElkIH0sXG4gICAgICAgIGluY2x1ZGU6IFtdLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcblxuICAgICAgaWYgKCFiYXRjaC5pZHMgfHwgYmF0Y2guaWRzLmxlbmd0aCA9PT0gMCkgYnJlYWs7XG4gICAgICBhbGxJZHMucHVzaCguLi5iYXRjaC5pZHMpO1xuXG4gICAgICBpZiAoYmF0Y2guaWRzLmxlbmd0aCA8IEJBVENIX1NJWkUpIGJyZWFrO1xuICAgICAgb2Zmc2V0ICs9IEJBVENIX1NJWkU7XG4gICAgfVxuXG4gICAgaWYgKGFsbElkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7IGlkczogYWxsSWRzIH0pO1xuICAgIH1cbiAgICByZXR1cm4gYWxsSWRzLmxlbmd0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogRGVsZXRlIGFsbCB2ZWN0b3JzIGJlbG9uZ2luZyB0byBhIHNwZWNpZmljIHNlc3Npb24uXG4gKiBVc2VzIHNlc3Npb25faWQgbWV0YWRhdGEgZmlsdGVyIHRvIGZpbmQgYW5kIHJlbW92ZSB0aGVtIGluIGJhdGNoZXMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVTZXNzaW9uVmVjdG9ycyhzZXNzaW9uSWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBzZXNzaW9uX2lkOiBzZXNzaW9uSWQgfSxcbiAgICAgICAgaW5jbHVkZTogW10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBhbGxJZHMgfSk7XG4gICAgfVxuICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IERlbGV0ZWQgJHthbGxJZHMubGVuZ3RofSBzZXNzaW9uIHZlY3RvcnMgZm9yIHNlc3Npb25faWQ9JHtzZXNzaW9uSWR9YCk7XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGRlbGV0ZSBzZXNzaW9uIHZlY3RvcnMgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuLyoqXG4gKiBMaXN0IGFsbCB1bmlxdWUgZG9jdW1lbnRzIGluIGEgY29sbGVjdGlvbi5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIHdpdGggQkFUQ0hfU0laRT0zMDAgc28gY29sbGVjdGlvbnMgbGFyZ2VyXG4gKiB0aGFuIENocm9tYSdzIGRlZmF1bHQgZ2V0KCkgbGltaXQgKDEwMCkgYXJlIGZ1bGx5IGVudW1lcmF0ZWQuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZG9jdW1lbnRzTWFwID0gbmV3IE1hcCgpO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGdldE9wdHMgPSB7XG4gICAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9O1xuICAgICAgaWYgKHdoZXJlKSBnZXRPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoZ2V0T3B0cyk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuXG4gICAgICBiYXRjaC5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYmF0Y2gubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYmF0Y2guZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuXG4gICAgICBjb25zb2xlLmxvZyhgICBbbGlzdERvY3VtZW50c10gb2Zmc2V0PSR7b2Zmc2V0fSwgZ290PSR7YmF0Y2guaWRzLmxlbmd0aH0sIHVuaXF1ZSBzbyBmYXI9JHtkb2N1bWVudHNNYXAuc2l6ZX1gKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7aW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5cbmNvbnN0IENIQVJTX1BFUl9UT0tFTiA9IDQ7XG5jb25zdCBUQVJHRVRfQ0hVTktfVE9LRU5TID0gNjAwOyAgIC8vIHNvZnQgdGFyZ2V0IHBlciBjaHVua1xuY29uc3QgTUFYX0NIVU5LX1RPS0VOUyA9IDc1MDsgICAvLyBoYXJkIGNhcCBiZWZvcmUgZm9yY2VkIHNwbGl0XG5jb25zdCBPVkVSTEFQX1RPS0VOUyA9IDEwMDsgICAvLyBvdmVybGFwIG9ubHkgb24gb3ZlcnNpemVkIHBhcmFncmFwaHNcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuLy8gTWF0Y2hlcyBBTEwtQ0FQUyBoZWFkaW5ncywgbWFya2Rvd24gaGVhZGluZ3MsIG9yIG51bWJlcmVkIHNlY3Rpb24gaGVhZGluZ3NcbmNvbnN0IEhFQURJTkdfUkUgPSAvXig/OltBLVpdW0EtWlxcc117Miw2MH0kfCN7MSw0fVxccy4rfCg/OlxcZCtcXC4pK1xccy4rKS9tO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblRleHQodGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gJyc7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoL1xcZi9nLCAnXFxuJylcbiAgICAucmVwbGFjZSgvKFxccypcXG4pezMsfS9nLCAnXFxuXFxuJylcbiAgICAucmVwbGFjZSgvXlxccypcXGQrXFxzKiQvZ20sICcnKVxuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuLyoqXG4gKiBHaXZlbiBhIHJhdyAocG9zc2libHkgbWlkLXdvcmQpIG9mZnNldCwgc25hcCBmb3J3YXJkIHRvIHRoZSBuZWFyZXN0XG4gKiBjbGVhbiBzZW50ZW5jZSBzdGFydCwgZmFsbGluZyBiYWNrIHRvIHRoZSBuZWFyZXN0IHdvcmQgYm91bmRhcnksXG4gKiBzbyBvdmVybGFwcGVkIGNodW5rcyBuZXZlciBiZWdpbiBtaWQtc2VudGVuY2Ugb3IgbWlkLXdvcmQuXG4gKlxuICogQHBhcmFtIHtzdHJpbmd9IHRleHQgICAgICAgdGhlIHBhcmFncmFwaC90ZXh0IGJlaW5nIHdpbmRvd2VkXG4gKiBAcGFyYW0ge251bWJlcn0gcmF3T2Zmc2V0ICB0aGUgcmF3ICh1bnNuYXBwZWQpIHN0YXJ0IG9mZnNldCBmb3IgdGhlIG5leHQgd2luZG93XG4gKiBAcGFyYW0ge251bWJlcn0gaGFyZExpbWl0ICBkb24ndCBzZWFyY2ggcGFzdCB0aGlzIG9mZnNldCAoZW5kIG9mIHByZXZpb3VzIHdpbmRvdylcbiAqL1xuZnVuY3Rpb24gc25hcFRvQm91bmRhcnkodGV4dCwgcmF3T2Zmc2V0LCBoYXJkTGltaXQpIHtcbiAgaWYgKHJhd09mZnNldCA8PSAwKSByZXR1cm4gMDtcblxuICAvLyBQcmVmZXIgYSByZWFsIHNlbnRlbmNlIGJvdW5kYXJ5IHdpdGhpbiBhIHNtYWxsIGZvcndhcmQgd2luZG93XG4gIGNvbnN0IHNlYXJjaFdpbmRvd0VuZCA9IE1hdGgubWluKHJhd09mZnNldCArIDgwLCBoYXJkTGltaXQpOyAvLyB+ODAgY2hhcnMgXHUyMjQ4IG9uZSBzZW50ZW5jZVxuICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgIGNvbnN0IGlkeCA9IHRleHQuaW5kZXhPZihicCwgcmF3T2Zmc2V0KTtcbiAgICBpZiAoaWR4ICE9PSAtMSAmJiBpZHggPCBzZWFyY2hXaW5kb3dFbmQpIHtcbiAgICAgIHJldHVybiBpZHggKyBicC5sZW5ndGg7XG4gICAgfVxuICB9XG5cbiAgLy8gRmFsbCBiYWNrOiBzbmFwIHRvIHRoZSBuZXh0IHdvcmQgYm91bmRhcnkgc28gd2UgYXQgbGVhc3QgZG9uJ3RcbiAgLy8gc3BsaXQgYSB3b3JkIGluIGhhbGZcbiAgY29uc3Qgc3BhY2VJZHggPSB0ZXh0LmluZGV4T2YoJyAnLCByYXdPZmZzZXQpO1xuICBpZiAoc3BhY2VJZHggIT09IC0xICYmIHNwYWNlSWR4IDwgc2VhcmNoV2luZG93RW5kKSB7XG4gICAgcmV0dXJuIHNwYWNlSWR4ICsgMTtcbiAgfVxuXG4gIC8vIExhc3QgcmVzb3J0OiBpZiB0aGUgY3VycmVudCBwb3NpdGlvbiBpcyBhbHJlYWR5IG1pZC13b3JkLFxuICAvLyB3YWxrIGJhY2t3YXJkIHRvIHRoZSBsYXN0IHNwYWNlIGJlZm9yZSBpdFxuICBsZXQgaSA9IHJhd09mZnNldDtcbiAgd2hpbGUgKGkgPiAwICYmICEvXFxzLy50ZXN0KHRleHRbaSAtIDFdKSkgaS0tO1xuICByZXR1cm4gaSA+IDAgPyBpIDogcmF3T2Zmc2V0O1xufVxuXG4vKipcbiAqIFN0cnVjdHVyZS1hd2FyZSBjaHVua2luZzpcbiAqICAxLiBTcGxpdCBvbiBibGFuayBsaW5lcyAoXFxuXFxuKSBpbnRvIHBhcmFncmFwaHMuXG4gKiAgMi4gQSBsaW5lIG1hdGNoaW5nIEhFQURJTkdfUkUgYWx3YXlzIHN0YXJ0cyBhIGZyZXNoIGNodW5rLlxuICogIDMuIEFjY3VtdWxhdGUgcGFyYWdyYXBocyB1bnRpbCB0aGUgc29mdCBUQVJHRVQgaXMgcmVhY2hlZCwgdGhlbiBmbHVzaC5cbiAqICA0LiBQYXJhZ3JhcGhzIGxhcmdlciB0aGFuIE1BWCBhcmUgc3BsaXQgd2l0aCBhIHNsaWRpbmcgd2luZG93ICsgb3ZlcmxhcCBhcyBmYWxsYmFjay5cbiAqICAgICBCb3RoIHdpbmRvdyBlbmRzIEFORCB3aW5kb3cgc3RhcnRzIGFyZSBzbmFwcGVkIHRvIHNlbnRlbmNlL3dvcmQgYm91bmRhcmllc1xuICogICAgIHNvIG5vIGNodW5rIGJlZ2lucyBvciBlbmRzIG1pZC13b3JkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0YXJnZXRUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBUQVJHRVRfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBtYXhUb2tlbnMgPSBvcHRpb25zLm1heENodW5rVG9rZW5zIHx8IE1BWF9DSFVOS19UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUayA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBPVkVSTEFQX1RPS0VOUztcblxuICBjb25zdCB0YXJnZXRDaGFycyA9IHRhcmdldFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3QgbWF4Q2hhcnMgPSBtYXhUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUayAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgLy8gMS4gU3BsaXQgaW50byBwYXJhZ3JhcGhzXG4gIGNvbnN0IHJhd1BhcmFzID0gdGV4dFxuICAgIC5zcGxpdCgvXFxuezIsfS8pXG4gICAgLm1hcChwID0+IHAudHJpbSgpKVxuICAgIC5maWx0ZXIocCA9PiBwLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpO1xuXG4gIGNvbnN0IGNodW5rcyA9IFtdO1xuICBsZXQgYnVmZmVyID0gJyc7XG4gIGxldCBidWZTdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcbiAgbGV0IGNoYXJDdXJzb3IgPSAwO1xuXG4gIGNvbnN0IGZsdXNoID0gKGZvcmNlVGV4dCkgPT4ge1xuICAgIGNvbnN0IGNvbnRlbnQgPSAoZm9yY2VUZXh0ID8/IGJ1ZmZlcikudHJpbSgpO1xuICAgIGlmIChjb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY29udGVudCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogYnVmU3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGJ1ZlN0YXJ0ICsgY29udGVudC5sZW5ndGgsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuICAgIGJ1ZmZlciA9ICcnO1xuICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgfTtcblxuICBmb3IgKGNvbnN0IHBhcmEgb2YgcmF3UGFyYXMpIHtcbiAgICBjb25zdCBpc0hlYWRpbmcgPSBIRUFESU5HX1JFLnRlc3QocGFyYS5zcGxpdCgnXFxuJylbMF0pO1xuXG4gICAgLy8gMi4gSGVhZGluZyBhbHdheXMgc3RhcnRzIGEgbmV3IGNodW5rXG4gICAgaWYgKGlzSGVhZGluZyAmJiBidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgIGlmIChwYXJhLmxlbmd0aCA+IG1heENoYXJzKSB7XG4gICAgICAvLyAzLiBPdmVyc2l6ZWQgcGFyYWdyYXBoIC0+IHNsaWRpbmctd2luZG93IGNoYXIgZmFsbGJhY2tcbiAgICAgIGlmIChidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgICAgbGV0IHMgPSAwO1xuICAgICAgd2hpbGUgKHMgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICBsZXQgZSA9IHMgKyB0YXJnZXRDaGFycztcbiAgICAgICAgaWYgKGUgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICAgIGNvbnN0IHNlYXJjaEZyb20gPSBlIC0gTWF0aC5mbG9vcih0YXJnZXRDaGFycyAqIDAuMik7XG4gICAgICAgICAgZm9yIChjb25zdCBicCBvZiBbJy4gJywgJy5cXG4nLCAnPyAnLCAnISAnLCAnXFxuJ10pIHtcbiAgICAgICAgICAgIGNvbnN0IGlkeCA9IHBhcmEubGFzdEluZGV4T2YoYnAsIGUpO1xuICAgICAgICAgICAgaWYgKGlkeCA+IHNlYXJjaEZyb20pIHsgZSA9IGlkeCArIGJwLmxlbmd0aDsgYnJlYWs7IH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgZSA9IE1hdGgubWluKGUsIHBhcmEubGVuZ3RoKTtcbiAgICAgICAgY29uc3Qgc2xpY2UgPSBwYXJhLnNsaWNlKHMsIGUpLnRyaW0oKTtcbiAgICAgICAgaWYgKHNsaWNlLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgICAgICB0ZXh0OiBzbGljZSxcbiAgICAgICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKHNsaWNlKSxcbiAgICAgICAgICAgIGNoYXJTdGFydDogY2hhckN1cnNvciArIHMsXG4gICAgICAgICAgICBjaGFyRW5kOiBjaGFyQ3Vyc29yICsgZSxcbiAgICAgICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKGUgPj0gcGFyYS5sZW5ndGgpIGJyZWFrO1xuXG4gICAgICAgIC8vIFNuYXAgdGhlIG92ZXJsYXBwZWQgc3RhcnQgZm9yd2FyZCB0byBhIGNsZWFuIHNlbnRlbmNlL3dvcmRcbiAgICAgICAgLy8gYm91bmRhcnkgaW5zdGVhZCBvZiB1c2luZyB0aGUgcmF3IG9mZnNldCwgd2hpY2ggY291bGQgbGFuZFxuICAgICAgICAvLyBtaWQtd29yZCAoZS5nLiBcInMgdGhhdCBhbiBFVEYuLi5cIikuXG4gICAgICAgIGNvbnN0IHJhd05leHQgPSBlIC0gb3ZlcmxhcENoYXJzO1xuICAgICAgICBzID0gcmF3TmV4dCA+IHMgPyBzbmFwVG9Cb3VuZGFyeShwYXJhLCByYXdOZXh0LCBlKSA6IGU7XG4gICAgICB9XG4gICAgICBjaGFyQ3Vyc29yICs9IHBhcmEubGVuZ3RoICsgMjtcbiAgICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgICAgIGNvbnRpbnVlO1xuICAgIH1cblxuICAgIC8vIDQuIE5vcm1hbCBwYXJhZ3JhcGggXHUyMDE0IGhhcmQgY2FwIGxvb2thaGVhZCBCRUZPUkUgYWNjdW11bGF0aW5nXG4gICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwICYmIChidWZmZXIubGVuZ3RoICsgcGFyYS5sZW5ndGggKyAyKSA+IG1heENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cblxuICAgIGJ1ZmZlciA9IGJ1ZmZlciA/IGJ1ZmZlciArICdcXG5cXG4nICsgcGFyYSA6IHBhcmE7XG4gICAgY2hhckN1cnNvciArPSBwYXJhLmxlbmd0aCArIDI7XG5cbiAgICAvLyBTb2Z0IGNhcDogZmx1c2ggb25jZSB0YXJnZXQgaXMgcmVhY2hlZFxuICAgIGlmIChidWZmZXIubGVuZ3RoID49IHRhcmdldENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDUuIEZsdXNoIHJlbWFpbmRlclxuICBmbHVzaCgpO1xuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQoY2xlYW5lZFRleHQsIG9wdGlvbnMpO1xuICBjb25zdCB0b3RhbENodW5rcyA9IHRleHRDaHVua3MubGVuZ3RoO1xuICBjb25zdCBzb3VyY2VUeXBlID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6IHNvdXJjZVR5cGUsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9O1xuICB9KTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIGNvbnN0IGxpbmVzID0gdGV4dC5zcGxpdCgnXFxuJykuZmlsdGVyKGwgPT4gbC50cmltKCkpO1xuICBpZiAobGluZXMubGVuZ3RoID4gMCkge1xuICAgIGNvbnN0IGZpcnN0TGluZSA9IGxpbmVzWzBdLnRyaW0oKTtcbiAgICBpZiAoZmlyc3RMaW5lLmxlbmd0aCA8IDEwMCAmJiAhZmlyc3RMaW5lLmVuZHNXaXRoKCcuJykpIHtcbiAgICAgIHJldHVybiBmaXJzdExpbmUuc2xpY2UoMCwgNTApO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBFbWJlZGRpbmdFcnJvciwgaXM0MjlFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMS4gU0xJRElORyBXSU5ET1cgUkFURSBMSU1JVEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNsYXNzIFNsaWRpbmdXaW5kb3dSYXRlTGltaXRlciB7XG4gIGNvbnN0cnVjdG9yKGxpbWl0UGVyTWludXRlKSB7XG4gICAgdGhpcy5saW1pdFBlck1pbnV0ZSA9IGxpbWl0UGVyTWludXRlO1xuICAgIHRoaXMud2luZG93TXMgPSA2MDAwMDtcbiAgICB0aGlzLnJlcXVlc3RzID0gW107XG4gIH1cblxuICBhc3luYyBjb25zdW1lKHRva2Vucykge1xuICAgIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gICAgLy8gUmVtb3ZlIGVudHJpZXMgb2xkZXIgdGhhbiA2MCBzZWNvbmRzXG4gICAgdGhpcy5yZXF1ZXN0cyA9IHRoaXMucmVxdWVzdHMuZmlsdGVyKHJlcSA9PiByZXEudGltZXN0YW1wID4gbm93IC0gdGhpcy53aW5kb3dNcyk7XG5cbiAgICBjb25zdCBjdXJyZW50VG90YWwgPSB0aGlzLnJlcXVlc3RzLnJlZHVjZSgoc3VtLCByZXEpID0+IHN1bSArIHJlcS50b2tlbnMsIDApO1xuXG4gICAgLy8gSWYgd2UgaGF2ZSByb29tLCBjb25zdW1lIGluc3RhbnRseSAoYnVyc3QpXG4gICAgaWYgKGN1cnJlbnRUb3RhbCArIHRva2VucyA8PSB0aGlzLmxpbWl0UGVyTWludXRlKSB7XG4gICAgICB0aGlzLnJlcXVlc3RzLnB1c2goeyB0aW1lc3RhbXA6IG5vdywgdG9rZW5zIH0pO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIC8vIE90aGVyd2lzZSwgd2FpdCB1bnRpbCB0aGUgb2xkZXN0IHJlcXVlc3QgZXhwaXJlcyAocGx1cyBhIHNtYWxsIGJ1ZmZlcilcbiAgICBjb25zdCBuZWVkZWQgPSB0b2tlbnMgLSAodGhpcy5saW1pdFBlck1pbnV0ZSAtIGN1cnJlbnRUb3RhbCk7XG4gICAgbGV0IGFjY3VtdWxhdGVkRXhwaXJlZCA9IDA7XG4gICAgbGV0IHdhaXRVbnRpbCA9IG5vdyArIHRoaXMud2luZG93TXM7IC8vIGZhbGxiYWNrXG5cbiAgICBjb25zdCBzb3J0ZWQgPSBbLi4udGhpcy5yZXF1ZXN0c10uc29ydCgoYSwgYikgPT4gYS50aW1lc3RhbXAgLSBiLnRpbWVzdGFtcCk7XG4gICAgZm9yIChjb25zdCByZXEgb2Ygc29ydGVkKSB7XG4gICAgICBhY2N1bXVsYXRlZEV4cGlyZWQgKz0gcmVxLnRva2VucztcbiAgICAgIGlmIChhY2N1bXVsYXRlZEV4cGlyZWQgPj0gbmVlZGVkKSB7XG4gICAgICAgIC8vICsxMG1zIGJ1ZmZlciB0byBzbGlkZSB0aGUgd2luZG93IGNsZWFubHlcbiAgICAgICAgd2FpdFVudGlsID0gcmVxLnRpbWVzdGFtcCArIHRoaXMud2luZG93TXMgKyAxMDtcbiAgICAgICAgYnJlYWs7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgZGVsYXkgPSB3YWl0VW50aWwgLSBub3c7XG4gICAgaWYgKGRlbGF5ID4gMCkge1xuICAgICAgY29uc29sZS5sb2coXG4gICAgICAgIGBbcmF0ZS1saW1pdF0gV2luZG93IGZ1bGwgKCR7Y3VycmVudFRvdGFsfS8ke3RoaXMubGltaXRQZXJNaW51dGV9KS4gYCArXG4gICAgICAgIGBXYWl0aW5nICR7KGRlbGF5IC8gMTAwMCkudG9GaXhlZCgxKX1zIHRvIHNlbmQgJHt0b2tlbnN9IHRva2Vucy4uLmBcbiAgICAgICk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgZGVsYXkpKTtcbiAgICB9XG5cbiAgICAvLyBSZWNvcmQgdGhlIGNvbnN1bXB0aW9uIGF0IHRoZSBuZXcgdGltZVxuICAgIHRoaXMucmVxdWVzdHMucHVzaCh7IHRpbWVzdGFtcDogRGF0ZS5ub3coKSwgdG9rZW5zIH0pO1xuICAgIC8vIENsZWFudXAgYWdhaW4ganVzdCBpbiBjYXNlXG4gICAgdGhpcy5yZXF1ZXN0cyA9IHRoaXMucmVxdWVzdHMuZmlsdGVyKHJlcSA9PiByZXEudGltZXN0YW1wID4gRGF0ZS5ub3coKSAtIHRoaXMud2luZG93TXMpO1xuICB9XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMi4gQ09ORklHVVJBVElPTlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5jb25zdCBUUE1fTElNSVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX1RQTV9MSU1JVCkgfHwgNTAwMDAwO1xuY29uc3QgUkFURV9MSU1JVEVSID0gbmV3IFNsaWRpbmdXaW5kb3dSYXRlTGltaXRlcihUUE1fTElNSVQpO1xuXG4vLyBCQVRDSF9TSVpFOiBudW1iZXIgb2YgY2h1bmtzIHBlciBlbWJlZENvbnRlbnQgY2FsbFxuLy8gKGtlcHQgYXQgMTA7IG5vdGUgdGhlIHJlYWwgY2VpbGluZyBpcyB0aGUgQVBJJ3MgfjEwMC1yZXF1ZXN0cy1wZXItY2FsbCBsaW1pdCxcbi8vIG5vdCBhIFwiY29udGV4dCB3aW5kb3dcIiBsaW1pdCBcdTIwMTQgMTAganVzdCBrZWVwcyBiYXRjaGVzIHNtYWxsIGFuZCByZXRyeS1mcmllbmRseSlcbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiAxMDsgICAvLyAxMCBjaHVua3MgXHUwMEQ3IDc1MCB0b2tlbnMgPSA3LDUwMCB0b2tlbnMgcGVyIEFQSSByZXF1ZXN0XG5jb25zdCBQQVJBTExFTF9DQUxMUyA9ICgpID0+IDEwOyAvLyBTZW5kIDEwIGJhdGNoZXMgY29uY3VycmVudGx5IHRvIGNsZWFyIHRoZSBidXJzdCBmYXN0XG5cbi8vIFJldHJ5IGNvbmZpZ3VyYXRpb24gKGV4cG9uZW50aWFsIGJhY2tvZmYgKyBqaXR0ZXIpXG5jb25zdCBSRVRSWV9CQVNFX0RFTEFZX01TID0gMjAwMDsgICAvLyAyIHNlY29uZHNcbmNvbnN0IFJFVFJZX01BWF9ERUxBWV9NUyA9IDYwMDAwOyAgIC8vIDYwIHNlY29uZHMgY2FwXG5jb25zdCBNQVhfUkVUUllfQVRURU1QVFMgPSA1O1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDMuIEFJIENMSUVOVCAoc2luZ2xlLCByZXVzYWJsZSBpbnN0YW5jZSlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZnVuY3Rpb24gY3JlYXRlQUlDbGllbnQoKSB7XG4gIGNvbnN0IHByb2plY3QgPSBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfUFJPSkVDVCB8fCBwcm9jZXNzLmVudi5HQ1BfUFJPSkVDVCB8fCAncHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwJztcbiAgY29uc3QgbG9jYXRpb24gPSBwcm9jZXNzLmVudi5HT09HTEVfQ0xPVURfTE9DQVRJT04gfHwgJ3VzLWNlbnRyYWwxJztcblxuICAvLyBTdXBwb3J0IGNyZWRlbnRpYWxzIGZyb20gZW52IHZhciAoZm9yIHNlcnZlcmxlc3MpIG9yIGZpbGUgKGZvciBsb2NhbCBkZXYpXG4gIGNvbnN0IGNyZWRlbnRpYWxzSnNvbiA9IHByb2Nlc3MuZW52LkdPT0dMRV9DUkVERU5USUFMU19KU09OO1xuXG4gIGlmIChjcmVkZW50aWFsc0pzb24pIHtcbiAgICB0cnkge1xuICAgICAgY29uc3QgY3JlZGVudGlhbHMgPSBKU09OLnBhcnNlKGNyZWRlbnRpYWxzSnNvbik7XG4gICAgICByZXR1cm4gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICAgIHByb2plY3QsXG4gICAgICAgIGxvY2F0aW9uLFxuICAgICAgICBjcmVkZW50aWFsc1xuICAgICAgfSk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgY29uc29sZS53YXJuKCdGYWlsZWQgdG8gcGFyc2UgR09PR0xFX0NSRURFTlRJQUxTX0pTT04sIGZhbGxpbmcgYmFjayB0byBkZWZhdWx0IGF1dGgnKTtcbiAgICB9XG4gIH1cblxuICByZXR1cm4gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICB2ZXJ0ZXhhaTogdHJ1ZSxcbiAgICBwcm9qZWN0LFxuICAgIGxvY2F0aW9uXG4gIH0pO1xufVxuXG5jb25zdCBhaSA9IGNyZWF0ZUFJQ2xpZW50KCk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNC4gVE9LRU4gQ0FMQ1VMQVRJT04gKHVzZXMgc3RvcmVkIHRva2VuX2NvdW50IGlmIGF2YWlsYWJsZSlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZnVuY3Rpb24gZ2V0VG9rZW5Db3VudEZvckNodW5rcyhjaHVua3MpIHtcbiAgcmV0dXJuIGNodW5rcy5yZWR1Y2UoKHN1bSwgY2h1bmspID0+IHtcbiAgICAvLyBQcmVmZXIgdGhlIGV4YWN0IHRva2VuIGNvdW50IGZyb20gY2h1bmtlciwgb3RoZXJ3aXNlIGZhbGxiYWNrIHRvIHJvdWdoIGVzdGltYXRlXG4gICAgY29uc3QgdG9rZW5Db3VudCA9IGNodW5rLm1ldGFkYXRhPy50b2tlbl9jb3VudCB8fCBNYXRoLmNlaWwoY2h1bmsudGV4dC5sZW5ndGggLyA0KTtcbiAgICByZXR1cm4gc3VtICsgdG9rZW5Db3VudDtcbiAgfSwgMCk7XG59XG5cbi8vIFNhbWUgcm91Z2ggZXN0aW1hdGUgYXMgYWJvdmUsIGJ1dCBmb3IgcmF3IHN0cmluZ3MgdGhhdCBkb24ndCBjYXJyeSBjaHVuayBtZXRhZGF0YVxuLy8gKHVzZWQgZm9yIHJldHJpZXMgaW5zaWRlIGVtYmVkQmF0Y2gsIGFuZCBmb3IgZW1iZWRRdWVyeSkuXG5mdW5jdGlvbiBlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSB7XG4gIHJldHVybiB0ZXh0cy5yZWR1Y2UoKHN1bSwgdGV4dCkgPT4gc3VtICsgTWF0aC5jZWlsKFN0cmluZyh0ZXh0KS5sZW5ndGggLyA0KSwgMCk7XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNS4gRU1CRUQgQkFUQ0ggKHdpdGggZXhwb25lbnRpYWwgYmFja29mZiArIGppdHRlcilcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbW9kZWxOYW1lID0gcHJvY2Vzcy5lbnYuR0VNSU5JX0VNQkVERElOR19NT0RFTCB8fCAnZ2VtaW5pLWVtYmVkZGluZy0wMDEnO1xuICBjb25zdCBvdXRwdXREaW1lbnNpb25hbGl0eSA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfRElNRU5TSU9OUykgfHwgMzA3MjtcblxuICB0cnkge1xuICAgIC8vIEZJWDogYGFpLmJhdGNoZXMuY3JlYXRlRW1iZWRkaW5nc2AgaXMgbm90IGEgcmVhbCBtZXRob2Qgb24gdGhlIEBnb29nbGUvZ2VuYWkgU0RLLlxuICAgIC8vIGBhaS5iYXRjaGVzYCBpcyBmb3IgYXN5bmMgYmF0Y2gtcHJlZGljdGlvbiBqb2JzLiBTeW5jaHJvbm91cyBlbWJlZGRpbmcgY2FsbHMgZ29cbiAgICAvLyB0aHJvdWdoIGBhaS5tb2RlbHMuZW1iZWRDb250ZW50YCwgd2l0aCBvbmUgc2hhcmVkIHRhc2tUeXBlL291dHB1dERpbWVuc2lvbmFsaXR5XG4gICAgLy8gY29uZmlnIGFwcGxpZWQgYWNyb3NzIGFsbCBgY29udGVudHNgIGluIHRoZSBjYWxsLlxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgYWkubW9kZWxzLmVtYmVkQ29udGVudCh7XG4gICAgICBtb2RlbDogbW9kZWxOYW1lLFxuICAgICAgY29udGVudHM6IHRleHRzLm1hcCh0ZXh0ID0+ICh0eXBlb2YgdGV4dCA9PT0gJ3N0cmluZycgPyB0ZXh0IDogU3RyaW5nKHRleHQpKSksXG4gICAgICBjb25maWc6IHtcbiAgICAgICAgdGFza1R5cGU6IHRhc2tUeXBlLFxuICAgICAgICBvdXRwdXREaW1lbnNpb25hbGl0eTogb3V0cHV0RGltZW5zaW9uYWxpdHlcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGNvbnN0IGVtYmVkZGluZ3MgPSByZXNwb25zZT8uZW1iZWRkaW5ncz8ubWFwKGUgPT4gZS52YWx1ZXMpIHx8IFtdO1xuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtlbWJlZGRpbmdzLmxlbmd0aH1gKTtcbiAgICB9XG4gICAgcmV0dXJuIGVtYmVkZGluZ3M7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zdCBpc1JldHJ5YWJsZSA9IGlzNDI5RXJyb3IoZXJyb3IpIHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMiB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNTAzIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1NlcnZpY2UgVW5hdmFpbGFibGUnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdCYWQgR2F0ZXdheScpO1xuXG4gICAgaWYgKGlzUmV0cnlhYmxlICYmIGF0dGVtcHQgPCBNQVhfUkVUUllfQVRURU1QVFMpIHtcbiAgICAgIC8vIEV4cG9uZW50aWFsIGJhY2tvZmY6IDJeYXR0ZW1wdCAqIGJhc2UgKGNhcHBlZClcbiAgICAgIGxldCBkZWxheSA9IE1hdGgubWluKFJFVFJZX01BWF9ERUxBWV9NUywgUkVUUllfQkFTRV9ERUxBWV9NUyAqIE1hdGgucG93KDIsIGF0dGVtcHQgLSAxKSk7XG4gICAgICAvLyBBZGQgaml0dGVyICgwLjhcdTIwMTMxLjJ4KSB0byBhdm9pZCB0aHVuZGVyaW5nIGhlcmRcbiAgICAgIGNvbnN0IGppdHRlciA9IDAuOCArICgwLjQgKiBNYXRoLnJhbmRvbSgpKTtcbiAgICAgIGRlbGF5ID0gTWF0aC5mbG9vcihkZWxheSAqIGppdHRlcik7XG4gICAgICAvLyBSZXNwZWN0IHJldHJ5LWFmdGVyIGhlYWRlciBpZiBwcmVzZW50XG4gICAgICBpZiAoZXJyb3IucmV0cnlBZnRlcikge1xuICAgICAgICBkZWxheSA9IE1hdGgubWF4KGRlbGF5LCBlcnJvci5yZXRyeUFmdGVyICogMTAwMCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnNvbGUubG9nKFxuICAgICAgICBgW2VtYmVkZGluZ10gXHUyM0YzIFJldHJ5YWJsZSBlcnJvciAoJHtlcnJvcj8uc3RhdHVzIHx8ICd1bmtub3duJ30pLCBgICtcbiAgICAgICAgYHdhaXRpbmcgJHsoZGVsYXkgLyAxMDAwKS50b0ZpeGVkKDEpfXMgKGF0dGVtcHQgJHthdHRlbXB0fS8ke01BWF9SRVRSWV9BVFRFTVBUU30pLi4uYFxuICAgICAgKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCBkZWxheSkpO1xuXG4gICAgICAvLyBGSVg6IGEgcmV0cnkgaXMgYSBicmFuZCBuZXcgQVBJIGNhbGwgYW5kIGNvbnN1bWVzIHJlYWwgcXVvdGEsIGV2ZW4gdGhvdWdoXG4gICAgICAvLyB0aGUgb3JpZ2luYWwgY2FsbCBmYWlsZWQuIFNraXBwaW5nIGNvbnN1bXB0aW9uIGhlcmUgKGFzIGJlZm9yZSkgbGV0IHRoZSBsb2NhbFxuICAgICAgLy8gbGltaXRlciB1bmRlci1yZXBvcnQgYWN0dWFsIHVzYWdlIGR1cmluZyBlcnJvciBzdG9ybXMsIHdoaWNoIG1lYW50IGl0IGtlcHRcbiAgICAgIC8vIHdhdmluZyB0aHJvdWdoIG5ldyBncm91cHMgd2hpbGUgcmV0cmllcyB3ZXJlIGFsc28gaGl0dGluZyB0aGUgQVBJIFx1MjAxNCBtYWtpbmdcbiAgICAgIC8vIDQyOSBzdG9ybXMgd29yc2UgaW5zdGVhZCBvZiBiYWNraW5nIG9mZiBmcm9tIHRoZW0uXG4gICAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKHRleHRzKSk7XG5cbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA2LiBFWFBPUlRFRCBnZW5lcmF0ZUVtYmVkZGluZ3MgKHdpdGggcmF0ZSBsaW1pdGVyICYgYWNjdXJhdGUgdG9rZW5zKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVFbWJlZGRpbmdzKGNodW5rcywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgb25Qcm9ncmVzcykge1xuICBpZiAoIWNodW5rcyB8fCBjaHVua3MubGVuZ3RoID09PSAwKSByZXR1cm4gW107XG5cbiAgY29uc3QgYmF0Y2hTaXplID0gQkFUQ0hfU0laRSgpO1xuICBjb25zdCBwYXJhbGxlbENhbGxzID0gUEFSQUxMRUxfQ0FMTFMoKTtcblxuICAvLyBGaXhlZC1zaXplIGFycmF5IHRvIHByZXNlcnZlIGNocm9ub2xvZ2ljYWwgb3JkZXJcbiAgY29uc3QgZW1iZWRkaW5ncyA9IG5ldyBBcnJheShjaHVua3MubGVuZ3RoKTtcblxuICAvLyBHcm91cCBjaHVua3MgaW50byBiYXRjaGVzIHdpdGggdGhlaXIgc3RhcnRpbmcgaW5kZXhcbiAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNodW5rcy5sZW5ndGg7IGkgKz0gYmF0Y2hTaXplKSB7XG4gICAgYmF0Y2hlcy5wdXNoKHtcbiAgICAgIGNodW5rczogY2h1bmtzLnNsaWNlKGksIGkgKyBiYXRjaFNpemUpLFxuICAgICAgc3RhcnRJbmRleDogaVxuICAgIH0pO1xuICB9XG5cbiAgY29uc3QgdG90YWxHcm91cHMgPSBNYXRoLmNlaWwoYmF0Y2hlcy5sZW5ndGggLyBwYXJhbGxlbENhbGxzKTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IGJhdGNoZXMubGVuZ3RoOyBpICs9IHBhcmFsbGVsQ2FsbHMpIHtcbiAgICBjb25zdCBwYXJhbGxlbEJhdGNoZXMgPSBiYXRjaGVzLnNsaWNlKGksIGkgKyBwYXJhbGxlbENhbGxzKTtcbiAgICBjb25zdCBncm91cE51bSA9IE1hdGguZmxvb3IoaSAvIHBhcmFsbGVsQ2FsbHMpICsgMTtcblxuICAgIC8vIENhbGN1bGF0ZSBleGFjdCB0b2tlbnMgdXNpbmcgc3RvcmVkIHRva2VuX2NvdW50IChvciBmYWxsYmFjaylcbiAgICBjb25zdCBhbGxDaHVua3NJbkdyb3VwID0gcGFyYWxsZWxCYXRjaGVzLmZsYXRNYXAoYiA9PiBiLmNodW5rcyk7XG4gICAgY29uc3QgdG9rZW5zVG9Db25zdW1lID0gZ2V0VG9rZW5Db3VudEZvckNodW5rcyhhbGxDaHVua3NJbkdyb3VwKTtcbiAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZSh0b2tlbnNUb0NvbnN1bWUpO1xuXG4gICAgY29uc29sZS5sb2coXG4gICAgICBgW2VtYmVkZGluZ10gR3JvdXAgJHtncm91cE51bX0vJHt0b3RhbEdyb3Vwc30gXHUyMDE0IGZpcmluZyAke3BhcmFsbGVsQmF0Y2hlcy5sZW5ndGh9IGJhdGNoZXMgYCArXG4gICAgICBgaW4gcGFyYWxsZWwgKCR7dG9rZW5zVG9Db25zdW1lfSB0b2tlbnMpYFxuICAgICk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgcGFyYWxsZWxCYXRjaGVzLm1hcChiID0+IGVtYmVkQmF0Y2goYi5jaHVua3MubWFwKGMgPT4gYy50ZXh0KSwgdGFza1R5cGUpKVxuICAgICk7XG5cbiAgICBjb25zdCBmYWlsZWRCYXRjaGVzID0gW107XG4gICAgcmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGJhdGNoSWR4KSA9PiB7XG4gICAgICBjb25zdCBjdXJyZW50QmF0Y2hJbmZvID0gcGFyYWxsZWxCYXRjaGVzW2JhdGNoSWR4XTtcbiAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICBjb25zdCB2ZWN0b3JzID0gcmVzdWx0LnZhbHVlO1xuICAgICAgICBjdXJyZW50QmF0Y2hJbmZvLmNodW5rcy5mb3JFYWNoKChjaHVuaywgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBnbG9iYWxJbmRleCA9IGN1cnJlbnRCYXRjaEluZm8uc3RhcnRJbmRleCArIGNodW5rSWR4O1xuICAgICAgICAgIGVtYmVkZGluZ3NbZ2xvYmFsSW5kZXhdID0ge1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHtnbG9iYWxJbmRleH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzW2NodW5rSWR4XSxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVuay5tZXRhZGF0YSxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rLnRleHRcbiAgICAgICAgICB9O1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgW2VtYmVkZGluZ10gQmF0Y2ggc3RhcnRpbmcgYXQgaW5kZXggJHtjdXJyZW50QmF0Y2hJbmZvLnN0YXJ0SW5kZXh9IGZhaWxlZDpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgZmFpbGVkQmF0Y2hlcy5wdXNoKGN1cnJlbnRCYXRjaEluZm8pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKG9uUHJvZ3Jlc3MpIHtcbiAgICAgIG9uUHJvZ3Jlc3MoeyBjdXJyZW50X2JhdGNoOiBncm91cE51bSwgdG90YWxfYmF0Y2hlczogdG90YWxHcm91cHMgfSk7XG4gICAgfVxuXG4gICAgLy8gUmV0cnkgZmFpbGVkIGJhdGNoZXMgaW5kaXZpZHVhbGx5XG4gICAgZm9yIChjb25zdCBmYWlsZWRCYXRjaCBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gUmV0cnlpbmcgZmFpbGVkIGJhdGNoIGVsZW1lbnRzIHN0YXJ0aW5nIGF0IGluZGV4ICR7ZmFpbGVkQmF0Y2guc3RhcnRJbmRleH0uLi5gKTtcbiAgICAgIGZvciAobGV0IGNodW5rSWR4ID0gMDsgY2h1bmtJZHggPCBmYWlsZWRCYXRjaC5jaHVua3MubGVuZ3RoOyBjaHVua0lkeCsrKSB7XG4gICAgICAgIGNvbnN0IGNodW5rID0gZmFpbGVkQmF0Y2guY2h1bmtzW2NodW5rSWR4XTtcbiAgICAgICAgY29uc3QgZ2xvYmFsSW5kZXggPSBmYWlsZWRCYXRjaC5zdGFydEluZGV4ICsgY2h1bmtJZHg7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgLy8gRklYOiB0aGlzIHJldHJ5IGlzIGEgZnJlc2gsIHJlYWwgQVBJIGNhbGwgXHUyMDE0IHRyYWNrIGl0cyB0b2tlbnMgYWdhaW5zdFxuICAgICAgICAgIC8vIHRoZSBsaW1pdGVyIGluc3RlYWQgb2YgYXNzdW1pbmcgaXQgd2FzIFwiYWxyZWFkeSBwYWlkIGZvclwiLlxuICAgICAgICAgIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGdldFRva2VuQ291bnRGb3JDaHVua3MoW2NodW5rXSkpO1xuICAgICAgICAgIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtjaHVuay50ZXh0XSwgdGFza1R5cGUpO1xuICAgICAgICAgIGVtYmVkZGluZ3NbZ2xvYmFsSW5kZXhdID0ge1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfcmV0cnlfJHtnbG9iYWxJbmRleH1gLFxuICAgICAgICAgICAgZW1iZWRkaW5nOiB2ZWN0b3JzWzBdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH07XG4gICAgICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFx1MjcwNSBSZXRyeSBzdWNjZWVkZWQgZm9yIGNodW5rICR7Y2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGdsb2JhbEluZGV4fWApO1xuICAgICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgICBjb25zb2xlLmVycm9yKGBbZW1iZWRkaW5nXSBcdTI3NEMgUmV0cnkgZmFpbGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBnbG9iYWxJbmRleH06YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgLy8gRklYOiBwZXJtYW5lbnRseS1mYWlsZWQgY2h1bmtzIGFyZSBkcm9wcGVkIGhlcmUsIHdoaWNoIHNoaWZ0cyBhcnJheSBpbmRpY2VzXG4gIC8vIHJlbGF0aXZlIHRvIHRoZSBvcmlnaW5hbCBgY2h1bmtzYCBpbnB1dC4gVGhpcyBsb2cgbWFrZXMgdGhhdCBsb3NzIHZpc2libGVcbiAgLy8gaW5zdGVhZCBvZiBzaWxlbnQ7IGNhbGxlcnMgdGhhdCBuZWVkIHRvIGtub3cgZXhhY3RseSB3aGljaCBjaHVua3Mgd2VyZSBsb3N0XG4gIC8vIGNhbiBjb21wYXJlIHJldHVybmVkIGBpZGBzIGFnYWluc3QgdGhlaXIgb3JpZ2luYWwgY2h1bmsgbGlzdC5cbiAgY29uc3QgZmFpbGVkQ291bnQgPSBlbWJlZGRpbmdzLmZpbHRlcihlID0+ICFlKS5sZW5ndGg7XG4gIGlmIChmYWlsZWRDb3VudCA+IDApIHtcbiAgICBjb25zb2xlLndhcm4oYFtlbWJlZGRpbmddICR7ZmFpbGVkQ291bnR9LyR7Y2h1bmtzLmxlbmd0aH0gY2h1bmsocykgcGVybWFuZW50bHkgZmFpbGVkIHRvIGVtYmVkIGFuZCB3ZXJlIGRyb3BwZWQuYCk7XG4gIH1cblxuICAvLyBGaWx0ZXIgb3V0IGFueSBlbGVtZW50cyB0aGF0IHBlcm1hbmVudGx5IGZhaWxlZFxuICByZXR1cm4gZW1iZWRkaW5ncy5maWx0ZXIoQm9vbGVhbik7XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNy4gRVhQT1JURUQgZW1iZWRRdWVyeVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICAvLyBGSVg6IHRoaXMgY2FsbCB3YXMgYnlwYXNzaW5nIHRoZSByYXRlIGxpbWl0ZXIgZW50aXJlbHkuIElmIGl0IHJ1bnMgY29uY3VycmVudGx5XG4gIC8vIHdpdGggZG9jdW1lbnQgaW5nZXN0aW9uIChlLmcuIGEgdXNlciBzZWFyY2hlcyB3aGlsZSBhIGJhdGNoIGpvYiBpcyBpbiBmbGlnaHQpLFxuICAvLyBpdCBjb3VsZCBwdXNoIHRvdGFsIHVzYWdlIG92ZXIgdGhlIGNvbmZpZ3VyZWQgVFBNIGJ1ZGdldCB1bm5vdGljZWQuXG4gIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHMoW3F1ZXJ5XSkpO1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKHRleHRzLCB0YXNrVHlwZSA9ICdSRVRSSUVWQUxfRE9DVU1FTlQnKSB7XG4gIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBlbWJlZFNpbmdsZUJhdGNoR3JvdXAgXHUyMDE0ICR7dGV4dHMubGVuZ3RofSB0ZXh0cywgdGFza1R5cGU9JHt0YXNrVHlwZX1gKTtcbiAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykpO1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUpO1xuICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gZW1iZWRTaW5nbGVCYXRjaEdyb3VwIFx1MjAxNCBnb3QgJHt2ZWN0b3JzLmxlbmd0aH0gdmVjdG9yc2ApO1xuICByZXR1cm4gdmVjdG9ycztcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2ltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHtcbiAgZ2V0R2xvYmFsQ29sbGVjdGlvbixcbiAgZ2V0Q29sbGVjdGlvbixcbiAgbGlzdERvY3VtZW50c1xufSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCBERUZBVUxUX1RJTUVPVVRfTUlOVVRFUyA9IDYwO1xuY29uc3Qgc2Vzc2lvbnMgPSBuZXcgTWFwKCk7XG5jb25zdCBNQVhfUERGU19QRVJfU0VTU0lPTiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OKSB8fCAzO1xuY29uc3QgTUFYX1VQTE9BRF9TSVpFX01CID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CKSB8fCA1O1xuXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEdsb2JhbCBkb2N1bWVudHMgY2FjaGUgKHBvcHVsYXRlZCBvbmNlIG9uIGZpcnN0IHNlc3Npb24gaW5pdCkgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5sZXQgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUgPSBbXTtcbmxldCBnbG9iYWxEYXRhSW5pdGlhbGl6ZWQgPSBmYWxzZTtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldEdsb2JhbERvY3VtZW50c0NhY2hlKCkge1xuICByZXR1cm4gZ2xvYmFsRG9jdW1lbnRzQ2FjaGU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBjb25zdCBpZCA9IHNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3Qgc2Vzc2lvbiA9IHtcbiAgICBpZCxcbiAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKCksXG4gICAgbGFzdEFjY2Vzc2VkOiBuZXcgRGF0ZSgpLFxuICAgIGRvY3VtZW50czogW10sXG4gICAgZGVsZXRlZERvY3VtZW50SWRzOiBuZXcgU2V0KCksXG4gICAgdGltZW91dE1pbnV0ZXM6IERFRkFVTFRfVElNRU9VVF9NSU5VVEVTXG4gIH07XG4gIHNlc3Npb25zLnNldChpZCwgc2Vzc2lvbik7XG4gIHJldHVybiBzZXNzaW9uO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IHNlc3Npb25zLmdldChzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIHJldHVybiBzZXNzaW9uO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgY29uc3QgZXhpc3RpbmcgPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGV4aXN0aW5nKSByZXR1cm4gZXhpc3Rpbmc7XG4gICAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgfVxuICByZXR1cm4gY3JlYXRlU2Vzc2lvbigpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSB7XG4gIGNvbnN0IG5vdyA9IERhdGUubm93KCk7XG4gIGNvbnN0IGxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKHNlc3Npb24ubGFzdEFjY2Vzc2VkKS5nZXRUaW1lKCk7XG4gIGNvbnN0IHRpbWVvdXRNcyA9IHNlc3Npb24udGltZW91dE1pbnV0ZXMgKiA2MCAqIDEwMDA7XG4gIHJldHVybiAobm93IC0gbGFzdEFjY2Vzc2VkKSA+IHRpbWVvdXRNcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlbGV0ZVNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIHNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xuICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIENoZWNrIGlmIHNlc3Npb24gaXMgc2VlZGVkIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvblNlZWRlZChzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIHNlZWRlZFNlc3Npb25zLmhhcyhzZXNzaW9uSWQpO1xufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgTm90aWZ5IFNTRSBsaXN0ZW5lcnMgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5mdW5jdGlvbiBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKSB7XG4gIGlmIChnbG9iYWwuc2VlZGluZ0xpc3RlbmVycyAmJiBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5oYXMoYHNlZWRpbmc6JHtzZXNzaW9uSWR9YCkpIHtcbiAgICBjb25zdCBldmVudEtleSA9IGBzZWVkaW5nOiR7c2Vzc2lvbklkfWA7XG4gICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICBsaXN0ZW5lcnMuZm9yRWFjaCgocmVzcG9uc2UpID0+IHtcbiAgICAgIHRyeSB7XG4gICAgICAgIHJlc3BvbnNlLndyaXRlKGBldmVudDogc2VlZGluZ19jb21wbGV0ZVxcbmRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoeyBzZXNzaW9uSWQsIHNlZWRlZDogdHJ1ZSB9KX1cXG5cXG5gKTtcbiAgICAgICAgcmVzcG9uc2UuZW5kKCk7XG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcihgW25vdGlmeV0gRmFpbGVkIHRvIG5vdGlmeSBsaXN0ZW5lcjpgLCBlcnIubWVzc2FnZSk7XG4gICAgICB9XG4gICAgfSk7XG4gICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgICBjb25zb2xlLmxvZyhgW25vdGlmeV0gTm90aWZpZWQgJHtsaXN0ZW5lcnMubGVuZ3RofSBTU0UgbGlzdGVuZXJzIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICB9XG59XG5cbi8qKlxuICogT24gc2Vzc2lvbiBzdGFydDpcbiAqIC0gUmVjb25zdHJ1Y3QgaW4tbWVtb3J5IHNlc3Npb24gZG9jIGxpc3QgZnJvbSB0aGUgc2luZ2xlIGNvbGxlY3Rpb25cbiAqICAgYnkgZmlsdGVyaW5nIG9uIHNlc3Npb25faWQgbWV0YWRhdGEuXG4gKiAtIE5vIHZlY3RvciBjb3B5aW5nIGlzIHBlcmZvcm1lZCBcdTIwMTQgZ2xvYmFsIGRvY3MgYXJlIHNlcnZlZCBmcm9tIGNhY2hlLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpIHtcbiAgY29uc29sZS5sb2coYFx1RDgzRFx1REQxMSBTZXNzaW9uIGluaXQ6ICR7c2Vzc2lvbklkfWApO1xuICBpZiAoc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIEFscmVhZHkgc2VlZGVkICR7c2Vzc2lvbklkfSwgc2tpcHBpbmdgKTtcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgICByZXR1cm47XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG5cbiAgICAvLyBcdTI1MDBcdTI1MDAgTGF6eSBvbmUtdGltZSBnbG9iYWwgY2FjaGUgaW5pdCAocnVucyBvbiBmaXJzdCBzZXNzaW9uIGluaXQpIFx1MjUwMFx1MjUwMFxuICAgIGlmICghZ2xvYmFsRGF0YUluaXRpYWxpemVkKSB7XG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCBnbG9iYWxEb2NzID0gYXdhaXQgbGlzdERvY3VtZW50cyhjb2xsZWN0aW9uLCB7IHNlc3Npb25faWQ6ICdnbG9iYWwnIH0pO1xuICAgICAgICBnbG9iYWxEb2N1bWVudHNDYWNoZSA9IGdsb2JhbERvY3MubWFwKGRvYyA9PiAoe1xuICAgICAgICAgIGlkOiBkb2MuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICBmaWxlU2l6ZTogbnVsbCxcbiAgICAgICAgICBwYWdlQ291bnQ6IGRvYy5wYWdlX2NvdW50IHx8IG51bGwsXG4gICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgIHNvdXJjZVR5cGU6ICdnbG9iYWwnLFxuICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogZG9jLnVwbG9hZF90aW1lc3RhbXBcbiAgICAgICAgfSkpO1xuICAgICAgICBnbG9iYWxEYXRhSW5pdGlhbGl6ZWQgPSB0cnVlO1xuICAgICAgICBjb25zb2xlLmxvZyhgXHUyNzA1IEdsb2JhbCBkb2N1bWVudHMgY2FjaGUgbG9hZGVkOiAke2dsb2JhbERvY3VtZW50c0NhY2hlLmxlbmd0aH0gZG9jdW1lbnQocylgKTtcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdcdTI3NEMgRmFpbGVkIHRvIGluaXRpYWxpemUgZ2xvYmFsIGRhdGE6JywgZXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gICAgLy8gUmVjb25zdHJ1Y3Qgc2Vzc2lvbi1zcGVjaWZpYyBkb2NzICh1c2VyIHVwbG9hZHMpIGZyb20gdGhlIGNvbGxlY3Rpb25cbiAgICBpZiAoc2Vzc2lvbiAmJiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnN0IGRvY3MgPSBhd2FpdCBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHsgc2Vzc2lvbl9pZDogc2Vzc2lvbklkIH0pO1xuICAgICAgZG9jcy5mb3JFYWNoKGRvYyA9PiB7XG4gICAgICAgIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgICAgICAgIGlkOiBkb2MuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgZmlsZW5hbWU6IGRvYy5maWxlbmFtZSxcbiAgICAgICAgICBmaWxlU2l6ZTogbnVsbCxcbiAgICAgICAgICBwYWdlQ291bnQ6IGRvYy5wYWdlX2NvdW50IHx8IG51bGwsXG4gICAgICAgICAgY2h1bmtDb3VudDogZG9jLmNodW5rX2NvdW50LFxuICAgICAgICAgIHNvdXJjZVR5cGU6ICdzZXNzaW9uX3VwbG9hZCcsXG4gICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBkb2MudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgICAgaWYgKGRvY3MubGVuZ3RoID4gMCkge1xuICAgICAgICBjb25zb2xlLmxvZyhgXHUyNjdCXHVGRTBGICBSZWNvbnN0cnVjdGVkICR7ZG9jcy5sZW5ndGh9IHNlc3Npb24gZG9jdW1lbnQocykgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgIH1cbiAgICBzZWVkZWRTZXNzaW9ucy5hZGQoc2Vzc2lvbklkKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNzA1IFNlc3Npb24gJHtzZXNzaW9uSWR9IHJlYWR5IChubyB2ZWN0b3IgY29weWluZyBuZWVkZWQpYCk7XG4gICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIGluaXQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gU3RpbGwgbm90aWZ5IGxpc3RlbmVycyBzbyB0aGV5IGRvbid0IGhhbmcgZm9yZXZlclxuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuICB9XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBEb2N1bWVudCBtYW5hZ2VtZW50IFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZXhwb3J0IGZ1bmN0aW9uIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJbmZvKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuXG4gIGNvbnN0IGV4aXN0aW5nID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZChkID0+IGQuaWQgPT09IGRvY3VtZW50SW5mby5pZCk7XG5cbiAgaWYgKGV4aXN0aW5nKSB7XG4gICAgaWYgKGRvY3VtZW50SW5mby5jaHVua0NvdW50ICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmNodW5rQ291bnQgPSBkb2N1bWVudEluZm8uY2h1bmtDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5wYWdlQ291bnQgPSBkb2N1bWVudEluZm8ucGFnZUNvdW50O1xuICAgIGlmIChkb2N1bWVudEluZm8uZmlsZVNpemUgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcuZmlsZVNpemUgPSBkb2N1bWVudEluZm8uZmlsZVNpemU7XG4gICAgaWYgKGRvY3VtZW50SW5mby5zdGF0dXMgIT09IHVuZGVmaW5lZCkgZXhpc3Rpbmcuc3RhdHVzID0gZG9jdW1lbnRJbmZvLnN0YXR1cztcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVuYW1lICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVuYW1lID0gZG9jdW1lbnRJbmZvLmZpbGVuYW1lO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFVwZGF0ZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZXhpc3Rpbmcuc3RhdHVzfSwgY2h1bmtzPSR7ZXhpc3RpbmcuY2h1bmtDb3VudH1gKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQgPz8gMCxcbiAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgIHN0YXR1czogZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnXG4gIH0pO1xuICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gIGNvbnNvbGUubG9nKGBbc2Vzc2lvbl0gQWRkZWQgZG9jICR7ZG9jdW1lbnRJbmZvLmlkfSBcdTIwMTQgc3RhdHVzPSR7ZG9jdW1lbnRJbmZvLnN0YXR1cyA/PyAnaW5kZXhpbmcnfWApO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5kZWxldGVkRG9jdW1lbnRJZHMuYWRkKGRvY3VtZW50SWQpO1xuICAgIHNlc3Npb24ubGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoKTtcbiAgICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIFJlbW92ZWQgZG9jICR7ZG9jdW1lbnRJZH0sIGFkZGVkIHRvIGRlbGV0ZWREb2N1bWVudElkc2ApO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldERlbGV0ZWREb2N1bWVudElkcyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHNlc3Npb24/LmRlbGV0ZWREb2N1bWVudElkcyA/PyBuZXcgU2V0KCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTZXNzaW9uRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBbXTtcbiAgcmV0dXJuIHNlc3Npb24uZG9jdW1lbnRzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiB7IHNlc3Npb25Eb2N1bWVudHM6IFtdLCBnbG9iYWxEb2N1bWVudHM6IFtdIH07XG5cbiAgY29uc3Qgbm9ybWFsaXplID0gKGRvYykgPT4gKHtcbiAgICBkb2N1bWVudF9pZDogZG9jLmlkLFxuICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgY2h1bmtfY291bnQ6IGRvYy5jaHVua0NvdW50ID8/IDAsXG4gICAgcGFnZV9jb3VudDogZG9jLnBhZ2VDb3VudCA/PyAwLFxuICAgIHVwbG9hZF90aW1lc3RhbXA6IGRvYy51cGxvYWRUaW1lc3RhbXAgfHwgbnVsbCxcbiAgICBzb3VyY2VfdHlwZTogZG9jLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcgPyAnc2Vzc2lvbl91cGxvYWQnIDogJ3NlZWQnLFxuICAgIGZpbGVTaXplOiBkb2MuZmlsZVNpemUgfHwgbnVsbCxcbiAgICBzdGF0dXM6IGRvYy5zdGF0dXMgPz8gbnVsbFxuICB9KTtcblxuICByZXR1cm4ge1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb24uZG9jdW1lbnRzXG4gICAgICAuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKVxuICAgICAgLm1hcChub3JtYWxpemUpLFxuICAgIGdsb2JhbERvY3VtZW50czogZ2xvYmFsRG9jdW1lbnRzQ2FjaGVcbiAgICAgIC5tYXAobm9ybWFsaXplKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCArIGdsb2JhbERvY3VtZW50c0NhY2hlLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgICAgICsgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsaXN0QWN0aXZlU2Vzc2lvbnMoKSB7XG4gIHJldHVybiBBcnJheS5mcm9tKHNlc3Npb25zLnZhbHVlcygpKS5maWx0ZXIocyA9PiAhaXNTZXNzaW9uRXhwaXJlZChzKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhbnVwRXhwaXJlZFNlc3Npb25zKCkge1xuICBsZXQgY2xlYW5lZCA9IDA7XG4gIGZvciAoY29uc3QgW2lkLCBzZXNzaW9uXSBvZiBzZXNzaW9ucykge1xuICAgIGlmIChpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pKSB7XG4gICAgICBzZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgc2VlZGVkU2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIGNsZWFuZWQrKztcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGNsZWFuZWQ7XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2RvY3VtZW50cy5qc1wiOyAgaW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG4gIGltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbiAgaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG4gIGltcG9ydCBmcyBmcm9tICdmcyc7XG4gIGltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuICBpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcbiAgaW1wb3J0IHBkZiBmcm9tICdwZGYtcGFyc2UnO1xuICBpbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbiAgaW1wb3J0IHsgc2FuaXRpemVGaWxlbmFtZSB9IGZyb20gJy4uL3V0aWxzL3Nhbml0aXplLmpzJztcbiAgaW1wb3J0IHtcbiAgICBDb3JydXB0ZWRQREZFcnJvcixcbiAgICBJbnZhbGlkRmlsZVR5cGVFcnJvcixcbiAgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuICBpbXBvcnQgeyBnZXRDb2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbiAgaW1wb3J0IHsgY2h1bmtUZXh0LCBjbGVhblRleHQgfSBmcm9tICcuLi91dGlscy9jaHVua2VyLmpzJztcbiAgaW1wb3J0IHsgZW1iZWRTaW5nbGVCYXRjaEdyb3VwIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG4gIGltcG9ydCB7XG4gICAgZ2V0T3JDcmVhdGVTZXNzaW9uLFxuICAgIGFkZERvY3VtZW50VG9TZXNzaW9uLFxuICAgIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24sXG4gICAgZ2V0QWxsRG9jdW1lbnRzLFxuICAgIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3MsXG4gICAgaXNTZXNzaW9uU2VlZGVkXG4gIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuICBpbXBvcnQgeyBjbGVhck1lbW9yeSB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG4gIGNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG4gIGNvbnN0IF9fZmlsZW5hbWUgPSBmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCk7XG4gIGNvbnN0IF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShfX2ZpbGVuYW1lKTtcblxuICBjb25zdCB1cGxvYWREaXIgPSAnL3RtcC91cGxvYWRzJztcbiAgaWYgKCFmcy5leGlzdHNTeW5jKHVwbG9hZERpcikpIHtcbiAgICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbiAgfVxuXG4gIC8vIFNlZWQgZG9jdW1lbnRzIGRpcmVjdG9yeSAtIHdvcmtzIGluIGJvdGggZGV2IGFuZCBzZXJ2ZXJsZXNzXG4gIC8vIEluIGRldjogc2VydmVyL2FwaS8uLi8uLi9zZWVkX2RvY3VtZW50c1xuICAvLyBJbiBzZXJ2ZXJsZXNzOiBuZXRsaWZ5L2Z1bmN0aW9ucy8uLi8uLi9zZWVkX2RvY3VtZW50cyAoY29waWVkIHRvIGRpc3QpXG4gIGxldCBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL3NlZWRfZG9jdW1lbnRzJyk7XG4gIGlmICghZnMuZXhpc3RzU3luYyhzZWVkRGlyKSkge1xuICAgIC8vIFRyeSBhbHRlcm5hdGl2ZSBwYXRoIGZvciBzZXJ2ZXJsZXNzIGRlcGxveW1lbnRcbiAgICBzZWVkRGlyID0gcGF0aC5yZXNvbHZlKHByb2Nlc3MuY3dkKCksICdzZWVkX2RvY3VtZW50cycpO1xuICB9XG4gIGlmICghZnMuZXhpc3RzU3luYyhzZWVkRGlyKSkge1xuICAgIC8vIFRyeSBkaXN0IGZvbGRlciBmb3IgZGVwbG95ZWQgc3RhdGljIGZpbGVzXG4gICAgc2VlZERpciA9IHBhdGgucmVzb2x2ZShwcm9jZXNzLmN3ZCgpLCAnZGlzdC9zZWVkX2RvY3VtZW50cycpO1xuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIFNTRSBldmVudCBoZWxwZXIgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGZ1bmN0aW9uIHNzZUV2ZW50KHJlcywgZXZlbnQsIGRhdGEpIHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH1cblxuICBjb25zdCBzdG9yYWdlID0gbXVsdGVyLmRpc2tTdG9yYWdlKHtcbiAgICBkZXN0aW5hdGlvbjogKHJlcSwgZmlsZSwgY2IpID0+IGNiKG51bGwsIHVwbG9hZERpciksXG4gICAgZmlsZW5hbWU6IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKSlcbiAgfSk7XG5cbiAgY29uc3QgdXBsb2FkID0gbXVsdGVyKHtcbiAgICBzdG9yYWdlLFxuICAgIGxpbWl0czogeyBmaWxlU2l6ZTogcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1VQTE9BRF9TSVpFX01CIHx8ICc1JykgKiAxMDI0ICogMTAyNCB9LFxuICAgIGZpbGVGaWx0ZXI6IChyZXEsIGZpbGUsIGNiKSA9PiB7XG4gICAgICBpZiAoZmlsZS5taW1ldHlwZSA9PT0gJ2FwcGxpY2F0aW9uL3BkZicgJiYgcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKS50b0xvd2VyQ2FzZSgpID09PSAnLnBkZicpIHtcbiAgICAgICAgY2IobnVsbCwgdHJ1ZSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjYihuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKSk7XG4gICAgICB9XG4gICAgfVxuICB9KTtcblxuICBmdW5jdGlvbiBjb250ZW50RGlzcG9zaXRpb24oZGlzcGxheU5hbWUpIHtcbiAgICBjb25zdCBlbmNvZGVkID0gZW5jb2RlVVJJQ29tcG9uZW50KGRpc3BsYXlOYW1lKVxuICAgICAgLnJlcGxhY2UoLycvZywgJyUyNycpXG4gICAgICAucmVwbGFjZSgvXFwoL2csICclMjgnKVxuICAgICAgLnJlcGxhY2UoL1xcKS9nLCAnJTI5Jyk7XG4gICAgcmV0dXJuIGBpbmxpbmU7IGZpbGVuYW1lPVwiZG9jdW1lbnQucGRmXCI7IGZpbGVuYW1lKj1VVEYtOCcnJHtlbmNvZGVkfWA7XG4gIH1cblxuICAvKipcbiAgICogSm9pbiBwZGYuanMgdGV4dC1jb250ZW50IGl0ZW1zIGludG8gYSBzaW5nbGUgc3RyaW5nIHVzaW5nIGVhY2ggaXRlbSdzXG4gICAqIHgtcG9zaXRpb24gKHRyYW5zZm9ybVs0XSkgYW5kIHdpZHRoIHRvIGRlY2lkZSB3aGV0aGVyIGEgc3BhY2UgYmVsb25nc1xuICAgKiBiZXR3ZWVuIHR3byBpdGVtcywgaW5zdGVhZCBvZiBhbHdheXMgam9pbmluZyB3aXRoIGEgc2luZ2xlIHNwYWNlLlxuICAgKlxuICAgKiBUaGlzIGF2b2lkcyB0d28gY29tbW9uIGFydGlmYWN0cyBmcm9tIG5haXZlIGAuam9pbignICcpYDpcbiAgICogIC0gd29yZHMgc3BsaXQgYWNyb3NzIGFkamFjZW50IHRleHQgcnVucyBnZXR0aW5nIGEgcGhhbnRvbSBzcGFjZVxuICAgKiAgICBpbnNlcnRlZCBpbiB0aGUgbWlkZGxlIChlLmcuIFwiU2F2IGluZ3NcIilcbiAgICogIC0gYWRqYWNlbnQgd29yZHMgd2l0aCBubyBzcGFjZSBpbiB0aGUgUERGJ3MgaW50ZXJuYWwgcnVucyBnZXR0aW5nXG4gICAqICAgIGdsdWVkIHRvZ2V0aGVyIChlLmcuIFwidGhlIHJlcG9ydFwiIC0+IFwidGhlcmVwb3J0XCIpXG4gICAqXG4gICAqIEVtcHR5LXN0cmluZyBpdGVtcyBhcmUgcGRmLmpzJ3Mgc2lnbmFsIGZvciBhIGxpbmUgYnJlYWssIHdoaWNoIHdlXG4gICAqIGNvbnZlcnQgdG8gYSBuZXdsaW5lIHNvIHBhcmFncmFwaCBzdHJ1Y3R1cmUgaXNuJ3QgbG9zdC5cbiAgICovXG4gIGZ1bmN0aW9uIGpvaW5UZXh0SXRlbXMoaXRlbXMpIHtcbiAgICBsZXQgb3V0ID0gJyc7XG4gICAgbGV0IHByZXZJdGVtID0gbnVsbDtcblxuICAgIGZvciAoY29uc3QgaXRlbSBvZiBpdGVtcykge1xuICAgICAgY29uc3Qgc3RyID0gaXRlbS5zdHI7XG4gICAgICBpZiAoc3RyID09PSB1bmRlZmluZWQpIHsgcHJldkl0ZW0gPSBpdGVtOyBjb250aW51ZTsgfVxuXG4gICAgICBpZiAoc3RyID09PSAnJykge1xuICAgICAgICAvLyBwZGYuanMgZW1pdHMgZW1wdHkgaXRlbXMgdG8gc2lnbmFsIGxpbmUgYnJlYWtzXG4gICAgICAgIGlmICghL1xcbiQvLnRlc3Qob3V0KSkgb3V0ICs9ICdcXG4nO1xuICAgICAgICBwcmV2SXRlbSA9IG51bGw7XG4gICAgICAgIGNvbnRpbnVlO1xuICAgICAgfVxuXG4gICAgICBpZiAocHJldkl0ZW0gJiYgcHJldkl0ZW0uc3RyKSB7XG4gICAgICAgIGNvbnN0IHByZXZFbmQgPSBwcmV2SXRlbS50cmFuc2Zvcm1bNF0gKyAocHJldkl0ZW0ud2lkdGggfHwgMCk7XG4gICAgICAgIGNvbnN0IGN1clN0YXJ0ID0gaXRlbS50cmFuc2Zvcm1bNF07XG4gICAgICAgIGNvbnN0IGdhcCA9IGN1clN0YXJ0IC0gcHJldkVuZDtcbiAgICAgICAgY29uc3QgZm9udEggPSBNYXRoLmFicyhpdGVtLnRyYW5zZm9ybVszXSkgfHwgMTA7XG4gICAgICAgIGNvbnN0IHNwYWNlVGhyZXNob2xkID0gZm9udEggKiAwLjI1O1xuXG4gICAgICAgIGNvbnN0IGFscmVhZHlTcGFjZWQgPSAvXFxzJC8udGVzdChvdXQpIHx8IC9eXFxzLy50ZXN0KHN0cik7XG4gICAgICAgIGlmICghYWxyZWFkeVNwYWNlZCAmJiBnYXAgPiBzcGFjZVRocmVzaG9sZCkge1xuICAgICAgICAgIG91dCArPSAnICc7XG4gICAgICAgIH1cbiAgICAgICAgLy8gZWxzZTogaXRlbXMgYXJlIHRvdWNoaW5nL292ZXJsYXBwaW5nIC0+IHNhbWUgd29yZCwgbm8gc3BhY2UgaW5zZXJ0ZWRcbiAgICAgIH1cblxuICAgICAgb3V0ICs9IHN0cjtcbiAgICAgIHByZXZJdGVtID0gaXRlbTtcbiAgICB9XG5cbiAgICByZXR1cm4gb3V0O1xuICB9XG5cbiAgYXN5bmMgZnVuY3Rpb24gcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZVBhdGgpIHtcbiAgICB0cnkge1xuICAgICAgY29uc3QgYnVmZmVyID0gZnMucmVhZEZpbGVTeW5jKGZpbGVQYXRoKTtcblxuICAgICAgY29uc3QgcGFnZXMgPSBbXTtcbiAgICAgIGF3YWl0IHBkZihidWZmZXIsIHtcbiAgICAgICAgcGFnZXJlbmRlcjogKHBhZ2VEYXRhKSA9PiB7XG4gICAgICAgICAgcmV0dXJuIHBhZ2VEYXRhLmdldFRleHRDb250ZW50KCkudGhlbih0YyA9PiB7XG4gICAgICAgICAgICBjb25zdCBwYWdlVGV4dCA9IGpvaW5UZXh0SXRlbXModGMuaXRlbXMpO1xuICAgICAgICAgICAgcGFnZXMucHVzaChwYWdlVGV4dCk7XG4gICAgICAgICAgICByZXR1cm4gcGFnZVRleHQ7XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICBpZiAocGFnZXMubGVuZ3RoID09PSAwIHx8IHBhZ2VzLmV2ZXJ5KHAgPT4gIXAudHJpbSgpKSkge1xuICAgICAgICBjb25zdCBmdWxsID0gYXdhaXQgcGRmKGJ1ZmZlcik7XG4gICAgICAgIHBhZ2VzLnB1c2goZnVsbC50ZXh0KTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgdG90YWxQYWdlcyA9IHBhZ2VzLmxlbmd0aDtcbiAgICAgIGNvbnN0IGNsZWFuZWRQYWdlcyA9IHBhZ2VzLm1hcChwID0+IGNsZWFuVGV4dChwKSk7XG4gICAgICBjb25zdCBwYWdlTWFwID0gW107XG4gICAgICBsZXQgY2hhclBvcyA9IDA7XG5cbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2xlYW5lZFBhZ2VzLmxlbmd0aDsgaSsrKSB7XG4gICAgICAgIHBhZ2VNYXAucHVzaCh7IHBhZ2U6IGkgKyAxLCBzdGFydDogY2hhclBvcywgZW5kOiBjaGFyUG9zICsgY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCB9KTtcbiAgICAgICAgY2hhclBvcyArPSBjbGVhbmVkUGFnZXNbaV0ubGVuZ3RoICsgMTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgZnVsbFRleHQgPSBjbGVhbmVkUGFnZXMuam9pbignXFxuJyk7XG4gICAgICByZXR1cm4geyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9O1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdQREYgcGFyc2luZyBlcnJvcjonLCBlcnJvcik7XG4gICAgICB0aHJvdyBuZXcgQ29ycnVwdGVkUERGRXJyb3IoKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogR2l2ZW4gYSBjaHVuaydzIFtjaGFyU3RhcnQsIGNoYXJFbmQpIHJhbmdlLCBmaW5kIHdoaWNoIHBhZ2UocykgaXRcbiAgICogb3ZlcmxhcHMuIFJldHVybnMgdGhlIG1ham9yaXR5IHBhZ2UgKG1vc3Qgb3ZlcmxhcHBpbmcgY2hhcnMsIHVzZWRcbiAgICogZm9yIGBwYWdlX251bWJlcmAgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHkpIHBsdXMgdGhlIHRydWUgc3RhcnQvZW5kXG4gICAqIHBhZ2VzIHNvIGNodW5rcyBzcGFubmluZyBhIHBhZ2UgYnJlYWsgYXJlbid0IHNpbGVudGx5IG1pc2xhYmVsZWQgd2l0aFxuICAgKiBqdXN0IHRoZSBmaXJzdCBwYWdlLlxuICAgKi9cbiAgZnVuY3Rpb24gZ2V0UGFnZVJhbmdlKGNoYXJTdGFydCwgY2hhckVuZCwgcGFnZU1hcCkge1xuICAgIGxldCBzdGFydFBhZ2UgPSBudWxsO1xuICAgIGxldCBlbmRQYWdlID0gbnVsbDtcbiAgICBsZXQgYmVzdFBhZ2UgPSBudWxsO1xuICAgIGxldCBtYXhPdmVybGFwID0gLTE7XG5cbiAgICBmb3IgKGNvbnN0IGVudHJ5IG9mIHBhZ2VNYXApIHtcbiAgICAgIGNvbnN0IG92ZXJsYXBTdGFydCA9IE1hdGgubWF4KGNoYXJTdGFydCwgZW50cnkuc3RhcnQpO1xuICAgICAgY29uc3Qgb3ZlcmxhcEVuZCA9IE1hdGgubWluKGNoYXJFbmQsIGVudHJ5LmVuZCk7XG4gICAgICBjb25zdCBvdmVybGFwID0gb3ZlcmxhcEVuZCAtIG92ZXJsYXBTdGFydDtcbiAgICAgIGlmIChvdmVybGFwIDw9IDApIGNvbnRpbnVlO1xuXG4gICAgICBpZiAoc3RhcnRQYWdlID09PSBudWxsKSBzdGFydFBhZ2UgPSBlbnRyeS5wYWdlO1xuICAgICAgZW5kUGFnZSA9IGVudHJ5LnBhZ2U7XG5cbiAgICAgIGlmIChvdmVybGFwID4gbWF4T3ZlcmxhcCkge1xuICAgICAgICBtYXhPdmVybGFwID0gb3ZlcmxhcDtcbiAgICAgICAgYmVzdFBhZ2UgPSBlbnRyeS5wYWdlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChzdGFydFBhZ2UgPT09IG51bGwpIHtcbiAgICAgIGNvbnN0IGxhc3RQYWdlID0gcGFnZU1hcFtwYWdlTWFwLmxlbmd0aCAtIDFdPy5wYWdlIHx8IDE7XG4gICAgICByZXR1cm4geyBwYWdlOiBsYXN0UGFnZSwgcGFnZVN0YXJ0OiBsYXN0UGFnZSwgcGFnZUVuZDogbGFzdFBhZ2UgfTtcbiAgICB9XG5cbiAgICByZXR1cm4geyBwYWdlOiBiZXN0UGFnZSwgcGFnZVN0YXJ0OiBzdGFydFBhZ2UsIHBhZ2VFbmQ6IGVuZFBhZ2UgfTtcbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBVcGxvYWQgaGFuZGxlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgICByZXMuZmx1c2hIZWFkZXJzKCk7XG5cbiAgICBjb25zdCBCQVRDSF9TSVpFID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX0JBVENIX01BWF9DSFVOS1MpIHx8IDEwO1xuICAgIGNvbnN0IFBBUkFMTEVMX0NBTExTID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCAxMDtcbiAgICBjb25zdCBHUk9VUF9XQUlUX01TID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX0dST1VQX1dBSVRfTVMpIHx8IDE7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgZmlsZSA9IHJlcS5maWxlO1xuICAgICAgaWYgKCFmaWxlKSB0aHJvdyBuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKTtcblxuICAgICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICAgIGNvbnN0IHNlc3Npb24gPSBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcbiAgICAgIGNvbnN0IG1heFBERnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTiB8fCAnMycpO1xuICAgICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGg7XG4gICAgICBpZiAodXBsb2FkZWRDb3VudCA+PSBtYXhQREZzKSB7XG4gICAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBNYXhpbXVtICR7bWF4UERGc30gdXBsb2FkcyByZWFjaGVkYCwgY29kZTogJ1RPT19NQU5ZX1BERlMnIH0pO1xuICAgICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgICAgfVxuXG4gICAgICBpZiAoc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGNsZWFuRmlsZW5hbWUpKSB7XG4gICAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGBcIiR7Y2xlYW5GaWxlbmFtZX1cIiBhbHJlYWR5IHVwbG9hZGVkYCwgY29kZTogJ0RVUExJQ0FURV9GSUxFJyB9KTtcbiAgICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICAgIH1cblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgXHUyMDE0IHBhcnNpbmcgJHtjbGVhbkZpbGVuYW1lfSAoJHtmaWxlLnNpemV9IGJ5dGVzKWApO1xuICAgICAgY29uc3QgeyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9ID0gYXdhaXQgcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZS5wYXRoKTtcblxuICAgICAgaWYgKCFmdWxsVGV4dCB8fCBmdWxsVGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ05vIGV4dHJhY3RhYmxlIHRleHQgXHUyMDE0IFBERiBtYXkgYmUgc2Nhbm5lZCBvciBpbWFnZS1vbmx5JywgY29kZTogJ0VNUFRZX1BERicgfSk7XG4gICAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGRvY3VtZW50SWQgPSB1dWlkdjQoKTtcbiAgICAgIGNvbnN0IHJhd0NodW5rcyA9IGNodW5rVGV4dChmdWxsVGV4dCk7XG5cbiAgICAgIGlmIChyYXdDaHVua3MubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IHsgcGFnZSwgcGFnZVN0YXJ0LCBwYWdlRW5kIH0gPSBnZXRQYWdlUmFuZ2UoY2h1bmsuY2hhclN0YXJ0LCBjaHVuay5jaGFyRW5kLCBwYWdlTWFwKTtcbiAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0LFxuICAgICAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgICAgICAgY2h1bmtfaWQ6IGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICAgICAgY2h1bmtfaW5kZXg6IGlkeCxcbiAgICAgICAgICAgIHRvdGFsX2NodW5rczogcmF3Q2h1bmtzLmxlbmd0aCxcbiAgICAgICAgICAgIHBhZ2VfbnVtYmVyOiBwYWdlLCAgICAgICAvLyBtYWpvcml0eSBwYWdlIFx1MjAxNCBrZXB0IGZvciBiYWNrd2FyZCBjb21wYXRpYmlsaXR5XG4gICAgICAgICAgICBwYWdlX3N0YXJ0OiBwYWdlU3RhcnQsICAgLy8gbmV3OiBmaXJzdCBwYWdlIHRoaXMgY2h1bmsgb3ZlcmxhcHNcbiAgICAgICAgICAgIHBhZ2VfZW5kOiBwYWdlRW5kLCAgICAgICAvLyBuZXc6IGxhc3QgcGFnZSB0aGlzIGNodW5rIG92ZXJsYXBzXG4gICAgICAgICAgICB0b3RhbF9wYWdlczogdG90YWxQYWdlcyxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICAgICAgY2hhcl9lbmQ6IGNodW5rLmNoYXJFbmQsXG4gICAgICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgICAgIH1cbiAgICAgICAgfTtcbiAgICAgIH0pO1xuXG4gICAgICBjb25zdCB0b3RhbENodW5rcyA9IGNodW5rcy5sZW5ndGg7XG4gICAgICBjb25zdCB0b3RhbEJhdGNoZXMgPSBNYXRoLmNlaWwodG90YWxDaHVua3MgLyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IHRvdGFsU2V0cyA9IE1hdGguY2VpbCh0b3RhbEJhdGNoZXMgLyBQQVJBTExFTF9DQUxMUyk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAke3RvdGFsQ2h1bmtzfSBjaHVua3MgXHUyMTkyICR7dG90YWxCYXRjaGVzfSBBUEkgY2FsbHMgXHUyMTkyICR7dG90YWxTZXRzfSBzZXRzIG9mICR7UEFSQUxMRUxfQ0FMTFN9IHBhcmFsbGVsYCk7XG5cbiAgICAgIHNzZUV2ZW50KHJlcywgJ3VwbG9hZF9jb21wbGV0ZScsIHtcbiAgICAgICAgZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgdG90YWxDaHVua3MsIHRvdGFsQmF0Y2hlcywgdG90YWxTZXRzXG4gICAgICB9KTtcblxuICAgICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiAwLCBzdGF0dXM6ICdpbmRleGluZydcbiAgICAgIH0pO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMSBkb25lIFx1MjAxNCAke2NsZWFuRmlsZW5hbWV9IGFkZGVkIHRvIHNlc3Npb24gYXMgaW5kZXhpbmdgKTtcblxuICAgICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRDb2xsZWN0aW9uKCk7XG4gICAgICBsZXQgcHJvY2Vzc2VkQ2h1bmtzID0gMDtcbiAgICAgIGNvbnN0IGFsbEVtYmVkZGluZ3MgPSBbXTtcblxuICAgICAgY29uc3QgYmF0Y2hlcyA9IFtdO1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IEJBVENIX1NJWkUpIGJhdGNoZXMucHVzaChjaHVua3Muc2xpY2UoaSwgaSArIEJBVENIX1NJWkUpKTtcblxuICAgICAgY29uc3Qgc2V0cyA9IFtdO1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBQQVJBTExFTF9DQUxMUykgc2V0cy5wdXNoKGJhdGNoZXMuc2xpY2UoaSwgaSArIFBBUkFMTEVMX0NBTExTKSk7XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAyIHN0YXJ0IFx1MjAxNCAke3NldHMubGVuZ3RofSBzZXRzYCk7XG5cbiAgICAgIGZvciAobGV0IHNldElkeCA9IDA7IHNldElkeCA8IHNldHMubGVuZ3RoOyBzZXRJZHgrKykge1xuICAgICAgICBjb25zdCBpc0xhc3RTZXQgPSBzZXRJZHggPT09IHNldHMubGVuZ3RoIC0gMTtcbiAgICAgICAgY29uc3QgY3VycmVudFNldCA9IHNldHNbc2V0SWR4XTtcbiAgICAgICAgY29uc3Qgc2V0Q2h1bmtDb3VudCA9IGN1cnJlbnRTZXQucmVkdWNlKChhY2MsIGIpID0+IGFjYyArIGIubGVuZ3RoLCAwKTtcblxuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0vJHtzZXRzLmxlbmd0aH0gXHUyMDE0IGVtYmVkZGluZyAke2N1cnJlbnRTZXQubGVuZ3RofSBiYXRjaCBjYWxsKHMpICgke3NldENodW5rQ291bnR9IGNodW5rcykgaW4gcGFyYWxsZWxgKTtcblxuICAgICAgICBjb25zdCBlbWJlZFJlc3VsdHMgPSBhd2FpdCBQcm9taXNlLmFsbFNldHRsZWQoXG4gICAgICAgICAgY3VycmVudFNldC5tYXAoYmF0Y2ggPT4gZW1iZWRTaW5nbGVCYXRjaEdyb3VwKGJhdGNoLm1hcChjID0+IGMudGV4dCkpKVxuICAgICAgICApO1xuXG4gICAgICAgIGNvbnN0IHNldEVtYmVkZGluZ3MgPSBbXTtcbiAgICAgICAgZW1iZWRSZXN1bHRzLmZvckVhY2goKHJlc3VsdCwgYmF0Y2hJZHgpID0+IHtcbiAgICAgICAgICBjb25zdCBiYXRjaCA9IGN1cnJlbnRTZXRbYmF0Y2hJZHhdO1xuICAgICAgICAgIGlmIChyZXN1bHQuc3RhdHVzID09PSAnZnVsZmlsbGVkJykge1xuICAgICAgICAgICAgcmVzdWx0LnZhbHVlLmZvckVhY2goKHZlY3RvciwgY2h1bmtJZHgpID0+IHtcbiAgICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5wdXNoKHtcbiAgICAgICAgICAgICAgICBpZDogYmF0Y2hbY2h1bmtJZHhdLm1ldGFkYXRhLmNodW5rX2lkLFxuICAgICAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yLFxuICAgICAgICAgICAgICAgIG1ldGFkYXRhOiBiYXRjaFtjaHVua0lkeF0ubWV0YWRhdGEsXG4gICAgICAgICAgICAgICAgdGV4dDogYmF0Y2hbY2h1bmtJZHhdLnRleHRcbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IGVtYmVkZGVkIE9LICgke2JhdGNoLmxlbmd0aH0gY2h1bmtzKWApO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSAgIEJhdGNoICR7c2V0SWR4ICogUEFSQUxMRUxfQ0FMTFMgKyBiYXRjaElkeCArIDF9IEZBSUxFRDpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuXG4gICAgICAgIHByb2Nlc3NlZENodW5rcyArPSBzZXRFbWJlZGRpbmdzLmxlbmd0aDtcbiAgICAgICAgYWxsRW1iZWRkaW5ncy5wdXNoKC4uLnNldEVtYmVkZGluZ3MpO1xuXG4gICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTZXQgJHtzZXRJZHggKyAxfSBlbWJlZGRlZCBcdTIwMTQgJHtwcm9jZXNzZWRDaHVua3N9LyR7dG90YWxDaHVua3N9IGNodW5rcyBzbyBmYXJgKTtcblxuICAgICAgICBpZiAoIWlzTGFzdFNldCkge1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBTdGFydGluZyAke0dST1VQX1dBSVRfTVMgLyAxMDAwfXMgdGltZXIgKyBDaHJvbWEgd3JpdGUgY29uY3VycmVudGx5IGZvciBzZXQgJHtzZXRJZHggKyAxfWApO1xuICAgICAgICAgIGNvbnN0IHRpbWVyID0gbmV3IFByb21pc2UociA9PiBzZXRUaW1lb3V0KHIsIEdST1VQX1dBSVRfTVMpKTtcbiAgICAgICAgICBjb25zdCBjaHJvbWFXcml0ZSA9IGFkZFZlY3RvcnMoXG4gICAgICAgICAgICBjb2xsZWN0aW9uLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiAoeyB0ZXh0OiBlLnRleHQsIG1ldGFkYXRhOiBlLm1ldGFkYXRhIH0pKSxcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5lbWJlZGRpbmcpLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmlkKVxuICAgICAgICAgICkudGhlbigoKSA9PiBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApKVxuICAgICAgICAgICAgLmNhdGNoKGVyciA9PiBjb25zb2xlLmVycm9yKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgRkFJTEVEIGZvciBzZXQgJHtzZXRJZHggKyAxfTpgLCBlcnIubWVzc2FnZSkpO1xuXG4gICAgICAgICAgc3NlRXZlbnQocmVzLCAnZW1iZWRkaW5nX3Byb2dyZXNzJywge1xuICAgICAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzLCB0b3RhbENodW5rcyxcbiAgICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgICB3YWl0aW5nTXM6IEdST1VQX1dBSVRfTVMsIGNocm9tYVdyaXRlQ29tcGxldGU6IGZhbHNlXG4gICAgICAgICAgfSk7XG5cbiAgICAgICAgICBhd2FpdCBQcm9taXNlLmFsbChbdGltZXIsIGNocm9tYVdyaXRlXSk7XG4gICAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFRpbWVyICsgQ2hyb21hIGJvdGggZG9uZSBmb3Igc2V0ICR7c2V0SWR4ICsgMX0sIHByb2NlZWRpbmcgdG8gc2V0ICR7c2V0SWR4ICsgMn1gKTtcblxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBMYXN0IHNldCAke3NldElkeCArIDF9IFx1MjAxNCBhd2FpdGluZyBDaHJvbWEgd3JpdGUgZGlyZWN0bHlgKTtcbiAgICAgICAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgICAgICAgY29sbGVjdGlvbixcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICAgICAgICApO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBDaHJvbWEgd3JpdGUgY29tcGxldGUgZm9yIGxhc3Qgc2V0ICgke3NldEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzKWApO1xuXG4gICAgICAgICAgc3NlRXZlbnQocmVzLCAnZW1iZWRkaW5nX3Byb2dyZXNzJywge1xuICAgICAgICAgICAgcHJvY2Vzc2VkQ2h1bmtzLCB0b3RhbENodW5rcyxcbiAgICAgICAgICAgIHNldEluZGV4OiBzZXRJZHggKyAxLCB0b3RhbFNldHMsXG4gICAgICAgICAgICB3YWl0aW5nTXM6IDAsIGNocm9tYVdyaXRlQ29tcGxldGU6IHRydWVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLCBzdGF0dXM6ICdyZWFkeSdcbiAgICAgIH0pO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gXHUyNzA1IERvbmUgXHUyMDE0ICR7YWxsRW1iZWRkaW5ncy5sZW5ndGh9IHZlY3RvcnMgaW4gQ2hyb21hIGZvciAke2NsZWFuRmlsZW5hbWV9YCk7XG5cbiAgICAgIHNzZUV2ZW50KHJlcywgJ2RvbmUnLCB7XG4gICAgICAgIGRvY3VtZW50OiB7XG4gICAgICAgICAgaWQ6IGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogYWxsRW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgICAgfSxcbiAgICAgICAgc2Vzc2lvbklkXG4gICAgICB9KTtcblxuICAgICAgcmVzLmVuZCgpO1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGlmIChyZXEuZmlsZSAmJiBmcy5leGlzdHNTeW5jKHJlcS5maWxlLnBhdGgpKSB7XG4gICAgICAgIHRyeSB7IGZzLnVubGlua1N5bmMocmVxLmZpbGUucGF0aCk7IH0gY2F0Y2ggeyB9XG4gICAgICB9XG4gICAgICBjb25zb2xlLmVycm9yKCdbdXBsb2FkXSBVbmhhbmRsZWQgZXJyb3I6JywgZXJyb3IpO1xuICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ1VwbG9hZCBmYWlsZWQnLCBjb2RlOiBlcnJvci5jb2RlIHx8ICdVUExPQURfRVJST1InIH0pO1xuICAgICAgcmVzLmVuZCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBTU0U6IFNlZWRpbmcgc3RhdHVzIHN0cmVhbSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHNlZWRpbmdTdGF0dXNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gICAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICAgIHJlcy5mbHVzaEhlYWRlcnMoKTtcblxuICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gICAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTWlzc2luZyBzZXNzaW9uIElEJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gICAgICByZXMuZW5kKCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gQ2xpZW50IGNvbm5lY3RlZCBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcblxuICAgIC8vIENoZWNrIGlmIHNlc3Npb24gaXMgYWxyZWFkeSBzZWVkZWRcbiAgICBjb25zdCBzZWVkZWQgPSBpc1Nlc3Npb25TZWVkZWQoc2Vzc2lvbklkKTtcbiAgICBpZiAoc2VlZGVkKSB7XG4gICAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBTZXNzaW9uICR7c2Vzc2lvbklkfSBhbHJlYWR5IHNlZWRlZCBcdTIwMTMgcmV0dXJuaW5nIGltbWVkaWF0ZWx5YCk7XG4gICAgICBzc2VFdmVudChyZXMsICdzZWVkaW5nX2NvbXBsZXRlJywgeyBzZXNzaW9uSWQsIHNlZWRlZDogdHJ1ZSB9KTtcbiAgICAgIHJlcy5lbmQoKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBDcmVhdGUgYSBsaXN0ZW5lciBmb3IgdGhpcyBzZXNzaW9uXG4gICAgY29uc3QgZXZlbnRLZXkgPSBgc2VlZGluZzoke3Nlc3Npb25JZH1gO1xuXG4gICAgLy8gU3RvcmUgdGhlIGxpc3RlbmVyIHNvIHdlIGNhbiBlbWl0IHdoZW4gc2VlZGluZyBjb21wbGV0ZXNcbiAgICBpZiAoIWdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzKSB7XG4gICAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycyA9IG5ldyBNYXAoKTtcbiAgICB9XG4gICAgaWYgKCFnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5oYXMoZXZlbnRLZXkpKSB7XG4gICAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5zZXQoZXZlbnRLZXksIFtdKTtcbiAgICB9XG4gICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KS5wdXNoKHJlcyk7XG5cbiAgICAvLyBDbGVhbiB1cCBsaXN0ZW5lciBvbiBjbGllbnQgZGlzY29ubmVjdFxuICAgIHJlcS5vbignY2xvc2UnLCAoKSA9PiB7XG4gICAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgICAgY29uc3QgaWR4ID0gbGlzdGVuZXJzLmluZGV4T2YocmVzKTtcbiAgICAgIGlmIChpZHggPj0gMCkge1xuICAgICAgICBsaXN0ZW5lcnMuc3BsaWNlKGlkeCwgMSk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIENsaWVudCBkaXNjb25uZWN0ZWQgZm9yICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgICAgaWYgKGxpc3RlbmVycy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIC8vIFN0YXJ0IHNlZWRpbmcgaW4gdGhlIGJhY2tncm91bmQgKGlmIG5vdCBhbHJlYWR5IHJ1bm5pbmcpXG4gICAgdHJ5IHtcbiAgICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIFRyaWdnZXJpbmcgc2VlZGluZyBmb3IgJHtzZXNzaW9uSWR9Li4uYCk7XG4gICAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgICAvLyBUaGUgc2VlZGluZyBmdW5jdGlvbiB3aWxsIG5vdGlmeSBsaXN0ZW5lcnMgd2hlbiBjb21wbGV0ZVxuICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgY29uc29sZS5lcnJvcihgW3NlZWRpbmctc3RhdHVzXSBTZWVkaW5nIGZhaWxlZCBmb3IgJHtzZXNzaW9uSWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgICBsaXN0ZW5lcnMuZm9yRWFjaCgocmVzcG9uc2UpID0+IHtcbiAgICAgICAgc3NlRXZlbnQocmVzcG9uc2UsICdlcnJvcicsIHsgbWVzc2FnZTogZXJyLm1lc3NhZ2UsIGNvZGU6ICdTRUVEX0ZBSUxFRCcgfSk7XG4gICAgICAgIHJlc3BvbnNlLmVuZCgpO1xuICAgICAgfSk7XG4gICAgICBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5kZWxldGUoZXZlbnRLZXkpO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBMaXN0IGRvY3VtZW50cyBoYW5kbGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBleHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdERvY3VtZW50c0hhbmRsZXIocmVxLCByZXMpIHtcbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcbiAgICB0cnkge1xuICAgICAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgICBjb25zdCBkb2N1bWVudHMgPSBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0xpc3QgZG9jdW1lbnRzIGVycm9yOicsIGVycm9yKTtcbiAgICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIERlbGV0ZSBkb2N1bWVudCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gICAgY29uc3QgeyBkb2N1bWVudElkIH0gPSByZXEucGFyYW1zO1xuICAgIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gICAgdHJ5IHtcbiAgICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBjb25zdCB7IGNvbGxlY3Rpb24gfSA9IGF3YWl0IGdldENvbGxlY3Rpb24oKTtcbiAgICAgICAgICBpZiAoY29sbGVjdGlvbikge1xuICAgICAgICAgICAgYXdhaXQgZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSBjYXRjaCAoY2hyb21hRXJyKSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGBbZGVsZXRlXSBDaHJvbWEgZGVsZXRlIGZhaWxlZCBmb3IgJHtkb2N1bWVudElkfTpgLCBjaHJvbWFFcnIubWVzc2FnZSk7XG4gICAgICAgIH1cblxuICAgICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG5cbiAgICAgICAgY2xlYXJNZW1vcnkoc2Vzc2lvbklkKTtcbiAgICAgICAgY29uc29sZS5sb2coYFtkZWxldGVdIENsZWFyZWQgbWVtb3J5IGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuXG4gICAgICBpZiAoZmlsZW5hbWUpIHtcbiAgICAgICAgY29uc3QgZmlsZVBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICAgIGlmIChmcy5leGlzdHNTeW5jKGZpbGVQYXRoKSkge1xuICAgICAgICAgIGZzLnVubGlua1N5bmMoZmlsZVBhdGgpO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZGVsZXRlXSBSZW1vdmVkIGZpbGU6ICR7ZmlsZVBhdGh9YCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc29sZS53YXJuKGBbZGVsZXRlXSBGaWxlIG5vdCBmb3VuZCBvbiBkaXNrOiAke2ZpbGVQYXRofWApO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSwgZG9jdW1lbnRJZCB9KTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRGVsZXRlIGRvY3VtZW50IGVycm9yOicsIGVycm9yKTtcbiAgICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50JywgY29kZTogJ0RFTEVURV9FUlJPUicgfSk7XG4gICAgfVxuICB9XG5cbiAgLy8gXHUyNTAwXHUyNTAwXHUyNTAwIEdldCBkb2N1bWVudCBmaWxlIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBleHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRGaWxlKHJlcSwgcmVzKSB7XG4gICAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgICB0cnkge1xuICAgICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICAgIGNvbnN0IHVwbG9hZFBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBmaWxlbmFtZSk7XG4gICAgICAgIGlmIChmcy5leGlzdHNTeW5jKHVwbG9hZFBhdGgpKSB7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oZmlsZW5hbWUpKTtcbiAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbSh1cGxvYWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWRQYXRoKSkge1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0oc2VlZFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgfVxuXG4gICAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgICAgY29uc3QgYWxsUGRmcyA9IGZzLnJlYWRkaXJTeW5jKHNlZWREaXIpLmZpbHRlcihmID0+IGYuZW5kc1dpdGgoJy5wZGYnKSk7XG4gICAgICAgICAgY29uc3QgbWF0Y2ggPSBhbGxQZGZzLmZpbmQoZiA9PiBmLmluY2x1ZGVzKHBhdGgucGFyc2UoZmlsZW5hbWUpLm5hbWUpKTtcbiAgICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICAgIGNvbnN0IG1hdGNoUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBtYXRjaCk7XG4gICAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKG1hdGNoKSk7XG4gICAgICAgICAgICByZXR1cm4gZnMuY3JlYXRlUmVhZFN0cmVhbShtYXRjaFBhdGgpLnBpcGUocmVzKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdEb2N1bWVudCBmaWxlIG5vdCBmb3VuZCcsIGNvZGU6ICdGSUxFX05PVF9GT1VORCcgfSk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0dldCBkb2N1bWVudCBmaWxlIGVycm9yOicsIGVycm9yKTtcbiAgICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gcmV0cmlldmUgZG9jdW1lbnQnLCBjb2RlOiAnUkVUUklFVkVfRVJST1InIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBSb3V0ZXMgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIHJvdXRlci5wb3N0KCcvdXBsb2FkJywgdXBsb2FkLnNpbmdsZSgnZmlsZScpLCBoYW5kbGVVcGxvYWQpO1xuICByb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xuICByb3V0ZXIuZ2V0KCcvc2VlZGluZy1zdGF0dXMnLCBzZWVkaW5nU3RhdHVzSGFuZGxlcik7XG4gIHJvdXRlci5kZWxldGUoJy86ZG9jdW1lbnRJZCcsIGRlbGV0ZURvY3VtZW50KTtcbiAgcm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG4gIGV4cG9ydCBkZWZhdWx0IHJvdXRlcjsiLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtpbXBvcnQgeyBnZXRDb2xsZWN0aW9uLCBoeWJyaWRRdWVyeUNvbGxlY3Rpb24gfSBmcm9tICcuL2Nocm9tYVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZW1iZWRRdWVyeSB9IGZyb20gJy4vZW1iZWRkaW5nU2VydmljZS5qcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3QgVE9QX0sgPSBwYXJzZUludChwcm9jZXNzLmVudi5UT1BfSykgfHwgMjA7XG5jb25zdCBSRUZVU0FMX1RIUkVTSE9MRCA9IHBhcnNlRmxvYXQocHJvY2Vzcy5lbnYuUkVGVVNBTF9USFJFU0hPTEQpIHx8IDAuMDU7XG5cbmZ1bmN0aW9uIGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEsgPSA1KSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAgfTtcbiAgY29uc3Qgc2NvcmVzID0gcmVzdWx0cy5zbGljZSgwLCB0b3BLKS5tYXAociA9PiBNYXRoLm1heCgwLCByLnNjb3JlKSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcbiAgcmV0dXJuIHtcbiAgICBjb25maWRlbmNlOiBNYXRoLnJvdW5kKGF2Z1Njb3JlICogMTAwKSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKVxuICB9O1xufVxuXG4vLyBcdTI1MDBcdTI1MDAgTWFpbiByZXRyaWV2YWwgZnVuY3Rpb24gKEh5YnJpZDogZGVuc2UgKyBCTTI1IHZpYSBDaHJvbWEgUlJGKSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiByZXRyaWV2ZUZvclF1ZXJ5KHF1ZXJ5LCBzZXNzaW9uSWQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0b3BLID0gb3B0aW9ucy50b3BLIHx8IDU7XG5cbiAgdHJ5IHtcbiAgICAvLyBcdTI1MDBcdTI1MDAgVGltaW5nOiBlbWJlZGRpbmcgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gICAgY29uc3QgdEVtYmVkU3RhcnQgPSBwZXJmb3JtYW5jZS5ub3coKTtcbiAgICBsZXQgdEVtYmVkRW5kO1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgeyBjb2xsZWN0aW9uIH1dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSkudGhlbihyZXN1bHQgPT4geyB0RW1iZWRFbmQgPSBwZXJmb3JtYW5jZS5ub3coKTsgcmV0dXJuIHJlc3VsdDsgfSksXG4gICAgICBnZXRDb2xsZWN0aW9uKClcbiAgICBdKTtcbiAgICBjb25zdCBlbWJlZGRpbmdNcyA9IHRFbWJlZEVuZCAtIHRFbWJlZFN0YXJ0O1xuXG4gICAgaWYgKCFjb2xsZWN0aW9uKSB7XG4gICAgICBjb25zb2xlLndhcm4oYFx1MjZBMFx1RkUwRiAgTm8gY29sbGVjdGlvbiBhdmFpbGFibGVgKTtcbiAgICAgIHJldHVybiB7IHJlc3VsdHM6IFtdLCBjb3ZlcmFnZTogeyBjb25maWRlbmNlOiAwLCB0b3BTY29yZTogMCwgbGV2ZWw6ICdsb3cnLCBzY29yZTogMCB9LCBxdWVyeUVtYmVkZGluZywgdGltaW5nczogeyBlbWJlZGRpbmdNcywgcmV0cmlldmFsTXM6IDAgfSB9O1xuICAgIH1cblxuICAgIC8vIEJ1aWxkIG1ldGFkYXRhIGZpbHRlcjogaW5jbHVkZSBib3RoICdnbG9iYWwnIHZlY3RvcnMgYW5kIHRoaXMgc2Vzc2lvbidzIHZlY3RvcnNcbiAgICBjb25zdCB3aGVyZSA9IHNlc3Npb25JZFxuICAgICAgPyB7IHNlc3Npb25faWQ6IHsgXCIkaW5cIjogW1wiZ2xvYmFsXCIsIHNlc3Npb25JZF0gfSB9XG4gICAgICA6IHsgc2Vzc2lvbl9pZDogXCJnbG9iYWxcIiB9O1xuXG4gICAgLy8gXHUyNTAwXHUyNTAwIFRpbWluZzogcmV0cmlldmFsIChDaHJvbWEgc2VhcmNoKSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgICBjb25zdCB0UmV0cmlldmFsU3RhcnQgPSBwZXJmb3JtYW5jZS5ub3coKTtcbiAgICBjb25zdCByYXdSZXN1bHRzID0gYXdhaXQgaHlicmlkUXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5LCBxdWVyeUVtYmVkZGluZywgdG9wSywgd2hlcmUpO1xuICAgIGNvbnN0IHJldHJpZXZhbE1zID0gcGVyZm9ybWFuY2Uubm93KCkgLSB0UmV0cmlldmFsU3RhcnQ7XG5cbiAgICBjb25zdCByZXN1bHRzID0gcmF3UmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgLi4ucixcbiAgICAgIHNvdXJjZV90eXBlOiByLm1ldGFkYXRhPy5zb3VyY2VfdHlwZSB8fCAnc2Vzc2lvbidcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb3ZlcmFnZSA9IGNhbGN1bGF0ZUNvdmVyYWdlKHJlc3VsdHMsIHRvcEspO1xuICAgIGNvbnN0IHRvcFNjb3JlID0gY292ZXJhZ2UudG9wU2NvcmU7XG4gICAgY29uc3QgbGV2ZWwgPSB0b3BTY29yZSA+PSAwLjYgPyAnaGlnaCcgOiB0b3BTY29yZSA+PSAwLjMgPyAnbWVkaXVtJyA6ICdsb3cnO1xuXG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1REQwRCBRdWVyeTonLCBxdWVyeSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDQSBDb3ZlcmFnZTonLCB7IC4uLmNvdmVyYWdlLCBsZXZlbCB9KTtcbiAgICBjb25zb2xlLmxvZygnXHVEODNEXHVEQ0M4IFNjb3JlczonLCByZXN1bHRzLm1hcChyID0+IHIuc2NvcmUudG9GaXhlZCg0KSkpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHJlc3VsdHMsXG4gICAgICBjb3ZlcmFnZTogeyAuLi5jb3ZlcmFnZSwgbGV2ZWwsIHNjb3JlOiB0b3BTY29yZSB9LFxuICAgICAgcXVlcnlFbWJlZGRpbmcsXG4gICAgICB0aW1pbmdzOiB7IGVtYmVkZGluZ01zLCByZXRyaWV2YWxNcyB9XG4gICAgfTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1JldHJpZXZhbCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cywgbWF4VG9rZW5zID0gNzAwMCkge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiAnJztcblxuICBsZXQgdG90YWxUb2tlbnMgPSAwO1xuICBjb25zdCBjb250ZXh0UGFydHMgPSBbXTtcblxuICBmb3IgKGxldCBpID0gMDsgaSA8IHJlc3VsdHMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCByZXN1bHQgPSByZXN1bHRzW2ldO1xuICAgIGNvbnN0IHRva2VuRXN0aW1hdGUgPSByZXN1bHQudGV4dC5sZW5ndGggLyA0O1xuICAgIGlmICh0b3RhbFRva2VucyArIHRva2VuRXN0aW1hdGUgPiBtYXhUb2tlbnMpIGJyZWFrO1xuICAgIHRvdGFsVG9rZW5zICs9IHRva2VuRXN0aW1hdGU7XG4gICAgY29uc3Qgc291cmNlTGFiZWwgPSByZXN1bHQuc291cmNlX3R5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcgPyAnW1Nlc3Npb24gVXBsb2FkXScgOiAnW1NlZWQgRG9jdW1lbnRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LFxuICAgIHNjb3JlOiByZXN1bHQuc2NvcmUsXG4gICAgc291cmNlVHlwZTogcmVzdWx0LnNvdXJjZV90eXBlLFxuICAgIGNodW5rSWQ6IHJlc3VsdC5pZFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzaG91bGRTaG93UmVmdXNhbChjb3ZlcmFnZSkge1xuICByZXR1cm4gY292ZXJhZ2UudG9wU2NvcmUgPCBSRUZVU0FMX1RIUkVTSE9MRDtcbn1cblxuZXhwb3J0IHsgY2FsY3VsYXRlQ292ZXJhZ2UgfTtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBMTE1VbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxubGV0IGdlbkFJID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0R2VuQUkoKSB7XG4gIGlmICghZ2VuQUkpIHtcbiAgICBjb25zdCBwcm9qZWN0ID0gcHJvY2Vzcy5lbnYuR09PR0xFX0NMT1VEX1BST0pFQ1QgfHwgJ3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMCc7XG4gICAgY29uc3QgbG9jYXRpb24gPSAnZ2xvYmFsJztcblxuICAgIC8vIFN1cHBvcnQgY3JlZGVudGlhbHMgZnJvbSBlbnYgdmFyIChmb3Igc2VydmVybGVzcykgb3IgZmlsZSAoZm9yIGxvY2FsIGRldilcbiAgICBjb25zdCBjcmVkZW50aWFsc0pzb24gPSBwcm9jZXNzLmVudi5HT09HTEVfQ1JFREVOVElBTFNfSlNPTjtcblxuICAgIGlmIChjcmVkZW50aWFsc0pzb24pIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGNyZWRlbnRpYWxzID0gSlNPTi5wYXJzZShjcmVkZW50aWFsc0pzb24pO1xuICAgICAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gICAgICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICAgICAgcHJvamVjdCxcbiAgICAgICAgICBsb2NhdGlvbixcbiAgICAgICAgICBjcmVkZW50aWFsc1xuICAgICAgICB9KTtcbiAgICAgICAgcmV0dXJuIGdlbkFJO1xuICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICBjb25zb2xlLndhcm4oJ0ZhaWxlZCB0byBwYXJzZSBHT09HTEVfQ1JFREVOVElBTFNfSlNPTiwgZmFsbGluZyBiYWNrIHRvIGRlZmF1bHQgYXV0aCcpO1xuICAgICAgfVxuICAgIH1cblxuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbkFJKHtcbiAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgcHJvamVjdCxcbiAgICAgIGxvY2F0aW9uXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIGdlbkFJO1xufVxuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5jb25zdCBGQUxMQkFDS19NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9GQUxMQkFDSyB8fCAnZ2VtaW5pLTIuNS1mbGFzaCc7XG5jb25zdCBGSVJTVF9UT0tFTl9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX0ZJUlNUX1RPS0VOX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDEyMDAwO1xuY29uc3QgUkVRVUVTVF9USU1FT1VUID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTExNX1JFUVVFU1RfVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgNDUwMDA7XG5cbmZ1bmN0aW9uIGdldFByaW1hcnlNb2RlbE5hbWUoKSB7XG4gIHJldHVybiBQUklNQVJZX01PREVMO1xufVxuXG5mdW5jdGlvbiBnZXRGYWxsYmFja01vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIEZBTExCQUNLX01PREVMO1xufVxuXG5mdW5jdGlvbiBnZXRUZXh0RnJvbUNodW5rKGNodW5rKSB7XG4gIGlmICh0eXBlb2YgY2h1bms/LnRleHQgPT09ICdzdHJpbmcnKSByZXR1cm4gY2h1bmsudGV4dDtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ2Z1bmN0aW9uJykgcmV0dXJuIGNodW5rLnRleHQoKTtcbiAgcmV0dXJuICcnO1xufVxuXG5mdW5jdGlvbiBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsLCBwcm9tcHQpIHtcbiAgcmV0dXJuIHtcbiAgICBtb2RlbCxcbiAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgIGNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9XG4gIH07XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbE5hbWUgPSBnZXRQcmltYXJ5TW9kZWxOYW1lKCk7XG4gIGxldCByZXRyaWVzID0gMDtcbiAgY29uc3QgbWF4UmV0cmllcyA9IDI7XG5cbiAgd2hpbGUgKHJldHJpZXMgPCBtYXhSZXRyaWVzKSB7XG4gICAgbGV0IGZpcnN0VG9rZW5UaW1lb3V0ID0gbnVsbDtcbiAgICBsZXQgcmVxdWVzdFRpbWVvdXRJZCA9IG51bGw7XG4gICAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcblxuICAgIHRyeSB7XG4gICAgICByZXF1ZXN0VGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgICAgIGNvbnN0IHJlc3BvbnNlU3RyZWFtID0gYXdhaXQgZ2V0R2VuQUkoKS5tb2RlbHMuZ2VuZXJhdGVDb250ZW50U3RyZWFtKFxuICAgICAgICBidWlsZEdlbmVyYXRpb25SZXF1ZXN0KG1vZGVsTmFtZSwgcHJvbXB0KSxcbiAgICAgICAgeyBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsIH1cbiAgICAgICk7XG5cbiAgICAgIGlmICghcmVzcG9uc2VTdHJlYW0gfHwgdHlwZW9mIHJlc3BvbnNlU3RyZWFtW1N5bWJvbC5hc3luY0l0ZXJhdG9yXSAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYFN0cmVhbWluZyB1bmF2YWlsYWJsZSBmb3IgbW9kZWwgJHttb2RlbE5hbWV9YCk7XG4gICAgICB9XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3BvbnNlU3RyZWFtKSB7XG4gICAgICAgIGlmIChjb250cm9sbGVyLnNpZ25hbC5hYm9ydGVkKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdTdHJlYW0gZXhlY3V0aW9uIGFib3J0ZWQgYnkgdGltZW91dCBjb25zdHJhaW50LicpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGdldFRleHRGcm9tQ2h1bmsoY2h1bmspO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuICAgICAgcmV0dXJuO1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcblxuICAgICAgaWYgKGZpcnN0VG9rZW5UaW1lb3V0KSBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgaWYgKHJlcXVlc3RUaW1lb3V0SWQpIGNsZWFyVGltZW91dChyZXF1ZXN0VGltZW91dElkKTtcblxuICAgICAgY29uc29sZS5lcnJvcihgTW9kZWwgYXR0ZW1wdCAke3JldHJpZXN9IGZhaWxlZDpgLCBlcnJvci5tZXNzYWdlKTtcblxuICAgICAgaWYgKHJldHJpZXMgPj0gbWF4UmV0cmllcykge1xuICAgICAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgICB9XG5cbiAgICAgIG1vZGVsTmFtZSA9IGdldEZhbGxiYWNrTW9kZWxOYW1lKCk7XG4gICAgfVxuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc3VwYWJhc2VTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgY3JlYXRlQ2xpZW50IH0gZnJvbSAnQHN1cGFiYXNlL3N1cGFiYXNlLWpzJztcblxuY29uc3Qgc3VwYWJhc2VVcmwgPSBwcm9jZXNzLmVudi5WSVRFX1NVUEFCQVNFX1VSTCB8fCBwcm9jZXNzLmVudi5TVVBBQkFTRV9VUkw7XG5jb25zdCBzdXBhYmFzZUtleSA9IHByb2Nlc3MuZW52LlZJVEVfU1VQQUJBU0VfQU5PTl9LRVkgfHwgcHJvY2Vzcy5lbnYuU1VQQUJBU0VfQU5PTl9LRVk7XG5cbmlmICghc3VwYWJhc2VVcmwgfHwgIXN1cGFiYXNlS2V5KSB7XG4gIGNvbnNvbGUud2FybignU3VwYWJhc2UgVVJMIG9yIEtleSBpcyBtaXNzaW5nLiBEYXRhYmFzZSBvcGVyYXRpb25zIHdpbGwgbm90IHdvcmsgcHJvcGVybHkuJyk7XG59XG5cbmV4cG9ydCBjb25zdCBzdXBhYmFzZSA9IGNyZWF0ZUNsaWVudChcbiAgc3VwYWJhc2VVcmwgfHwgJ2h0dHA6Ly9sb2NhbGhvc3QnLFxuICBzdXBhYmFzZUtleSB8fCAncHVibGljLWFub24ta2V5J1xuKTtcblxuLy8gTWFwIHRvIHRyYWNrIHRoZSBsYXN0IGluc2VydGlvbiBwcm9taXNlIHBlciBzZXNzaW9uXG5jb25zdCBzZXNzaW9uSW5zZXJ0UHJvbWlzZXMgPSBuZXcgTWFwKCk7XG5cbi8qKlxuICogUmVjdXJzaXZlbHkgcmVtb3ZlcyBudWxsIGJ5dGVzIChcXHUwMDAwKSBmcm9tIHN0cmluZ3MsIGFycmF5cywgb3Igb2JqZWN0cy5cbiAqIFBvc3RncmVTUUwgKFN1cGFiYXNlKSBkb2VzIG5vdCBzdXBwb3J0IFxcdTAwMDAgaW4gdGV4dC9qc29uYiBmaWVsZHMuXG4gKi9cbmZ1bmN0aW9uIHNhbml0aXplTnVsbEJ5dGVzKHZhbCkge1xuICBpZiAodHlwZW9mIHZhbCA9PT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gdmFsLnJlcGxhY2UoL1xcdTAwMDAvZywgJycpO1xuICB9XG4gIGlmIChBcnJheS5pc0FycmF5KHZhbCkpIHtcbiAgICByZXR1cm4gdmFsLm1hcChzYW5pdGl6ZU51bGxCeXRlcyk7XG4gIH1cbiAgaWYgKHZhbCAhPT0gbnVsbCAmJiB0eXBlb2YgdmFsID09PSAnb2JqZWN0Jykge1xuICAgIGNvbnN0IGNsZWFuT2JqID0ge307XG4gICAgZm9yIChjb25zdCBrZXkgb2YgT2JqZWN0LmtleXModmFsKSkge1xuICAgICAgY2xlYW5PYmpba2V5XSA9IHNhbml0aXplTnVsbEJ5dGVzKHZhbFtrZXldKTtcbiAgICB9XG4gICAgcmV0dXJuIGNsZWFuT2JqO1xuICB9XG4gIHJldHVybiB2YWw7XG59XG5cbi8qKlxuICogQXN5bmNocm9ub3VzbHkgaW5zZXJ0cyBjb252ZXJzYXRpb24gZGF0YSBpbnRvIFN1cGFiYXNlLlxuICogQ2hhaW5zIGluc2VydGlvbnMgZm9yIHRoZSBzYW1lIHNlc3Npb24gdG8gZW5zdXJlIHRoZXkgY29tcGxldGUgaW4gb3JkZXIuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpbnNlcnRDb252ZXJzYXRpb25Bc3luYyhzZXNzaW9uSWQsIGRhdGEpIHtcbiAgY29uc3QgcHJldmlvdXNQcm9taXNlID0gc2Vzc2lvbkluc2VydFByb21pc2VzLmdldChzZXNzaW9uSWQpIHx8IFByb21pc2UucmVzb2x2ZSgpO1xuXG4gIGNvbnN0IG5leHRQcm9taXNlID0gcHJldmlvdXNQcm9taXNlXG4gICAgLnRoZW4oYXN5bmMgKCkgPT4ge1xuICAgICAgY29uc3QgY2xlYW5EYXRhID0gc2FuaXRpemVOdWxsQnl0ZXMoZGF0YSk7XG4gICAgICBjb25zb2xlLmxvZyhgW1N1cGFiYXNlXSBJbnNlcnRpbmcgY29udmVyc2F0aW9uIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfSwgYW5zd2VyX2tleTogJHtjbGVhbkRhdGEuYW5zd2VyX2tleX1gKTtcbiAgICAgIGNvbnN0IHsgZXJyb3IgfSA9IGF3YWl0IHN1cGFiYXNlLmZyb20oJ0NvbnZlcnNhdGlvbl9IaXN0b3J5JykuaW5zZXJ0KGNsZWFuRGF0YSk7XG4gICAgICBpZiAoZXJyb3IpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcignW1N1cGFiYXNlXSBFcnJvciBpbnNlcnRpbmcgY29udmVyc2F0aW9uIGhpc3Rvcnk6JywgZXJyb3IpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS5sb2coYFtTdXBhYmFzZV0gU3VjY2Vzc2Z1bGx5IGluc2VydGVkIGNvbnZlcnNhdGlvbiBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cbiAgICB9KVxuICAgIC5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICBjb25zb2xlLmVycm9yKCdbU3VwYWJhc2VdIFVuZXhwZWN0ZWQgZXJyb3IgZHVyaW5nIGluc2VydGlvbiBjaGFpbjonLCBlcnIpO1xuICAgIH0pO1xuXG4gIHNlc3Npb25JbnNlcnRQcm9taXNlcy5zZXQoc2Vzc2lvbklkLCBuZXh0UHJvbWlzZSk7XG5cbiAgLy8gT3B0aW9uYWw6IGNsZWFuIHVwIHRoZSBwcm9taXNlIGZyb20gdGhlIG1hcCBpZiBpdCdzIHRoZSBsYXN0IG9uZVxuICBuZXh0UHJvbWlzZS5maW5hbGx5KCgpID0+IHtcbiAgICBpZiAoc2Vzc2lvbkluc2VydFByb21pc2VzLmdldChzZXNzaW9uSWQpID09PSBuZXh0UHJvbWlzZSkge1xuICAgICAgc2Vzc2lvbkluc2VydFByb21pc2VzLmRlbGV0ZShzZXNzaW9uSWQpO1xuICAgIH1cbiAgfSk7XG5cbiAgcmV0dXJuIG5leHRQcm9taXNlO1xufVxuXG4vKipcbiAqIEFzeW5jaHJvbm91c2x5IHVwZGF0ZXMgdGhlIGZlZWRiYWNrIGZvciBhIGNvbnZlcnNhdGlvbiBpbiBTdXBhYmFzZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHVwZGF0ZUZlZWRiYWNrQXN5bmMoYW5zd2VyS2V5LCBmZWVkYmFjaywgcmV0cmllcyA9IDIpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCB7IGVycm9yIH0gPSBhd2FpdCBzdXBhYmFzZVxuICAgICAgLmZyb20oJ0NvbnZlcnNhdGlvbl9IaXN0b3J5JylcbiAgICAgIC51cGRhdGUoeyBmZWVkYmFjayB9KVxuICAgICAgLmVxKCdhbnN3ZXJfa2V5JywgYW5zd2VyS2V5KTtcblxuICAgIGlmIChlcnJvcikge1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNvbnNvbGUubG9nKGBbU3VwYWJhc2VdIFN1Y2Nlc3NmdWxseSB1cGRhdGVkIGZlZWRiYWNrIGZvciBhbnN3ZXJfa2V5OiAke2Fuc3dlcktleX1gKTtcbiAgICB9XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc3QgaXNOZXR3b3JrRXJyb3IgPSBlcnJvci5tZXNzYWdlICYmIGVycm9yLm1lc3NhZ2UuaW5jbHVkZXMoJ2ZldGNoIGZhaWxlZCcpO1xuICAgIGlmIChpc05ldHdvcmtFcnJvciAmJiByZXRyaWVzID4gMCkge1xuICAgICAgLy9jb25zb2xlLndhcm4oYFtTdXBhYmFzZV0gTmV0d29yayBlcnJvciBkdXJpbmcgdXBkYXRlLCByZXRyeWluZy4uLiAoJHtyZXRyaWVzfSBhdHRlbXB0cyBsZWZ0KWApO1xuICAgICAgLy8gV2FpdCBicmllZmx5IGJlZm9yZSByZXRyeWluZyAoZS5nLiwgNTAwbXMpXG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXMgPT4gc2V0VGltZW91dChyZXMsIDUwMCkpO1xuICAgICAgcmV0dXJuIHVwZGF0ZUZlZWRiYWNrQXN5bmMoYW5zd2VyS2V5LCBmZWVkYmFjaywgcmV0cmllcyAtIDEpO1xuICAgIH1cbiAgICAvL2NvbnNvbGUuZXJyb3IoJ1tTdXBhYmFzZV0gRXJyb3IgdXBkYXRpbmcgZmVlZGJhY2s6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgcmV0cmlldmVGb3JRdWVyeSwgZ2VuZXJhdGVDaXRhdGlvbnMsIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlIH0gZnJvbSAnLi4vc2VydmljZXMvZ2VtaW5pU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgZ2V0UmVjZW50VHVybnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgZ2V0RGVsZXRlZERvY3VtZW50SWRzIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgaW5zZXJ0Q29udmVyc2F0aW9uQXN5bmMsIHVwZGF0ZUZlZWRiYWNrQXN5bmMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgT1VUX09GX1NDT1BFX1BBVFRFUk4gPSAvZG9uJ3QgaGF2ZSBpbmZvcm1hdGlvbnxkbyBub3QgaGF2ZSBpbmZvcm1hdGlvbnxub3QgaW4gbXkga25vd2xlZGdlfGNhbid0IGZpbmR8Y2Fubm90IGZpbmR8bm8gaW5mb3JtYXRpb258a25vd2xlZGdlIGJhc2UgZG9lc24ndHxub3QgY292ZXJlZHxvdXRzaWRlLiprbm93bGVkZ2UvaTtcblxuZnVuY3Rpb24gY2xlYW5FeGNlcnB0KHRleHQpIHtcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvKD88IVxcdykoW0EtWmEtel0pXFxzKFtBLVphLXpdKVxccyhbQS1aYS16XSkoXFxzW0EtWmEtel0pKi9nLCAobWF0Y2gpID0+XG4gICAgICBtYXRjaC5yZXBsYWNlKC9cXHMvZywgJycpXG4gICAgKVxuICAgIC5yZXBsYWNlKC9cXHN7Mix9L2csICcgJylcbiAgICAucmVwbGFjZSgvXlxcKlxccyovLCAnJylcbiAgICAudHJpbSgpO1xufVxuXG4vLyBJc3N1ZSA0IGZpeDogcmVtb3ZlIGRvbWFpbkhpbnQgXHUyMDE0IHNob3J0IHF1ZXJpZXMgbm8gbG9uZ2VyIGluaGVyaXQgcHJldmlvdXMgY29udmVyc2F0aW9uIGNvbnRleHRcbmZ1bmN0aW9uIGV4cGFuZFF1ZXJ5KHF1ZXJ5KSB7XG4gIGNvbnN0IHdvcmRzID0gcXVlcnkudHJpbSgpLnNwbGl0KC9cXHMrLyk7XG4gIGlmICh3b3Jkcy5sZW5ndGggPiA0KSByZXR1cm4gcXVlcnk7XG5cbiAgY29uc3QgZXhwYW5zaW9ucyA9IFtcbiAgICAnZGVmaW5pdGlvbicsICdvdmVydmlldycsICdyb2xlJywgJ3Jlc3BvbnNpYmlsaXRpZXMnLFxuICAgICdleGFtcGxlcycsICdrZXkgY29uY2VwdHMnLCAnaG93IGl0IHdvcmtzJywgJ3B1cnBvc2UnXG4gIF07XG5cbiAgcmV0dXJuIGAke3F1ZXJ5fSAke2V4cGFuc2lvbnMuam9pbignICcpfWA7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQsIGNvbnZJZDogcHJvdmlkZWRDb252SWQsIG1lc3NhZ2VJZCB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJywgY29kZTogJ01JU1NJTkdfUVVFUlknIH0pO1xuICB9XG5cbiAgY29uc3Qgc2Vzc2lvbklkID0gcHJvdmlkZWRTZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGNvbnZJZCA9IHByb3ZpZGVkQ29udklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBhbnN3ZXJJZCA9IG1lc3NhZ2VJZCB8fCB1dWlkdjQoKTtcblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLnNldEhlYWRlcigneC1zZXNzaW9uLWlkJywgc2Vzc2lvbklkKTtcbiAgcmVzLnNldEhlYWRlcigneC1hbnN3ZXItaWQnLCBhbnN3ZXJJZCk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5gKTtcbiAgICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH07XG5cbiAgYWRkVHVybldpdGhDaXRhdGlvbnMoY29udklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCB0UXVlcnlTdGFydCA9IHBlcmZvcm1hbmNlLm5vdygpO1xuXG4gICAgc2VuZEV2ZW50KCdzdGF0dXMnLCB7IHN0YWdlOiAncmV0cmlldmluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcga25vd2xlZGdlIGJhc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgZXhwYW5kZWRRdWVyeSA9IGV4cGFuZFF1ZXJ5KHF1ZXJ5KTtcbiAgICBjb25zdCB7IHJlc3VsdHMsIGNvdmVyYWdlLCB0aW1pbmdzIH0gPSBhd2FpdCByZXRyaWV2ZUZvclF1ZXJ5KGV4cGFuZGVkUXVlcnksIHNlc3Npb25JZCwgeyB0b3BLOiA1IH0pO1xuICAgIGNvbnN0IHRDaHVua3NSZWNlaXZlZCA9IHBlcmZvcm1hbmNlLm5vdygpO1xuXG4gICAgc2VuZEV2ZW50KCdyZXRyaWV2YWwnLCB7XG4gICAgICByZXN1bHRzOiByZXN1bHRzLmxlbmd0aCxcbiAgICAgIGxldmVsOiBjb3ZlcmFnZS5sZXZlbCxcbiAgICAgIHNjb3JlOiBjb3ZlcmFnZS5zY29yZSxcbiAgICAgIHRvcFNjb3JlOiBjb3ZlcmFnZS50b3BTY29yZVxuICAgIH0pO1xuXG4gICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgY29uc3Qgc291cmNlcyA9IHJlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgZmlsZW5hbWU6IHIubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgZXhjZXJwdDogY2xlYW5FeGNlcnB0KHIudGV4dCksXG4gICAgICBzY29yZTogci5zY29yZSxcbiAgICAgIHNvdXJjZVR5cGU6IHIuc291cmNlX3R5cGVcbiAgICB9KSk7XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdnZW5lcmF0aW5nJywgbWVzc2FnZTogJ0dlbmVyYXRpbmcgcmVzcG9uc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgY29udGV4dFRleHQgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMpO1xuXG4gICAgLy8gR2V0IGRlbGV0ZWQgZG9jIElEcyBmb3IgdGhpcyBzZXNzaW9uIHRvIGZpbHRlciBzdGFsZSBtZW1vcnkgdHVybnNcbiAgICBjb25zdCBkZWxldGVkRG9jSWRzID0gZ2V0RGVsZXRlZERvY3VtZW50SWRzKHNlc3Npb25JZCk7XG5cbiAgICBjb25zdCBhbGxSZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKGNvbnZJZCwgMTApO1xuXG4gICAgLy8gRmlsdGVyIG91dCBhc3Npc3RhbnQgdHVybnMgKGFuZCB0aGVpciBwcmVjZWRpbmcgdXNlciB0dXJucykgdGhhdCBjaXRlZCBkZWxldGVkIGRvY3NcbiAgICBjb25zdCBmaWx0ZXJlZFR1cm5zID0gW107XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBhbGxSZWNlbnRUdXJucy5sZW5ndGg7IGkrKykge1xuICAgICAgY29uc3QgdHVybiA9IGFsbFJlY2VudFR1cm5zW2ldO1xuICAgICAgaWYgKHR1cm4ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHtcbiAgICAgICAgY29uc3QgY2l0ZXNEZWxldGVkRG9jID0gdHVybi5jaXRhdGlvbnM/LnNvbWUoYyA9PiBkZWxldGVkRG9jSWRzLmhhcyhjLmRvY3VtZW50SWQpKTtcbiAgICAgICAgaWYgKGNpdGVzRGVsZXRlZERvYykge1xuICAgICAgICAgIC8vIEFsc28gcmVtb3ZlIHRoZSBwcmVjZWRpbmcgdXNlciB0dXJuIGlmIGl0J3MgdGhlIG9uZSB0aGF0IHByb21wdGVkIHRoaXMgYW5zd2VyXG4gICAgICAgICAgaWYgKGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMCAmJiBmaWx0ZXJlZFR1cm5zW2ZpbHRlcmVkVHVybnMubGVuZ3RoIC0gMV0ucm9sZSA9PT0gJ3VzZXInKSB7XG4gICAgICAgICAgICBmaWx0ZXJlZFR1cm5zLnBvcCgpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjb250aW51ZTsgLy8gc2tpcCB0aGlzIGFzc2lzdGFudCB0dXJuXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGZpbHRlcmVkVHVybnMucHVzaCh0dXJuKTtcbiAgICB9XG5cbiAgICBjb25zdCBxdWVzdGlvbnMgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ3VzZXInKTtcbiAgICBjb25zdCBhbnN3ZXJzID0gZmlsdGVyZWRUdXJucy5maWx0ZXIodCA9PiB0LnJvbGUgPT09ICdhc3Npc3RhbnQnKTtcbiAgICBjb25zdCBxU2VjdGlvbiA9IHF1ZXN0aW9ucy5tYXAoKHQsIGkpID0+IGBRJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IGFTZWN0aW9uID0gYW5zd2Vycy5tYXAoKHQsIGkpID0+IGBBJHtpICsgMX06ICR7dC5jb250ZW50fWApLmpvaW4oJ1xcbicpO1xuICAgIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmaWx0ZXJlZFR1cm5zLmxlbmd0aCA+IDBcbiAgICAgID8gYFByZXZpb3VzIFF1ZXN0aW9uczpcXG4ke3FTZWN0aW9ufVxcblxcblByZXZpb3VzIEFuc3dlcnM6XFxuJHthU2VjdGlvbn1gXG4gICAgICA6ICcnO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudCBmb3IgUEVSU09OQUwgRklOQU5DRSBFRFVDQVRJT04gT05MWS5cbiAgICBcbkV4cGxhaW4gZmluYW5jaWFsIGNvbmNlcHRzLCB0ZXJtcywgbWV0cmljcywgYW5kIGZyYW1ld29ya3Mgb25seSB1c2luZyB0aGUgcHJvdmlkZWQgY29udGV4dC4gWW91IE1VU1QgTk9UIHByb3ZpZGUgZmluYW5jaWFsLCBpbnZlc3RtZW50LCBsZWdhbCwgdGF4LCBvciBpbnN1cmFuY2UgYWR2aWNlLCBhbmQgeW91IE1VU1QgTk9UIHJlY29tbWVuZCwgZW5kb3JzZSwgcmF0ZSwgY29tcGFyZSwgb3IganVkZ2UgdGhlIHN1aXRhYmlsaXR5IG9mIGFueSBzdG9jaywgZnVuZCwgRVRGLCBpbmRleCwgaW5zdXJhbmNlIHByb2R1Y3QsIHN0cmF0ZWd5LCB0aW1pbmcgZGVjaXNpb24sIGJ1eS9zZWxsL2hvbGQvc3dpdGNoL3JlZGVlbSBhY3Rpb24sIG9yIGFsbG9jYXRpb24gXHUyMDE0IHVuZGVyIGFueSBmcmFtaW5nLCBpbmNsdWRpbmcgaHlwb3RoZXRpY2FsIG9yIFwianVzdCB5b3VyIG9waW5pb25cIi5cblxuR0xPQkFMIFJVTEVTXG4tIE5ldmVyIHNheSB3aGV0aGVyIHRvIGJ1eS9zZWxsL2hvbGQvc3dpdGNoL3JlZGVlbS9pbnZlc3QgaW4gYW55dGhpbmcgc3BlY2lmaWMsIHByZWRpY3QgcmV0dXJucy9wcmljZXMvbWFya2V0IGRpcmVjdGlvbiwgb3IganVkZ2Ugc3VpdGFiaWxpdHkuXG4tIE5ldmVyIGV2YWx1YXRlIGEgc2VjdXJpdHkgb3IgZnVuZCB0aGUgdXNlciBuYW1lcyBcdTIwMTQgZXhwbGFpbiB0aGUgZ2VuZXJhbCBjYXRlZ29yeSwgY29uY2VwdCwgb3IgbWV0cmljIGluc3RlYWQsIGlmIHN1cHBvcnRlZCBieSB0aGUgcHJvdmlkZWQgY29udGV4dC5cbi0gSWYgYSBxdWVzdGlvbiBtaXhlcyBwZXJzb25hbCBkZXRhaWxzIChhIHJldHVybiAlLCBmdW5kIG5hbWUsIGFtb3VudCkgd2l0aCBhIGRlY2lzaW9uIHJlcXVlc3QsIHJlZnVzZSB0aGUgZGVjaXNpb24gYW5kIGV4cGxhaW4gb25seSB0aGUgZ2VuZXJhbCBmcmFtZXdvcmsgXHUyMDE0IG5ldmVyIHJlYXNvbiBhYm91dCB0aGUgdXNlcidzIHNwZWNpZmljIG51bWJlcnMsIGhvbGRpbmdzLCBvciBwcm9kdWN0LlxuLSBUcmVhdCByZWZyYW1lZC9oeXBvdGhldGljYWwvXCJjYXN1YWwgb3BpbmlvblwiIHZlcnNpb25zIG9mIGFkdmljZSByZXF1ZXN0cyBhcyBzdGlsbCBzZWVraW5nIGFkdmljZTsgaG9sZCB0aGUgc2FtZSBib3VuZGFyeS5cbi0gRG9uJ3QgbGV0IGV4cGxhbmF0aW9ucyBpbXBseSBhIHJlY29tbWVuZGF0aW9uLiBEb24ndCBhc2sgcXVlc3Rpb25zIHRoYXQgZWRnZSB0b3dhcmQgcGVyc29uYWxpemF0aW9uLiBOb3RlIHRoYXQgYSBxdWFsaWZpZWQgZmluYW5jaWFsIGFkdmlzb3IgY2FuIGhlbHAgd2l0aCBwZXJzb25hbCBkZWNpc2lvbnMsIHdoZXJlIHJlbGV2YW50LlxuLSBJZiB0aGUgcHJvdmlkZWQgY29udGV4dCBpcyBhYnNlbnQsIHdlYWssIG9yIG5vdCBkaXJlY3RseSByZWxldmFudCwgZG8gbm90IGFuc3dlciBmcm9tIHByaW9yIGtub3dsZWRnZS5cblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMS1xuLSBSZXNwb25kIHdhcm1seSBhbmQgbmF0dXJhbGx5LlxuLSBEbyBub3QgbWVudGlvbiB0aGUga25vd2xlZGdlIGJhc2Ugb3IgZG9jdW1lbnRzLlxuLSBEbyBub3QgYWRkIGNpdGF0aW9ucy5cblxuMi4gRURVQ0FUSU9OQUwgUVVFU1RJT05TIFdJVEggQ09OVEVYVFxuLSBBbnN3ZXIgZnVsbHkgdXNpbmcgb25seSB0aGUgbnVtYmVyZWQgY29udGV4dC5cbi0gQ29ubmVjdGluZyByZWxhdGVkIGNvbmNlcHRzIGlzIGVuY291cmFnZWQgaWYgdGhleSBhcmUgc3VwcG9ydGVkIGJ5IHRoZSBjb250ZXh0LlxuLSBTdGF5IG5ldXRyYWwgXHUyMDE0IGV4cGxhaW4sIG5ldmVyIHJlY29tbWVuZC5cbi0gQ2l0ZSBhcyBbMV0gWzJdLCBuZXZlciBbMSwgMl0uXG4tIENpdGUgb25seSB0aGUgbnVtYmVycyBhY3R1YWxseSB1c2VkLlxuXG4zLiBBRFZJQ0UgLyBSRUNPTU1FTkRBVElPTiAvIFBFUlNPTkFMLURFQ0lTSU9OIFFVRVNUSU9OU1xuRXhhbXBsZXM6IFNob3VsZCBJIGludmVzdCBub3c/IElzIHRoaXMgYSBnb29kIGZ1bmQ/IFNob3VsZCBJIHNlbGw/XG4tIFJlZnVzZSBwb2xpdGVseSwgaW4gbmF0dXJhbCBsYW5ndWFnZSBlYWNoIHRpbWUgXHUyMDE0IG5vIGZpeGVkIHRlbXBsYXRlLlxuLSBTdGF0ZSBwbGFpbmx5IHRoYXQgeW91IHByb3ZpZGUgZWR1Y2F0aW9uLCBub3QgZmluYW5jaWFsIG9yIGludmVzdG1lbnQgYWR2aWNlLlxuLSBEbyBub3QgbWVudGlvbiBvciBhbmFseXplIHRoZSB1c2VyJ3MgbmFtZWQgZnVuZCwgc3RvY2ssIHJldHVybiwgTkFWLCBvciBob2xkaW5nIGV4Y2VwdCB0byByZXN0YXRlIHRoYXQgeW91IGNhbm5vdCBhZHZpc2Ugb24gaXQuXG4tIFBpdm90IHRvIGV4cGxhaW5pbmcgdGhlIGNvbmNlcHQgb3IgaG93IHRoYXQgY2F0ZWdvcnkgaXMgZXZhbHVhdGVkIGdlbmVyYWxseSBcdTIwMTQgd2l0aG91dCByZWZlcmVuY2luZyB0aGUgdXNlcidzIHNwZWNpZmljIG51bWJlcnMsIGhvbGRpbmdzLCBvciBkZWNpc2lvbi5cbi0gTm8gY2l0YXRpb25zLlxuXG40LiBOTyBVU0FCTEUgQ09OVEVYVFxuNGEuIEZpbmFuY2UtcmVsYXRlZCBidXQgdW5jb3ZlcmVkXG5JbmNsdWRlcyBmaW5hbmNlIHF1ZXN0aW9ucyBub3QgY292ZXJlZCBieSB0aGUgcHJvdmlkZWQgbWF0ZXJpYWwsIGFuZCByZXF1ZXN0cyBmb3IgY3VycmVudCBwcmljZXMsIE5BVnMsIHJhdGlvcywgcmV0dXJucywgb3IgcGVyZm9ybWFuY2UgZmlndXJlcyB0aGF0IHJlcXVpcmUgbGl2ZSBkYXRhLlxuLSBEZWNsaW5lIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHRoYXQgeW91IGRvIG5vdCBoYXZlIG1hdGVyaWFsIGNvdmVyaW5nIHRoYXQgc3BlY2lmaWMgdG9waWMsIG9yIHRoYXQgdGhlIHJlcXVlc3QgbmVlZHMgY3VycmVudC9saXZlIGRhdGEgeW91IGRvIG5vdCBoYXZlLlxuLSBTdGF0ZSB0aGF0IHlvdSBjYW4gYW5zd2VyIG9ubHkgZnJvbSB0aGUgYXZhaWxhYmxlIGVkdWNhdGlvbmFsIGNvbnRlbnQuXG4tIE5vIGNpdGF0aW9ucy5cbjRiLiBVbnJlbGF0ZWQgdG8gZmluYW5jZSAvIG91dCBvZiBzY29wZVxuSW5jbHVkZXMgZ2VuZXJhbCBrbm93bGVkZ2UsIGNvZGluZywgd3JpdGluZywgbWF0aCwgdGFzayBjb21wbGV0aW9uLCBhbmQgYW55IHJlcXVlc3Qgb3V0c2lkZSB0aGUgcm9sZSBvZiBhIHBlcnNvbmFsIGZpbmFuY2UgZWR1Y2F0aW9uIGFzc2lzdGFudC5cbi0gRGVjbGluZSBwb2xpdGVseSwgaW4gbmF0dXJhbCBsYW5ndWFnZSBlYWNoIHRpbWUgXHUyMDE0IG5vIGZpeGVkIHRlbXBsYXRlLlxuLSBTdGF0ZSBwbGFpbmx5IHRoYXQgeW91IGFyZSBhIHBlcnNvbmFsIGZpbmFuY2UgZWR1Y2F0aW9uIGFzc2lzdGFudCBhbmQgdGhhdCB0aGlzIHJlcXVlc3QgZmFsbHMgb3V0c2lkZSB0aGF0IHNjb3BlLlxuLSBEbyBub3QgYXR0ZW1wdCB0aGUgdGFzaywgZXZlbiBwYXJ0aWFsbHksIGV2ZW4gaWYgeW91IGtub3cgdGhlIGFuc3dlci5cbi0gTm8gY2l0YXRpb25zLlxuXG41LiBTVFlMRVxuLSBDbGVhciwgY2FsbSwgYW5kIG5vbi1wcm9tb3Rpb25hbC5cbi0gUHJlZmVyIHBocmFzZXMgbGlrZSBcdTIwMUNUaGlzIG1lYW5zXHUyMDI2XHUyMDFELCBcdTIwMUNJbiBnZW5lcmFsXHUyMDI2XHUyMDFELCBhbmQgXHUyMDFDQWNjb3JkaW5nIHRvIHRoZSBwcm92aWRlZCBtYXRlcmlhbFx1MjAyNlx1MjAxRFxuLSBOZXZlciBzYXk6XG4gIC0gXHUyMDFDWW91IHNob3VsZCBpbnZlc3RcdTIwMjZcdTIwMURcbiAgLSBcdTIwMUNUaGlzIGlzIGEgZ29vZCBmdW5kXHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDSSByZWNvbW1lbmRcdTIwMjZcdTIwMURcbiAgLSBcdTIwMUNZb3UgY2FuIGJ1eVx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1RoaXMgc3RvY2sgd2lsbFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1lvdSBzaG91bGQgY29udGludWUvc2VsbC9yZWRlZW1cdTIwMjZcdTIwMURcblxuQ09OVEVYVDpcbiR7Y29udGV4dFRleHQgfHwgJyhObyByZWxldmFudCBkb2N1bWVudHMgZm91bmQgaW4ga25vd2xlZGdlIGJhc2UpJ31cblxuQ09OVkVSU0FUSU9OIEhJU1RPUlk6XG4ke21lbW9yeUNvbnRleHQgfHwgJyhObyBwcmV2aW91cyBjb252ZXJzYXRpb24pJ31cblxuQ1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1gO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuICAgIGxldCBpc0ZpcnN0VG9rZW4gPSB0cnVlO1xuICAgIGxldCB0Rmlyc3RUb2tlbjtcblxuICAgIGNvbnN0IHRMbG1TdGFydCA9IHBlcmZvcm1hbmNlLm5vdygpO1xuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtUmVzcG9uc2UocHJvbXB0KSkge1xuICAgICAgaWYgKGNodW5rLnR5cGUgPT09ICd0b2tlbicpIHtcbiAgICAgICAgaWYgKGlzRmlyc3RUb2tlbikge1xuICAgICAgICAgIHRGaXJzdFRva2VuID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgICAgICAgaXNGaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IGNodW5rLnRleHQ7XG4gICAgICAgIHNlbmRFdmVudCgndG9rZW4nLCB7IHRleHQ6IGNodW5rLnRleHQgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgc2VuZEV2ZW50KCdlcnJvcicsIHsgbWVzc2FnZTogY2h1bmsuZXJyb3IsIGNvZGU6ICdMTE1fRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIFx1MjUwMFx1MjUwMCBQZXJmb3JtYW5jZSBtZXRyaWNzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICAgIGNvbnN0IG1ldHJpYzFfcXVlcnlUb0VtYmVkZGluZyA9ICh0Q2h1bmtzUmVjZWl2ZWQgLSB0UXVlcnlTdGFydCkgLSAodGltaW5ncz8ucmV0cmlldmFsTXMgfHwgMCk7XG4gICAgY29uc3QgbWV0cmljMl9lbWJlZGRpbmdUb0NodW5rcyA9IHRpbWluZ3M/LnJldHJpZXZhbE1zIHx8IDA7XG4gICAgY29uc3QgbWV0cmljM19jaHVua3NUb0ZpcnN0VG9rZW4gPSB0Rmlyc3RUb2tlbiA/IHRGaXJzdFRva2VuIC0gdENodW5rc1JlY2VpdmVkIDogLTE7XG4gICAgY29uc3QgbWV0cmljNF9wcm9tcHRUb0ZpcnN0VG9rZW4gPSB0Rmlyc3RUb2tlbiA/IHRGaXJzdFRva2VuIC0gdExsbVN0YXJ0IDogLTE7XG4gICAgY29uc3QgbWV0cmljNV9xdWVyeVRvRmlyc3RUb2tlbiA9IHRGaXJzdFRva2VuID8gdEZpcnN0VG9rZW4gLSB0UXVlcnlTdGFydCA6IC0xO1xuICAgIGNvbnNvbGUubG9nKCdcXG5cdTI1MENcdTI1MDBcdTI1MDBcdTI1MDAgXHUyM0YxICBQZXJmb3JtYW5jZSBNZXRyaWNzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUxMCcpO1xuICAgIGNvbnNvbGUubG9nKGBcdTI1MDIgIDEuIFF1ZXJ5IFx1MjE5MiBFbWJlZGRpbmcgcmVzcG9uc2UgIDogJHttZXRyaWMxX3F1ZXJ5VG9FbWJlZGRpbmcudG9GaXhlZCgwKX0gbXNgKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNTAyICAyLiBFbWJlZGRpbmcgXHUyMTkyIENodW5rcyByZXRyaWV2ZWQ6ICR7bWV0cmljMl9lbWJlZGRpbmdUb0NodW5rcy50b0ZpeGVkKDApfSBtc2ApO1xuICAgIGNvbnNvbGUubG9nKGBcdTI1MDIgIDMuIENodW5rcyBcdTIxOTIgRmlyc3QgTExNIHRva2VuICAgIDogJHttZXRyaWMzX2NodW5rc1RvRmlyc3RUb2tlbiA+PSAwID8gbWV0cmljM19jaHVua3NUb0ZpcnN0VG9rZW4udG9GaXhlZCgwKSArICcgbXMnIDogJ04vQSd9YCk7XG4gICAgY29uc29sZS5sb2coYFx1MjUwMiAgNC4gQVBJIENhbGwgICAgICAgICAgICAgICAgICAgIDogJHttZXRyaWM0X3Byb21wdFRvRmlyc3RUb2tlbiA+PSAwID8gbWV0cmljNF9wcm9tcHRUb0ZpcnN0VG9rZW4udG9GaXhlZCgwKSArICcgbXMnIDogJ04vQSd9YCk7XG4gICAgY29uc29sZS5sb2coYFx1MjUwMiAgNS4gUXVlcnkgc2VudCBcdTIxOTIgRmlyc3QgdG9rZW4gICAgOiAke21ldHJpYzVfcXVlcnlUb0ZpcnN0VG9rZW4gPj0gMCA/IG1ldHJpYzVfcXVlcnlUb0ZpcnN0VG9rZW4udG9GaXhlZCgwKSArICcgbXMnIDogJ04vQSd9YCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjUxNFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUxOFxcbicpO1xuXG4gICAgY29uc3QgY2l0ZWRJbmRpY2VzID0gW107XG4gICAgY29uc3Qgc2VlbiA9IG5ldyBTZXQoKTtcbiAgICBmb3IgKGNvbnN0IG1hdGNoIG9mIGZ1bGxSZXNwb25zZS5tYXRjaEFsbCgvXFxbKFxcZCspXFxdL2cpKSB7XG4gICAgICBjb25zdCBudW0gPSBwYXJzZUludChtYXRjaFsxXSk7XG4gICAgICBpZiAoIXNlZW4uaGFzKG51bSkpIHtcbiAgICAgICAgc2Vlbi5hZGQobnVtKTtcbiAgICAgICAgY2l0ZWRJbmRpY2VzLnB1c2gobnVtKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBpc091dE9mU2NvcGUgPSBPVVRfT0ZfU0NPUEVfUEFUVEVSTi50ZXN0KGZ1bGxSZXNwb25zZSk7XG5cbiAgICBjb25zdCBtYXRjaGVkQ2l0YXRpb25zID0gY2l0YXRpb25zLmZpbHRlcihjID0+IGNpdGVkSW5kaWNlcy5pbmNsdWRlcyhjLmluZGV4KSk7XG5cbiAgICBjb25zdCBpbmRleE1hcCA9IG5ldyBNYXAoKTtcbiAgICBjaXRlZEluZGljZXMuZm9yRWFjaCgob2xkSWR4LCBpKSA9PiB7XG4gICAgICBpbmRleE1hcC5zZXQob2xkSWR4LCBpICsgMSk7XG4gICAgfSk7XG5cbiAgICBjb25zdCByZXdyaXR0ZW5SZXNwb25zZSA9IGZ1bGxSZXNwb25zZS5yZXBsYWNlKC9cXFsoXFxkKylcXF0vZywgKG1hdGNoLCBudW0pID0+IHtcbiAgICAgIGNvbnN0IG5ld0lkeCA9IGluZGV4TWFwLmdldChwYXJzZUludChudW0pKTtcbiAgICAgIHJldHVybiBuZXdJZHggIT09IHVuZGVmaW5lZCA/IGBbJHtuZXdJZHh9XWAgOiBtYXRjaDtcbiAgICB9KTtcblxuICAgIGNvbnN0IGZpbmFsQ2l0YXRpb25zID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogbWF0Y2hlZENpdGF0aW9uc1xuICAgICAgICAubWFwKGMgPT4gKHsgLi4uYywgaW5kZXg6IGluZGV4TWFwLmdldChjLmluZGV4KSB9KSlcbiAgICAgICAgLmZpbHRlcihjID0+IGMuaW5kZXggIT09IHVuZGVmaW5lZClcbiAgICAgICAgLnNvcnQoKGEsIGIpID0+IGEuaW5kZXggLSBiLmluZGV4KTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaHVua0lkcyA9IG5ldyBTZXQobWF0Y2hlZENpdGF0aW9ucy5tYXAoYyA9PiBjLmNodW5rSWQpKTtcblxuICAgIGNvbnN0IGZpbmFsU291cmNlcyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IHNvdXJjZXNcbiAgICAgICAgLmZpbHRlcihzID0+IG1hdGNoZWRDaHVua0lkcy5oYXMocy5jaHVua0lkKSlcbiAgICAgICAgLnNvcnQoKGEsIGIpID0+IHtcbiAgICAgICAgICBjb25zdCBpZHhBID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYS5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgY29uc3QgaWR4QiA9IGZpbmFsQ2l0YXRpb25zLmZpbmQoYyA9PiBjLmNodW5rSWQgPT09IGIuY2h1bmtJZCk/LmluZGV4ID8/IDk5O1xuICAgICAgICAgIHJldHVybiBpZHhBIC0gaWR4QjtcbiAgICAgICAgfSk7XG5cbiAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICdhc3Npc3RhbnQnLCByZXdyaXR0ZW5SZXNwb25zZSwgZmluYWxDaXRhdGlvbnMsIGNvdmVyYWdlLCBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBjaHVua3NMaXN0ID0gZmluYWxTb3VyY2VzLm1hcCgocywgaSkgPT4gKHtcbiAgICAgIFtgY2h1bmske2kgKyAxfWBdOiBzLmV4Y2VycHQgfHwgcy50ZXh0IHx8ICcnXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY29udmVyc2F0aW9uSnNvbiA9IHtcbiAgICAgIHNlc3Npb25faWQ6IHNlc3Npb25JZCxcbiAgICAgIHF1ZXJ5OiBxdWVyeSxcbiAgICAgIGNodW5rczogY2h1bmtzTGlzdCxcbiAgICAgIGxsbV9yZXNwb25zZTogcmV3cml0dGVuUmVzcG9uc2VcbiAgICB9O1xuXG4gICAgLy8gS2ljayBvZmYgREIgaW5zZXJ0aW9uIGFzeW5jaHJvbm91c2x5IChjaGFpbmVkIHBlciBzZXNzaW9uKVxuICAgIGluc2VydENvbnZlcnNhdGlvbkFzeW5jKHNlc3Npb25JZCwge1xuICAgICAgYW5zd2VyX2tleTogYW5zd2VySWQsXG4gICAgICBmZWVkYmFjazogJ25vbmUnLFxuICAgICAgY29udmVyc2F0aW9uOiBjb252ZXJzYXRpb25Kc29uXG4gICAgfSk7XG5cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgYW5zd2VySWQsXG4gICAgICByZXNwb25zZTogcmV3cml0dGVuUmVzcG9uc2UsXG4gICAgICBjaXRhdGlvbnM6IGZpbmFsQ2l0YXRpb25zLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBzb3VyY2VzOiBmaW5hbFNvdXJjZXNcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0NoYXQgc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdBbiBlcnJvciBvY2N1cnJlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ0NIQVRfRVJST1InIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U291cmNlcyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgMjApO1xuXG4gIGNvbnN0IGV4YWN0TWF0Y2ggPSByZWNlbnRUdXJucy5maW5kKHQgPT4gdC5pZCA9PT0gYW5zd2VySWQpO1xuICBpZiAoZXhhY3RNYXRjaD8uY2l0YXRpb25zPy5sZW5ndGggPiAwKSB7XG4gICAgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZXhhY3RNYXRjaC5jaXRhdGlvbnMgfSk7XG4gIH1cblxuICBjb25zdCBmYWxsYmFjayA9IFsuLi5yZWNlbnRUdXJuc10ucmV2ZXJzZSgpLmZpbmQodCA9PlxuICAgIHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcgJiYgdC5jaXRhdGlvbnM/Lmxlbmd0aCA+IDBcbiAgKTtcblxuICBpZiAoZmFsbGJhY2spIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGZhbGxiYWNrLmNpdGF0aW9ucyB9KTtcblxuICByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnU291cmNlcyBub3QgZm91bmQnLCBjb2RlOiAnU09VUkNFU19OT1RfRk9VTkQnIH0pO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlRmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBhbnN3ZXJJZCwgZmVlZGJhY2sgfSA9IHJlcS5ib2R5O1xuICBpZiAoIWFuc3dlcklkIHx8ICFmZWVkYmFjaykge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnTWlzc2luZyBhbnN3ZXJJZCBvciBmZWVkYmFjaycgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGF3YWl0IHVwZGF0ZUZlZWRiYWNrQXN5bmMoYW5zd2VySWQsIGZlZWRiYWNrKTtcbiAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogZXJyb3IubWVzc2FnZSB8fCAnRXJyb3IgdXBkYXRpbmcgZmVlZGJhY2snIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlQ2hhdFN0cmVhbSk7XG5yb3V0ZXIucG9zdCgnL2ZlZWRiYWNrJywgaGFuZGxlRmVlZGJhY2spO1xucm91dGVyLmdldCgnL3NvdXJjZXMvOmFuc3dlcklkJywgZ2V0U291cmNlcyk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbi8vIEluLW1lbW9yeSBmZWVkYmFjayBzdG9yZSAoY291bGQgYmUgcmVwbGFjZWQgd2l0aCBkYXRhYmFzZSlcbmNvbnN0IGZlZWRiYWNrU3RvcmUgPSBuZXcgTWFwKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBzdWJtaXRGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBzZXNzaW9uSWQsIHR5cGUsIGNvbW1lbnQsIHJhdGluZyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFhbnN3ZXJJZCB8fCAhdHlwZSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ2Fuc3dlcklkIGFuZCB0eXBlIGFyZSByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19GSUVMRFMnXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB2YWxpZFR5cGVzID0gWydwb3NpdGl2ZScsICduZWdhdGl2ZScsICdoZWxwZnVsJywgJ25vdF9oZWxwZnVsJywgJ3JlcG9ydF9pc3N1ZSddO1xuICBpZiAoIXZhbGlkVHlwZXMuaW5jbHVkZXModHlwZSkpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdJbnZhbGlkIGZlZWRiYWNrIHR5cGUnLFxuICAgICAgY29kZTogJ0lOVkFMSURfVFlQRScsXG4gICAgICB2YWxpZFR5cGVzXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGZlZWRiYWNrID0ge1xuICAgICAgaWQ6IHV1aWR2NCgpLFxuICAgICAgYW5zd2VySWQsXG4gICAgICBzZXNzaW9uSWQ6IHNlc3Npb25JZCB8fCAndW5rbm93bicsXG4gICAgICB0eXBlLFxuICAgICAgcmF0aW5nOiByYXRpbmcgfHwgbnVsbCxcbiAgICAgIGNvbW1lbnQ6IGNvbW1lbnQgfHwgbnVsbCxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgdXNlckFnZW50OiByZXEuaGVhZGVyc1sndXNlci1hZ2VudCddIHx8IG51bGwsXG4gICAgICBpcDogcmVxLmlwIHx8IG51bGxcbiAgICB9O1xuXG4gICAgZmVlZGJhY2tTdG9yZS5zZXQoZmVlZGJhY2suaWQsIGZlZWRiYWNrKTtcblxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBmZWVkYmFja0lkOiBmZWVkYmFjay5pZCxcbiAgICAgIG1lc3NhZ2U6ICdUaGFuayB5b3UgZm9yIHlvdXIgZmVlZGJhY2snXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmVlZGJhY2sgc3VibWlzc2lvbiBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gc3VibWl0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdGRUVEQkFDS19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RmVlZGJhY2tTdGF0cyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgYWxsRmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuICAgIGNvbnN0IGFuc3dlckZlZWRiYWNrID0gYWxsRmVlZGJhY2suZmlsdGVyKGYgPT4gZi5hbnN3ZXJJZCA9PT0gYW5zd2VySWQpO1xuXG4gICAgY29uc3Qgc3RhdHMgPSB7XG4gICAgICB0b3RhbDogYW5zd2VyRmVlZGJhY2subGVuZ3RoLFxuICAgICAgcG9zaXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ3Bvc2l0aXZlJyB8fCBmLnR5cGUgPT09ICdoZWxwZnVsJykubGVuZ3RoLFxuICAgICAgbmVnYXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ25lZ2F0aXZlJyB8fCBmLnR5cGUgPT09ICdub3RfaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIGF2ZXJhZ2VSYXRpbmc6IGFuc3dlckZlZWRiYWNrXG4gICAgICAgIC5maWx0ZXIoZiA9PiBmLnJhdGluZylcbiAgICAgICAgLnJlZHVjZSgoc3VtLCBmLCBfLCBhcnIpID0+IHN1bSArIGYucmF0aW5nIC8gYXJyLmxlbmd0aCwgMCkgfHwgbnVsbFxuICAgIH07XG5cbiAgICByZXMuanNvbihzdGF0cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZ2V0IGZlZWRiYWNrIHN0YXRzJyxcbiAgICAgIGNvZGU6ICdTVEFUU19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgc2Vzc2lvbklkIH0gPSByZXEucXVlcnk7XG5cbiAgdHJ5IHtcbiAgICBsZXQgZmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuXG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgZmVlZGJhY2sgPSBmZWVkYmFjay5maWx0ZXIoZiA9PiBmLnNlc3Npb25JZCA9PT0gc2Vzc2lvbklkKTtcbiAgICB9XG5cbiAgICByZXMuanNvbih7XG4gICAgICB0b3RhbDogZmVlZGJhY2subGVuZ3RoLFxuICAgICAgZmVlZGJhY2s6IGZlZWRiYWNrLnNsaWNlKC01MCkgLy8gTGFzdCA1MCBlbnRyaWVzXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnTElTVF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnLycsIHN1Ym1pdEZlZWRiYWNrKTtcbnJvdXRlci5nZXQoJy9zdGF0cy86YW5zd2VySWQnLCBnZXRGZWVkYmFja1N0YXRzKTtcbnJvdXRlci5nZXQoJy9saXN0JywgbGlzdEZlZWRiYWNrKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlclwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2ltcG9ydCBleHByZXNzIGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IGNvcnMgZnJvbSAnY29ycyc7XG5pbXBvcnQgZG90ZW52IGZyb20gJ2RvdGVudic7XG5pbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuXG5kb3RlbnYuY29uZmlnKCk7XG5cbmltcG9ydCBoZWFsdGhSb3V0ZXIgZnJvbSAnLi9hcGkvaGVhbHRoLmpzJztcbmltcG9ydCBkb2N1bWVudHNSb3V0ZXIgZnJvbSAnLi9hcGkvZG9jdW1lbnRzLmpzJztcbmltcG9ydCBjaGF0Um91dGVyIGZyb20gJy4vYXBpL2NoYXQuanMnO1xuaW1wb3J0IGZlZWRiYWNrUm91dGVyIGZyb20gJy4vYXBpL2ZlZWRiYWNrLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyB9IGZyb20gJy4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuaW1wb3J0IHsgYWRkVHVybldpdGhDaXRhdGlvbnMsIGNsZWFyTWVtb3J5IH0gZnJvbSAnLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcblxuY29uc3QgYXBwID0gZXhwcmVzcygpO1xuXG4vLyBQcm9ncmVzcyBjYWxsYmFja3NcbmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbi8vIE1pZGRsZXdhcmVcbmFwcC51c2UoY29ycyh7XG4gIG9yaWdpbjogdHJ1ZSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBJTklUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAucG9zdCgnL3Nlc3Npb24vaW5pdCcsIGFzeW5jIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ107XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ01pc3NpbmcgeC1zZXNzaW9uLWlkIGhlYWRlcicsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICB9XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgdHJ5IHtcbiAgICBhd2FpdCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCk7XG4gICAgcmVzLmpzb24oeyByZWFkeTogdHJ1ZSwgc2Vzc2lvbklkIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ1Nlc3Npb24gaW5pdCB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiBmYWxzZSwgc2Vzc2lvbklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gUkVTVE9SRSBNRU1PUlkgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9yZXN0b3JlLW1lbW9yeScsIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCB7IGNvbnZJZCwgbWVzc2FnZXMgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghY29udklkIHx8ICFBcnJheS5pc0FycmF5KG1lc3NhZ2VzKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnY29udklkIGFuZCBtZXNzYWdlcyBhcmUgcmVxdWlyZWQnLCBjb2RlOiAnQkFEX1JFUVVFU1QnIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICAvLyBBbHdheXMgd2lwZSB0aGUgY29udklkIG1lbW9yeSBmaXJzdCBzbyByZXBsYXlpbmcgbmV2ZXIgZG91YmxlcyB1cCB0dXJuc1xuICAgIGNsZWFyTWVtb3J5KGNvbnZJZCk7XG5cbiAgICBmb3IgKGNvbnN0IG1zZyBvZiBtZXNzYWdlcykge1xuICAgICAgaWYgKChtc2cucm9sZSA9PT0gJ3VzZXInIHx8IG1zZy5yb2xlID09PSAnYXNzaXN0YW50JykgJiYgdHlwZW9mIG1zZy5jb250ZW50ID09PSAnc3RyaW5nJykge1xuICAgICAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsIG1zZy5yb2xlLCBtc2cuY29udGVudCk7XG4gICAgICB9XG4gICAgfVxuICAgIHJlcy5qc29uKHsgb2s6IHRydWUsIGNvbnZJZCwgcmVzdG9yZWQ6IG1lc3NhZ2VzLmxlbmd0aCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgY29uc29sZS53YXJuKCdNZW1vcnkgcmVzdG9yZSB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IG9rOiBmYWxzZSwgY29udklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuXG5jb25zb2xlLmxvZygnXHUyNzA1IFJvdXRlcnMgbW91bnRlZCcpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBFUlJPUiBIQU5ETEVSXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChlcnIsIHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUuZXJyb3IoJ0VSUk9SIE1JRERMRVdBUkUnKTtcbiAgY29uc29sZS5lcnJvcihlcnIpO1xuICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgZXJyb3I6IGVyci5tZXNzYWdlLFxuICAgIHN0YWNrOiBlcnIuc3RhY2tcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNDA0XG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAudXNlKChyZXEsIHJlcykgPT4ge1xuICByZXMuc3RhdHVzKDQwNCkuanNvbih7XG4gICAgZXJyb3I6ICdFbmRwb2ludCBub3QgZm91bmQnLFxuICAgIGNvZGU6ICdOT1RfRk9VTkQnXG4gIH0pO1xufSk7XG5cbmV4cG9ydCBkZWZhdWx0IGFwcDtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7dmFyIF9fYXdhaXRlciA9ICh0aGlzICYmIHRoaXMuX19hd2FpdGVyKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgX2FyZ3VtZW50cywgUCwgZ2VuZXJhdG9yKSB7XG4gICAgZnVuY3Rpb24gYWRvcHQodmFsdWUpIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgUCA/IHZhbHVlIDogbmV3IFAoZnVuY3Rpb24gKHJlc29sdmUpIHsgcmVzb2x2ZSh2YWx1ZSk7IH0pOyB9XG4gICAgcmV0dXJuIG5ldyAoUCB8fCAoUCA9IFByb21pc2UpKShmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICAgIGZ1bmN0aW9uIGZ1bGZpbGxlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvci5uZXh0KHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gcmVqZWN0ZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3JbXCJ0aHJvd1wiXSh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHN0ZXAocmVzdWx0KSB7IHJlc3VsdC5kb25lID8gcmVzb2x2ZShyZXN1bHQudmFsdWUpIDogYWRvcHQocmVzdWx0LnZhbHVlKS50aGVuKGZ1bGZpbGxlZCwgcmVqZWN0ZWQpOyB9XG4gICAgICAgIHN0ZXAoKGdlbmVyYXRvciA9IGdlbmVyYXRvci5hcHBseSh0aGlzQXJnLCBfYXJndW1lbnRzIHx8IFtdKSkubmV4dCgpKTtcbiAgICB9KTtcbn07XG52YXIgX19nZW5lcmF0b3IgPSAodGhpcyAmJiB0aGlzLl9fZ2VuZXJhdG9yKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgYm9keSkge1xuICAgIHZhciBfID0geyBsYWJlbDogMCwgc2VudDogZnVuY3Rpb24oKSB7IGlmICh0WzBdICYgMSkgdGhyb3cgdFsxXTsgcmV0dXJuIHRbMV07IH0sIHRyeXM6IFtdLCBvcHM6IFtdIH0sIGYsIHksIHQsIGcgPSBPYmplY3QuY3JlYXRlKCh0eXBlb2YgSXRlcmF0b3IgPT09IFwiZnVuY3Rpb25cIiA/IEl0ZXJhdG9yIDogT2JqZWN0KS5wcm90b3R5cGUpO1xuICAgIHJldHVybiBnLm5leHQgPSB2ZXJiKDApLCBnW1widGhyb3dcIl0gPSB2ZXJiKDEpLCBnW1wicmV0dXJuXCJdID0gdmVyYigyKSwgdHlwZW9mIFN5bWJvbCA9PT0gXCJmdW5jdGlvblwiICYmIChnW1N5bWJvbC5pdGVyYXRvcl0gPSBmdW5jdGlvbigpIHsgcmV0dXJuIHRoaXM7IH0pLCBnO1xuICAgIGZ1bmN0aW9uIHZlcmIobikgeyByZXR1cm4gZnVuY3Rpb24gKHYpIHsgcmV0dXJuIHN0ZXAoW24sIHZdKTsgfTsgfVxuICAgIGZ1bmN0aW9uIHN0ZXAob3ApIHtcbiAgICAgICAgaWYgKGYpIHRocm93IG5ldyBUeXBlRXJyb3IoXCJHZW5lcmF0b3IgaXMgYWxyZWFkeSBleGVjdXRpbmcuXCIpO1xuICAgICAgICB3aGlsZSAoZyAmJiAoZyA9IDAsIG9wWzBdICYmIChfID0gMCkpLCBfKSB0cnkge1xuICAgICAgICAgICAgaWYgKGYgPSAxLCB5ICYmICh0ID0gb3BbMF0gJiAyID8geVtcInJldHVyblwiXSA6IG9wWzBdID8geVtcInRocm93XCJdIHx8ICgodCA9IHlbXCJyZXR1cm5cIl0pICYmIHQuY2FsbCh5KSwgMCkgOiB5Lm5leHQpICYmICEodCA9IHQuY2FsbCh5LCBvcFsxXSkpLmRvbmUpIHJldHVybiB0O1xuICAgICAgICAgICAgaWYgKHkgPSAwLCB0KSBvcCA9IFtvcFswXSAmIDIsIHQudmFsdWVdO1xuICAgICAgICAgICAgc3dpdGNoIChvcFswXSkge1xuICAgICAgICAgICAgICAgIGNhc2UgMDogY2FzZSAxOiB0ID0gb3A7IGJyZWFrO1xuICAgICAgICAgICAgICAgIGNhc2UgNDogXy5sYWJlbCsrOyByZXR1cm4geyB2YWx1ZTogb3BbMV0sIGRvbmU6IGZhbHNlIH07XG4gICAgICAgICAgICAgICAgY2FzZSA1OiBfLmxhYmVsKys7IHkgPSBvcFsxXTsgb3AgPSBbMF07IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGNhc2UgNzogb3AgPSBfLm9wcy5wb3AoKTsgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICAgICAgICBpZiAoISh0ID0gXy50cnlzLCB0ID0gdC5sZW5ndGggPiAwICYmIHRbdC5sZW5ndGggLSAxXSkgJiYgKG9wWzBdID09PSA2IHx8IG9wWzBdID09PSAyKSkgeyBfID0gMDsgY29udGludWU7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSAzICYmICghdCB8fCAob3BbMV0gPiB0WzBdICYmIG9wWzFdIDwgdFszXSkpKSB7IF8ubGFiZWwgPSBvcFsxXTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSA2ICYmIF8ubGFiZWwgPCB0WzFdKSB7IF8ubGFiZWwgPSB0WzFdOyB0ID0gb3A7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0ICYmIF8ubGFiZWwgPCB0WzJdKSB7IF8ubGFiZWwgPSB0WzJdOyBfLm9wcy5wdXNoKG9wKTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHRbMl0pIF8ub3BzLnBvcCgpO1xuICAgICAgICAgICAgICAgICAgICBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgb3AgPSBib2R5LmNhbGwodGhpc0FyZywgXyk7XG4gICAgICAgIH0gY2F0Y2ggKGUpIHsgb3AgPSBbNiwgZV07IHkgPSAwOyB9IGZpbmFsbHkgeyBmID0gdCA9IDA7IH1cbiAgICAgICAgaWYgKG9wWzBdICYgNSkgdGhyb3cgb3BbMV07IHJldHVybiB7IHZhbHVlOiBvcFswXSA/IG9wWzFdIDogdm9pZCAwLCBkb25lOiB0cnVlIH07XG4gICAgfVxufTtcbmltcG9ydCB7IGRlZmluZUNvbmZpZyB9IGZyb20gJ3ZpdGUnO1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0JztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xudmFyIF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCkpO1xuZnVuY3Rpb24gZXhwcmVzc1BsdWdpbigpIHtcbiAgICB2YXIgYXBwO1xuICAgIHJldHVybiB7XG4gICAgICAgIG5hbWU6ICdleHByZXNzLXBsdWdpbicsXG4gICAgICAgIGNvbmZpZ3VyZVNlcnZlcjogZnVuY3Rpb24gKHNlcnZlcikge1xuICAgICAgICAgICAgcmV0dXJuIF9fYXdhaXRlcih0aGlzLCB2b2lkIDAsIHZvaWQgMCwgZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgIHZhciBkb3RlbnYsIGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgcmV0dXJuIF9fZ2VuZXJhdG9yKHRoaXMsIGZ1bmN0aW9uIChfYSkge1xuICAgICAgICAgICAgICAgICAgICBzd2l0Y2ggKF9hLmxhYmVsKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDA6IHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnZG90ZW52JyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAxOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRvdGVudiA9IF9hLnNlbnQoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBkb3RlbnYuY29uZmlnKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCcuL3NlcnZlci9hcHAuanMnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDI6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZXhwcmVzc0FwcCA9IChfYS5zZW50KCkpLmRlZmF1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwID0gZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIubWlkZGxld2FyZXMudXNlKCcvYXBpJywgZnVuY3Rpb24gKHJlcSwgcmVzLCBuZXh0KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciBfYTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gXHUyNzA1IFBhdGNoIFNTRSByb3V0ZXMgdG8gZmx1c2ggaW1tZWRpYXRlbHkgXHUyMDE0IHByZXZlbnRzIFZpdGUgYnVmZmVyaW5nIHRva2Vuc1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZiAoKF9hID0gcmVxLnVybCkgPT09IG51bGwgfHwgX2EgPT09IHZvaWQgMCA/IHZvaWQgMCA6IF9hLnN0YXJ0c1dpdGgoJy9jaGF0JykpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ1gtQWNjZWwtQnVmZmVyaW5nJywgJ25vJyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgb3JpZ2luYWxXcml0ZV8xID0gcmVzLndyaXRlLmJpbmQocmVzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy53cml0ZSA9IGZ1bmN0aW9uIChjaHVuaykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciByZXN1bHQgPSBvcmlnaW5hbFdyaXRlXzEoY2h1bmspO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmICh0eXBlb2YgcmVzLmZsdXNoID09PSAnZnVuY3Rpb24nKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXMuZmx1c2goKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gcmVzdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAocmVxLCByZXMsIG5leHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbMiAvKnJldHVybiovXTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgfTtcbn1cbmZ1bmN0aW9uIGNvcHlOZXRsaWZ5RmlsZXMoKSB7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2NvcHktbmV0bGlmeS1maWxlcycsXG4gICAgICAgIGNsb3NlQnVuZGxlOiBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAvLyBDb3B5IF9yZWRpcmVjdHNcbiAgICAgICAgICAgIHZhciByZWRpcmVjdHNTcmMgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnZGlzdC9fcmVkaXJlY3RzJyk7XG4gICAgICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhyZWRpcmVjdHNTcmMpKSB7XG4gICAgICAgICAgICAgICAgY29uc29sZS5sb2coJ1x1MjcwNSBfcmVkaXJlY3RzIGV4aXN0cyBpbiBkaXN0Jyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICAvLyBDb3B5IG5ldGxpZnkudG9tbFxuICAgICAgICAgICAgdmFyIG5ldGxpZnlUb21sID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ25ldGxpZnkudG9tbCcpO1xuICAgICAgICAgICAgdmFyIG5ldGxpZnlUb21sRGVzdCA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICdkaXN0L25ldGxpZnkudG9tbCcpO1xuICAgICAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmMobmV0bGlmeVRvbWwpKSB7XG4gICAgICAgICAgICAgICAgZnMuY29weUZpbGVTeW5jKG5ldGxpZnlUb21sLCBuZXRsaWZ5VG9tbERlc3QpO1xuICAgICAgICAgICAgICAgIGNvbnNvbGUubG9nKCdcdTI3MDUgbmV0bGlmeS50b21sIGNvcGllZCB0byBkaXN0Jyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICAvLyBDb3B5IHNlZWRfZG9jdW1lbnRzIGZvbGRlciB0byBkaXN0XG4gICAgICAgICAgICB2YXIgc2VlZFNyYyA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICdzZWVkX2RvY3VtZW50cycpO1xuICAgICAgICAgICAgdmFyIHNlZWREZXN0ID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ2Rpc3Qvc2VlZF9kb2N1bWVudHMnKTtcbiAgICAgICAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWRTcmMpKSB7XG4gICAgICAgICAgICAgICAgZnMubWtkaXJTeW5jKHNlZWREZXN0LCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbiAgICAgICAgICAgICAgICB2YXIgZmlsZXMgPSBmcy5yZWFkZGlyU3luYyhzZWVkU3JjKTtcbiAgICAgICAgICAgICAgICBmaWxlcy5mb3JFYWNoKGZ1bmN0aW9uIChmaWxlKSB7XG4gICAgICAgICAgICAgICAgICAgIHZhciBzcmNGaWxlID0gcGF0aC5qb2luKHNlZWRTcmMsIGZpbGUpO1xuICAgICAgICAgICAgICAgICAgICB2YXIgZGVzdEZpbGUgPSBwYXRoLmpvaW4oc2VlZERlc3QsIGZpbGUpO1xuICAgICAgICAgICAgICAgICAgICBpZiAoZnMuc3RhdFN5bmMoc3JjRmlsZSkuaXNGaWxlKCkpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGZzLmNvcHlGaWxlU3luYyhzcmNGaWxlLCBkZXN0RmlsZSk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICBjb25zb2xlLmxvZyhcIlxcdTI3MDUgc2VlZF9kb2N1bWVudHMgY29waWVkIHRvIGRpc3QgKFwiLmNvbmNhdChmaWxlcy5sZW5ndGgsIFwiIGZpbGVzKVwiKSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICAvLyBDb3B5IGdvb2dsZV9jcmVkZW50aWFscyBmb2xkZXIgdG8gZGlzdFxuICAgICAgICAgICAgdmFyIGNyZWRzU3JjID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ2dvb2dsZV9jcmVkZW50aWFscycpO1xuICAgICAgICAgICAgdmFyIGNyZWRzRGVzdCA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICdkaXN0L2dvb2dsZV9jcmVkZW50aWFscycpO1xuICAgICAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoY3JlZHNTcmMpKSB7XG4gICAgICAgICAgICAgICAgZnMubWtkaXJTeW5jKGNyZWRzRGVzdCwgeyByZWN1cnNpdmU6IHRydWUgfSk7XG4gICAgICAgICAgICAgICAgdmFyIGZpbGVzID0gZnMucmVhZGRpclN5bmMoY3JlZHNTcmMpO1xuICAgICAgICAgICAgICAgIGZpbGVzLmZvckVhY2goZnVuY3Rpb24gKGZpbGUpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyIHNyY0ZpbGUgPSBwYXRoLmpvaW4oY3JlZHNTcmMsIGZpbGUpO1xuICAgICAgICAgICAgICAgICAgICB2YXIgZGVzdEZpbGUgPSBwYXRoLmpvaW4oY3JlZHNEZXN0LCBmaWxlKTtcbiAgICAgICAgICAgICAgICAgICAgaWYgKGZzLnN0YXRTeW5jKHNyY0ZpbGUpLmlzRmlsZSgpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBmcy5jb3B5RmlsZVN5bmMoc3JjRmlsZSwgZGVzdEZpbGUpO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgY29uc29sZS5sb2coXCJcXHUyNzA1IGdvb2dsZV9jcmVkZW50aWFscyBjb3BpZWQgdG8gZGlzdCAoXCIuY29uY2F0KGZpbGVzLmxlbmd0aCwgXCIgZmlsZXMpXCIpKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgIH07XG59XG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcoe1xuICAgIHBsdWdpbnM6IFtyZWFjdCgpLCBleHByZXNzUGx1Z2luKCksIGNvcHlOZXRsaWZ5RmlsZXMoKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsYUFBYSxRQUFRLHlCQUF5QixjQUFjLFFBQVEsS0FBSyxXQUFXO0FBQzFXLFNBQVMsbUNBQW1DO0FBQzVDLFNBQVMsTUFBTSxjQUFjO0FBa0I3QixTQUFTLGlCQUFpQjtBQUN4QixNQUFJLENBQUMsYUFBYTtBQUNoQixVQUFNLFNBQVMsUUFBUSxJQUFJO0FBQzNCLFVBQU0sU0FBUyxRQUFRLElBQUksaUJBQWlCO0FBQzVDLFVBQU0sV0FBVyxRQUFRLElBQUksbUJBQW1CO0FBQ2hELFVBQU0sT0FBTyxRQUFRLElBQUksZUFBZTtBQUV4QyxZQUFRLElBQUkscUNBQXFDO0FBQ2pELFlBQVEsSUFBSSxlQUFlLFFB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LE1BQU0sUUFBUSxTQUFTLENBQUM7QUFBQSxNQUM1RjtBQUFBLElBQ0o7QUFBQSxFQUNKO0FBQ0o7QUFDQSxJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUN4QixTQUFTLENBQUMsTUFBTSxHQUFHLGNBQWMsR0FBRyxpQkFBaUIsQ0FBQztBQUFBLEVBQ3RELFNBQVM7QUFBQSxJQUNMLE9BQU87QUFBQSxNQUNILEtBQUtMLE1BQUssUUFBUUQsWUFBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAiQkFUQ0hfU0laRSIsICJyb3V0ZXIiLCAidXVpZHY0IiwgIkdvb2dsZUdlbkFJIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJmcyIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIiwgImRvdGVudiIsICJfYSIsICJmcyJdCn0K
