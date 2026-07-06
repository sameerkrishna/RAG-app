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
import fs from "fs";
import path2 from "path";
import { fileURLToPath } from "url";
function loadGoogleCredentials() {
  const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;
  if (credentialsJson) {
    try {
      return JSON.parse(credentialsJson);
    } catch (e) {
      try {
        const decoded = Buffer.from(credentialsJson, "base64").toString("utf-8");
        return JSON.parse(decoded);
      } catch (e2) {
        console.warn("[embedding] Failed to parse GOOGLE_CREDENTIALS_JSON (tried raw and base64)");
      }
    }
  }
  const credsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (credsPath) {
    try {
      const absolutePath = path2.isAbsolute(credsPath) ? credsPath : path2.resolve(process.cwd(), credsPath);
      if (fs.existsSync(absolutePath)) {
        return JSON.parse(fs.readFileSync(absolutePath, "utf-8"));
      }
    } catch (e) {
      console.warn("[embedding] Failed to read GOOGLE_APPLICATION_CREDENTIALS:", e.message);
    }
  }
  const possiblePaths = [
    path2.resolve(__dirname, "../../google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"),
    path2.resolve(process.cwd(), "google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"),
    path2.resolve(process.cwd(), "dist/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"),
    "/var/task/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json",
    "/tmp/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"
  ];
  for (const p of possiblePaths) {
    try {
      if (fs.existsSync(p)) {
        console.log("[embedding] Found credentials at:", p);
        return JSON.parse(fs.readFileSync(p, "utf-8"));
      }
    } catch (e) {
    }
  }
  return null;
}
function createAIClient() {
  const project = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || "project-d48e2f39-2685-4746-aa0";
  const location = process.env.GOOGLE_CLOUD_LOCATION || "us-central1";
  const credentials = loadGoogleCredentials();
  if (credentials) {
    console.log("[embedding] Using explicit Google credentials");
    return new GoogleGenAI({
      vertexai: true,
      project,
      location,
      credentials
    });
  }
  console.log("[embedding] Using default Google auth");
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
var __vite_injected_original_import_meta_url, __filename, __dirname, SlidingWindowRateLimiter, TPM_LIMIT, RATE_LIMITER, RETRY_BASE_DELAY_MS, RETRY_MAX_DELAY_MS, MAX_RETRY_ATTEMPTS, ai;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    __vite_injected_original_import_meta_url = "file:///home/project/server/services/embeddingService.js";
    __filename = fileURLToPath(__vite_injected_original_import_meta_url);
    __dirname = path2.dirname(__filename);
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
import path3 from "path";
import fs2 from "fs";
import { v4 as uuidv43 } from "file:///home/project/node_modules/uuid/wrapper.mjs";
import { createHash } from "crypto";
import pdf from "file:///home/project/node_modules/pdf-parse/index.js";
import { fileURLToPath as fileURLToPath2 } from "url";
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
    const buffer = fs2.readFileSync(filePath);
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
      fs2.unlinkSync(file.path);
      sseEvent(res, "error", { message: `Maximum ${maxPDFs} uploads reached`, code: "TOO_MANY_PDFS" });
      return res.end();
    }
    if (session.documents.some((d) => d.filename === cleanFilename)) {
      fs2.unlinkSync(file.path);
      sseEvent(res, "error", { message: `"${cleanFilename}" already uploaded`, code: "DUPLICATE_FILE" });
      return res.end();
    }
    console.log(`[upload] [${sessionId}] Phase 1 \u2014 parsing ${cleanFilename} (${file.size} bytes)`);
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.path);
    if (!fullText || fullText.trim().length < 50) {
      fs2.unlinkSync(file.path);
      sseEvent(res, "error", { message: "No extractable text \u2014 PDF may be scanned or image-only", code: "EMPTY_PDF" });
      return res.end();
    }
    const documentId = uuidv43();
    const rawChunks = chunkText(fullText);
    if (rawChunks.length === 0) {
      fs2.unlinkSync(file.path);
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
    if (req.file && fs2.existsSync(req.file.path)) {
      try {
        fs2.unlinkSync(req.file.path);
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
      const filePath = path3.join(uploadDir, filename);
      if (fs2.existsSync(filePath)) {
        fs2.unlinkSync(filePath);
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
      const uploadPath = path3.join(uploadDir, filename);
      if (fs2.existsSync(uploadPath)) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", contentDisposition(filename));
        return fs2.createReadStream(uploadPath).pipe(res);
      }
      const seedPath = path3.join(seedDir, filename);
      if (fs2.existsSync(seedPath)) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", contentDisposition(filename));
        return fs2.createReadStream(seedPath).pipe(res);
      }
      if (fs2.existsSync(seedDir)) {
        const allPdfs = fs2.readdirSync(seedDir).filter((f) => f.endsWith(".pdf"));
        const match = allPdfs.find((f) => f.includes(path3.parse(filename).name));
        if (match) {
          const matchPath = path3.join(seedDir, match);
          res.setHeader("Content-Type", "application/pdf");
          res.setHeader("Content-Disposition", contentDisposition(match));
          return fs2.createReadStream(matchPath).pipe(res);
        }
      }
    }
    return res.status(404).json({ error: "Document file not found", code: "FILE_NOT_FOUND" });
  } catch (error) {
    console.error("Get document file error:", error);
    res.status(500).json({ error: "Failed to retrieve document", code: "RETRIEVE_ERROR" });
  }
}
var __vite_injected_original_import_meta_url2, router2, __filename2, __dirname2, uploadDir, seedDir, storage, upload, documents_default;
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
    __vite_injected_original_import_meta_url2 = "file:///home/project/server/api/documents.js";
    router2 = Router2();
    __filename2 = fileURLToPath2(__vite_injected_original_import_meta_url2);
    __dirname2 = path3.dirname(__filename2);
    uploadDir = "/tmp/uploads";
    if (!fs2.existsSync(uploadDir)) {
      fs2.mkdirSync(uploadDir, { recursive: true });
    }
    seedDir = path3.resolve(__dirname2, "../../seed_documents");
    if (!fs2.existsSync(seedDir)) {
      seedDir = path3.resolve(process.cwd(), "seed_documents");
    }
    if (!fs2.existsSync(seedDir)) {
      seedDir = path3.resolve(process.cwd(), "dist/seed_documents");
    }
    storage = multer.diskStorage({
      destination: (req, file, cb) => cb(null, uploadDir),
      filename: (req, file, cb) => cb(null, sanitizeFilename(file.originalname))
    });
    upload = multer({
      storage,
      limits: { fileSize: parseInt(process.env.MAX_UPLOAD_SIZE_MB || "5") * 1024 * 1024 },
      fileFilter: (req, file, cb) => {
        if (file.mimetype === "application/pdf" && path3.extname(file.originalname).toLowerCase() === ".pdf") {
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
import fs3 from "fs";
import path4 from "path";
import { fileURLToPath as fileURLToPath3 } from "url";
function loadGoogleCredentials2() {
  const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;
  if (credentialsJson) {
    try {
      return JSON.parse(credentialsJson);
    } catch (e) {
      try {
        const decoded = Buffer.from(credentialsJson, "base64").toString("utf-8");
        return JSON.parse(decoded);
      } catch (e2) {
        console.warn("[gemini] Failed to parse GOOGLE_CREDENTIALS_JSON (tried raw and base64)");
      }
    }
  }
  const credsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (credsPath) {
    try {
      const absolutePath = path4.isAbsolute(credsPath) ? credsPath : path4.resolve(process.cwd(), credsPath);
      if (fs3.existsSync(absolutePath)) {
        return JSON.parse(fs3.readFileSync(absolutePath, "utf-8"));
      }
    } catch (e) {
      console.warn("[gemini] Failed to read GOOGLE_APPLICATION_CREDENTIALS:", e.message);
    }
  }
  const possiblePaths = [
    path4.resolve(__dirname3, "../../google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"),
    path4.resolve(process.cwd(), "google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"),
    path4.resolve(process.cwd(), "dist/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"),
    "/var/task/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json",
    "/tmp/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json"
  ];
  for (const p of possiblePaths) {
    try {
      if (fs3.existsSync(p)) {
        console.log("[gemini] Found credentials at:", p);
        return JSON.parse(fs3.readFileSync(p, "utf-8"));
      }
    } catch (e) {
    }
  }
  return null;
}
function getGenAI() {
  if (!genAI) {
    const project = process.env.GOOGLE_CLOUD_PROJECT || "project-d48e2f39-2685-4746-aa0";
    const location = "global";
    const credentials = loadGoogleCredentials2();
    if (credentials) {
      console.log("[gemini] Using explicit Google credentials");
      genAI = new GoogleGenAI2({
        vertexai: true,
        project,
        location,
        credentials
      });
    } else {
      console.log("[gemini] Using default Google auth");
      genAI = new GoogleGenAI2({
        vertexai: true,
        project,
        location
      });
    }
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
var __vite_injected_original_import_meta_url3, __filename3, __dirname3, genAI, PRIMARY_MODEL, FALLBACK_MODEL, FIRST_TOKEN_TIMEOUT, REQUEST_TIMEOUT;
var init_geminiService = __esm({
  "server/services/geminiService.js"() {
    "use strict";
    init_errors();
    __vite_injected_original_import_meta_url3 = "file:///home/project/server/services/geminiService.js";
    __filename3 = fileURLToPath3(__vite_injected_original_import_meta_url3);
    __dirname3 = path4.dirname(__filename3);
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
import path5 from "path";
import { fileURLToPath as fileURLToPath4 } from "url";
import fs4 from "fs";
var __vite_injected_original_import_meta_url4 = "file:///home/project/vite.config.js";
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
var __dirname4 = path5.dirname(fileURLToPath4(__vite_injected_original_import_meta_url4));
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
      var redirectsSrc = path5.resolve(__dirname4, "dist/_redirects");
      if (fs4.existsSync(redirectsSrc)) {
        console.log("\u2705 _redirects exists in dist");
      }
      var netlifyToml = path5.resolve(__dirname4, "netlify.toml");
      var netlifyTomlDest = path5.resolve(__dirname4, "dist/netlify.toml");
      if (fs4.existsSync(netlifyToml)) {
        fs4.copyFileSync(netlifyToml, netlifyTomlDest);
        console.log("\u2705 netlify.toml copied to dist");
      }
      var seedSrc = path5.resolve(__dirname4, "seed_documents");
      var seedDest = path5.resolve(__dirname4, "dist/seed_documents");
      if (fs4.existsSync(seedSrc)) {
        fs4.mkdirSync(seedDest, { recursive: true });
        var files = fs4.readdirSync(seedSrc);
        files.forEach(function(file) {
          var srcFile = path5.join(seedSrc, file);
          var destFile = path5.join(seedDest, file);
          if (fs4.statSync(srcFile).isFile()) {
            fs4.copyFileSync(srcFile, destFile);
          }
        });
        console.log("\u2705 seed_documents copied to dist (".concat(files.length, " files)"));
      }
      var credsSrc = path5.resolve(__dirname4, "google_credentials");
      var credsDest = path5.resolve(__dirname4, "dist/google_credentials");
      if (fs4.existsSync(credsSrc)) {
        fs4.mkdirSync(credsDest, { recursive: true });
        var files = fs4.readdirSync(credsSrc);
        files.forEach(function(file) {
          var srcFile = path5.join(credsSrc, file);
          var destFile = path5.join(credsDest, file);
          if (fs4.statSync(srcFile).isFile()) {
            fs4.copyFileSync(srcFile, destFile);
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
      "@": path5.resolve(__dirname4, "./src")
    }
  },
  server: {
    port: 5173
  }
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9oZWFsdGguanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanMiLCAic2VydmVyL2FwaS9jaGF0LmpzIiwgInNlcnZlci9hcGkvZmVlZGJhY2suanMiLCAic2VydmVyL2FwcC5qcyIsICJ2aXRlLmNvbmZpZy5qcyJdLAogICJzb3VyY2VzQ29udGVudCI6IFsiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanNcIjtpbXBvcnQgeyBDbG91ZENsaWVudCwgU2NoZW1hLCBTcGFyc2VWZWN0b3JJbmRleENvbmZpZywgRE9DVU1FTlRfS0VZLCBTZWFyY2gsIEtubiwgUnJmIH0gZnJvbSAnY2hyb21hZGInO1xuaW1wb3J0IHsgQ2hyb21hQm0yNUVtYmVkZGluZ0Z1bmN0aW9uIH0gZnJvbSAnQGNocm9tYS1jb3JlL2Nocm9tYS1ibTI1JztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCBCQVRDSF9TSVpFID0gMzAwO1xuXG4vLyBcdTI1MDBcdTI1MDAgU2hhcmVkIHNjaGVtYTogZGVuc2UgZW1iZWRkaW5ncyAobWFuYWdlZCBleHRlcm5hbGx5KSArIEJNMjUgc3BhcnNlIGluZGV4IFx1MjUwMFx1MjUwMFxuY29uc3QgYm0yNUVtYmVkZGluZ0Z1bmN0aW9uID0gbmV3IENocm9tYUJtMjVFbWJlZGRpbmdGdW5jdGlvbigpO1xuY29uc3QgY29sbGVjdGlvblNjaGVtYSA9IG5ldyBTY2hlbWEoKS5jcmVhdGVJbmRleChcbiAgbmV3IFNwYXJzZVZlY3RvckluZGV4Q29uZmlnKHtcbiAgICBlbWJlZGRpbmdGdW5jdGlvbjogYm0yNUVtYmVkZGluZ0Z1bmN0aW9uLFxuICAgIHNvdXJjZUtleTogRE9DVU1FTlRfS0VZLFxuICAgIGJtMjU6IHRydWVcbiAgfSksXG4gICdzcGFyc2VfYm0yNSdcbik7XG5cbmxldCBjbG91ZENsaWVudCA9IG51bGw7XG5sZXQgZ2xvYmFsQ29sbGVjdGlvbiA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldENsb3VkQ2xpZW50KCkge1xuICBpZiAoIWNsb3VkQ2xpZW50KSB7XG4gICAgY29uc3QgYXBpS2V5ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0FQSV9LRVk7XG4gICAgY29uc3QgdGVuYW50ID0gcHJvY2Vzcy5lbnYuQ0hST01BX1RFTkFOVCB8fCAnZGVmYXVsdF90ZW5hbnQnO1xuICAgIGNvbnN0IGRhdGFiYXNlID0gcHJvY2Vzcy5lbnYuQ0hST01BX0RBVEFCQVNFIHx8ICdkZWZhdWx0X2RhdGFiYXNlJztcbiAgICBjb25zdCBob3N0ID0gcHJvY2Vzcy5lbnYuQ0hST01BX0hPU1QgfHwgdW5kZWZpbmVkO1xuXG4gICAgY29uc29sZS5sb2coXCItLS0tIENIUk9NQSBDT05ORUNUSVZJVFkgREVCVUcgLS0tLVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIkhvc3Q6ICAgICAgXCIsIGhvc3QgfHwgXCJhcGkudHJ5Y2hyb21hLmNvbSAoZGVmYXVsdClcIik7XG4gICAgY29uc29sZS5sb2coXCJUZW5hbnQ6ICAgIFwiLCB0ZW5hbnQpO1xuICAgIGNvbnNvbGUubG9nKFwiREIgTmFtZTogICBcIiwgZGF0YWJhc2UpO1xuICAgIGNvbnNvbGUubG9nKFwiQVBJIEtleTogICBcIiwgYXBpS2V5ID8gXCJMT0FERUQgKFZBTElEKVwiIDogXCJNSVNTSU5HIChVTkRFRklORUQpXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cIik7XG5cbiAgICBpZiAoIWFwaUtleSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBcIkNSSVRJQ0FMIEVSUk9SOiBDSFJPTUFfQVBJX0tFWSBpcyB1bmRlZmluZWQuIFwiICtcbiAgICAgICAgXCJFbnN1cmUgeW91ciBlbnZpcm9ubWVudCB2YXJpYWJsZXMgYXJlIGNvcnJlY3RseSBsb2FkZWQgYmVmb3JlIGV4ZWN1dGluZyB0aGlzIGZpbGUuXCJcbiAgICAgICk7XG4gICAgfVxuXG4gICAgY29uc3QgY2xpZW50T3B0aW9ucyA9IHsgYXBpS2V5LCB0ZW5hbnQsIGRhdGFiYXNlIH07XG4gICAgaWYgKGhvc3QpIGNsaWVudE9wdGlvbnMuaG9zdCA9IGhvc3Q7XG4gICAgY2xvdWRDbGllbnQgPSBuZXcgQ2xvdWRDbGllbnQoY2xpZW50T3B0aW9ucyk7XG4gIH1cbiAgcmV0dXJuIGNsb3VkQ2xpZW50O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0R2xvYmFsQ29sbGVjdGlvbigpIHtcbiAgaWYgKCFnbG9iYWxDb2xsZWN0aW9uKSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IHByb2Nlc3MuZW52LkNIUk9NQV9HTE9CQUxfQ09MTEVDVElPTiB8fCAnc2VlZF9kYic7XG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIHNjaGVtYTogY29sbGVjdGlvblNjaGVtYSxcbiAgICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgICBkZXNjcmlwdGlvbjogJ1Blcm1hbmVudCBzZWVkIGRvY3VtZW50cyBmb3IgUkFHJyxcbiAgICAgICAgICB0eXBlOiAnZ2xvYmFsX2tub3dsZWRnZSdcbiAgICAgICAgfSxcbiAgICAgICAgZW1iZWRkaW5nRnVuY3Rpb246IG51bGxcbiAgICAgIH0pO1xuICAgICAgY29uc29sZS5sb2coYFxcdTI3MDUgR2xvYmFsIGNvbGxlY3Rpb24gcmVhZHk6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjb25uZWN0IHRvIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHRoZSBzaW5nbGUgc2hhcmVkIGNvbGxlY3Rpb24uXG4gKiBEcm9wLWluIHJlcGxhY2VtZW50IGZvciB0aGUgb2xkIGdldFNlc3Npb25Db2xsZWN0aW9uIFx1MjAxNCBjYWxsZXJzIHRoYXRcbiAqIHByZXZpb3VzbHkgZGVzdHJ1Y3R1cmVkIHsgY29sbGVjdGlvbiB9IHdpbGwgc3RpbGwgd29yay5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldENvbGxlY3Rpb24oKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRHbG9iYWxDb2xsZWN0aW9uKCk7XG4gIHJldHVybiB7IGNvbGxlY3Rpb24sIGlzTmV3OiBmYWxzZSB9O1xufVxuXG4vKipcbiAqIEFkZCB2ZWN0b3JzIGluIGJhdGNoZXMgb2YgQkFUQ0hfU0laRSB0byBhdm9pZCBDaHJvbWEgcGF5bG9hZCBsaW1pdHMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBhZGRWZWN0b3JzKGNvbGxlY3Rpb24sIHZlY3RvcnMsIGVtYmVkZGluZ3MsIGlkcykge1xuICB0cnkge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgaWRzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSB7XG4gICAgICBjb25zdCBiYXRjaElkcyA9IGlkcy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCBiYXRjaEVtYmVkZGluZ3MgPSBlbWJlZGRpbmdzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKTtcbiAgICAgIGNvbnN0IGJhdGNoRG9jdW1lbnRzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi50ZXh0KTtcbiAgICAgIGNvbnN0IGJhdGNoTWV0YWRhdGFzID0gdmVjdG9ycy5zbGljZShpLCBpICsgQkFUQ0hfU0laRSkubWFwKHYgPT4gdi5tZXRhZGF0YSk7XG5cbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uYWRkKHtcbiAgICAgICAgaWRzOiBiYXRjaElkcyxcbiAgICAgICAgZW1iZWRkaW5nczogYmF0Y2hFbWJlZGRpbmdzLFxuICAgICAgICBkb2N1bWVudHM6IGJhdGNoRG9jdW1lbnRzLFxuICAgICAgICBtZXRhZGF0YXM6IGJhdGNoTWV0YWRhdGFzXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGAgIFthZGRWZWN0b3JzXSBiYXRjaCAke01hdGguZmxvb3IoaSAvIEJBVENIX1NJWkUpICsgMX06IGFkZGVkICR7YmF0Y2hJZHMubGVuZ3RofSB2ZWN0b3JzYCk7XG4gICAgfVxuICAgIHJldHVybiB0cnVlO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBhZGQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSyA9IDUsIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcXVlcnlPcHRzID0ge1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9O1xuICAgIGlmICh3aGVyZSkgcXVlcnlPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgY29sbGVjdGlvbi5xdWVyeShxdWVyeU9wdHMpO1xuXG4gICAgaWYgKCFyZXN1bHRzLmlkcyB8fCByZXN1bHRzLmlkcy5sZW5ndGggPT09IDAgfHwgcmVzdWx0cy5pZHNbMF0ubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgcmV0dXJuIHJlc3VsdHMuaWRzWzBdLm1hcCgoaWQsIGlkeCkgPT4gKHtcbiAgICAgIGlkLFxuICAgICAgdGV4dDogcmVzdWx0cy5kb2N1bWVudHNbMF1baWR4XSxcbiAgICAgIG1ldGFkYXRhOiByZXN1bHRzLm1ldGFkYXRhc1swXVtpZHhdLFxuICAgICAgZGlzdGFuY2U6IHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF0sXG4gICAgICBzY29yZTogMSAtIHJlc3VsdHMuZGlzdGFuY2VzWzBdW2lkeF1cbiAgICB9KSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIHF1ZXJ5IGNvbGxlY3Rpb246JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogSHlicmlkIHNlYXJjaCB1c2luZyBDaHJvbWEgQ2xvdWQgU2VhcmNoIEFQSSB3aXRoIFJSRiAoZGVuc2UgKyBzcGFyc2UgQk0yNSkuXG4gKiBSZXR1cm5zIHJlc3VsdHMgaW4gdGhlIHNhbWUgc2hhcGUgYXMgcXVlcnlDb2xsZWN0aW9uKCkgZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHkuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcgKGUuZy4gc2Vzc2lvbl9pZCAkaW4pLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaHlicmlkUXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5VGV4dCwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1LCB3aGVyZSA9IHVuZGVmaW5lZCkge1xuICB0cnkge1xuICAgIGxldCBzZWFyY2ggPSBuZXcgU2VhcmNoKClcbiAgICAgIC5yYW5rKFJyZih7XG4gICAgICAgIHJhbmtzOiBbXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5RW1iZWRkaW5nLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMjAgfSksXG4gICAgICAgICAgS25uKHsgcXVlcnk6IHF1ZXJ5VGV4dCwga2V5OiAnc3BhcnNlX2JtMjUnLCByZXR1cm5SYW5rOiB0cnVlLCBsaW1pdDogMjAgfSlcbiAgICAgICAgXSxcbiAgICAgICAgd2VpZ2h0czogWzAuOSwgMC4xXSxcbiAgICAgICAgazogNjBcbiAgICAgIH0pKVxuICAgICAgLndoZXJlKHdoZXJlKVxuICAgICAgLnNlbGVjdChcIiNkb2N1bWVudFwiLCBcIiNtZXRhZGF0YVwiLCBcIiNzY29yZVwiKVxuICAgICAgLmxpbWl0KHRvcEspO1xuXG4gICAgY29uc3QgcmF3ID0gYXdhaXQgY29sbGVjdGlvbi5zZWFyY2goc2VhcmNoKTtcblxuICAgIC8vIFBhcmFsbGVsXHUyMDExYXJyYXkgc3RydWN0dXJlOiBpZHNbMF0sIGRvY3VtZW50c1swXSwgbWV0YWRhdGFzWzBdLCBzY29yZXNbMF1cbiAgICBpZiAoIXJhdy5pZHMgfHwgIXJhdy5pZHNbMF0gfHwgcmF3Lmlkc1swXS5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICBjb25zdCBpZHMgPSByYXcuaWRzWzBdO1xuICAgIGNvbnN0IGRvY3MgPSByYXcuZG9jdW1lbnRzPy5bMF0gPz8gW107XG4gICAgY29uc3QgbWV0YXMgPSByYXcubWV0YWRhdGFzPy5bMF0gPz8gW107XG4gICAgY29uc3Qgc2NvcmVzID0gcmF3LnNjb3Jlcz8uWzBdID8/IFtdO1xuXG4gICAgLy8gMS4gRGVmaW5lIGdsb2JhbCBSUkYgYm91bmRzIGJhc2VkIG9uIHlvdXIgd2VpZ2h0cyBbMC43LCAwLjNdIGFuZCBsaW1pdHMgKDEwMClcbiAgICAvLyBNYXggcG9zc2libGUgcmF3IFJSRjogMSAvICg2MCArIDEpID0gMC4wMTYzOTM0XG4gICAgLy8gTWluIHBvc3NpYmxlIHJhdyBSUkY6IDEgLyAoNjAgKyAxMDApID0gMC4wMDYyNTAwXG4gICAgY29uc3QgTUFYX1JSRiA9IDEgLyA2MTtcbiAgICBjb25zdCBNSU5fUlJGID0gMSAvIDE2MDtcblxuICAgIHJldHVybiBpZHMubWFwKChpZCwgaWR4KSA9PiB7XG4gICAgICAvLyBDaHJvbWEgcmV0dXJucyBuZWdhdGl2ZSB2YWx1ZXMgKGUuZy4gLTAuMDE2MzkpLCBjb252ZXJ0IHRvIHBvc2l0aXZlIHJhdyBSUkZcbiAgICAgIGNvbnN0IHJhd1JSRiA9IE1hdGguYWJzKHNjb3Jlc1tpZHhdID8/IE1JTl9SUkYpO1xuXG4gICAgICAvLyAyLiBMaW5lYXIgbWluLW1heCBub3JtYWxpemF0aW9uIHRvIGZpdCBwZXJmZWN0bHkgYmV0d2VlbiAwLjAgYW5kIDEuMFxuICAgICAgbGV0IG5vcm1hbGl6ZWRTY29yZSA9IChyYXdSUkYgLSBNSU5fUlJGKSAvIChNQVhfUlJGIC0gTUlOX1JSRik7XG5cbiAgICAgIC8vIEJvdW5kYXJ5IHByb3RlY3Rpb25cbiAgICAgIG5vcm1hbGl6ZWRTY29yZSA9IE1hdGgubWF4KDAsIE1hdGgubWluKDEsIG5vcm1hbGl6ZWRTY29yZSkpO1xuXG4gICAgICAvL2NvbnN0IGZpbmFsU2NvcmUgPSBNYXRoLnJvdW5kKG5vcm1hbGl6ZWRTY29yZSAqIDEwMCkgLyAxMDA7XG5cbiAgICAgIHJldHVybiB7XG4gICAgICAgIGlkLFxuICAgICAgICB0ZXh0OiBkb2NzW2lkeF0gPz8gJycsXG4gICAgICAgIG1ldGFkYXRhOiBtZXRhc1tpZHhdID8/IHt9LFxuICAgICAgICBkaXN0YW5jZTogMSAtIG5vcm1hbGl6ZWRTY29yZSxcbiAgICAgICAgc2NvcmU6IG5vcm1hbGl6ZWRTY29yZVxuICAgICAgfTtcbiAgICB9KTtcblxuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignSHlicmlkIHF1ZXJ5IGZhaWxlZCwgZmFsbGluZyBiYWNrIHRvIGRlbnNlLW9ubHk6JywgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gR3JhY2VmdWwgZmFsbGJhY2sgdG8gZGVuc2Utb25seSBzZWFyY2ggZm9yIGJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICByZXR1cm4gcXVlcnlDb2xsZWN0aW9uKGNvbGxlY3Rpb24sIHF1ZXJ5RW1iZWRkaW5nLCB0b3BLLCB3aGVyZSk7XG4gIH1cbn1cblxuLyoqXG4gKiBEZWxldGUgYWxsIHZlY3RvcnMgZm9yIGEgZ2l2ZW4gZG9jdW1lbnRJZC5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIGluIEJBVENIX1NJWkUgY2h1bmtzIHNvIGRvY3VtZW50cyB3aXRoXG4gKiBtYW55IGNodW5rcyAoPmRlZmF1bHQgMTAwIGxpbWl0KSBhcmUgZnVsbHkgZGVsZXRlZC5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYWxsSWRzID0gW107XG4gICAgbGV0IG9mZnNldCA9IDA7XG5cbiAgICB3aGlsZSAodHJ1ZSkge1xuICAgICAgY29uc3QgYmF0Y2ggPSBhd2FpdCBjb2xsZWN0aW9uLmdldCh7XG4gICAgICAgIHdoZXJlOiB7IGRvY3VtZW50X2lkOiBkb2N1bWVudElkIH0sXG4gICAgICAgIGluY2x1ZGU6IFtdLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9KTtcblxuICAgICAgaWYgKCFiYXRjaC5pZHMgfHwgYmF0Y2guaWRzLmxlbmd0aCA9PT0gMCkgYnJlYWs7XG4gICAgICBhbGxJZHMucHVzaCguLi5iYXRjaC5pZHMpO1xuXG4gICAgICBpZiAoYmF0Y2guaWRzLmxlbmd0aCA8IEJBVENIX1NJWkUpIGJyZWFrO1xuICAgICAgb2Zmc2V0ICs9IEJBVENIX1NJWkU7XG4gICAgfVxuXG4gICAgaWYgKGFsbElkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7IGlkczogYWxsSWRzIH0pO1xuICAgIH1cbiAgICByZXR1cm4gYWxsSWRzLmxlbmd0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50IHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbi8qKlxuICogRGVsZXRlIGFsbCB2ZWN0b3JzIGJlbG9uZ2luZyB0byBhIHNwZWNpZmljIHNlc3Npb24uXG4gKiBVc2VzIHNlc3Npb25faWQgbWV0YWRhdGEgZmlsdGVyIHRvIGZpbmQgYW5kIHJlbW92ZSB0aGVtIGluIGJhdGNoZXMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVTZXNzaW9uVmVjdG9ycyhzZXNzaW9uSWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuICAgIGNvbnN0IGFsbElkcyA9IFtdO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgICB3aGVyZTogeyBzZXNzaW9uX2lkOiBzZXNzaW9uSWQgfSxcbiAgICAgICAgaW5jbHVkZTogW10sXG4gICAgICAgIGxpbWl0OiBCQVRDSF9TSVpFLFxuICAgICAgICBvZmZzZXRcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoIWJhdGNoLmlkcyB8fCBiYXRjaC5pZHMubGVuZ3RoID09PSAwKSBicmVhaztcbiAgICAgIGFsbElkcy5wdXNoKC4uLmJhdGNoLmlkcyk7XG5cbiAgICAgIGlmIChiYXRjaC5pZHMubGVuZ3RoIDwgQkFUQ0hfU0laRSkgYnJlYWs7XG4gICAgICBvZmZzZXQgKz0gQkFUQ0hfU0laRTtcbiAgICB9XG5cbiAgICBpZiAoYWxsSWRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGF3YWl0IGNvbGxlY3Rpb24uZGVsZXRlKHsgaWRzOiBhbGxJZHMgfSk7XG4gICAgfVxuICAgIGNvbnNvbGUubG9nKGBcXHUyNzA1IERlbGV0ZWQgJHthbGxJZHMubGVuZ3RofSBzZXNzaW9uIHZlY3RvcnMgZm9yIHNlc3Npb25faWQ9JHtzZXNzaW9uSWR9YCk7XG4gICAgcmV0dXJuIGFsbElkcy5sZW5ndGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGRlbGV0ZSBzZXNzaW9uIHZlY3RvcnMgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuLyoqXG4gKiBMaXN0IGFsbCB1bmlxdWUgZG9jdW1lbnRzIGluIGEgY29sbGVjdGlvbi5cbiAqIFBhZ2luYXRlcyBjb2xsZWN0aW9uLmdldCgpIHdpdGggQkFUQ0hfU0laRT0zMDAgc28gY29sbGVjdGlvbnMgbGFyZ2VyXG4gKiB0aGFuIENocm9tYSdzIGRlZmF1bHQgZ2V0KCkgbGltaXQgKDEwMCkgYXJlIGZ1bGx5IGVudW1lcmF0ZWQuXG4gKiBBY2NlcHRzIGFuIG9wdGlvbmFsIGB3aGVyZWAgY2xhdXNlIGZvciBtZXRhZGF0YSBmaWx0ZXJpbmcuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzKGNvbGxlY3Rpb24sIHdoZXJlID0gdW5kZWZpbmVkKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgZG9jdW1lbnRzTWFwID0gbmV3IE1hcCgpO1xuICAgIGxldCBvZmZzZXQgPSAwO1xuXG4gICAgd2hpbGUgKHRydWUpIHtcbiAgICAgIGNvbnN0IGdldE9wdHMgPSB7XG4gICAgICAgIGluY2x1ZGU6IFsnbWV0YWRhdGFzJywgJ2RvY3VtZW50cyddLFxuICAgICAgICBsaW1pdDogQkFUQ0hfU0laRSxcbiAgICAgICAgb2Zmc2V0XG4gICAgICB9O1xuICAgICAgaWYgKHdoZXJlKSBnZXRPcHRzLndoZXJlID0gd2hlcmU7XG5cbiAgICAgIGNvbnN0IGJhdGNoID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoZ2V0T3B0cyk7XG5cbiAgICAgIGlmICghYmF0Y2guaWRzIHx8IGJhdGNoLmlkcy5sZW5ndGggPT09IDApIGJyZWFrO1xuXG4gICAgICBiYXRjaC5pZHMuZm9yRWFjaCgoaWQsIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCBtZXRhID0gYmF0Y2gubWV0YWRhdGFzW2lkeF07XG4gICAgICAgIGNvbnN0IGRvY0lkID0gbWV0YS5kb2N1bWVudF9pZDtcblxuICAgICAgICBpZiAoIWRvY3VtZW50c01hcC5oYXMoZG9jSWQpKSB7XG4gICAgICAgICAgZG9jdW1lbnRzTWFwLnNldChkb2NJZCwge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY0lkLFxuICAgICAgICAgICAgZmlsZW5hbWU6IG1ldGEuZmlsZW5hbWUsXG4gICAgICAgICAgICBjaHVua19jb3VudDogMCxcbiAgICAgICAgICAgIHBhZ2VfY291bnQ6IG1ldGEucGFnZV9udW1iZXIgfHwgMSxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcCxcbiAgICAgICAgICAgIHNvdXJjZV90eXBlOiBtZXRhLnNvdXJjZV90eXBlLFxuICAgICAgICAgICAgZmlyc3RfY2h1bmtfdGV4dDogYmF0Y2guZG9jdW1lbnRzW2lkeF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGRvYyA9IGRvY3VtZW50c01hcC5nZXQoZG9jSWQpO1xuICAgICAgICBkb2MuY2h1bmtfY291bnQrKztcbiAgICAgICAgZG9jLnBhZ2VfY291bnQgPSBNYXRoLm1heChkb2MucGFnZV9jb3VudCwgbWV0YS5wYWdlX251bWJlciB8fCAxKTtcbiAgICAgIH0pO1xuXG4gICAgICBjb25zb2xlLmxvZyhgICBbbGlzdERvY3VtZW50c10gb2Zmc2V0PSR7b2Zmc2V0fSwgZ290PSR7YmF0Y2guaWRzLmxlbmd0aH0sIHVuaXF1ZSBzbyBmYXI9JHtkb2N1bWVudHNNYXAuc2l6ZX1gKTtcblxuICAgICAgaWYgKGJhdGNoLmlkcy5sZW5ndGggPCBCQVRDSF9TSVpFKSBicmVhaztcbiAgICAgIG9mZnNldCArPSBCQVRDSF9TSVpFO1xuICAgIH1cblxuICAgIHJldHVybiBBcnJheS5mcm9tKGRvY3VtZW50c01hcC52YWx1ZXMoKSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzOicsIGVycm9yKTtcbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aENoZWNrKCkge1xuICB0cnkge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgaGVhcnRiZWF0ID0gYXdhaXQgY2xpZW50LmhlYXJ0YmVhdCgpO1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICdoZWFsdGh5JyxcbiAgICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgaGVhcnRiZWF0XG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXR1cm4ge1xuICAgICAgc3RhdHVzOiAndW5oZWFsdGh5JyxcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICB9O1xuICB9XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvaGVhbHRoLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyBoZWFsdGhDaGVjayBhcyBjaHJvbWFIZWFsdGhDaGVjayB9IGZyb20gJy4uL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhlYWx0aChyZXEsIHJlcykge1xuICBjb25zdCBoZWFsdGhTdGF0dXMgPSB7XG4gICAgc3RhdHVzOiAnb2snLFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgIHNlcnZpY2VzOiB7fVxuICB9O1xuXG4gIC8vIENoZWNrIENocm9tYURCXG4gIHRyeSB7XG4gICAgY29uc3QgY2hyb21hSGVhbHRoID0gYXdhaXQgY2hyb21hSGVhbHRoQ2hlY2soKTtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSBjaHJvbWFIZWFsdGg7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgaGVhbHRoU3RhdHVzLnNlcnZpY2VzLmNocm9tYWRiID0ge1xuICAgICAgc3RhdHVzOiAnZXJyb3InLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2VcbiAgICB9O1xuICB9XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENvdmVyYWdlVG9vTG93RXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdJbnN1ZmZpY2llbnQgaW5mb3JtYXRpb24gaW4ga25vd2xlZGdlIGJhc2UnLCAnQ09WRVJBR0VfVE9PX0xPVycsIDIwMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzUmV0cnlhYmxlRXJyb3IoZXJyb3IpIHtcbiAgY29uc3QgcmV0cnlhYmxlQ29kZXMgPSBbJ1JBVEVfTElNSVRfRVhDRUVERUQnLCAnRU1CRURESU5HX0VSUk9SJywgJ0xMTV9VTkFWQUlMQUJMRSddO1xuICByZXR1cm4gcmV0cnlhYmxlQ29kZXMuaW5jbHVkZXMoZXJyb3IuY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpczQyOUVycm9yKGVycm9yKSB7XG4gIHJldHVybiBlcnJvcj8uY29kZSA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8uc3RhdHVzID09PSA0MjkgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnNDI5JykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnUkVTT1VSQ0VfRVhIQVVTVEVEJykgfHxcbiAgICAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnVG9vIE1hbnkgUmVxdWVzdHMnKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL3Nhbml0aXplLmpzXCI7aW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBWYWxpZGF0aW9uRXJyb3IgfSBmcm9tICcuL2Vycm9ycy5qcyc7XG5cbmNvbnN0IERBTkdFUk9VU19QQVRURVJOUyA9IC9bPD46XCJ8PypcXHgwMC1cXHgxZl0vZztcbmNvbnN0IFBBVEhfVFJBVkVSU0FMID0gL1xcLlxcLi9nO1xuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVGaWxlbmFtZShmaWxlbmFtZSkge1xuICBpZiAoIWZpbGVuYW1lIHx8IHR5cGVvZiBmaWxlbmFtZSAhPT0gJ3N0cmluZycpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGZpbGVuYW1lJyk7XG4gIH1cblxuICAvLyBSZW1vdmUgcGF0aCBjb21wb25lbnRzIGFuZCBnZXQgYmFzZW5hbWVcbiAgY29uc3QgYmFzZW5hbWUgPSBwYXRoLmJhc2VuYW1lKGZpbGVuYW1lKTtcblxuICAvLyBSZW1vdmUgZGFuZ2Vyb3VzIGNoYXJhY3RlcnNcbiAgbGV0IHNhbml0aXplZCA9IGJhc2VuYW1lLnJlcGxhY2UoREFOR0VST1VTX1BBVFRFUk5TLCAnXycpO1xuXG4gIC8vIFJlbW92ZSBwYXRoIHRyYXZlcnNhbCBhdHRlbXB0c1xuICBzYW5pdGl6ZWQgPSBzYW5pdGl6ZWQucmVwbGFjZShQQVRIX1RSQVZFUlNBTCwgJycpO1xuXG4gIC8vIFRyaW0gd2hpdGVzcGFjZSBhbmQgbGltaXQgbGVuZ3RoXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC50cmltKCkuc2xpY2UoMCwgMjU1KTtcblxuICBpZiAoIXNhbml0aXplZCkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUgYWZ0ZXIgc2FuaXRpemF0aW9uJyk7XG4gIH1cblxuICByZXR1cm4gc2FuaXRpemVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVQREZGaWxlKGZpbGUpIHtcbiAgaWYgKCFmaWxlKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignTm8gZmlsZSBwcm92aWRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgTUlNRSB0eXBlXG4gIGNvbnN0IHZhbGlkTWltZVR5cGVzID0gWydhcHBsaWNhdGlvbi9wZGYnXTtcbiAgaWYgKCF2YWxpZE1pbWVUeXBlcy5pbmNsdWRlcyhmaWxlLm1pbWV0eXBlKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ09ubHkgUERGIGZpbGVzIGFyZSBhY2NlcHRlZCcpO1xuICB9XG5cbiAgLy8gQ2hlY2sgZXh0ZW5zaW9uXG4gIGNvbnN0IGV4dCA9IHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSB8fCAnJykudG9Mb3dlckNhc2UoKTtcbiAgaWYgKGV4dCAhPT0gJy5wZGYnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignRmlsZSBtdXN0IGhhdmUgLnBkZiBleHRlbnNpb24nKTtcbiAgfVxuXG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVGaWxlU2l6ZShzaXplQnl0ZXMsIG1heFNpemVNQikge1xuICBjb25zdCBtYXhCeXRlcyA9IG1heFNpemVNQiAqIDEwMjQgKiAxMDI0O1xuICBpZiAoc2l6ZUJ5dGVzID4gbWF4Qnl0ZXMpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKGBGaWxlIGV4Y2VlZHMgbWF4aW11bSBzaXplIG9mICR7bWF4U2l6ZU1CfU1CYCk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUlucHV0KGlucHV0LCBtYXhMZW5ndGggPSAxMDAwMCkge1xuICBpZiAoIWlucHV0IHx8IHR5cGVvZiBpbnB1dCAhPT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gJyc7XG4gIH1cbiAgcmV0dXJuIGlucHV0LnRyaW0oKS5zbGljZSgwLCBtYXhMZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmFsaWRhdGVEb2N1bWVudElkKGlkKSB7XG4gIGlmICghaWQgfHwgdHlwZW9mIGlkICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQnKTtcbiAgfVxuICBjb25zdCB1dWlkUmVnZXggPSAvXlswLTlhLWZdezh9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezR9LVswLTlhLWZdezEyfSQvaTtcbiAgaWYgKCF1dWlkUmVnZXgudGVzdChpZCkpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdJbnZhbGlkIGRvY3VtZW50IElEIGZvcm1hdCcpO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFRleHRGcm9tUERGQnVmZmVyKGJ1ZmZlcikge1xuICAvLyBUaGlzIHdpbGwgYmUgdXNlZCB3aXRoIHBkZi1wYXJzZVxuICByZXR1cm4gYnVmZmVyO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvY2h1bmtlci5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7aW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5cbmNvbnN0IENIQVJTX1BFUl9UT0tFTiA9IDQ7XG5jb25zdCBUQVJHRVRfQ0hVTktfVE9LRU5TID0gNjAwOyAgIC8vIHNvZnQgdGFyZ2V0IHBlciBjaHVua1xuY29uc3QgTUFYX0NIVU5LX1RPS0VOUyA9IDc1MDsgICAvLyBoYXJkIGNhcCBiZWZvcmUgZm9yY2VkIHNwbGl0XG5jb25zdCBPVkVSTEFQX1RPS0VOUyA9IDEwMDsgICAvLyBvdmVybGFwIG9ubHkgb24gb3ZlcnNpemVkIHBhcmFncmFwaHNcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuLy8gTWF0Y2hlcyBBTEwtQ0FQUyBoZWFkaW5ncywgbWFya2Rvd24gaGVhZGluZ3MsIG9yIG51bWJlcmVkIHNlY3Rpb24gaGVhZGluZ3NcbmNvbnN0IEhFQURJTkdfUkUgPSAvXig/OltBLVpdW0EtWlxcc117Miw2MH0kfCN7MSw0fVxccy4rfCg/OlxcZCtcXC4pK1xccy4rKS9tO1xuXG5leHBvcnQgZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnModGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gMDtcbiAgcmV0dXJuIE1hdGguY2VpbCh0ZXh0Lmxlbmd0aCAvIENIQVJTX1BFUl9UT0tFTik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhblRleHQodGV4dCkge1xuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gJyc7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoL1xcZi9nLCAnXFxuJylcbiAgICAucmVwbGFjZSgvKFxccypcXG4pezMsfS9nLCAnXFxuXFxuJylcbiAgICAucmVwbGFjZSgvXlxccypcXGQrXFxzKiQvZ20sICcnKVxuICAgIC5yZXBsYWNlKC9bIFxcdF17Mix9L2csICcgJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBnZW5lcmF0ZUNodW5rSWQodGV4dCwgZmlsZW5hbWUpIHtcbiAgcmV0dXJuIGNyZWF0ZUhhc2goJ21kNScpXG4gICAgLnVwZGF0ZShgJHtmaWxlbmFtZX06OiR7dGV4dH1gKVxuICAgIC5kaWdlc3QoJ2hleCcpXG4gICAgLnNsaWNlKDAsIDE2KTtcbn1cblxuLyoqXG4gKiBHaXZlbiBhIHJhdyAocG9zc2libHkgbWlkLXdvcmQpIG9mZnNldCwgc25hcCBmb3J3YXJkIHRvIHRoZSBuZWFyZXN0XG4gKiBjbGVhbiBzZW50ZW5jZSBzdGFydCwgZmFsbGluZyBiYWNrIHRvIHRoZSBuZWFyZXN0IHdvcmQgYm91bmRhcnksXG4gKiBzbyBvdmVybGFwcGVkIGNodW5rcyBuZXZlciBiZWdpbiBtaWQtc2VudGVuY2Ugb3IgbWlkLXdvcmQuXG4gKlxuICogQHBhcmFtIHtzdHJpbmd9IHRleHQgICAgICAgdGhlIHBhcmFncmFwaC90ZXh0IGJlaW5nIHdpbmRvd2VkXG4gKiBAcGFyYW0ge251bWJlcn0gcmF3T2Zmc2V0ICB0aGUgcmF3ICh1bnNuYXBwZWQpIHN0YXJ0IG9mZnNldCBmb3IgdGhlIG5leHQgd2luZG93XG4gKiBAcGFyYW0ge251bWJlcn0gaGFyZExpbWl0ICBkb24ndCBzZWFyY2ggcGFzdCB0aGlzIG9mZnNldCAoZW5kIG9mIHByZXZpb3VzIHdpbmRvdylcbiAqL1xuZnVuY3Rpb24gc25hcFRvQm91bmRhcnkodGV4dCwgcmF3T2Zmc2V0LCBoYXJkTGltaXQpIHtcbiAgaWYgKHJhd09mZnNldCA8PSAwKSByZXR1cm4gMDtcblxuICAvLyBQcmVmZXIgYSByZWFsIHNlbnRlbmNlIGJvdW5kYXJ5IHdpdGhpbiBhIHNtYWxsIGZvcndhcmQgd2luZG93XG4gIGNvbnN0IHNlYXJjaFdpbmRvd0VuZCA9IE1hdGgubWluKHJhd09mZnNldCArIDgwLCBoYXJkTGltaXQpOyAvLyB+ODAgY2hhcnMgXHUyMjQ4IG9uZSBzZW50ZW5jZVxuICBmb3IgKGNvbnN0IGJwIG9mIFsnLiAnLCAnLlxcbicsICc/ICcsICchICcsICdcXG4nXSkge1xuICAgIGNvbnN0IGlkeCA9IHRleHQuaW5kZXhPZihicCwgcmF3T2Zmc2V0KTtcbiAgICBpZiAoaWR4ICE9PSAtMSAmJiBpZHggPCBzZWFyY2hXaW5kb3dFbmQpIHtcbiAgICAgIHJldHVybiBpZHggKyBicC5sZW5ndGg7XG4gICAgfVxuICB9XG5cbiAgLy8gRmFsbCBiYWNrOiBzbmFwIHRvIHRoZSBuZXh0IHdvcmQgYm91bmRhcnkgc28gd2UgYXQgbGVhc3QgZG9uJ3RcbiAgLy8gc3BsaXQgYSB3b3JkIGluIGhhbGZcbiAgY29uc3Qgc3BhY2VJZHggPSB0ZXh0LmluZGV4T2YoJyAnLCByYXdPZmZzZXQpO1xuICBpZiAoc3BhY2VJZHggIT09IC0xICYmIHNwYWNlSWR4IDwgc2VhcmNoV2luZG93RW5kKSB7XG4gICAgcmV0dXJuIHNwYWNlSWR4ICsgMTtcbiAgfVxuXG4gIC8vIExhc3QgcmVzb3J0OiBpZiB0aGUgY3VycmVudCBwb3NpdGlvbiBpcyBhbHJlYWR5IG1pZC13b3JkLFxuICAvLyB3YWxrIGJhY2t3YXJkIHRvIHRoZSBsYXN0IHNwYWNlIGJlZm9yZSBpdFxuICBsZXQgaSA9IHJhd09mZnNldDtcbiAgd2hpbGUgKGkgPiAwICYmICEvXFxzLy50ZXN0KHRleHRbaSAtIDFdKSkgaS0tO1xuICByZXR1cm4gaSA+IDAgPyBpIDogcmF3T2Zmc2V0O1xufVxuXG4vKipcbiAqIFN0cnVjdHVyZS1hd2FyZSBjaHVua2luZzpcbiAqICAxLiBTcGxpdCBvbiBibGFuayBsaW5lcyAoXFxuXFxuKSBpbnRvIHBhcmFncmFwaHMuXG4gKiAgMi4gQSBsaW5lIG1hdGNoaW5nIEhFQURJTkdfUkUgYWx3YXlzIHN0YXJ0cyBhIGZyZXNoIGNodW5rLlxuICogIDMuIEFjY3VtdWxhdGUgcGFyYWdyYXBocyB1bnRpbCB0aGUgc29mdCBUQVJHRVQgaXMgcmVhY2hlZCwgdGhlbiBmbHVzaC5cbiAqICA0LiBQYXJhZ3JhcGhzIGxhcmdlciB0aGFuIE1BWCBhcmUgc3BsaXQgd2l0aCBhIHNsaWRpbmcgd2luZG93ICsgb3ZlcmxhcCBhcyBmYWxsYmFjay5cbiAqICAgICBCb3RoIHdpbmRvdyBlbmRzIEFORCB3aW5kb3cgc3RhcnRzIGFyZSBzbmFwcGVkIHRvIHNlbnRlbmNlL3dvcmQgYm91bmRhcmllc1xuICogICAgIHNvIG5vIGNodW5rIGJlZ2lucyBvciBlbmRzIG1pZC13b3JkLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtUZXh0KHRleHQsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB0YXJnZXRUb2tlbnMgPSBvcHRpb25zLmNodW5rU2l6ZVRva2VucyB8fCBUQVJHRVRfQ0hVTktfVE9LRU5TO1xuICBjb25zdCBtYXhUb2tlbnMgPSBvcHRpb25zLm1heENodW5rVG9rZW5zIHx8IE1BWF9DSFVOS19UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUayA9IG9wdGlvbnMub3ZlcmxhcFRva2VucyB8fCBPVkVSTEFQX1RPS0VOUztcblxuICBjb25zdCB0YXJnZXRDaGFycyA9IHRhcmdldFRva2VucyAqIENIQVJTX1BFUl9UT0tFTjtcbiAgY29uc3QgbWF4Q2hhcnMgPSBtYXhUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUayAqIENIQVJTX1BFUl9UT0tFTjtcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgLy8gMS4gU3BsaXQgaW50byBwYXJhZ3JhcGhzXG4gIGNvbnN0IHJhd1BhcmFzID0gdGV4dFxuICAgIC5zcGxpdCgvXFxuezIsfS8pXG4gICAgLm1hcChwID0+IHAudHJpbSgpKVxuICAgIC5maWx0ZXIocCA9PiBwLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpO1xuXG4gIGNvbnN0IGNodW5rcyA9IFtdO1xuICBsZXQgYnVmZmVyID0gJyc7XG4gIGxldCBidWZTdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcbiAgbGV0IGNoYXJDdXJzb3IgPSAwO1xuXG4gIGNvbnN0IGZsdXNoID0gKGZvcmNlVGV4dCkgPT4ge1xuICAgIGNvbnN0IGNvbnRlbnQgPSAoZm9yY2VUZXh0ID8/IGJ1ZmZlcikudHJpbSgpO1xuICAgIGlmIChjb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY29udGVudCxcbiAgICAgICAgdG9rZW5Db3VudDogZXN0aW1hdGVUb2tlbnMoY29udGVudCksXG4gICAgICAgIGNoYXJTdGFydDogYnVmU3RhcnQsXG4gICAgICAgIGNoYXJFbmQ6IGJ1ZlN0YXJ0ICsgY29udGVudC5sZW5ndGgsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuICAgIGJ1ZmZlciA9ICcnO1xuICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgfTtcblxuICBmb3IgKGNvbnN0IHBhcmEgb2YgcmF3UGFyYXMpIHtcbiAgICBjb25zdCBpc0hlYWRpbmcgPSBIRUFESU5HX1JFLnRlc3QocGFyYS5zcGxpdCgnXFxuJylbMF0pO1xuXG4gICAgLy8gMi4gSGVhZGluZyBhbHdheXMgc3RhcnRzIGEgbmV3IGNodW5rXG4gICAgaWYgKGlzSGVhZGluZyAmJiBidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgIGlmIChwYXJhLmxlbmd0aCA+IG1heENoYXJzKSB7XG4gICAgICAvLyAzLiBPdmVyc2l6ZWQgcGFyYWdyYXBoIC0+IHNsaWRpbmctd2luZG93IGNoYXIgZmFsbGJhY2tcbiAgICAgIGlmIChidWZmZXIubGVuZ3RoID4gMCkgZmx1c2goKTtcblxuICAgICAgbGV0IHMgPSAwO1xuICAgICAgd2hpbGUgKHMgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICBsZXQgZSA9IHMgKyB0YXJnZXRDaGFycztcbiAgICAgICAgaWYgKGUgPCBwYXJhLmxlbmd0aCkge1xuICAgICAgICAgIGNvbnN0IHNlYXJjaEZyb20gPSBlIC0gTWF0aC5mbG9vcih0YXJnZXRDaGFycyAqIDAuMik7XG4gICAgICAgICAgZm9yIChjb25zdCBicCBvZiBbJy4gJywgJy5cXG4nLCAnPyAnLCAnISAnLCAnXFxuJ10pIHtcbiAgICAgICAgICAgIGNvbnN0IGlkeCA9IHBhcmEubGFzdEluZGV4T2YoYnAsIGUpO1xuICAgICAgICAgICAgaWYgKGlkeCA+IHNlYXJjaEZyb20pIHsgZSA9IGlkeCArIGJwLmxlbmd0aDsgYnJlYWs7IH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgZSA9IE1hdGgubWluKGUsIHBhcmEubGVuZ3RoKTtcbiAgICAgICAgY29uc3Qgc2xpY2UgPSBwYXJhLnNsaWNlKHMsIGUpLnRyaW0oKTtcbiAgICAgICAgaWYgKHNsaWNlLmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgICAgICBjaHVua3MucHVzaCh7XG4gICAgICAgICAgICB0ZXh0OiBzbGljZSxcbiAgICAgICAgICAgIHRva2VuQ291bnQ6IGVzdGltYXRlVG9rZW5zKHNsaWNlKSxcbiAgICAgICAgICAgIGNoYXJTdGFydDogY2hhckN1cnNvciArIHMsXG4gICAgICAgICAgICBjaGFyRW5kOiBjaGFyQ3Vyc29yICsgZSxcbiAgICAgICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKGUgPj0gcGFyYS5sZW5ndGgpIGJyZWFrO1xuXG4gICAgICAgIC8vIFNuYXAgdGhlIG92ZXJsYXBwZWQgc3RhcnQgZm9yd2FyZCB0byBhIGNsZWFuIHNlbnRlbmNlL3dvcmRcbiAgICAgICAgLy8gYm91bmRhcnkgaW5zdGVhZCBvZiB1c2luZyB0aGUgcmF3IG9mZnNldCwgd2hpY2ggY291bGQgbGFuZFxuICAgICAgICAvLyBtaWQtd29yZCAoZS5nLiBcInMgdGhhdCBhbiBFVEYuLi5cIikuXG4gICAgICAgIGNvbnN0IHJhd05leHQgPSBlIC0gb3ZlcmxhcENoYXJzO1xuICAgICAgICBzID0gcmF3TmV4dCA+IHMgPyBzbmFwVG9Cb3VuZGFyeShwYXJhLCByYXdOZXh0LCBlKSA6IGU7XG4gICAgICB9XG4gICAgICBjaGFyQ3Vyc29yICs9IHBhcmEubGVuZ3RoICsgMjtcbiAgICAgIGJ1ZlN0YXJ0ID0gY2hhckN1cnNvcjtcbiAgICAgIGNvbnRpbnVlO1xuICAgIH1cblxuICAgIC8vIDQuIE5vcm1hbCBwYXJhZ3JhcGggXHUyMDE0IGhhcmQgY2FwIGxvb2thaGVhZCBCRUZPUkUgYWNjdW11bGF0aW5nXG4gICAgaWYgKGJ1ZmZlci5sZW5ndGggPiAwICYmIChidWZmZXIubGVuZ3RoICsgcGFyYS5sZW5ndGggKyAyKSA+IG1heENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cblxuICAgIGJ1ZmZlciA9IGJ1ZmZlciA/IGJ1ZmZlciArICdcXG5cXG4nICsgcGFyYSA6IHBhcmE7XG4gICAgY2hhckN1cnNvciArPSBwYXJhLmxlbmd0aCArIDI7XG5cbiAgICAvLyBTb2Z0IGNhcDogZmx1c2ggb25jZSB0YXJnZXQgaXMgcmVhY2hlZFxuICAgIGlmIChidWZmZXIubGVuZ3RoID49IHRhcmdldENoYXJzKSB7XG4gICAgICBmbHVzaCgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDUuIEZsdXNoIHJlbWFpbmRlclxuICBmbHVzaCgpO1xuXG4gIHJldHVybiBjaHVua3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1BERkNvbnRlbnQocGRmRGF0YSwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IHsgZmlsZW5hbWUsIGRvY3VtZW50SWQsIHBhZ2VOdW1iZXIsIHRleHQsIHRvdGFsUGFnZXMgfSA9IHBkZkRhdGE7XG5cbiAgaWYgKCF0ZXh0IHx8IHRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgICR7ZmlsZW5hbWV9IHBhZ2UgJHtwYWdlTnVtYmVyfTogZXh0cmFjdGVkIHRleHQgdG9vIHNob3J0IFx1MjAxNCBtYXkgYmUgYSBzY2FubmVkIHBhZ2UsIHNraXBwaW5nYCk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgY29uc3QgY2xlYW5lZFRleHQgPSBjbGVhblRleHQodGV4dCk7XG4gIGNvbnN0IHRleHRDaHVua3MgPSBjaHVua1RleHQoY2xlYW5lZFRleHQsIG9wdGlvbnMpO1xuICBjb25zdCB0b3RhbENodW5rcyA9IHRleHRDaHVua3MubGVuZ3RoO1xuICBjb25zdCBzb3VyY2VUeXBlID0gb3B0aW9ucy5zb3VyY2VUeXBlIHx8ICdwZGYnO1xuXG4gIHJldHVybiB0ZXh0Q2h1bmtzLm1hcChjaHVuayA9PiB7XG4gICAgY29uc3QgY2h1bmtJZCA9IGdlbmVyYXRlQ2h1bmtJZChjaHVuay50ZXh0LCBmaWxlbmFtZSk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWUsXG4gICAgICAgIGNodW5rX2lkOiBjaHVua0lkLFxuICAgICAgICBjaHVua19pbmRleDogY2h1bmsuY2h1bmtJbmRleCxcbiAgICAgICAgdG90YWxfY2h1bmtzOiB0b3RhbENodW5rcyxcbiAgICAgICAgcGFnZV9udW1iZXI6IHBhZ2VOdW1iZXIgfHwgMSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMgfHwgbnVsbCxcbiAgICAgICAgc2VjdGlvbl90aXRsZTogZXh0cmFjdFNlY3Rpb25UaXRsZShjaHVuay50ZXh0KSxcbiAgICAgICAgc291cmNlX3R5cGU6IHNvdXJjZVR5cGUsXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9O1xuICB9KTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIGNvbnN0IGxpbmVzID0gdGV4dC5zcGxpdCgnXFxuJykuZmlsdGVyKGwgPT4gbC50cmltKCkpO1xuICBpZiAobGluZXMubGVuZ3RoID4gMCkge1xuICAgIGNvbnN0IGZpcnN0TGluZSA9IGxpbmVzWzBdLnRyaW0oKTtcbiAgICBpZiAoZmlyc3RMaW5lLmxlbmd0aCA8IDEwMCAmJiAhZmlyc3RMaW5lLmVuZHNXaXRoKCcuJykpIHtcbiAgICAgIHJldHVybiBmaXJzdExpbmUuc2xpY2UoMCwgNTApO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbkFJIH0gZnJvbSAnQGdvb2dsZS9nZW5haSc7XG5pbXBvcnQgeyBFbWJlZGRpbmdFcnJvciwgaXM0MjlFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcblxuY29uc3QgX19maWxlbmFtZSA9IGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKTtcbmNvbnN0IF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShfX2ZpbGVuYW1lKTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAxLiBTTElESU5HIFdJTkRPVyBSQVRFIExJTUlURVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY2xhc3MgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyIHtcbiAgY29uc3RydWN0b3IobGltaXRQZXJNaW51dGUpIHtcbiAgICB0aGlzLmxpbWl0UGVyTWludXRlID0gbGltaXRQZXJNaW51dGU7XG4gICAgdGhpcy53aW5kb3dNcyA9IDYwMDAwO1xuICAgIHRoaXMucmVxdWVzdHMgPSBbXTtcbiAgfVxuXG4gIGFzeW5jIGNvbnN1bWUodG9rZW5zKSB7XG4gICAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgICAvLyBSZW1vdmUgZW50cmllcyBvbGRlciB0aGFuIDYwIHNlY29uZHNcbiAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBub3cgLSB0aGlzLndpbmRvd01zKTtcblxuICAgIGNvbnN0IGN1cnJlbnRUb3RhbCA9IHRoaXMucmVxdWVzdHMucmVkdWNlKChzdW0sIHJlcSkgPT4gc3VtICsgcmVxLnRva2VucywgMCk7XG5cbiAgICAvLyBJZiB3ZSBoYXZlIHJvb20sIGNvbnN1bWUgaW5zdGFudGx5IChidXJzdClcbiAgICBpZiAoY3VycmVudFRvdGFsICsgdG9rZW5zIDw9IHRoaXMubGltaXRQZXJNaW51dGUpIHtcbiAgICAgIHRoaXMucmVxdWVzdHMucHVzaCh7IHRpbWVzdGFtcDogbm93LCB0b2tlbnMgfSk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gT3RoZXJ3aXNlLCB3YWl0IHVudGlsIHRoZSBvbGRlc3QgcmVxdWVzdCBleHBpcmVzIChwbHVzIGEgc21hbGwgYnVmZmVyKVxuICAgIGNvbnN0IG5lZWRlZCA9IHRva2VucyAtICh0aGlzLmxpbWl0UGVyTWludXRlIC0gY3VycmVudFRvdGFsKTtcbiAgICBsZXQgYWNjdW11bGF0ZWRFeHBpcmVkID0gMDtcbiAgICBsZXQgd2FpdFVudGlsID0gbm93ICsgdGhpcy53aW5kb3dNczsgLy8gZmFsbGJhY2tcblxuICAgIGNvbnN0IHNvcnRlZCA9IFsuLi50aGlzLnJlcXVlc3RzXS5zb3J0KChhLCBiKSA9PiBhLnRpbWVzdGFtcCAtIGIudGltZXN0YW1wKTtcbiAgICBmb3IgKGNvbnN0IHJlcSBvZiBzb3J0ZWQpIHtcbiAgICAgIGFjY3VtdWxhdGVkRXhwaXJlZCArPSByZXEudG9rZW5zO1xuICAgICAgaWYgKGFjY3VtdWxhdGVkRXhwaXJlZCA+PSBuZWVkZWQpIHtcbiAgICAgICAgLy8gKzEwbXMgYnVmZmVyIHRvIHNsaWRlIHRoZSB3aW5kb3cgY2xlYW5seVxuICAgICAgICB3YWl0VW50aWwgPSByZXEudGltZXN0YW1wICsgdGhpcy53aW5kb3dNcyArIDEwO1xuICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBkZWxheSA9IHdhaXRVbnRpbCAtIG5vdztcbiAgICBpZiAoZGVsYXkgPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgYFtyYXRlLWxpbWl0XSBXaW5kb3cgZnVsbCAoJHtjdXJyZW50VG90YWx9LyR7dGhpcy5saW1pdFBlck1pbnV0ZX0pLiBgICtcbiAgICAgICAgYFdhaXRpbmcgJHsoZGVsYXkgLyAxMDAwKS50b0ZpeGVkKDEpfXMgdG8gc2VuZCAke3Rva2Vuc30gdG9rZW5zLi4uYFxuICAgICAgKTtcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlc29sdmUgPT4gc2V0VGltZW91dChyZXNvbHZlLCBkZWxheSkpO1xuICAgIH1cblxuICAgIC8vIFJlY29yZCB0aGUgY29uc3VtcHRpb24gYXQgdGhlIG5ldyB0aW1lXG4gICAgdGhpcy5yZXF1ZXN0cy5wdXNoKHsgdGltZXN0YW1wOiBEYXRlLm5vdygpLCB0b2tlbnMgfSk7XG4gICAgLy8gQ2xlYW51cCBhZ2FpbiBqdXN0IGluIGNhc2VcbiAgICB0aGlzLnJlcXVlc3RzID0gdGhpcy5yZXF1ZXN0cy5maWx0ZXIocmVxID0+IHJlcS50aW1lc3RhbXAgPiBEYXRlLm5vdygpIC0gdGhpcy53aW5kb3dNcyk7XG4gIH1cbn1cblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyAyLiBDT05GSUdVUkFUSU9OXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnN0IFRQTV9MSU1JVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfVFBNX0xJTUlUKSB8fCA1MDAwMDA7XG5jb25zdCBSQVRFX0xJTUlURVIgPSBuZXcgU2xpZGluZ1dpbmRvd1JhdGVMaW1pdGVyKFRQTV9MSU1JVCk7XG5cbi8vIEJBVENIX1NJWkU6IG51bWJlciBvZiBjaHVua3MgcGVyIGVtYmVkQ29udGVudCBjYWxsXG4vLyAoa2VwdCBhdCAxMDsgbm90ZSB0aGUgcmVhbCBjZWlsaW5nIGlzIHRoZSBBUEkncyB+MTAwLXJlcXVlc3RzLXBlci1jYWxsIGxpbWl0LFxuLy8gbm90IGEgXCJjb250ZXh0IHdpbmRvd1wiIGxpbWl0IFx1MjAxNCAxMCBqdXN0IGtlZXBzIGJhdGNoZXMgc21hbGwgYW5kIHJldHJ5LWZyaWVuZGx5KVxuY29uc3QgQkFUQ0hfU0laRSA9ICgpID0+IDEwOyAgIC8vIDEwIGNodW5rcyBcdTAwRDcgNzUwIHRva2VucyA9IDcsNTAwIHRva2VucyBwZXIgQVBJIHJlcXVlc3RcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gMTA7IC8vIFNlbmQgMTAgYmF0Y2hlcyBjb25jdXJyZW50bHkgdG8gY2xlYXIgdGhlIGJ1cnN0IGZhc3RcblxuLy8gUmV0cnkgY29uZmlndXJhdGlvbiAoZXhwb25lbnRpYWwgYmFja29mZiArIGppdHRlcilcbmNvbnN0IFJFVFJZX0JBU0VfREVMQVlfTVMgPSAyMDAwOyAgIC8vIDIgc2Vjb25kc1xuY29uc3QgUkVUUllfTUFYX0RFTEFZX01TID0gNjAwMDA7ICAgLy8gNjAgc2Vjb25kcyBjYXBcbmNvbnN0IE1BWF9SRVRSWV9BVFRFTVBUUyA9IDU7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gMy4gQUkgQ0xJRU5UIChzaW5nbGUsIHJldXNhYmxlIGluc3RhbmNlKVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5mdW5jdGlvbiBsb2FkR29vZ2xlQ3JlZGVudGlhbHMoKSB7XG4gIC8vIDEuIFRyeSBlbnYgdmFyIGZpcnN0IChzdXBwb3J0cyByYXcgSlNPTiBvciBiYXNlNjQtZW5jb2RlZCBKU09OKVxuICBjb25zdCBjcmVkZW50aWFsc0pzb24gPSBwcm9jZXNzLmVudi5HT09HTEVfQ1JFREVOVElBTFNfSlNPTjtcbiAgaWYgKGNyZWRlbnRpYWxzSnNvbikge1xuICAgIHRyeSB7XG4gICAgICAvLyBUcnkgcmF3IEpTT04gZmlyc3RcbiAgICAgIHJldHVybiBKU09OLnBhcnNlKGNyZWRlbnRpYWxzSnNvbik7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgLy8gVHJ5IGJhc2U2NCBkZWNvZGUgKHVzZWZ1bCBmb3IgcGxhdGZvcm1zIHdpdGggY2hhcmFjdGVyIGxpbWl0IGlzc3VlcylcbiAgICAgICAgY29uc3QgZGVjb2RlZCA9IEJ1ZmZlci5mcm9tKGNyZWRlbnRpYWxzSnNvbiwgJ2Jhc2U2NCcpLnRvU3RyaW5nKCd1dGYtOCcpO1xuICAgICAgICByZXR1cm4gSlNPTi5wYXJzZShkZWNvZGVkKTtcbiAgICAgIH0gY2F0Y2ggKGUyKSB7XG4gICAgICAgIGNvbnNvbGUud2FybignW2VtYmVkZGluZ10gRmFpbGVkIHRvIHBhcnNlIEdPT0dMRV9DUkVERU5USUFMU19KU09OICh0cmllZCByYXcgYW5kIGJhc2U2NCknKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvLyAyLiBUcnkgR09PR0xFX0FQUExJQ0FUSU9OX0NSRURFTlRJQUxTIGZpbGUgcGF0aFxuICBjb25zdCBjcmVkc1BhdGggPSBwcm9jZXNzLmVudi5HT09HTEVfQVBQTElDQVRJT05fQ1JFREVOVElBTFM7XG4gIGlmIChjcmVkc1BhdGgpIHtcbiAgICB0cnkge1xuICAgICAgLy8gSGFuZGxlIHJlbGF0aXZlIHBhdGhzXG4gICAgICBjb25zdCBhYnNvbHV0ZVBhdGggPSBwYXRoLmlzQWJzb2x1dGUoY3JlZHNQYXRoKVxuICAgICAgICA/IGNyZWRzUGF0aFxuICAgICAgICA6IHBhdGgucmVzb2x2ZShwcm9jZXNzLmN3ZCgpLCBjcmVkc1BhdGgpO1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoYWJzb2x1dGVQYXRoKSkge1xuICAgICAgICByZXR1cm4gSlNPTi5wYXJzZShmcy5yZWFkRmlsZVN5bmMoYWJzb2x1dGVQYXRoLCAndXRmLTgnKSk7XG4gICAgICB9XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgY29uc29sZS53YXJuKCdbZW1iZWRkaW5nXSBGYWlsZWQgdG8gcmVhZCBHT09HTEVfQVBQTElDQVRJT05fQ1JFREVOVElBTFM6JywgZS5tZXNzYWdlKTtcbiAgICB9XG4gIH1cblxuICAvLyAzLiBUcnkgY29tbW9uIGRlcGxveWVkIGxvY2F0aW9uc1xuICBjb25zdCBwb3NzaWJsZVBhdGhzID0gW1xuICAgIHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuLi8uLi9nb29nbGVfY3JlZGVudGlhbHMvcHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwLWU4MGE0ODkzZDFiYy5qc29uJyksXG4gICAgcGF0aC5yZXNvbHZlKHByb2Nlc3MuY3dkKCksICdnb29nbGVfY3JlZGVudGlhbHMvcHJvamVjdC1kNDhlMmYzOS0yNjg1LTQ3NDYtYWEwLWU4MGE0ODkzZDFiYy5qc29uJyksXG4gICAgcGF0aC5yZXNvbHZlKHByb2Nlc3MuY3dkKCksICdkaXN0L2dvb2dsZV9jcmVkZW50aWFscy9wcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAtZTgwYTQ4OTNkMWJjLmpzb24nKSxcbiAgICAnL3Zhci90YXNrL2dvb2dsZV9jcmVkZW50aWFscy9wcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAtZTgwYTQ4OTNkMWJjLmpzb24nLFxuICAgICcvdG1wL2dvb2dsZV9jcmVkZW50aWFscy9wcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAtZTgwYTQ4OTNkMWJjLmpzb24nXG4gIF07XG5cbiAgZm9yIChjb25zdCBwIG9mIHBvc3NpYmxlUGF0aHMpIHtcbiAgICB0cnkge1xuICAgICAgaWYgKGZzLmV4aXN0c1N5bmMocCkpIHtcbiAgICAgICAgY29uc29sZS5sb2coJ1tlbWJlZGRpbmddIEZvdW5kIGNyZWRlbnRpYWxzIGF0OicsIHApO1xuICAgICAgICByZXR1cm4gSlNPTi5wYXJzZShmcy5yZWFkRmlsZVN5bmMocCwgJ3V0Zi04JykpO1xuICAgICAgfVxuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIC8vIENvbnRpbnVlIHRvIG5leHQgcGF0aFxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBudWxsO1xufVxuXG5mdW5jdGlvbiBjcmVhdGVBSUNsaWVudCgpIHtcbiAgY29uc3QgcHJvamVjdCA9IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9QUk9KRUNUIHx8IHByb2Nlc3MuZW52LkdDUF9QUk9KRUNUIHx8ICdwcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAnO1xuICBjb25zdCBsb2NhdGlvbiA9IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9MT0NBVElPTiB8fCAndXMtY2VudHJhbDEnO1xuXG4gIGNvbnN0IGNyZWRlbnRpYWxzID0gbG9hZEdvb2dsZUNyZWRlbnRpYWxzKCk7XG5cbiAgaWYgKGNyZWRlbnRpYWxzKSB7XG4gICAgY29uc29sZS5sb2coJ1tlbWJlZGRpbmddIFVzaW5nIGV4cGxpY2l0IEdvb2dsZSBjcmVkZW50aWFscycpO1xuICAgIHJldHVybiBuZXcgR29vZ2xlR2VuQUkoe1xuICAgICAgdmVydGV4YWk6IHRydWUsXG4gICAgICBwcm9qZWN0LFxuICAgICAgbG9jYXRpb24sXG4gICAgICBjcmVkZW50aWFsc1xuICAgIH0pO1xuICB9XG5cbiAgY29uc29sZS5sb2coJ1tlbWJlZGRpbmddIFVzaW5nIGRlZmF1bHQgR29vZ2xlIGF1dGgnKTtcbiAgcmV0dXJuIG5ldyBHb29nbGVHZW5BSSh7XG4gICAgdmVydGV4YWk6IHRydWUsXG4gICAgcHJvamVjdCxcbiAgICBsb2NhdGlvblxuICB9KTtcbn1cblxuY29uc3QgYWkgPSBjcmVhdGVBSUNsaWVudCgpO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDQuIFRPS0VOIENBTENVTEFUSU9OICh1c2VzIHN0b3JlZCB0b2tlbl9jb3VudCBpZiBhdmFpbGFibGUpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmZ1bmN0aW9uIGdldFRva2VuQ291bnRGb3JDaHVua3MoY2h1bmtzKSB7XG4gIHJldHVybiBjaHVua3MucmVkdWNlKChzdW0sIGNodW5rKSA9PiB7XG4gICAgLy8gUHJlZmVyIHRoZSBleGFjdCB0b2tlbiBjb3VudCBmcm9tIGNodW5rZXIsIG90aGVyd2lzZSBmYWxsYmFjayB0byByb3VnaCBlc3RpbWF0ZVxuICAgIGNvbnN0IHRva2VuQ291bnQgPSBjaHVuay5tZXRhZGF0YT8udG9rZW5fY291bnQgfHwgTWF0aC5jZWlsKGNodW5rLnRleHQubGVuZ3RoIC8gNCk7XG4gICAgcmV0dXJuIHN1bSArIHRva2VuQ291bnQ7XG4gIH0sIDApO1xufVxuXG4vLyBTYW1lIHJvdWdoIGVzdGltYXRlIGFzIGFib3ZlLCBidXQgZm9yIHJhdyBzdHJpbmdzIHRoYXQgZG9uJ3QgY2FycnkgY2h1bmsgbWV0YWRhdGFcbi8vICh1c2VkIGZvciByZXRyaWVzIGluc2lkZSBlbWJlZEJhdGNoLCBhbmQgZm9yIGVtYmVkUXVlcnkpLlxuZnVuY3Rpb24gZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykge1xuICByZXR1cm4gdGV4dHMucmVkdWNlKChzdW0sIHRleHQpID0+IHN1bSArIE1hdGguY2VpbChTdHJpbmcodGV4dCkubGVuZ3RoIC8gNCksIDApO1xufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDUuIEVNQkVEIEJBVENIICh3aXRoIGV4cG9uZW50aWFsIGJhY2tvZmYgKyBqaXR0ZXIpXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFzeW5jIGZ1bmN0aW9uIGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIGF0dGVtcHQgPSAxKSB7XG4gIGNvbnN0IG1vZGVsTmFtZSA9IHByb2Nlc3MuZW52LkdFTUlOSV9FTUJFRERJTkdfTU9ERUwgfHwgJ2dlbWluaS1lbWJlZGRpbmctMDAxJztcbiAgY29uc3Qgb3V0cHV0RGltZW5zaW9uYWxpdHkgPSBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7XG5cbiAgdHJ5IHtcbiAgICAvLyBGSVg6IGBhaS5iYXRjaGVzLmNyZWF0ZUVtYmVkZGluZ3NgIGlzIG5vdCBhIHJlYWwgbWV0aG9kIG9uIHRoZSBAZ29vZ2xlL2dlbmFpIFNESy5cbiAgICAvLyBgYWkuYmF0Y2hlc2AgaXMgZm9yIGFzeW5jIGJhdGNoLXByZWRpY3Rpb24gam9icy4gU3luY2hyb25vdXMgZW1iZWRkaW5nIGNhbGxzIGdvXG4gICAgLy8gdGhyb3VnaCBgYWkubW9kZWxzLmVtYmVkQ29udGVudGAsIHdpdGggb25lIHNoYXJlZCB0YXNrVHlwZS9vdXRwdXREaW1lbnNpb25hbGl0eVxuICAgIC8vIGNvbmZpZyBhcHBsaWVkIGFjcm9zcyBhbGwgYGNvbnRlbnRzYCBpbiB0aGUgY2FsbC5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IGFpLm1vZGVscy5lbWJlZENvbnRlbnQoe1xuICAgICAgbW9kZWw6IG1vZGVsTmFtZSxcbiAgICAgIGNvbnRlbnRzOiB0ZXh0cy5tYXAodGV4dCA9PiAodHlwZW9mIHRleHQgPT09ICdzdHJpbmcnID8gdGV4dCA6IFN0cmluZyh0ZXh0KSkpLFxuICAgICAgY29uZmlnOiB7XG4gICAgICAgIHRhc2tUeXBlOiB0YXNrVHlwZSxcbiAgICAgICAgb3V0cHV0RGltZW5zaW9uYWxpdHk6IG91dHB1dERpbWVuc2lvbmFsaXR5XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjb25zdCBlbWJlZGRpbmdzID0gcmVzcG9uc2U/LmVtYmVkZGluZ3M/Lm1hcChlID0+IGUudmFsdWVzKSB8fCBbXTtcbiAgICBpZiAoZW1iZWRkaW5ncy5sZW5ndGggIT09IHRleHRzLmxlbmd0aCkge1xuICAgICAgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKGBFeHBlY3RlZCAke3RleHRzLmxlbmd0aH0gZW1iZWRkaW5ncywgZ290ICR7ZW1iZWRkaW5ncy5sZW5ndGh9YCk7XG4gICAgfVxuICAgIHJldHVybiBlbWJlZGRpbmdzO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc3QgaXNSZXRyeWFibGUgPSBpczQyOUVycm9yKGVycm9yKSB8fFxuICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICBlcnJvcj8uc3RhdHVzID09PSA1MDIgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDUwMyB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdTZXJ2aWNlIFVuYXZhaWxhYmxlJykgfHxcbiAgICAgIGVycm9yPy5tZXNzYWdlPy5pbmNsdWRlcygnQmFkIEdhdGV3YXknKTtcblxuICAgIGlmIChpc1JldHJ5YWJsZSAmJiBhdHRlbXB0IDwgTUFYX1JFVFJZX0FUVEVNUFRTKSB7XG4gICAgICAvLyBFeHBvbmVudGlhbCBiYWNrb2ZmOiAyXmF0dGVtcHQgKiBiYXNlIChjYXBwZWQpXG4gICAgICBsZXQgZGVsYXkgPSBNYXRoLm1pbihSRVRSWV9NQVhfREVMQVlfTVMsIFJFVFJZX0JBU0VfREVMQVlfTVMgKiBNYXRoLnBvdygyLCBhdHRlbXB0IC0gMSkpO1xuICAgICAgLy8gQWRkIGppdHRlciAoMC44XHUyMDEzMS4yeCkgdG8gYXZvaWQgdGh1bmRlcmluZyBoZXJkXG4gICAgICBjb25zdCBqaXR0ZXIgPSAwLjggKyAoMC40ICogTWF0aC5yYW5kb20oKSk7XG4gICAgICBkZWxheSA9IE1hdGguZmxvb3IoZGVsYXkgKiBqaXR0ZXIpO1xuICAgICAgLy8gUmVzcGVjdCByZXRyeS1hZnRlciBoZWFkZXIgaWYgcHJlc2VudFxuICAgICAgaWYgKGVycm9yLnJldHJ5QWZ0ZXIpIHtcbiAgICAgICAgZGVsYXkgPSBNYXRoLm1heChkZWxheSwgZXJyb3IucmV0cnlBZnRlciAqIDEwMDApO1xuICAgICAgfVxuXG4gICAgICBjb25zb2xlLmxvZyhcbiAgICAgICAgYFtlbWJlZGRpbmddIFx1MjNGMyBSZXRyeWFibGUgZXJyb3IgKCR7ZXJyb3I/LnN0YXR1cyB8fCAndW5rbm93bid9KSwgYCArXG4gICAgICAgIGB3YWl0aW5nICR7KGRlbGF5IC8gMTAwMCkudG9GaXhlZCgxKX1zIChhdHRlbXB0ICR7YXR0ZW1wdH0vJHtNQVhfUkVUUllfQVRURU1QVFN9KS4uLmBcbiAgICAgICk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgZGVsYXkpKTtcblxuICAgICAgLy8gRklYOiBhIHJldHJ5IGlzIGEgYnJhbmQgbmV3IEFQSSBjYWxsIGFuZCBjb25zdW1lcyByZWFsIHF1b3RhLCBldmVuIHRob3VnaFxuICAgICAgLy8gdGhlIG9yaWdpbmFsIGNhbGwgZmFpbGVkLiBTa2lwcGluZyBjb25zdW1wdGlvbiBoZXJlIChhcyBiZWZvcmUpIGxldCB0aGUgbG9jYWxcbiAgICAgIC8vIGxpbWl0ZXIgdW5kZXItcmVwb3J0IGFjdHVhbCB1c2FnZSBkdXJpbmcgZXJyb3Igc3Rvcm1zLCB3aGljaCBtZWFudCBpdCBrZXB0XG4gICAgICAvLyB3YXZpbmcgdGhyb3VnaCBuZXcgZ3JvdXBzIHdoaWxlIHJldHJpZXMgd2VyZSBhbHNvIGhpdHRpbmcgdGhlIEFQSSBcdTIwMTQgbWFraW5nXG4gICAgICAvLyA0Mjkgc3Rvcm1zIHdvcnNlIGluc3RlYWQgb2YgYmFja2luZyBvZmYgZnJvbSB0aGVtLlxuICAgICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUoZXN0aW1hdGVUb2tlbnNGb3JUZXh0cyh0ZXh0cykpO1xuXG4gICAgICByZXR1cm4gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUsIGF0dGVtcHQgKyAxKTtcbiAgICB9XG5cbiAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoZXJyb3IubWVzc2FnZSB8fCAnQmF0Y2ggZW1iZWRkaW5nIGZhaWxlZCcpO1xuICB9XG59XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gNi4gRVhQT1JURUQgZ2VuZXJhdGVFbWJlZGRpbmdzICh3aXRoIHJhdGUgbGltaXRlciAmIGFjY3VyYXRlIHRva2Vucylcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSA9IEJBVENIX1NJWkUoKTtcbiAgY29uc3QgcGFyYWxsZWxDYWxscyA9IFBBUkFMTEVMX0NBTExTKCk7XG5cbiAgLy8gRml4ZWQtc2l6ZSBhcnJheSB0byBwcmVzZXJ2ZSBjaHJvbm9sb2dpY2FsIG9yZGVyXG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBuZXcgQXJyYXkoY2h1bmtzLmxlbmd0aCk7XG5cbiAgLy8gR3JvdXAgY2h1bmtzIGludG8gYmF0Y2hlcyB3aXRoIHRoZWlyIHN0YXJ0aW5nIGluZGV4XG4gIGNvbnN0IGJhdGNoZXMgPSBbXTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBjaHVua3MubGVuZ3RoOyBpICs9IGJhdGNoU2l6ZSkge1xuICAgIGJhdGNoZXMucHVzaCh7XG4gICAgICBjaHVua3M6IGNodW5rcy5zbGljZShpLCBpICsgYmF0Y2hTaXplKSxcbiAgICAgIHN0YXJ0SW5kZXg6IGlcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG5cbiAgICAvLyBDYWxjdWxhdGUgZXhhY3QgdG9rZW5zIHVzaW5nIHN0b3JlZCB0b2tlbl9jb3VudCAob3IgZmFsbGJhY2spXG4gICAgY29uc3QgYWxsQ2h1bmtzSW5Hcm91cCA9IHBhcmFsbGVsQmF0Y2hlcy5mbGF0TWFwKGIgPT4gYi5jaHVua3MpO1xuICAgIGNvbnN0IHRva2Vuc1RvQ29uc3VtZSA9IGdldFRva2VuQ291bnRGb3JDaHVua3MoYWxsQ2h1bmtzSW5Hcm91cCk7XG4gICAgYXdhaXQgUkFURV9MSU1JVEVSLmNvbnN1bWUodG9rZW5zVG9Db25zdW1lKTtcblxuICAgIGNvbnNvbGUubG9nKFxuICAgICAgYFtlbWJlZGRpbmddIEdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCBmaXJpbmcgJHtwYXJhbGxlbEJhdGNoZXMubGVuZ3RofSBiYXRjaGVzIGAgK1xuICAgICAgYGluIHBhcmFsbGVsICgke3Rva2Vuc1RvQ29uc3VtZX0gdG9rZW5zKWBcbiAgICApO1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsU2V0dGxlZChcbiAgICAgIHBhcmFsbGVsQmF0Y2hlcy5tYXAoYiA9PiBlbWJlZEJhdGNoKGIuY2h1bmtzLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgY29uc3QgZmFpbGVkQmF0Y2hlcyA9IFtdO1xuICAgIHJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgY29uc3QgY3VycmVudEJhdGNoSW5mbyA9IHBhcmFsbGVsQmF0Y2hlc1tiYXRjaElkeF07XG4gICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgY29uc3QgdmVjdG9ycyA9IHJlc3VsdC52YWx1ZTtcbiAgICAgICAgY3VycmVudEJhdGNoSW5mby5jaHVua3MuZm9yRWFjaCgoY2h1bmssIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgY29uc3QgZ2xvYmFsSW5kZXggPSBjdXJyZW50QmF0Y2hJbmZvLnN0YXJ0SW5kZXggKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzW2dsb2JhbEluZGV4XSA9IHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rXyR7Z2xvYmFsSW5kZXh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1tjaHVua0lkeF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfTtcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLndhcm4oYFtlbWJlZGRpbmddIEJhdGNoIHN0YXJ0aW5nIGF0IGluZGV4ICR7Y3VycmVudEJhdGNoSW5mby5zdGFydEluZGV4fSBmYWlsZWQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgIGZhaWxlZEJhdGNoZXMucHVzaChjdXJyZW50QmF0Y2hJbmZvKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChvblByb2dyZXNzKSB7XG4gICAgICBvblByb2dyZXNzKHsgY3VycmVudF9iYXRjaDogZ3JvdXBOdW0sIHRvdGFsX2JhdGNoZXM6IHRvdGFsR3JvdXBzIH0pO1xuICAgIH1cblxuICAgIC8vIFJldHJ5IGZhaWxlZCBiYXRjaGVzIGluZGl2aWR1YWxseVxuICAgIGZvciAoY29uc3QgZmFpbGVkQmF0Y2ggb2YgZmFpbGVkQmF0Y2hlcykge1xuICAgICAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIFJldHJ5aW5nIGZhaWxlZCBiYXRjaCBlbGVtZW50cyBzdGFydGluZyBhdCBpbmRleCAke2ZhaWxlZEJhdGNoLnN0YXJ0SW5kZXh9Li4uYCk7XG4gICAgICBmb3IgKGxldCBjaHVua0lkeCA9IDA7IGNodW5rSWR4IDwgZmFpbGVkQmF0Y2guY2h1bmtzLmxlbmd0aDsgY2h1bmtJZHgrKykge1xuICAgICAgICBjb25zdCBjaHVuayA9IGZhaWxlZEJhdGNoLmNodW5rc1tjaHVua0lkeF07XG4gICAgICAgIGNvbnN0IGdsb2JhbEluZGV4ID0gZmFpbGVkQmF0Y2guc3RhcnRJbmRleCArIGNodW5rSWR4O1xuICAgICAgICB0cnkge1xuICAgICAgICAgIC8vIEZJWDogdGhpcyByZXRyeSBpcyBhIGZyZXNoLCByZWFsIEFQSSBjYWxsIFx1MjAxNCB0cmFjayBpdHMgdG9rZW5zIGFnYWluc3RcbiAgICAgICAgICAvLyB0aGUgbGltaXRlciBpbnN0ZWFkIG9mIGFzc3VtaW5nIGl0IHdhcyBcImFscmVhZHkgcGFpZCBmb3JcIi5cbiAgICAgICAgICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShnZXRUb2tlbkNvdW50Rm9yQ2h1bmtzKFtjaHVua10pKTtcbiAgICAgICAgICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbY2h1bmsudGV4dF0sIHRhc2tUeXBlKTtcbiAgICAgICAgICBlbWJlZGRpbmdzW2dsb2JhbEluZGV4XSA9IHtcbiAgICAgICAgICAgIGlkOiBjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgYGNodW5rX3JldHJ5XyR7Z2xvYmFsSW5kZXh9YCxcbiAgICAgICAgICAgIGVtYmVkZGluZzogdmVjdG9yc1swXSxcbiAgICAgICAgICAgIG1ldGFkYXRhOiBjaHVuay5tZXRhZGF0YSxcbiAgICAgICAgICAgIHRleHQ6IGNodW5rLnRleHRcbiAgICAgICAgICB9O1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbZW1iZWRkaW5nXSBcdTI3MDUgUmV0cnkgc3VjY2VlZGVkIGZvciBjaHVuayAke2NodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBnbG9iYWxJbmRleH1gKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgY29uc29sZS5lcnJvcihgW2VtYmVkZGluZ10gXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWQgfHwgZ2xvYmFsSW5kZXh9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8vIEZJWDogcGVybWFuZW50bHktZmFpbGVkIGNodW5rcyBhcmUgZHJvcHBlZCBoZXJlLCB3aGljaCBzaGlmdHMgYXJyYXkgaW5kaWNlc1xuICAvLyByZWxhdGl2ZSB0byB0aGUgb3JpZ2luYWwgYGNodW5rc2AgaW5wdXQuIFRoaXMgbG9nIG1ha2VzIHRoYXQgbG9zcyB2aXNpYmxlXG4gIC8vIGluc3RlYWQgb2Ygc2lsZW50OyBjYWxsZXJzIHRoYXQgbmVlZCB0byBrbm93IGV4YWN0bHkgd2hpY2ggY2h1bmtzIHdlcmUgbG9zdFxuICAvLyBjYW4gY29tcGFyZSByZXR1cm5lZCBgaWRgcyBhZ2FpbnN0IHRoZWlyIG9yaWdpbmFsIGNodW5rIGxpc3QuXG4gIGNvbnN0IGZhaWxlZENvdW50ID0gZW1iZWRkaW5ncy5maWx0ZXIoZSA9PiAhZSkubGVuZ3RoO1xuICBpZiAoZmFpbGVkQ291bnQgPiAwKSB7XG4gICAgY29uc29sZS53YXJuKGBbZW1iZWRkaW5nXSAke2ZhaWxlZENvdW50fS8ke2NodW5rcy5sZW5ndGh9IGNodW5rKHMpIHBlcm1hbmVudGx5IGZhaWxlZCB0byBlbWJlZCBhbmQgd2VyZSBkcm9wcGVkLmApO1xuICB9XG5cbiAgLy8gRmlsdGVyIG91dCBhbnkgZWxlbWVudHMgdGhhdCBwZXJtYW5lbnRseSBmYWlsZWRcbiAgcmV0dXJuIGVtYmVkZGluZ3MuZmlsdGVyKEJvb2xlYW4pO1xufVxuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDcuIEVYUE9SVEVEIGVtYmVkUXVlcnlcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkUXVlcnkocXVlcnkpIHtcbiAgLy8gRklYOiB0aGlzIGNhbGwgd2FzIGJ5cGFzc2luZyB0aGUgcmF0ZSBsaW1pdGVyIGVudGlyZWx5LiBJZiBpdCBydW5zIGNvbmN1cnJlbnRseVxuICAvLyB3aXRoIGRvY3VtZW50IGluZ2VzdGlvbiAoZS5nLiBhIHVzZXIgc2VhcmNoZXMgd2hpbGUgYSBiYXRjaCBqb2IgaXMgaW4gZmxpZ2h0KSxcbiAgLy8gaXQgY291bGQgcHVzaCB0b3RhbCB1c2FnZSBvdmVyIHRoZSBjb25maWd1cmVkIFRQTSBidWRnZXQgdW5ub3RpY2VkLlxuICBhd2FpdCBSQVRFX0xJTUlURVIuY29uc3VtZShlc3RpbWF0ZVRva2Vuc0ZvclRleHRzKFtxdWVyeV0pKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2goW3F1ZXJ5XSwgJ1JFVFJJRVZBTF9RVUVSWScpO1xuICByZXR1cm4gdmVjdG9yc1swXTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJykge1xuICBjb25zb2xlLmxvZyhgW2VtYmVkZGluZ10gZW1iZWRTaW5nbGVCYXRjaEdyb3VwIFx1MjAxNCAke3RleHRzLmxlbmd0aH0gdGV4dHMsIHRhc2tUeXBlPSR7dGFza1R5cGV9YCk7XG4gIGF3YWl0IFJBVEVfTElNSVRFUi5jb25zdW1lKGVzdGltYXRlVG9rZW5zRm9yVGV4dHModGV4dHMpKTtcbiAgY29uc3QgdmVjdG9ycyA9IGF3YWl0IGVtYmVkQmF0Y2godGV4dHMsIHRhc2tUeXBlKTtcbiAgY29uc29sZS5sb2coYFtlbWJlZGRpbmddIGVtYmVkU2luZ2xlQmF0Y2hHcm91cCBcdTIwMTQgZ290ICR7dmVjdG9ycy5sZW5ndGh9IHZlY3RvcnNgKTtcbiAgcmV0dXJuIHZlY3RvcnM7XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanNcIjtpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbmltcG9ydCB7XG4gIGdldEdsb2JhbENvbGxlY3Rpb24sXG4gIGdldENvbGxlY3Rpb24sXG4gIGxpc3REb2N1bWVudHNcbn0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcblxuY29uc3QgREVGQVVMVF9USU1FT1VUX01JTlVURVMgPSA2MDtcbmNvbnN0IHNlc3Npb25zID0gbmV3IE1hcCgpO1xuY29uc3QgTUFYX1BERlNfUEVSX1NFU1NJT04gPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTikgfHwgMztcbmNvbnN0IE1BWF9VUExPQURfU0laRV9NQiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQikgfHwgNTtcblxuY29uc3Qgc2VlZGVkU2Vzc2lvbnMgPSBuZXcgU2V0KCk7XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBHbG9iYWwgZG9jdW1lbnRzIGNhY2hlIChwb3B1bGF0ZWQgb25jZSBvbiBmaXJzdCBzZXNzaW9uIGluaXQpIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxubGV0IGdsb2JhbERvY3VtZW50c0NhY2hlID0gW107XG5sZXQgZ2xvYmFsRGF0YUluaXRpYWxpemVkID0gZmFsc2U7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRHbG9iYWxEb2N1bWVudHNDYWNoZSgpIHtcbiAgcmV0dXJuIGdsb2JhbERvY3VtZW50c0NhY2hlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgY29uc3QgaWQgPSBzZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IHNlc3Npb24gPSB7XG4gICAgaWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIGRlbGV0ZWREb2N1bWVudElkczogbmV3IFNldCgpLFxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuICBzZXNzaW9ucy5zZXQoaWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykgcmV0dXJuIGV4aXN0aW5nO1xuICAgIHJldHVybiBjcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gIH1cbiAgcmV0dXJuIGNyZWF0ZVNlc3Npb24oKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikge1xuICBjb25zdCBub3cgPSBEYXRlLm5vdygpO1xuICBjb25zdCBsYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZShzZXNzaW9uLmxhc3RBY2Nlc3NlZCkuZ2V0VGltZSgpO1xuICBjb25zdCB0aW1lb3V0TXMgPSBzZXNzaW9uLnRpbWVvdXRNaW51dGVzICogNjAgKiAxMDAwO1xuICByZXR1cm4gKG5vdyAtIGxhc3RBY2Nlc3NlZCkgPiB0aW1lb3V0TXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCkge1xuICBzZXNzaW9ucy5kZWxldGUoc2Vzc2lvbklkKTtcbiAgc2VlZGVkU2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbi8vIFx1MjUwMFx1MjUwMFx1MjUwMCBDaGVjayBpZiBzZXNzaW9uIGlzIHNlZWRlZCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25TZWVkZWQoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBzZWVkZWRTZXNzaW9ucy5oYXMoc2Vzc2lvbklkKTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwXHUyNTAwIE5vdGlmeSBTU0UgbGlzdGVuZXJzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuZnVuY3Rpb24gbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCkge1xuICBpZiAoZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMgJiYgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuaGFzKGBzZWVkaW5nOiR7c2Vzc2lvbklkfWApKSB7XG4gICAgY29uc3QgZXZlbnRLZXkgPSBgc2VlZGluZzoke3Nlc3Npb25JZH1gO1xuICAgIGNvbnN0IGxpc3RlbmVycyA9IGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkgfHwgW107XG4gICAgbGlzdGVuZXJzLmZvckVhY2goKHJlc3BvbnNlKSA9PiB7XG4gICAgICB0cnkge1xuICAgICAgICByZXNwb25zZS53cml0ZShgZXZlbnQ6IHNlZWRpbmdfY29tcGxldGVcXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KHsgc2Vzc2lvbklkLCBzZWVkZWQ6IHRydWUgfSl9XFxuXFxuYCk7XG4gICAgICAgIHJlc3BvbnNlLmVuZCgpO1xuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoYFtub3RpZnldIEZhaWxlZCB0byBub3RpZnkgbGlzdGVuZXI6YCwgZXJyLm1lc3NhZ2UpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgY29uc29sZS5sb2coYFtub3RpZnldIE5vdGlmaWVkICR7bGlzdGVuZXJzLmxlbmd0aH0gU1NFIGxpc3RlbmVycyBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgfVxufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6XG4gKiAtIFJlY29uc3RydWN0IGluLW1lbW9yeSBzZXNzaW9uIGRvYyBsaXN0IGZyb20gdGhlIHNpbmdsZSBjb2xsZWN0aW9uXG4gKiAgIGJ5IGZpbHRlcmluZyBvbiBzZXNzaW9uX2lkIG1ldGFkYXRhLlxuICogLSBObyB2ZWN0b3IgY29weWluZyBpcyBwZXJmb3JtZWQgXHUyMDE0IGdsb2JhbCBkb2NzIGFyZSBzZXJ2ZWQgZnJvbSBjYWNoZS5cbiAqL1xuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKSB7XG4gIGNvbnNvbGUubG9nKGBcdUQ4M0RcdUREMTEgU2Vzc2lvbiBpbml0OiAke3Nlc3Npb25JZH1gKTtcbiAgaWYgKHNlZWRlZFNlc3Npb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBBbHJlYWR5IHNlZWRlZCAke3Nlc3Npb25JZH0sIHNraXBwaW5nYCk7XG4gICAgbm90aWZ5U2VlZGluZ0NvbXBsZXRlKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0R2xvYmFsQ29sbGVjdGlvbigpO1xuXG4gICAgLy8gXHUyNTAwXHUyNTAwIExhenkgb25lLXRpbWUgZ2xvYmFsIGNhY2hlIGluaXQgKHJ1bnMgb24gZmlyc3Qgc2Vzc2lvbiBpbml0KSBcdTI1MDBcdTI1MDBcbiAgICBpZiAoIWdsb2JhbERhdGFJbml0aWFsaXplZCkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgZ2xvYmFsRG9jcyA9IGF3YWl0IGxpc3REb2N1bWVudHMoY29sbGVjdGlvbiwgeyBzZXNzaW9uX2lkOiAnZ2xvYmFsJyB9KTtcbiAgICAgICAgZ2xvYmFsRG9jdW1lbnRzQ2FjaGUgPSBnbG9iYWxEb2NzLm1hcChkb2MgPT4gKHtcbiAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgIGNodW5rQ291bnQ6IGRvYy5jaHVua19jb3VudCxcbiAgICAgICAgICBzb3VyY2VUeXBlOiAnZ2xvYmFsJyxcbiAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IGRvYy51cGxvYWRfdGltZXN0YW1wXG4gICAgICAgIH0pKTtcbiAgICAgICAgZ2xvYmFsRGF0YUluaXRpYWxpemVkID0gdHJ1ZTtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjcwNSBHbG9iYWwgZG9jdW1lbnRzIGNhY2hlIGxvYWRlZDogJHtnbG9iYWxEb2N1bWVudHNDYWNoZS5sZW5ndGh9IGRvY3VtZW50KHMpYCk7XG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgY29uc29sZS5lcnJvcignXHUyNzRDIEZhaWxlZCB0byBpbml0aWFsaXplIGdsb2JhbCBkYXRhOicsIGVyci5tZXNzYWdlKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcblxuICAgIC8vIFJlY29uc3RydWN0IHNlc3Npb24tc3BlY2lmaWMgZG9jcyAodXNlciB1cGxvYWRzKSBmcm9tIHRoZSBjb2xsZWN0aW9uXG4gICAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMubGVuZ3RoID09PSAwKSB7XG4gICAgICBjb25zdCBkb2NzID0gYXdhaXQgbGlzdERvY3VtZW50cyhjb2xsZWN0aW9uLCB7IHNlc3Npb25faWQ6IHNlc3Npb25JZCB9KTtcbiAgICAgIGRvY3MuZm9yRWFjaChkb2MgPT4ge1xuICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICAgICAgICBpZDogZG9jLmRvY3VtZW50X2lkLFxuICAgICAgICAgIGZpbGVuYW1lOiBkb2MuZmlsZW5hbWUsXG4gICAgICAgICAgZmlsZVNpemU6IG51bGwsXG4gICAgICAgICAgcGFnZUNvdW50OiBkb2MucGFnZV9jb3VudCB8fCBudWxsLFxuICAgICAgICAgIGNodW5rQ291bnQ6IGRvYy5jaHVua19jb3VudCxcbiAgICAgICAgICBzb3VyY2VUeXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogZG9jLnVwbG9hZF90aW1lc3RhbXBcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICAgIGlmIChkb2NzLmxlbmd0aCA+IDApIHtcbiAgICAgICAgY29uc29sZS5sb2coYFx1MjY3Qlx1RkUwRiAgUmVjb25zdHJ1Y3RlZCAke2RvY3MubGVuZ3RofSBzZXNzaW9uIGRvY3VtZW50KHMpIGZvciAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cbiAgICB9XG4gICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgY29uc29sZS5sb2coYFx1MjcwNSBTZXNzaW9uICR7c2Vzc2lvbklkfSByZWFkeSAobm8gdmVjdG9yIGNvcHlpbmcgbmVlZGVkKWApO1xuICAgIG5vdGlmeVNlZWRpbmdDb21wbGV0ZShzZXNzaW9uSWQpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgXHUyNzRDIEZhaWxlZCB0byBpbml0IHNlc3Npb24gJHtzZXNzaW9uSWR9OmAsIGVycm9yLm1lc3NhZ2UpO1xuICAgIC8vIFN0aWxsIG5vdGlmeSBsaXN0ZW5lcnMgc28gdGhleSBkb24ndCBoYW5nIGZvcmV2ZXJcbiAgICBub3RpZnlTZWVkaW5nQ29tcGxldGUoc2Vzc2lvbklkKTtcbiAgfVxufVxuXG4vLyBcdTI1MDBcdTI1MDBcdTI1MDAgRG9jdW1lbnQgbWFuYWdlbWVudCBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbmV4cG9ydCBmdW5jdGlvbiBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SW5mbykge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcblxuICBjb25zdCBleGlzdGluZyA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbmQoZCA9PiBkLmlkID09PSBkb2N1bWVudEluZm8uaWQpO1xuXG4gIGlmIChleGlzdGluZykge1xuICAgIGlmIChkb2N1bWVudEluZm8uY2h1bmtDb3VudCAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5jaHVua0NvdW50ID0gZG9jdW1lbnRJbmZvLmNodW5rQ291bnQ7XG4gICAgaWYgKGRvY3VtZW50SW5mby5wYWdlQ291bnQgIT09IHVuZGVmaW5lZCkgZXhpc3RpbmcucGFnZUNvdW50ID0gZG9jdW1lbnRJbmZvLnBhZ2VDb3VudDtcbiAgICBpZiAoZG9jdW1lbnRJbmZvLmZpbGVTaXplICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLmZpbGVTaXplID0gZG9jdW1lbnRJbmZvLmZpbGVTaXplO1xuICAgIGlmIChkb2N1bWVudEluZm8uc3RhdHVzICE9PSB1bmRlZmluZWQpIGV4aXN0aW5nLnN0YXR1cyA9IGRvY3VtZW50SW5mby5zdGF0dXM7XG4gICAgaWYgKGRvY3VtZW50SW5mby5maWxlbmFtZSAhPT0gdW5kZWZpbmVkKSBleGlzdGluZy5maWxlbmFtZSA9IGRvY3VtZW50SW5mby5maWxlbmFtZTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBVcGRhdGVkIGRvYyAke2RvY3VtZW50SW5mby5pZH0gXHUyMDE0IHN0YXR1cz0ke2V4aXN0aW5nLnN0YXR1c30sIGNodW5rcz0ke2V4aXN0aW5nLmNodW5rQ291bnR9YCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKHtcbiAgICBpZDogZG9jdW1lbnRJbmZvLmlkLFxuICAgIGZpbGVuYW1lOiBkb2N1bWVudEluZm8uZmlsZW5hbWUsXG4gICAgZmlsZVNpemU6IGRvY3VtZW50SW5mby5maWxlU2l6ZSxcbiAgICBwYWdlQ291bnQ6IGRvY3VtZW50SW5mby5wYWdlQ291bnQsXG4gICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIGNodW5rQ291bnQ6IGRvY3VtZW50SW5mby5jaHVua0NvdW50ID8/IDAsXG4gICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICBzdGF0dXM6IGRvY3VtZW50SW5mby5zdGF0dXMgPz8gJ2luZGV4aW5nJ1xuICB9KTtcbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICBjb25zb2xlLmxvZyhgW3Nlc3Npb25dIEFkZGVkIGRvYyAke2RvY3VtZW50SW5mby5pZH0gXHUyMDE0IHN0YXR1cz0ke2RvY3VtZW50SW5mby5zdGF0dXMgPz8gJ2luZGV4aW5nJ31gKTtcbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjYW5BY2NlcHRVcGxvYWQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIHsgY2FuVXBsb2FkOiBmYWxzZSwgcmVhc29uOiAnU2Vzc2lvbiBub3QgZm91bmQnIH07XG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aDtcbiAgaWYgKHVwbG9hZGVkQ291bnQgPj0gTUFYX1BERlNfUEVSX1NFU1NJT04pIHtcbiAgICByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246IGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gIH07XG4gIH1cbiAgcmV0dXJuIHsgY2FuVXBsb2FkOiB0cnVlIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVVwbG9hZChzZXNzaW9uSWQsIGZpbGUsIGZpbGVuYW1lKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGNvbnN0IGVycm9ycyA9IFtdO1xuXG4gIGlmIChmaWxlLnNpemUgPiBNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIGV4Y2VlZHMgJHtNQVhfVVBMT0FEX1NJWkVfTUJ9TUIgbGltaXRgKTtcbiAgfVxuXG4gIGNvbnN0IHVwbG9hZGVkQ291bnQgPSBzZXNzaW9uXG4gICAgPyBzZXNzaW9uLmRvY3VtZW50cy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdzZXNzaW9uX3VwbG9hZCcpLmxlbmd0aFxuICAgIDogMDtcblxuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIGVycm9ycy5wdXNoKGBNYXhpbXVtICR7TUFYX1BERlNfUEVSX1NFU1NJT059IFBERnMgcGVyIHNlc3Npb25gKTtcbiAgfVxuXG4gIGlmIChzZXNzaW9uICYmIHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBmaWxlbmFtZSkpIHtcbiAgICBlcnJvcnMucHVzaChgRmlsZSBcIiR7ZmlsZW5hbWV9XCIgYWxyZWFkeSBleGlzdHMgaW4gdGhpcyBzZXNzaW9uYCk7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGlzVmFsaWQ6IGVycm9ycy5sZW5ndGggPT09IDAsXG4gICAgZXJyb3JzLFxuICAgIGlzTGFyZ2VGaWxlOiBmaWxlLnNpemUgPiAoTUFYX1VQTE9BRF9TSVpFX01CICogMTAyNCAqIDEwMjQgKiAwLjYpXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBmYWxzZTtcbiAgY29uc3QgaWR4ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmluZEluZGV4KGQgPT4gZC5pZCA9PT0gZG9jdW1lbnRJZCk7XG4gIGlmIChpZHggPj0gMCkge1xuICAgIHNlc3Npb24uZG9jdW1lbnRzLnNwbGljZShpZHgsIDEpO1xuICAgIHNlc3Npb24uZGVsZXRlZERvY3VtZW50SWRzLmFkZChkb2N1bWVudElkKTtcbiAgICBzZXNzaW9uLmxhc3RBY2Nlc3NlZCA9IG5ldyBEYXRlKCk7XG4gICAgY29uc29sZS5sb2coYFtzZXNzaW9uXSBSZW1vdmVkIGRvYyAke2RvY3VtZW50SWR9LCBhZGRlZCB0byBkZWxldGVkRG9jdW1lbnRJZHNgKTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuICByZXR1cm4gZmFsc2U7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIHJldHVybiBzZXNzaW9uPy5kZWxldGVkRG9jdW1lbnRJZHMgPz8gbmV3IFNldCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gW107XG4gIHJldHVybiBzZXNzaW9uLmRvY3VtZW50cztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBzZXNzaW9uRG9jdW1lbnRzOiBbXSwgZ2xvYmFsRG9jdW1lbnRzOiBbXSB9O1xuXG4gIGNvbnN0IG5vcm1hbGl6ZSA9IChkb2MpID0+ICh7XG4gICAgZG9jdW1lbnRfaWQ6IGRvYy5pZCxcbiAgICBmaWxlbmFtZTogZG9jLmZpbGVuYW1lLFxuICAgIGNodW5rX2NvdW50OiBkb2MuY2h1bmtDb3VudCA/PyAwLFxuICAgIHBhZ2VfY291bnQ6IGRvYy5wYWdlQ291bnQgPz8gMCxcbiAgICB1cGxvYWRfdGltZXN0YW1wOiBkb2MudXBsb2FkVGltZXN0YW1wIHx8IG51bGwsXG4gICAgc291cmNlX3R5cGU6IGRvYy5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ3Nlc3Npb25fdXBsb2FkJyA6ICdzZWVkJyxcbiAgICBmaWxlU2l6ZTogZG9jLmZpbGVTaXplIHx8IG51bGwsXG4gICAgc3RhdHVzOiBkb2Muc3RhdHVzID8/IG51bGxcbiAgfSk7XG5cbiAgcmV0dXJuIHtcbiAgICBzZXNzaW9uRG9jdW1lbnRzOiBzZXNzaW9uLmRvY3VtZW50c1xuICAgICAgLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJylcbiAgICAgIC5tYXAobm9ybWFsaXplKSxcbiAgICBnbG9iYWxEb2N1bWVudHM6IGdsb2JhbERvY3VtZW50c0NhY2hlXG4gICAgICAubWFwKG5vcm1hbGl6ZSlcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25TdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgcmV0dXJuIHtcbiAgICBpZDogc2Vzc2lvbi5pZCxcbiAgICBkb2N1bWVudENvdW50OiBzZXNzaW9uLmRvY3VtZW50cy5sZW5ndGggKyBnbG9iYWxEb2N1bWVudHNDYWNoZS5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBzZXNzaW9uLmNyZWF0ZWRBdCxcbiAgICBsYXN0QWNjZXNzZWQ6IHNlc3Npb24ubGFzdEFjY2Vzc2VkLFxuICAgIHRvdGFsU2l6ZTogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmZpbGVTaXplIHx8IDApLCAwKSxcbiAgICB0b3RhbENodW5rczogc2Vzc2lvbi5kb2N1bWVudHMucmVkdWNlKChzdW0sIGQpID0+IHN1bSArIChkLmNodW5rQ291bnQgfHwgMCksIDApXG4gICAgICArIGdsb2JhbERvY3VtZW50c0NhY2hlLnJlZHVjZSgoc3VtLCBkKSA9PiBzdW0gKyAoZC5jaHVua0NvdW50IHx8IDApLCAwKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdEFjdGl2ZVNlc3Npb25zKCkge1xuICByZXR1cm4gQXJyYXkuZnJvbShzZXNzaW9ucy52YWx1ZXMoKSkuZmlsdGVyKHMgPT4gIWlzU2Vzc2lvbkV4cGlyZWQocykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW51cEV4cGlyZWRTZXNzaW9ucygpIHtcbiAgbGV0IGNsZWFuZWQgPSAwO1xuICBmb3IgKGNvbnN0IFtpZCwgc2Vzc2lvbl0gb2Ygc2Vzc2lvbnMpIHtcbiAgICBpZiAoaXNTZXNzaW9uRXhwaXJlZChzZXNzaW9uKSkge1xuICAgICAgc2Vzc2lvbnMuZGVsZXRlKGlkKTtcbiAgICAgIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBjbGVhbmVkKys7XG4gICAgfVxuICB9XG4gIHJldHVybiBjbGVhbmVkO1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IG1lbW9yeU1hcCA9IG5ldyBNYXAoKTtcbmNvbnN0IERFRkFVTFRfTUVNT1JZX1dJTkRPVyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IDEwO1xuXG5leHBvcnQgZnVuY3Rpb24gaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpIHtcbiAgaWYgKCFtZW1vcnlNYXAuaGFzKHNlc3Npb25JZCkpIHtcbiAgICBtZW1vcnlNYXAuc2V0KHNlc3Npb25JZCwge1xuICAgICAgdHVybnM6IFtdLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpXG4gICAgfSk7XG4gIH1cbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBtZXRhZGF0YSA9IHt9KSB7XG4gIGNvbnN0IG1lbW9yeSA9IG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IG1heFR1cm5zID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuXG4gIGNvbnN0IHR1cm4gPSB7XG4gICAgaWQ6IGB0dXJuXyR7RGF0ZS5ub3coKX1fJHtNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zdWJzdHIoMiwgOSl9YCxcbiAgICByb2xlLFxuICAgIGNvbnRlbnQsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLFxuICAgIC4uLm1ldGFkYXRhXG4gIH07XG5cbiAgbWVtb3J5LnR1cm5zLnB1c2godHVybik7XG5cbiAgaWYgKG1lbW9yeS50dXJucy5sZW5ndGggPiBtYXhUdXJucykge1xuICAgIG1lbW9yeS50dXJucyA9IG1lbW9yeS50dXJucy5zbGljZSgtbWF4VHVybnMpO1xuICB9XG5cbiAgcmV0dXJuIHR1cm47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCBtYXhUdXJucyA9IG51bGwpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGNvbnN0IGxpbWl0ID0gbWF4VHVybnMgfHwgcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgREVGQVVMVF9NRU1PUllfV0lORE9XO1xuICByZXR1cm4gbWVtb3J5LnR1cm5zLnNsaWNlKC1saW1pdCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRDb252ZXJzYXRpb25Db250ZXh0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIHJldHVybiB0dXJucy5tYXAodCA9PiAoe1xuICAgIHJvbGU6IHQucm9sZSxcbiAgICBjb250ZW50OiB0LmNvbnRlbnRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkge1xuICBjb25zdCB0dXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCk7XG4gIGlmICh0dXJucy5sZW5ndGggPT09IDApIHJldHVybiAnJztcblxuICByZXR1cm4gdHVybnMubWFwKHQgPT4ge1xuICAgIGNvbnN0IHByZWZpeCA9IHQucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXI6JyA6ICdBc3Npc3RhbnQ6JztcbiAgICByZXR1cm4gYCR7cHJlZml4fSAke3QuY29udGVudH1gO1xuICB9KS5qb2luKCdcXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFyTWVtb3J5KHNlc3Npb25JZCkge1xuICBtZW1vcnlNYXAuZGVsZXRlKHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRNZW1vcnlTdGF0cyhzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIHJldHVybiB7XG4gICAgdHVybkNvdW50OiBtZW1vcnkudHVybnMubGVuZ3RoLFxuICAgIGNyZWF0ZWRBdDogbWVtb3J5LmNyZWF0ZWRBdCxcbiAgICBsYXN0VHVybkF0OiBtZW1vcnkudHVybnMubGVuZ3RoID4gMCA/IG1lbW9yeS50dXJuc1ttZW1vcnkudHVybnMubGVuZ3RoIC0gMV0udGltZXN0YW1wIDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCBjaXRhdGlvbnMgPSBbXSwgY292ZXJhZ2UgPSBudWxsLCBhbnN3ZXJJZCA9IG51bGwpIHtcbiAgcmV0dXJuIGFkZFR1cm4oc2Vzc2lvbklkLCByb2xlLCBjb250ZW50LCB7XG4gICAgLi4uKGFuc3dlcklkICYmIHsgaWQ6IGFuc3dlcklkIH0pLFxuICAgIGNpdGF0aW9ucyxcbiAgICBjb3ZlcmFnZSxcbiAgICBoYXNDaXRhdGlvbnM6IGNpdGF0aW9ucy5sZW5ndGggPiAwXG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdFVzZXJNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ3VzZXInKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TGFzdEFzc2lzdGFudE1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAnYXNzaXN0YW50JykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjsgIGltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuICBpbXBvcnQgbXVsdGVyIGZyb20gJ211bHRlcic7XG4gIGltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuICBpbXBvcnQgZnMgZnJvbSAnZnMnO1xuICBpbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcbiAgaW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG4gIGltcG9ydCBwZGYgZnJvbSAncGRmLXBhcnNlJztcbiAgaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG4gIGltcG9ydCB7IHNhbml0aXplRmlsZW5hbWUgfSBmcm9tICcuLi91dGlscy9zYW5pdGl6ZS5qcyc7XG4gIGltcG9ydCB7XG4gICAgQ29ycnVwdGVkUERGRXJyb3IsXG4gICAgSW52YWxpZEZpbGVUeXBlRXJyb3IsXG4gIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcbiAgaW1wb3J0IHsgZ2V0Q29sbGVjdGlvbiwgYWRkVmVjdG9ycywgZGVsZXRlRG9jdW1lbnRWZWN0b3JzIH0gZnJvbSAnLi4vc2VydmljZXMvY2hyb21hU2VydmljZS5qcyc7XG4gIGltcG9ydCB7IGNodW5rVGV4dCwgY2xlYW5UZXh0IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG4gIGltcG9ydCB7IGVtYmVkU2luZ2xlQmF0Y2hHcm91cCB9IGZyb20gJy4uL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuICBpbXBvcnQge1xuICAgIGdldE9yQ3JlYXRlU2Vzc2lvbixcbiAgICBhZGREb2N1bWVudFRvU2Vzc2lvbixcbiAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uLFxuICAgIGdldEFsbERvY3VtZW50cyxcbiAgICBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzLFxuICAgIGlzU2Vzc2lvblNlZWRlZFxuICB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbiAgaW1wb3J0IHsgY2xlYXJNZW1vcnkgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcblxuICBjb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuICBjb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuICBjb25zdCBfX2Rpcm5hbWUgPSBwYXRoLmRpcm5hbWUoX19maWxlbmFtZSk7XG5cbiAgY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG4gIGlmICghZnMuZXhpc3RzU3luYyh1cGxvYWREaXIpKSB7XG4gICAgZnMubWtkaXJTeW5jKHVwbG9hZERpciwgeyByZWN1cnNpdmU6IHRydWUgfSk7XG4gIH1cblxuICAvLyBTZWVkIGRvY3VtZW50cyBkaXJlY3RvcnkgLSB3b3JrcyBpbiBib3RoIGRldiBhbmQgc2VydmVybGVzc1xuICAvLyBJbiBkZXY6IHNlcnZlci9hcGkvLi4vLi4vc2VlZF9kb2N1bWVudHNcbiAgLy8gSW4gc2VydmVybGVzczogbmV0bGlmeS9mdW5jdGlvbnMvLi4vLi4vc2VlZF9kb2N1bWVudHMgKGNvcGllZCB0byBkaXN0KVxuICBsZXQgc2VlZERpciA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuLi8uLi9zZWVkX2RvY3VtZW50cycpO1xuICBpZiAoIWZzLmV4aXN0c1N5bmMoc2VlZERpcikpIHtcbiAgICAvLyBUcnkgYWx0ZXJuYXRpdmUgcGF0aCBmb3Igc2VydmVybGVzcyBkZXBsb3ltZW50XG4gICAgc2VlZERpciA9IHBhdGgucmVzb2x2ZShwcm9jZXNzLmN3ZCgpLCAnc2VlZF9kb2N1bWVudHMnKTtcbiAgfVxuICBpZiAoIWZzLmV4aXN0c1N5bmMoc2VlZERpcikpIHtcbiAgICAvLyBUcnkgZGlzdCBmb2xkZXIgZm9yIGRlcGxveWVkIHN0YXRpYyBmaWxlc1xuICAgIHNlZWREaXIgPSBwYXRoLnJlc29sdmUocHJvY2Vzcy5jd2QoKSwgJ2Rpc3Qvc2VlZF9kb2N1bWVudHMnKTtcbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBTU0UgZXZlbnQgaGVscGVyIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICBmdW5jdGlvbiBzc2VFdmVudChyZXMsIGV2ZW50LCBkYXRhKSB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5kYXRhOiAke0pTT04uc3RyaW5naWZ5KGRhdGEpfVxcblxcbmApO1xuICB9XG5cbiAgY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gICAgZGVzdGluYXRpb246IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCB1cGxvYWREaXIpLFxuICAgIGZpbGVuYW1lOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgc2FuaXRpemVGaWxlbmFtZShmaWxlLm9yaWdpbmFsbmFtZSkpXG4gIH0pO1xuXG4gIGNvbnN0IHVwbG9hZCA9IG11bHRlcih7XG4gICAgc3RvcmFnZSxcbiAgICBsaW1pdHM6IHsgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjQgfSxcbiAgICBmaWxlRmlsdGVyOiAocmVxLCBmaWxlLCBjYikgPT4ge1xuICAgICAgaWYgKGZpbGUubWltZXR5cGUgPT09ICdhcHBsaWNhdGlvbi9wZGYnICYmIHBhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSkudG9Mb3dlckNhc2UoKSA9PT0gJy5wZGYnKSB7XG4gICAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY2IobmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCkpO1xuICAgICAgfVxuICAgIH1cbiAgfSk7XG5cbiAgZnVuY3Rpb24gY29udGVudERpc3Bvc2l0aW9uKGRpc3BsYXlOYW1lKSB7XG4gICAgY29uc3QgZW5jb2RlZCA9IGVuY29kZVVSSUNvbXBvbmVudChkaXNwbGF5TmFtZSlcbiAgICAgIC5yZXBsYWNlKC8nL2csICclMjcnKVxuICAgICAgLnJlcGxhY2UoL1xcKC9nLCAnJTI4JylcbiAgICAgIC5yZXBsYWNlKC9cXCkvZywgJyUyOScpO1xuICAgIHJldHVybiBgaW5saW5lOyBmaWxlbmFtZT1cImRvY3VtZW50LnBkZlwiOyBmaWxlbmFtZSo9VVRGLTgnJyR7ZW5jb2RlZH1gO1xuICB9XG5cbiAgLyoqXG4gICAqIEpvaW4gcGRmLmpzIHRleHQtY29udGVudCBpdGVtcyBpbnRvIGEgc2luZ2xlIHN0cmluZyB1c2luZyBlYWNoIGl0ZW0nc1xuICAgKiB4LXBvc2l0aW9uICh0cmFuc2Zvcm1bNF0pIGFuZCB3aWR0aCB0byBkZWNpZGUgd2hldGhlciBhIHNwYWNlIGJlbG9uZ3NcbiAgICogYmV0d2VlbiB0d28gaXRlbXMsIGluc3RlYWQgb2YgYWx3YXlzIGpvaW5pbmcgd2l0aCBhIHNpbmdsZSBzcGFjZS5cbiAgICpcbiAgICogVGhpcyBhdm9pZHMgdHdvIGNvbW1vbiBhcnRpZmFjdHMgZnJvbSBuYWl2ZSBgLmpvaW4oJyAnKWA6XG4gICAqICAtIHdvcmRzIHNwbGl0IGFjcm9zcyBhZGphY2VudCB0ZXh0IHJ1bnMgZ2V0dGluZyBhIHBoYW50b20gc3BhY2VcbiAgICogICAgaW5zZXJ0ZWQgaW4gdGhlIG1pZGRsZSAoZS5nLiBcIlNhdiBpbmdzXCIpXG4gICAqICAtIGFkamFjZW50IHdvcmRzIHdpdGggbm8gc3BhY2UgaW4gdGhlIFBERidzIGludGVybmFsIHJ1bnMgZ2V0dGluZ1xuICAgKiAgICBnbHVlZCB0b2dldGhlciAoZS5nLiBcInRoZSByZXBvcnRcIiAtPiBcInRoZXJlcG9ydFwiKVxuICAgKlxuICAgKiBFbXB0eS1zdHJpbmcgaXRlbXMgYXJlIHBkZi5qcydzIHNpZ25hbCBmb3IgYSBsaW5lIGJyZWFrLCB3aGljaCB3ZVxuICAgKiBjb252ZXJ0IHRvIGEgbmV3bGluZSBzbyBwYXJhZ3JhcGggc3RydWN0dXJlIGlzbid0IGxvc3QuXG4gICAqL1xuICBmdW5jdGlvbiBqb2luVGV4dEl0ZW1zKGl0ZW1zKSB7XG4gICAgbGV0IG91dCA9ICcnO1xuICAgIGxldCBwcmV2SXRlbSA9IG51bGw7XG5cbiAgICBmb3IgKGNvbnN0IGl0ZW0gb2YgaXRlbXMpIHtcbiAgICAgIGNvbnN0IHN0ciA9IGl0ZW0uc3RyO1xuICAgICAgaWYgKHN0ciA9PT0gdW5kZWZpbmVkKSB7IHByZXZJdGVtID0gaXRlbTsgY29udGludWU7IH1cblxuICAgICAgaWYgKHN0ciA9PT0gJycpIHtcbiAgICAgICAgLy8gcGRmLmpzIGVtaXRzIGVtcHR5IGl0ZW1zIHRvIHNpZ25hbCBsaW5lIGJyZWFrc1xuICAgICAgICBpZiAoIS9cXG4kLy50ZXN0KG91dCkpIG91dCArPSAnXFxuJztcbiAgICAgICAgcHJldkl0ZW0gPSBudWxsO1xuICAgICAgICBjb250aW51ZTtcbiAgICAgIH1cblxuICAgICAgaWYgKHByZXZJdGVtICYmIHByZXZJdGVtLnN0cikge1xuICAgICAgICBjb25zdCBwcmV2RW5kID0gcHJldkl0ZW0udHJhbnNmb3JtWzRdICsgKHByZXZJdGVtLndpZHRoIHx8IDApO1xuICAgICAgICBjb25zdCBjdXJTdGFydCA9IGl0ZW0udHJhbnNmb3JtWzRdO1xuICAgICAgICBjb25zdCBnYXAgPSBjdXJTdGFydCAtIHByZXZFbmQ7XG4gICAgICAgIGNvbnN0IGZvbnRIID0gTWF0aC5hYnMoaXRlbS50cmFuc2Zvcm1bM10pIHx8IDEwO1xuICAgICAgICBjb25zdCBzcGFjZVRocmVzaG9sZCA9IGZvbnRIICogMC4yNTtcblxuICAgICAgICBjb25zdCBhbHJlYWR5U3BhY2VkID0gL1xccyQvLnRlc3Qob3V0KSB8fCAvXlxccy8udGVzdChzdHIpO1xuICAgICAgICBpZiAoIWFscmVhZHlTcGFjZWQgJiYgZ2FwID4gc3BhY2VUaHJlc2hvbGQpIHtcbiAgICAgICAgICBvdXQgKz0gJyAnO1xuICAgICAgICB9XG4gICAgICAgIC8vIGVsc2U6IGl0ZW1zIGFyZSB0b3VjaGluZy9vdmVybGFwcGluZyAtPiBzYW1lIHdvcmQsIG5vIHNwYWNlIGluc2VydGVkXG4gICAgICB9XG5cbiAgICAgIG91dCArPSBzdHI7XG4gICAgICBwcmV2SXRlbSA9IGl0ZW07XG4gICAgfVxuXG4gICAgcmV0dXJuIG91dDtcbiAgfVxuXG4gIGFzeW5jIGZ1bmN0aW9uIHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGVQYXRoKSB7XG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGJ1ZmZlciA9IGZzLnJlYWRGaWxlU3luYyhmaWxlUGF0aCk7XG5cbiAgICAgIGNvbnN0IHBhZ2VzID0gW107XG4gICAgICBhd2FpdCBwZGYoYnVmZmVyLCB7XG4gICAgICAgIHBhZ2VyZW5kZXI6IChwYWdlRGF0YSkgPT4ge1xuICAgICAgICAgIHJldHVybiBwYWdlRGF0YS5nZXRUZXh0Q29udGVudCgpLnRoZW4odGMgPT4ge1xuICAgICAgICAgICAgY29uc3QgcGFnZVRleHQgPSBqb2luVGV4dEl0ZW1zKHRjLml0ZW1zKTtcbiAgICAgICAgICAgIHBhZ2VzLnB1c2gocGFnZVRleHQpO1xuICAgICAgICAgICAgcmV0dXJuIHBhZ2VUZXh0O1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgaWYgKHBhZ2VzLmxlbmd0aCA9PT0gMCB8fCBwYWdlcy5ldmVyeShwID0+ICFwLnRyaW0oKSkpIHtcbiAgICAgICAgY29uc3QgZnVsbCA9IGF3YWl0IHBkZihidWZmZXIpO1xuICAgICAgICBwYWdlcy5wdXNoKGZ1bGwudGV4dCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHRvdGFsUGFnZXMgPSBwYWdlcy5sZW5ndGg7XG4gICAgICBjb25zdCBjbGVhbmVkUGFnZXMgPSBwYWdlcy5tYXAocCA9PiBjbGVhblRleHQocCkpO1xuICAgICAgY29uc3QgcGFnZU1hcCA9IFtdO1xuICAgICAgbGV0IGNoYXJQb3MgPSAwO1xuXG4gICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGNsZWFuZWRQYWdlcy5sZW5ndGg7IGkrKykge1xuICAgICAgICBwYWdlTWFwLnB1c2goeyBwYWdlOiBpICsgMSwgc3RhcnQ6IGNoYXJQb3MsIGVuZDogY2hhclBvcyArIGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggfSk7XG4gICAgICAgIGNoYXJQb3MgKz0gY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCArIDE7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGZ1bGxUZXh0ID0gY2xlYW5lZFBhZ2VzLmpvaW4oJ1xcbicpO1xuICAgICAgcmV0dXJuIHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignUERGIHBhcnNpbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgICAgdGhyb3cgbmV3IENvcnJ1cHRlZFBERkVycm9yKCk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEdpdmVuIGEgY2h1bmsncyBbY2hhclN0YXJ0LCBjaGFyRW5kKSByYW5nZSwgZmluZCB3aGljaCBwYWdlKHMpIGl0XG4gICAqIG92ZXJsYXBzLiBSZXR1cm5zIHRoZSBtYWpvcml0eSBwYWdlIChtb3N0IG92ZXJsYXBwaW5nIGNoYXJzLCB1c2VkXG4gICAqIGZvciBgcGFnZV9udW1iZXJgIGZvciBiYWNrd2FyZCBjb21wYXRpYmlsaXR5KSBwbHVzIHRoZSB0cnVlIHN0YXJ0L2VuZFxuICAgKiBwYWdlcyBzbyBjaHVua3Mgc3Bhbm5pbmcgYSBwYWdlIGJyZWFrIGFyZW4ndCBzaWxlbnRseSBtaXNsYWJlbGVkIHdpdGhcbiAgICoganVzdCB0aGUgZmlyc3QgcGFnZS5cbiAgICovXG4gIGZ1bmN0aW9uIGdldFBhZ2VSYW5nZShjaGFyU3RhcnQsIGNoYXJFbmQsIHBhZ2VNYXApIHtcbiAgICBsZXQgc3RhcnRQYWdlID0gbnVsbDtcbiAgICBsZXQgZW5kUGFnZSA9IG51bGw7XG4gICAgbGV0IGJlc3RQYWdlID0gbnVsbDtcbiAgICBsZXQgbWF4T3ZlcmxhcCA9IC0xO1xuXG4gICAgZm9yIChjb25zdCBlbnRyeSBvZiBwYWdlTWFwKSB7XG4gICAgICBjb25zdCBvdmVybGFwU3RhcnQgPSBNYXRoLm1heChjaGFyU3RhcnQsIGVudHJ5LnN0YXJ0KTtcbiAgICAgIGNvbnN0IG92ZXJsYXBFbmQgPSBNYXRoLm1pbihjaGFyRW5kLCBlbnRyeS5lbmQpO1xuICAgICAgY29uc3Qgb3ZlcmxhcCA9IG92ZXJsYXBFbmQgLSBvdmVybGFwU3RhcnQ7XG4gICAgICBpZiAob3ZlcmxhcCA8PSAwKSBjb250aW51ZTtcblxuICAgICAgaWYgKHN0YXJ0UGFnZSA9PT0gbnVsbCkgc3RhcnRQYWdlID0gZW50cnkucGFnZTtcbiAgICAgIGVuZFBhZ2UgPSBlbnRyeS5wYWdlO1xuXG4gICAgICBpZiAob3ZlcmxhcCA+IG1heE92ZXJsYXApIHtcbiAgICAgICAgbWF4T3ZlcmxhcCA9IG92ZXJsYXA7XG4gICAgICAgIGJlc3RQYWdlID0gZW50cnkucGFnZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoc3RhcnRQYWdlID09PSBudWxsKSB7XG4gICAgICBjb25zdCBsYXN0UGFnZSA9IHBhZ2VNYXBbcGFnZU1hcC5sZW5ndGggLSAxXT8ucGFnZSB8fCAxO1xuICAgICAgcmV0dXJuIHsgcGFnZTogbGFzdFBhZ2UsIHBhZ2VTdGFydDogbGFzdFBhZ2UsIHBhZ2VFbmQ6IGxhc3RQYWdlIH07XG4gICAgfVxuXG4gICAgcmV0dXJuIHsgcGFnZTogYmVzdFBhZ2UsIHBhZ2VTdGFydDogc3RhcnRQYWdlLCBwYWdlRW5kOiBlbmRQYWdlIH07XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgVXBsb2FkIGhhbmRsZXIgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gICAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG4gICAgcmVzLmZsdXNoSGVhZGVycygpO1xuXG4gICAgY29uc3QgQkFUQ0hfU0laRSA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19CQVRDSF9NQVhfQ0hVTktTKSB8fCAxMDtcbiAgICBjb25zdCBQQVJBTExFTF9DQUxMUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19QQVJBTExFTF9DQUxMUykgfHwgMTA7XG4gICAgY29uc3QgR1JPVVBfV0FJVF9NUyA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19HUk9VUF9XQUlUX01TKSB8fCAxO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICAgIGlmICghZmlsZSkgdGhyb3cgbmV3IEludmFsaWRGaWxlVHlwZUVycm9yKCk7XG5cbiAgICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgICBjb25zdCBzZXNzaW9uID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgICBjb25zdCBtYXhQREZzID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUFYX1BERlNfUEVSX1NFU1NJT04gfHwgJzMnKTtcbiAgICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgICAgaWYgKHVwbG9hZGVkQ291bnQgPj0gbWF4UERGcykge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgTWF4aW11bSAke21heFBERnN9IHVwbG9hZHMgcmVhY2hlZGAsIGNvZGU6ICdUT09fTUFOWV9QREZTJyB9KTtcbiAgICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICAgIH1cblxuICAgICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBgXCIke2NsZWFuRmlsZW5hbWV9XCIgYWxyZWFkeSB1cGxvYWRlZGAsIGNvZGU6ICdEVVBMSUNBVEVfRklMRScgfSk7XG4gICAgICAgIHJldHVybiByZXMuZW5kKCk7XG4gICAgICB9XG5cbiAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBQaGFzZSAxIFx1MjAxNCBwYXJzaW5nICR7Y2xlYW5GaWxlbmFtZX0gKCR7ZmlsZS5zaXplfSBieXRlcylgKTtcbiAgICAgIGNvbnN0IHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfSA9IGF3YWl0IHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGUucGF0aCk7XG5cbiAgICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgICAgc3NlRXZlbnQocmVzLCAnZXJyb3InLCB7IG1lc3NhZ2U6ICdObyBleHRyYWN0YWJsZSB0ZXh0IFx1MjAxNCBQREYgbWF5IGJlIHNjYW5uZWQgb3IgaW1hZ2Utb25seScsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgICAgICByZXR1cm4gcmVzLmVuZCgpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCBkb2N1bWVudElkID0gdXVpZHY0KCk7XG4gICAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQpO1xuXG4gICAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICAgICAgcmV0dXJuIHJlcy5lbmQoKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgY2h1bmtzID0gcmF3Q2h1bmtzLm1hcCgoY2h1bmssIGlkeCkgPT4ge1xuICAgICAgICBjb25zdCB7IHBhZ2UsIHBhZ2VTdGFydCwgcGFnZUVuZCB9ID0gZ2V0UGFnZVJhbmdlKGNodW5rLmNoYXJTdGFydCwgY2h1bmsuY2hhckVuZCwgcGFnZU1hcCk7XG4gICAgICAgIHJldHVybiB7XG4gICAgICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgICAgICBtZXRhZGF0YToge1xuICAgICAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2lkOiBjcmVhdGVIYXNoKCdtZDUnKS51cGRhdGUoYCR7Y2xlYW5GaWxlbmFtZX06OiR7Y2h1bmsudGV4dH1gKS5kaWdlc3QoJ2hleCcpLnNsaWNlKDAsIDE2KSxcbiAgICAgICAgICAgIGNodW5rX2luZGV4OiBpZHgsXG4gICAgICAgICAgICB0b3RhbF9jaHVua3M6IHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgICAgICBwYWdlX251bWJlcjogcGFnZSwgICAgICAgLy8gbWFqb3JpdHkgcGFnZSBcdTIwMTQga2VwdCBmb3IgYmFja3dhcmQgY29tcGF0aWJpbGl0eVxuICAgICAgICAgICAgcGFnZV9zdGFydDogcGFnZVN0YXJ0LCAgIC8vIG5ldzogZmlyc3QgcGFnZSB0aGlzIGNodW5rIG92ZXJsYXBzXG4gICAgICAgICAgICBwYWdlX2VuZDogcGFnZUVuZCwgICAgICAgLy8gbmV3OiBsYXN0IHBhZ2UgdGhpcyBjaHVuayBvdmVybGFwc1xuICAgICAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMsXG4gICAgICAgICAgICBzb3VyY2VfdHlwZTogJ3Nlc3Npb25fdXBsb2FkJyxcbiAgICAgICAgICAgIHNlc3Npb25faWQ6IHNlc3Npb25JZCxcbiAgICAgICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgICAgIGNoYXJfc3RhcnQ6IGNodW5rLmNoYXJTdGFydCxcbiAgICAgICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgICAgICB9XG4gICAgICAgIH07XG4gICAgICB9KTtcblxuICAgICAgY29uc3QgdG90YWxDaHVua3MgPSBjaHVua3MubGVuZ3RoO1xuICAgICAgY29uc3QgdG90YWxCYXRjaGVzID0gTWF0aC5jZWlsKHRvdGFsQ2h1bmtzIC8gQkFUQ0hfU0laRSk7XG4gICAgICBjb25zdCB0b3RhbFNldHMgPSBNYXRoLmNlaWwodG90YWxCYXRjaGVzIC8gUEFSQUxMRUxfQ0FMTFMpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gJHt0b3RhbENodW5rc30gY2h1bmtzIFx1MjE5MiAke3RvdGFsQmF0Y2hlc30gQVBJIGNhbGxzIFx1MjE5MiAke3RvdGFsU2V0c30gc2V0cyBvZiAke1BBUkFMTEVMX0NBTExTfSBwYXJhbGxlbGApO1xuXG4gICAgICBzc2VFdmVudChyZXMsICd1cGxvYWRfY29tcGxldGUnLCB7XG4gICAgICAgIGRvY3VtZW50SWQsIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLCBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIHRvdGFsQ2h1bmtzLCB0b3RhbEJhdGNoZXMsIHRvdGFsU2V0c1xuICAgICAgfSk7XG5cbiAgICAgIGFkZERvY3VtZW50VG9TZXNzaW9uKHNlc3Npb25JZCwge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCwgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcywgY2h1bmtDb3VudDogMCwgc3RhdHVzOiAnaW5kZXhpbmcnXG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFBoYXNlIDEgZG9uZSBcdTIwMTQgJHtjbGVhbkZpbGVuYW1lfSBhZGRlZCB0byBzZXNzaW9uIGFzIGluZGV4aW5nYCk7XG5cbiAgICAgIGNvbnN0IHsgY29sbGVjdGlvbiB9ID0gYXdhaXQgZ2V0Q29sbGVjdGlvbigpO1xuICAgICAgbGV0IHByb2Nlc3NlZENodW5rcyA9IDA7XG4gICAgICBjb25zdCBhbGxFbWJlZGRpbmdzID0gW107XG5cbiAgICAgIGNvbnN0IGJhdGNoZXMgPSBbXTtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBCQVRDSF9TSVpFKSBiYXRjaGVzLnB1c2goY2h1bmtzLnNsaWNlKGksIGkgKyBCQVRDSF9TSVpFKSk7XG5cbiAgICAgIGNvbnN0IHNldHMgPSBbXTtcbiAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgYmF0Y2hlcy5sZW5ndGg7IGkgKz0gUEFSQUxMRUxfQ0FMTFMpIHNldHMucHVzaChiYXRjaGVzLnNsaWNlKGksIGkgKyBQQVJBTExFTF9DQUxMUykpO1xuXG4gICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gUGhhc2UgMiBzdGFydCBcdTIwMTQgJHtzZXRzLmxlbmd0aH0gc2V0c2ApO1xuXG4gICAgICBmb3IgKGxldCBzZXRJZHggPSAwOyBzZXRJZHggPCBzZXRzLmxlbmd0aDsgc2V0SWR4KyspIHtcbiAgICAgICAgY29uc3QgaXNMYXN0U2V0ID0gc2V0SWR4ID09PSBzZXRzLmxlbmd0aCAtIDE7XG4gICAgICAgIGNvbnN0IGN1cnJlbnRTZXQgPSBzZXRzW3NldElkeF07XG4gICAgICAgIGNvbnN0IHNldENodW5rQ291bnQgPSBjdXJyZW50U2V0LnJlZHVjZSgoYWNjLCBiKSA9PiBhY2MgKyBiLmxlbmd0aCwgMCk7XG5cbiAgICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFNldCAke3NldElkeCArIDF9LyR7c2V0cy5sZW5ndGh9IFx1MjAxNCBlbWJlZGRpbmcgJHtjdXJyZW50U2V0Lmxlbmd0aH0gYmF0Y2ggY2FsbChzKSAoJHtzZXRDaHVua0NvdW50fSBjaHVua3MpIGluIHBhcmFsbGVsYCk7XG5cbiAgICAgICAgY29uc3QgZW1iZWRSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgICAgIGN1cnJlbnRTZXQubWFwKGJhdGNoID0+IGVtYmVkU2luZ2xlQmF0Y2hHcm91cChiYXRjaC5tYXAoYyA9PiBjLnRleHQpKSlcbiAgICAgICAgKTtcblxuICAgICAgICBjb25zdCBzZXRFbWJlZGRpbmdzID0gW107XG4gICAgICAgIGVtYmVkUmVzdWx0cy5mb3JFYWNoKChyZXN1bHQsIGJhdGNoSWR4KSA9PiB7XG4gICAgICAgICAgY29uc3QgYmF0Y2ggPSBjdXJyZW50U2V0W2JhdGNoSWR4XTtcbiAgICAgICAgICBpZiAocmVzdWx0LnN0YXR1cyA9PT0gJ2Z1bGZpbGxlZCcpIHtcbiAgICAgICAgICAgIHJlc3VsdC52YWx1ZS5mb3JFYWNoKCh2ZWN0b3IsIGNodW5rSWR4KSA9PiB7XG4gICAgICAgICAgICAgIHNldEVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICAgICAgaWQ6IGJhdGNoW2NodW5rSWR4XS5tZXRhZGF0YS5jaHVua19pZCxcbiAgICAgICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcixcbiAgICAgICAgICAgICAgICBtZXRhZGF0YTogYmF0Y2hbY2h1bmtJZHhdLm1ldGFkYXRhLFxuICAgICAgICAgICAgICAgIHRleHQ6IGJhdGNoW2NodW5rSWR4XS50ZXh0XG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBlbWJlZGRlZCBPSyAoJHtiYXRjaC5sZW5ndGh9IGNodW5rcylgKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgY29uc29sZS5lcnJvcihgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gICBCYXRjaCAke3NldElkeCAqIFBBUkFMTEVMX0NBTExTICsgYmF0Y2hJZHggKyAxfSBGQUlMRUQ6YCwgcmVzdWx0LnJlYXNvbj8ubWVzc2FnZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICBwcm9jZXNzZWRDaHVua3MgKz0gc2V0RW1iZWRkaW5ncy5sZW5ndGg7XG4gICAgICAgIGFsbEVtYmVkZGluZ3MucHVzaCguLi5zZXRFbWJlZGRpbmdzKTtcblxuICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU2V0ICR7c2V0SWR4ICsgMX0gZW1iZWRkZWQgXHUyMDE0ICR7cHJvY2Vzc2VkQ2h1bmtzfS8ke3RvdGFsQ2h1bmtzfSBjaHVua3Mgc28gZmFyYCk7XG5cbiAgICAgICAgaWYgKCFpc0xhc3RTZXQpIHtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gU3RhcnRpbmcgJHtHUk9VUF9XQUlUX01TIC8gMTAwMH1zIHRpbWVyICsgQ2hyb21hIHdyaXRlIGNvbmN1cnJlbnRseSBmb3Igc2V0ICR7c2V0SWR4ICsgMX1gKTtcbiAgICAgICAgICBjb25zdCB0aW1lciA9IG5ldyBQcm9taXNlKHIgPT4gc2V0VGltZW91dChyLCBHUk9VUF9XQUlUX01TKSk7XG4gICAgICAgICAgY29uc3QgY2hyb21hV3JpdGUgPSBhZGRWZWN0b3JzKFxuICAgICAgICAgICAgY29sbGVjdGlvbixcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgICAgICAgIHNldEVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICAgICAgICApLnRoZW4oKCkgPT4gY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIENocm9tYSB3cml0ZSBkb25lIGZvciBzZXQgJHtzZXRJZHggKyAxfSAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKSlcbiAgICAgICAgICAgIC5jYXRjaChlcnIgPT4gY29uc29sZS5lcnJvcihgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIEZBSUxFRCBmb3Igc2V0ICR7c2V0SWR4ICsgMX06YCwgZXJyLm1lc3NhZ2UpKTtcblxuICAgICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgICAgd2FpdGluZ01zOiBHUk9VUF9XQUlUX01TLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiBmYWxzZVxuICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgYXdhaXQgUHJvbWlzZS5hbGwoW3RpbWVyLCBjaHJvbWFXcml0ZV0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGBbdXBsb2FkXSBbJHtzZXNzaW9uSWR9XSBUaW1lciArIENocm9tYSBib3RoIGRvbmUgZm9yIHNldCAke3NldElkeCArIDF9LCBwcm9jZWVkaW5nIHRvIHNldCAke3NldElkeCArIDJ9YCk7XG5cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gTGFzdCBzZXQgJHtzZXRJZHggKyAxfSBcdTIwMTQgYXdhaXRpbmcgQ2hyb21hIHdyaXRlIGRpcmVjdGx5YCk7XG4gICAgICAgICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgICAgICAgIGNvbGxlY3Rpb24sXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgICAgICAgc2V0RW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICAgICAgICBzZXRFbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgICAgICAgKTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW3VwbG9hZF0gWyR7c2Vzc2lvbklkfV0gQ2hyb21hIHdyaXRlIGNvbXBsZXRlIGZvciBsYXN0IHNldCAoJHtzZXRFbWJlZGRpbmdzLmxlbmd0aH0gdmVjdG9ycylgKTtcblxuICAgICAgICAgIHNzZUV2ZW50KHJlcywgJ2VtYmVkZGluZ19wcm9ncmVzcycsIHtcbiAgICAgICAgICAgIHByb2Nlc3NlZENodW5rcywgdG90YWxDaHVua3MsXG4gICAgICAgICAgICBzZXRJbmRleDogc2V0SWR4ICsgMSwgdG90YWxTZXRzLFxuICAgICAgICAgICAgd2FpdGluZ01zOiAwLCBjaHJvbWFXcml0ZUNvbXBsZXRlOiB0cnVlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLCBjaHVua0NvdW50OiBhbGxFbWJlZGRpbmdzLmxlbmd0aCwgc3RhdHVzOiAncmVhZHknXG4gICAgICB9KTtcblxuICAgICAgY29uc29sZS5sb2coYFt1cGxvYWRdIFske3Nlc3Npb25JZH1dIFx1MjcwNSBEb25lIFx1MjAxNCAke2FsbEVtYmVkZGluZ3MubGVuZ3RofSB2ZWN0b3JzIGluIENocm9tYSBmb3IgJHtjbGVhbkZpbGVuYW1lfWApO1xuXG4gICAgICBzc2VFdmVudChyZXMsICdkb25lJywge1xuICAgICAgICBkb2N1bWVudDoge1xuICAgICAgICAgIGlkOiBkb2N1bWVudElkLCBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSwgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgICBwYWdlQ291bnQ6IHRvdGFsUGFnZXMsIGNodW5rQ291bnQ6IGFsbEVtYmVkZGluZ3MubGVuZ3RoLFxuICAgICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICAgIH0sXG4gICAgICAgIHNlc3Npb25JZFxuICAgICAgfSk7XG5cbiAgICAgIHJlcy5lbmQoKTtcblxuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgICB0cnkgeyBmcy51bmxpbmtTeW5jKHJlcS5maWxlLnBhdGgpOyB9IGNhdGNoIHsgfVxuICAgICAgfVxuICAgICAgY29uc29sZS5lcnJvcignW3VwbG9hZF0gVW5oYW5kbGVkIGVycm9yOicsIGVycm9yKTtcbiAgICAgIHNzZUV2ZW50KHJlcywgJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdVcGxvYWQgZmFpbGVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJyB9KTtcbiAgICAgIHJlcy5lbmQoKTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgU1NFOiBTZWVkaW5nIHN0YXR1cyBzdHJlYW0gXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBzZWVkaW5nU3RhdHVzSGFuZGxlcihyZXEsIHJlcykge1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICAgIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgICByZXMuZmx1c2hIZWFkZXJzKCk7XG5cbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICAgIGlmICghc2Vzc2lvbklkKSB7XG4gICAgICBzc2VFdmVudChyZXMsICdlcnJvcicsIHsgbWVzc2FnZTogJ01pc3Npbmcgc2Vzc2lvbiBJRCcsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICAgICAgcmVzLmVuZCgpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnNvbGUubG9nKGBbc2VlZGluZy1zdGF0dXNdIENsaWVudCBjb25uZWN0ZWQgZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG5cbiAgICAvLyBDaGVjayBpZiBzZXNzaW9uIGlzIGFscmVhZHkgc2VlZGVkXG4gICAgY29uc3Qgc2VlZGVkID0gaXNTZXNzaW9uU2VlZGVkKHNlc3Npb25JZCk7XG4gICAgaWYgKHNlZWRlZCkge1xuICAgICAgY29uc29sZS5sb2coYFtzZWVkaW5nLXN0YXR1c10gU2Vzc2lvbiAke3Nlc3Npb25JZH0gYWxyZWFkeSBzZWVkZWQgXHUyMDEzIHJldHVybmluZyBpbW1lZGlhdGVseWApO1xuICAgICAgc3NlRXZlbnQocmVzLCAnc2VlZGluZ19jb21wbGV0ZScsIHsgc2Vzc2lvbklkLCBzZWVkZWQ6IHRydWUgfSk7XG4gICAgICByZXMuZW5kKCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgLy8gQ3JlYXRlIGEgbGlzdGVuZXIgZm9yIHRoaXMgc2Vzc2lvblxuICAgIGNvbnN0IGV2ZW50S2V5ID0gYHNlZWRpbmc6JHtzZXNzaW9uSWR9YDtcblxuICAgIC8vIFN0b3JlIHRoZSBsaXN0ZW5lciBzbyB3ZSBjYW4gZW1pdCB3aGVuIHNlZWRpbmcgY29tcGxldGVzXG4gICAgaWYgKCFnbG9iYWwuc2VlZGluZ0xpc3RlbmVycykge1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMgPSBuZXcgTWFwKCk7XG4gICAgfVxuICAgIGlmICghZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuaGFzKGV2ZW50S2V5KSkge1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuc2V0KGV2ZW50S2V5LCBbXSk7XG4gICAgfVxuICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmdldChldmVudEtleSkucHVzaChyZXMpO1xuXG4gICAgLy8gQ2xlYW4gdXAgbGlzdGVuZXIgb24gY2xpZW50IGRpc2Nvbm5lY3RcbiAgICByZXEub24oJ2Nsb3NlJywgKCkgPT4ge1xuICAgICAgY29uc3QgbGlzdGVuZXJzID0gZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZ2V0KGV2ZW50S2V5KSB8fCBbXTtcbiAgICAgIGNvbnN0IGlkeCA9IGxpc3RlbmVycy5pbmRleE9mKHJlcyk7XG4gICAgICBpZiAoaWR4ID49IDApIHtcbiAgICAgICAgbGlzdGVuZXJzLnNwbGljZShpZHgsIDEpO1xuICAgICAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBDbGllbnQgZGlzY29ubmVjdGVkIGZvciAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cbiAgICAgIGlmIChsaXN0ZW5lcnMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIGdsb2JhbC5zZWVkaW5nTGlzdGVuZXJzLmRlbGV0ZShldmVudEtleSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICAvLyBTdGFydCBzZWVkaW5nIGluIHRoZSBiYWNrZ3JvdW5kIChpZiBub3QgYWxyZWFkeSBydW5uaW5nKVxuICAgIHRyeSB7XG4gICAgICBjb25zb2xlLmxvZyhgW3NlZWRpbmctc3RhdHVzXSBUcmlnZ2VyaW5nIHNlZWRpbmcgZm9yICR7c2Vzc2lvbklkfS4uLmApO1xuICAgICAgYXdhaXQgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpO1xuICAgICAgLy8gVGhlIHNlZWRpbmcgZnVuY3Rpb24gd2lsbCBub3RpZnkgbGlzdGVuZXJzIHdoZW4gY29tcGxldGVcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoYFtzZWVkaW5nLXN0YXR1c10gU2VlZGluZyBmYWlsZWQgZm9yICR7c2Vzc2lvbklkfTpgLCBlcnIubWVzc2FnZSk7XG4gICAgICBjb25zdCBsaXN0ZW5lcnMgPSBnbG9iYWwuc2VlZGluZ0xpc3RlbmVycy5nZXQoZXZlbnRLZXkpIHx8IFtdO1xuICAgICAgbGlzdGVuZXJzLmZvckVhY2goKHJlc3BvbnNlKSA9PiB7XG4gICAgICAgIHNzZUV2ZW50KHJlc3BvbnNlLCAnZXJyb3InLCB7IG1lc3NhZ2U6IGVyci5tZXNzYWdlLCBjb2RlOiAnU0VFRF9GQUlMRUQnIH0pO1xuICAgICAgICByZXNwb25zZS5lbmQoKTtcbiAgICAgIH0pO1xuICAgICAgZ2xvYmFsLnNlZWRpbmdMaXN0ZW5lcnMuZGVsZXRlKGV2ZW50S2V5KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgTGlzdCBkb2N1bWVudHMgaGFuZGxlciBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG4gICAgdHJ5IHtcbiAgICAgIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgICAgY29uc3QgZG9jdW1lbnRzID0gZ2V0QWxsRG9jdW1lbnRzKHNlc3Npb25JZCk7XG4gICAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdMaXN0IGRvY3VtZW50cyBlcnJvcjonLCBlcnJvcik7XG4gICAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJywgY29kZTogJ0xJU1RfRVJST1InIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBEZWxldGUgZG9jdW1lbnQgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gIGV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICAgIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgICBjb25zdCBmaWxlbmFtZSA9IHJlcS5xdWVyeS5maWxlbmFtZTtcbiAgICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICAgIHRyeSB7XG4gICAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgY29uc3QgeyBjb2xsZWN0aW9uIH0gPSBhd2FpdCBnZXRDb2xsZWN0aW9uKCk7XG4gICAgICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgICAgIGF3YWl0IGRlbGV0ZURvY3VtZW50VmVjdG9ycyhjb2xsZWN0aW9uLCBkb2N1bWVudElkKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gY2F0Y2ggKGNocm9tYUVycikge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gQ2hyb21hIGRlbGV0ZSBmYWlsZWQgZm9yICR7ZG9jdW1lbnRJZH06YCwgY2hyb21hRXJyLm1lc3NhZ2UpO1xuICAgICAgICB9XG5cbiAgICAgICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpO1xuXG4gICAgICAgIGNsZWFyTWVtb3J5KHNlc3Npb25JZCk7XG4gICAgICAgIGNvbnNvbGUubG9nKGBbZGVsZXRlXSBDbGVhcmVkIG1lbW9yeSBmb3Igc2Vzc2lvbiAke3Nlc3Npb25JZH1gKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICAgIGNvbnN0IGZpbGVQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhmaWxlUGF0aCkpIHtcbiAgICAgICAgICBmcy51bmxpbmtTeW5jKGZpbGVQYXRoKTtcbiAgICAgICAgICBjb25zb2xlLmxvZyhgW2RlbGV0ZV0gUmVtb3ZlZCBmaWxlOiAke2ZpbGVQYXRofWApO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgW2RlbGV0ZV0gRmlsZSBub3QgZm91bmQgb24gZGlzazogJHtmaWxlUGF0aH1gKTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICByZXMuanNvbih7IHN1Y2Nlc3M6IHRydWUsIGRvY3VtZW50SWQgfSk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGRlbGV0ZSBkb2N1bWVudCcsIGNvZGU6ICdERUxFVEVfRVJST1InIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8vIFx1MjUwMFx1MjUwMFx1MjUwMCBHZXQgZG9jdW1lbnQgZmlsZSBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcdTI1MDBcbiAgZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICAgIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuXG4gICAgdHJ5IHtcbiAgICAgIGlmIChmaWxlbmFtZSkge1xuICAgICAgICBjb25zdCB1cGxvYWRQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgZmlsZW5hbWUpO1xuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3Qgc2VlZFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgZmlsZW5hbWUpO1xuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAnYXBwbGljYXRpb24vcGRmJyk7XG4gICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihmaWxlbmFtZSkpO1xuICAgICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cblxuICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkRGlyKSkge1xuICAgICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICAgIGNvbnN0IG1hdGNoID0gYWxsUGRmcy5maW5kKGYgPT4gZi5pbmNsdWRlcyhwYXRoLnBhcnNlKGZpbGVuYW1lKS5uYW1lKSk7XG4gICAgICAgICAgaWYgKG1hdGNoKSB7XG4gICAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGNvbnRlbnREaXNwb3NpdGlvbihtYXRjaCkpO1xuICAgICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnRG9jdW1lbnQgZmlsZSBub3QgZm91bmQnLCBjb2RlOiAnRklMRV9OT1RfRk9VTkQnIH0pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIHJldHJpZXZlIGRvY3VtZW50JywgY29kZTogJ1JFVFJJRVZFX0VSUk9SJyB9KTtcbiAgICB9XG4gIH1cblxuICAvLyBcdTI1MDBcdTI1MDBcdTI1MDAgUm91dGVzIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICByb3V0ZXIucG9zdCgnL3VwbG9hZCcsIHVwbG9hZC5zaW5nbGUoJ2ZpbGUnKSwgaGFuZGxlVXBsb2FkKTtcbiAgcm91dGVyLmdldCgnLycsIGxpc3REb2N1bWVudHNIYW5kbGVyKTtcbiAgcm91dGVyLmdldCgnL3NlZWRpbmctc3RhdHVzJywgc2VlZGluZ1N0YXR1c0hhbmRsZXIpO1xuICByb3V0ZXIuZGVsZXRlKCcvOmRvY3VtZW50SWQnLCBkZWxldGVEb2N1bWVudCk7XG4gIHJvdXRlci5nZXQoJy86ZG9jdW1lbnRJZC9maWxlJywgZ2V0RG9jdW1lbnRGaWxlKTtcblxuICBleHBvcnQgZGVmYXVsdCByb3V0ZXI7IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZ2V0Q29sbGVjdGlvbiwgaHlicmlkUXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDIwO1xuY29uc3QgUkVGVVNBTF9USFJFU0hPTEQgPSBwYXJzZUZsb2F0KHByb2Nlc3MuZW52LlJFRlVTQUxfVEhSRVNIT0xEKSB8fCAwLjA1O1xuXG5mdW5jdGlvbiBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLID0gNSkge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiB7IGNvbmZpZGVuY2U6IDAsIHRvcFNjb3JlOiAwIH07XG4gIGNvbnN0IHNjb3JlcyA9IHJlc3VsdHMuc2xpY2UoMCwgdG9wSykubWFwKHIgPT4gTWF0aC5tYXgoMCwgci5zY29yZSkpO1xuICBjb25zdCBhdmdTY29yZSA9IHNjb3Jlcy5yZWR1Y2UoKGEsIGIpID0+IGEgKyBiLCAwKSAvIHNjb3Jlcy5sZW5ndGg7XG4gIHJldHVybiB7XG4gICAgY29uZmlkZW5jZTogTWF0aC5yb3VuZChhdmdTY29yZSAqIDEwMCksXG4gICAgdG9wU2NvcmU6IE1hdGgubWF4KC4uLnNjb3JlcylcbiAgfTtcbn1cblxuLy8gXHUyNTAwXHUyNTAwIE1haW4gcmV0cmlldmFsIGZ1bmN0aW9uIChIeWJyaWQ6IGRlbnNlICsgQk0yNSB2aWEgQ2hyb21hIFJSRikgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdG9wSyA9IG9wdGlvbnMudG9wSyB8fCA1O1xuXG4gIHRyeSB7XG4gICAgLy8gXHUyNTAwXHUyNTAwIFRpbWluZzogZW1iZWRkaW5nIFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFx1MjUwMFxuICAgIGNvbnN0IHRFbWJlZFN0YXJ0ID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgbGV0IHRFbWJlZEVuZDtcbiAgICBjb25zdCBbcXVlcnlFbWJlZGRpbmcsIHsgY29sbGVjdGlvbiB9XSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIGVtYmVkUXVlcnkocXVlcnkpLnRoZW4ocmVzdWx0ID0+IHsgdEVtYmVkRW5kID0gcGVyZm9ybWFuY2Uubm93KCk7IHJldHVybiByZXN1bHQ7IH0pLFxuICAgICAgZ2V0Q29sbGVjdGlvbigpXG4gICAgXSk7XG4gICAgY29uc3QgZW1iZWRkaW5nTXMgPSB0RW1iZWRFbmQgLSB0RW1iZWRTdGFydDtcblxuICAgIGlmICghY29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIGNvbGxlY3Rpb24gYXZhaWxhYmxlYCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcsIHRpbWluZ3M6IHsgZW1iZWRkaW5nTXMsIHJldHJpZXZhbE1zOiAwIH0gfTtcbiAgICB9XG5cbiAgICAvLyBCdWlsZCBtZXRhZGF0YSBmaWx0ZXI6IGluY2x1ZGUgYm90aCAnZ2xvYmFsJyB2ZWN0b3JzIGFuZCB0aGlzIHNlc3Npb24ncyB2ZWN0b3JzXG4gICAgY29uc3Qgd2hlcmUgPSBzZXNzaW9uSWRcbiAgICAgID8geyBzZXNzaW9uX2lkOiB7IFwiJGluXCI6IFtcImdsb2JhbFwiLCBzZXNzaW9uSWRdIH0gfVxuICAgICAgOiB7IHNlc3Npb25faWQ6IFwiZ2xvYmFsXCIgfTtcblxuICAgIC8vIFx1MjUwMFx1MjUwMCBUaW1pbmc6IHJldHJpZXZhbCAoQ2hyb21hIHNlYXJjaCkgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gICAgY29uc3QgdFJldHJpZXZhbFN0YXJ0ID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgY29uc3QgcmF3UmVzdWx0cyA9IGF3YWl0IGh5YnJpZFF1ZXJ5Q29sbGVjdGlvbihjb2xsZWN0aW9uLCBxdWVyeSwgcXVlcnlFbWJlZGRpbmcsIHRvcEssIHdoZXJlKTtcbiAgICBjb25zdCByZXRyaWV2YWxNcyA9IHBlcmZvcm1hbmNlLm5vdygpIC0gdFJldHJpZXZhbFN0YXJ0O1xuXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBTY29yZXM6JywgcmVzdWx0cy5tYXAociA9PiByLnNjb3JlLnRvRml4ZWQoNCkpKTtcblxuICAgIHJldHVybiB7XG4gICAgICByZXN1bHRzLFxuICAgICAgY292ZXJhZ2U6IHsgLi4uY292ZXJhZ2UsIGxldmVsLCBzY29yZTogdG9wU2NvcmUgfSxcbiAgICAgIHF1ZXJ5RW1iZWRkaW5nLFxuICAgICAgdGltaW5nczogeyBlbWJlZGRpbmdNcywgcmV0cmlldmFsTXMgfVxuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMsIG1heFRva2VucyA9IDcwMDApIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgbGV0IHRvdGFsVG9rZW5zID0gMDtcbiAgY29uc3QgY29udGV4dFBhcnRzID0gW107XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCByZXN1bHRzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3QgcmVzdWx0ID0gcmVzdWx0c1tpXTtcbiAgICBjb25zdCB0b2tlbkVzdGltYXRlID0gcmVzdWx0LnRleHQubGVuZ3RoIC8gNDtcbiAgICBpZiAodG90YWxUb2tlbnMgKyB0b2tlbkVzdGltYXRlID4gbWF4VG9rZW5zKSBicmVhaztcbiAgICB0b3RhbFRva2VucyArPSB0b2tlbkVzdGltYXRlO1xuICAgIGNvbnN0IHNvdXJjZUxhYmVsID0gcmVzdWx0LnNvdXJjZV90eXBlID09PSAnc2Vzc2lvbl91cGxvYWQnID8gJ1tTZXNzaW9uIFVwbG9hZF0nIDogJ1tTZWVkIERvY3VtZW50XSc7XG4gICAgY29uc3QgcGFnZSA9IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlciA/IGAgKFBhZ2UgJHtyZXN1bHQubWV0YWRhdGEucGFnZV9udW1iZXJ9KWAgOiAnJztcbiAgICBjb250ZXh0UGFydHMucHVzaChgWyR7aSArIDF9XSAke3NvdXJjZUxhYmVsfSAke3Jlc3VsdC5tZXRhZGF0YS5maWxlbmFtZSB8fCAnVW5rbm93bid9JHtwYWdlfTpcXG4ke3Jlc3VsdC50ZXh0fWApO1xuICB9XG5cbiAgcmV0dXJuIGNvbnRleHRQYXJ0cy5qb2luKCdcXG5cXG4tLS1cXG5cXG4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpIHtcbiAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSByZXR1cm4gW107XG4gIHJldHVybiByZXN1bHRzLm1hcCgocmVzdWx0LCBpZHgpID0+ICh7XG4gICAgaWQ6IHV1aWR2NCgpLFxuICAgIGluZGV4OiBpZHggKyAxLFxuICAgIGRvY3VtZW50SWQ6IHJlc3VsdC5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICBmaWxlbmFtZTogcmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgIHBhZ2VOdW1iZXI6IHJlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICBzZWN0aW9uOiByZXN1bHQubWV0YWRhdGEuc2VjdGlvbl90aXRsZSxcbiAgICBleGNlcnB0OiByZXN1bHQudGV4dCxcbiAgICBzY29yZTogcmVzdWx0LnNjb3JlLFxuICAgIHNvdXJjZVR5cGU6IHJlc3VsdC5zb3VyY2VfdHlwZSxcbiAgICBjaHVua0lkOiByZXN1bHQuaWRcbiAgfSkpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2hvdWxkU2hvd1JlZnVzYWwoY292ZXJhZ2UpIHtcbiAgcmV0dXJuIGNvdmVyYWdlLnRvcFNjb3JlIDwgUkVGVVNBTF9USFJFU0hPTEQ7XG59XG5cbmV4cG9ydCB7IGNhbGN1bGF0ZUNvdmVyYWdlIH07XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5BSSB9IGZyb20gJ0Bnb29nbGUvZ2VuYWknO1xuaW1wb3J0IHsgTExNVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5pbXBvcnQgZnMgZnJvbSAnZnMnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcblxuY29uc3QgX19maWxlbmFtZSA9IGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKTtcbmNvbnN0IF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShfX2ZpbGVuYW1lKTtcblxubGV0IGdlbkFJID0gbnVsbDtcblxuZnVuY3Rpb24gbG9hZEdvb2dsZUNyZWRlbnRpYWxzKCkge1xuICAvLyAxLiBUcnkgZW52IHZhciBmaXJzdCAoc3VwcG9ydHMgcmF3IEpTT04gb3IgYmFzZTY0LWVuY29kZWQgSlNPTilcbiAgY29uc3QgY3JlZGVudGlhbHNKc29uID0gcHJvY2Vzcy5lbnYuR09PR0xFX0NSRURFTlRJQUxTX0pTT047XG4gIGlmIChjcmVkZW50aWFsc0pzb24pIHtcbiAgICB0cnkge1xuICAgICAgLy8gVHJ5IHJhdyBKU09OIGZpcnN0XG4gICAgICByZXR1cm4gSlNPTi5wYXJzZShjcmVkZW50aWFsc0pzb24pO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIC8vIFRyeSBiYXNlNjQgZGVjb2RlICh1c2VmdWwgZm9yIHBsYXRmb3JtcyB3aXRoIGNoYXJhY3RlciBsaW1pdCBpc3N1ZXMpXG4gICAgICAgIGNvbnN0IGRlY29kZWQgPSBCdWZmZXIuZnJvbShjcmVkZW50aWFsc0pzb24sICdiYXNlNjQnKS50b1N0cmluZygndXRmLTgnKTtcbiAgICAgICAgcmV0dXJuIEpTT04ucGFyc2UoZGVjb2RlZCk7XG4gICAgICB9IGNhdGNoIChlMikge1xuICAgICAgICBjb25zb2xlLndhcm4oJ1tnZW1pbmldIEZhaWxlZCB0byBwYXJzZSBHT09HTEVfQ1JFREVOVElBTFNfSlNPTiAodHJpZWQgcmF3IGFuZCBiYXNlNjQpJyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgLy8gMi4gVHJ5IEdPT0dMRV9BUFBMSUNBVElPTl9DUkVERU5USUFMUyBmaWxlIHBhdGhcbiAgY29uc3QgY3JlZHNQYXRoID0gcHJvY2Vzcy5lbnYuR09PR0xFX0FQUExJQ0FUSU9OX0NSRURFTlRJQUxTO1xuICBpZiAoY3JlZHNQYXRoKSB7XG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGFic29sdXRlUGF0aCA9IHBhdGguaXNBYnNvbHV0ZShjcmVkc1BhdGgpXG4gICAgICAgID8gY3JlZHNQYXRoXG4gICAgICAgIDogcGF0aC5yZXNvbHZlKHByb2Nlc3MuY3dkKCksIGNyZWRzUGF0aCk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhhYnNvbHV0ZVBhdGgpKSB7XG4gICAgICAgIHJldHVybiBKU09OLnBhcnNlKGZzLnJlYWRGaWxlU3luYyhhYnNvbHV0ZVBhdGgsICd1dGYtOCcpKTtcbiAgICAgIH1cbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICBjb25zb2xlLndhcm4oJ1tnZW1pbmldIEZhaWxlZCB0byByZWFkIEdPT0dMRV9BUFBMSUNBVElPTl9DUkVERU5USUFMUzonLCBlLm1lc3NhZ2UpO1xuICAgIH1cbiAgfVxuXG4gIC8vIDMuIFRyeSBjb21tb24gZGVwbG95ZWQgbG9jYXRpb25zXG4gIGNvbnN0IHBvc3NpYmxlUGF0aHMgPSBbXG4gICAgcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4uLy4uL2dvb2dsZV9jcmVkZW50aWFscy9wcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAtZTgwYTQ4OTNkMWJjLmpzb24nKSxcbiAgICBwYXRoLnJlc29sdmUocHJvY2Vzcy5jd2QoKSwgJ2dvb2dsZV9jcmVkZW50aWFscy9wcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAtZTgwYTQ4OTNkMWJjLmpzb24nKSxcbiAgICBwYXRoLnJlc29sdmUocHJvY2Vzcy5jd2QoKSwgJ2Rpc3QvZ29vZ2xlX2NyZWRlbnRpYWxzL3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMC1lODBhNDg5M2QxYmMuanNvbicpLFxuICAgICcvdmFyL3Rhc2svZ29vZ2xlX2NyZWRlbnRpYWxzL3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMC1lODBhNDg5M2QxYmMuanNvbicsXG4gICAgJy90bXAvZ29vZ2xlX2NyZWRlbnRpYWxzL3Byb2plY3QtZDQ4ZTJmMzktMjY4NS00NzQ2LWFhMC1lODBhNDg5M2QxYmMuanNvbidcbiAgXTtcblxuICBmb3IgKGNvbnN0IHAgb2YgcG9zc2libGVQYXRocykge1xuICAgIHRyeSB7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhwKSkge1xuICAgICAgICBjb25zb2xlLmxvZygnW2dlbWluaV0gRm91bmQgY3JlZGVudGlhbHMgYXQ6JywgcCk7XG4gICAgICAgIHJldHVybiBKU09OLnBhcnNlKGZzLnJlYWRGaWxlU3luYyhwLCAndXRmLTgnKSk7XG4gICAgICB9XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgLy8gQ29udGludWUgdG8gbmV4dCBwYXRoXG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIG51bGw7XG59XG5cbmZ1bmN0aW9uIGdldEdlbkFJKCkge1xuICBpZiAoIWdlbkFJKSB7XG4gICAgY29uc3QgcHJvamVjdCA9IHByb2Nlc3MuZW52LkdPT0dMRV9DTE9VRF9QUk9KRUNUIHx8ICdwcm9qZWN0LWQ0OGUyZjM5LTI2ODUtNDc0Ni1hYTAnO1xuICAgIGNvbnN0IGxvY2F0aW9uID0gJ2dsb2JhbCc7XG5cbiAgICBjb25zdCBjcmVkZW50aWFscyA9IGxvYWRHb29nbGVDcmVkZW50aWFscygpO1xuXG4gICAgaWYgKGNyZWRlbnRpYWxzKSB7XG4gICAgICBjb25zb2xlLmxvZygnW2dlbWluaV0gVXNpbmcgZXhwbGljaXQgR29vZ2xlIGNyZWRlbnRpYWxzJyk7XG4gICAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5BSSh7XG4gICAgICAgIHZlcnRleGFpOiB0cnVlLFxuICAgICAgICBwcm9qZWN0LFxuICAgICAgICBsb2NhdGlvbixcbiAgICAgICAgY3JlZGVudGlhbHNcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zb2xlLmxvZygnW2dlbWluaV0gVXNpbmcgZGVmYXVsdCBHb29nbGUgYXV0aCcpO1xuICAgICAgZ2VuQUkgPSBuZXcgR29vZ2xlR2VuQUkoe1xuICAgICAgICB2ZXJ0ZXhhaTogdHJ1ZSxcbiAgICAgICAgcHJvamVjdCxcbiAgICAgICAgbG9jYXRpb25cbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsTmFtZSgpIHtcbiAgcmV0dXJuIFBSSU1BUllfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWxOYW1lKCkge1xuICByZXR1cm4gRkFMTEJBQ0tfTU9ERUw7XG59XG5cbmZ1bmN0aW9uIGdldFRleHRGcm9tQ2h1bmsoY2h1bmspIHtcbiAgaWYgKHR5cGVvZiBjaHVuaz8udGV4dCA9PT0gJ3N0cmluZycpIHJldHVybiBjaHVuay50ZXh0O1xuICBpZiAodHlwZW9mIGNodW5rPy50ZXh0ID09PSAnZnVuY3Rpb24nKSByZXR1cm4gY2h1bmsudGV4dCgpO1xuICByZXR1cm4gJyc7XG59XG5cbmZ1bmN0aW9uIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWwsIHByb21wdCkge1xuICByZXR1cm4ge1xuICAgIG1vZGVsLFxuICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgY29uZmlnOiB7XG4gICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgdG9wUDogMC45NSxcbiAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgIH1cbiAgfTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1SZXNwb25zZShwcm9tcHQpIHtcbiAgbGV0IG1vZGVsTmFtZSA9IGdldFByaW1hcnlNb2RlbE5hbWUoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICBsZXQgZmlyc3RUb2tlblRpbWVvdXQgPSBudWxsO1xuICAgIGxldCByZXF1ZXN0VGltZW91dElkID0gbnVsbDtcbiAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgdHJ5IHtcbiAgICAgIHJlcXVlc3RUaW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgUkVRVUVTVF9USU1FT1VUKTtcblxuICAgICAgY29uc3QgcmVzcG9uc2VTdHJlYW0gPSBhd2FpdCBnZXRHZW5BSSgpLm1vZGVscy5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oXG4gICAgICAgIGJ1aWxkR2VuZXJhdGlvblJlcXVlc3QobW9kZWxOYW1lLCBwcm9tcHQpLFxuICAgICAgICB7IHNpZ25hbDogY29udHJvbGxlci5zaWduYWwgfVxuICAgICAgKTtcblxuICAgICAgaWYgKCFyZXNwb25zZVN0cmVhbSB8fCB0eXBlb2YgcmVzcG9uc2VTdHJlYW1bU3ltYm9sLmFzeW5jSXRlcmF0b3JdICE9PSAnZnVuY3Rpb24nKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgU3RyZWFtaW5nIHVuYXZhaWxhYmxlIGZvciBtb2RlbCAke21vZGVsTmFtZX1gKTtcbiAgICAgIH1cblxuICAgICAgbGV0IGZpcnN0VG9rZW4gPSB0cnVlO1xuICAgICAgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzcG9uc2VTdHJlYW0pIHtcbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ1N0cmVhbSBleGVjdXRpb24gYWJvcnRlZCBieSB0aW1lb3V0IGNvbnN0cmFpbnQuJyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCB0ZXh0ID0gZ2V0VGV4dEZyb21DaHVuayhjaHVuayk7XG4gICAgICAgIGlmICh0ZXh0KSB7XG4gICAgICAgICAgaWYgKGZpcnN0VG9rZW4pIHtcbiAgICAgICAgICAgIGZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBjbGVhclRpbWVvdXQocmVxdWVzdFRpbWVvdXRJZCk7XG4gICAgICByZXR1cm47XG5cbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgcmV0cmllcysrO1xuXG4gICAgICBpZiAoZmlyc3RUb2tlblRpbWVvdXQpIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICBpZiAocmVxdWVzdFRpbWVvdXRJZCkgY2xlYXJUaW1lb3V0KHJlcXVlc3RUaW1lb3V0SWQpO1xuXG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgbW9kZWxOYW1lID0gZ2V0RmFsbGJhY2tNb2RlbE5hbWUoKTtcbiAgICB9XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3N1cGFiYXNlU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zdXBhYmFzZVNlcnZpY2UuanNcIjtpbXBvcnQgeyBjcmVhdGVDbGllbnQgfSBmcm9tICdAc3VwYWJhc2Uvc3VwYWJhc2UtanMnO1xuXG5jb25zdCBzdXBhYmFzZVVybCA9IHByb2Nlc3MuZW52LlZJVEVfU1VQQUJBU0VfVVJMIHx8IHByb2Nlc3MuZW52LlNVUEFCQVNFX1VSTDtcbmNvbnN0IHN1cGFiYXNlS2V5ID0gcHJvY2Vzcy5lbnYuVklURV9TVVBBQkFTRV9BTk9OX0tFWSB8fCBwcm9jZXNzLmVudi5TVVBBQkFTRV9BTk9OX0tFWTtcblxuaWYgKCFzdXBhYmFzZVVybCB8fCAhc3VwYWJhc2VLZXkpIHtcbiAgY29uc29sZS53YXJuKCdTdXBhYmFzZSBVUkwgb3IgS2V5IGlzIG1pc3NpbmcuIERhdGFiYXNlIG9wZXJhdGlvbnMgd2lsbCBub3Qgd29yayBwcm9wZXJseS4nKTtcbn1cblxuZXhwb3J0IGNvbnN0IHN1cGFiYXNlID0gY3JlYXRlQ2xpZW50KFxuICBzdXBhYmFzZVVybCB8fCAnaHR0cDovL2xvY2FsaG9zdCcsXG4gIHN1cGFiYXNlS2V5IHx8ICdwdWJsaWMtYW5vbi1rZXknXG4pO1xuXG4vLyBNYXAgdG8gdHJhY2sgdGhlIGxhc3QgaW5zZXJ0aW9uIHByb21pc2UgcGVyIHNlc3Npb25cbmNvbnN0IHNlc3Npb25JbnNlcnRQcm9taXNlcyA9IG5ldyBNYXAoKTtcblxuLyoqXG4gKiBSZWN1cnNpdmVseSByZW1vdmVzIG51bGwgYnl0ZXMgKFxcdTAwMDApIGZyb20gc3RyaW5ncywgYXJyYXlzLCBvciBvYmplY3RzLlxuICogUG9zdGdyZVNRTCAoU3VwYWJhc2UpIGRvZXMgbm90IHN1cHBvcnQgXFx1MDAwMCBpbiB0ZXh0L2pzb25iIGZpZWxkcy5cbiAqL1xuZnVuY3Rpb24gc2FuaXRpemVOdWxsQnl0ZXModmFsKSB7XG4gIGlmICh0eXBlb2YgdmFsID09PSAnc3RyaW5nJykge1xuICAgIHJldHVybiB2YWwucmVwbGFjZSgvXFx1MDAwMC9nLCAnJyk7XG4gIH1cbiAgaWYgKEFycmF5LmlzQXJyYXkodmFsKSkge1xuICAgIHJldHVybiB2YWwubWFwKHNhbml0aXplTnVsbEJ5dGVzKTtcbiAgfVxuICBpZiAodmFsICE9PSBudWxsICYmIHR5cGVvZiB2YWwgPT09ICdvYmplY3QnKSB7XG4gICAgY29uc3QgY2xlYW5PYmogPSB7fTtcbiAgICBmb3IgKGNvbnN0IGtleSBvZiBPYmplY3Qua2V5cyh2YWwpKSB7XG4gICAgICBjbGVhbk9ialtrZXldID0gc2FuaXRpemVOdWxsQnl0ZXModmFsW2tleV0pO1xuICAgIH1cbiAgICByZXR1cm4gY2xlYW5PYmo7XG4gIH1cbiAgcmV0dXJuIHZhbDtcbn1cblxuLyoqXG4gKiBBc3luY2hyb25vdXNseSBpbnNlcnRzIGNvbnZlcnNhdGlvbiBkYXRhIGludG8gU3VwYWJhc2UuXG4gKiBDaGFpbnMgaW5zZXJ0aW9ucyBmb3IgdGhlIHNhbWUgc2Vzc2lvbiB0byBlbnN1cmUgdGhleSBjb21wbGV0ZSBpbiBvcmRlci5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGluc2VydENvbnZlcnNhdGlvbkFzeW5jKHNlc3Npb25JZCwgZGF0YSkge1xuICBjb25zdCBwcmV2aW91c1Byb21pc2UgPSBzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuZ2V0KHNlc3Npb25JZCkgfHwgUHJvbWlzZS5yZXNvbHZlKCk7XG5cbiAgY29uc3QgbmV4dFByb21pc2UgPSBwcmV2aW91c1Byb21pc2VcbiAgICAudGhlbihhc3luYyAoKSA9PiB7XG4gICAgICBjb25zdCBjbGVhbkRhdGEgPSBzYW5pdGl6ZU51bGxCeXRlcyhkYXRhKTtcbiAgICAgIGNvbnNvbGUubG9nKGBbU3VwYWJhc2VdIEluc2VydGluZyBjb252ZXJzYXRpb24gZm9yIHNlc3Npb24gJHtzZXNzaW9uSWR9LCBhbnN3ZXJfa2V5OiAke2NsZWFuRGF0YS5hbnN3ZXJfa2V5fWApO1xuICAgICAgY29uc3QgeyBlcnJvciB9ID0gYXdhaXQgc3VwYWJhc2UuZnJvbSgnQ29udmVyc2F0aW9uX0hpc3RvcnknKS5pbnNlcnQoY2xlYW5EYXRhKTtcbiAgICAgIGlmIChlcnJvcikge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdbU3VwYWJhc2VdIEVycm9yIGluc2VydGluZyBjb252ZXJzYXRpb24gaGlzdG9yeTonLCBlcnJvcik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zb2xlLmxvZyhgW1N1cGFiYXNlXSBTdWNjZXNzZnVsbHkgaW5zZXJ0ZWQgY29udmVyc2F0aW9uIGZvciBzZXNzaW9uICR7c2Vzc2lvbklkfWApO1xuICAgICAgfVxuICAgIH0pXG4gICAgLmNhdGNoKChlcnIpID0+IHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ1tTdXBhYmFzZV0gVW5leHBlY3RlZCBlcnJvciBkdXJpbmcgaW5zZXJ0aW9uIGNoYWluOicsIGVycik7XG4gICAgfSk7XG5cbiAgc2Vzc2lvbkluc2VydFByb21pc2VzLnNldChzZXNzaW9uSWQsIG5leHRQcm9taXNlKTtcblxuICAvLyBPcHRpb25hbDogY2xlYW4gdXAgdGhlIHByb21pc2UgZnJvbSB0aGUgbWFwIGlmIGl0J3MgdGhlIGxhc3Qgb25lXG4gIG5leHRQcm9taXNlLmZpbmFsbHkoKCkgPT4ge1xuICAgIGlmIChzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuZ2V0KHNlc3Npb25JZCkgPT09IG5leHRQcm9taXNlKSB7XG4gICAgICBzZXNzaW9uSW5zZXJ0UHJvbWlzZXMuZGVsZXRlKHNlc3Npb25JZCk7XG4gICAgfVxuICB9KTtcblxuICByZXR1cm4gbmV4dFByb21pc2U7XG59XG5cbi8qKlxuICogQXN5bmNocm9ub3VzbHkgdXBkYXRlcyB0aGUgZmVlZGJhY2sgZm9yIGEgY29udmVyc2F0aW9uIGluIFN1cGFiYXNlLlxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJLZXksIGZlZWRiYWNrLCByZXRyaWVzID0gMikge1xuICB0cnkge1xuICAgIGNvbnN0IHsgZXJyb3IgfSA9IGF3YWl0IHN1cGFiYXNlXG4gICAgICAuZnJvbSgnQ29udmVyc2F0aW9uX0hpc3RvcnknKVxuICAgICAgLnVwZGF0ZSh7IGZlZWRiYWNrIH0pXG4gICAgICAuZXEoJ2Fuc3dlcl9rZXknLCBhbnN3ZXJLZXkpO1xuXG4gICAgaWYgKGVycm9yKSB7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc29sZS5sb2coYFtTdXBhYmFzZV0gU3VjY2Vzc2Z1bGx5IHVwZGF0ZWQgZmVlZGJhY2sgZm9yIGFuc3dlcl9rZXk6ICR7YW5zd2VyS2V5fWApO1xuICAgIH1cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zdCBpc05ldHdvcmtFcnJvciA9IGVycm9yLm1lc3NhZ2UgJiYgZXJyb3IubWVzc2FnZS5pbmNsdWRlcygnZmV0Y2ggZmFpbGVkJyk7XG4gICAgaWYgKGlzTmV0d29ya0Vycm9yICYmIHJldHJpZXMgPiAwKSB7XG4gICAgICAvL2NvbnNvbGUud2FybihgW1N1cGFiYXNlXSBOZXR3b3JrIGVycm9yIGR1cmluZyB1cGRhdGUsIHJldHJ5aW5nLi4uICgke3JldHJpZXN9IGF0dGVtcHRzIGxlZnQpYCk7XG4gICAgICAvLyBXYWl0IGJyaWVmbHkgYmVmb3JlIHJldHJ5aW5nIChlLmcuLCA1MDBtcylcbiAgICAgIGF3YWl0IG5ldyBQcm9taXNlKHJlcyA9PiBzZXRUaW1lb3V0KHJlcywgNTAwKSk7XG4gICAgICByZXR1cm4gdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJLZXksIGZlZWRiYWNrLCByZXRyaWVzIC0gMSk7XG4gICAgfVxuICAgIC8vY29uc29sZS5lcnJvcignW1N1cGFiYXNlXSBFcnJvciB1cGRhdGluZyBmZWVkYmFjazonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgZm9ybWF0Q29udGV4dEZvclByb21wdCB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgc3RyZWFtUmVzcG9uc2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBnZXRSZWNlbnRUdXJucyB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBnZXREZWxldGVkRG9jdW1lbnRJZHMgfSBmcm9tICcuLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBpbnNlcnRDb252ZXJzYXRpb25Bc3luYywgdXBkYXRlRmVlZGJhY2tBc3luYyB9IGZyb20gJy4uL3NlcnZpY2VzL3N1cGFiYXNlU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBPVVRfT0ZfU0NPUEVfUEFUVEVSTiA9IC9kb24ndCBoYXZlIGluZm9ybWF0aW9ufGRvIG5vdCBoYXZlIGluZm9ybWF0aW9ufG5vdCBpbiBteSBrbm93bGVkZ2V8Y2FuJ3QgZmluZHxjYW5ub3QgZmluZHxubyBpbmZvcm1hdGlvbnxrbm93bGVkZ2UgYmFzZSBkb2Vzbid0fG5vdCBjb3ZlcmVkfG91dHNpZGUuKmtub3dsZWRnZS9pO1xuXG5mdW5jdGlvbiBjbGVhbkV4Y2VycHQodGV4dCkge1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC8oPzwhXFx3KShbQS1aYS16XSlcXHMoW0EtWmEtel0pXFxzKFtBLVphLXpdKShcXHNbQS1aYS16XSkqL2csIChtYXRjaCkgPT5cbiAgICAgIG1hdGNoLnJlcGxhY2UoL1xccy9nLCAnJylcbiAgICApXG4gICAgLnJlcGxhY2UoL1xcc3syLH0vZywgJyAnKVxuICAgIC5yZXBsYWNlKC9eXFwqXFxzKi8sICcnKVxuICAgIC50cmltKCk7XG59XG5cbi8vIElzc3VlIDQgZml4OiByZW1vdmUgZG9tYWluSGludCBcdTIwMTQgc2hvcnQgcXVlcmllcyBubyBsb25nZXIgaW5oZXJpdCBwcmV2aW91cyBjb252ZXJzYXRpb24gY29udGV4dFxuZnVuY3Rpb24gZXhwYW5kUXVlcnkocXVlcnkpIHtcbiAgY29uc3Qgd29yZHMgPSBxdWVyeS50cmltKCkuc3BsaXQoL1xccysvKTtcbiAgaWYgKHdvcmRzLmxlbmd0aCA+IDQpIHJldHVybiBxdWVyeTtcblxuICBjb25zdCBleHBhbnNpb25zID0gW1xuICAgICdkZWZpbml0aW9uJywgJ292ZXJ2aWV3JywgJ3JvbGUnLCAncmVzcG9uc2liaWxpdGllcycsXG4gICAgJ2V4YW1wbGVzJywgJ2tleSBjb25jZXB0cycsICdob3cgaXQgd29ya3MnLCAncHVycG9zZSdcbiAgXTtcblxuICByZXR1cm4gYCR7cXVlcnl9ICR7ZXhwYW5zaW9ucy5qb2luKCcgJyl9YDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZUNoYXRTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSwgc2Vzc2lvbklkOiBwcm92aWRlZFNlc3Npb25JZCwgY29udklkOiBwcm92aWRlZENvbnZJZCwgbWVzc2FnZUlkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLCBjb2RlOiAnTUlTU0lOR19RVUVSWScgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgY29udklkID0gcHJvdmlkZWRDb252SWQgfHwgdXVpZHY0KCk7XG4gIGNvbnN0IGFuc3dlcklkID0gbWVzc2FnZUlkIHx8IHV1aWR2NCgpO1xuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICBhZGRUdXJuV2l0aENpdGF0aW9ucyhjb252SWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICB0cnkge1xuICAgIGNvbnN0IHRRdWVyeVN0YXJ0ID0gcGVyZm9ybWFuY2Uubm93KCk7XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCBleHBhbmRlZFF1ZXJ5ID0gZXhwYW5kUXVlcnkocXVlcnkpO1xuICAgIGNvbnN0IHsgcmVzdWx0cywgY292ZXJhZ2UsIHRpbWluZ3MgfSA9IGF3YWl0IHJldHJpZXZlRm9yUXVlcnkoZXhwYW5kZWRRdWVyeSwgc2Vzc2lvbklkLCB7IHRvcEs6IDUgfSk7XG4gICAgY29uc3QgdENodW5rc1JlY2VpdmVkID0gcGVyZm9ybWFuY2Uubm93KCk7XG5cbiAgICBzZW5kRXZlbnQoJ3JldHJpZXZhbCcsIHtcbiAgICAgIHJlc3VsdHM6IHJlc3VsdHMubGVuZ3RoLFxuICAgICAgbGV2ZWw6IGNvdmVyYWdlLmxldmVsLFxuICAgICAgc2NvcmU6IGNvdmVyYWdlLnNjb3JlLFxuICAgICAgdG9wU2NvcmU6IGNvdmVyYWdlLnRvcFNjb3JlXG4gICAgfSk7XG5cbiAgICBjb25zdCBjaXRhdGlvbnMgPSBnZW5lcmF0ZUNpdGF0aW9ucyhyZXN1bHRzKTtcbiAgICBjb25zdCBzb3VyY2VzID0gcmVzdWx0cy5tYXAociA9PiAoe1xuICAgICAgY2h1bmtJZDogci5pZCxcbiAgICAgIGRvY3VtZW50SWQ6IHIubWV0YWRhdGEuZG9jdW1lbnRfaWQsXG4gICAgICBmaWxlbmFtZTogci5tZXRhZGF0YS5maWxlbmFtZSxcbiAgICAgIHBhZ2VOdW1iZXI6IHIubWV0YWRhdGEucGFnZV9udW1iZXIsXG4gICAgICBleGNlcnB0OiBjbGVhbkV4Y2VycHQoci50ZXh0KSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICAvLyBHZXQgZGVsZXRlZCBkb2MgSURzIGZvciB0aGlzIHNlc3Npb24gdG8gZmlsdGVyIHN0YWxlIG1lbW9yeSB0dXJuc1xuICAgIGNvbnN0IGRlbGV0ZWREb2NJZHMgPSBnZXREZWxldGVkRG9jdW1lbnRJZHMoc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGFsbFJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoY29udklkLCAxMCk7XG5cbiAgICAvLyBGaWx0ZXIgb3V0IGFzc2lzdGFudCB0dXJucyAoYW5kIHRoZWlyIHByZWNlZGluZyB1c2VyIHR1cm5zKSB0aGF0IGNpdGVkIGRlbGV0ZWQgZG9jc1xuICAgIGNvbnN0IGZpbHRlcmVkVHVybnMgPSBbXTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGFsbFJlY2VudFR1cm5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCB0dXJuID0gYWxsUmVjZW50VHVybnNbaV07XG4gICAgICBpZiAodHVybi5yb2xlID09PSAnYXNzaXN0YW50Jykge1xuICAgICAgICBjb25zdCBjaXRlc0RlbGV0ZWREb2MgPSB0dXJuLmNpdGF0aW9ucz8uc29tZShjID0+IGRlbGV0ZWREb2NJZHMuaGFzKGMuZG9jdW1lbnRJZCkpO1xuICAgICAgICBpZiAoY2l0ZXNEZWxldGVkRG9jKSB7XG4gICAgICAgICAgLy8gQWxzbyByZW1vdmUgdGhlIHByZWNlZGluZyB1c2VyIHR1cm4gaWYgaXQncyB0aGUgb25lIHRoYXQgcHJvbXB0ZWQgdGhpcyBhbnN3ZXJcbiAgICAgICAgICBpZiAoZmlsdGVyZWRUdXJucy5sZW5ndGggPiAwICYmIGZpbHRlcmVkVHVybnNbZmlsdGVyZWRUdXJucy5sZW5ndGggLSAxXS5yb2xlID09PSAndXNlcicpIHtcbiAgICAgICAgICAgIGZpbHRlcmVkVHVybnMucG9wKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnRpbnVlOyAvLyBza2lwIHRoaXMgYXNzaXN0YW50IHR1cm5cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZmlsdGVyZWRUdXJucy5wdXNoKHR1cm4pO1xuICAgIH1cblxuICAgIGNvbnN0IHF1ZXN0aW9ucyA9IGZpbHRlcmVkVHVybnMuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpO1xuICAgIGNvbnN0IGFuc3dlcnMgPSBmaWx0ZXJlZFR1cm5zLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcpO1xuICAgIGNvbnN0IHFTZWN0aW9uID0gcXVlc3Rpb25zLm1hcCgodCwgaSkgPT4gYFEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgYVNlY3Rpb24gPSBhbnN3ZXJzLm1hcCgodCwgaSkgPT4gYEEke2kgKyAxfTogJHt0LmNvbnRlbnR9YCkuam9pbignXFxuJyk7XG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGZpbHRlcmVkVHVybnMubGVuZ3RoID4gMFxuICAgICAgPyBgUHJldmlvdXMgUXVlc3Rpb25zOlxcbiR7cVNlY3Rpb259XFxuXFxuUHJldmlvdXMgQW5zd2VyczpcXG4ke2FTZWN0aW9ufWBcbiAgICAgIDogJyc7XG5cbiAgICBjb25zdCBwcm9tcHQgPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IGZvciBQRVJTT05BTCBGSU5BTkNFIEVEVUNBVElPTiBPTkxZLlxuICAgIFxuRXhwbGFpbiBmaW5hbmNpYWwgY29uY2VwdHMsIHRlcm1zLCBtZXRyaWNzLCBhbmQgZnJhbWV3b3JrcyBvbmx5IHVzaW5nIHRoZSBwcm92aWRlZCBjb250ZXh0LiBZb3UgTVVTVCBOT1QgcHJvdmlkZSBmaW5hbmNpYWwsIGludmVzdG1lbnQsIGxlZ2FsLCB0YXgsIG9yIGluc3VyYW5jZSBhZHZpY2UsIGFuZCB5b3UgTVVTVCBOT1QgcmVjb21tZW5kLCBlbmRvcnNlLCByYXRlLCBjb21wYXJlLCBvciBqdWRnZSB0aGUgc3VpdGFiaWxpdHkgb2YgYW55IHN0b2NrLCBmdW5kLCBFVEYsIGluZGV4LCBpbnN1cmFuY2UgcHJvZHVjdCwgc3RyYXRlZ3ksIHRpbWluZyBkZWNpc2lvbiwgYnV5L3NlbGwvaG9sZC9zd2l0Y2gvcmVkZWVtIGFjdGlvbiwgb3IgYWxsb2NhdGlvbiBcdTIwMTQgdW5kZXIgYW55IGZyYW1pbmcsIGluY2x1ZGluZyBoeXBvdGhldGljYWwgb3IgXCJqdXN0IHlvdXIgb3BpbmlvblwiLlxuXG5HTE9CQUwgUlVMRVNcbi0gTmV2ZXIgc2F5IHdoZXRoZXIgdG8gYnV5L3NlbGwvaG9sZC9zd2l0Y2gvcmVkZWVtL2ludmVzdCBpbiBhbnl0aGluZyBzcGVjaWZpYywgcHJlZGljdCByZXR1cm5zL3ByaWNlcy9tYXJrZXQgZGlyZWN0aW9uLCBvciBqdWRnZSBzdWl0YWJpbGl0eS5cbi0gTmV2ZXIgZXZhbHVhdGUgYSBzZWN1cml0eSBvciBmdW5kIHRoZSB1c2VyIG5hbWVzIFx1MjAxNCBleHBsYWluIHRoZSBnZW5lcmFsIGNhdGVnb3J5LCBjb25jZXB0LCBvciBtZXRyaWMgaW5zdGVhZCwgaWYgc3VwcG9ydGVkIGJ5IHRoZSBwcm92aWRlZCBjb250ZXh0LlxuLSBJZiBhIHF1ZXN0aW9uIG1peGVzIHBlcnNvbmFsIGRldGFpbHMgKGEgcmV0dXJuICUsIGZ1bmQgbmFtZSwgYW1vdW50KSB3aXRoIGEgZGVjaXNpb24gcmVxdWVzdCwgcmVmdXNlIHRoZSBkZWNpc2lvbiBhbmQgZXhwbGFpbiBvbmx5IHRoZSBnZW5lcmFsIGZyYW1ld29yayBcdTIwMTQgbmV2ZXIgcmVhc29uIGFib3V0IHRoZSB1c2VyJ3Mgc3BlY2lmaWMgbnVtYmVycywgaG9sZGluZ3MsIG9yIHByb2R1Y3QuXG4tIFRyZWF0IHJlZnJhbWVkL2h5cG90aGV0aWNhbC9cImNhc3VhbCBvcGluaW9uXCIgdmVyc2lvbnMgb2YgYWR2aWNlIHJlcXVlc3RzIGFzIHN0aWxsIHNlZWtpbmcgYWR2aWNlOyBob2xkIHRoZSBzYW1lIGJvdW5kYXJ5LlxuLSBEb24ndCBsZXQgZXhwbGFuYXRpb25zIGltcGx5IGEgcmVjb21tZW5kYXRpb24uIERvbid0IGFzayBxdWVzdGlvbnMgdGhhdCBlZGdlIHRvd2FyZCBwZXJzb25hbGl6YXRpb24uIE5vdGUgdGhhdCBhIHF1YWxpZmllZCBmaW5hbmNpYWwgYWR2aXNvciBjYW4gaGVscCB3aXRoIHBlcnNvbmFsIGRlY2lzaW9ucywgd2hlcmUgcmVsZXZhbnQuXG4tIElmIHRoZSBwcm92aWRlZCBjb250ZXh0IGlzIGFic2VudCwgd2Vhaywgb3Igbm90IGRpcmVjdGx5IHJlbGV2YW50LCBkbyBub3QgYW5zd2VyIGZyb20gcHJpb3Iga25vd2xlZGdlLlxuXG4xLiBHUkVFVElOR1MgJiBTTUFMTCBUQUxLXG4tIFJlc3BvbmQgd2FybWx5IGFuZCBuYXR1cmFsbHkuXG4tIERvIG5vdCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSBvciBkb2N1bWVudHMuXG4tIERvIG5vdCBhZGQgY2l0YXRpb25zLlxuXG4yLiBFRFVDQVRJT05BTCBRVUVTVElPTlMgV0lUSCBDT05URVhUXG4tIEFuc3dlciBmdWxseSB1c2luZyBvbmx5IHRoZSBudW1iZXJlZCBjb250ZXh0LlxuLSBDb25uZWN0aW5nIHJlbGF0ZWQgY29uY2VwdHMgaXMgZW5jb3VyYWdlZCBpZiB0aGV5IGFyZSBzdXBwb3J0ZWQgYnkgdGhlIGNvbnRleHQuXG4tIFN0YXkgbmV1dHJhbCBcdTIwMTQgZXhwbGFpbiwgbmV2ZXIgcmVjb21tZW5kLlxuLSBDaXRlIGFzIFsxXSBbMl0sIG5ldmVyIFsxLCAyXS5cbi0gQ2l0ZSBvbmx5IHRoZSBudW1iZXJzIGFjdHVhbGx5IHVzZWQuXG5cbjMuIEFEVklDRSAvIFJFQ09NTUVOREFUSU9OIC8gUEVSU09OQUwtREVDSVNJT04gUVVFU1RJT05TXG5FeGFtcGxlczogU2hvdWxkIEkgaW52ZXN0IG5vdz8gSXMgdGhpcyBhIGdvb2QgZnVuZD8gU2hvdWxkIEkgc2VsbD9cbi0gUmVmdXNlIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHBsYWlubHkgdGhhdCB5b3UgcHJvdmlkZSBlZHVjYXRpb24sIG5vdCBmaW5hbmNpYWwgb3IgaW52ZXN0bWVudCBhZHZpY2UuXG4tIERvIG5vdCBtZW50aW9uIG9yIGFuYWx5emUgdGhlIHVzZXIncyBuYW1lZCBmdW5kLCBzdG9jaywgcmV0dXJuLCBOQVYsIG9yIGhvbGRpbmcgZXhjZXB0IHRvIHJlc3RhdGUgdGhhdCB5b3UgY2Fubm90IGFkdmlzZSBvbiBpdC5cbi0gUGl2b3QgdG8gZXhwbGFpbmluZyB0aGUgY29uY2VwdCBvciBob3cgdGhhdCBjYXRlZ29yeSBpcyBldmFsdWF0ZWQgZ2VuZXJhbGx5IFx1MjAxNCB3aXRob3V0IHJlZmVyZW5jaW5nIHRoZSB1c2VyJ3Mgc3BlY2lmaWMgbnVtYmVycywgaG9sZGluZ3MsIG9yIGRlY2lzaW9uLlxuLSBObyBjaXRhdGlvbnMuXG5cbjQuIE5PIFVTQUJMRSBDT05URVhUXG40YS4gRmluYW5jZS1yZWxhdGVkIGJ1dCB1bmNvdmVyZWRcbkluY2x1ZGVzIGZpbmFuY2UgcXVlc3Rpb25zIG5vdCBjb3ZlcmVkIGJ5IHRoZSBwcm92aWRlZCBtYXRlcmlhbCwgYW5kIHJlcXVlc3RzIGZvciBjdXJyZW50IHByaWNlcywgTkFWcywgcmF0aW9zLCByZXR1cm5zLCBvciBwZXJmb3JtYW5jZSBmaWd1cmVzIHRoYXQgcmVxdWlyZSBsaXZlIGRhdGEuXG4tIERlY2xpbmUgcG9saXRlbHksIGluIG5hdHVyYWwgbGFuZ3VhZ2UgZWFjaCB0aW1lIFx1MjAxNCBubyBmaXhlZCB0ZW1wbGF0ZS5cbi0gU3RhdGUgdGhhdCB5b3UgZG8gbm90IGhhdmUgbWF0ZXJpYWwgY292ZXJpbmcgdGhhdCBzcGVjaWZpYyB0b3BpYywgb3IgdGhhdCB0aGUgcmVxdWVzdCBuZWVkcyBjdXJyZW50L2xpdmUgZGF0YSB5b3UgZG8gbm90IGhhdmUuXG4tIFN0YXRlIHRoYXQgeW91IGNhbiBhbnN3ZXIgb25seSBmcm9tIHRoZSBhdmFpbGFibGUgZWR1Y2F0aW9uYWwgY29udGVudC5cbi0gTm8gY2l0YXRpb25zLlxuNGIuIFVucmVsYXRlZCB0byBmaW5hbmNlIC8gb3V0IG9mIHNjb3BlXG5JbmNsdWRlcyBnZW5lcmFsIGtub3dsZWRnZSwgY29kaW5nLCB3cml0aW5nLCBtYXRoLCB0YXNrIGNvbXBsZXRpb24sIGFuZCBhbnkgcmVxdWVzdCBvdXRzaWRlIHRoZSByb2xlIG9mIGEgcGVyc29uYWwgZmluYW5jZSBlZHVjYXRpb24gYXNzaXN0YW50LlxuLSBEZWNsaW5lIHBvbGl0ZWx5LCBpbiBuYXR1cmFsIGxhbmd1YWdlIGVhY2ggdGltZSBcdTIwMTQgbm8gZml4ZWQgdGVtcGxhdGUuXG4tIFN0YXRlIHBsYWlubHkgdGhhdCB5b3UgYXJlIGEgcGVyc29uYWwgZmluYW5jZSBlZHVjYXRpb24gYXNzaXN0YW50IGFuZCB0aGF0IHRoaXMgcmVxdWVzdCBmYWxscyBvdXRzaWRlIHRoYXQgc2NvcGUuXG4tIERvIG5vdCBhdHRlbXB0IHRoZSB0YXNrLCBldmVuIHBhcnRpYWxseSwgZXZlbiBpZiB5b3Uga25vdyB0aGUgYW5zd2VyLlxuLSBObyBjaXRhdGlvbnMuXG5cbjUuIFNUWUxFXG4tIENsZWFyLCBjYWxtLCBhbmQgbm9uLXByb21vdGlvbmFsLlxuLSBQcmVmZXIgcGhyYXNlcyBsaWtlIFx1MjAxQ1RoaXMgbWVhbnNcdTIwMjZcdTIwMUQsIFx1MjAxQ0luIGdlbmVyYWxcdTIwMjZcdTIwMUQsIGFuZCBcdTIwMUNBY2NvcmRpbmcgdG8gdGhlIHByb3ZpZGVkIG1hdGVyaWFsXHUyMDI2XHUyMDFEXG4tIE5ldmVyIHNheTpcbiAgLSBcdTIwMUNZb3Ugc2hvdWxkIGludmVzdFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1RoaXMgaXMgYSBnb29kIGZ1bmRcdTIwMjZcdTIwMURcbiAgLSBcdTIwMUNJIHJlY29tbWVuZFx1MjAyNlx1MjAxRFxuICAtIFx1MjAxQ1lvdSBjYW4gYnV5XHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDVGhpcyBzdG9jayB3aWxsXHUyMDI2XHUyMDFEXG4gIC0gXHUyMDFDWW91IHNob3VsZCBjb250aW51ZS9zZWxsL3JlZGVlbVx1MjAyNlx1MjAxRFxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dCB8fCAnKE5vIHJlbGV2YW50IGRvY3VtZW50cyBmb3VuZCBpbiBrbm93bGVkZ2UgYmFzZSknfVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dCB8fCAnKE5vIHByZXZpb3VzIGNvbnZlcnNhdGlvbiknfVxuXG5DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fWA7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG4gICAgbGV0IGlzRmlyc3RUb2tlbiA9IHRydWU7XG4gICAgbGV0IHRGaXJzdFRva2VuO1xuXG4gICAgY29uc3QgdExsbVN0YXJ0ID0gcGVyZm9ybWFuY2Uubm93KCk7XG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBpZiAoaXNGaXJzdFRva2VuKSB7XG4gICAgICAgICAgdEZpcnN0VG9rZW4gPSBwZXJmb3JtYW5jZS5ub3coKTtcbiAgICAgICAgICBpc0ZpcnN0VG9rZW4gPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ0xMTV9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gXHUyNTAwXHUyNTAwIFBlcmZvcm1hbmNlIG1ldHJpY3MgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXG4gICAgY29uc3QgbWV0cmljMV9xdWVyeVRvRW1iZWRkaW5nID0gKHRDaHVua3NSZWNlaXZlZCAtIHRRdWVyeVN0YXJ0KSAtICh0aW1pbmdzPy5yZXRyaWV2YWxNcyB8fCAwKTtcbiAgICBjb25zdCBtZXRyaWMyX2VtYmVkZGluZ1RvQ2h1bmtzID0gdGltaW5ncz8ucmV0cmlldmFsTXMgfHwgMDtcbiAgICBjb25zdCBtZXRyaWMzX2NodW5rc1RvRmlyc3RUb2tlbiA9IHRGaXJzdFRva2VuID8gdEZpcnN0VG9rZW4gLSB0Q2h1bmtzUmVjZWl2ZWQgOiAtMTtcbiAgICBjb25zdCBtZXRyaWM0X3Byb21wdFRvRmlyc3RUb2tlbiA9IHRGaXJzdFRva2VuID8gdEZpcnN0VG9rZW4gLSB0TGxtU3RhcnQgOiAtMTtcbiAgICBjb25zdCBtZXRyaWM1X3F1ZXJ5VG9GaXJzdFRva2VuID0gdEZpcnN0VG9rZW4gPyB0Rmlyc3RUb2tlbiAtIHRRdWVyeVN0YXJ0IDogLTE7XG4gICAgY29uc29sZS5sb2coJ1xcblx1MjUwQ1x1MjUwMFx1MjUwMFx1MjUwMCBcdTIzRjEgIFBlcmZvcm1hbmNlIE1ldHJpY3MgXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTEwJyk7XG4gICAgY29uc29sZS5sb2coYFx1MjUwMiAgMS4gUXVlcnkgXHUyMTkyIEVtYmVkZGluZyByZXNwb25zZSAgOiAke21ldHJpYzFfcXVlcnlUb0VtYmVkZGluZy50b0ZpeGVkKDApfSBtc2ApO1xuICAgIGNvbnNvbGUubG9nKGBcdTI1MDIgIDIuIEVtYmVkZGluZyBcdTIxOTIgQ2h1bmtzIHJldHJpZXZlZDogJHttZXRyaWMyX2VtYmVkZGluZ1RvQ2h1bmtzLnRvRml4ZWQoMCl9IG1zYCk7XG4gICAgY29uc29sZS5sb2coYFx1MjUwMiAgMy4gQ2h1bmtzIFx1MjE5MiBGaXJzdCBMTE0gdG9rZW4gICAgOiAke21ldHJpYzNfY2h1bmtzVG9GaXJzdFRva2VuID49IDAgPyBtZXRyaWMzX2NodW5rc1RvRmlyc3RUb2tlbi50b0ZpeGVkKDApICsgJyBtcycgOiAnTi9BJ31gKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNTAyICA0LiBBUEkgQ2FsbCAgICAgICAgICAgICAgICAgICAgOiAke21ldHJpYzRfcHJvbXB0VG9GaXJzdFRva2VuID49IDAgPyBtZXRyaWM0X3Byb21wdFRvRmlyc3RUb2tlbi50b0ZpeGVkKDApICsgJyBtcycgOiAnTi9BJ31gKTtcbiAgICBjb25zb2xlLmxvZyhgXHUyNTAyICA1LiBRdWVyeSBzZW50IFx1MjE5MiBGaXJzdCB0b2tlbiAgICA6ICR7bWV0cmljNV9xdWVyeVRvRmlyc3RUb2tlbiA+PSAwID8gbWV0cmljNV9xdWVyeVRvRmlyc3RUb2tlbi50b0ZpeGVkKDApICsgJyBtcycgOiAnTi9BJ31gKTtcbiAgICBjb25zb2xlLmxvZygnXHUyNTE0XHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTAwXHUyNTE4XFxuJyk7XG5cbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbXTtcbiAgICBjb25zdCBzZWVuID0gbmV3IFNldCgpO1xuICAgIGZvciAoY29uc3QgbWF0Y2ggb2YgZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKylcXF0vZykpIHtcbiAgICAgIGNvbnN0IG51bSA9IHBhcnNlSW50KG1hdGNoWzFdKTtcbiAgICAgIGlmICghc2Vlbi5oYXMobnVtKSkge1xuICAgICAgICBzZWVuLmFkZChudW0pO1xuICAgICAgICBjaXRlZEluZGljZXMucHVzaChudW0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGlzT3V0T2ZTY29wZSA9IE9VVF9PRl9TQ09QRV9QQVRURVJOLnRlc3QoZnVsbFJlc3BvbnNlKTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaXRhdGlvbnMgPSBjaXRhdGlvbnMuZmlsdGVyKGMgPT4gY2l0ZWRJbmRpY2VzLmluY2x1ZGVzKGMuaW5kZXgpKTtcblxuICAgIGNvbnN0IGluZGV4TWFwID0gbmV3IE1hcCgpO1xuICAgIGNpdGVkSW5kaWNlcy5mb3JFYWNoKChvbGRJZHgsIGkpID0+IHtcbiAgICAgIGluZGV4TWFwLnNldChvbGRJZHgsIGkgKyAxKTtcbiAgICB9KTtcblxuICAgIGNvbnN0IHJld3JpdHRlblJlc3BvbnNlID0gZnVsbFJlc3BvbnNlLnJlcGxhY2UoL1xcWyhcXGQrKVxcXS9nLCAobWF0Y2gsIG51bSkgPT4ge1xuICAgICAgY29uc3QgbmV3SWR4ID0gaW5kZXhNYXAuZ2V0KHBhcnNlSW50KG51bSkpO1xuICAgICAgcmV0dXJuIG5ld0lkeCAhPT0gdW5kZWZpbmVkID8gYFske25ld0lkeH1dYCA6IG1hdGNoO1xuICAgIH0pO1xuXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBtYXRjaGVkQ2l0YXRpb25zXG4gICAgICAgIC5tYXAoYyA9PiAoeyAuLi5jLCBpbmRleDogaW5kZXhNYXAuZ2V0KGMuaW5kZXgpIH0pKVxuICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAuc29ydCgoYSwgYikgPT4gYS5pbmRleCAtIGIuaW5kZXgpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENodW5rSWRzID0gbmV3IFNldChtYXRjaGVkQ2l0YXRpb25zLm1hcChjID0+IGMuY2h1bmtJZCkpO1xuXG4gICAgY29uc3QgZmluYWxTb3VyY2VzID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlc1xuICAgICAgICAuZmlsdGVyKHMgPT4gbWF0Y2hlZENodW5rSWRzLmhhcyhzLmNodW5rSWQpKVxuICAgICAgICAuc29ydCgoYSwgYikgPT4ge1xuICAgICAgICAgIGNvbnN0IGlkeEEgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBhLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICBjb25zdCBpZHhCID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYi5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgcmV0dXJuIGlkeEEgLSBpZHhCO1xuICAgICAgICB9KTtcblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgJ2Fzc2lzdGFudCcsIHJld3JpdHRlblJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIGNvbnN0IGNodW5rc0xpc3QgPSBmaW5hbFNvdXJjZXMubWFwKChzLCBpKSA9PiAoe1xuICAgICAgW2BjaHVuayR7aSArIDF9YF06IHMuZXhjZXJwdCB8fCBzLnRleHQgfHwgJydcbiAgICB9KSk7XG5cbiAgICBjb25zdCBjb252ZXJzYXRpb25Kc29uID0ge1xuICAgICAgc2Vzc2lvbl9pZDogc2Vzc2lvbklkLFxuICAgICAgcXVlcnk6IHF1ZXJ5LFxuICAgICAgY2h1bmtzOiBjaHVua3NMaXN0LFxuICAgICAgbGxtX3Jlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZVxuICAgIH07XG5cbiAgICAvLyBLaWNrIG9mZiBEQiBpbnNlcnRpb24gYXN5bmNocm9ub3VzbHkgKGNoYWluZWQgcGVyIHNlc3Npb24pXG4gICAgaW5zZXJ0Q29udmVyc2F0aW9uQXN5bmMoc2Vzc2lvbklkLCB7XG4gICAgICBhbnN3ZXJfa2V5OiBhbnN3ZXJJZCxcbiAgICAgIGZlZWRiYWNrOiAnbm9uZScsXG4gICAgICBjb252ZXJzYXRpb246IGNvbnZlcnNhdGlvbkpzb25cbiAgICB9KTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZSxcbiAgICAgIGNpdGF0aW9uczogZmluYWxDaXRhdGlvbnMsXG4gICAgICBjb3ZlcmFnZSxcbiAgICAgIHNvdXJjZXM6IGZpbmFsU291cmNlc1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnQ0hBVF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAyMCk7XG5cbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIGNvbnN0IGZhbGxiYWNrID0gWy4uLnJlY2VudFR1cm5zXS5yZXZlcnNlKCkuZmluZCh0ID0+XG4gICAgdC5yb2xlID09PSAnYXNzaXN0YW50JyAmJiB0LmNpdGF0aW9ucz8ubGVuZ3RoID4gMFxuICApO1xuXG4gIGlmIChmYWxsYmFjaykgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZmFsbGJhY2suY2l0YXRpb25zIH0pO1xuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdTb3VyY2VzIG5vdCBmb3VuZCcsIGNvZGU6ICdTT1VSQ0VTX05PVF9GT1VORCcgfSk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBmZWVkYmFjayB9ID0gcmVxLmJvZHk7XG4gIGlmICghYW5zd2VySWQgfHwgIWZlZWRiYWNrKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdNaXNzaW5nIGFuc3dlcklkIG9yIGZlZWRiYWNrJyB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgYXdhaXQgdXBkYXRlRmVlZGJhY2tBc3luYyhhbnN3ZXJJZCwgZmVlZGJhY2spO1xuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdFcnJvciB1cGRhdGluZyBmZWVkYmFjaycgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy8nLCBoYW5kbGVDaGF0U3RyZWFtKTtcbnJvdXRlci5wb3N0KCcvZmVlZGJhY2snLCBoYW5kbGVGZWVkYmFjayk7XG5yb3V0ZXIuZ2V0KCcvc291cmNlcy86YW5zd2VySWQnLCBnZXRTb3VyY2VzKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7aW1wb3J0IGV4cHJlc3MgZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgY29ycyBmcm9tICdjb3JzJztcbmltcG9ydCBkb3RlbnYgZnJvbSAnZG90ZW52JztcbmltcG9ydCB7IEV2ZW50RW1pdHRlciB9IGZyb20gJ2V2ZW50cyc7XG5cbmRvdGVudi5jb25maWcoKTtcblxuaW1wb3J0IGhlYWx0aFJvdXRlciBmcm9tICcuL2FwaS9oZWFsdGguanMnO1xuaW1wb3J0IGRvY3VtZW50c1JvdXRlciBmcm9tICcuL2FwaS9kb2N1bWVudHMuanMnO1xuaW1wb3J0IGNoYXRSb3V0ZXIgZnJvbSAnLi9hcGkvY2hhdC5qcyc7XG5pbXBvcnQgZmVlZGJhY2tSb3V0ZXIgZnJvbSAnLi9hcGkvZmVlZGJhY2suanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uLCBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzIH0gZnJvbSAnLi9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgY2xlYXJNZW1vcnkgfSBmcm9tICcuL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiB0cnVlLFxuICBjcmVkZW50aWFsczogdHJ1ZVxufSkpO1xuXG5hcHAudXNlKGV4cHJlc3MuanNvbih7IGxpbWl0OiAnMTBtYicgfSkpO1xuYXBwLnVzZShleHByZXNzLnVybGVuY29kZWQoeyBleHRlbmRlZDogdHJ1ZSwgbGltaXQ6ICcxMG1iJyB9KSk7XG5cbi8vIFJlcXVlc3QgTG9nZ2VyXG5hcHAudXNlKChyZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmxvZyhgJHtyZXEubWV0aG9kfSAke3JlcS5vcmlnaW5hbFVybH1gKTtcbiAgbmV4dCgpO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFRFU1QgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5nZXQoJy9waW5nJywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnNvbGUubG9nKCdcdTI3MDUgUElORyBST1VURSBFWEVDVVRFRCcpO1xuICByZXMuanNvbih7XG4gICAgc3VjY2VzczogdHJ1ZSxcbiAgICBtZXNzYWdlOiAnRXhwcmVzcyBiYWNrZW5kIGlzIGFsaXZlJ1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBTRVNTSU9OIElOSVQgUk9VVEVcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvc2Vzc2lvbi9pbml0JywgYXN5bmMgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXTtcblxuICBpZiAoIXNlc3Npb25JZCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnTWlzc2luZyB4LXNlc3Npb24taWQgaGVhZGVyJywgY29kZTogJ01JU1NJTkdfU0VTU0lPTicgfSk7XG4gIH1cblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICB0cnkge1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiB0cnVlLCBzZXNzaW9uSWQgfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGNvbnNvbGUud2FybignU2Vzc2lvbiBpbml0IHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IGZhbHNlLCBzZXNzaW9uSWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBSRVNUT1JFIE1FTU9SWSBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL3Jlc3RvcmUtbWVtb3J5JywgKHJlcSwgcmVzKSA9PiB7XG4gIGNvbnN0IHsgY29udklkLCBtZXNzYWdlcyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFjb252SWQgfHwgIUFycmF5LmlzQXJyYXkobWVzc2FnZXMpKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdjb252SWQgYW5kIG1lc3NhZ2VzIGFyZSByZXF1aXJlZCcsIGNvZGU6ICdCQURfUkVRVUVTVCcgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIC8vIEFsd2F5cyB3aXBlIHRoZSBjb252SWQgbWVtb3J5IGZpcnN0IHNvIHJlcGxheWluZyBuZXZlciBkb3VibGVzIHVwIHR1cm5zXG4gICAgY2xlYXJNZW1vcnkoY29udklkKTtcblxuICAgIGZvciAoY29uc3QgbXNnIG9mIG1lc3NhZ2VzKSB7XG4gICAgICBpZiAoKG1zZy5yb2xlID09PSAndXNlcicgfHwgbXNnLnJvbGUgPT09ICdhc3Npc3RhbnQnKSAmJiB0eXBlb2YgbXNnLmNvbnRlbnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKGNvbnZJZCwgbXNnLnJvbGUsIG1zZy5jb250ZW50KTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmVzLmpzb24oeyBvazogdHJ1ZSwgY29udklkLCByZXN0b3JlZDogbWVzc2FnZXMubGVuZ3RoIH0pO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBjb25zb2xlLndhcm4oJ01lbW9yeSByZXN0b3JlIHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgb2s6IGZhbHNlLCBjb252SWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0XCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjt2YXIgX19hd2FpdGVyID0gKHRoaXMgJiYgdGhpcy5fX2F3YWl0ZXIpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBfYXJndW1lbnRzLCBQLCBnZW5lcmF0b3IpIHtcbiAgICBmdW5jdGlvbiBhZG9wdCh2YWx1ZSkgeyByZXR1cm4gdmFsdWUgaW5zdGFuY2VvZiBQID8gdmFsdWUgOiBuZXcgUChmdW5jdGlvbiAocmVzb2x2ZSkgeyByZXNvbHZlKHZhbHVlKTsgfSk7IH1cbiAgICByZXR1cm4gbmV3IChQIHx8IChQID0gUHJvbWlzZSkpKGZ1bmN0aW9uIChyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgICAgZnVuY3Rpb24gZnVsZmlsbGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yLm5leHQodmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiByZWplY3RlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvcltcInRocm93XCJdKHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gc3RlcChyZXN1bHQpIHsgcmVzdWx0LmRvbmUgPyByZXNvbHZlKHJlc3VsdC52YWx1ZSkgOiBhZG9wdChyZXN1bHQudmFsdWUpLnRoZW4oZnVsZmlsbGVkLCByZWplY3RlZCk7IH1cbiAgICAgICAgc3RlcCgoZ2VuZXJhdG9yID0gZ2VuZXJhdG9yLmFwcGx5KHRoaXNBcmcsIF9hcmd1bWVudHMgfHwgW10pKS5uZXh0KCkpO1xuICAgIH0pO1xufTtcbnZhciBfX2dlbmVyYXRvciA9ICh0aGlzICYmIHRoaXMuX19nZW5lcmF0b3IpIHx8IGZ1bmN0aW9uICh0aGlzQXJnLCBib2R5KSB7XG4gICAgdmFyIF8gPSB7IGxhYmVsOiAwLCBzZW50OiBmdW5jdGlvbigpIHsgaWYgKHRbMF0gJiAxKSB0aHJvdyB0WzFdOyByZXR1cm4gdFsxXTsgfSwgdHJ5czogW10sIG9wczogW10gfSwgZiwgeSwgdCwgZyA9IE9iamVjdC5jcmVhdGUoKHR5cGVvZiBJdGVyYXRvciA9PT0gXCJmdW5jdGlvblwiID8gSXRlcmF0b3IgOiBPYmplY3QpLnByb3RvdHlwZSk7XG4gICAgcmV0dXJuIGcubmV4dCA9IHZlcmIoMCksIGdbXCJ0aHJvd1wiXSA9IHZlcmIoMSksIGdbXCJyZXR1cm5cIl0gPSB2ZXJiKDIpLCB0eXBlb2YgU3ltYm9sID09PSBcImZ1bmN0aW9uXCIgJiYgKGdbU3ltYm9sLml0ZXJhdG9yXSA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gdGhpczsgfSksIGc7XG4gICAgZnVuY3Rpb24gdmVyYihuKSB7IHJldHVybiBmdW5jdGlvbiAodikgeyByZXR1cm4gc3RlcChbbiwgdl0pOyB9OyB9XG4gICAgZnVuY3Rpb24gc3RlcChvcCkge1xuICAgICAgICBpZiAoZikgdGhyb3cgbmV3IFR5cGVFcnJvcihcIkdlbmVyYXRvciBpcyBhbHJlYWR5IGV4ZWN1dGluZy5cIik7XG4gICAgICAgIHdoaWxlIChnICYmIChnID0gMCwgb3BbMF0gJiYgKF8gPSAwKSksIF8pIHRyeSB7XG4gICAgICAgICAgICBpZiAoZiA9IDEsIHkgJiYgKHQgPSBvcFswXSAmIDIgPyB5W1wicmV0dXJuXCJdIDogb3BbMF0gPyB5W1widGhyb3dcIl0gfHwgKCh0ID0geVtcInJldHVyblwiXSkgJiYgdC5jYWxsKHkpLCAwKSA6IHkubmV4dCkgJiYgISh0ID0gdC5jYWxsKHksIG9wWzFdKSkuZG9uZSkgcmV0dXJuIHQ7XG4gICAgICAgICAgICBpZiAoeSA9IDAsIHQpIG9wID0gW29wWzBdICYgMiwgdC52YWx1ZV07XG4gICAgICAgICAgICBzd2l0Y2ggKG9wWzBdKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAwOiBjYXNlIDE6IHQgPSBvcDsgYnJlYWs7XG4gICAgICAgICAgICAgICAgY2FzZSA0OiBfLmxhYmVsKys7IHJldHVybiB7IHZhbHVlOiBvcFsxXSwgZG9uZTogZmFsc2UgfTtcbiAgICAgICAgICAgICAgICBjYXNlIDU6IF8ubGFiZWwrKzsgeSA9IG9wWzFdOyBvcCA9IFswXTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgY2FzZSA3OiBvcCA9IF8ub3BzLnBvcCgpOyBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgICAgICAgICAgIGlmICghKHQgPSBfLnRyeXMsIHQgPSB0Lmxlbmd0aCA+IDAgJiYgdFt0Lmxlbmd0aCAtIDFdKSAmJiAob3BbMF0gPT09IDYgfHwgb3BbMF0gPT09IDIpKSB7IF8gPSAwOyBjb250aW51ZTsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDMgJiYgKCF0IHx8IChvcFsxXSA+IHRbMF0gJiYgb3BbMV0gPCB0WzNdKSkpIHsgXy5sYWJlbCA9IG9wWzFdOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAob3BbMF0gPT09IDYgJiYgXy5sYWJlbCA8IHRbMV0pIHsgXy5sYWJlbCA9IHRbMV07IHQgPSBvcDsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHQgJiYgXy5sYWJlbCA8IHRbMl0pIHsgXy5sYWJlbCA9IHRbMl07IF8ub3BzLnB1c2gob3ApOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodFsyXSkgXy5vcHMucG9wKCk7XG4gICAgICAgICAgICAgICAgICAgIF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBvcCA9IGJvZHkuY2FsbCh0aGlzQXJnLCBfKTtcbiAgICAgICAgfSBjYXRjaCAoZSkgeyBvcCA9IFs2LCBlXTsgeSA9IDA7IH0gZmluYWxseSB7IGYgPSB0ID0gMDsgfVxuICAgICAgICBpZiAob3BbMF0gJiA1KSB0aHJvdyBvcFsxXTsgcmV0dXJuIHsgdmFsdWU6IG9wWzBdID8gb3BbMV0gOiB2b2lkIDAsIGRvbmU6IHRydWUgfTtcbiAgICB9XG59O1xuaW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSc7XG5pbXBvcnQgcmVhY3QgZnJvbSAnQHZpdGVqcy9wbHVnaW4tcmVhY3QnO1xuaW1wb3J0IHBhdGggZnJvbSAncGF0aCc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJztcbmltcG9ydCBmcyBmcm9tICdmcyc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGRvdGVudiwgZXhwcmVzc0FwcDtcbiAgICAgICAgICAgICAgICByZXR1cm4gX19nZW5lcmF0b3IodGhpcywgZnVuY3Rpb24gKF9hKSB7XG4gICAgICAgICAgICAgICAgICAgIHN3aXRjaCAoX2EubGFiZWwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMDogcmV0dXJuIFs0IC8qeWllbGQqLywgaW1wb3J0KCdkb3RlbnYnKV07XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDE6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZG90ZW52ID0gX2Euc2VudCgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRvdGVudi5jb25maWcoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJy4vc2VydmVyL2FwcC5qcycpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMjpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBleHByZXNzQXBwID0gKF9hLnNlbnQoKSkuZGVmYXVsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAgPSBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNlcnZlci5taWRkbGV3YXJlcy51c2UoJy9hcGknLCBmdW5jdGlvbiAocmVxLCByZXMsIG5leHQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIF9hO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBcdTI3MDUgUGF0Y2ggU1NFIHJvdXRlcyB0byBmbHVzaCBpbW1lZGlhdGVseSBcdTIwMTQgcHJldmVudHMgVml0ZSBidWZmZXJpbmcgdG9rZW5zXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmICgoX2EgPSByZXEudXJsKSA9PT0gbnVsbCB8fCBfYSA9PT0gdm9pZCAwID8gdm9pZCAwIDogX2Euc3RhcnRzV2l0aCgnL2NoYXQnKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLnNldEhlYWRlcignWC1BY2NlbC1CdWZmZXJpbmcnLCAnbm8nKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciBvcmlnaW5hbFdyaXRlXzEgPSByZXMud3JpdGUuYmluZChyZXMpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzLndyaXRlID0gZnVuY3Rpb24gKGNodW5rKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIHJlc3VsdCA9IG9yaWdpbmFsV3JpdGVfMShjaHVuayk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYgKHR5cGVvZiByZXMuZmx1c2ggPT09ICdmdW5jdGlvbicpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcy5mbHVzaCgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiByZXN1bHQ7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcChyZXEsIHJlcywgbmV4dCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIFsyIC8qcmV0dXJuKi9dO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICB9O1xufVxuZnVuY3Rpb24gY29weU5ldGxpZnlGaWxlcygpIHtcbiAgICByZXR1cm4ge1xuICAgICAgICBuYW1lOiAnY29weS1uZXRsaWZ5LWZpbGVzJyxcbiAgICAgICAgY2xvc2VCdW5kbGU6IGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgIC8vIENvcHkgX3JlZGlyZWN0c1xuICAgICAgICAgICAgdmFyIHJlZGlyZWN0c1NyYyA9IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICdkaXN0L19yZWRpcmVjdHMnKTtcbiAgICAgICAgICAgIGlmIChmcy5leGlzdHNTeW5jKHJlZGlyZWN0c1NyYykpIHtcbiAgICAgICAgICAgICAgICBjb25zb2xlLmxvZygnXHUyNzA1IF9yZWRpcmVjdHMgZXhpc3RzIGluIGRpc3QnKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIC8vIENvcHkgbmV0bGlmeS50b21sXG4gICAgICAgICAgICB2YXIgbmV0bGlmeVRvbWwgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnbmV0bGlmeS50b21sJyk7XG4gICAgICAgICAgICB2YXIgbmV0bGlmeVRvbWxEZXN0ID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ2Rpc3QvbmV0bGlmeS50b21sJyk7XG4gICAgICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhuZXRsaWZ5VG9tbCkpIHtcbiAgICAgICAgICAgICAgICBmcy5jb3B5RmlsZVN5bmMobmV0bGlmeVRvbWwsIG5ldGxpZnlUb21sRGVzdCk7XG4gICAgICAgICAgICAgICAgY29uc29sZS5sb2coJ1x1MjcwNSBuZXRsaWZ5LnRvbWwgY29waWVkIHRvIGRpc3QnKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIC8vIENvcHkgc2VlZF9kb2N1bWVudHMgZm9sZGVyIHRvIGRpc3RcbiAgICAgICAgICAgIHZhciBzZWVkU3JjID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ3NlZWRfZG9jdW1lbnRzJyk7XG4gICAgICAgICAgICB2YXIgc2VlZERlc3QgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnZGlzdC9zZWVkX2RvY3VtZW50cycpO1xuICAgICAgICAgICAgaWYgKGZzLmV4aXN0c1N5bmMoc2VlZFNyYykpIHtcbiAgICAgICAgICAgICAgICBmcy5ta2RpclN5bmMoc2VlZERlc3QsIHsgcmVjdXJzaXZlOiB0cnVlIH0pO1xuICAgICAgICAgICAgICAgIHZhciBmaWxlcyA9IGZzLnJlYWRkaXJTeW5jKHNlZWRTcmMpO1xuICAgICAgICAgICAgICAgIGZpbGVzLmZvckVhY2goZnVuY3Rpb24gKGZpbGUpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyIHNyY0ZpbGUgPSBwYXRoLmpvaW4oc2VlZFNyYywgZmlsZSk7XG4gICAgICAgICAgICAgICAgICAgIHZhciBkZXN0RmlsZSA9IHBhdGguam9pbihzZWVkRGVzdCwgZmlsZSk7XG4gICAgICAgICAgICAgICAgICAgIGlmIChmcy5zdGF0U3luYyhzcmNGaWxlKS5pc0ZpbGUoKSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgZnMuY29weUZpbGVTeW5jKHNyY0ZpbGUsIGRlc3RGaWxlKTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgIGNvbnNvbGUubG9nKFwiXFx1MjcwNSBzZWVkX2RvY3VtZW50cyBjb3BpZWQgdG8gZGlzdCAoXCIuY29uY2F0KGZpbGVzLmxlbmd0aCwgXCIgZmlsZXMpXCIpKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIC8vIENvcHkgZ29vZ2xlX2NyZWRlbnRpYWxzIGZvbGRlciB0byBkaXN0XG4gICAgICAgICAgICB2YXIgY3JlZHNTcmMgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnZ29vZ2xlX2NyZWRlbnRpYWxzJyk7XG4gICAgICAgICAgICB2YXIgY3JlZHNEZXN0ID0gcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJ2Rpc3QvZ29vZ2xlX2NyZWRlbnRpYWxzJyk7XG4gICAgICAgICAgICBpZiAoZnMuZXhpc3RzU3luYyhjcmVkc1NyYykpIHtcbiAgICAgICAgICAgICAgICBmcy5ta2RpclN5bmMoY3JlZHNEZXN0LCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbiAgICAgICAgICAgICAgICB2YXIgZmlsZXMgPSBmcy5yZWFkZGlyU3luYyhjcmVkc1NyYyk7XG4gICAgICAgICAgICAgICAgZmlsZXMuZm9yRWFjaChmdW5jdGlvbiAoZmlsZSkge1xuICAgICAgICAgICAgICAgICAgICB2YXIgc3JjRmlsZSA9IHBhdGguam9pbihjcmVkc1NyYywgZmlsZSk7XG4gICAgICAgICAgICAgICAgICAgIHZhciBkZXN0RmlsZSA9IHBhdGguam9pbihjcmVkc0Rlc3QsIGZpbGUpO1xuICAgICAgICAgICAgICAgICAgICBpZiAoZnMuc3RhdFN5bmMoc3JjRmlsZSkuaXNGaWxlKCkpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGZzLmNvcHlGaWxlU3luYyhzcmNGaWxlLCBkZXN0RmlsZSk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICBjb25zb2xlLmxvZyhcIlxcdTI3MDUgZ29vZ2xlX2NyZWRlbnRpYWxzIGNvcGllZCB0byBkaXN0IChcIi5jb25jYXQoZmlsZXMubGVuZ3RoLCBcIiBmaWxlcylcIikpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9XG4gICAgfTtcbn1cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gICAgcGx1Z2luczogW3JlYWN0KCksIGV4cHJlc3NQbHVnaW4oKSwgY29weU5ldGxpZnlGaWxlcygpXSxcbiAgICByZXNvbHZlOiB7XG4gICAgICAgIGFsaWFzOiB7XG4gICAgICAgICAgICAnQCc6IHBhdGgucmVzb2x2ZShfX2Rpcm5hbWUsICcuL3NyYycpLFxuICAgICAgICB9LFxuICAgIH0sXG4gICAgc2VydmVyOiB7XG4gICAgICAgIHBvcnQ6IDUxNzMsXG4gICAgfSxcbn0pO1xuIl0sCiAgIm1hcHBpbmdzIjogIjs7Ozs7Ozs7Ozs7QUFBNlEsU0FBUyxhQUFhLFFBQVEseUJBQXlCLGNBQWMsUUFBUSxLQUFLLFdBQVc7QUFDMVcsU0FBUyxtQ0FBbUM7QUFDNUMsU0FBUyxNQUFNLGNBQWM7QUFrQjdCLFNBQVMsaUJBQWlCO0FBQ3hCLE1BQUksQ0FBQyxhQUFhO0FBQ2hCLFVBQU0sU0FBUyxRQUFRLElBQUk7QUFDM0IsVUFBTSxTQUFTLFFBQVEsSUFBSSxpQkFBaUI7QUFDNUMsVUFBTSxXQUFXLFFBQVEsSUFBSSxtQkFBbUI7QUFDaEQsVUFBTSxPQUFPLFFBQVEsSUFBSSxlQUFlO0FBRXhDLFlBQVEsSUFBSSxxQ0FBcUM7QUFDakQsWUFBUSxJQUFJLGVBQWUsUUFBUSw2QkFBNkI7QUFDaEUsWUFBUSxJQUFJLGVBQWUsTUFBTTtBQUNqQyxZQUFRLElBQUksZUFBZSxRQUFRO0FBQ25DLFlBQVEsSUFBSSxlQUFlLFNBQVMsbUJBQW1CLHFCQUFxQjtBQUM1RSxZQUFRLElBQUkscUNBQXFDO0FBRWpELFFBQUksQ0FBQyxRQUFRO0FBQ1gsWUFBTSxJQUFJO0FBQUEsUUFDUjtBQUFBLE1BRUY7QUFBQSxJQUNGO0FBRUEsVUFBTSxnQkFBZ0IsRUFBRSxRQUFRLFFBQVEsU0FBUztBQUNqRCxRQUFJLEtBQU0sZUFBYyxPQUFPO0FBQy9CLGtCQUFjLElBQUksWUFBWSxhQUFhO0FBQUEsRUFDN0M7QUFDQSxTQUFPO0FBQ1Q7QUFFQSxlQUFzQixzQkFBc0I7QUFDMUMsTUFBSSxDQUFDLGtCQUFrQjtBQUNyQixVQUFNLFNBQVMsZUFBZTtBQUM5QixVQUFNLGlCQUFpQixRQUFRLElBQUksNEJB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SjtBQUFBLEVBQ0o7QUFDSjtBQUNBLElBQU8sc0JBQVEsYUFBYTtBQUFBLEVBQ3hCLFNBQVMsQ0FBQyxNQUFNLEdBQUcsY0FBYyxHQUFHLGlCQUFpQixDQUFDO0FBQUEsRUFDdEQsU0FBUztBQUFBLElBQ0wsT0FBTztBQUFBLE1BQ0gsS0FBS0wsTUFBSyxRQUFRRCxZQUFXLE9BQU87QUFBQSxJQUN4QztBQUFBLEVBQ0o7QUFBQSxFQUNBLFFBQVE7QUFBQSxJQUNKLE1BQU07QUFBQSxFQUNWO0FBQ0osQ0FBQzsiLAogICJuYW1lcyI6IFsicGF0aCIsICJ1dWlkdjQiLCAiUm91dGVyIiwgInBhdGgiLCAiZnMiLCAidXVpZHY0IiwgImZpbGVVUkxUb1BhdGgiLCAiQkFUQ0hfU0laRSIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgInJvdXRlciIsICJfX2ZpbGVuYW1lIiwgIl9fZGlybmFtZSIsICJ1dWlkdjQiLCAiR29vZ2xlR2VuQUkiLCAiZnMiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgImxvYWRHb29nbGVDcmVkZW50aWFscyIsICJfX2Rpcm5hbWUiLCAiX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCIsICJfX2ZpbGVuYW1lIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgIlJvdXRlciIsICJ1dWlkdjQiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJmcyIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIiwgImRvdGVudiIsICJfYSIsICJmcyJdCn0K
