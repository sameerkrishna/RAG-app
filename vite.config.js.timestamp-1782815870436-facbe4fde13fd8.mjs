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
    return sessionCollections.get(sessionId);
  }
  const client = getCloudClient();
  const collectionName = `session_${sessionId}`;
  try {
    const collection = await client.getOrCreateCollection({
      name: collectionName,
      metadata: {
        type: "session_upload",
        session_id: sessionId,
        created: (/* @__PURE__ */ new Date()).toISOString()
      },
      embeddingFunction: null
    });
    sessionCollections.set(sessionId, collection);
    console.log(`\u2705 Session collection ready: ${collectionName}`);
    return collection;
  } catch (error) {
    console.error(`Failed to create session collection ${collectionName}:`, error);
    throw error;
  }
}
async function addVectors(collection, vectors, embeddings, ids) {
  try {
    await collection.add({
      ids,
      embeddings,
      documents: vectors.map((v) => v.text),
      metadatas: vectors.map((v) => v.metadata)
    });
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
    const existing = await collection.get({
      where: { document_id: documentId }
    });
    if (existing.ids && existing.ids.length > 0) {
      await collection.delete({ ids: existing.ids });
      return existing.ids.length;
    }
    return 0;
  } catch (error) {
    console.error("Failed to delete document vectors:", error);
    throw error;
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
var cloudClient, globalCollection, sessionCollections;
var init_chromaService = __esm({
  "server/services/chromaService.js"() {
    "use strict";
    cloudClient = null;
    globalCollection = null;
    sessionCollections = /* @__PURE__ */ new Map();
  }
});

// server/utils/errors.js
function is429Error(error) {
  return error?.code === 429 || error?.status === 429 || error?.message?.includes("429") || error?.message?.includes("RESOURCE_EXHAUSTED") || error?.message?.includes("Too Many Requests");
}
var AppError, ValidationError, InvalidFileTypeError, TooManyPDFsError, DuplicateFileError, CorruptedPDFError, LLMUnavailableError, EmbeddingError, WebSearchUnavailableError;
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
    TooManyPDFsError = class extends AppError {
      constructor(max) {
        super(`Maximum ${max} PDFs allowed per session`, "TOO_MANY_PDFS", 400);
      }
    };
    DuplicateFileError = class extends AppError {
      constructor(filename) {
        super(`File "${filename}" already exists in this session`, "DUPLICATE_FILE", 409);
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
import { GoogleGenerativeAI } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getEmbeddingModel() {
  if (!embeddingModel) {
    genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
    embeddingModel = genAI.getGenerativeModel({
      model: process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-001"
    });
  }
  return embeddingModel;
}
async function embedBatch(texts, taskType = "RETRIEVAL_DOCUMENT", attempt = 1) {
  const maxAttempts = 5;
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || "gemini-embedding-001";
  try {
    const model2 = getEmbeddingModel();
    const result = await model2.batchEmbedContents({
      requests: texts.map((text) => ({
        model: `models/${modelName}`,
        content: { parts: [{ text }] },
        taskType,
        outputDimensionality: OUTPUT_DIMENSIONS()
      }))
    });
    if (!result?.embeddings || result.embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${result?.embeddings?.length ?? 0}`);
    }
    return result.embeddings.map((e) => {
      if (!e?.values) throw new EmbeddingError("Missing values in embedding response");
      return e.values;
    });
  } catch (error) {
    const is429 = is429Error(error) || error?.status === 429 || error?.message?.includes("RESOURCE_EXHAUSTED");
    if (is429 && attempt < maxAttempts) {
      const retryDelay = error.retryAfter || GROUP_WAIT_MS;
      console.log(`Rate limited, waiting ${retryDelay / 1e3}s (attempt ${attempt}/${maxAttempts})`);
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      return embedBatch(texts, taskType, attempt + 1);
    }
    throw new EmbeddingError(error.message || "Batch embedding failed");
  }
}
async function generateEmbeddings(chunks, taskType = "RETRIEVAL_DOCUMENT", onProgress) {
  if (!chunks || chunks.length === 0) return [];
  const batchSize = BATCH_SIZE();
  const parallelCalls = PARALLEL_CALLS();
  const embeddings = [];
  const batches = [];
  for (let i = 0; i < chunks.length; i += batchSize) {
    batches.push(chunks.slice(i, i + batchSize));
  }
  const totalGroups = Math.ceil(batches.length / parallelCalls);
  for (let i = 0; i < batches.length; i += parallelCalls) {
    const parallelBatches = batches.slice(i, i + parallelCalls);
    const groupNum = Math.floor(i / parallelCalls) + 1;
    const chunksCovered = Math.min((i + parallelCalls) * batchSize, chunks.length);
    console.log(`  Embedding group ${groupNum}/${totalGroups} \u2014 ${parallelBatches.length} batch call(s) in parallel (chunks ${i * batchSize + 1}\u2013${chunksCovered})...`);
    const results = await Promise.allSettled(
      parallelBatches.map((batch) => embedBatch(batch.map((c) => c.text), taskType))
    );
    const failedBatches = [];
    results.forEach((result, batchIdx) => {
      const batch = parallelBatches[batchIdx];
      if (result.status === "fulfilled") {
        const vectors = result.value;
        batch.forEach((chunk, chunkIdx) => {
          const absoluteChunkIdx = (i + batchIdx) * batchSize + chunkIdx;
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_${absoluteChunkIdx}`,
            embedding: vectors[chunkIdx],
            metadata: chunk.metadata,
            text: chunk.text
          });
        });
      } else {
        console.warn(`  Batch ${i + batchIdx} failed, will retry individually:`, result.reason?.message);
        failedBatches.push({ batch, batchIdx: i + batchIdx });
      }
    });
    if (onProgress) {
      onProgress({ current_batch: groupNum, total_batches: totalGroups });
    }
    const isLastGroup = i + parallelCalls >= batches.length;
    if (!isLastGroup || failedBatches.length > 0) {
      console.log(`  Waiting ${GROUP_WAIT_MS / 1e3}s before next group...`);
      await new Promise((resolve) => setTimeout(resolve, GROUP_WAIT_MS));
    }
    for (const { batch, batchIdx } of failedBatches) {
      console.log(`  Waiting ${RETRY_WAIT_MS / 1e3}s before retrying failed batch ${batchIdx}...`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_WAIT_MS));
      for (const chunk of batch) {
        try {
          const vectors = await embedBatch([chunk.text], taskType);
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_retry_${batchIdx}`,
            embedding: vectors[0],
            metadata: chunk.metadata,
            text: chunk.text
          });
          console.log(`  \u2705 Retry succeeded for chunk ${chunk.metadata?.chunk_id}`);
        } catch (err) {
          console.error(`  \u274C Retry failed for chunk ${chunk.metadata?.chunk_id}:`, err.message);
        }
      }
    }
  }
  return embeddings;
}
async function embedQuery(query) {
  const vectors = await embedBatch([query], "RETRIEVAL_QUERY");
  return vectors[0];
}
function getRateLimitState() {
  return {
    maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 3e4,
    parallelCalls: PARALLEL_CALLS(),
    maxChunksPerCall: BATCH_SIZE(),
    outputDimensions: OUTPUT_DIMENSIONS()
  };
}
var genAI, embeddingModel, BATCH_SIZE, PARALLEL_CALLS, OUTPUT_DIMENSIONS, GROUP_WAIT_MS, RETRY_WAIT_MS;
var init_embeddingService = __esm({
  "server/services/embeddingService.js"() {
    "use strict";
    init_errors();
    genAI = null;
    embeddingModel = null;
    BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
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
function createSession() {
  const sessionId = uuidv42();
  const session = {
    id: sessionId,
    createdAt: /* @__PURE__ */ new Date(),
    lastAccessed: /* @__PURE__ */ new Date(),
    documents: [],
    timeoutMinutes: DEFAULT_TIMEOUT_MINUTES
  };
  sessions.set(sessionId, session);
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
  console.log("In the init sessionwith gobaldocs function");
  if (seededSessions.has(sessionId)) return;
  try {
    console.log(`\u{1F331} Seeding session ${sessionId} from global collection...`);
    const globalCollection2 = await getGlobalCollection();
    const sessionCollection = await getSessionCollection(sessionId);
    const allGlobal = await globalCollection2.get({
      include: ["embeddings", "documents", "metadatas"]
    });
    if (!allGlobal.ids || allGlobal.ids.length === 0) {
      console.log("\u26A0\uFE0F  Global collection is empty \u2014 nothing to seed.");
      seededSessions.add(sessionId);
      return;
    }
    const existing = await sessionCollection.get({ ids: allGlobal.ids });
    const existingIds = new Set(existing.ids || []);
    const newIds = allGlobal.ids.filter((id) => !existingIds.has(id));
    if (newIds.length === 0) {
      console.log(`\u2705 Session ${sessionId} already has all global vectors.`);
      seededSessions.add(sessionId);
      return;
    }
    const newIndices = allGlobal.ids.map((id, i) => i).filter((i) => newIds.includes(allGlobal.ids[i]));
    await sessionCollection.add({
      ids: newIndices.map((i) => allGlobal.ids[i]),
      embeddings: newIndices.map((i) => allGlobal.embeddings[i]),
      documents: newIndices.map((i) => allGlobal.documents[i]),
      metadatas: newIndices.map((i) => ({
        ...allGlobal.metadatas[i],
        source_type: "global"
        // preserve so UI can still show Seed badge
      }))
    });
    console.log(`\u2705 Seeded ${newIds.length} vectors into session ${sessionId}`);
    seededSessions.add(sessionId);
    const session = getSession(sessionId);
    if (session) {
      const docsMap = /* @__PURE__ */ new Map();
      allGlobal.metadatas.forEach((meta) => {
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
  session.documents.push({
    id: documentInfo.id,
    filename: documentInfo.filename,
    fileSize: documentInfo.fileSize,
    pageCount: documentInfo.pageCount,
    uploadTimestamp: /* @__PURE__ */ new Date(),
    chunkCount: documentInfo.chunkCount,
    sourceType: "session_upload"
  });
  session.lastAccessed = /* @__PURE__ */ new Date();
  return true;
}
function removeDocumentFromSession(sessionId, documentId) {
  const session = getSession(sessionId);
  if (!session) return false;
  const idx = session.documents.findIndex((d) => d.id === documentId);
  if (idx >= 0) {
    session.documents.splice(idx, 1);
    session.lastAccessed = /* @__PURE__ */ new Date();
    return true;
  }
  return false;
}
function getSessionDocuments(sessionId) {
  const session = getSession(sessionId);
  if (!session) return [];
  return session.documents;
}
async function getAllDocuments(sessionId) {
  const sessionDocs = getSessionDocuments(sessionId);
  return {
    // Split for UI — global seeded docs vs user-uploaded docs
    sessionDocuments: sessionDocs.filter((d) => d.sourceType === "session_upload"),
    globalDocuments: sessionDocs.filter((d) => d.sourceType === "global")
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
    const collection = await getSessionCollection(sessionId);
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
async function handleUpload(req, res) {
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
      throw new TooManyPDFsError(maxPDFs);
    }
    if (session.documents.some((d) => d.filename === cleanFilename)) {
      fs.unlinkSync(file.path);
      throw new DuplicateFileError(cleanFilename);
    }
    const { fullText, pageMap, totalPages } = await parsePDFWithBoundaryMap(file.path);
    if (!fullText || fullText.trim().length < 50) {
      fs.unlinkSync(file.path);
      return res.status(422).json({
        error: "No extractable text found \u2014 PDF may be scanned or image-only",
        code: "EMPTY_PDF"
      });
    }
    const documentId = path2.parse(file.filename).name;
    const rawChunks = chunkText(fullText, {
      chunkSizeTokens: 1e3,
      overlapTokens: 200
    });
    if (rawChunks.length === 0) {
      fs.unlinkSync(file.path);
      return res.status(422).json({ error: "No content could be extracted from PDF", code: "EMPTY_PDF" });
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
        // always session_upload for user uploads
        upload_timestamp: (/* @__PURE__ */ new Date()).toISOString(),
        char_start: chunk.charStart,
        char_end: chunk.charEnd,
        token_count: chunk.tokenCount
      }
    }));
    const collection = await getSessionCollection(sessionId);
    const embeddings = await generateEmbeddings(
      chunks,
      "RETRIEVAL_DOCUMENT",
      ({ current_batch, total_batches }) => {
        if (req.app.locals.progressCallbacks) {
          req.app.locals.progressCallbacks.emit(`progress_${sessionId}`, {
            documentId,
            current_batch,
            total_batches,
            stage: "embedding"
          });
        }
      }
    );
    if (embeddings.length === 0) {
      fs.unlinkSync(file.path);
      return res.status(503).json({ error: "Failed to generate embeddings", code: "EMBEDDING_FAILED" });
    }
    await addVectors(
      collection,
      embeddings.map((e) => ({ text: e.text, metadata: e.metadata })),
      embeddings.map((e) => e.embedding),
      embeddings.map((e) => e.id)
    );
    invalidateSessionCollectionCache(sessionId);
    addDocumentToSession(sessionId, {
      id: documentId,
      filename: cleanFilename,
      fileSize: file.size,
      pageCount: totalPages,
      chunkCount: embeddings.length
    });
    res.status(201).json({
      success: true,
      document: {
        id: documentId,
        filename: cleanFilename,
        fileSize: file.size,
        pageCount: totalPages,
        chunkCount: embeddings.length,
        uploadTimestamp: (/* @__PURE__ */ new Date()).toISOString()
      },
      sessionId
    });
  } catch (error) {
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    console.error("Upload error:", error);
    res.status(error.statusCode || 500).json({
      error: error.message,
      code: error.code || "UPLOAD_ERROR"
    });
  }
}
async function listDocumentsHandler(req, res) {
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  try {
    const documents = await getAllDocuments(sessionId);
    res.json(documents);
  } catch (error) {
    console.error("List documents error:", error);
    res.status(500).json({ error: "Failed to list documents", code: "LIST_ERROR" });
  }
}
async function deleteDocument(req, res) {
  const { documentId } = req.params;
  const sessionId = req.headers["x-session-id"] || req.query.sessionId;
  try {
    if (sessionId) {
      const collection = await getSessionCollection(sessionId);
      if (collection) {
        const count = await deleteDocumentVectors(collection, documentId);
        if (count > 0) {
          removeDocumentFromSession(sessionId, documentId);
          invalidateSessionCollectionCache(sessionId);
        }
      }
    }
    const tmpPath = path2.join(uploadDir, `${documentId}.pdf`);
    if (fs.existsSync(tmpPath)) {
      fs.unlinkSync(tmpPath);
      console.log(`\u{1F5D1}\uFE0F  Deleted tmp file: ${tmpPath}`);
    }
    res.json({ success: true, documentId });
  } catch (error) {
    console.error("Delete document error:", error);
    res.status(500).json({ error: "Failed to delete document", code: "DELETE_ERROR" });
  }
}
async function getDocumentFile(req, res) {
  const { documentId } = req.params;
  const filename = req.query.filename;
  try {
    const uploadPath = path2.join(uploadDir, `${documentId}.pdf`);
    if (fs.existsSync(uploadPath)) {
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Disposition", contentDisposition(`${documentId}.pdf`));
      return fs.createReadStream(uploadPath).pipe(res);
    }
    if (filename) {
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
      filename: (req, file, cb) => cb(null, `${uuidv44()}${path2.extname(file.originalname)}`)
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

// server/services/promptService.js
var init_promptService = __esm({
  "server/services/promptService.js"() {
    "use strict";
    init_memoryService();
    init_retrievalService();
  }
});

// server/services/geminiService.js
import { GoogleGenerativeAI as GoogleGenerativeAI2 } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getGenAI() {
  if (!genAI2) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("GEMINI_API_KEY is undefined");
    genAI2 = new GoogleGenerativeAI2(apiKey);
  }
  return genAI2;
}
function getPrimaryModel() {
  if (!primaryModel) {
    primaryModel = getGenAI().getGenerativeModel({ model: PRIMARY_MODEL });
  }
  return primaryModel;
}
function getFallbackModel() {
  if (!fallbackModel) {
    fallbackModel = getGenAI().getGenerativeModel({ model: FALLBACK_MODEL });
  }
  return fallbackModel;
}
async function* streamResponse(prompt) {
  let model2 = getPrimaryModel();
  let retries = 0;
  const maxRetries = 2;
  while (retries < maxRetries) {
    try {
      const controller = new AbortController();
      const result = await model2.generateContentStream({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      });
      let firstToken = true;
      const firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);
      for await (const chunk of result.stream) {
        if (controller.signal.aborted) {
          clearTimeout(firstTokenTimeout);
          throw new Error("First token timeout \u2014 no response from model");
        }
        const text = chunk.text();
        if (text) {
          if (firstToken) {
            firstToken = false;
            clearTimeout(firstTokenTimeout);
          }
          yield { type: "token", text };
        }
      }
      clearTimeout(firstTokenTimeout);
      return { success: true };
    } catch (error) {
      retries++;
      console.error(`Model attempt ${retries} failed:`, error.message);
      if (retries >= maxRetries) {
        yield { type: "error", error: error.message };
        throw new LLMUnavailableError();
      }
      model2 = getFallbackModel();
    }
  }
}
var genAI2, PRIMARY_MODEL, FALLBACK_MODEL, FIRST_TOKEN_TIMEOUT, REQUEST_TIMEOUT, primaryModel, fallbackModel;
var init_geminiService = __esm({
  "server/services/geminiService.js"() {
    "use strict";
    init_promptService();
    init_errors();
    genAI2 = null;
    PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || "gemini-3.1-flash-lite";
    FALLBACK_MODEL = process.env.GEMINI_MODEL_FALLBACK || "gemini-2.5-flash";
    FIRST_TOKEN_TIMEOUT = parseInt(process.env.LLM_FIRST_TOKEN_TIMEOUT_SECONDS) * 1e3 || 12e3;
    REQUEST_TIMEOUT = parseInt(process.env.LLM_REQUEST_TIMEOUT_SECONDS) * 1e3 || 45e3;
    primaryModel = null;
    fallbackModel = null;
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
function expandQuery(query, sessionId) {
  const words = query.trim().split(/\s+/);
  if (words.length > 4) return query;
  const recentTurns = getRecentTurns(sessionId, 4);
  const recentContext = recentTurns.filter((t) => t.role === "user").map((t) => t.content).join(" ");
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
  const queryWords = query.toLowerCase().split(/\s+/);
  const contextRelevant = queryWords.some(
    (w) => w.length > 3 && recentContext.toLowerCase().includes(w)
  );
  const domainHint = contextRelevant ? `${recentContext.slice(0, 80)}: ` : "";
  return `${domainHint}${query} ${expansions.join(" ")}`;
}
async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId } = req.body;
  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return res.status(400).json({ error: "Query is required", code: "MISSING_QUERY" });
  }
  const sessionId = providedSessionId || uuidv45();
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
  addTurnWithCitations(sessionId, "user", query.trim());
  try {
    sendEvent("status", { stage: "retrieving", message: "Searching knowledge base..." });
    const expandedQuery = expandQuery(query, sessionId);
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
    const memoryContext = getRecentTurns(sessionId, 5).map((t) => `${t.role === "user" ? "User" : "Assistant"}: ${t.content}`).join("\n\n");
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
${memoryContext}

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
    addTurnWithCitations(sessionId, "assistant", rewrittenResponse, finalCitations, coverage, answerId);
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
import { GoogleGenerativeAI as GoogleGenerativeAI3 } from "file:///home/project/node_modules/@google/generative-ai/dist/index.mjs";
function getModel() {
  if (!model) {
    model = genAI3.getGenerativeModel({ model: PRIMARY_MODEL2 });
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
var genAI3, PRIMARY_MODEL2, model;
var init_webSearchService = __esm({
  "server/services/webSearchService.js"() {
    "use strict";
    init_errors();
    genAI3 = new GoogleGenerativeAI3(process.env.GEMINI_API_KEY);
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcblxuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuZnVuY3Rpb24gZ2V0Q2xpZW50KCkge1xuICByZXR1cm4gZ2V0Q2xvdWRDbGllbnQoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEdsb2JhbENvbGxlY3Rpb24oKSB7XG4gIGlmICghZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBwcm9jZXNzLmVudi5DSFJPTUFfR0xPQkFMX0NPTExFQ1RJT04gfHwgJ2Rldic7XG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgICAgZGVzY3JpcHRpb246ICdQZXJtYW5lbnQgc2VlZCBkb2N1bWVudHMgZm9yIFJBRycsXG4gICAgICAgICAgdHlwZTogJ2dsb2JhbF9rbm93bGVkZ2UnXG4gICAgICAgIH0sXG4gICAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgR2xvYmFsIGNvbGxlY3Rpb24gcmVhZHk6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjb25uZWN0IHRvIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHNlc3Npb25Db2xsZWN0aW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgfVxuICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRPckNyZWF0ZUNvbGxlY3Rpb24oe1xuICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICB0eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICBzZXNzaW9uX2lkOiBzZXNzaW9uSWQsXG4gICAgICAgIGNyZWF0ZWQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgfSk7XG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLnNldChzZXNzaW9uSWQsIGNvbGxlY3Rpb24pO1xuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIHJldHVybiBjb2xsZWN0aW9uO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBjcmVhdGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGF3YWl0IGNsaWVudC5kZWxldGVDb2xsZWN0aW9uKHsgbmFtZTogY29sbGVjdGlvbk5hbWUgfSk7XG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGRlbGV0ZWQ6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGRlbGV0ZSBzZXNzaW9uIGNvbGxlY3Rpb24gJHtjb2xsZWN0aW9uTmFtZX06YCwgZXJyb3IpO1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gYWRkVmVjdG9ycyhjb2xsZWN0aW9uLCB2ZWN0b3JzLCBlbWJlZGRpbmdzLCBpZHMpIHtcbiAgdHJ5IHtcbiAgICBhd2FpdCBjb2xsZWN0aW9uLmFkZCh7XG4gICAgICBpZHMsXG4gICAgICBlbWJlZGRpbmdzLFxuICAgICAgZG9jdW1lbnRzOiB2ZWN0b3JzLm1hcCh2ID0+IHYudGV4dCksXG4gICAgICBtZXRhZGF0YXM6IHZlY3RvcnMubWFwKHYgPT4gdi5tZXRhZGF0YSlcbiAgICB9KTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gYWRkIHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBxdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IGNvbGxlY3Rpb24ucXVlcnkoe1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9KTtcblxuICAgIGlmICghcmVzdWx0cy5pZHMgfHwgcmVzdWx0cy5pZHMubGVuZ3RoID09PSAwIHx8IHJlc3VsdHMuaWRzWzBdLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHRzLmlkc1swXS5tYXAoKGlkLCBpZHgpID0+ICh7XG4gICAgICBpZCxcbiAgICAgIHRleHQ6IHJlc3VsdHMuZG9jdW1lbnRzWzBdW2lkeF0sXG4gICAgICBtZXRhZGF0YTogcmVzdWx0cy5tZXRhZGF0YXNbMF1baWR4XSxcbiAgICAgIGRpc3RhbmNlOiByZXN1bHRzLmRpc3RhbmNlc1swXVtpZHhdLFxuICAgICAgc2NvcmU6IDEgLSByZXN1bHRzLmRpc3RhbmNlc1swXVtpZHhdXG4gICAgfSkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBxdWVyeSBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBleGlzdGluZyA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgIHdoZXJlOiB7IGRvY3VtZW50X2lkOiBkb2N1bWVudElkIH1cbiAgICB9KTtcbiAgICBpZiAoZXhpc3RpbmcuaWRzICYmIGV4aXN0aW5nLmlkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7IGlkczogZXhpc3RpbmcuaWRzIH0pO1xuICAgICAgcmV0dXJuIGV4aXN0aW5nLmlkcy5sZW5ndGg7XG4gICAgfVxuICAgIHJldHVybiAwO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbEl0ZW1zID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ11cbiAgICB9KTtcblxuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcblxuICAgIGlmIChhbGxJdGVtcy5pZHMpIHtcbiAgICAgIGFsbEl0ZW1zLmlkcy5mb3JFYWNoKChpZCwgaWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IG1ldGEgPSBhbGxJdGVtcy5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogZG9jSWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2NvdW50OiAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6IG1ldGEuc291cmNlX3R5cGUsXG4gICAgICAgICAgICBmaXJzdF9jaHVua190ZXh0OiBhbGxJdGVtcy5kb2N1bWVudHNbaWR4XVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgZG9jID0gZG9jdW1lbnRzTWFwLmdldChkb2NJZCk7XG4gICAgICAgIGRvYy5jaHVua19jb3VudCsrO1xuICAgICAgICBkb2MucGFnZV9jb3VudCA9IE1hdGgubWF4KGRvYy5wYWdlX2NvdW50LCBtZXRhLnBhZ2VfbnVtYmVyIHx8IDEpO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIEFycmF5LmZyb20oZG9jdW1lbnRzTWFwLnZhbHVlcygpKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHM6JywgZXJyb3IpO1xuICAgIHJldHVybiBbXTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoQ2hlY2soKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBoZWFydGJlYXQgPSBhd2FpdCBjbGllbnQuaGVhcnRiZWF0KCk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ2hlYWx0aHknLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBoZWFydGJlYXRcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICd1bmhlYWx0aHknLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgIH07XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGNsZWFudXBTZXNzaW9uQ29sbGVjdGlvbnMoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9ucyA9IGF3YWl0IGNsaWVudC5saXN0Q29sbGVjdGlvbnMoKTtcblxuICAgIC8vIEZJWCAxOiBjb2xsZWN0aW9ucyBpcyBhbiBhcnJheSBvZiBvYmplY3RzIHdpdGggLm5hbWUsIG5vdCBwbGFpbiBzdHJpbmdzXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcdTI3MDUgTm8gc3RhbGUgc2Vzc2lvbiBjb2xsZWN0aW9ucyBmb3VuZC4nKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHVEODNFXHVEREY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFx1MjcwNSBEZWxldGVkOiAke25hbWV9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgICBcdTI2QTBcdUZFMEYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY2xlYW51cCBjb21wbGV0ZS4nKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLndhcm4oJ1x1MjZBMFx1RkUwRiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdXZWIgc2VhcmNoIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1dFQl9TRUFSQ0hfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xubGV0IGVtYmVkZGluZ01vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0RW1iZWRkaW5nTW9kZWwoKSB7XG4gIGlmICghZW1iZWRkaW5nTW9kZWwpIHtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkocHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkpO1xuICAgIGVtYmVkZGluZ01vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHtcbiAgICAgIG1vZGVsOiBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZW1iZWRkaW5nTW9kZWw7XG59XG5cbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0O1xuY29uc3QgT1VUUFVUX0RJTUVOU0lPTlMgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7XG5jb25zdCBHUk9VUF9XQUlUX01TID0gNjEwMDA7XG5jb25zdCBSRVRSWV9XQUlUX01TID0gMTUwMDA7IC8vIEZJWCAzOiB3YWl0IGJlZm9yZSBpbmRpdmlkdWFsIGNodW5rIHJldHJpZXNcblxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbWF4QXR0ZW1wdHMgPSA1O1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldEVtYmVkZGluZ01vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5iYXRjaEVtYmVkQ29udGVudHMoe1xuICAgICAgcmVxdWVzdHM6IHRleHRzLm1hcCh0ZXh0ID0+ICh7XG4gICAgICAgIG1vZGVsOiBgbW9kZWxzLyR7bW9kZWxOYW1lfWAsXG4gICAgICAgIGNvbnRlbnQ6IHsgcGFydHM6IFt7IHRleHQgfV0gfSxcbiAgICAgICAgdGFza1R5cGUsXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBPVVRQVVRfRElNRU5TSU9OUygpXG4gICAgICB9KSlcbiAgICB9KTtcblxuICAgIGlmICghcmVzdWx0Py5lbWJlZGRpbmdzIHx8IHJlc3VsdC5lbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtyZXN1bHQ/LmVtYmVkZGluZ3M/Lmxlbmd0aCA/PyAwfWApO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHQuZW1iZWRkaW5ncy5tYXAoZSA9PiB7XG4gICAgICBpZiAoIWU/LnZhbHVlcykgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdNaXNzaW5nIHZhbHVlcyBpbiBlbWJlZGRpbmcgcmVzcG9uc2UnKTtcbiAgICAgIHJldHVybiBlLnZhbHVlcztcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzNDI5ID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKTtcblxuICAgIGlmIChpczQyOSAmJiBhdHRlbXB0IDwgbWF4QXR0ZW1wdHMpIHtcbiAgICAgIGNvbnN0IHJldHJ5RGVsYXkgPSBlcnJvci5yZXRyeUFmdGVyIHx8IEdST1VQX1dBSVRfTVM7XG4gICAgICBjb25zb2xlLmxvZyhgUmF0ZSBsaW1pdGVkLCB3YWl0aW5nICR7cmV0cnlEZWxheSAvIDEwMDB9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9KWApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSA9IEJBVENIX1NJWkUoKTtcbiAgY29uc3QgcGFyYWxsZWxDYWxscyA9IFBBUkFMTEVMX0NBTExTKCk7XG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBbXTtcblxuICBjb25zdCBiYXRjaGVzID0gW107XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBiYXRjaFNpemUpIHtcbiAgICBiYXRjaGVzLnB1c2goY2h1bmtzLnNsaWNlKGksIGkgKyBiYXRjaFNpemUpKTtcbiAgfVxuXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG4gICAgY29uc3QgY2h1bmtzQ292ZXJlZCA9IE1hdGgubWluKChpICsgcGFyYWxsZWxDYWxscykgKiBiYXRjaFNpemUsIGNodW5rcy5sZW5ndGgpO1xuXG4gICAgY29uc29sZS5sb2coYCAgRW1iZWRkaW5nIGdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCAke3BhcmFsbGVsQmF0Y2hlcy5sZW5ndGh9IGJhdGNoIGNhbGwocykgaW4gcGFyYWxsZWwgKGNodW5rcyAke2kgKiBiYXRjaFNpemUgKyAxfVx1MjAxMyR7Y2h1bmtzQ292ZXJlZH0pLi4uYCk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgcGFyYWxsZWxCYXRjaGVzLm1hcChiYXRjaCA9PiBlbWJlZEJhdGNoKGJhdGNoLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgY29uc3QgZmFpbGVkQmF0Y2hlcyA9IFtdO1xuICAgIHJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgY29uc3QgYmF0Y2ggPSBwYXJhbGxlbEJhdGNoZXNbYmF0Y2hJZHhdO1xuICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgIGNvbnN0IHZlY3RvcnMgPSByZXN1bHQudmFsdWU7XG4gICAgICAgIGJhdGNoLmZvckVhY2goKGNodW5rLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgIC8vIEZJWCAyOiBjb3JyZWN0IGZhbGxiYWNrIGNodW5rIElEIFx1MjAxNCAoaSArIGJhdGNoSWR4KSBpcyB0aGUgYWJzb2x1dGUgYmF0Y2ggaW5kZXhcbiAgICAgICAgICBjb25zdCBhYnNvbHV0ZUNodW5rSWR4ID0gKGkgKyBiYXRjaElkeCkgKiBiYXRjaFNpemUgKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHthYnNvbHV0ZUNodW5rSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgICBCYXRjaCAke2kgKyBiYXRjaElkeH0gZmFpbGVkLCB3aWxsIHJldHJ5IGluZGl2aWR1YWxseTpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgZmFpbGVkQmF0Y2hlcy5wdXNoKHsgYmF0Y2gsIGJhdGNoSWR4OiBpICsgYmF0Y2hJZHggfSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAob25Qcm9ncmVzcykge1xuICAgICAgb25Qcm9ncmVzcyh7IGN1cnJlbnRfYmF0Y2g6IGdyb3VwTnVtLCB0b3RhbF9iYXRjaGVzOiB0b3RhbEdyb3VwcyB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBpc0xhc3RHcm91cCA9IGkgKyBwYXJhbGxlbENhbGxzID49IGJhdGNoZXMubGVuZ3RoO1xuICAgIGlmICghaXNMYXN0R3JvdXAgfHwgZmFpbGVkQmF0Y2hlcy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgbmV4dCBncm91cC4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIEdST1VQX1dBSVRfTVMpKTtcbiAgICB9XG5cbiAgICAvLyBGSVggMzogd2FpdCBiZWZvcmUgcmV0cnlpbmcgaW5kaXZpZHVhbCBjaHVua3MgdG8gYXZvaWQgaW1tZWRpYXRlIDQyOVxuICAgIGZvciAoY29uc3QgeyBiYXRjaCwgYmF0Y2hJZHggfSBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7UkVUUllfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgcmV0cnlpbmcgZmFpbGVkIGJhdGNoICR7YmF0Y2hJZHh9Li4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgUkVUUllfV0FJVF9NUykpO1xuICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBiYXRjaCkge1xuICAgICAgICB0cnkge1xuICAgICAgICAgIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtjaHVuay50ZXh0XSwgdGFza1R5cGUpO1xuICAgICAgICAgIGVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua19yZXRyeV8ke2JhdGNoSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbMF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfSk7XG4gICAgICAgICAgY29uc29sZS5sb2coYCAgXHUyNzA1IFJldHJ5IHN1Y2NlZWRlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYCAgXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbdGV4dF0sICdSRVRSSUVWQUxfRE9DVU1FTlQnKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSYXRlTGltaXRTdGF0ZSgpIHtcbiAgcmV0dXJuIHtcbiAgICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgICBwYXJhbGxlbENhbGxzOiBQQVJBTExFTF9DQUxMUygpLFxuICAgIG1heENodW5rc1BlckNhbGw6IEJBVENIX1NJWkUoKSxcbiAgICBvdXRwdXREaW1lbnNpb25zOiBPVVRQVVRfRElNRU5TSU9OUygpXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldFJhdGVMaW1pdFN0YXRlIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBDaGVjayBHZW1pbmkgKHZpYSBBUEkga2V5IHByZXNlbmNlKVxuICBoZWFsdGhTdGF0dXMuc2VydmljZXMuZ2VtaW5pID0ge1xuICAgIHN0YXR1czogcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkgPyAnY29uZmlndXJlZCcgOiAnbm90X2NvbmZpZ3VyZWQnXG4gIH07XG5cbiAgLy8gR2V0IHJhdGUgbGltaXQgc3RhdGVcbiAgaGVhbHRoU3RhdHVzLnJhdGVMaW1pdCA9IGdldFJhdGVMaW1pdFN0YXRlKCk7XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcblxuY29uc3QgQ0hBUlNfUEVSX1RPS0VOID0gNDtcbmNvbnN0IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlMgPSAxMDAwO1xuY29uc3QgREVGQVVMVF9PVkVSTEFQX1RPS0VOUyA9IDIwMDtcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW5UZXh0KHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuICcnO1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC9cXGYvZywgJ1xcbicpXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJylcbiAgICAucmVwbGFjZSgvWyBcXHRdezIsfS9nLCAnICcpXG4gICAgLnRyaW0oKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDaHVua0lkKHRleHQsIGZpbGVuYW1lKSB7XG4gIHJldHVybiBjcmVhdGVIYXNoKCdtZDUnKVxuICAgIC51cGRhdGUoYCR7ZmlsZW5hbWV9Ojoke3RleHR9YClcbiAgICAuZGlnZXN0KCdoZXgnKVxuICAgIC5zbGljZSgwLCAxNik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1RleHQodGV4dCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IGNodW5rU2l6ZVRva2VucyA9IG9wdGlvbnMuY2h1bmtTaXplVG9rZW5zIHx8IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUb2tlbnMgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgfHwgREVGQVVMVF9PVkVSTEFQX1RPS0VOUztcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgY29uc3QgY2h1bmtTaXplQ2hhcnMgPSBjaHVua1NpemVUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG5cbiAgY29uc3QgY2h1bmtzID0gW107XG4gIGxldCBzdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcblxuICB3aGlsZSAoc3RhcnQgPCB0ZXh0Lmxlbmd0aCkge1xuICAgIGxldCBlbmQgPSBzdGFydCArIGNodW5rU2l6ZUNoYXJzO1xuXG4gICAgaWYgKGVuZCA8IHRleHQubGVuZ3RoKSB7XG4gICAgICBjb25zdCBicmVha1BvaW50cyA9IFsnLiAnLCAnLlxcbicsICchICcsICc/ICcsICdcXG5cXG4nLCAnXFxuJywgJyAnXTtcbiAgICAgIGNvbnN0IHNlYXJjaFN0YXJ0ID0gZW5kIC0gTWF0aC5mbG9vcihjaHVua1NpemVDaGFycyAqIDAuMik7XG5cbiAgICAgIGZvciAoY29uc3QgYnJlYWtwb2ludCBvZiBicmVha1BvaW50cykge1xuICAgICAgICBjb25zdCBpZHggPSB0ZXh0Lmxhc3RJbmRleE9mKGJyZWFrcG9pbnQsIGVuZCk7XG4gICAgICAgIGlmIChpZHggPiBzZWFyY2hTdGFydCAmJiBpZHggPiBzdGFydCkge1xuICAgICAgICAgIGVuZCA9IGlkeCArIGJyZWFrcG9pbnQubGVuZ3RoO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgZW5kID0gTWF0aC5taW4oZW5kLCB0ZXh0Lmxlbmd0aCk7XG4gICAgY29uc3QgY2h1bmtDb250ZW50ID0gdGV4dC5zbGljZShzdGFydCwgZW5kKS50cmltKCk7XG5cbiAgICBpZiAoY2h1bmtDb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY2h1bmtDb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2VucyhjaHVua0NvbnRlbnQpLFxuICAgICAgICBjaGFyU3RhcnQ6IHN0YXJ0LFxuICAgICAgICBjaGFyRW5kOiBlbmQsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgbmV4dFN0YXJ0ID0gZW5kIC0gb3ZlcmxhcENoYXJzO1xuICAgIHN0YXJ0ID0gbmV4dFN0YXJ0ID4gc3RhcnQgPyBuZXh0U3RhcnQgOiBlbmQ7XG5cbiAgICBpZiAoY2h1bmtJbmRleCA+IDEwMDAwKSB7XG4gICAgICBjb25zb2xlLndhcm4oJ0NodW5rIGxpbWl0IHJlYWNoZWQsIHN0b3BwaW5nJyk7XG4gICAgICBicmVhaztcbiAgICB9XG4gIH1cblxuICByZXR1cm4gY2h1bmtzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtQREZDb250ZW50KHBkZkRhdGEsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB7IGZpbGVuYW1lLCBkb2N1bWVudElkLCBwYWdlTnVtYmVyLCB0ZXh0LCB0b3RhbFBhZ2VzIH0gPSBwZGZEYXRhO1xuXG4gIGlmICghdGV4dCB8fCB0ZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICAke2ZpbGVuYW1lfSBwYWdlICR7cGFnZU51bWJlcn06IGV4dHJhY3RlZCB0ZXh0IHRvbyBzaG9ydCBcdTIwMTQgbWF5IGJlIGEgc2Nhbm5lZCBwYWdlLCBza2lwcGluZ2ApO1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGNsZWFuZWRUZXh0ID0gY2xlYW5UZXh0KHRleHQpO1xuICBjb25zdCB0ZXh0Q2h1bmtzID0gY2h1bmtUZXh0KGNsZWFuZWRUZXh0LCBvcHRpb25zKTtcbiAgY29uc3QgdG90YWxDaHVua3MgPSB0ZXh0Q2h1bmtzLmxlbmd0aDtcblxuICAvLyBGSVggNDogdXNlIHNvdXJjZVR5cGUgZnJvbSBvcHRpb25zLCBmYWxsIGJhY2sgdG8gJ3BkZidcbiAgY29uc3Qgc291cmNlVHlwZSA9IG9wdGlvbnMuc291cmNlVHlwZSB8fCAncGRmJztcblxuICByZXR1cm4gdGV4dENodW5rcy5tYXAoY2h1bmsgPT4ge1xuICAgIGNvbnN0IGNodW5rSWQgPSBnZW5lcmF0ZUNodW5rSWQoY2h1bmsudGV4dCwgZmlsZW5hbWUpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY2h1bmtJZCxcbiAgICAgICAgY2h1bmtfaW5kZXg6IGNodW5rLmNodW5rSW5kZXgsXG4gICAgICAgIHRvdGFsX2NodW5rczogdG90YWxDaHVua3MsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiBwYWdlTnVtYmVyIHx8IDEsXG4gICAgICAgIHRvdGFsX3BhZ2VzOiB0b3RhbFBhZ2VzIHx8IG51bGwsXG4gICAgICAgIHNlY3Rpb25fdGl0bGU6IGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICAgIHNvdXJjZV90eXBlOiBzb3VyY2VUeXBlLCAgICAgICAgICAgIC8vIEZJWCA0XG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9O1xuICB9KTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIGNvbnN0IGxpbmVzID0gdGV4dC5zcGxpdCgnXFxuJykuZmlsdGVyKGwgPT4gbC50cmltKCkpO1xuICBpZiAobGluZXMubGVuZ3RoID4gMCkge1xuICAgIGNvbnN0IGZpcnN0TGluZSA9IGxpbmVzWzBdLnRyaW0oKTtcbiAgICBpZiAoZmlyc3RMaW5lLmxlbmd0aCA8IDEwMCAmJiAhZmlyc3RMaW5lLmVuZHNXaXRoKCcuJykpIHtcbiAgICAgIHJldHVybiBmaXJzdExpbmUuc2xpY2UoMCwgNTApO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2ltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHtcbiAgZ2V0R2xvYmFsQ29sbGVjdGlvbixcbiAgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sXG4gIGxpc3REb2N1bWVudHMsXG4gIGFkZFZlY3RvcnNcbn0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcblxuY29uc3QgREVGQVVMVF9USU1FT1VUX01JTlVURVMgPSA2MDtcbmNvbnN0IHNlc3Npb25zID0gbmV3IE1hcCgpO1xuY29uc3QgTUFYX1BERlNfUEVSX1NFU1NJT04gPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTikgfHwgMztcbmNvbnN0IE1BWF9VUExPQURfU0laRV9NQiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQikgfHwgNTtcblxuLy8gVHJhY2sgd2hpY2ggc2Vzc2lvbnMgaGF2ZSBiZWVuIHNlZWRlZCBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkOiBzZXNzaW9uSWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuXG4gIHNlc3Npb25zLnNldChzZXNzaW9uSWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykgcmV0dXJuIGV4aXN0aW5nO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6IGNvcHkgYWxsIGdsb2JhbCBjb2xsZWN0aW9uIHZlY3RvcnMgaW50byB0aGUgc2Vzc2lvbiBjb2xsZWN0aW9uLlxuICogVGhpcyBvbmx5IHJ1bnMgT05DRSBwZXIgc2Vzc2lvbklkIFx1MjAxNCBza2lwcGVkIG9uIHN1YnNlcXVlbnQgY2FsbHMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBjb25zb2xlLmxvZyhcIkluIHRoZSBpbml0IHNlc3Npb253aXRoIGdvYmFsZG9jcyBmdW5jdGlvblwiKVxuICBpZiAoc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCkpIHJldHVybjsgLy8gYWxyZWFkeSBkb25lXG5cbiAgdHJ5IHtcbiAgICBjb25zb2xlLmxvZyhgXHVEODNDXHVERjMxIFNlZWRpbmcgc2Vzc2lvbiAke3Nlc3Npb25JZH0gZnJvbSBnbG9iYWwgY29sbGVjdGlvbi4uLmApO1xuXG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCBzZXNzaW9uQ29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG5cbiAgICAvLyBGZXRjaCBBTEwgdmVjdG9ycyBmcm9tIGdsb2JhbCAoZG9jdW1lbnRzICsgZW1iZWRkaW5ncyArIG1ldGFkYXRhcyArIGlkcylcbiAgICBjb25zdCBhbGxHbG9iYWwgPSBhd2FpdCBnbG9iYWxDb2xsZWN0aW9uLmdldCh7XG4gICAgICBpbmNsdWRlOiBbJ2VtYmVkZGluZ3MnLCAnZG9jdW1lbnRzJywgJ21ldGFkYXRhcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIWFsbEdsb2JhbC5pZHMgfHwgYWxsR2xvYmFsLmlkcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcdTI2QTBcdUZFMEYgIEdsb2JhbCBjb2xsZWN0aW9uIGlzIGVtcHR5IFx1MjAxNCBub3RoaW5nIHRvIHNlZWQuJyk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5hZGQoc2Vzc2lvbklkKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBDaGVjayB3aGljaCBpZHMgYWxyZWFkeSBleGlzdCBpbiBzZXNzaW9uIChhdm9pZCBkdXBsaWNhdGUgaW5zZXJ0cyBvbiByZWNvbm5lY3QpXG4gICAgY29uc3QgZXhpc3RpbmcgPSBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5nZXQoeyBpZHM6IGFsbEdsb2JhbC5pZHMgfSk7XG4gICAgY29uc3QgZXhpc3RpbmdJZHMgPSBuZXcgU2V0KGV4aXN0aW5nLmlkcyB8fCBbXSk7XG4gICAgY29uc3QgbmV3SWRzID0gYWxsR2xvYmFsLmlkcy5maWx0ZXIoaWQgPT4gIWV4aXN0aW5nSWRzLmhhcyhpZCkpO1xuXG4gICAgaWYgKG5ld0lkcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiAke3Nlc3Npb25JZH0gYWxyZWFkeSBoYXMgYWxsIGdsb2JhbCB2ZWN0b3JzLmApO1xuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3QgbmV3SW5kaWNlcyA9IGFsbEdsb2JhbC5pZHNcbiAgICAgIC5tYXAoKGlkLCBpKSA9PiBpKVxuICAgICAgLmZpbHRlcihpID0+IG5ld0lkcy5pbmNsdWRlcyhhbGxHbG9iYWwuaWRzW2ldKSk7XG5cbiAgICBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5hZGQoe1xuICAgICAgaWRzOiBuZXdJbmRpY2VzLm1hcChpID0+IGFsbEdsb2JhbC5pZHNbaV0pLFxuICAgICAgZW1iZWRkaW5nczogbmV3SW5kaWNlcy5tYXAoaSA9PiBhbGxHbG9iYWwuZW1iZWRkaW5nc1tpXSksXG4gICAgICBkb2N1bWVudHM6IG5ld0luZGljZXMubWFwKGkgPT4gYWxsR2xvYmFsLmRvY3VtZW50c1tpXSksXG4gICAgICBtZXRhZGF0YXM6IG5ld0luZGljZXMubWFwKGkgPT4gKHtcbiAgICAgICAgLi4uYWxsR2xvYmFsLm1ldGFkYXRhc1tpXSxcbiAgICAgICAgc291cmNlX3R5cGU6ICdnbG9iYWwnICAvLyBwcmVzZXJ2ZSBzbyBVSSBjYW4gc3RpbGwgc2hvdyBTZWVkIGJhZGdlXG4gICAgICB9KSlcbiAgICB9KTtcblxuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2VlZGVkICR7bmV3SWRzLmxlbmd0aH0gdmVjdG9ycyBpbnRvIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG5cbiAgICAvLyBSZWdpc3RlciBnbG9iYWwgZG9jcyBpbiBzZXNzaW9uIGRvY3VtZW50IGxpc3QgZm9yIGxpc3RpbmcgaW4gVUlcbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChzZXNzaW9uKSB7XG4gICAgICBjb25zdCBkb2NzTWFwID0gbmV3IE1hcCgpO1xuICAgICAgYWxsR2xvYmFsLm1ldGFkYXRhcy5mb3JFYWNoKG1ldGEgPT4ge1xuICAgICAgICBpZiAoIWRvY3NNYXAuaGFzKG1ldGEuZG9jdW1lbnRfaWQpKSB7XG4gICAgICAgICAgZG9jc01hcC5zZXQobWV0YS5kb2N1bWVudF9pZCwge1xuICAgICAgICAgICAgaWQ6IG1ldGEuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBtZXRhLnRvdGFsX3BhZ2VzIHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiAwLFxuICAgICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGRvY3NNYXAuZ2V0KG1ldGEuZG9jdW1lbnRfaWQpLmNodW5rQ291bnQrKztcbiAgICAgIH0pO1xuXG4gICAgICBmb3IgKGNvbnN0IGRvYyBvZiBkb2NzTWFwLnZhbHVlcygpKSB7XG4gICAgICAgIGlmICghc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuaWQgPT09IGRvYy5pZCkpIHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKGRvYyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gTm9uLWZhdGFsIFx1MjAxNCBzZXNzaW9uIGNhbiBzdGlsbCB3b3JrIHdpdGhvdXQgZ2xvYmFsIGRvY3NcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQsXG4gICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJ1xuICB9KTtcbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcblxuICAvLyBDb3VudCBvbmx5IHNlc3Npb24tdXBsb2FkZWQgZG9jcyAobm90IGdsb2JhbCBzZWVkcylcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25Eb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIFtdO1xuICByZXR1cm4gc2Vzc2lvbi5kb2N1bWVudHM7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb25Eb2NzID0gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIC8vIFNwbGl0IGZvciBVSSBcdTIwMTQgZ2xvYmFsIHNlZWRlZCBkb2NzIHZzIHVzZXItdXBsb2FkZWQgZG9jc1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb25Eb2NzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJyksXG4gICAgZ2xvYmFsRG9jdW1lbnRzOiBzZXNzaW9uRG9jcy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdnbG9iYWwnKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtpbXBvcnQgeyBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDU7XG5jb25zdCBSRUZVU0FMX1RIUkVTSE9MRCA9IHBhcnNlRmxvYXQocHJvY2Vzcy5lbnYuUkVGVVNBTF9USFJFU0hPTEQpIHx8IDAuMDU7XG5cbi8vIENhY2hlIHJlc29sdmVkIGNvbGxlY3Rpb24gb2JqZWN0c1xuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGNvbGxlY3Rpb24pIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IFRPUF9LKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAgfTtcbiAgY29uc3Qgc2NvcmVzID0gcmVzdWx0cy5zbGljZSgwLCB0b3BLKS5tYXAociA9PiBNYXRoLm1heCgwLCByLnNjb3JlKSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcbiAgcmV0dXJuIHtcbiAgICBjb25maWRlbmNlOiBNYXRoLnJvdW5kKGF2Z1Njb3JlICogMTAwKSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKVxuICB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdG9wSyA9IG9wdGlvbnMudG9wSyB8fCBUT1BfSztcblxuICB0cnkge1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgc2Vzc2lvbkNvbGxlY3Rpb25dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBzZXNzaW9uSWQgPyBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSA6IFByb21pc2UucmVzb2x2ZShudWxsKVxuICAgIF0pO1xuXG4gICAgaWYgKCFzZXNzaW9uQ29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIHNlc3Npb24gY29sbGVjdGlvbiBmb3VuZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcgfTtcbiAgICB9XG5cbiAgICAvLyBTaW5nbGUgcXVlcnkgXHUyMDE0IHNlc3Npb24gY29sbGVjdGlvbiBoYXMgZ2xvYmFsIHZlY3RvcnMgYWxyZWFkeSBjb3BpZWQgaW5cbiAgICBjb25zdCByYXdSZXN1bHRzID0gYXdhaXQgcXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgIC5jYXRjaCgoKSA9PiBbXSk7XG5cbiAgICAvLyBQcmVzZXJ2ZSBzb3VyY2VfdHlwZSBmcm9tIG1ldGFkYXRhIHNvIFVJIGJhZGdlIChTZWVkL1Nlc3Npb24pIHN0aWxsIHdvcmtzXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBSYXcgc2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9OyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IGZzIGZyb20gJ2ZzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5pbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJzs7XG5pbXBvcnQgeyBzYW5pdGl6ZUZpbGVuYW1lLCB2YWxpZGF0ZVBERkZpbGUsIHZhbGlkYXRlRmlsZVNpemUgfSBmcm9tICcuLi91dGlscy9zYW5pdGl6ZS5qcyc7XG5pbXBvcnQge1xuICBDb3JydXB0ZWRQREZFcnJvcixcbiAgSW52YWxpZEZpbGVUeXBlRXJyb3IsXG4gIEZpbGVUb29MYXJnZUVycm9yLFxuICBUb29NYW55UERGc0Vycm9yLFxuICBEdXBsaWNhdGVGaWxlRXJyb3Jcbn0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcbmltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNodW5rVGV4dCwgY2xlYW5UZXh0IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG5pbXBvcnQgeyBnZW5lcmF0ZUVtYmVkZGluZ3MgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7XG4gIGdldE9yQ3JlYXRlU2Vzc2lvbixcbiAgY2FuQWNjZXB0VXBsb2FkLFxuICBhZGREb2N1bWVudFRvU2Vzc2lvbixcbiAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbixcbiAgZ2V0QWxsRG9jdW1lbnRzXG59IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlIH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG4vLyBBbGwgdXBsb2FkZWQgUERGcyBnbyB0byAvdG1wIFx1MjAxNCBuZXZlciB0byBzZWVkX2RvY3VtZW50c1xuY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG5pZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbn1cblxuLy8gU2VlZCBQREZzIGxpdmUgaGVyZSBcdTIwMTQgb25seSB1c2VkIGZvciBzZXJ2aW5nIHRoZSBmaWxlIChWaWV3IFBERiksIG5ldmVyIHdyaXR0ZW4gdG9cbmNvbnN0IHNlZWREaXIgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi4vLi4vc2VlZF9kb2N1bWVudHMnKTtcblxuY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gIGRlc3RpbmF0aW9uOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgdXBsb2FkRGlyKSxcbiAgZmlsZW5hbWU6IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCBgJHt1dWlkdjQoKX0ke3BhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSl9YClcbn0pO1xuXG5jb25zdCB1cGxvYWQgPSBtdWx0ZXIoe1xuICBzdG9yYWdlLFxuICBsaW1pdHM6IHsgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjQgfSxcbiAgZmlsZUZpbHRlcjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBpZiAoZmlsZS5taW1ldHlwZSA9PT0gJ2FwcGxpY2F0aW9uL3BkZicgJiYgcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKS50b0xvd2VyQ2FzZSgpID09PSAnLnBkZicpIHtcbiAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjYihuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKSk7XG4gICAgfVxuICB9XG59KTtcblxuLy8gUkZDIDU5ODcgXHUyMDE0IHNhZmUgQ29udGVudC1EaXNwb3NpdGlvbiBmb3IgZmlsZW5hbWVzIHdpdGggc3BlY2lhbCBjaGFycywgdW5pY29kZSwgZXRjLlxuZnVuY3Rpb24gY29udGVudERpc3Bvc2l0aW9uKGRpc3BsYXlOYW1lKSB7XG4gIGNvbnN0IGVuY29kZWQgPSBlbmNvZGVVUklDb21wb25lbnQoZGlzcGxheU5hbWUpXG4gICAgLnJlcGxhY2UoLycvZywgJyUyNycpXG4gICAgLnJlcGxhY2UoL1xcKC9nLCAnJTI4JylcbiAgICAucmVwbGFjZSgvXFwpL2csICclMjknKTtcbiAgcmV0dXJuIGBpbmxpbmU7IGZpbGVuYW1lPVwiZG9jdW1lbnQucGRmXCI7IGZpbGVuYW1lKj1VVEYtOCcnJHtlbmNvZGVkfWA7XG59XG5cbmFzeW5jIGZ1bmN0aW9uIHBhcnNlUERGV2l0aEJvdW5kYXJ5TWFwKGZpbGVQYXRoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgYnVmZmVyID0gZnMucmVhZEZpbGVTeW5jKGZpbGVQYXRoKTtcblxuICAgIGNvbnN0IHBhZ2VzID0gW107XG4gICAgYXdhaXQgcGRmKGJ1ZmZlciwge1xuICAgICAgcGFnZXJlbmRlcjogKHBhZ2VEYXRhKSA9PiB7XG4gICAgICAgIHJldHVybiBwYWdlRGF0YS5nZXRUZXh0Q29udGVudCgpLnRoZW4odGMgPT4ge1xuICAgICAgICAgIGNvbnN0IHBhZ2VUZXh0ID0gdGMuaXRlbXMubWFwKGkgPT4gaS5zdHIpLmpvaW4oJyAnKTtcbiAgICAgICAgICBwYWdlcy5wdXNoKHBhZ2VUZXh0KTtcbiAgICAgICAgICByZXR1cm4gcGFnZVRleHQ7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKHBhZ2VzLmxlbmd0aCA9PT0gMCB8fCBwYWdlcy5ldmVyeShwID0+ICFwLnRyaW0oKSkpIHtcbiAgICAgIGNvbnN0IGZ1bGwgPSBhd2FpdCBwZGYoYnVmZmVyKTtcbiAgICAgIHBhZ2VzLnB1c2goZnVsbC50ZXh0KTtcbiAgICB9XG5cbiAgICBjb25zdCB0b3RhbFBhZ2VzID0gcGFnZXMubGVuZ3RoO1xuICAgIGNvbnN0IGNsZWFuZWRQYWdlcyA9IHBhZ2VzLm1hcChwID0+IGNsZWFuVGV4dChwKSk7XG4gICAgY29uc3QgcGFnZU1hcCA9IFtdO1xuICAgIGxldCBjaGFyUG9zID0gMDtcblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2xlYW5lZFBhZ2VzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBwYWdlTWFwLnB1c2goeyBwYWdlOiBpICsgMSwgc3RhcnQ6IGNoYXJQb3MsIGVuZDogY2hhclBvcyArIGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggfSk7XG4gICAgICBjaGFyUG9zICs9IGNsZWFuZWRQYWdlc1tpXS5sZW5ndGggKyAxO1xuICAgIH1cblxuICAgIGNvbnN0IGZ1bGxUZXh0ID0gY2xlYW5lZFBhZ2VzLmpvaW4oJ1xcbicpO1xuICAgIHJldHVybiB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignUERGIHBhcnNpbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IG5ldyBDb3JydXB0ZWRQREZFcnJvcigpO1xuICB9XG59XG5cbmZ1bmN0aW9uIGdldFBhZ2VOdW1iZXIoY2hhclN0YXJ0LCBwYWdlTWFwKSB7XG4gIGZvciAoY29uc3QgZW50cnkgb2YgcGFnZU1hcCkge1xuICAgIGlmIChjaGFyU3RhcnQgPj0gZW50cnkuc3RhcnQgJiYgY2hhclN0YXJ0IDwgZW50cnkuZW5kKSByZXR1cm4gZW50cnkucGFnZTtcbiAgfVxuICByZXR1cm4gcGFnZU1hcFtwYWdlTWFwLmxlbmd0aCAtIDFdPy5wYWdlIHx8IDE7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVVcGxvYWQocmVxLCByZXMpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBmaWxlID0gcmVxLmZpbGU7XG4gICAgaWYgKCFmaWxlKSB0aHJvdyBuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKTtcblxuICAgIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEuYm9keS5zZXNzaW9uSWQgfHwgdXVpZHY0KCk7XG4gICAgY29uc3Qgc2Vzc2lvbiA9IGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGNvbnN0IG1heFBERnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTiB8fCAnMycpO1xuICAgIGNvbnN0IGNsZWFuRmlsZW5hbWUgPSBzYW5pdGl6ZUZpbGVuYW1lKGZpbGUub3JpZ2luYWxuYW1lKTtcblxuICAgIC8vIENvdW50IG9ubHkgdXNlci11cGxvYWRlZCBkb2NzIChub3QgZ2xvYmFsIHNlZWRzKSB0b3dhcmQgdGhlIGxpbWl0XG4gICAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICAgIGlmICh1cGxvYWRlZENvdW50ID49IG1heFBERnMpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBUb29NYW55UERGc0Vycm9yKG1heFBERnMpO1xuICAgIH1cblxuICAgIGlmIChzZXNzaW9uLmRvY3VtZW50cy5zb21lKGQgPT4gZC5maWxlbmFtZSA9PT0gY2xlYW5GaWxlbmFtZSkpIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHRocm93IG5ldyBEdXBsaWNhdGVGaWxlRXJyb3IoY2xlYW5GaWxlbmFtZSk7XG4gICAgfVxuXG4gICAgY29uc3QgeyBmdWxsVGV4dCwgcGFnZU1hcCwgdG90YWxQYWdlcyB9ID0gYXdhaXQgcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZS5wYXRoKTtcblxuICAgIGlmICghZnVsbFRleHQgfHwgZnVsbFRleHQudHJpbSgpLmxlbmd0aCA8IDUwKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKGZpbGUucGF0aCk7XG4gICAgICByZXR1cm4gcmVzLnN0YXR1cyg0MjIpLmpzb24oe1xuICAgICAgICBlcnJvcjogJ05vIGV4dHJhY3RhYmxlIHRleHQgZm91bmQgXHUyMDE0IFBERiBtYXkgYmUgc2Nhbm5lZCBvciBpbWFnZS1vbmx5JyxcbiAgICAgICAgY29kZTogJ0VNUFRZX1BERidcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGRvY3VtZW50SWQgPSBwYXRoLnBhcnNlKGZpbGUuZmlsZW5hbWUpLm5hbWU7XG5cbiAgICBjb25zdCByYXdDaHVua3MgPSBjaHVua1RleHQoZnVsbFRleHQsIHtcbiAgICAgIGNodW5rU2l6ZVRva2VuczogMTAwMCxcbiAgICAgIG92ZXJsYXBUb2tlbnM6IDIwMFxuICAgIH0pO1xuXG4gICAgaWYgKHJhd0NodW5rcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQyMikuanNvbih7IGVycm9yOiAnTm8gY29udGVudCBjb3VsZCBiZSBleHRyYWN0ZWQgZnJvbSBQREYnLCBjb2RlOiAnRU1QVFlfUERGJyB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBjaHVua3MgPSByYXdDaHVua3MubWFwKChjaHVuaywgaWR4KSA9PiAoe1xuICAgICAgdGV4dDogY2h1bmsudGV4dCxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIGRvY3VtZW50X2lkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgY2h1bmtfaWQ6IGNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShgJHtjbGVhbkZpbGVuYW1lfTo6JHtjaHVuay50ZXh0fWApLmRpZ2VzdCgnaGV4Jykuc2xpY2UoMCwgMTYpLFxuICAgICAgICBjaHVua19pbmRleDogaWR4LFxuICAgICAgICB0b3RhbF9jaHVua3M6IHJhd0NodW5rcy5sZW5ndGgsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiBnZXRQYWdlTnVtYmVyKGNodW5rLmNoYXJTdGFydCwgcGFnZU1hcCksXG4gICAgICAgIHRvdGFsX3BhZ2VzOiB0b3RhbFBhZ2VzLFxuICAgICAgICBzb3VyY2VfdHlwZTogJ3Nlc3Npb25fdXBsb2FkJywgIC8vIGFsd2F5cyBzZXNzaW9uX3VwbG9hZCBmb3IgdXNlciB1cGxvYWRzXG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9KSk7XG5cbiAgICAvLyBVcGxvYWQgYWx3YXlzIHRhcmdldHMgc2Vzc2lvbiBjb2xsZWN0aW9uIFx1MjAxNCBuZXZlciBnbG9iYWxcbiAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcblxuICAgIGNvbnN0IGVtYmVkZGluZ3MgPSBhd2FpdCBnZW5lcmF0ZUVtYmVkZGluZ3MoXG4gICAgICBjaHVua3MsXG4gICAgICAnUkVUUklFVkFMX0RPQ1VNRU5UJyxcbiAgICAgICh7IGN1cnJlbnRfYmF0Y2gsIHRvdGFsX2JhdGNoZXMgfSkgPT4ge1xuICAgICAgICBpZiAocmVxLmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MpIHtcbiAgICAgICAgICByZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcy5lbWl0KGBwcm9ncmVzc18ke3Nlc3Npb25JZH1gLCB7XG4gICAgICAgICAgICBkb2N1bWVudElkLFxuICAgICAgICAgICAgY3VycmVudF9iYXRjaCxcbiAgICAgICAgICAgIHRvdGFsX2JhdGNoZXMsXG4gICAgICAgICAgICBzdGFnZTogJ2VtYmVkZGluZydcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICk7XG5cbiAgICBpZiAoZW1iZWRkaW5ncy5sZW5ndGggPT09IDApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDUwMykuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGdlbmVyYXRlIGVtYmVkZGluZ3MnLCBjb2RlOiAnRU1CRURESU5HX0ZBSUxFRCcgfSk7XG4gICAgfVxuXG4gICAgYXdhaXQgYWRkVmVjdG9ycyhcbiAgICAgIGNvbGxlY3Rpb24sXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+ICh7IHRleHQ6IGUudGV4dCwgbWV0YWRhdGE6IGUubWV0YWRhdGEgfSkpLFxuICAgICAgZW1iZWRkaW5ncy5tYXAoZSA9PiBlLmVtYmVkZGluZyksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuaWQpXG4gICAgKTtcblxuICAgIC8vIEludmFsaWRhdGUgcmV0cmlldmFsIGNhY2hlIHNvIG5leHQgcXVlcnkgcGlja3MgdXAgbmV3IHZlY3RvcnNcbiAgICBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpO1xuXG4gICAgYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCB7XG4gICAgICBpZDogZG9jdW1lbnRJZCxcbiAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcyxcbiAgICAgIGNodW5rQ291bnQ6IGVtYmVkZGluZ3MubGVuZ3RoXG4gICAgfSk7XG5cbiAgICAvLyBGaWxlIHN0YXlzIGluIC90bXAgXHUyMDE0IG5vdCBkZWxldGVkIGFmdGVyIHVwbG9hZFxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBkb2N1bWVudDoge1xuICAgICAgICBpZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICAgIGZpbGVTaXplOiBmaWxlLnNpemUsXG4gICAgICAgIHBhZ2VDb3VudDogdG90YWxQYWdlcyxcbiAgICAgICAgY2h1bmtDb3VudDogZW1iZWRkaW5ncy5sZW5ndGgsXG4gICAgICAgIHVwbG9hZFRpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpXG4gICAgICB9LFxuICAgICAgc2Vzc2lvbklkXG4gICAgfSk7XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBpZiAocmVxLmZpbGUgJiYgZnMuZXhpc3RzU3luYyhyZXEuZmlsZS5wYXRoKSkge1xuICAgICAgZnMudW5saW5rU3luYyhyZXEuZmlsZS5wYXRoKTtcbiAgICB9XG4gICAgY29uc29sZS5lcnJvcignVXBsb2FkIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKGVycm9yLnN0YXR1c0NvZGUgfHwgNTAwKS5qc29uKHtcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlLFxuICAgICAgY29kZTogZXJyb3IuY29kZSB8fCAnVVBMT0FEX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RG9jdW1lbnRzSGFuZGxlcihyZXEsIHJlcykge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcbiAgdHJ5IHtcbiAgICBjb25zdCBkb2N1bWVudHMgPSBhd2FpdCBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbihkb2N1bWVudHMpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0xpc3QgZG9jdW1lbnRzIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7IGVycm9yOiAnRmFpbGVkIHRvIGxpc3QgZG9jdW1lbnRzJywgY29kZTogJ0xJU1RfRVJST1InIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBkZWxldGVEb2N1bWVudChyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIHRyeSB7XG4gICAgLy8gT25seSBkZWxldGUgZnJvbSBzZXNzaW9uIGNvbGxlY3Rpb24gXHUyMDE0IG5ldmVyIHRvdWNoZXMgZ2xvYmFsIGNvbGxlY3Rpb25cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBjb25zdCBjb2xsZWN0aW9uID0gYXdhaXQgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKTtcbiAgICAgIGlmIChjb2xsZWN0aW9uKSB7XG4gICAgICAgIGNvbnN0IGNvdW50ID0gYXdhaXQgZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpO1xuICAgICAgICBpZiAoY291bnQgPiAwKSB7XG4gICAgICAgICAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbihzZXNzaW9uSWQsIGRvY3VtZW50SWQpO1xuICAgICAgICAgIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBEZWxldGUgdGhlIHVwbG9hZGVkIGZpbGUgZnJvbSAvdG1wIG9ubHkgKG5ldmVyIGZyb20gc2VlZF9kb2N1bWVudHMpXG4gICAgY29uc3QgdG1wUGF0aCA9IHBhdGguam9pbih1cGxvYWREaXIsIGAke2RvY3VtZW50SWR9LnBkZmApO1xuICAgIGlmIChmcy5leGlzdHNTeW5jKHRtcFBhdGgpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKHRtcFBhdGgpO1xuICAgICAgY29uc29sZS5sb2coYFx1RDgzRFx1REREMVx1RkUwRiAgRGVsZXRlZCB0bXAgZmlsZTogJHt0bXBQYXRofWApO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHsgc3VjY2VzczogdHJ1ZSwgZG9jdW1lbnRJZCB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdEZWxldGUgZG9jdW1lbnQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZGVsZXRlIGRvY3VtZW50JywgY29kZTogJ0RFTEVURV9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50RmlsZShyZXEsIHJlcykge1xuICBjb25zdCB7IGRvY3VtZW50SWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IGZpbGVuYW1lID0gcmVxLnF1ZXJ5LmZpbGVuYW1lO1xuXG4gIHRyeSB7XG4gICAgLy8gQ2hlY2sgL3RtcCBmaXJzdCAodXNlci11cGxvYWRlZClcbiAgICBjb25zdCB1cGxvYWRQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgYCR7ZG9jdW1lbnRJZH0ucGRmYCk7XG4gICAgaWYgKGZzLmV4aXN0c1N5bmModXBsb2FkUGF0aCkpIHtcbiAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtRGlzcG9zaXRpb24nLCBjb250ZW50RGlzcG9zaXRpb24oYCR7ZG9jdW1lbnRJZH0ucGRmYCkpO1xuICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgIH1cblxuICAgIC8vIFNlZWQgZG9jIFx1MjAxNCBzZXJ2ZSBmcm9tIHNlZWRfZG9jdW1lbnRzIChyZWFkLW9ubHksIG5ldmVyIGRlbGV0ZWQpXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKGZpbGVuYW1lKSk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIC8vIEZhbGxiYWNrOiBzY2FuIHNlZWREaXJcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICBjb25zdCBtYXRjaCA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgY29udGVudERpc3Bvc2l0aW9uKG1hdGNoKSk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG5yb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xucm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xucm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanNcIjtjb25zdCBtZW1vcnlNYXAgPSBuZXcgTWFwKCk7XG5jb25zdCBERUZBVUxUX01FTU9SWV9XSU5ET1cgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCAxMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIGlmICghbWVtb3J5TWFwLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgbWVtb3J5TWFwLnNldChzZXNzaW9uSWQsIHtcbiAgICAgIHR1cm5zOiBbXSxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKVxuICAgIH0pO1xuICB9XG4gIHJldHVybiBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgbWV0YWRhdGEgPSB7fSkge1xuICBjb25zdCBtZW1vcnkgPSBtZW1vcnlNYXAuZ2V0KHNlc3Npb25JZCkgfHwgaW5pdGlhbGl6ZU1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBtYXhUdXJucyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcblxuICBjb25zdCB0dXJuID0ge1xuICAgIGlkOiBgdHVybl8ke0RhdGUubm93KCl9XyR7TWF0aC5yYW5kb20oKS50b1N0cmluZygzNikuc3Vic3RyKDIsIDkpfWAsXG4gICAgcm9sZSxcbiAgICBjb250ZW50LFxuICAgIHRpbWVzdGFtcDogbmV3IERhdGUoKSxcbiAgICAuLi5tZXRhZGF0YVxuICB9O1xuXG4gIG1lbW9yeS50dXJucy5wdXNoKHR1cm4pO1xuXG4gIGlmIChtZW1vcnkudHVybnMubGVuZ3RoID4gbWF4VHVybnMpIHtcbiAgICBtZW1vcnkudHVybnMgPSBtZW1vcnkudHVybnMuc2xpY2UoLW1heFR1cm5zKTtcbiAgfVxuXG4gIHJldHVybiB0dXJuO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5KHNlc3Npb25JZCkge1xuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgbWF4VHVybnMgPSBudWxsKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBjb25zdCBsaW1pdCA9IG1heFR1cm5zIHx8IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1FTU9SWV9XSU5ET1dfVFVSTlMpIHx8IERFRkFVTFRfTUVNT1JZX1dJTkRPVztcbiAgcmV0dXJuIG1lbW9yeS50dXJucy5zbGljZSgtbGltaXQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0Q29udmVyc2F0aW9uQ29udGV4dChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICByZXR1cm4gdHVybnMubWFwKHQgPT4gKHtcbiAgICByb2xlOiB0LnJvbGUsXG4gICAgY29udGVudDogdC5jb250ZW50XG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIHtcbiAgY29uc3QgdHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQpO1xuICBpZiAodHVybnMubGVuZ3RoID09PSAwKSByZXR1cm4gJyc7XG5cbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+IHtcbiAgICBjb25zdCBwcmVmaXggPSB0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyOicgOiAnQXNzaXN0YW50Oic7XG4gICAgcmV0dXJuIGAke3ByZWZpeH0gJHt0LmNvbnRlbnR9YDtcbiAgfSkuam9pbignXFxuXFxuJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbGVhck1lbW9yeShzZXNzaW9uSWQpIHtcbiAgbWVtb3J5TWFwLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0TWVtb3J5U3RhdHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIHR1cm5Db3VudDogbWVtb3J5LnR1cm5zLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IG1lbW9yeS5jcmVhdGVkQXQsXG4gICAgbGFzdFR1cm5BdDogbWVtb3J5LnR1cm5zLmxlbmd0aCA+IDAgPyBtZW1vcnkudHVybnNbbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDFdLnRpbWVzdGFtcCA6IG51bGxcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwgY2l0YXRpb25zID0gW10sIGNvdmVyYWdlID0gbnVsbCwgYW5zd2VySWQgPSBudWxsKSB7XG4gIHJldHVybiBhZGRUdXJuKHNlc3Npb25JZCwgcm9sZSwgY29udGVudCwge1xuICAgIC4uLihhbnN3ZXJJZCAmJiB7IGlkOiBhbnN3ZXJJZCB9KSxcbiAgICBjaXRhdGlvbnMsXG4gICAgY292ZXJhZ2UsXG4gICAgaGFzQ2l0YXRpb25zOiBjaXRhdGlvbnMubGVuZ3RoID4gMFxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RVc2VyTWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICd1c2VyJykgcmV0dXJuIG1lbW9yeS50dXJuc1tpXTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldExhc3RBc3Npc3RhbnRNZXNzYWdlKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgZm9yIChsZXQgaSA9IG1lbW9yeS50dXJucy5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGlmIChtZW1vcnkudHVybnNbaV0ucm9sZSA9PT0gJ2Fzc2lzdGFudCcpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3Byb21wdFNlcnZpY2UuanNcIjtpbXBvcnQgeyBmb3JtYXRNZW1vcnlGb3JQcm9tcHQgfSBmcm9tICcuL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZm9ybWF0Q29udGV4dEZvclByb21wdCwgY2FsY3VsYXRlQ292ZXJhZ2UgfSBmcm9tICcuL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuXG5jb25zdCBTWVNURU1fSU5TVFJVQ1RJT04gPSBgWW91IGFyZSBhbiBBSSBLbm93bGVkZ2UgQXNzaXN0YW50IHRoYXQgYW5zd2VycyBxdWVzdGlvbnMgYmFzZWQgb24gaW5kZXhlZCBkb2N1bWVudHMgd2hlbiBhdmFpbGFibGUuXG5cblJVTEVTOlxuMS4gV2hlbiBjb250ZXh0IGlzIHByb3ZpZGVkLCBhbnN3ZXIgYmFzZWQgb24gaXQgYW5kIGNpdGUgc291cmNlcyB1c2luZyBbMV0sIFsyXSwgZXRjLlxuMi4gRm9yIGdlbmVyYWwgY29udmVyc2F0aW9uIChncmVldGluZ3MsIGNsYXJpZnlpbmcgcXVlc3Rpb25zLCBzbWFsbCB0YWxrKSwgcmVzcG9uZCBuYXR1cmFsbHkgYW5kIGhlbHBmdWxseSB3aXRob3V0IHJlcXVpcmluZyBjb250ZXh0LlxuMy4gSWYgYSBmYWN0dWFsIHF1ZXN0aW9uIGlzIGFza2VkIGJ1dCBjb250ZXh0IGlzIGluc3VmZmljaWVudCwgc2F5IHNvIGNsZWFybHkgYW5kIHN1Z2dlc3QgdXBsb2FkaW5nIHJlbGV2YW50IGRvY3VtZW50cy5cbjQuIEJlIGNvbmNpc2UgYnV0IHRob3JvdWdoLiBVc2UgYnVsbGV0IHBvaW50cyBvciBudW1iZXJlZCBsaXN0cyBmb3IgY29tcGxleCBhbnN3ZXJzLlxuNS4gTWFpbnRhaW4gY29udmVyc2F0aW9uIGNvbnRpbnVpdHkgYnV0IGRvbid0IHJlcGVhdCBpbmZvcm1hdGlvbiB1bm5lY2Vzc2FyaWx5LlxuNi4gRm9ybWF0IHJlc3BvbnNlcyBpbiBjbGVhciwgcmVhZGFibGUgbWFya2Rvd24uYDtcblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkUHJvbXB0KHsgcXVlcnksIGNvbnRleHQsIG1lbW9yeUNvbnRleHQsIGNvdmVyYWdlIH0pIHtcbiAgY29uc3QgcGFydHMgPSBbXTtcbiAgcGFydHMucHVzaChTWVNURU1fSU5TVFJVQ1RJT04pO1xuICBpZiAobWVtb3J5Q29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChtZW1vcnlDb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIFBSRVZJT1VTIENPTlZFUlNBVElPTiAtLS1cXG4nKTtcbiAgfVxuICBpZiAoY29udGV4dCkge1xuICAgIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBSRUxFVkFOVCBDT05URVhUIEZST00gS05PV0xFREdFIEJBU0UgLS0tXFxuJyk7XG4gICAgcGFydHMucHVzaChjb250ZXh0KTtcbiAgICBwYXJ0cy5wdXNoKCdcXG4tLS0gRU5EIENPTlRFWFQgLS0tXFxuJyk7XG4gIH1cbiAgcGFydHMucHVzaCgnXFxuXFxuLS0tIENVUlJFTlQgUVVFU1RJT04gLS0tXFxuJyk7XG4gIHBhcnRzLnB1c2gocXVlcnkpO1xuICBwYXJ0cy5wdXNoKCdcXG5cXG5SZW1lbWJlcjogQW5zd2VyIGJhc2VkIE9OTFkgb24gdGhlIHByb3ZpZGVkIGNvbnRleHQuIFVzZSBbMV0sIFsyXSwgZXRjLiBmb3IgY2l0YXRpb25zLiBJZiB0aGUgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5LicpO1xuICByZXR1cm4gcGFydHMuam9pbignJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFN0cmVhbWluZ1Byb21wdChxdWVyeSwgcmV0cmlldmVkUmVzdWx0cywgc2Vzc2lvbklkLCBtZW1vcnlTZXJ2aWNlKSB7XG4gIGNvbnN0IG1lbW9yeUNvbnRleHQgPSBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKTtcbiAgY29uc3QgY29udGV4dFN0cmluZyA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmV0cmlldmVkUmVzdWx0cyk7XG4gIHJldHVybiBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFN0cmluZyxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiBjYWxjdWxhdGVDb3ZlcmFnZShyZXRyaWV2ZWRSZXN1bHRzKVxuICB9KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxSZXNwb25zZSgpIHtcbiAgLy8gTm8gbG9uZ2VyIHVzZWQgXHUyMDE0IExMTSBnZW5lcmF0ZXMgaXRzIG93biBuYXR1cmFsIHJlZnVzYWxcbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTeXN0ZW1JbnN0cnVjdGlvbigpIHtcbiAgcmV0dXJuIFNZU1RFTV9JTlNUUlVDVElPTjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGJ1aWxkV2ViU2VhcmNoUHJvbXB0KHF1ZXJ5LCBncm91bmRpbmdNZXRhZGF0YSkge1xuICByZXR1cm4gYEJhc2VkIG9uIHdlYiBzZWFyY2ggcmVzdWx0cywgYW5zd2VyIHRoZSBmb2xsb3dpbmcgcXVlc3Rpb246ICR7cXVlcnl9XG5cbkd1aWRlbGluZXM6XG4tIFVzZSBpbmZvcm1hdGlvbiBmcm9tIHRoZSB3ZWIgc2VhcmNoXG4tIFByb3ZpZGUgc291cmNlcy9VUkxzIHdoZXJlIGFwcGxpY2FibGVcbi0gQmUgY29uY2lzZSBhbmQgaW5mb3JtYXRpdmVcbi0gSWYgbXVsdGlwbGUgc291cmNlcyBhZ3JlZSBvciBjb250cmFkaWN0LCBtZW50aW9uIHRoYXRgO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0R2VuZXJhdGlvbkNvbmZpZyhjdXN0b21Db25maWcgPSB7fSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgdG9wUDogMC45NSxcbiAgICB0b3BLOiA0MCxcbiAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDgsXG4gICAgLi4uY3VzdG9tQ29uZmlnXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0U291cmNlc0Zyb21SZXNwb25zZShyZXNwb25zZSkge1xuICBjb25zdCBjaXRhdGlvblBhdHRlcm4gPSAvXFxbKFxcZCspXFxdL2c7XG4gIGNvbnN0IGNpdGF0aW9ucyA9IG5ldyBTZXQoKTtcbiAgbGV0IG1hdGNoO1xuICB3aGlsZSAoKG1hdGNoID0gY2l0YXRpb25QYXR0ZXJuLmV4ZWMocmVzcG9uc2UpKSAhPT0gbnVsbCkge1xuICAgIGNpdGF0aW9ucy5hZGQocGFyc2VJbnQobWF0Y2hbMV0pKTtcbiAgfVxuICByZXR1cm4gQXJyYXkuZnJvbShjaXRhdGlvbnMpLnNvcnQoKGEsIGIpID0+IGEgLSBiKTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvZ2VtaW5pU2VydmljZS5qc1wiO2ltcG9ydCB7IEdvb2dsZUdlbmVyYXRpdmVBSSB9IGZyb20gJ0Bnb29nbGUvZ2VuZXJhdGl2ZS1haSc7XG5pbXBvcnQgeyBidWlsZFByb21wdCwgZ2V0UmVmdXNhbFJlc3BvbnNlIH0gZnJvbSAnLi9wcm9tcHRTZXJ2aWNlLmpzJztcbmltcG9ydCB7IExMTVVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xuXG5mdW5jdGlvbiBnZXRHZW5BSSgpIHtcbiAgaWYgKCFnZW5BSSkge1xuICAgIGNvbnN0IGFwaUtleSA9IHByb2Nlc3MuZW52LkdFTUlOSV9BUElfS0VZO1xuICAgIGlmICghYXBpS2V5KSB0aHJvdyBuZXcgRXJyb3IoJ0dFTUlOSV9BUElfS0VZIGlzIHVuZGVmaW5lZCcpO1xuICAgIGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShhcGlLZXkpO1xuICB9XG4gIHJldHVybiBnZW5BSTtcbn1cblxuY29uc3QgUFJJTUFSWV9NT0RFTCA9IHByb2Nlc3MuZW52LkdFTUlOSV9NT0RFTF9QUklNQVJZIHx8ICdnZW1pbmktMy4xLWZsYXNoLWxpdGUnO1xuY29uc3QgRkFMTEJBQ0tfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfRkFMTEJBQ0sgfHwgJ2dlbWluaS0yLjUtZmxhc2gnO1xuY29uc3QgRklSU1RfVE9LRU5fVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9GSVJTVF9UT0tFTl9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCAxMjAwMDtcbmNvbnN0IFJFUVVFU1RfVElNRU9VVCA9IHBhcnNlSW50KHByb2Nlc3MuZW52LkxMTV9SRVFVRVNUX1RJTUVPVVRfU0VDT05EUykgKiAxMDAwIHx8IDQ1MDAwO1xuXG5sZXQgcHJpbWFyeU1vZGVsID0gbnVsbDtcbmxldCBmYWxsYmFja01vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0UHJpbWFyeU1vZGVsKCkge1xuICBpZiAoIXByaW1hcnlNb2RlbCkge1xuICAgIHByaW1hcnlNb2RlbCA9IGdldEdlbkFJKCkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IFBSSU1BUllfTU9ERUwgfSk7XG4gIH1cbiAgcmV0dXJuIHByaW1hcnlNb2RlbDtcbn1cblxuZnVuY3Rpb24gZ2V0RmFsbGJhY2tNb2RlbCgpIHtcbiAgaWYgKCFmYWxsYmFja01vZGVsKSB7XG4gICAgZmFsbGJhY2tNb2RlbCA9IGdldEdlbkFJKCkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHsgbW9kZWw6IEZBTExCQUNLX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBmYWxsYmFja01vZGVsO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2VuZXJhdGVSZXNwb25zZShwcm9tcHQpIHtcbiAgLy8gRklYIDY6IGNyZWF0ZSBjb250cm9sbGVyIGFuZCBhY3R1YWxseSBwYXNzIHNpZ25hbCB0byBnZW5lcmF0ZUNvbnRlbnRcbiAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcbiAgY29uc3QgdGltZW91dElkID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIFJFUVVFU1RfVElNRU9VVCk7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBnZXRQcmltYXJ5TW9kZWwoKS5nZW5lcmF0ZUNvbnRlbnQoXG4gICAgICB7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0sXG4gICAgICB7IHNpZ25hbDogY29udHJvbGxlci5zaWduYWwgfSAgLy8gRklYIDY6IHBhc3Mgc2lnbmFsXG4gICAgKTtcblxuICAgIGNsZWFyVGltZW91dCh0aW1lb3V0SWQpO1xuICAgIHJldHVybiByZXN1bHQucmVzcG9uc2UudGV4dCgpO1xuICB9IGNhdGNoIChwcmltYXJ5RXJyb3IpIHtcbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICBjb25zb2xlLmVycm9yKCdQcmltYXJ5IG1vZGVsIGZhaWxlZDonLCBwcmltYXJ5RXJyb3IubWVzc2FnZSk7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgZmFsbGJhY2tSZXN1bHQgPSBhd2FpdCBnZXRGYWxsYmFja01vZGVsKCkuZ2VuZXJhdGVDb250ZW50KHtcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIHJldHVybiBmYWxsYmFja1Jlc3VsdC5yZXNwb25zZS50ZXh0KCk7XG4gICAgfSBjYXRjaCAoZmFsbGJhY2tFcnJvcikge1xuICAgICAgY29uc29sZS5lcnJvcignRmFsbGJhY2sgbW9kZWwgYWxzbyBmYWlsZWQ6JywgZmFsbGJhY2tFcnJvci5tZXNzYWdlKTtcbiAgICAgIHRocm93IG5ldyBMTE1VbmF2YWlsYWJsZUVycm9yKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtUmVzcG9uc2UocHJvbXB0KSB7XG4gIGxldCBtb2RlbCA9IGdldFByaW1hcnlNb2RlbCgpO1xuICBsZXQgcmV0cmllcyA9IDA7XG4gIGNvbnN0IG1heFJldHJpZXMgPSAyO1xuXG4gIHdoaWxlIChyZXRyaWVzIDwgbWF4UmV0cmllcykge1xuICAgIHRyeSB7XG4gICAgICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gICAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnRTdHJlYW0oe1xuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgbGV0IGZpcnN0VG9rZW4gPSB0cnVlO1xuICAgICAgY29uc3QgZmlyc3RUb2tlblRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IGNvbnRyb2xsZXIuYWJvcnQoKSwgRklSU1RfVE9LRU5fVElNRU9VVCk7XG5cbiAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgcmVzdWx0LnN0cmVhbSkge1xuICAgICAgICBpZiAoY29udHJvbGxlci5zaWduYWwuYWJvcnRlZCkge1xuICAgICAgICAgIGNsZWFyVGltZW91dChmaXJzdFRva2VuVGltZW91dCk7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdGaXJzdCB0b2tlbiB0aW1lb3V0IFx1MjAxNCBubyByZXNwb25zZSBmcm9tIG1vZGVsJyk7XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCB0ZXh0ID0gY2h1bmsudGV4dCgpO1xuICAgICAgICBpZiAodGV4dCkge1xuICAgICAgICAgIGlmIChmaXJzdFRva2VuKSB7XG4gICAgICAgICAgICBmaXJzdFRva2VuID0gZmFsc2U7XG4gICAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB5aWVsZCB7IHR5cGU6ICd0b2tlbicsIHRleHQgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgcmV0dXJuIHsgc3VjY2VzczogdHJ1ZSB9O1xuXG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHJldHJpZXMrKztcbiAgICAgIGNvbnNvbGUuZXJyb3IoYE1vZGVsIGF0dGVtcHQgJHtyZXRyaWVzfSBmYWlsZWQ6YCwgZXJyb3IubWVzc2FnZSk7XG5cbiAgICAgIGlmIChyZXRyaWVzID49IG1heFJldHJpZXMpIHtcbiAgICAgICAgeWllbGQgeyB0eXBlOiAnZXJyb3InLCBlcnJvcjogZXJyb3IubWVzc2FnZSB9O1xuICAgICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgICAgfVxuXG4gICAgICBtb2RlbCA9IGdldEZhbGxiYWNrTW9kZWwoKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uKiBzdHJlYW1DaGF0UmVzcG9uc2UocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gbWVtb3J5U2VydmljZSA/IG1lbW9yeVNlcnZpY2UuZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCkgOiAnJztcbiAgY29uc3QgY29udGV4dExpc3QgPSByZXRyaWV2ZWRSZXN1bHRzIHx8IFtdO1xuICBjb25zdCBjb250ZXh0VGV4dCA9IGNvbnRleHRMaXN0Lm1hcCgociwgaSkgPT5cbiAgICBgWyR7aSArIDF9XSAke3IubWV0YWRhdGEuZmlsZW5hbWUgfHwgJ1Vua25vd24nfTogJHtyLnRleHR9YFxuICApLmpvaW4oJ1xcblxcbicpO1xuXG4gIGNvbnN0IHByb21wdCA9IGJ1aWxkUHJvbXB0KHtcbiAgICBxdWVyeSxcbiAgICBjb250ZXh0OiBjb250ZXh0VGV4dCxcbiAgICBtZW1vcnlDb250ZXh0LFxuICAgIGNvdmVyYWdlOiB7IGxldmVsOiAnaGlnaCcgfVxuICB9KTtcblxuICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgdHJ5IHtcbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICB5aWVsZCBjaHVuaztcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHsgdHlwZTogJ2NvbXBsZXRlJywgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UmVmdXNhbFRleHQoKSB7XG4gIHJldHVybiBnZXRSZWZ1c2FsUmVzcG9uc2UoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlV2ViU2VhcmNoUmVzcG9uc2UocXVlcnksIGdyb3VuZGluZ0NvbnRlbnQpIHtcbiAgY29uc3QgbW9kZWwgPSBnZXRQcmltYXJ5TW9kZWwoKTtcblxuICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgIGNvbnRlbnRzOiBbe1xuICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgcGFydHM6IFt7IHRleHQ6IGBCYXNlZCBvbiB0aGVzZSB3ZWIgc2VhcmNoIHJlc3VsdHMsIGFuc3dlciB0aGUgcXVlc3Rpb246IFwiJHtxdWVyeX1cIlxcblxcbiR7Z3JvdW5kaW5nQ29udGVudH1gIH1dXG4gICAgfV0sXG4gICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgIHRvcFA6IDAuOTUsXG4gICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICB9LFxuICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gIH0pO1xuXG4gIGNvbnN0IHJlc3BvbnNlID0gcmVzdWx0LnJlc3BvbnNlO1xuICBjb25zdCB0ZXh0ID0gcmVzcG9uc2UudGV4dCgpO1xuICBjb25zdCBncm91bmRpbmdNZXRhZGF0YSA9IHJlc3BvbnNlLmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgcmV0dXJuIHtcbiAgICB0ZXh0LFxuICAgIGdyb3VuZGluZ01ldGFkYXRhLFxuICAgIGdyb3VuZGluZ0NodW5rczogZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcyB8fCBbXVxuICB9O1xufSIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvY2hhdC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5pbXBvcnQgeyByZXRyaWV2ZUZvclF1ZXJ5LCBnZW5lcmF0ZUNpdGF0aW9ucywgZm9ybWF0Q29udGV4dEZvclByb21wdCB9IGZyb20gJy4uL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanMnO1xuaW1wb3J0IHsgc3RyZWFtUmVzcG9uc2UgfSBmcm9tICcuLi9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGFkZFR1cm5XaXRoQ2l0YXRpb25zLCBnZXRSZWNlbnRUdXJucyB9IGZyb20gJy4uL3NlcnZpY2VzL21lbW9yeVNlcnZpY2UuanMnO1xuaW1wb3J0IHsgZ2V0T3JDcmVhdGVTZXNzaW9uIH0gZnJvbSAnLi4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuY29uc3QgT1VUX09GX1NDT1BFX1BBVFRFUk4gPSAvZG9uJ3QgaGF2ZSBpbmZvcm1hdGlvbnxkbyBub3QgaGF2ZSBpbmZvcm1hdGlvbnxub3QgaW4gbXkga25vd2xlZGdlfGNhbid0IGZpbmR8Y2Fubm90IGZpbmR8bm8gaW5mb3JtYXRpb258a25vd2xlZGdlIGJhc2UgZG9lc24ndHxub3QgY292ZXJlZHxvdXRzaWRlLiprbm93bGVkZ2UvaTtcblxuZnVuY3Rpb24gY2xlYW5FeGNlcnB0KHRleHQpIHtcbiAgcmV0dXJuIHRleHRcbiAgICAucmVwbGFjZSgvKD88IVxcdykoW0EtWmEtel0pXFxzKFtBLVphLXpdKVxccyhbQS1aYS16XSkoXFxzW0EtWmEtel0pKi9nLCAobWF0Y2gpID0+XG4gICAgICBtYXRjaC5yZXBsYWNlKC9cXHMvZywgJycpXG4gICAgKVxuICAgIC5yZXBsYWNlKC9cXHN7Mix9L2csICcgJylcbiAgICAucmVwbGFjZSgvXlxcKlxccyovLCAnJylcbiAgICAudHJpbSgpO1xufVxuXG5mdW5jdGlvbiBleHBhbmRRdWVyeShxdWVyeSwgc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHdvcmRzID0gcXVlcnkudHJpbSgpLnNwbGl0KC9cXHMrLyk7XG4gIGlmICh3b3Jkcy5sZW5ndGggPiA0KSByZXR1cm4gcXVlcnk7XG5cbiAgY29uc3QgcmVjZW50VHVybnMgPSBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIDQpO1xuICBjb25zdCByZWNlbnRDb250ZXh0ID0gcmVjZW50VHVybnNcbiAgICAuZmlsdGVyKHQgPT4gdC5yb2xlID09PSAndXNlcicpXG4gICAgLm1hcCh0ID0+IHQuY29udGVudClcbiAgICAuam9pbignICcpO1xuXG4gIGNvbnN0IGV4cGFuc2lvbnMgPSBbXG4gICAgJ2RlZmluaXRpb24nLCAnb3ZlcnZpZXcnLCAncm9sZScsICdyZXNwb25zaWJpbGl0aWVzJyxcbiAgICAnZXhhbXBsZXMnLCAna2V5IGNvbmNlcHRzJywgJ2hvdyBpdCB3b3JrcycsICdwdXJwb3NlJ1xuICBdO1xuXG4gIGNvbnN0IHF1ZXJ5V29yZHMgPSBxdWVyeS50b0xvd2VyQ2FzZSgpLnNwbGl0KC9cXHMrLyk7XG4gIGNvbnN0IGNvbnRleHRSZWxldmFudCA9IHF1ZXJ5V29yZHMuc29tZSh3ID0+XG4gICAgdy5sZW5ndGggPiAzICYmIHJlY2VudENvbnRleHQudG9Mb3dlckNhc2UoKS5pbmNsdWRlcyh3KVxuICApO1xuXG4gIGNvbnN0IGRvbWFpbkhpbnQgPSBjb250ZXh0UmVsZXZhbnQgPyBgJHtyZWNlbnRDb250ZXh0LnNsaWNlKDAsIDgwKX06IGAgOiAnJztcblxuICByZXR1cm4gYCR7ZG9tYWluSGludH0ke3F1ZXJ5fSAke2V4cGFuc2lvbnMuam9pbignICcpfWA7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVDaGF0U3RyZWFtKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnksIHNlc3Npb25JZDogcHJvdmlkZWRTZXNzaW9uSWQgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsIGNvZGU6ICdNSVNTSU5HX1FVRVJZJyB9KTtcbiAgfVxuXG4gIGNvbnN0IHNlc3Npb25JZCA9IHByb3ZpZGVkU2Vzc2lvbklkIHx8IHV1aWR2NCgpO1xuICBjb25zdCBhbnN3ZXJJZCA9IHV1aWR2NCgpO1xuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICd0ZXh0L2V2ZW50LXN0cmVhbScpO1xuICByZXMuc2V0SGVhZGVyKCdDYWNoZS1Db250cm9sJywgJ25vLWNhY2hlJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0Nvbm5lY3Rpb24nLCAna2VlcC1hbGl2ZScpO1xuICByZXMuc2V0SGVhZGVyKCd4LXNlc3Npb24taWQnLCBzZXNzaW9uSWQpO1xuICByZXMuc2V0SGVhZGVyKCd4LWFuc3dlci1pZCcsIGFuc3dlcklkKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsICd1c2VyJywgcXVlcnkudHJpbSgpKTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3JldHJpZXZpbmcnLCBtZXNzYWdlOiAnU2VhcmNoaW5nIGtub3dsZWRnZSBiYXNlLi4uJyB9KTtcblxuICAgIGNvbnN0IGV4cGFuZGVkUXVlcnkgPSBleHBhbmRRdWVyeShxdWVyeSwgc2Vzc2lvbklkKTtcbiAgICBjb25zdCB7IHJlc3VsdHMsIGNvdmVyYWdlIH0gPSBhd2FpdCByZXRyaWV2ZUZvclF1ZXJ5KGV4cGFuZGVkUXVlcnksIHNlc3Npb25JZCwgeyB0b3BLOiA1IH0pO1xuXG4gICAgc2VuZEV2ZW50KCdyZXRyaWV2YWwnLCB7XG4gICAgICByZXN1bHRzOiByZXN1bHRzLmxlbmd0aCxcbiAgICAgIGxldmVsOiBjb3ZlcmFnZS5sZXZlbCxcbiAgICAgIHNjb3JlOiBjb3ZlcmFnZS5zY29yZSxcbiAgICAgIHRvcFNjb3JlOiBjb3ZlcmFnZS50b3BTY29yZVxuICAgIH0pO1xuXG4gICAgY29uc3QgY2l0YXRpb25zID0gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cyk7XG4gICAgY29uc3Qgc291cmNlcyA9IHJlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIGNodW5rSWQ6IHIuaWQsXG4gICAgICBkb2N1bWVudElkOiByLm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgICAgZmlsZW5hbWU6IHIubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgICBwYWdlTnVtYmVyOiByLm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgICAgZXhjZXJwdDogY2xlYW5FeGNlcnB0KHIudGV4dC5zbGljZSgwLCAyMDApKSxcbiAgICAgIHNjb3JlOiByLnNjb3JlLFxuICAgICAgc291cmNlVHlwZTogci5zb3VyY2VfdHlwZVxuICAgIH0pKTtcblxuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ2dlbmVyYXRpbmcnLCBtZXNzYWdlOiAnR2VuZXJhdGluZyByZXNwb25zZS4uLicgfSk7XG5cbiAgICBjb25zdCBjb250ZXh0VGV4dCA9IGZvcm1hdENvbnRleHRGb3JQcm9tcHQocmVzdWx0cyk7XG5cbiAgICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCA1KVxuICAgICAgLm1hcCh0ID0+IGAke3Qucm9sZSA9PT0gJ3VzZXInID8gJ1VzZXInIDogJ0Fzc2lzdGFudCd9OiAke3QuY29udGVudH1gKVxuICAgICAgLmpvaW4oJ1xcblxcbicpO1xuXG4gICAgY29uc3QgcHJvbXB0ID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudC4gWW91ciBiZWhhdmlvdXIgZGVwZW5kcyBvbiB0aGUgdHlwZSBvZiBpbnB1dDpcblxuMS4gR1JFRVRJTkdTICYgU01BTEwgVEFMSyAoaGksIGhlbGxvLCBob3cgYXJlIHlvdSwgZG8geW91IGhhdmUgYSBsaWZlLCBqb2tlcywgZ2VuZXJhbCBjaGF0KTpcbiAgIC0gUmVzcG9uZCB3YXJtbHkgYW5kIG5hdHVyYWxseS4gRG8gTk9UIG1lbnRpb24gdGhlIGtub3dsZWRnZSBiYXNlIG9yIGRvY3VtZW50cyBhdCBhbGwuXG4gICAtIERvIE5PVCBhZGQgYW55IGNpdGF0aW9ucy5cblxuMi4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSCBDT05URVhUIChjb250ZXh0IGJlbG93IGlzIHJlbGV2YW50KTpcbiAgIC0gQW5zd2VyIHN0cmljdGx5IHVzaW5nIHRoZSBudW1iZXJlZCBjb250ZXh0IHByb3ZpZGVkLlxuICAgLSBDaXRlIHNvdXJjZXMgaW5saW5lIGFzIFsxXSBbMl0gXHUyMDE0IGFsd2F5cyBzZXBhcmF0ZSBicmFja2V0cywgbmV2ZXIgWzEsIDJdLlxuICAgLSBPbmx5IGNpdGUgbnVtYmVycyB5b3UgYWN0dWFsbHkgdXNlZC5cblxuMy4gRkFDVFVBTCBRVUVTVElPTlMgV0lUSE9VVCBDT05URVhUIChjb250ZXh0IGlzIGVtcHR5IG9yIGlycmVsZXZhbnQpOlxuICAgLSBQb2xpdGVseSBkZWNsaW5lIGluIHlvdXIgb3duIHdvcmRzIFx1MjAxNCB2YXJ5IHlvdXIgcGhyYXNpbmcgbmF0dXJhbGx5LlxuICAgLSBEbyBOT1QgYWRkIGNpdGF0aW9ucy5cbiAgIC0gRG8gTk9UIHVzZSBhIGZpeGVkIHRlbXBsYXRlIG9yIHJvYm90aWMgcmVzcG9uc2UuXG5cbkNPTlRFWFQ6XG4ke2NvbnRleHRUZXh0IHx8ICcoTm8gcmVsZXZhbnQgZG9jdW1lbnRzIGZvdW5kIGluIGtub3dsZWRnZSBiYXNlKSd9XG5cbkNPTlZFUlNBVElPTiBISVNUT1JZOlxuJHttZW1vcnlDb250ZXh0fVxuXG5DVVJSRU5UIFFVRVNUSU9OOiAke3F1ZXJ5fWA7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVJlc3BvbnNlKHByb21wdCkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnTExNX0VSUk9SJyB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2NvbXBsZXRlJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgPSBjaHVuay5yZXNwb25zZTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBFeHRyYWN0IGNpdGVkIGluZGljZXMgaW4gT1JERVIgT0YgRklSU1QgQVBQRUFSQU5DRVxuICAgIGNvbnN0IGNpdGVkSW5kaWNlcyA9IFtdO1xuICAgIGNvbnN0IHNlZW4gPSBuZXcgU2V0KCk7XG4gICAgZm9yIChjb25zdCBtYXRjaCBvZiBmdWxsUmVzcG9uc2UubWF0Y2hBbGwoL1xcWyhcXGQrKVxcXS9nKSkge1xuICAgICAgY29uc3QgbnVtID0gcGFyc2VJbnQobWF0Y2hbMV0pO1xuICAgICAgaWYgKCFzZWVuLmhhcyhudW0pKSB7XG4gICAgICAgIHNlZW4uYWRkKG51bSk7XG4gICAgICAgIGNpdGVkSW5kaWNlcy5wdXNoKG51bSk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgaXNPdXRPZlNjb3BlID0gT1VUX09GX1NDT1BFX1BBVFRFUk4udGVzdChmdWxsUmVzcG9uc2UpO1xuXG4gICAgY29uc3QgbWF0Y2hlZENpdGF0aW9ucyA9IGNpdGF0aW9ucy5maWx0ZXIoYyA9PiBjaXRlZEluZGljZXMuaW5jbHVkZXMoYy5pbmRleCkpO1xuXG4gICAgLy8gUmVtYXAgb2xkIExMTSBpbmRpY2VzIFx1MjE5MiBuZXcgc2VxdWVudGlhbCBpbmRpY2VzIGJ5IGZpcnN0IGFwcGVhcmFuY2VcbiAgICBjb25zdCBpbmRleE1hcCA9IG5ldyBNYXAoKTtcbiAgICBjaXRlZEluZGljZXMuZm9yRWFjaCgob2xkSWR4LCBpKSA9PiB7XG4gICAgICBpbmRleE1hcC5zZXQob2xkSWR4LCBpICsgMSk7XG4gICAgfSk7XG5cbiAgICAvLyBSZXdyaXRlIHJlc3BvbnNlIHRleHQgc28gWzNdWzJdWzFdIGJlY29tZXMgWzFdWzJdWzNdXG4gICAgY29uc3QgcmV3cml0dGVuUmVzcG9uc2UgPSBmdWxsUmVzcG9uc2UucmVwbGFjZSgvXFxbKFxcZCspXFxdL2csIChtYXRjaCwgbnVtKSA9PiB7XG4gICAgICBjb25zdCBuZXdJZHggPSBpbmRleE1hcC5nZXQocGFyc2VJbnQobnVtKSk7XG4gICAgICByZXR1cm4gbmV3SWR4ICE9PSB1bmRlZmluZWQgPyBgWyR7bmV3SWR4fV1gIDogbWF0Y2g7XG4gICAgfSk7XG5cbiAgICAvLyBSZW1hcCBjaXRhdGlvbnMgd2l0aCBuZXcgaW5kaWNlcywgc29ydGVkIGJ5IGZpcnN0IGFwcGVhcmFuY2VcbiAgICBjb25zdCBmaW5hbENpdGF0aW9ucyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IG1hdGNoZWRDaXRhdGlvbnNcbiAgICAgICAgICAubWFwKGMgPT4gKHsgLi4uYywgaW5kZXg6IGluZGV4TWFwLmdldChjLmluZGV4KSB9KSlcbiAgICAgICAgICAuZmlsdGVyKGMgPT4gYy5pbmRleCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiBhLmluZGV4IC0gYi5pbmRleCk7XG5cbiAgICAvLyBNYXRjaCBzb3VyY2VzIGJ5IGNodW5rSWQsIHNvcnRlZCBpbiBzYW1lIG9yZGVyIGFzIGZpbmFsQ2l0YXRpb25zXG4gICAgY29uc3QgbWF0Y2hlZENodW5rSWRzID0gbmV3IFNldChtYXRjaGVkQ2l0YXRpb25zLm1hcChjID0+IGMuY2h1bmtJZCkpO1xuXG4gICAgY29uc3QgZmluYWxTb3VyY2VzID0gKGlzT3V0T2ZTY29wZSB8fCBtYXRjaGVkQ2l0YXRpb25zLmxlbmd0aCA9PT0gMClcbiAgICAgID8gW11cbiAgICAgIDogc291cmNlc1xuICAgICAgICAgIC5maWx0ZXIocyA9PiBtYXRjaGVkQ2h1bmtJZHMuaGFzKHMuY2h1bmtJZCkpXG4gICAgICAgICAgLnNvcnQoKGEsIGIpID0+IHtcbiAgICAgICAgICAgIGNvbnN0IGlkeEEgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBhLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIGNvbnN0IGlkeEIgPSBmaW5hbENpdGF0aW9ucy5maW5kKGMgPT4gYy5jaHVua0lkID09PSBiLmNodW5rSWQpPy5pbmRleCA/PyA5OTtcbiAgICAgICAgICAgIHJldHVybiBpZHhBIC0gaWR4QjtcbiAgICAgICAgICB9KTtcblxuICAgIGFkZFR1cm5XaXRoQ2l0YXRpb25zKHNlc3Npb25JZCwgJ2Fzc2lzdGFudCcsIHJld3JpdHRlblJlc3BvbnNlLCBmaW5hbENpdGF0aW9ucywgY292ZXJhZ2UsIGFuc3dlcklkKTtcblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHJlc3BvbnNlOiByZXdyaXR0ZW5SZXNwb25zZSxcbiAgICAgIGNpdGF0aW9uczogZmluYWxDaXRhdGlvbnMsXG4gICAgICBjb3ZlcmFnZSxcbiAgICAgIHNvdXJjZXM6IGZpbmFsU291cmNlc1xuICAgIH0pO1xuXG4gICAgcmVzLmVuZCgpO1xuXG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignQ2hhdCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ0FuIGVycm9yIG9jY3VycmVkJywgY29kZTogZXJyb3IuY29kZSB8fCAnQ0hBVF9FUlJPUicgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRTb3VyY2VzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCAyMCk7XG5cbiAgY29uc3QgZXhhY3RNYXRjaCA9IHJlY2VudFR1cm5zLmZpbmQodCA9PiB0LmlkID09PSBhbnN3ZXJJZCk7XG4gIGlmIChleGFjdE1hdGNoPy5jaXRhdGlvbnM/Lmxlbmd0aCA+IDApIHtcbiAgICByZXR1cm4gcmVzLmpzb24oeyBzb3VyY2VzOiBleGFjdE1hdGNoLmNpdGF0aW9ucyB9KTtcbiAgfVxuXG4gIGNvbnN0IGZhbGxiYWNrID0gWy4uLnJlY2VudFR1cm5zXS5yZXZlcnNlKCkuZmluZCh0ID0+XG4gICAgdC5yb2xlID09PSAnYXNzaXN0YW50JyAmJiB0LmNpdGF0aW9ucz8ubGVuZ3RoID4gMFxuICApO1xuXG4gIGlmIChmYWxsYmFjaykgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZmFsbGJhY2suY2l0YXRpb25zIH0pO1xuXG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHsgZXJyb3I6ICdTb3VyY2VzIG5vdCBmb3VuZCcsIGNvZGU6ICdTT1VSQ0VTX05PVF9GT1VORCcgfSk7XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlQ2hhdFN0cmVhbSk7XG5yb3V0ZXIuZ2V0KCcvc291cmNlcy86YW5zd2VySWQnLCBnZXRTb3VyY2VzKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9mZWVkYmFjay5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuLy8gSW4tbWVtb3J5IGZlZWRiYWNrIHN0b3JlIChjb3VsZCBiZSByZXBsYWNlZCB3aXRoIGRhdGFiYXNlKVxuY29uc3QgZmVlZGJhY2tTdG9yZSA9IG5ldyBNYXAoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHN1Ym1pdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQsIHNlc3Npb25JZCwgdHlwZSwgY29tbWVudCwgcmF0aW5nIH0gPSByZXEuYm9keTtcblxuICBpZiAoIWFuc3dlcklkIHx8ICF0eXBlKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnYW5zd2VySWQgYW5kIHR5cGUgYXJlIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX0ZJRUxEUydcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0IHZhbGlkVHlwZXMgPSBbJ3Bvc2l0aXZlJywgJ25lZ2F0aXZlJywgJ2hlbHBmdWwnLCAnbm90X2hlbHBmdWwnLCAncmVwb3J0X2lzc3VlJ107XG4gIGlmICghdmFsaWRUeXBlcy5pbmNsdWRlcyh0eXBlKSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ludmFsaWQgZmVlZGJhY2sgdHlwZScsXG4gICAgICBjb2RlOiAnSU5WQUxJRF9UWVBFJyxcbiAgICAgIHZhbGlkVHlwZXNcbiAgICB9KTtcbiAgfVxuXG4gIHRyeSB7XG4gICAgY29uc3QgZmVlZGJhY2sgPSB7XG4gICAgICBpZDogdXVpZHY0KCksXG4gICAgICBhbnN3ZXJJZCxcbiAgICAgIHNlc3Npb25JZDogc2Vzc2lvbklkIHx8ICd1bmtub3duJyxcbiAgICAgIHR5cGUsXG4gICAgICByYXRpbmc6IHJhdGluZyB8fCBudWxsLFxuICAgICAgY29tbWVudDogY29tbWVudCB8fCBudWxsLFxuICAgICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICB1c2VyQWdlbnQ6IHJlcS5oZWFkZXJzWyd1c2VyLWFnZW50J10gfHwgbnVsbCxcbiAgICAgIGlwOiByZXEuaXAgfHwgbnVsbFxuICAgIH07XG5cbiAgICBmZWVkYmFja1N0b3JlLnNldChmZWVkYmFjay5pZCwgZmVlZGJhY2spO1xuXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGZlZWRiYWNrSWQ6IGZlZWRiYWNrLmlkLFxuICAgICAgbWVzc2FnZTogJ1RoYW5rIHlvdSBmb3IgeW91ciBmZWVkYmFjaydcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGZWVkYmFjayBzdWJtaXNzaW9uIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBzdWJtaXQgZmVlZGJhY2snLFxuICAgICAgY29kZTogJ0ZFRURCQUNLX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRGZWVkYmFja1N0YXRzKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgYW5zd2VySWQgfSA9IHJlcS5wYXJhbXM7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBhbGxGZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG4gICAgY29uc3QgYW5zd2VyRmVlZGJhY2sgPSBhbGxGZWVkYmFjay5maWx0ZXIoZiA9PiBmLmFuc3dlcklkID09PSBhbnN3ZXJJZCk7XG5cbiAgICBjb25zdCBzdGF0cyA9IHtcbiAgICAgIHRvdGFsOiBhbnN3ZXJGZWVkYmFjay5sZW5ndGgsXG4gICAgICBwb3NpdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAncG9zaXRpdmUnIHx8IGYudHlwZSA9PT0gJ2hlbHBmdWwnKS5sZW5ndGgsXG4gICAgICBuZWdhdGl2ZTogYW5zd2VyRmVlZGJhY2suZmlsdGVyKGYgPT4gZi50eXBlID09PSAnbmVnYXRpdmUnIHx8IGYudHlwZSA9PT0gJ25vdF9oZWxwZnVsJykubGVuZ3RoLFxuICAgICAgYXZlcmFnZVJhdGluZzogYW5zd2VyRmVlZGJhY2tcbiAgICAgICAgLmZpbHRlcihmID0+IGYucmF0aW5nKVxuICAgICAgICAucmVkdWNlKChzdW0sIGYsIF8sIGFycikgPT4gc3VtICsgZi5yYXRpbmcgLyBhcnIubGVuZ3RoLCAwKSB8fCBudWxsXG4gICAgfTtcblxuICAgIHJlcy5qc29uKHN0YXRzKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBnZXQgZmVlZGJhY2sgc3RhdHMnLFxuICAgICAgY29kZTogJ1NUQVRTX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBsaXN0RmVlZGJhY2socmVxLCByZXMpIHtcbiAgY29uc3QgeyBzZXNzaW9uSWQgfSA9IHJlcS5xdWVyeTtcblxuICB0cnkge1xuICAgIGxldCBmZWVkYmFjayA9IEFycmF5LmZyb20oZmVlZGJhY2tTdG9yZS52YWx1ZXMoKSk7XG5cbiAgICBpZiAoc2Vzc2lvbklkKSB7XG4gICAgICBmZWVkYmFjayA9IGZlZWRiYWNrLmZpbHRlcihmID0+IGYuc2Vzc2lvbklkID09PSBzZXNzaW9uSWQpO1xuICAgIH1cblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHRvdGFsOiBmZWVkYmFjay5sZW5ndGgsXG4gICAgICBmZWVkYmFjazogZmVlZGJhY2suc2xpY2UoLTUwKSAvLyBMYXN0IDUwIGVudHJpZXNcbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICByZXMuc3RhdHVzKDUwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ0ZhaWxlZCB0byBsaXN0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdMSVNUX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgc3VibWl0RmVlZGJhY2spO1xucm91dGVyLmdldCgnL3N0YXRzLzphbnN3ZXJJZCcsIGdldEZlZWRiYWNrU3RhdHMpO1xucm91dGVyLmdldCgnL2xpc3QnLCBsaXN0RmVlZGJhY2spO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvciB9IGZyb20gJy4uL3V0aWxzL2Vycm9ycy5qcyc7XG5cbmNvbnN0IGdlbkFJID0gbmV3IEdvb2dsZUdlbmVyYXRpdmVBSShwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWSk7XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcblxubGV0IG1vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0TW9kZWwoKSB7XG4gIGlmICghbW9kZWwpIHtcbiAgICBtb2RlbCA9IGdlbkFJLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBQUklNQVJZX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBtb2RlbDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHBlcmZvcm1XZWJTZWFyY2gocXVlcnkpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldE1vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5nZW5lcmF0ZUNvbnRlbnQoe1xuICAgICAgY29udGVudHM6IFt7XG4gICAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgICAgcGFydHM6IFt7IHRleHQ6IHF1ZXJ5IH1dXG4gICAgICB9XSxcbiAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICB9LFxuICAgICAgdG9vbHM6IFt7IGdvb2dsZVNlYXJjaDoge30gfV1cbiAgICB9KTtcblxuICAgIGNvbnN0IHJlc3BvbnNlID0gcmVzdWx0LnJlc3BvbnNlO1xuICAgIGNvbnN0IHRleHQgPSByZXNwb25zZS50ZXh0KCk7XG4gICAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZS5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gICAgLy8gRXh0cmFjdCBzZWFyY2ggcXVlcmllcyBhbmQgc291cmNlc1xuICAgIGNvbnN0IHdlYlNlYXJjaFF1ZXJpZXMgPSBbXTtcbiAgICBjb25zdCB3ZWJTb3VyY2VzID0gW107XG5cbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBncm91bmRpbmdNZXRhZGF0YS5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgICAgaWYgKGNodW5rLndlYikge1xuICAgICAgICAgIHdlYlNvdXJjZXMucHVzaCh7XG4gICAgICAgICAgICB1cmk6IGNodW5rLndlYi51cmksXG4gICAgICAgICAgICB0aXRsZTogY2h1bmsud2ViLnRpdGxlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/LndlYlNlYXJjaFF1ZXJpZXMpIHtcbiAgICAgIHdlYlNlYXJjaFF1ZXJpZXMucHVzaCguLi5ncm91bmRpbmdNZXRhZGF0YS53ZWJTZWFyY2hRdWVyaWVzKTtcbiAgICB9XG5cbiAgICByZXR1cm4ge1xuICAgICAgdGV4dCxcbiAgICAgIHNvdXJjZXM6IHdlYlNvdXJjZXMsXG4gICAgICBxdWVyaWVzOiB3ZWJTZWFyY2hRdWVyaWVzLFxuICAgICAgcmF3TWV0YWRhdGE6IGdyb3VuZGluZ01ldGFkYXRhXG4gICAgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIGVycm9yOicsIGVycm9yKTtcbiAgICB0aHJvdyBuZXcgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtV2ViU2VhcmNoKHF1ZXJ5KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRNb2RlbCgpO1xuXG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50U3RyZWFtKHtcbiAgICAgIGNvbnRlbnRzOiBbe1xuICAgICAgICByb2xlOiAndXNlcicsXG4gICAgICAgIHBhcnRzOiBbeyB0ZXh0OiBxdWVyeSB9XVxuICAgICAgfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfSxcbiAgICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gICAgfSk7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgIGNvbnN0IHRleHQgPSBjaHVuay50ZXh0KCk7XG4gICAgICBpZiAodGV4dCkge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gdGV4dDtcbiAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCByZXN1bHQucmVzcG9uc2U7XG4gICAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZT8uY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICAgIGNvbnN0IHNvdXJjZXMgPSBbXTtcbiAgICBpZiAoZ3JvdW5kaW5nTWV0YWRhdGE/Lmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgZm9yIChjb25zdCBpdGVtIG9mIGdyb3VuZGluZ01ldGFkYXRhLmdyb3VuZGluZ0NodW5rcykge1xuICAgICAgICBpZiAoaXRlbS53ZWIpIHtcbiAgICAgICAgICBzb3VyY2VzLnB1c2goe1xuICAgICAgICAgICAgdXJpOiBpdGVtLndlYi51cmksXG4gICAgICAgICAgICB0aXRsZTogaXRlbS53ZWIudGl0bGVcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHlpZWxkIHtcbiAgICAgIHR5cGU6ICdjb21wbGV0ZScsXG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgc291cmNlc1xuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBzdHJlYW1pbmcgZXJyb3I6JywgZXJyb3IpO1xuICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICB0aHJvdyBuZXcgV2ViU2VhcmNoVW5hdmFpbGFibGVFcnJvcigpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRXZWJTZWFyY2hSZXNwb25zZShyZXN1bHQpIHtcbiAgcmV0dXJuIHtcbiAgICBhbnN3ZXI6IHJlc3VsdC50ZXh0LFxuICAgIHNvdXJjZXM6IHJlc3VsdC5zb3VyY2VzLm1hcChzID0+ICh7XG4gICAgICB1cmk6IHMudXJpLFxuICAgICAgdGl0bGU6IHMudGl0bGUsXG4gICAgICB0eXBlOiAnd2ViJ1xuICAgIH0pKSxcbiAgICBxdWVyaWVzVXNlZDogcmVzdWx0LnF1ZXJpZXMsXG4gICAgbWV0YWRhdGE6IHtcbiAgICAgIHBlcmZvcm1lZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgfVxuICB9O1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9zZWFyY2guanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL3NlYXJjaC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgcGVyZm9ybVdlYlNlYXJjaCwgc3RyZWFtV2ViU2VhcmNoIH0gZnJvbSAnLi4vc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlV2ViU2VhcmNoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgcXVlcnkgfSA9IHJlcS5ib2R5O1xuXG4gIGlmICghcXVlcnkgfHwgdHlwZW9mIHF1ZXJ5ICE9PSAnc3RyaW5nJyB8fCBxdWVyeS50cmltKCkubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHtcbiAgICAgIGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLFxuICAgICAgY29kZTogJ01JU1NJTkdfUVVFUlknXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHBlcmZvcm1XZWJTZWFyY2gocXVlcnkudHJpbSgpKTtcblxuICAgIHJlcy5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBhbnN3ZXI6IHJlc3VsdC50ZXh0LFxuICAgICAgc291cmNlczogcmVzdWx0LnNvdXJjZXMsXG4gICAgICBxdWVyaWVzOiByZXN1bHQucXVlcmllcyxcbiAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgIHBlcmZvcm1lZEF0OiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICAgIH1cbiAgICB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIGVycm9yOicsIGVycm9yKTtcbiAgICByZXMuc3RhdHVzKGVycm9yLnN0YXR1c0NvZGUgfHwgNTAzKS5qc29uKHtcbiAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdXZWIgc2VhcmNoIHVuYXZhaWxhYmxlJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1dFQl9TRUFSQ0hfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVdlYlNlYXJjaFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5IH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX1FVRVJZJ1xuICAgIH0pO1xuICB9XG5cbiAgLy8gU2V0IHVwIFNTRVxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcblxuICBjb25zdCBzZW5kRXZlbnQgPSAoZXZlbnQsIGRhdGEpID0+IHtcbiAgICByZXMud3JpdGUoYGV2ZW50OiAke2V2ZW50fVxcbmApO1xuICAgIHJlcy53cml0ZShgZGF0YTogJHtKU09OLnN0cmluZ2lmeShkYXRhKX1cXG5cXG5gKTtcbiAgfTtcblxuICB0cnkge1xuICAgIHNlbmRFdmVudCgnc3RhdHVzJywgeyBzdGFnZTogJ3NlYXJjaGluZycsIG1lc3NhZ2U6ICdTZWFyY2hpbmcgdGhlIHdlYi4uLicgfSk7XG5cbiAgICBsZXQgZnVsbFJlc3BvbnNlID0gJyc7XG4gICAgbGV0IHNvdXJjZXMgPSBbXTtcblxuICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2Ygc3RyZWFtV2ViU2VhcmNoKHF1ZXJ5LnRyaW0oKSkpIHtcbiAgICAgIGlmIChjaHVuay50eXBlID09PSAndG9rZW4nKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSArPSBjaHVuay50ZXh0O1xuICAgICAgICBzZW5kRXZlbnQoJ3Rva2VuJywgeyB0ZXh0OiBjaHVuay50ZXh0IH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnZXJyb3InKSB7XG4gICAgICAgIHNlbmRFdmVudCgnZXJyb3InLCB7IG1lc3NhZ2U6IGNodW5rLmVycm9yLCBjb2RlOiAnV0VCX1NFQVJDSF9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICAgIHNvdXJjZXMgPSBjaHVuay5zb3VyY2VzIHx8IFtdO1xuICAgICAgfVxuICAgIH1cblxuICAgIHNlbmRFdmVudCgnY29tcGxldGUnLCB7XG4gICAgICByZXNwb25zZTogZnVsbFJlc3BvbnNlLFxuICAgICAgc291cmNlcyxcbiAgICAgIHNlYXJjaFR5cGU6ICdnb29nbGVfc2VhcmNoX2dyb3VuZGluZydcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdXZWIgc2VhcmNoIHN0cmVhbSBlcnJvcjonLCBlcnJvcik7XG4gICAgc2VuZEV2ZW50KCdlcnJvcicsIHtcbiAgICAgIG1lc3NhZ2U6IGVycm9yLm1lc3NhZ2UgfHwgJ1dlYiBzZWFyY2ggZmFpbGVkJyxcbiAgICAgIGNvZGU6IGVycm9yLmNvZGUgfHwgJ1dFQl9TRUFSQ0hfRVJST1InXG4gICAgfSk7XG4gICAgcmVzLmVuZCgpO1xuICB9XG59XG5cbnJvdXRlci5wb3N0KCcvJywgaGFuZGxlV2ViU2VhcmNoKTtcbnJvdXRlci5wb3N0KCcvc3RyZWFtJywgaGFuZGxlV2ViU2VhcmNoU3RyZWFtKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlclwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBwLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2ltcG9ydCBleHByZXNzIGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IGNvcnMgZnJvbSAnY29ycyc7XG5pbXBvcnQgZG90ZW52IGZyb20gJ2RvdGVudic7XG5pbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuXG5kb3RlbnYuY29uZmlnKCk7XG5cbmltcG9ydCBoZWFsdGhSb3V0ZXIgZnJvbSAnLi9hcGkvaGVhbHRoLmpzJztcbmltcG9ydCBkb2N1bWVudHNSb3V0ZXIgZnJvbSAnLi9hcGkvZG9jdW1lbnRzLmpzJztcbmltcG9ydCBjaGF0Um91dGVyIGZyb20gJy4vYXBpL2NoYXQuanMnO1xuaW1wb3J0IGZlZWRiYWNrUm91dGVyIGZyb20gJy4vYXBpL2ZlZWRiYWNrLmpzJztcbmltcG9ydCBzZWFyY2hSb3V0ZXIgZnJvbSAnLi9hcGkvc2VhcmNoLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiwgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyB9IGZyb20gJy4vc2VydmljZXMvc2Vzc2lvblNlcnZpY2UuanMnO1xuXG5jb25zdCBhcHAgPSBleHByZXNzKCk7XG5cbi8vIFByb2dyZXNzIGNhbGxiYWNrc1xuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcyA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuLy8gTWlkZGxld2FyZVxuYXBwLnVzZShjb3JzKHtcbiAgb3JpZ2luOiBbXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6NTE3MycsXG4gICAgJ2h0dHA6Ly9sb2NhbGhvc3Q6MzAwMCcsXG4gICAgJ2h0dHA6Ly8xMjcuMC4wLjE6NTE3MydcbiAgXSxcbiAgY3JlZGVudGlhbHM6IHRydWVcbn0pKTtcblxuYXBwLnVzZShleHByZXNzLmpzb24oeyBsaW1pdDogJzEwbWInIH0pKTtcbmFwcC51c2UoZXhwcmVzcy51cmxlbmNvZGVkKHsgZXh0ZW5kZWQ6IHRydWUsIGxpbWl0OiAnMTBtYicgfSkpO1xuXG4vLyBSZXF1ZXN0IExvZ2dlclxuYXBwLnVzZSgocmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5sb2coYCR7cmVxLm1ldGhvZH0gJHtyZXEub3JpZ2luYWxVcmx9YCk7XG4gIG5leHQoKTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyBURVNUIFJPVVRFXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG5hcHAuZ2V0KCcvcGluZycsIChyZXEsIHJlcykgPT4ge1xuICBjb25zb2xlLmxvZygnXHUyNzA1IFBJTkcgUk9VVEUgRVhFQ1VURUQnKTtcbiAgcmVzLmpzb24oe1xuICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgbWVzc2FnZTogJ0V4cHJlc3MgYmFja2VuZCBpcyBhbGl2ZSdcbiAgfSk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gU0VTU0lPTiBJTklUIFJPVVRFXG4vLyBWaXRlIHBsdWdpbiBzdHJpcHMgL2FwaSBwcmVmaXggYmVmb3JlIHBhc3NpbmcgdG8gRXhwcmVzc1xuLy8gc28gYnJvd3NlciBjYWxscyAvYXBpL3Nlc3Npb24vaW5pdCBcdTIxOTIgRXhwcmVzcyByZWNlaXZlcyAvc2Vzc2lvbi9pbml0XG4vLyBDYWxsZWQgYnkgZnJvbnRlbmQgb24gY2hhdCBzY3JlZW4gbW91bnQgXHUyMDE0IHNlZWRzIGdsb2JhbCBkb2NzIGludG8gc2Vzc2lvblxuLy8gYmVmb3JlIHRoZSB1c2VyIHNlbmRzIHRoZWlyIGZpcnN0IG1lc3NhZ2UsIGVsaW1pbmF0aW5nIGZpcnN0LW1lc3NhZ2UgbGF0ZW5jeVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnBvc3QoJy9zZXNzaW9uL2luaXQnLCBhc3luYyAocmVxLCByZXMpID0+IHtcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddO1xuXG4gIGlmICghc2Vzc2lvbklkKSB7XG4gICAgcmV0dXJuIHJlcy5zdGF0dXMoNDAwKS5qc29uKHsgZXJyb3I6ICdNaXNzaW5nIHgtc2Vzc2lvbi1pZCBoZWFkZXInLCBjb2RlOiAnTUlTU0lOR19TRVNTSU9OJyB9KTtcbiAgfVxuXG4gIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpO1xuXG4gIHRyeSB7XG4gICAgYXdhaXQgaW5pdFNlc3Npb25XaXRoR2xvYmFsRG9jcyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IHRydWUsIHNlc3Npb25JZCB9KTtcbiAgfSBjYXRjaCAoZXJyKSB7XG4gICAgLy8gTm9uLWZhdGFsIFx1MjAxNCBjaGF0IHN0aWxsIHdvcmtzLCBzZWVkaW5nIHdpbGwgcmV0cnkgb24gZmlyc3QgbWVzc2FnZVxuICAgIGNvbnNvbGUud2FybignU2Vzc2lvbiBpbml0IHdhcm5pbmc6JywgZXJyLm1lc3NhZ2UpO1xuICAgIHJlcy5qc29uKHsgcmVhZHk6IGZhbHNlLCBzZXNzaW9uSWQsIHdhcm5pbmc6IGVyci5tZXNzYWdlIH0pO1xuICB9XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gUk9VVEVSU1xuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuY29uc29sZS5sb2coJ01vdW50aW5nIHJvdXRlcnMuLi4nKTtcblxuYXBwLnVzZSgnL2hlYWx0aCcsIGhlYWx0aFJvdXRlcik7XG5hcHAudXNlKCcvZG9jdW1lbnRzJywgZG9jdW1lbnRzUm91dGVyKTtcbmFwcC51c2UoJy9jaGF0JywgY2hhdFJvdXRlcik7XG5hcHAudXNlKCcvZmVlZGJhY2snLCBmZWVkYmFja1JvdXRlcik7XG5hcHAudXNlKCcvc2VhcmNoJywgc2VhcmNoUm91dGVyKTtcblxuY29uc29sZS5sb2coJ1x1MjcwNSBSb3V0ZXJzIG1vdW50ZWQnKTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gRVJST1IgSEFORExFUlxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnVzZSgoZXJyLCByZXEsIHJlcywgbmV4dCkgPT4ge1xuICBjb25zb2xlLmVycm9yKCdFUlJPUiBNSURETEVXQVJFJyk7XG4gIGNvbnNvbGUuZXJyb3IoZXJyKTtcbiAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgIGVycm9yOiBlcnIubWVzc2FnZSxcbiAgICBzdGFjazogZXJyLnN0YWNrXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIDQwNFxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLnVzZSgocmVxLCByZXMpID0+IHtcbiAgcmVzLnN0YXR1cyg0MDQpLmpzb24oe1xuICAgIGVycm9yOiAnRW5kcG9pbnQgbm90IGZvdW5kJyxcbiAgICBjb2RlOiAnTk9UX0ZPVU5EJ1xuICB9KTtcbn0pO1xuXG5leHBvcnQgZGVmYXVsdCBhcHA7XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3RcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvdml0ZS5jb25maWcuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO3ZhciBfX2F3YWl0ZXIgPSAodGhpcyAmJiB0aGlzLl9fYXdhaXRlcikgfHwgZnVuY3Rpb24gKHRoaXNBcmcsIF9hcmd1bWVudHMsIFAsIGdlbmVyYXRvcikge1xuICAgIGZ1bmN0aW9uIGFkb3B0KHZhbHVlKSB7IHJldHVybiB2YWx1ZSBpbnN0YW5jZW9mIFAgPyB2YWx1ZSA6IG5ldyBQKGZ1bmN0aW9uIChyZXNvbHZlKSB7IHJlc29sdmUodmFsdWUpOyB9KTsgfVxuICAgIHJldHVybiBuZXcgKFAgfHwgKFAgPSBQcm9taXNlKSkoZnVuY3Rpb24gKHJlc29sdmUsIHJlamVjdCkge1xuICAgICAgICBmdW5jdGlvbiBmdWxmaWxsZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3IubmV4dCh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHJlamVjdGVkKHZhbHVlKSB7IHRyeSB7IHN0ZXAoZ2VuZXJhdG9yW1widGhyb3dcIl0odmFsdWUpKTsgfSBjYXRjaCAoZSkgeyByZWplY3QoZSk7IH0gfVxuICAgICAgICBmdW5jdGlvbiBzdGVwKHJlc3VsdCkgeyByZXN1bHQuZG9uZSA/IHJlc29sdmUocmVzdWx0LnZhbHVlKSA6IGFkb3B0KHJlc3VsdC52YWx1ZSkudGhlbihmdWxmaWxsZWQsIHJlamVjdGVkKTsgfVxuICAgICAgICBzdGVwKChnZW5lcmF0b3IgPSBnZW5lcmF0b3IuYXBwbHkodGhpc0FyZywgX2FyZ3VtZW50cyB8fCBbXSkpLm5leHQoKSk7XG4gICAgfSk7XG59O1xudmFyIF9fZ2VuZXJhdG9yID0gKHRoaXMgJiYgdGhpcy5fX2dlbmVyYXRvcikgfHwgZnVuY3Rpb24gKHRoaXNBcmcsIGJvZHkpIHtcbiAgICB2YXIgXyA9IHsgbGFiZWw6IDAsIHNlbnQ6IGZ1bmN0aW9uKCkgeyBpZiAodFswXSAmIDEpIHRocm93IHRbMV07IHJldHVybiB0WzFdOyB9LCB0cnlzOiBbXSwgb3BzOiBbXSB9LCBmLCB5LCB0LCBnID0gT2JqZWN0LmNyZWF0ZSgodHlwZW9mIEl0ZXJhdG9yID09PSBcImZ1bmN0aW9uXCIgPyBJdGVyYXRvciA6IE9iamVjdCkucHJvdG90eXBlKTtcbiAgICByZXR1cm4gZy5uZXh0ID0gdmVyYigwKSwgZ1tcInRocm93XCJdID0gdmVyYigxKSwgZ1tcInJldHVyblwiXSA9IHZlcmIoMiksIHR5cGVvZiBTeW1ib2wgPT09IFwiZnVuY3Rpb25cIiAmJiAoZ1tTeW1ib2wuaXRlcmF0b3JdID0gZnVuY3Rpb24oKSB7IHJldHVybiB0aGlzOyB9KSwgZztcbiAgICBmdW5jdGlvbiB2ZXJiKG4pIHsgcmV0dXJuIGZ1bmN0aW9uICh2KSB7IHJldHVybiBzdGVwKFtuLCB2XSk7IH07IH1cbiAgICBmdW5jdGlvbiBzdGVwKG9wKSB7XG4gICAgICAgIGlmIChmKSB0aHJvdyBuZXcgVHlwZUVycm9yKFwiR2VuZXJhdG9yIGlzIGFscmVhZHkgZXhlY3V0aW5nLlwiKTtcbiAgICAgICAgd2hpbGUgKGcgJiYgKGcgPSAwLCBvcFswXSAmJiAoXyA9IDApKSwgXykgdHJ5IHtcbiAgICAgICAgICAgIGlmIChmID0gMSwgeSAmJiAodCA9IG9wWzBdICYgMiA/IHlbXCJyZXR1cm5cIl0gOiBvcFswXSA/IHlbXCJ0aHJvd1wiXSB8fCAoKHQgPSB5W1wicmV0dXJuXCJdKSAmJiB0LmNhbGwoeSksIDApIDogeS5uZXh0KSAmJiAhKHQgPSB0LmNhbGwoeSwgb3BbMV0pKS5kb25lKSByZXR1cm4gdDtcbiAgICAgICAgICAgIGlmICh5ID0gMCwgdCkgb3AgPSBbb3BbMF0gJiAyLCB0LnZhbHVlXTtcbiAgICAgICAgICAgIHN3aXRjaCAob3BbMF0pIHtcbiAgICAgICAgICAgICAgICBjYXNlIDA6IGNhc2UgMTogdCA9IG9wOyBicmVhaztcbiAgICAgICAgICAgICAgICBjYXNlIDQ6IF8ubGFiZWwrKzsgcmV0dXJuIHsgdmFsdWU6IG9wWzFdLCBkb25lOiBmYWxzZSB9O1xuICAgICAgICAgICAgICAgIGNhc2UgNTogXy5sYWJlbCsrOyB5ID0gb3BbMV07IG9wID0gWzBdOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBjYXNlIDc6IG9wID0gXy5vcHMucG9wKCk7IF8udHJ5cy5wb3AoKTsgY29udGludWU7XG4gICAgICAgICAgICAgICAgZGVmYXVsdDpcbiAgICAgICAgICAgICAgICAgICAgaWYgKCEodCA9IF8udHJ5cywgdCA9IHQubGVuZ3RoID4gMCAmJiB0W3QubGVuZ3RoIC0gMV0pICYmIChvcFswXSA9PT0gNiB8fCBvcFswXSA9PT0gMikpIHsgXyA9IDA7IGNvbnRpbnVlOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmIChvcFswXSA9PT0gMyAmJiAoIXQgfHwgKG9wWzFdID4gdFswXSAmJiBvcFsxXSA8IHRbM10pKSkgeyBfLmxhYmVsID0gb3BbMV07IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmIChvcFswXSA9PT0gNiAmJiBfLmxhYmVsIDwgdFsxXSkgeyBfLmxhYmVsID0gdFsxXTsgdCA9IG9wOyBicmVhazsgfVxuICAgICAgICAgICAgICAgICAgICBpZiAodCAmJiBfLmxhYmVsIDwgdFsyXSkgeyBfLmxhYmVsID0gdFsyXTsgXy5vcHMucHVzaChvcCk7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0WzJdKSBfLm9wcy5wb3AoKTtcbiAgICAgICAgICAgICAgICAgICAgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIG9wID0gYm9keS5jYWxsKHRoaXNBcmcsIF8pO1xuICAgICAgICB9IGNhdGNoIChlKSB7IG9wID0gWzYsIGVdOyB5ID0gMDsgfSBmaW5hbGx5IHsgZiA9IHQgPSAwOyB9XG4gICAgICAgIGlmIChvcFswXSAmIDUpIHRocm93IG9wWzFdOyByZXR1cm4geyB2YWx1ZTogb3BbMF0gPyBvcFsxXSA6IHZvaWQgMCwgZG9uZTogdHJ1ZSB9O1xuICAgIH1cbn07XG5pbXBvcnQgeyBkZWZpbmVDb25maWcgfSBmcm9tICd2aXRlJztcbmltcG9ydCByZWFjdCBmcm9tICdAdml0ZWpzL3BsdWdpbi1yZWFjdCc7XG5pbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IGZpbGVVUkxUb1BhdGggfSBmcm9tICd1cmwnO1xudmFyIF9fZGlybmFtZSA9IHBhdGguZGlybmFtZShmaWxlVVJMVG9QYXRoKGltcG9ydC5tZXRhLnVybCkpO1xuZnVuY3Rpb24gZXhwcmVzc1BsdWdpbigpIHtcbiAgICB2YXIgYXBwO1xuICAgIHJldHVybiB7XG4gICAgICAgIG5hbWU6ICdleHByZXNzLXBsdWdpbicsXG4gICAgICAgIGNvbmZpZ3VyZVNlcnZlcjogZnVuY3Rpb24gKHNlcnZlcikge1xuICAgICAgICAgICAgcmV0dXJuIF9fYXdhaXRlcih0aGlzLCB2b2lkIDAsIHZvaWQgMCwgZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgIHZhciBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgIHJldHVybiBfX2dlbmVyYXRvcih0aGlzLCBmdW5jdGlvbiAoX2EpIHtcbiAgICAgICAgICAgICAgICAgICAgc3dpdGNoIChfYS5sYWJlbCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAwOiByZXR1cm4gWzQgLyp5aWVsZCovLCBpbXBvcnQoJy4vc2VydmVyL2FwcC5qcycpXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgMTpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBleHByZXNzQXBwID0gKF9hLnNlbnQoKSkuZGVmYXVsdDtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAgPSBleHByZXNzQXBwO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNlcnZlci5taWRkbGV3YXJlcy51c2UoJy9hcGknLCBmdW5jdGlvbiAocmVxLCByZXMsIG5leHQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYXBwKHJlcSwgcmVzLCBuZXh0KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gWzIgLypyZXR1cm4qL107XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgIH07XG59XG5leHBvcnQgZGVmYXVsdCBkZWZpbmVDb25maWcoe1xuICAgIHBsdWdpbnM6IFtyZWFjdCgpLCBleHByZXNzUGx1Z2luKCldLFxuICAgIHJlc29sdmU6IHtcbiAgICAgICAgYWxpYXM6IHtcbiAgICAgICAgICAgICdAJzogcGF0aC5yZXNvbHZlKF9fZGlybmFtZSwgJy4vc3JjJyksXG4gICAgICAgIH0sXG4gICAgfSxcbiAgICBzZXJ2ZXI6IHtcbiAgICAgICAgcG9ydDogNTE3MyxcbiAgICB9LFxufSk7XG4iXSwKICAibWFwcGluZ3MiOiAiOzs7Ozs7Ozs7OztBQUE2USxTQUFTLG1CQUFtQjtBQUN6UyxTQUFTLE1BQU0sY0FBYztBQU03QixTQUFTLGlCQUFpQjtBQUN4QixNQUFJLENBQUMsYUFBYTtBQUNoQixVQUFNLFNBQVMsUUFBUSxJQUFJO0FBQzNCLFVBQU0sU0FBUyxRQUFRLElBQUksaUJBQWlCO0FBQzVDLFVBQU0sV0FBVyxRQUFRLElBQUksbUJBQW1CO0FBQ2hELFVBQU0sT0FBTyxRQUFRLElBQUksZUFBZTtBQUV4QyxZQUFRLElBQUkscUNBQXFDO0FBQ2pELFlBQVEsSUFBSSxlQUFlLFFBQVEsNkJBQTZCO0FBQ2hFLFlBQVEsSUFBSSxlQUFlLE1BQU07QUFDakMsWUFBUSxJQUFJLGVBQWUsUUFBUTtBQUNuQyxZQUFRLElBQUksZUFB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LGNBQWMsQ0FBQztBQUFBLEVBQ2xDLFNBQVM7QUFBQSxJQUNMLE9BQU87QUFBQSxNQUNILEtBQUtGLE1BQUssUUFBUUQsWUFBVyxPQUFPO0FBQUEsSUFDeEM7QUFBQSxFQUNKO0FBQUEsRUFDQSxRQUFRO0FBQUEsSUFDSixNQUFNO0FBQUEsRUFDVjtBQUNKLENBQUM7IiwKICAibmFtZXMiOiBbIm1vZGVsIiwgInV1aWR2NCIsICJnbG9iYWxDb2xsZWN0aW9uIiwgInV1aWR2NCIsICJSb3V0ZXIiLCAicGF0aCIsICJ1dWlkdjQiLCAicm91dGVyIiwgIkdvb2dsZUdlbmVyYXRpdmVBSSIsICJnZW5BSSIsICJtb2RlbCIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJSb3V0ZXIiLCAidXVpZHY0IiwgInJvdXRlciIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAiZ2VuQUkiLCAiUFJJTUFSWV9NT0RFTCIsICJtb2RlbCIsICJSb3V0ZXIiLCAicm91dGVyIiwgInBhdGgiLCAiZmlsZVVSTFRvUGF0aCIsICJfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsIiwgIl9fZGlybmFtZSIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiYXBwIl0KfQo=
