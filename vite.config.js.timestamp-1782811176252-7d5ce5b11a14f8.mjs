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
      res.setHeader("Content-Disposition", `inline; filename="${documentId}.pdf"`);
      return fs.createReadStream(uploadPath).pipe(res);
    }
    if (filename) {
      const seedPath = path2.join(seedDir, filename);
      if (fs.existsSync(seedPath)) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", `inline; filename="${filename}"`);
        return fs.createReadStream(seedPath).pipe(res);
      }
      if (fs.existsSync(seedDir)) {
        const allPdfs = fs.readdirSync(seedDir).filter((f) => f.endsWith(".pdf"));
        const match = allPdfs.find((f) => f.includes(path2.parse(filename).name));
        if (match) {
          const matchPath = path2.join(seedDir, match);
          res.setHeader("Content-Type", "application/pdf");
          res.setHeader("Content-Disposition", `inline; filename="${match}"`);
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
    app.post("/api/session/init", async (req, res) => {
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
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsic2VydmVyL3NlcnZpY2VzL2Nocm9tYVNlcnZpY2UuanMiLCAic2VydmVyL3V0aWxzL2Vycm9ycy5qcyIsICJzZXJ2ZXIvc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyIsICJzZXJ2ZXIvYXBpL2hlYWx0aC5qcyIsICJzZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanMiLCAic2VydmVyL3V0aWxzL2NodW5rZXIuanMiLCAic2VydmVyL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvZG9jdW1lbnRzLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzIiwgInNlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvY2hhdC5qcyIsICJzZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzIiwgInNlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzIiwgInNlcnZlci9hcGkvc2VhcmNoLmpzIiwgInNlcnZlci9hcHAuanMiLCAidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvY2hyb21hU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgQ2xvdWRDbGllbnQgfSBmcm9tICdjaHJvbWFkYic7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxubGV0IGNsb3VkQ2xpZW50ID0gbnVsbDtcbmxldCBnbG9iYWxDb2xsZWN0aW9uID0gbnVsbDtcbmNvbnN0IHNlc3Npb25Db2xsZWN0aW9ucyA9IG5ldyBNYXAoKTtcblxuZnVuY3Rpb24gZ2V0Q2xvdWRDbGllbnQoKSB7XG4gIGlmICghY2xvdWRDbGllbnQpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5DSFJPTUFfQVBJX0tFWTtcbiAgICBjb25zdCB0ZW5hbnQgPSBwcm9jZXNzLmVudi5DSFJPTUFfVEVOQU5UIHx8ICdkZWZhdWx0X3RlbmFudCc7XG4gICAgY29uc3QgZGF0YWJhc2UgPSBwcm9jZXNzLmVudi5DSFJPTUFfREFUQUJBU0UgfHwgJ2RlZmF1bHRfZGF0YWJhc2UnO1xuICAgIGNvbnN0IGhvc3QgPSBwcm9jZXNzLmVudi5DSFJPTUFfSE9TVCB8fCB1bmRlZmluZWQ7XG5cbiAgICBjb25zb2xlLmxvZyhcIi0tLS0gQ0hST01BIENPTk5FQ1RJVklUWSBERUJVRyAtLS0tXCIpO1xuICAgIGNvbnNvbGUubG9nKFwiSG9zdDogICAgICBcIiwgaG9zdCB8fCBcImFwaS50cnljaHJvbWEuY29tIChkZWZhdWx0KVwiKTtcbiAgICBjb25zb2xlLmxvZyhcIlRlbmFudDogICAgXCIsIHRlbmFudCk7XG4gICAgY29uc29sZS5sb2coXCJEQiBOYW1lOiAgIFwiLCBkYXRhYmFzZSk7XG4gICAgY29uc29sZS5sb2coXCJBUEkgS2V5OiAgIFwiLCBhcGlLZXkgPyBcIkxPQURFRCAoVkFMSUQpXCIgOiBcIk1JU1NJTkcgKFVOREVGSU5FRClcIik7XG4gICAgY29uc29sZS5sb2coXCItLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVwiKTtcblxuICAgIGlmICghYXBpS2V5KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIFwiQ1JJVElDQUwgRVJST1I6IENIUk9NQV9BUElfS0VZIGlzIHVuZGVmaW5lZC4gXCIgK1xuICAgICAgICBcIkVuc3VyZSB5b3VyIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgY29ycmVjdGx5IGxvYWRlZCBiZWZvcmUgZXhlY3V0aW5nIHRoaXMgZmlsZS5cIlxuICAgICAgKTtcbiAgICB9XG5cbiAgICBjb25zdCBjbGllbnRPcHRpb25zID0geyBhcGlLZXksIHRlbmFudCwgZGF0YWJhc2UgfTtcbiAgICBpZiAoaG9zdCkgY2xpZW50T3B0aW9ucy5ob3N0ID0gaG9zdDtcblxuICAgIGNsb3VkQ2xpZW50ID0gbmV3IENsb3VkQ2xpZW50KGNsaWVudE9wdGlvbnMpO1xuICB9XG4gIHJldHVybiBjbG91ZENsaWVudDtcbn1cblxuZnVuY3Rpb24gZ2V0Q2xpZW50KCkge1xuICByZXR1cm4gZ2V0Q2xvdWRDbGllbnQoKTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldEdsb2JhbENvbGxlY3Rpb24oKSB7XG4gIGlmICghZ2xvYmFsQ29sbGVjdGlvbikge1xuICAgIGNvbnN0IGNsaWVudCA9IGdldENsb3VkQ2xpZW50KCk7XG4gICAgY29uc3QgY29sbGVjdGlvbk5hbWUgPSBwcm9jZXNzLmVudi5DSFJPTUFfR0xPQkFMX0NPTExFQ1RJT04gfHwgJ2Rldic7XG4gICAgdHJ5IHtcbiAgICAgIGdsb2JhbENvbGxlY3Rpb24gPSBhd2FpdCBjbGllbnQuZ2V0T3JDcmVhdGVDb2xsZWN0aW9uKHtcbiAgICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICAgIG1ldGFkYXRhOiB7XG4gICAgICAgICAgZGVzY3JpcHRpb246ICdQZXJtYW5lbnQgc2VlZCBkb2N1bWVudHMgZm9yIFJBRycsXG4gICAgICAgICAgdHlwZTogJ2dsb2JhbF9rbm93bGVkZ2UnXG4gICAgICAgIH0sXG4gICAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgICB9KTtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgR2xvYmFsIGNvbGxlY3Rpb24gcmVhZHk6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBjb25uZWN0IHRvIGdsb2JhbCBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxuICByZXR1cm4gZ2xvYmFsQ29sbGVjdGlvbjtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCkge1xuICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25zLmhhcyhzZXNzaW9uSWQpKSB7XG4gICAgcmV0dXJuIHNlc3Npb25Db2xsZWN0aW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgfVxuICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICBjb25zdCBjb2xsZWN0aW9uTmFtZSA9IGBzZXNzaW9uXyR7c2Vzc2lvbklkfWA7XG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGNsaWVudC5nZXRPckNyZWF0ZUNvbGxlY3Rpb24oe1xuICAgICAgbmFtZTogY29sbGVjdGlvbk5hbWUsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICB0eXBlOiAnc2Vzc2lvbl91cGxvYWQnLFxuICAgICAgICBzZXNzaW9uX2lkOiBzZXNzaW9uSWQsXG4gICAgICAgIGNyZWF0ZWQ6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgICAgfSxcbiAgICAgIGVtYmVkZGluZ0Z1bmN0aW9uOiBudWxsXG4gICAgfSk7XG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLnNldChzZXNzaW9uSWQsIGNvbGxlY3Rpb24pO1xuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIHJlYWR5OiAke2NvbGxlY3Rpb25OYW1lfWApO1xuICAgIHJldHVybiBjb2xsZWN0aW9uO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCB0byBjcmVhdGUgc2Vzc2lvbiBjb2xsZWN0aW9uICR7Y29sbGVjdGlvbk5hbWV9OmAsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IGNvbGxlY3Rpb25OYW1lID0gYHNlc3Npb25fJHtzZXNzaW9uSWR9YDtcbiAgdHJ5IHtcbiAgICBjb25zdCBjbGllbnQgPSBnZXRDbG91ZENsaWVudCgpO1xuICAgIGF3YWl0IGNsaWVudC5kZWxldGVDb2xsZWN0aW9uKHsgbmFtZTogY29sbGVjdGlvbk5hbWUgfSk7XG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiBjb2xsZWN0aW9uIGRlbGV0ZWQ6ICR7Y29sbGVjdGlvbk5hbWV9YCk7XG4gICAgcmV0dXJuIHRydWU7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIGRlbGV0ZSBzZXNzaW9uIGNvbGxlY3Rpb24gJHtjb2xsZWN0aW9uTmFtZX06YCwgZXJyb3IpO1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gYWRkVmVjdG9ycyhjb2xsZWN0aW9uLCB2ZWN0b3JzLCBlbWJlZGRpbmdzLCBpZHMpIHtcbiAgdHJ5IHtcbiAgICBhd2FpdCBjb2xsZWN0aW9uLmFkZCh7XG4gICAgICBpZHMsXG4gICAgICBlbWJlZGRpbmdzLFxuICAgICAgZG9jdW1lbnRzOiB2ZWN0b3JzLm1hcCh2ID0+IHYudGV4dCksXG4gICAgICBtZXRhZGF0YXM6IHZlY3RvcnMubWFwKHYgPT4gdi5tZXRhZGF0YSlcbiAgICB9KTtcbiAgICByZXR1cm4gdHJ1ZTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gYWRkIHZlY3RvcnM6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBxdWVyeUNvbGxlY3Rpb24oY29sbGVjdGlvbiwgcXVlcnlFbWJlZGRpbmcsIHRvcEsgPSA1KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IGNvbGxlY3Rpb24ucXVlcnkoe1xuICAgICAgcXVlcnlFbWJlZGRpbmdzOiBbcXVlcnlFbWJlZGRpbmddLFxuICAgICAgblJlc3VsdHM6IHRvcEssXG4gICAgICBpbmNsdWRlOiBbJ2RvY3VtZW50cycsICdtZXRhZGF0YXMnLCAnZGlzdGFuY2VzJ11cbiAgICB9KTtcblxuICAgIGlmICghcmVzdWx0cy5pZHMgfHwgcmVzdWx0cy5pZHMubGVuZ3RoID09PSAwIHx8IHJlc3VsdHMuaWRzWzBdLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHRzLmlkc1swXS5tYXAoKGlkLCBpZHgpID0+ICh7XG4gICAgICBpZCxcbiAgICAgIHRleHQ6IHJlc3VsdHMuZG9jdW1lbnRzWzBdW2lkeF0sXG4gICAgICBtZXRhZGF0YTogcmVzdWx0cy5tZXRhZGF0YXNbMF1baWR4XSxcbiAgICAgIGRpc3RhbmNlOiByZXN1bHRzLmRpc3RhbmNlc1swXVtpZHhdLFxuICAgICAgc2NvcmU6IDEgLSByZXN1bHRzLmRpc3RhbmNlc1swXVtpZHhdXG4gICAgfSkpO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBxdWVyeSBjb2xsZWN0aW9uOicsIGVycm9yKTtcbiAgICB0aHJvdyBlcnJvcjtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZGVsZXRlRG9jdW1lbnRWZWN0b3JzKGNvbGxlY3Rpb24sIGRvY3VtZW50SWQpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBleGlzdGluZyA9IGF3YWl0IGNvbGxlY3Rpb24uZ2V0KHtcbiAgICAgIHdoZXJlOiB7IGRvY3VtZW50X2lkOiBkb2N1bWVudElkIH1cbiAgICB9KTtcbiAgICBpZiAoZXhpc3RpbmcuaWRzICYmIGV4aXN0aW5nLmlkcy5sZW5ndGggPiAwKSB7XG4gICAgICBhd2FpdCBjb2xsZWN0aW9uLmRlbGV0ZSh7IGlkczogZXhpc3RpbmcuaWRzIH0pO1xuICAgICAgcmV0dXJuIGV4aXN0aW5nLmlkcy5sZW5ndGg7XG4gICAgfVxuICAgIHJldHVybiAwO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQgdmVjdG9yczonLCBlcnJvcik7XG4gICAgdGhyb3cgZXJyb3I7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdldERvY3VtZW50Q291bnQoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIHJldHVybiBhd2FpdCBjb2xsZWN0aW9uLmNvdW50KCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmFpbGVkIHRvIGdldCBkb2N1bWVudCBjb3VudDonLCBlcnJvcik7XG4gICAgcmV0dXJuIDA7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHMoY29sbGVjdGlvbikge1xuICB0cnkge1xuICAgIGNvbnN0IGFsbEl0ZW1zID0gYXdhaXQgY29sbGVjdGlvbi5nZXQoe1xuICAgICAgaW5jbHVkZTogWydtZXRhZGF0YXMnLCAnZG9jdW1lbnRzJ11cbiAgICB9KTtcblxuICAgIGNvbnN0IGRvY3VtZW50c01hcCA9IG5ldyBNYXAoKTtcblxuICAgIGlmIChhbGxJdGVtcy5pZHMpIHtcbiAgICAgIGFsbEl0ZW1zLmlkcy5mb3JFYWNoKChpZCwgaWR4KSA9PiB7XG4gICAgICAgIGNvbnN0IG1ldGEgPSBhbGxJdGVtcy5tZXRhZGF0YXNbaWR4XTtcbiAgICAgICAgY29uc3QgZG9jSWQgPSBtZXRhLmRvY3VtZW50X2lkO1xuXG4gICAgICAgIGlmICghZG9jdW1lbnRzTWFwLmhhcyhkb2NJZCkpIHtcbiAgICAgICAgICBkb2N1bWVudHNNYXAuc2V0KGRvY0lkLCB7XG4gICAgICAgICAgICBkb2N1bWVudF9pZDogZG9jSWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGNodW5rX2NvdW50OiAwLFxuICAgICAgICAgICAgcGFnZV9jb3VudDogbWV0YS5wYWdlX251bWJlciB8fCAxLFxuICAgICAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbWV0YS51cGxvYWRfdGltZXN0YW1wLFxuICAgICAgICAgICAgc291cmNlX3R5cGU6IG1ldGEuc291cmNlX3R5cGUsXG4gICAgICAgICAgICBmaXJzdF9jaHVua190ZXh0OiBhbGxJdGVtcy5kb2N1bWVudHNbaWR4XVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgZG9jID0gZG9jdW1lbnRzTWFwLmdldChkb2NJZCk7XG4gICAgICAgIGRvYy5jaHVua19jb3VudCsrO1xuICAgICAgICBkb2MucGFnZV9jb3VudCA9IE1hdGgubWF4KGRvYy5wYWdlX2NvdW50LCBtZXRhLnBhZ2VfbnVtYmVyIHx8IDEpO1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIEFycmF5LmZyb20oZG9jdW1lbnRzTWFwLnZhbHVlcygpKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHM6JywgZXJyb3IpO1xuICAgIHJldHVybiBbXTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoQ2hlY2soKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBoZWFydGJlYXQgPSBhd2FpdCBjbGllbnQuaGVhcnRiZWF0KCk7XG4gICAgcmV0dXJuIHtcbiAgICAgIHN0YXR1czogJ2hlYWx0aHknLFxuICAgICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgICBoZWFydGJlYXRcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJldHVybiB7XG4gICAgICBzdGF0dXM6ICd1bmhlYWx0aHknLFxuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKVxuICAgIH07XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGNsZWFudXBTZXNzaW9uQ29sbGVjdGlvbnMoKSB7XG4gIHRyeSB7XG4gICAgY29uc3QgY2xpZW50ID0gZ2V0Q2xvdWRDbGllbnQoKTtcbiAgICBjb25zdCBjb2xsZWN0aW9ucyA9IGF3YWl0IGNsaWVudC5saXN0Q29sbGVjdGlvbnMoKTtcblxuICAgIC8vIEZJWCAxOiBjb2xsZWN0aW9ucyBpcyBhbiBhcnJheSBvZiBvYmplY3RzIHdpdGggLm5hbWUsIG5vdCBwbGFpbiBzdHJpbmdzXG4gICAgY29uc3Qgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcyA9IGNvbGxlY3Rpb25zXG4gICAgICAubWFwKGMgPT4gKHR5cGVvZiBjID09PSAnc3RyaW5nJyA/IGMgOiBjLm5hbWUpKVxuICAgICAgLmZpbHRlcihuYW1lID0+IG5hbWUuc3RhcnRzV2l0aCgnc2Vzc2lvbl8nKSk7XG5cbiAgICBpZiAoc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcdTI3MDUgTm8gc3RhbGUgc2Vzc2lvbiBjb2xsZWN0aW9ucyBmb3VuZC4nKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zb2xlLmxvZyhgXHVEODNFXHVEREY5IENsZWFuaW5nIHVwICR7c2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5sZW5ndGh9IHN0YWxlIHNlc3Npb24gY29sbGVjdGlvbihzKS4uLmApO1xuXG4gICAgYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgc2Vzc2lvbkNvbGxlY3Rpb25OYW1lcy5tYXAoYXN5bmMgbmFtZSA9PiB7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgYXdhaXQgY2xpZW50LmRlbGV0ZUNvbGxlY3Rpb24oeyBuYW1lIH0pO1xuICAgICAgICAgIGNvbnNvbGUubG9nKGAgIFx1MjcwNSBEZWxldGVkOiAke25hbWV9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUud2FybihgICBcdTI2QTBcdUZFMEYgQ291bGQgbm90IGRlbGV0ZSAke25hbWV9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICApO1xuXG4gICAgc2Vzc2lvbkNvbGxlY3Rpb25zLmNsZWFyKCk7XG4gICAgY29uc29sZS5sb2coJ1x1MjcwNSBTZXNzaW9uIGNvbGxlY3Rpb24gY2xlYW51cCBjb21wbGV0ZS4nKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLndhcm4oJ1x1MjZBMFx1RkUwRiBTZXNzaW9uIGNsZWFudXAgZmFpbGVkIChub24tZmF0YWwpOicsIGVycm9yLm1lc3NhZ2UpO1xuICB9XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvZXJyb3JzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2Vycm9ycy5qc1wiO2V4cG9ydCBjbGFzcyBBcHBFcnJvciBleHRlbmRzIEVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZSwgY29kZSwgc3RhdHVzQ29kZSA9IDUwMCkge1xuICAgIHN1cGVyKG1lc3NhZ2UpO1xuICAgIHRoaXMuY29kZSA9IGNvZGU7XG4gICAgdGhpcy5zdGF0dXNDb2RlID0gc3RhdHVzQ29kZTtcbiAgICB0aGlzLmlzT3BlcmF0aW9uYWwgPSB0cnVlO1xuICAgIEVycm9yLmNhcHR1cmVTdGFja1RyYWNlKHRoaXMsIHRoaXMuY29uc3RydWN0b3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBWYWxpZGF0aW9uRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKG1lc3NhZ2UsIGNvZGUgPSAnVkFMSURBVElPTl9FUlJPUicpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBVcGxvYWRMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlLCBjb2RlID0gJ1VQTE9BRF9MSU1JVF9FWENFRURFRCcpIHtcbiAgICBzdXBlcihtZXNzYWdlLCBjb2RlLCA0MDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBGaWxlVG9vTGFyZ2VFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4U2l6ZU1CKSB7XG4gICAgc3VwZXIoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgLCAnRklMRV9UT09fTEFSR0UnLCA0MTMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBJbnZhbGlkRmlsZVR5cGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ09ubHkgUERGIGZpbGVzIGFyZSBhbGxvd2VkJywgJ0lOVkFMSURfRklMRV9UWVBFJywgNDE1KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgVG9vTWFueVBERnNFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IobWF4KSB7XG4gICAgc3VwZXIoYE1heGltdW0gJHttYXh9IFBERnMgYWxsb3dlZCBwZXIgc2Vzc2lvbmAsICdUT09fTUFOWV9QREZTJywgNDAwKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgRHVwbGljYXRlRmlsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihmaWxlbmFtZSkge1xuICAgIHN1cGVyKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gLCAnRFVQTElDQVRFX0ZJTEUnLCA0MDkpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3JydXB0ZWRQREZFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0ZhaWxlZCB0byBwYXJzZSBQREYgZmlsZS4gSXQgbWF5IGJlIGNvcnJ1cHRlZC4nLCAnQ09SUlVQVEVEX1BERicsIDQyMik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFJhdGVMaW1pdEVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihyZXRyeUFmdGVyID0gNjApIHtcbiAgICBzdXBlcignUmF0ZSBsaW1pdCBleGNlZWRlZC4gUGxlYXNlIHRyeSBhZ2FpbiBsYXRlci4nLCAnUkFURV9MSU1JVF9FWENFRURFRCcsIDQyOSk7XG4gICAgdGhpcy5yZXRyeUFmdGVyID0gcmV0cnlBZnRlcjtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTExNVW5hdmFpbGFibGVFcnJvciBleHRlbmRzIEFwcEVycm9yIHtcbiAgY29uc3RydWN0b3IoKSB7XG4gICAgc3VwZXIoJ0FJIHNlcnZpY2UgaXMgdGVtcG9yYXJpbHkgdW5hdmFpbGFibGUuIFBsZWFzZSB0cnkgYWdhaW4uJywgJ0xMTV9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIEVtYmVkZGluZ0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlID0gJ0ZhaWxlZCB0byBnZW5lcmF0ZSBlbWJlZGRpbmdzJykge1xuICAgIHN1cGVyKG1lc3NhZ2UsICdFTUJFRERJTkdfRVJST1InLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBSZXRyaWV2YWxVbmF2YWlsYWJsZUVycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignRG9jdW1lbnQgcmV0cmlldmFsIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1JFVFJJRVZBTF9VTkFWQUlMQUJMRScsIDUwMyk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgZXh0ZW5kcyBBcHBFcnJvciB7XG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCdXZWIgc2VhcmNoIGlzIHRlbXBvcmFyaWx5IHVuYXZhaWxhYmxlJywgJ1dFQl9TRUFSQ0hfVU5BVkFJTEFCTEUnLCA1MDMpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDb3ZlcmFnZVRvb0xvd0Vycm9yIGV4dGVuZHMgQXBwRXJyb3Ige1xuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcignSW5zdWZmaWNpZW50IGluZm9ybWF0aW9uIGluIGtub3dsZWRnZSBiYXNlJywgJ0NPVkVSQUdFX1RPT19MT1cnLCAyMDApO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1JldHJ5YWJsZUVycm9yKGVycm9yKSB7XG4gIGNvbnN0IHJldHJ5YWJsZUNvZGVzID0gWydSQVRFX0xJTUlUX0VYQ0VFREVEJywgJ0VNQkVERElOR19FUlJPUicsICdMTE1fVU5BVkFJTEFCTEUnXTtcbiAgcmV0dXJuIHJldHJ5YWJsZUNvZGVzLmluY2x1ZGVzKGVycm9yLmNvZGUpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXM0MjlFcnJvcihlcnJvcikge1xuICByZXR1cm4gZXJyb3I/LmNvZGUgPT09IDQyOSB8fFxuICAgICAgICAgZXJyb3I/LnN0YXR1cyA9PT0gNDI5IHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJzQyOScpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1JFU09VUkNFX0VYSEFVU1RFRCcpIHx8XG4gICAgICAgICBlcnJvcj8ubWVzc2FnZT8uaW5jbHVkZXMoJ1RvbyBNYW55IFJlcXVlc3RzJyk7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2VtYmVkZGluZ1NlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgRW1iZWRkaW5nRXJyb3IsIGlzNDI5RXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5sZXQgZ2VuQUkgPSBudWxsO1xubGV0IGVtYmVkZGluZ01vZGVsID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0RW1iZWRkaW5nTW9kZWwoKSB7XG4gIGlmICghZW1iZWRkaW5nTW9kZWwpIHtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkocHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkpO1xuICAgIGVtYmVkZGluZ01vZGVsID0gZ2VuQUkuZ2V0R2VuZXJhdGl2ZU1vZGVsKHtcbiAgICAgIG1vZGVsOiBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSdcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gZW1iZWRkaW5nTW9kZWw7XG59XG5cbmNvbnN0IEJBVENIX1NJWkUgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5FTUJFRERJTkdfQkFUQ0hfTUFYX0NIVU5LUykgfHwgNztcbmNvbnN0IFBBUkFMTEVMX0NBTExTID0gKCkgPT4gcGFyc2VJbnQocHJvY2Vzcy5lbnYuRU1CRURESU5HX1BBUkFMTEVMX0NBTExTKSB8fCA0O1xuY29uc3QgT1VUUFVUX0RJTUVOU0lPTlMgPSAoKSA9PiBwYXJzZUludChwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX0RJTUVOU0lPTlMpIHx8IDMwNzI7XG5jb25zdCBHUk9VUF9XQUlUX01TID0gNjEwMDA7XG5jb25zdCBSRVRSWV9XQUlUX01TID0gMTUwMDA7IC8vIEZJWCAzOiB3YWl0IGJlZm9yZSBpbmRpdmlkdWFsIGNodW5rIHJldHJpZXNcblxuYXN5bmMgZnVuY3Rpb24gZW1iZWRCYXRjaCh0ZXh0cywgdGFza1R5cGUgPSAnUkVUUklFVkFMX0RPQ1VNRU5UJywgYXR0ZW1wdCA9IDEpIHtcbiAgY29uc3QgbWF4QXR0ZW1wdHMgPSA1O1xuICBjb25zdCBtb2RlbE5hbWUgPSBwcm9jZXNzLmVudi5HRU1JTklfRU1CRURESU5HX01PREVMIHx8ICdnZW1pbmktZW1iZWRkaW5nLTAwMSc7XG5cbiAgdHJ5IHtcbiAgICBjb25zdCBtb2RlbCA9IGdldEVtYmVkZGluZ01vZGVsKCk7XG5cbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBtb2RlbC5iYXRjaEVtYmVkQ29udGVudHMoe1xuICAgICAgcmVxdWVzdHM6IHRleHRzLm1hcCh0ZXh0ID0+ICh7XG4gICAgICAgIG1vZGVsOiBgbW9kZWxzLyR7bW9kZWxOYW1lfWAsXG4gICAgICAgIGNvbnRlbnQ6IHsgcGFydHM6IFt7IHRleHQgfV0gfSxcbiAgICAgICAgdGFza1R5cGUsXG4gICAgICAgIG91dHB1dERpbWVuc2lvbmFsaXR5OiBPVVRQVVRfRElNRU5TSU9OUygpXG4gICAgICB9KSlcbiAgICB9KTtcblxuICAgIGlmICghcmVzdWx0Py5lbWJlZGRpbmdzIHx8IHJlc3VsdC5lbWJlZGRpbmdzLmxlbmd0aCAhPT0gdGV4dHMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRW1iZWRkaW5nRXJyb3IoYEV4cGVjdGVkICR7dGV4dHMubGVuZ3RofSBlbWJlZGRpbmdzLCBnb3QgJHtyZXN1bHQ/LmVtYmVkZGluZ3M/Lmxlbmd0aCA/PyAwfWApO1xuICAgIH1cblxuICAgIHJldHVybiByZXN1bHQuZW1iZWRkaW5ncy5tYXAoZSA9PiB7XG4gICAgICBpZiAoIWU/LnZhbHVlcykgdGhyb3cgbmV3IEVtYmVkZGluZ0Vycm9yKCdNaXNzaW5nIHZhbHVlcyBpbiBlbWJlZGRpbmcgcmVzcG9uc2UnKTtcbiAgICAgIHJldHVybiBlLnZhbHVlcztcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnN0IGlzNDI5ID0gaXM0MjlFcnJvcihlcnJvcikgfHxcbiAgICAgIGVycm9yPy5zdGF0dXMgPT09IDQyOSB8fFxuICAgICAgZXJyb3I/Lm1lc3NhZ2U/LmluY2x1ZGVzKCdSRVNPVVJDRV9FWEhBVVNURUQnKTtcblxuICAgIGlmIChpczQyOSAmJiBhdHRlbXB0IDwgbWF4QXR0ZW1wdHMpIHtcbiAgICAgIGNvbnN0IHJldHJ5RGVsYXkgPSBlcnJvci5yZXRyeUFmdGVyIHx8IEdST1VQX1dBSVRfTVM7XG4gICAgICBjb25zb2xlLmxvZyhgUmF0ZSBsaW1pdGVkLCB3YWl0aW5nICR7cmV0cnlEZWxheSAvIDEwMDB9cyAoYXR0ZW1wdCAke2F0dGVtcHR9LyR7bWF4QXR0ZW1wdHN9KWApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIHJldHJ5RGVsYXkpKTtcbiAgICAgIHJldHVybiBlbWJlZEJhdGNoKHRleHRzLCB0YXNrVHlwZSwgYXR0ZW1wdCArIDEpO1xuICAgIH1cblxuICAgIHRocm93IG5ldyBFbWJlZGRpbmdFcnJvcihlcnJvci5tZXNzYWdlIHx8ICdCYXRjaCBlbWJlZGRpbmcgZmFpbGVkJyk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlRW1iZWRkaW5ncyhjaHVua3MsIHRhc2tUeXBlID0gJ1JFVFJJRVZBTF9ET0NVTUVOVCcsIG9uUHJvZ3Jlc3MpIHtcbiAgaWYgKCFjaHVua3MgfHwgY2h1bmtzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIFtdO1xuXG4gIGNvbnN0IGJhdGNoU2l6ZSA9IEJBVENIX1NJWkUoKTtcbiAgY29uc3QgcGFyYWxsZWxDYWxscyA9IFBBUkFMTEVMX0NBTExTKCk7XG4gIGNvbnN0IGVtYmVkZGluZ3MgPSBbXTtcblxuICBjb25zdCBiYXRjaGVzID0gW107XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY2h1bmtzLmxlbmd0aDsgaSArPSBiYXRjaFNpemUpIHtcbiAgICBiYXRjaGVzLnB1c2goY2h1bmtzLnNsaWNlKGksIGkgKyBiYXRjaFNpemUpKTtcbiAgfVxuXG4gIGNvbnN0IHRvdGFsR3JvdXBzID0gTWF0aC5jZWlsKGJhdGNoZXMubGVuZ3RoIC8gcGFyYWxsZWxDYWxscyk7XG5cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBiYXRjaGVzLmxlbmd0aDsgaSArPSBwYXJhbGxlbENhbGxzKSB7XG4gICAgY29uc3QgcGFyYWxsZWxCYXRjaGVzID0gYmF0Y2hlcy5zbGljZShpLCBpICsgcGFyYWxsZWxDYWxscyk7XG4gICAgY29uc3QgZ3JvdXBOdW0gPSBNYXRoLmZsb29yKGkgLyBwYXJhbGxlbENhbGxzKSArIDE7XG4gICAgY29uc3QgY2h1bmtzQ292ZXJlZCA9IE1hdGgubWluKChpICsgcGFyYWxsZWxDYWxscykgKiBiYXRjaFNpemUsIGNodW5rcy5sZW5ndGgpO1xuXG4gICAgY29uc29sZS5sb2coYCAgRW1iZWRkaW5nIGdyb3VwICR7Z3JvdXBOdW19LyR7dG90YWxHcm91cHN9IFx1MjAxNCAke3BhcmFsbGVsQmF0Y2hlcy5sZW5ndGh9IGJhdGNoIGNhbGwocykgaW4gcGFyYWxsZWwgKGNodW5rcyAke2kgKiBiYXRjaFNpemUgKyAxfVx1MjAxMyR7Y2h1bmtzQ292ZXJlZH0pLi4uYCk7XG5cbiAgICBjb25zdCByZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGxTZXR0bGVkKFxuICAgICAgcGFyYWxsZWxCYXRjaGVzLm1hcChiYXRjaCA9PiBlbWJlZEJhdGNoKGJhdGNoLm1hcChjID0+IGMudGV4dCksIHRhc2tUeXBlKSlcbiAgICApO1xuXG4gICAgY29uc3QgZmFpbGVkQmF0Y2hlcyA9IFtdO1xuICAgIHJlc3VsdHMuZm9yRWFjaCgocmVzdWx0LCBiYXRjaElkeCkgPT4ge1xuICAgICAgY29uc3QgYmF0Y2ggPSBwYXJhbGxlbEJhdGNoZXNbYmF0Y2hJZHhdO1xuICAgICAgaWYgKHJlc3VsdC5zdGF0dXMgPT09ICdmdWxmaWxsZWQnKSB7XG4gICAgICAgIGNvbnN0IHZlY3RvcnMgPSByZXN1bHQudmFsdWU7XG4gICAgICAgIGJhdGNoLmZvckVhY2goKGNodW5rLCBjaHVua0lkeCkgPT4ge1xuICAgICAgICAgIC8vIEZJWCAyOiBjb3JyZWN0IGZhbGxiYWNrIGNodW5rIElEIFx1MjAxNCAoaSArIGJhdGNoSWR4KSBpcyB0aGUgYWJzb2x1dGUgYmF0Y2ggaW5kZXhcbiAgICAgICAgICBjb25zdCBhYnNvbHV0ZUNodW5rSWR4ID0gKGkgKyBiYXRjaElkeCkgKiBiYXRjaFNpemUgKyBjaHVua0lkeDtcbiAgICAgICAgICBlbWJlZGRpbmdzLnB1c2goe1xuICAgICAgICAgICAgaWQ6IGNodW5rLm1ldGFkYXRhPy5jaHVua19pZCB8fCBgY2h1bmtfJHthYnNvbHV0ZUNodW5rSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbY2h1bmtJZHhdLFxuICAgICAgICAgICAgbWV0YWRhdGE6IGNodW5rLm1ldGFkYXRhLFxuICAgICAgICAgICAgdGV4dDogY2h1bmsudGV4dFxuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGNvbnNvbGUud2FybihgICBCYXRjaCAke2kgKyBiYXRjaElkeH0gZmFpbGVkLCB3aWxsIHJldHJ5IGluZGl2aWR1YWxseTpgLCByZXN1bHQucmVhc29uPy5tZXNzYWdlKTtcbiAgICAgICAgZmFpbGVkQmF0Y2hlcy5wdXNoKHsgYmF0Y2gsIGJhdGNoSWR4OiBpICsgYmF0Y2hJZHggfSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAob25Qcm9ncmVzcykge1xuICAgICAgb25Qcm9ncmVzcyh7IGN1cnJlbnRfYmF0Y2g6IGdyb3VwTnVtLCB0b3RhbF9iYXRjaGVzOiB0b3RhbEdyb3VwcyB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBpc0xhc3RHcm91cCA9IGkgKyBwYXJhbGxlbENhbGxzID49IGJhdGNoZXMubGVuZ3RoO1xuICAgIGlmICghaXNMYXN0R3JvdXAgfHwgZmFpbGVkQmF0Y2hlcy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7R1JPVVBfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgbmV4dCBncm91cC4uLmApO1xuICAgICAgYXdhaXQgbmV3IFByb21pc2UocmVzb2x2ZSA9PiBzZXRUaW1lb3V0KHJlc29sdmUsIEdST1VQX1dBSVRfTVMpKTtcbiAgICB9XG5cbiAgICAvLyBGSVggMzogd2FpdCBiZWZvcmUgcmV0cnlpbmcgaW5kaXZpZHVhbCBjaHVua3MgdG8gYXZvaWQgaW1tZWRpYXRlIDQyOVxuICAgIGZvciAoY29uc3QgeyBiYXRjaCwgYmF0Y2hJZHggfSBvZiBmYWlsZWRCYXRjaGVzKSB7XG4gICAgICBjb25zb2xlLmxvZyhgICBXYWl0aW5nICR7UkVUUllfV0FJVF9NUyAvIDEwMDB9cyBiZWZvcmUgcmV0cnlpbmcgZmFpbGVkIGJhdGNoICR7YmF0Y2hJZHh9Li4uYCk7XG4gICAgICBhd2FpdCBuZXcgUHJvbWlzZShyZXNvbHZlID0+IHNldFRpbWVvdXQocmVzb2x2ZSwgUkVUUllfV0FJVF9NUykpO1xuICAgICAgZm9yIChjb25zdCBjaHVuayBvZiBiYXRjaCkge1xuICAgICAgICB0cnkge1xuICAgICAgICAgIGNvbnN0IHZlY3RvcnMgPSBhd2FpdCBlbWJlZEJhdGNoKFtjaHVuay50ZXh0XSwgdGFza1R5cGUpO1xuICAgICAgICAgIGVtYmVkZGluZ3MucHVzaCh7XG4gICAgICAgICAgICBpZDogY2h1bmsubWV0YWRhdGE/LmNodW5rX2lkIHx8IGBjaHVua19yZXRyeV8ke2JhdGNoSWR4fWAsXG4gICAgICAgICAgICBlbWJlZGRpbmc6IHZlY3RvcnNbMF0sXG4gICAgICAgICAgICBtZXRhZGF0YTogY2h1bmsubWV0YWRhdGEsXG4gICAgICAgICAgICB0ZXh0OiBjaHVuay50ZXh0XG4gICAgICAgICAgfSk7XG4gICAgICAgICAgY29uc29sZS5sb2coYCAgXHUyNzA1IFJldHJ5IHN1Y2NlZWRlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9YCk7XG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIGNvbnNvbGUuZXJyb3IoYCAgXHUyNzRDIFJldHJ5IGZhaWxlZCBmb3IgY2h1bmsgJHtjaHVuay5tZXRhZGF0YT8uY2h1bmtfaWR9OmAsIGVyci5tZXNzYWdlKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWJlZGRpbmdzO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRRdWVyeShxdWVyeSkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbcXVlcnldLCAnUkVUUklFVkFMX1FVRVJZJyk7XG4gIHJldHVybiB2ZWN0b3JzWzBdO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZW1iZWRTaW5nbGUodGV4dCkge1xuICBjb25zdCB2ZWN0b3JzID0gYXdhaXQgZW1iZWRCYXRjaChbdGV4dF0sICdSRVRSSUVWQUxfRE9DVU1FTlQnKTtcbiAgcmV0dXJuIHZlY3RvcnNbMF07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSYXRlTGltaXRTdGF0ZSgpIHtcbiAgcmV0dXJuIHtcbiAgICBtYXhUb2tlbnNQZXJNaW51dGU6IHBhcnNlSW50KHByb2Nlc3MuZW52LkVNQkVERElOR19SQVRFX0xJTUlUX1RPS0VOU19QRVJfTUlOVVRFKSB8fCAzMDAwMCxcbiAgICBwYXJhbGxlbENhbGxzOiBQQVJBTExFTF9DQUxMUygpLFxuICAgIG1heENodW5rc1BlckNhbGw6IEJBVENIX1NJWkUoKSxcbiAgICBvdXRwdXREaW1lbnNpb25zOiBPVVRQVVRfRElNRU5TSU9OUygpXG4gIH07XG59IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGlcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9oZWFsdGguanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2hlYWx0aC5qc1wiO2ltcG9ydCB7IFJvdXRlciB9IGZyb20gJ2V4cHJlc3MnO1xuaW1wb3J0IHsgaGVhbHRoQ2hlY2sgYXMgY2hyb21hSGVhbHRoQ2hlY2sgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldFJhdGVMaW1pdFN0YXRlIH0gZnJvbSAnLi4vc2VydmljZXMvZW1iZWRkaW5nU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGVhbHRoKHJlcSwgcmVzKSB7XG4gIGNvbnN0IGhlYWx0aFN0YXR1cyA9IHtcbiAgICBzdGF0dXM6ICdvaycsXG4gICAgdGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKCksXG4gICAgc2VydmljZXM6IHt9XG4gIH07XG5cbiAgLy8gQ2hlY2sgQ2hyb21hREJcbiAgdHJ5IHtcbiAgICBjb25zdCBjaHJvbWFIZWFsdGggPSBhd2FpdCBjaHJvbWFIZWFsdGhDaGVjaygpO1xuICAgIGhlYWx0aFN0YXR1cy5zZXJ2aWNlcy5jaHJvbWFkYiA9IGNocm9tYUhlYWx0aDtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBoZWFsdGhTdGF0dXMuc2VydmljZXMuY2hyb21hZGIgPSB7XG4gICAgICBzdGF0dXM6ICdlcnJvcicsXG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZVxuICAgIH07XG4gIH1cblxuICAvLyBDaGVjayBHZW1pbmkgKHZpYSBBUEkga2V5IHByZXNlbmNlKVxuICBoZWFsdGhTdGF0dXMuc2VydmljZXMuZ2VtaW5pID0ge1xuICAgIHN0YXR1czogcHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkgPyAnY29uZmlndXJlZCcgOiAnbm90X2NvbmZpZ3VyZWQnXG4gIH07XG5cbiAgLy8gR2V0IHJhdGUgbGltaXQgc3RhdGVcbiAgaGVhbHRoU3RhdHVzLnJhdGVMaW1pdCA9IGdldFJhdGVMaW1pdFN0YXRlKCk7XG5cbiAgLy8gT3ZlcmFsbCBzdGF0dXNcbiAgY29uc3QgaGFzRXJyb3JzID0gT2JqZWN0LnZhbHVlcyhoZWFsdGhTdGF0dXMuc2VydmljZXMpLnNvbWUoXG4gICAgcyA9PiBzLnN0YXR1cyA9PT0gJ2Vycm9yJyB8fCBzLnN0YXR1cyA9PT0gJ3VuaGVhbHRoeSdcbiAgKTtcblxuICBpZiAoaGFzRXJyb3JzKSB7XG4gICAgaGVhbHRoU3RhdHVzLnN0YXR1cyA9ICdkZWdyYWRlZCc7XG4gIH1cblxuICByZXMuanNvbihoZWFsdGhTdGF0dXMpO1xufVxuXG5yb3V0ZXIuZ2V0KCcvJywgaGVhbHRoKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlsc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvdXRpbHMvc2FuaXRpemUuanNcIjtpbXBvcnQgcGF0aCBmcm9tICdwYXRoJztcbmltcG9ydCB7IFZhbGlkYXRpb25FcnJvciB9IGZyb20gJy4vZXJyb3JzLmpzJztcblxuY29uc3QgREFOR0VST1VTX1BBVFRFUk5TID0gL1s8PjpcInw/KlxceDAwLVxceDFmXS9nO1xuY29uc3QgUEFUSF9UUkFWRVJTQUwgPSAvXFwuXFwuL2c7XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZUZpbGVuYW1lKGZpbGVuYW1lKSB7XG4gIGlmICghZmlsZW5hbWUgfHwgdHlwZW9mIGZpbGVuYW1lICE9PSAnc3RyaW5nJykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZmlsZW5hbWUnKTtcbiAgfVxuXG4gIC8vIFJlbW92ZSBwYXRoIGNvbXBvbmVudHMgYW5kIGdldCBiYXNlbmFtZVxuICBjb25zdCBiYXNlbmFtZSA9IHBhdGguYmFzZW5hbWUoZmlsZW5hbWUpO1xuXG4gIC8vIFJlbW92ZSBkYW5nZXJvdXMgY2hhcmFjdGVyc1xuICBsZXQgc2FuaXRpemVkID0gYmFzZW5hbWUucmVwbGFjZShEQU5HRVJPVVNfUEFUVEVSTlMsICdfJyk7XG5cbiAgLy8gUmVtb3ZlIHBhdGggdHJhdmVyc2FsIGF0dGVtcHRzXG4gIHNhbml0aXplZCA9IHNhbml0aXplZC5yZXBsYWNlKFBBVEhfVFJBVkVSU0FMLCAnJyk7XG5cbiAgLy8gVHJpbSB3aGl0ZXNwYWNlIGFuZCBsaW1pdCBsZW5ndGhcbiAgc2FuaXRpemVkID0gc2FuaXRpemVkLnRyaW0oKS5zbGljZSgwLCAyNTUpO1xuXG4gIGlmICghc2FuaXRpemVkKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBmaWxlbmFtZSBhZnRlciBzYW5pdGl6YXRpb24nKTtcbiAgfVxuXG4gIHJldHVybiBzYW5pdGl6ZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZVBERkZpbGUoZmlsZSkge1xuICBpZiAoIWZpbGUpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdObyBmaWxlIHByb3ZpZGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBNSU1FIHR5cGVcbiAgY29uc3QgdmFsaWRNaW1lVHlwZXMgPSBbJ2FwcGxpY2F0aW9uL3BkZiddO1xuICBpZiAoIXZhbGlkTWltZVR5cGVzLmluY2x1ZGVzKGZpbGUubWltZXR5cGUpKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignT25seSBQREYgZmlsZXMgYXJlIGFjY2VwdGVkJyk7XG4gIH1cblxuICAvLyBDaGVjayBleHRlbnNpb25cbiAgY29uc3QgZXh0ID0gcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lIHx8ICcnKS50b0xvd2VyQ2FzZSgpO1xuICBpZiAoZXh0ICE9PSAnLnBkZicpIHtcbiAgICB0aHJvdyBuZXcgVmFsaWRhdGlvbkVycm9yKCdGaWxlIG11c3QgaGF2ZSAucGRmIGV4dGVuc2lvbicpO1xuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZUZpbGVTaXplKHNpemVCeXRlcywgbWF4U2l6ZU1CKSB7XG4gIGNvbnN0IG1heEJ5dGVzID0gbWF4U2l6ZU1CICogMTAyNCAqIDEwMjQ7XG4gIGlmIChzaXplQnl0ZXMgPiBtYXhCeXRlcykge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoYEZpbGUgZXhjZWVkcyBtYXhpbXVtIHNpemUgb2YgJHttYXhTaXplTUJ9TUJgKTtcbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSW5wdXQoaW5wdXQsIG1heExlbmd0aCA9IDEwMDAwKSB7XG4gIGlmICghaW5wdXQgfHwgdHlwZW9mIGlucHV0ICE9PSAnc3RyaW5nJykge1xuICAgIHJldHVybiAnJztcbiAgfVxuICByZXR1cm4gaW5wdXQudHJpbSgpLnNsaWNlKDAsIG1heExlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2YWxpZGF0ZURvY3VtZW50SWQoaWQpIHtcbiAgaWYgKCFpZCB8fCB0eXBlb2YgaWQgIT09ICdzdHJpbmcnKSB7XG4gICAgdGhyb3cgbmV3IFZhbGlkYXRpb25FcnJvcignSW52YWxpZCBkb2N1bWVudCBJRCcpO1xuICB9XG4gIGNvbnN0IHV1aWRSZWdleCA9IC9eWzAtOWEtZl17OH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17NH0tWzAtOWEtZl17MTJ9JC9pO1xuICBpZiAoIXV1aWRSZWdleC50ZXN0KGlkKSkge1xuICAgIHRocm93IG5ldyBWYWxpZGF0aW9uRXJyb3IoJ0ludmFsaWQgZG9jdW1lbnQgSUQgZm9ybWF0Jyk7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0VGV4dEZyb21QREZCdWZmZXIoYnVmZmVyKSB7XG4gIC8vIFRoaXMgd2lsbCBiZSB1c2VkIHdpdGggcGRmLXBhcnNlXG4gIHJldHVybiBidWZmZXI7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci91dGlscy9jaHVua2VyLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3V0aWxzL2NodW5rZXIuanNcIjtpbXBvcnQgeyBjcmVhdGVIYXNoIH0gZnJvbSAnY3J5cHRvJztcblxuY29uc3QgQ0hBUlNfUEVSX1RPS0VOID0gNDtcbmNvbnN0IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlMgPSAxMDAwO1xuY29uc3QgREVGQVVMVF9PVkVSTEFQX1RPS0VOUyA9IDIwMDtcbmNvbnN0IE1JTl9DSFVOS19DSEFSUyA9IDEwMDtcblxuZXhwb3J0IGZ1bmN0aW9uIGVzdGltYXRlVG9rZW5zKHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuIDA7XG4gIHJldHVybiBNYXRoLmNlaWwodGV4dC5sZW5ndGggLyBDSEFSU19QRVJfVE9LRU4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYW5UZXh0KHRleHQpIHtcbiAgaWYgKCF0ZXh0IHx8IHR5cGVvZiB0ZXh0ICE9PSAnc3RyaW5nJykgcmV0dXJuICcnO1xuICByZXR1cm4gdGV4dFxuICAgIC5yZXBsYWNlKC9cXGYvZywgJ1xcbicpXG4gICAgLnJlcGxhY2UoLyhcXHMqXFxuKXszLH0vZywgJ1xcblxcbicpXG4gICAgLnJlcGxhY2UoL15cXHMqXFxkK1xccyokL2dtLCAnJylcbiAgICAucmVwbGFjZSgvWyBcXHRdezIsfS9nLCAnICcpXG4gICAgLnRyaW0oKTtcbn1cblxuZnVuY3Rpb24gZ2VuZXJhdGVDaHVua0lkKHRleHQsIGZpbGVuYW1lKSB7XG4gIHJldHVybiBjcmVhdGVIYXNoKCdtZDUnKVxuICAgIC51cGRhdGUoYCR7ZmlsZW5hbWV9Ojoke3RleHR9YClcbiAgICAuZGlnZXN0KCdoZXgnKVxuICAgIC5zbGljZSgwLCAxNik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaHVua1RleHQodGV4dCwgb3B0aW9ucyA9IHt9KSB7XG4gIGNvbnN0IGNodW5rU2l6ZVRva2VucyA9IG9wdGlvbnMuY2h1bmtTaXplVG9rZW5zIHx8IERFRkFVTFRfQ0hVTktfU0laRV9UT0tFTlM7XG4gIGNvbnN0IG92ZXJsYXBUb2tlbnMgPSBvcHRpb25zLm92ZXJsYXBUb2tlbnMgfHwgREVGQVVMVF9PVkVSTEFQX1RPS0VOUztcblxuICBpZiAoIXRleHQgfHwgdHlwZW9mIHRleHQgIT09ICdzdHJpbmcnKSByZXR1cm4gW107XG5cbiAgY29uc3QgY2h1bmtTaXplQ2hhcnMgPSBjaHVua1NpemVUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG4gIGNvbnN0IG92ZXJsYXBDaGFycyA9IG92ZXJsYXBUb2tlbnMgKiBDSEFSU19QRVJfVE9LRU47XG5cbiAgY29uc3QgY2h1bmtzID0gW107XG4gIGxldCBzdGFydCA9IDA7XG4gIGxldCBjaHVua0luZGV4ID0gMDtcblxuICB3aGlsZSAoc3RhcnQgPCB0ZXh0Lmxlbmd0aCkge1xuICAgIGxldCBlbmQgPSBzdGFydCArIGNodW5rU2l6ZUNoYXJzO1xuXG4gICAgaWYgKGVuZCA8IHRleHQubGVuZ3RoKSB7XG4gICAgICBjb25zdCBicmVha1BvaW50cyA9IFsnLiAnLCAnLlxcbicsICchICcsICc/ICcsICdcXG5cXG4nLCAnXFxuJywgJyAnXTtcbiAgICAgIGNvbnN0IHNlYXJjaFN0YXJ0ID0gZW5kIC0gTWF0aC5mbG9vcihjaHVua1NpemVDaGFycyAqIDAuMik7XG5cbiAgICAgIGZvciAoY29uc3QgYnJlYWtwb2ludCBvZiBicmVha1BvaW50cykge1xuICAgICAgICBjb25zdCBpZHggPSB0ZXh0Lmxhc3RJbmRleE9mKGJyZWFrcG9pbnQsIGVuZCk7XG4gICAgICAgIGlmIChpZHggPiBzZWFyY2hTdGFydCAmJiBpZHggPiBzdGFydCkge1xuICAgICAgICAgIGVuZCA9IGlkeCArIGJyZWFrcG9pbnQubGVuZ3RoO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgZW5kID0gTWF0aC5taW4oZW5kLCB0ZXh0Lmxlbmd0aCk7XG4gICAgY29uc3QgY2h1bmtDb250ZW50ID0gdGV4dC5zbGljZShzdGFydCwgZW5kKS50cmltKCk7XG5cbiAgICBpZiAoY2h1bmtDb250ZW50Lmxlbmd0aCA+PSBNSU5fQ0hVTktfQ0hBUlMpIHtcbiAgICAgIGNodW5rcy5wdXNoKHtcbiAgICAgICAgdGV4dDogY2h1bmtDb250ZW50LFxuICAgICAgICB0b2tlbkNvdW50OiBlc3RpbWF0ZVRva2VucyhjaHVua0NvbnRlbnQpLFxuICAgICAgICBjaGFyU3RhcnQ6IHN0YXJ0LFxuICAgICAgICBjaGFyRW5kOiBlbmQsXG4gICAgICAgIGNodW5rSW5kZXg6IGNodW5rSW5kZXgrK1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgbmV4dFN0YXJ0ID0gZW5kIC0gb3ZlcmxhcENoYXJzO1xuICAgIHN0YXJ0ID0gbmV4dFN0YXJ0ID4gc3RhcnQgPyBuZXh0U3RhcnQgOiBlbmQ7XG5cbiAgICBpZiAoY2h1bmtJbmRleCA+IDEwMDAwKSB7XG4gICAgICBjb25zb2xlLndhcm4oJ0NodW5rIGxpbWl0IHJlYWNoZWQsIHN0b3BwaW5nJyk7XG4gICAgICBicmVhaztcbiAgICB9XG4gIH1cblxuICByZXR1cm4gY2h1bmtzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2h1bmtQREZDb250ZW50KHBkZkRhdGEsIG9wdGlvbnMgPSB7fSkge1xuICBjb25zdCB7IGZpbGVuYW1lLCBkb2N1bWVudElkLCBwYWdlTnVtYmVyLCB0ZXh0LCB0b3RhbFBhZ2VzIH0gPSBwZGZEYXRhO1xuXG4gIGlmICghdGV4dCB8fCB0ZXh0LnRyaW0oKS5sZW5ndGggPCA1MCkge1xuICAgIGNvbnNvbGUud2FybihgXHUyNkEwXHVGRTBGICAke2ZpbGVuYW1lfSBwYWdlICR7cGFnZU51bWJlcn06IGV4dHJhY3RlZCB0ZXh0IHRvbyBzaG9ydCBcdTIwMTQgbWF5IGJlIGEgc2Nhbm5lZCBwYWdlLCBza2lwcGluZ2ApO1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGNvbnN0IGNsZWFuZWRUZXh0ID0gY2xlYW5UZXh0KHRleHQpO1xuICBjb25zdCB0ZXh0Q2h1bmtzID0gY2h1bmtUZXh0KGNsZWFuZWRUZXh0LCBvcHRpb25zKTtcbiAgY29uc3QgdG90YWxDaHVua3MgPSB0ZXh0Q2h1bmtzLmxlbmd0aDtcblxuICAvLyBGSVggNDogdXNlIHNvdXJjZVR5cGUgZnJvbSBvcHRpb25zLCBmYWxsIGJhY2sgdG8gJ3BkZidcbiAgY29uc3Qgc291cmNlVHlwZSA9IG9wdGlvbnMuc291cmNlVHlwZSB8fCAncGRmJztcblxuICByZXR1cm4gdGV4dENodW5rcy5tYXAoY2h1bmsgPT4ge1xuICAgIGNvbnN0IGNodW5rSWQgPSBnZW5lcmF0ZUNodW5rSWQoY2h1bmsudGV4dCwgZmlsZW5hbWUpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQ6IGNodW5rLnRleHQsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBkb2N1bWVudF9pZDogZG9jdW1lbnRJZCxcbiAgICAgICAgZmlsZW5hbWU6IGZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY2h1bmtJZCxcbiAgICAgICAgY2h1bmtfaW5kZXg6IGNodW5rLmNodW5rSW5kZXgsXG4gICAgICAgIHRvdGFsX2NodW5rczogdG90YWxDaHVua3MsXG4gICAgICAgIHBhZ2VfbnVtYmVyOiBwYWdlTnVtYmVyIHx8IDEsXG4gICAgICAgIHRvdGFsX3BhZ2VzOiB0b3RhbFBhZ2VzIHx8IG51bGwsXG4gICAgICAgIHNlY3Rpb25fdGl0bGU6IGV4dHJhY3RTZWN0aW9uVGl0bGUoY2h1bmsudGV4dCksXG4gICAgICAgIHNvdXJjZV90eXBlOiBzb3VyY2VUeXBlLCAgICAgICAgICAgIC8vIEZJWCA0XG4gICAgICAgIHVwbG9hZF90aW1lc3RhbXA6IG5ldyBEYXRlKCkudG9JU09TdHJpbmcoKSxcbiAgICAgICAgY2hhcl9zdGFydDogY2h1bmsuY2hhclN0YXJ0LFxuICAgICAgICBjaGFyX2VuZDogY2h1bmsuY2hhckVuZCxcbiAgICAgICAgdG9rZW5fY291bnQ6IGNodW5rLnRva2VuQ291bnRcbiAgICAgIH1cbiAgICB9O1xuICB9KTtcbn1cblxuZnVuY3Rpb24gZXh0cmFjdFNlY3Rpb25UaXRsZSh0ZXh0KSB7XG4gIGNvbnN0IGxpbmVzID0gdGV4dC5zcGxpdCgnXFxuJykuZmlsdGVyKGwgPT4gbC50cmltKCkpO1xuICBpZiAobGluZXMubGVuZ3RoID4gMCkge1xuICAgIGNvbnN0IGZpcnN0TGluZSA9IGxpbmVzWzBdLnRyaW0oKTtcbiAgICBpZiAoZmlyc3RMaW5lLmxlbmd0aCA8IDEwMCAmJiAhZmlyc3RMaW5lLmVuZHNXaXRoKCcuJykpIHtcbiAgICAgIHJldHVybiBmaXJzdExpbmUuc2xpY2UoMCwgNTApO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9zZXNzaW9uU2VydmljZS5qc1wiO2ltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHtcbiAgZ2V0R2xvYmFsQ29sbGVjdGlvbixcbiAgZ2V0U2Vzc2lvbkNvbGxlY3Rpb24sXG4gIGxpc3REb2N1bWVudHMsXG4gIGFkZFZlY3RvcnNcbn0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcblxuY29uc3QgREVGQVVMVF9USU1FT1VUX01JTlVURVMgPSA2MDtcbmNvbnN0IHNlc3Npb25zID0gbmV3IE1hcCgpO1xuY29uc3QgTUFYX1BERlNfUEVSX1NFU1NJT04gPSBwYXJzZUludChwcm9jZXNzLmVudi5NQVhfUERGU19QRVJfU0VTU0lPTikgfHwgMztcbmNvbnN0IE1BWF9VUExPQURfU0laRV9NQiA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQikgfHwgNTtcblxuLy8gVHJhY2sgd2hpY2ggc2Vzc2lvbnMgaGF2ZSBiZWVuIHNlZWRlZCBmcm9tIGdsb2JhbCBjb2xsZWN0aW9uXG5jb25zdCBzZWVkZWRTZXNzaW9ucyA9IG5ldyBTZXQoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVNlc3Npb24oKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHV1aWR2NCgpO1xuICBjb25zdCBzZXNzaW9uID0ge1xuICAgIGlkOiBzZXNzaW9uSWQsXG4gICAgY3JlYXRlZEF0OiBuZXcgRGF0ZSgpLFxuICAgIGxhc3RBY2Nlc3NlZDogbmV3IERhdGUoKSxcbiAgICBkb2N1bWVudHM6IFtdLFxuICAgIHRpbWVvdXRNaW51dGVzOiBERUZBVUxUX1RJTUVPVVRfTUlOVVRFU1xuICB9O1xuXG4gIHNlc3Npb25zLnNldChzZXNzaW9uSWQsIHNlc3Npb24pO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb24oc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBzZXNzaW9ucy5nZXQoc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gbnVsbDtcbiAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICBkZWxldGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gc2Vzc2lvbjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yQ3JlYXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgaWYgKHNlc3Npb25JZCkge1xuICAgIGNvbnN0IGV4aXN0aW5nID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChleGlzdGluZykgcmV0dXJuIGV4aXN0aW5nO1xuICB9XG4gIHJldHVybiBjcmVhdGVTZXNzaW9uKCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nlc3Npb25FeHBpcmVkKHNlc3Npb24pIHtcbiAgY29uc3Qgbm93ID0gRGF0ZS5ub3coKTtcbiAgY29uc3QgbGFzdEFjY2Vzc2VkID0gbmV3IERhdGUoc2Vzc2lvbi5sYXN0QWNjZXNzZWQpLmdldFRpbWUoKTtcbiAgY29uc3QgdGltZW91dE1zID0gc2Vzc2lvbi50aW1lb3V0TWludXRlcyAqIDYwICogMTAwMDtcbiAgcmV0dXJuIChub3cgLSBsYXN0QWNjZXNzZWQpID4gdGltZW91dE1zO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZGVsZXRlU2Vzc2lvbihzZXNzaW9uSWQpIHtcbiAgc2Vzc2lvbnMuZGVsZXRlKHNlc3Npb25JZCk7XG4gIHNlZWRlZFNlc3Npb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG4vKipcbiAqIE9uIHNlc3Npb24gc3RhcnQ6IGNvcHkgYWxsIGdsb2JhbCBjb2xsZWN0aW9uIHZlY3RvcnMgaW50byB0aGUgc2Vzc2lvbiBjb2xsZWN0aW9uLlxuICogVGhpcyBvbmx5IHJ1bnMgT05DRSBwZXIgc2Vzc2lvbklkIFx1MjAxNCBza2lwcGVkIG9uIHN1YnNlcXVlbnQgY2FsbHMuXG4gKi9cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBpbml0U2Vzc2lvbldpdGhHbG9iYWxEb2NzKHNlc3Npb25JZCkge1xuICBpZiAoc2VlZGVkU2Vzc2lvbnMuaGFzKHNlc3Npb25JZCkpIHJldHVybjsgLy8gYWxyZWFkeSBkb25lXG5cbiAgdHJ5IHtcbiAgICBjb25zb2xlLmxvZyhgXHVEODNDXHVERjMxIFNlZWRpbmcgc2Vzc2lvbiAke3Nlc3Npb25JZH0gZnJvbSBnbG9iYWwgY29sbGVjdGlvbi4uLmApO1xuXG4gICAgY29uc3QgZ2xvYmFsQ29sbGVjdGlvbiA9IGF3YWl0IGdldEdsb2JhbENvbGxlY3Rpb24oKTtcbiAgICBjb25zdCBzZXNzaW9uQ29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG5cbiAgICAvLyBGZXRjaCBBTEwgdmVjdG9ycyBmcm9tIGdsb2JhbCAoZG9jdW1lbnRzICsgZW1iZWRkaW5ncyArIG1ldGFkYXRhcyArIGlkcylcbiAgICBjb25zdCBhbGxHbG9iYWwgPSBhd2FpdCBnbG9iYWxDb2xsZWN0aW9uLmdldCh7XG4gICAgICBpbmNsdWRlOiBbJ2VtYmVkZGluZ3MnLCAnZG9jdW1lbnRzJywgJ21ldGFkYXRhcyddXG4gICAgfSk7XG5cbiAgICBpZiAoIWFsbEdsb2JhbC5pZHMgfHwgYWxsR2xvYmFsLmlkcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKCdcdTI2QTBcdUZFMEYgIEdsb2JhbCBjb2xsZWN0aW9uIGlzIGVtcHR5IFx1MjAxNCBub3RoaW5nIHRvIHNlZWQuJyk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5hZGQoc2Vzc2lvbklkKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICAvLyBDaGVjayB3aGljaCBpZHMgYWxyZWFkeSBleGlzdCBpbiBzZXNzaW9uIChhdm9pZCBkdXBsaWNhdGUgaW5zZXJ0cyBvbiByZWNvbm5lY3QpXG4gICAgY29uc3QgZXhpc3RpbmcgPSBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5nZXQoeyBpZHM6IGFsbEdsb2JhbC5pZHMgfSk7XG4gICAgY29uc3QgZXhpc3RpbmdJZHMgPSBuZXcgU2V0KGV4aXN0aW5nLmlkcyB8fCBbXSk7XG4gICAgY29uc3QgbmV3SWRzID0gYWxsR2xvYmFsLmlkcy5maWx0ZXIoaWQgPT4gIWV4aXN0aW5nSWRzLmhhcyhpZCkpO1xuXG4gICAgaWYgKG5ld0lkcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2Vzc2lvbiAke3Nlc3Npb25JZH0gYWxyZWFkeSBoYXMgYWxsIGdsb2JhbCB2ZWN0b3JzLmApO1xuICAgICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3QgbmV3SW5kaWNlcyA9IGFsbEdsb2JhbC5pZHNcbiAgICAgIC5tYXAoKGlkLCBpKSA9PiBpKVxuICAgICAgLmZpbHRlcihpID0+IG5ld0lkcy5pbmNsdWRlcyhhbGxHbG9iYWwuaWRzW2ldKSk7XG5cbiAgICBhd2FpdCBzZXNzaW9uQ29sbGVjdGlvbi5hZGQoe1xuICAgICAgaWRzOiBuZXdJbmRpY2VzLm1hcChpID0+IGFsbEdsb2JhbC5pZHNbaV0pLFxuICAgICAgZW1iZWRkaW5nczogbmV3SW5kaWNlcy5tYXAoaSA9PiBhbGxHbG9iYWwuZW1iZWRkaW5nc1tpXSksXG4gICAgICBkb2N1bWVudHM6IG5ld0luZGljZXMubWFwKGkgPT4gYWxsR2xvYmFsLmRvY3VtZW50c1tpXSksXG4gICAgICBtZXRhZGF0YXM6IG5ld0luZGljZXMubWFwKGkgPT4gKHtcbiAgICAgICAgLi4uYWxsR2xvYmFsLm1ldGFkYXRhc1tpXSxcbiAgICAgICAgc291cmNlX3R5cGU6ICdnbG9iYWwnICAvLyBwcmVzZXJ2ZSBzbyBVSSBjYW4gc3RpbGwgc2hvdyBTZWVkIGJhZGdlXG4gICAgICB9KSlcbiAgICB9KTtcblxuICAgIGNvbnNvbGUubG9nKGBcdTI3MDUgU2VlZGVkICR7bmV3SWRzLmxlbmd0aH0gdmVjdG9ycyBpbnRvIHNlc3Npb24gJHtzZXNzaW9uSWR9YCk7XG4gICAgc2VlZGVkU2Vzc2lvbnMuYWRkKHNlc3Npb25JZCk7XG5cbiAgICAvLyBSZWdpc3RlciBnbG9iYWwgZG9jcyBpbiBzZXNzaW9uIGRvY3VtZW50IGxpc3QgZm9yIGxpc3RpbmcgaW4gVUlcbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICAgIGlmIChzZXNzaW9uKSB7XG4gICAgICBjb25zdCBkb2NzTWFwID0gbmV3IE1hcCgpO1xuICAgICAgYWxsR2xvYmFsLm1ldGFkYXRhcy5mb3JFYWNoKG1ldGEgPT4ge1xuICAgICAgICBpZiAoIWRvY3NNYXAuaGFzKG1ldGEuZG9jdW1lbnRfaWQpKSB7XG4gICAgICAgICAgZG9jc01hcC5zZXQobWV0YS5kb2N1bWVudF9pZCwge1xuICAgICAgICAgICAgaWQ6IG1ldGEuZG9jdW1lbnRfaWQsXG4gICAgICAgICAgICBmaWxlbmFtZTogbWV0YS5maWxlbmFtZSxcbiAgICAgICAgICAgIGZpbGVTaXplOiBudWxsLFxuICAgICAgICAgICAgcGFnZUNvdW50OiBtZXRhLnRvdGFsX3BhZ2VzIHx8IG51bGwsXG4gICAgICAgICAgICBjaHVua0NvdW50OiAwLFxuICAgICAgICAgICAgc291cmNlVHlwZTogJ2dsb2JhbCcsXG4gICAgICAgICAgICB1cGxvYWRUaW1lc3RhbXA6IG1ldGEudXBsb2FkX3RpbWVzdGFtcFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGRvY3NNYXAuZ2V0KG1ldGEuZG9jdW1lbnRfaWQpLmNodW5rQ291bnQrKztcbiAgICAgIH0pO1xuXG4gICAgICBmb3IgKGNvbnN0IGRvYyBvZiBkb2NzTWFwLnZhbHVlcygpKSB7XG4gICAgICAgIGlmICghc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuaWQgPT09IGRvYy5pZCkpIHtcbiAgICAgICAgICBzZXNzaW9uLmRvY3VtZW50cy5wdXNoKGRvYyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKGBcdTI3NEMgRmFpbGVkIHRvIHNlZWQgc2Vzc2lvbiAke3Nlc3Npb25JZH06YCwgZXJyb3IubWVzc2FnZSk7XG4gICAgLy8gTm9uLWZhdGFsIFx1MjAxNCBzZXNzaW9uIGNhbiBzdGlsbCB3b3JrIHdpdGhvdXQgZ2xvYmFsIGRvY3NcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkRG9jdW1lbnRUb1Nlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudEluZm8pIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4gZmFsc2U7XG4gIHNlc3Npb24uZG9jdW1lbnRzLnB1c2goe1xuICAgIGlkOiBkb2N1bWVudEluZm8uaWQsXG4gICAgZmlsZW5hbWU6IGRvY3VtZW50SW5mby5maWxlbmFtZSxcbiAgICBmaWxlU2l6ZTogZG9jdW1lbnRJbmZvLmZpbGVTaXplLFxuICAgIHBhZ2VDb3VudDogZG9jdW1lbnRJbmZvLnBhZ2VDb3VudCxcbiAgICB1cGxvYWRUaW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgY2h1bmtDb3VudDogZG9jdW1lbnRJbmZvLmNodW5rQ291bnQsXG4gICAgc291cmNlVHlwZTogJ3Nlc3Npb25fdXBsb2FkJ1xuICB9KTtcbiAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbkFjY2VwdFVwbG9hZChzZXNzaW9uSWQpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgaWYgKCFzZXNzaW9uKSByZXR1cm4geyBjYW5VcGxvYWQ6IGZhbHNlLCByZWFzb246ICdTZXNzaW9uIG5vdCBmb3VuZCcgfTtcblxuICAvLyBDb3VudCBvbmx5IHNlc3Npb24tdXBsb2FkZWQgZG9jcyAobm90IGdsb2JhbCBzZWVkcylcbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoO1xuICBpZiAodXBsb2FkZWRDb3VudCA+PSBNQVhfUERGU19QRVJfU0VTU0lPTikge1xuICAgIHJldHVybiB7IGNhblVwbG9hZDogZmFsc2UsIHJlYXNvbjogYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmAgfTtcbiAgfVxuICByZXR1cm4geyBjYW5VcGxvYWQ6IHRydWUgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlVXBsb2FkKHNlc3Npb25JZCwgZmlsZSwgZmlsZW5hbWUpIHtcbiAgY29uc3Qgc2Vzc2lvbiA9IGdldFNlc3Npb24oc2Vzc2lvbklkKTtcbiAgY29uc3QgZXJyb3JzID0gW107XG5cbiAgaWYgKGZpbGUuc2l6ZSA+IE1BWF9VUExPQURfU0laRV9NQiAqIDEwMjQgKiAxMDI0KSB7XG4gICAgZXJyb3JzLnB1c2goYEZpbGUgZXhjZWVkcyAke01BWF9VUExPQURfU0laRV9NQn1NQiBsaW1pdGApO1xuICB9XG5cbiAgY29uc3QgdXBsb2FkZWRDb3VudCA9IHNlc3Npb25cbiAgICA/IHNlc3Npb24uZG9jdW1lbnRzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJykubGVuZ3RoXG4gICAgOiAwO1xuXG4gIGlmICh1cGxvYWRlZENvdW50ID49IE1BWF9QREZTX1BFUl9TRVNTSU9OKSB7XG4gICAgZXJyb3JzLnB1c2goYE1heGltdW0gJHtNQVhfUERGU19QRVJfU0VTU0lPTn0gUERGcyBwZXIgc2Vzc2lvbmApO1xuICB9XG5cbiAgaWYgKHNlc3Npb24gJiYgc2Vzc2lvbi5kb2N1bWVudHMuc29tZShkID0+IGQuZmlsZW5hbWUgPT09IGZpbGVuYW1lKSkge1xuICAgIGVycm9ycy5wdXNoKGBGaWxlIFwiJHtmaWxlbmFtZX1cIiBhbHJlYWR5IGV4aXN0cyBpbiB0aGlzIHNlc3Npb25gKTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgaXNWYWxpZDogZXJyb3JzLmxlbmd0aCA9PT0gMCxcbiAgICBlcnJvcnMsXG4gICAgaXNMYXJnZUZpbGU6IGZpbGUuc2l6ZSA+IChNQVhfVVBMT0FEX1NJWkVfTUIgKiAxMDI0ICogMTAyNCAqIDAuNilcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbW92ZURvY3VtZW50RnJvbVNlc3Npb24oc2Vzc2lvbklkLCBkb2N1bWVudElkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIGZhbHNlO1xuICBjb25zdCBpZHggPSBzZXNzaW9uLmRvY3VtZW50cy5maW5kSW5kZXgoZCA9PiBkLmlkID09PSBkb2N1bWVudElkKTtcbiAgaWYgKGlkeCA+PSAwKSB7XG4gICAgc2Vzc2lvbi5kb2N1bWVudHMuc3BsaWNlKGlkeCwgMSk7XG4gICAgc2Vzc2lvbi5sYXN0QWNjZXNzZWQgPSBuZXcgRGF0ZSgpO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlc3Npb25Eb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb24gPSBnZXRTZXNzaW9uKHNlc3Npb25JZCk7XG4gIGlmICghc2Vzc2lvbikgcmV0dXJuIFtdO1xuICByZXR1cm4gc2Vzc2lvbi5kb2N1bWVudHM7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZXRBbGxEb2N1bWVudHMoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHNlc3Npb25Eb2NzID0gZ2V0U2Vzc2lvbkRvY3VtZW50cyhzZXNzaW9uSWQpO1xuICByZXR1cm4ge1xuICAgIC8vIFNwbGl0IGZvciBVSSBcdTIwMTQgZ2xvYmFsIHNlZWRlZCBkb2NzIHZzIHVzZXItdXBsb2FkZWQgZG9jc1xuICAgIHNlc3Npb25Eb2N1bWVudHM6IHNlc3Npb25Eb2NzLmZpbHRlcihkID0+IGQuc291cmNlVHlwZSA9PT0gJ3Nlc3Npb25fdXBsb2FkJyksXG4gICAgZ2xvYmFsRG9jdW1lbnRzOiBzZXNzaW9uRG9jcy5maWx0ZXIoZCA9PiBkLnNvdXJjZVR5cGUgPT09ICdnbG9iYWwnKVxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2Vzc2lvblN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBzZXNzaW9uID0gZ2V0U2Vzc2lvbihzZXNzaW9uSWQpO1xuICBpZiAoIXNlc3Npb24pIHJldHVybiBudWxsO1xuICByZXR1cm4ge1xuICAgIGlkOiBzZXNzaW9uLmlkLFxuICAgIGRvY3VtZW50Q291bnQ6IHNlc3Npb24uZG9jdW1lbnRzLmxlbmd0aCxcbiAgICBjcmVhdGVkQXQ6IHNlc3Npb24uY3JlYXRlZEF0LFxuICAgIGxhc3RBY2Nlc3NlZDogc2Vzc2lvbi5sYXN0QWNjZXNzZWQsXG4gICAgdG90YWxTaXplOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuZmlsZVNpemUgfHwgMCksIDApLFxuICAgIHRvdGFsQ2h1bmtzOiBzZXNzaW9uLmRvY3VtZW50cy5yZWR1Y2UoKHN1bSwgZCkgPT4gc3VtICsgKGQuY2h1bmtDb3VudCB8fCAwKSwgMClcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxpc3RBY3RpdmVTZXNzaW9ucygpIHtcbiAgcmV0dXJuIEFycmF5LmZyb20oc2Vzc2lvbnMudmFsdWVzKCkpLmZpbHRlcihzID0+ICFpc1Nlc3Npb25FeHBpcmVkKHMpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNsZWFudXBFeHBpcmVkU2Vzc2lvbnMoKSB7XG4gIGxldCBjbGVhbmVkID0gMDtcbiAgZm9yIChjb25zdCBbaWQsIHNlc3Npb25dIG9mIHNlc3Npb25zKSB7XG4gICAgaWYgKGlzU2Vzc2lvbkV4cGlyZWQoc2Vzc2lvbikpIHtcbiAgICAgIHNlc3Npb25zLmRlbGV0ZShpZCk7XG4gICAgICBzZWVkZWRTZXNzaW9ucy5kZWxldGUoaWQpO1xuICAgICAgY2xlYW5lZCsrO1xuICAgIH1cbiAgfVxuICByZXR1cm4gY2xlYW5lZDtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL3JldHJpZXZhbFNlcnZpY2UuanNcIjtpbXBvcnQgeyBnZXRTZXNzaW9uQ29sbGVjdGlvbiwgcXVlcnlDb2xsZWN0aW9uIH0gZnJvbSAnLi9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGVtYmVkUXVlcnkgfSBmcm9tICcuL2VtYmVkZGluZ1NlcnZpY2UuanMnO1xuaW1wb3J0IHsgdjQgYXMgdXVpZHY0IH0gZnJvbSAndXVpZCc7XG5cbmNvbnN0IFRPUF9LID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuVE9QX0spIHx8IDU7XG5jb25zdCBSRUZVU0FMX1RIUkVTSE9MRCA9IHBhcnNlRmxvYXQocHJvY2Vzcy5lbnYuUkVGVVNBTF9USFJFU0hPTEQpIHx8IDAuMDU7XG5cbi8vIENhY2hlIHJlc29sdmVkIGNvbGxlY3Rpb24gb2JqZWN0c1xuY29uc3QgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zID0gbmV3IE1hcCgpO1xuXG5hc3luYyBmdW5jdGlvbiBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSB7XG4gIGlmIChjYWNoZWRTZXNzaW9uQ29sbGVjdGlvbnMuaGFzKHNlc3Npb25JZCkpIHtcbiAgICByZXR1cm4gY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmdldChzZXNzaW9uSWQpO1xuICB9XG4gIHRyeSB7XG4gICAgY29uc3QgY29sbGVjdGlvbiA9IGF3YWl0IGdldFNlc3Npb25Db2xsZWN0aW9uKHNlc3Npb25JZCk7XG4gICAgaWYgKGNvbGxlY3Rpb24pIGNhY2hlZFNlc3Npb25Db2xsZWN0aW9ucy5zZXQoc2Vzc2lvbklkLCBjb2xsZWN0aW9uKTtcbiAgICByZXR1cm4gY29sbGVjdGlvbjtcbiAgfSBjYXRjaCB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsY3VsYXRlQ292ZXJhZ2UocmVzdWx0cywgdG9wSyA9IFRPUF9LKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuIHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAgfTtcbiAgY29uc3Qgc2NvcmVzID0gcmVzdWx0cy5zbGljZSgwLCB0b3BLKS5tYXAociA9PiBNYXRoLm1heCgwLCByLnNjb3JlKSk7XG4gIGNvbnN0IGF2Z1Njb3JlID0gc2NvcmVzLnJlZHVjZSgoYSwgYikgPT4gYSArIGIsIDApIC8gc2NvcmVzLmxlbmd0aDtcbiAgcmV0dXJuIHtcbiAgICBjb25maWRlbmNlOiBNYXRoLnJvdW5kKGF2Z1Njb3JlICogMTAwKSxcbiAgICB0b3BTY29yZTogTWF0aC5tYXgoLi4uc2NvcmVzKVxuICB9O1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gcmV0cmlldmVGb3JRdWVyeShxdWVyeSwgc2Vzc2lvbklkLCBvcHRpb25zID0ge30pIHtcbiAgY29uc3QgdG9wSyA9IG9wdGlvbnMudG9wSyB8fCBUT1BfSztcblxuICB0cnkge1xuICAgIGNvbnN0IFtxdWVyeUVtYmVkZGluZywgc2Vzc2lvbkNvbGxlY3Rpb25dID0gYXdhaXQgUHJvbWlzZS5hbGwoW1xuICAgICAgZW1iZWRRdWVyeShxdWVyeSksXG4gICAgICBzZXNzaW9uSWQgPyBnZXRPckNhY2hlU2Vzc2lvbkNvbGxlY3Rpb24oc2Vzc2lvbklkKSA6IFByb21pc2UucmVzb2x2ZShudWxsKVxuICAgIF0pO1xuXG4gICAgaWYgKCFzZXNzaW9uQ29sbGVjdGlvbikge1xuICAgICAgY29uc29sZS53YXJuKGBcdTI2QTBcdUZFMEYgIE5vIHNlc3Npb24gY29sbGVjdGlvbiBmb3VuZCBmb3IgJHtzZXNzaW9uSWR9YCk7XG4gICAgICByZXR1cm4geyByZXN1bHRzOiBbXSwgY292ZXJhZ2U6IHsgY29uZmlkZW5jZTogMCwgdG9wU2NvcmU6IDAsIGxldmVsOiAnbG93Jywgc2NvcmU6IDAgfSwgcXVlcnlFbWJlZGRpbmcgfTtcbiAgICB9XG5cbiAgICAvLyBTaW5nbGUgcXVlcnkgXHUyMDE0IHNlc3Npb24gY29sbGVjdGlvbiBoYXMgZ2xvYmFsIHZlY3RvcnMgYWxyZWFkeSBjb3BpZWQgaW5cbiAgICBjb25zdCByYXdSZXN1bHRzID0gYXdhaXQgcXVlcnlDb2xsZWN0aW9uKHNlc3Npb25Db2xsZWN0aW9uLCBxdWVyeUVtYmVkZGluZywgdG9wSylcbiAgICAgIC5jYXRjaCgoKSA9PiBbXSk7XG5cbiAgICAvLyBQcmVzZXJ2ZSBzb3VyY2VfdHlwZSBmcm9tIG1ldGFkYXRhIHNvIFVJIGJhZGdlIChTZWVkL1Nlc3Npb24pIHN0aWxsIHdvcmtzXG4gICAgY29uc3QgcmVzdWx0cyA9IHJhd1Jlc3VsdHMubWFwKHIgPT4gKHtcbiAgICAgIC4uLnIsXG4gICAgICBzb3VyY2VfdHlwZTogci5tZXRhZGF0YT8uc291cmNlX3R5cGUgfHwgJ3Nlc3Npb24nXG4gICAgfSkpO1xuXG4gICAgY29uc3QgY292ZXJhZ2UgPSBjYWxjdWxhdGVDb3ZlcmFnZShyZXN1bHRzLCB0b3BLKTtcbiAgICBjb25zdCB0b3BTY29yZSA9IGNvdmVyYWdlLnRvcFNjb3JlO1xuICAgIGNvbnN0IGxldmVsID0gdG9wU2NvcmUgPj0gMC42ID8gJ2hpZ2gnIDogdG9wU2NvcmUgPj0gMC4zID8gJ21lZGl1bScgOiAnbG93JztcblxuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdUREMEQgUXVlcnk6JywgcXVlcnkpO1xuICAgIGNvbnNvbGUubG9nKCdcdUQ4M0RcdURDQ0EgQ292ZXJhZ2U6JywgeyAuLi5jb3ZlcmFnZSwgbGV2ZWwgfSk7XG4gICAgY29uc29sZS5sb2coJ1x1RDgzRFx1RENDOCBSYXcgc2NvcmVzOicsIHJlc3VsdHMubWFwKHIgPT4gci5zY29yZS50b0ZpeGVkKDQpKSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgcmVzdWx0cyxcbiAgICAgIGNvdmVyYWdlOiB7IC4uLmNvdmVyYWdlLCBsZXZlbCwgc2NvcmU6IHRvcFNjb3JlIH0sXG4gICAgICBxdWVyeUVtYmVkZGluZ1xuICAgIH07XG5cbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdSZXRyaWV2YWwgZXJyb3I6JywgZXJyb3IpO1xuICAgIHRocm93IGVycm9yO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnZhbGlkYXRlU2Vzc2lvbkNvbGxlY3Rpb25DYWNoZShzZXNzaW9uSWQpIHtcbiAgY2FjaGVkU2Vzc2lvbkNvbGxlY3Rpb25zLmRlbGV0ZShzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0Q29udGV4dEZvclByb21wdChyZXN1bHRzLCBtYXhUb2tlbnMgPSA3MDAwKSB7XG4gIGlmICghcmVzdWx0cyB8fCByZXN1bHRzLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIGxldCB0b3RhbFRva2VucyA9IDA7XG4gIGNvbnN0IGNvbnRleHRQYXJ0cyA9IFtdO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVzdWx0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IHJlc3VsdCA9IHJlc3VsdHNbaV07XG4gICAgY29uc3QgdG9rZW5Fc3RpbWF0ZSA9IHJlc3VsdC50ZXh0Lmxlbmd0aCAvIDQ7XG4gICAgaWYgKHRvdGFsVG9rZW5zICsgdG9rZW5Fc3RpbWF0ZSA+IG1heFRva2VucykgYnJlYWs7XG4gICAgdG90YWxUb2tlbnMgKz0gdG9rZW5Fc3RpbWF0ZTtcbiAgICBjb25zdCBzb3VyY2VMYWJlbCA9IHJlc3VsdC5zb3VyY2VfdHlwZSA9PT0gJ2dsb2JhbCcgPyAnW1NlZWQgRG9jdW1lbnRdJyA6ICdbU2Vzc2lvbiBVcGxvYWRdJztcbiAgICBjb25zdCBwYWdlID0gcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyID8gYCAoUGFnZSAke3Jlc3VsdC5tZXRhZGF0YS5wYWdlX251bWJlcn0pYCA6ICcnO1xuICAgIGNvbnRleHRQYXJ0cy5wdXNoKGBbJHtpICsgMX1dICR7c291cmNlTGFiZWx9ICR7cmVzdWx0Lm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ30ke3BhZ2V9OlxcbiR7cmVzdWx0LnRleHR9YCk7XG4gIH1cblxuICByZXR1cm4gY29udGV4dFBhcnRzLmpvaW4oJ1xcblxcbi0tLVxcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2VuZXJhdGVDaXRhdGlvbnMocmVzdWx0cykge1xuICBpZiAoIXJlc3VsdHMgfHwgcmVzdWx0cy5sZW5ndGggPT09IDApIHJldHVybiBbXTtcbiAgcmV0dXJuIHJlc3VsdHMubWFwKChyZXN1bHQsIGlkeCkgPT4gKHtcbiAgICBpZDogdXVpZHY0KCksXG4gICAgaW5kZXg6IGlkeCArIDEsXG4gICAgZG9jdW1lbnRJZDogcmVzdWx0Lm1ldGFkYXRhLmRvY3VtZW50X2lkLFxuICAgIGZpbGVuYW1lOiByZXN1bHQubWV0YWRhdGEuZmlsZW5hbWUsXG4gICAgcGFnZU51bWJlcjogcmVzdWx0Lm1ldGFkYXRhLnBhZ2VfbnVtYmVyLFxuICAgIHNlY3Rpb246IHJlc3VsdC5tZXRhZGF0YS5zZWN0aW9uX3RpdGxlLFxuICAgIGV4Y2VycHQ6IHJlc3VsdC50ZXh0LnNsaWNlKDAsIDIwMCkgKyAocmVzdWx0LnRleHQubGVuZ3RoID4gMjAwID8gJy4uLicgOiAnJyksXG4gICAgc2NvcmU6IHJlc3VsdC5zY29yZSxcbiAgICBzb3VyY2VUeXBlOiByZXN1bHQuc291cmNlX3R5cGUsXG4gICAgY2h1bmtJZDogcmVzdWx0LmlkXG4gIH0pKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNob3VsZFNob3dSZWZ1c2FsKGNvdmVyYWdlKSB7XG4gIHJldHVybiBjb3ZlcmFnZS50b3BTY29yZSA8IFJFRlVTQUxfVEhSRVNIT0xEO1xufVxuXG5leHBvcnQgeyBjYWxjdWxhdGVDb3ZlcmFnZSB9OyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZG9jdW1lbnRzLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9kb2N1bWVudHMuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCBtdWx0ZXIgZnJvbSAnbXVsdGVyJztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IGZzIGZyb20gJ2ZzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgY3JlYXRlSGFzaCB9IGZyb20gJ2NyeXB0byc7XG5pbXBvcnQgcGRmIGZyb20gJ3BkZi1wYXJzZSc7XG5pbXBvcnQgeyBmaWxlVVJMVG9QYXRoIH0gZnJvbSAndXJsJzs7XG5pbXBvcnQgeyBzYW5pdGl6ZUZpbGVuYW1lLCB2YWxpZGF0ZVBERkZpbGUsIHZhbGlkYXRlRmlsZVNpemUgfSBmcm9tICcuLi91dGlscy9zYW5pdGl6ZS5qcyc7XG5pbXBvcnQge1xuICBDb3JydXB0ZWRQREZFcnJvcixcbiAgSW52YWxpZEZpbGVUeXBlRXJyb3IsXG4gIEZpbGVUb29MYXJnZUVycm9yLFxuICBUb29NYW55UERGc0Vycm9yLFxuICBEdXBsaWNhdGVGaWxlRXJyb3Jcbn0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcbmltcG9ydCB7IGdldFNlc3Npb25Db2xsZWN0aW9uLCBhZGRWZWN0b3JzLCBkZWxldGVEb2N1bWVudFZlY3RvcnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9jaHJvbWFTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGNodW5rVGV4dCwgY2xlYW5UZXh0IH0gZnJvbSAnLi4vdXRpbHMvY2h1bmtlci5qcyc7XG5pbXBvcnQgeyBnZW5lcmF0ZUVtYmVkZGluZ3MgfSBmcm9tICcuLi9zZXJ2aWNlcy9lbWJlZGRpbmdTZXJ2aWNlLmpzJztcbmltcG9ydCB7XG4gIGdldE9yQ3JlYXRlU2Vzc2lvbixcbiAgY2FuQWNjZXB0VXBsb2FkLFxuICBhZGREb2N1bWVudFRvU2Vzc2lvbixcbiAgcmVtb3ZlRG9jdW1lbnRGcm9tU2Vzc2lvbixcbiAgZ2V0QWxsRG9jdW1lbnRzXG59IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcbmltcG9ydCB7IGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlIH0gZnJvbSAnLi4vc2VydmljZXMvcmV0cmlldmFsU2VydmljZS5qcyc7XG5cbmNvbnN0IHJvdXRlciA9IFJvdXRlcigpO1xuXG5jb25zdCBfX2ZpbGVuYW1lID0gZmlsZVVSTFRvUGF0aChpbXBvcnQubWV0YS51cmwpO1xuY29uc3QgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKF9fZmlsZW5hbWUpO1xuXG4vLyBBbGwgdXBsb2FkZWQgUERGcyBnbyB0byAvdG1wIFx1MjAxNCBuZXZlciB0byBzZWVkX2RvY3VtZW50c1xuY29uc3QgdXBsb2FkRGlyID0gJy90bXAvdXBsb2Fkcyc7XG5pZiAoIWZzLmV4aXN0c1N5bmModXBsb2FkRGlyKSkge1xuICBmcy5ta2RpclN5bmModXBsb2FkRGlyLCB7IHJlY3Vyc2l2ZTogdHJ1ZSB9KTtcbn1cblxuLy8gU2VlZCBQREZzIGxpdmUgaGVyZSBcdTIwMTQgb25seSB1c2VkIGZvciBzZXJ2aW5nIHRoZSBmaWxlIChWaWV3IFBERiksIG5ldmVyIHdyaXR0ZW4gdG9cbmNvbnN0IHNlZWREaXIgPSBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi4vLi4vc2VlZF9kb2N1bWVudHMnKTtcblxuY29uc3Qgc3RvcmFnZSA9IG11bHRlci5kaXNrU3RvcmFnZSh7XG4gIGRlc3RpbmF0aW9uOiAocmVxLCBmaWxlLCBjYikgPT4gY2IobnVsbCwgdXBsb2FkRGlyKSxcbiAgZmlsZW5hbWU6IChyZXEsIGZpbGUsIGNiKSA9PiBjYihudWxsLCBgJHt1dWlkdjQoKX0ke3BhdGguZXh0bmFtZShmaWxlLm9yaWdpbmFsbmFtZSl9YClcbn0pO1xuXG5jb25zdCB1cGxvYWQgPSBtdWx0ZXIoe1xuICBzdG9yYWdlLFxuICBsaW1pdHM6IHsgZmlsZVNpemU6IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9VUExPQURfU0laRV9NQiB8fCAnNScpICogMTAyNCAqIDEwMjQgfSxcbiAgZmlsZUZpbHRlcjogKHJlcSwgZmlsZSwgY2IpID0+IHtcbiAgICBpZiAoZmlsZS5taW1ldHlwZSA9PT0gJ2FwcGxpY2F0aW9uL3BkZicgJiYgcGF0aC5leHRuYW1lKGZpbGUub3JpZ2luYWxuYW1lKS50b0xvd2VyQ2FzZSgpID09PSAnLnBkZicpIHtcbiAgICAgIGNiKG51bGwsIHRydWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjYihuZXcgSW52YWxpZEZpbGVUeXBlRXJyb3IoKSk7XG4gICAgfVxuICB9XG59KTtcblxuYXN5bmMgZnVuY3Rpb24gcGFyc2VQREZXaXRoQm91bmRhcnlNYXAoZmlsZVBhdGgpIHtcbiAgdHJ5IHtcbiAgICBjb25zdCBidWZmZXIgPSBmcy5yZWFkRmlsZVN5bmMoZmlsZVBhdGgpO1xuXG4gICAgY29uc3QgcGFnZXMgPSBbXTtcbiAgICBhd2FpdCBwZGYoYnVmZmVyLCB7XG4gICAgICBwYWdlcmVuZGVyOiAocGFnZURhdGEpID0+IHtcbiAgICAgICAgcmV0dXJuIHBhZ2VEYXRhLmdldFRleHRDb250ZW50KCkudGhlbih0YyA9PiB7XG4gICAgICAgICAgY29uc3QgcGFnZVRleHQgPSB0Yy5pdGVtcy5tYXAoaSA9PiBpLnN0cikuam9pbignICcpO1xuICAgICAgICAgIHBhZ2VzLnB1c2gocGFnZVRleHQpO1xuICAgICAgICAgIHJldHVybiBwYWdlVGV4dDtcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpZiAocGFnZXMubGVuZ3RoID09PSAwIHx8IHBhZ2VzLmV2ZXJ5KHAgPT4gIXAudHJpbSgpKSkge1xuICAgICAgY29uc3QgZnVsbCA9IGF3YWl0IHBkZihidWZmZXIpO1xuICAgICAgcGFnZXMucHVzaChmdWxsLnRleHQpO1xuICAgIH1cblxuICAgIGNvbnN0IHRvdGFsUGFnZXMgPSBwYWdlcy5sZW5ndGg7XG4gICAgY29uc3QgY2xlYW5lZFBhZ2VzID0gcGFnZXMubWFwKHAgPT4gY2xlYW5UZXh0KHApKTtcbiAgICBjb25zdCBwYWdlTWFwID0gW107XG4gICAgbGV0IGNoYXJQb3MgPSAwO1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBjbGVhbmVkUGFnZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHBhZ2VNYXAucHVzaCh7IHBhZ2U6IGkgKyAxLCBzdGFydDogY2hhclBvcywgZW5kOiBjaGFyUG9zICsgY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCB9KTtcbiAgICAgIGNoYXJQb3MgKz0gY2xlYW5lZFBhZ2VzW2ldLmxlbmd0aCArIDE7XG4gICAgfVxuXG4gICAgY29uc3QgZnVsbFRleHQgPSBjbGVhbmVkUGFnZXMuam9pbignXFxuJyk7XG4gICAgcmV0dXJuIHsgZnVsbFRleHQsIHBhZ2VNYXAsIHRvdGFsUGFnZXMgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdQREYgcGFyc2luZyBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgbmV3IENvcnJ1cHRlZFBERkVycm9yKCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0UGFnZU51bWJlcihjaGFyU3RhcnQsIHBhZ2VNYXApIHtcbiAgZm9yIChjb25zdCBlbnRyeSBvZiBwYWdlTWFwKSB7XG4gICAgaWYgKGNoYXJTdGFydCA+PSBlbnRyeS5zdGFydCAmJiBjaGFyU3RhcnQgPCBlbnRyeS5lbmQpIHJldHVybiBlbnRyeS5wYWdlO1xuICB9XG4gIHJldHVybiBwYWdlTWFwW3BhZ2VNYXAubGVuZ3RoIC0gMV0/LnBhZ2UgfHwgMTtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVVwbG9hZChyZXEsIHJlcykge1xuICB0cnkge1xuICAgIGNvbnN0IGZpbGUgPSByZXEuZmlsZTtcbiAgICBpZiAoIWZpbGUpIHRocm93IG5ldyBJbnZhbGlkRmlsZVR5cGVFcnJvcigpO1xuXG4gICAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5ib2R5LnNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgICBjb25zdCBzZXNzaW9uID0gZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG4gICAgY29uc3QgbWF4UERGcyA9IHBhcnNlSW50KHByb2Nlc3MuZW52Lk1BWF9QREZTX1BFUl9TRVNTSU9OIHx8ICczJyk7XG4gICAgY29uc3QgY2xlYW5GaWxlbmFtZSA9IHNhbml0aXplRmlsZW5hbWUoZmlsZS5vcmlnaW5hbG5hbWUpO1xuXG4gICAgLy8gQ291bnQgb25seSB1c2VyLXVwbG9hZGVkIGRvY3MgKG5vdCBnbG9iYWwgc2VlZHMpIHRvd2FyZCB0aGUgbGltaXRcbiAgICBjb25zdCB1cGxvYWRlZENvdW50ID0gc2Vzc2lvbi5kb2N1bWVudHMuZmlsdGVyKGQgPT4gZC5zb3VyY2VUeXBlID09PSAnc2Vzc2lvbl91cGxvYWQnKS5sZW5ndGg7XG4gICAgaWYgKHVwbG9hZGVkQ291bnQgPj0gbWF4UERGcykge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgdGhyb3cgbmV3IFRvb01hbnlQREZzRXJyb3IobWF4UERGcyk7XG4gICAgfVxuXG4gICAgaWYgKHNlc3Npb24uZG9jdW1lbnRzLnNvbWUoZCA9PiBkLmZpbGVuYW1lID09PSBjbGVhbkZpbGVuYW1lKSkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgdGhyb3cgbmV3IER1cGxpY2F0ZUZpbGVFcnJvcihjbGVhbkZpbGVuYW1lKTtcbiAgICB9XG5cbiAgICBjb25zdCB7IGZ1bGxUZXh0LCBwYWdlTWFwLCB0b3RhbFBhZ2VzIH0gPSBhd2FpdCBwYXJzZVBERldpdGhCb3VuZGFyeU1hcChmaWxlLnBhdGgpO1xuXG4gICAgaWYgKCFmdWxsVGV4dCB8fCBmdWxsVGV4dC50cmltKCkubGVuZ3RoIDwgNTApIHtcbiAgICAgIGZzLnVubGlua1N5bmMoZmlsZS5wYXRoKTtcbiAgICAgIHJldHVybiByZXMuc3RhdHVzKDQyMikuanNvbih7XG4gICAgICAgIGVycm9yOiAnTm8gZXh0cmFjdGFibGUgdGV4dCBmb3VuZCBcdTIwMTQgUERGIG1heSBiZSBzY2FubmVkIG9yIGltYWdlLW9ubHknLFxuICAgICAgICBjb2RlOiAnRU1QVFlfUERGJ1xuICAgICAgfSk7XG4gICAgfVxuXG4gICAgY29uc3QgZG9jdW1lbnRJZCA9IHBhdGgucGFyc2UoZmlsZS5maWxlbmFtZSkubmFtZTtcblxuICAgIGNvbnN0IHJhd0NodW5rcyA9IGNodW5rVGV4dChmdWxsVGV4dCwge1xuICAgICAgY2h1bmtTaXplVG9rZW5zOiAxMDAwLFxuICAgICAgb3ZlcmxhcFRva2VuczogMjAwXG4gICAgfSk7XG5cbiAgICBpZiAocmF3Q2h1bmtzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNDIyKS5qc29uKHsgZXJyb3I6ICdObyBjb250ZW50IGNvdWxkIGJlIGV4dHJhY3RlZCBmcm9tIFBERicsIGNvZGU6ICdFTVBUWV9QREYnIH0pO1xuICAgIH1cblxuICAgIGNvbnN0IGNodW5rcyA9IHJhd0NodW5rcy5tYXAoKGNodW5rLCBpZHgpID0+ICh7XG4gICAgICB0ZXh0OiBjaHVuay50ZXh0LFxuICAgICAgbWV0YWRhdGE6IHtcbiAgICAgICAgZG9jdW1lbnRfaWQ6IGRvY3VtZW50SWQsXG4gICAgICAgIGZpbGVuYW1lOiBjbGVhbkZpbGVuYW1lLFxuICAgICAgICBjaHVua19pZDogY3JlYXRlSGFzaCgnbWQ1JykudXBkYXRlKGAke2NsZWFuRmlsZW5hbWV9Ojoke2NodW5rLnRleHR9YCkuZGlnZXN0KCdoZXgnKS5zbGljZSgwLCAxNiksXG4gICAgICAgIGNodW5rX2luZGV4OiBpZHgsXG4gICAgICAgIHRvdGFsX2NodW5rczogcmF3Q2h1bmtzLmxlbmd0aCxcbiAgICAgICAgcGFnZV9udW1iZXI6IGdldFBhZ2VOdW1iZXIoY2h1bmsuY2hhclN0YXJ0LCBwYWdlTWFwKSxcbiAgICAgICAgdG90YWxfcGFnZXM6IHRvdGFsUGFnZXMsXG4gICAgICAgIHNvdXJjZV90eXBlOiAnc2Vzc2lvbl91cGxvYWQnLCAgLy8gYWx3YXlzIHNlc3Npb25fdXBsb2FkIGZvciB1c2VyIHVwbG9hZHNcbiAgICAgICAgdXBsb2FkX3RpbWVzdGFtcDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBjaGFyX3N0YXJ0OiBjaHVuay5jaGFyU3RhcnQsXG4gICAgICAgIGNoYXJfZW5kOiBjaHVuay5jaGFyRW5kLFxuICAgICAgICB0b2tlbl9jb3VudDogY2h1bmsudG9rZW5Db3VudFxuICAgICAgfVxuICAgIH0pKTtcblxuICAgIC8vIFVwbG9hZCBhbHdheXMgdGFyZ2V0cyBzZXNzaW9uIGNvbGxlY3Rpb24gXHUyMDE0IG5ldmVyIGdsb2JhbFxuICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuXG4gICAgY29uc3QgZW1iZWRkaW5ncyA9IGF3YWl0IGdlbmVyYXRlRW1iZWRkaW5ncyhcbiAgICAgIGNodW5rcyxcbiAgICAgICdSRVRSSUVWQUxfRE9DVU1FTlQnLFxuICAgICAgKHsgY3VycmVudF9iYXRjaCwgdG90YWxfYmF0Y2hlcyB9KSA9PiB7XG4gICAgICAgIGlmIChyZXEuYXBwLmxvY2Fscy5wcm9ncmVzc0NhbGxiYWNrcykge1xuICAgICAgICAgIHJlcS5hcHAubG9jYWxzLnByb2dyZXNzQ2FsbGJhY2tzLmVtaXQoYHByb2dyZXNzXyR7c2Vzc2lvbklkfWAsIHtcbiAgICAgICAgICAgIGRvY3VtZW50SWQsXG4gICAgICAgICAgICBjdXJyZW50X2JhdGNoLFxuICAgICAgICAgICAgdG90YWxfYmF0Y2hlcyxcbiAgICAgICAgICAgIHN0YWdlOiAnZW1iZWRkaW5nJ1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgKTtcblxuICAgIGlmIChlbWJlZGRpbmdzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZnMudW5saW5rU3luYyhmaWxlLnBhdGgpO1xuICAgICAgcmV0dXJuIHJlcy5zdGF0dXMoNTAzKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gZ2VuZXJhdGUgZW1iZWRkaW5ncycsIGNvZGU6ICdFTUJFRERJTkdfRkFJTEVEJyB9KTtcbiAgICB9XG5cbiAgICBhd2FpdCBhZGRWZWN0b3JzKFxuICAgICAgY29sbGVjdGlvbixcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gKHsgdGV4dDogZS50ZXh0LCBtZXRhZGF0YTogZS5tZXRhZGF0YSB9KSksXG4gICAgICBlbWJlZGRpbmdzLm1hcChlID0+IGUuZW1iZWRkaW5nKSxcbiAgICAgIGVtYmVkZGluZ3MubWFwKGUgPT4gZS5pZClcbiAgICApO1xuXG4gICAgLy8gSW52YWxpZGF0ZSByZXRyaWV2YWwgY2FjaGUgc28gbmV4dCBxdWVyeSBwaWNrcyB1cCBuZXcgdmVjdG9yc1xuICAgIGludmFsaWRhdGVTZXNzaW9uQ29sbGVjdGlvbkNhY2hlKHNlc3Npb25JZCk7XG5cbiAgICBhZGREb2N1bWVudFRvU2Vzc2lvbihzZXNzaW9uSWQsIHtcbiAgICAgIGlkOiBkb2N1bWVudElkLFxuICAgICAgZmlsZW5hbWU6IGNsZWFuRmlsZW5hbWUsXG4gICAgICBmaWxlU2l6ZTogZmlsZS5zaXplLFxuICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLFxuICAgICAgY2h1bmtDb3VudDogZW1iZWRkaW5ncy5sZW5ndGhcbiAgICB9KTtcblxuICAgIC8vIEZpbGUgc3RheXMgaW4gL3RtcCBcdTIwMTQgbm90IGRlbGV0ZWQgYWZ0ZXIgdXBsb2FkXG4gICAgcmVzLnN0YXR1cygyMDEpLmpzb24oe1xuICAgICAgc3VjY2VzczogdHJ1ZSxcbiAgICAgIGRvY3VtZW50OiB7XG4gICAgICAgIGlkOiBkb2N1bWVudElkLFxuICAgICAgICBmaWxlbmFtZTogY2xlYW5GaWxlbmFtZSxcbiAgICAgICAgZmlsZVNpemU6IGZpbGUuc2l6ZSxcbiAgICAgICAgcGFnZUNvdW50OiB0b3RhbFBhZ2VzLFxuICAgICAgICBjaHVua0NvdW50OiBlbWJlZGRpbmdzLmxlbmd0aCxcbiAgICAgICAgdXBsb2FkVGltZXN0YW1wOiBuZXcgRGF0ZSgpLnRvSVNPU3RyaW5nKClcbiAgICAgIH0sXG4gICAgICBzZXNzaW9uSWRcbiAgICB9KTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGlmIChyZXEuZmlsZSAmJiBmcy5leGlzdHNTeW5jKHJlcS5maWxlLnBhdGgpKSB7XG4gICAgICBmcy51bmxpbmtTeW5jKHJlcS5maWxlLnBhdGgpO1xuICAgIH1cbiAgICBjb25zb2xlLmVycm9yKCdVcGxvYWQgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoZXJyb3Iuc3RhdHVzQ29kZSB8fCA1MDApLmpzb24oe1xuICAgICAgZXJyb3I6IGVycm9yLm1lc3NhZ2UsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdVUExPQURfRVJST1InXG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGxpc3REb2N1bWVudHNIYW5kbGVyKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHNlc3Npb25JZCA9IHJlcS5oZWFkZXJzWyd4LXNlc3Npb24taWQnXSB8fCByZXEucXVlcnkuc2Vzc2lvbklkO1xuICB0cnkge1xuICAgIGNvbnN0IGRvY3VtZW50cyA9IGF3YWl0IGdldEFsbERvY3VtZW50cyhzZXNzaW9uSWQpO1xuICAgIHJlcy5qc29uKGRvY3VtZW50cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignTGlzdCBkb2N1bWVudHMgZXJyb3I6JywgZXJyb3IpO1xuICAgIHJlcy5zdGF0dXMoNTAwKS5qc29uKHsgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBkb2N1bWVudHMnLCBjb2RlOiAnTElTVF9FUlJPUicgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGRlbGV0ZURvY3VtZW50KHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3Qgc2Vzc2lvbklkID0gcmVxLmhlYWRlcnNbJ3gtc2Vzc2lvbi1pZCddIHx8IHJlcS5xdWVyeS5zZXNzaW9uSWQ7XG5cbiAgdHJ5IHtcbiAgICAvLyBPbmx5IGRlbGV0ZSBmcm9tIHNlc3Npb24gY29sbGVjdGlvbiBcdTIwMTQgbmV2ZXIgdG91Y2hlcyBnbG9iYWwgY29sbGVjdGlvblxuICAgIGlmIChzZXNzaW9uSWQpIHtcbiAgICAgIGNvbnN0IGNvbGxlY3Rpb24gPSBhd2FpdCBnZXRTZXNzaW9uQ29sbGVjdGlvbihzZXNzaW9uSWQpO1xuICAgICAgaWYgKGNvbGxlY3Rpb24pIHtcbiAgICAgICAgY29uc3QgY291bnQgPSBhd2FpdCBkZWxldGVEb2N1bWVudFZlY3RvcnMoY29sbGVjdGlvbiwgZG9jdW1lbnRJZCk7XG4gICAgICAgIGlmIChjb3VudCA+IDApIHtcbiAgICAgICAgICByZW1vdmVEb2N1bWVudEZyb21TZXNzaW9uKHNlc3Npb25JZCwgZG9jdW1lbnRJZCk7XG4gICAgICAgICAgaW52YWxpZGF0ZVNlc3Npb25Db2xsZWN0aW9uQ2FjaGUoc2Vzc2lvbklkKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIC8vIERlbGV0ZSB0aGUgdXBsb2FkZWQgZmlsZSBmcm9tIC90bXAgb25seSAobmV2ZXIgZnJvbSBzZWVkX2RvY3VtZW50cylcbiAgICBjb25zdCB0bXBQYXRoID0gcGF0aC5qb2luKHVwbG9hZERpciwgYCR7ZG9jdW1lbnRJZH0ucGRmYCk7XG4gICAgaWYgKGZzLmV4aXN0c1N5bmModG1wUGF0aCkpIHtcbiAgICAgIGZzLnVubGlua1N5bmModG1wUGF0aCk7XG4gICAgICBjb25zb2xlLmxvZyhgXHVEODNEXHVEREQxXHVGRTBGICBEZWxldGVkIHRtcCBmaWxlOiAke3RtcFBhdGh9YCk7XG4gICAgfVxuXG4gICAgcmVzLmpzb24oeyBzdWNjZXNzOiB0cnVlLCBkb2N1bWVudElkIH0pO1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0RlbGV0ZSBkb2N1bWVudCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byBkZWxldGUgZG9jdW1lbnQnLCBjb2RlOiAnREVMRVRFX0VSUk9SJyB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RG9jdW1lbnRGaWxlKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgZG9jdW1lbnRJZCB9ID0gcmVxLnBhcmFtcztcbiAgY29uc3QgZmlsZW5hbWUgPSByZXEucXVlcnkuZmlsZW5hbWU7XG5cbiAgdHJ5IHtcbiAgICAvLyBDaGVjayAvdG1wIGZpcnN0ICh1c2VyLXVwbG9hZGVkKVxuICAgIGNvbnN0IHVwbG9hZFBhdGggPSBwYXRoLmpvaW4odXBsb2FkRGlyLCBgJHtkb2N1bWVudElkfS5wZGZgKTtcbiAgICBpZiAoZnMuZXhpc3RzU3luYyh1cGxvYWRQYXRoKSkge1xuICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1EaXNwb3NpdGlvbicsIGBpbmxpbmU7IGZpbGVuYW1lPVwiJHtkb2N1bWVudElkfS5wZGZcImApO1xuICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0odXBsb2FkUGF0aCkucGlwZShyZXMpO1xuICAgIH1cblxuICAgIC8vIFNlZWQgZG9jIFx1MjAxNCBzZXJ2ZSBmcm9tIHNlZWRfZG9jdW1lbnRzIChyZWFkLW9ubHksIG5ldmVyIGRlbGV0ZWQpXG4gICAgaWYgKGZpbGVuYW1lKSB7XG4gICAgICBjb25zdCBzZWVkUGF0aCA9IHBhdGguam9pbihzZWVkRGlyLCBmaWxlbmFtZSk7XG4gICAgICBpZiAoZnMuZXhpc3RzU3luYyhzZWVkUGF0aCkpIHtcbiAgICAgICAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ2FwcGxpY2F0aW9uL3BkZicpO1xuICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgYGlubGluZTsgZmlsZW5hbWU9XCIke2ZpbGVuYW1lfVwiYCk7XG4gICAgICAgIHJldHVybiBmcy5jcmVhdGVSZWFkU3RyZWFtKHNlZWRQYXRoKS5waXBlKHJlcyk7XG4gICAgICB9XG5cbiAgICAgIC8vIEZhbGxiYWNrOiBzY2FuIHNlZWREaXJcbiAgICAgIGlmIChmcy5leGlzdHNTeW5jKHNlZWREaXIpKSB7XG4gICAgICAgIGNvbnN0IGFsbFBkZnMgPSBmcy5yZWFkZGlyU3luYyhzZWVkRGlyKS5maWx0ZXIoZiA9PiBmLmVuZHNXaXRoKCcucGRmJykpO1xuICAgICAgICBjb25zdCBtYXRjaCA9IGFsbFBkZnMuZmluZChmID0+IGYuaW5jbHVkZXMocGF0aC5wYXJzZShmaWxlbmFtZSkubmFtZSkpO1xuICAgICAgICBpZiAobWF0Y2gpIHtcbiAgICAgICAgICBjb25zdCBtYXRjaFBhdGggPSBwYXRoLmpvaW4oc2VlZERpciwgbWF0Y2gpO1xuICAgICAgICAgIHJlcy5zZXRIZWFkZXIoJ0NvbnRlbnQtVHlwZScsICdhcHBsaWNhdGlvbi9wZGYnKTtcbiAgICAgICAgICByZXMuc2V0SGVhZGVyKCdDb250ZW50LURpc3Bvc2l0aW9uJywgYGlubGluZTsgZmlsZW5hbWU9XCIke21hdGNofVwiYCk7XG4gICAgICAgICAgcmV0dXJuIGZzLmNyZWF0ZVJlYWRTdHJlYW0obWF0Y2hQYXRoKS5waXBlKHJlcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDQpLmpzb24oeyBlcnJvcjogJ0RvY3VtZW50IGZpbGUgbm90IGZvdW5kJywgY29kZTogJ0ZJTEVfTk9UX0ZPVU5EJyB9KTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBjb25zb2xlLmVycm9yKCdHZXQgZG9jdW1lbnQgZmlsZSBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oeyBlcnJvcjogJ0ZhaWxlZCB0byByZXRyaWV2ZSBkb2N1bWVudCcsIGNvZGU6ICdSRVRSSUVWRV9FUlJPUicgfSk7XG4gIH1cbn1cblxucm91dGVyLnBvc3QoJy91cGxvYWQnLCB1cGxvYWQuc2luZ2xlKCdmaWxlJyksIGhhbmRsZVVwbG9hZCk7XG5yb3V0ZXIuZ2V0KCcvJywgbGlzdERvY3VtZW50c0hhbmRsZXIpO1xucm91dGVyLmRlbGV0ZSgnLzpkb2N1bWVudElkJywgZGVsZXRlRG9jdW1lbnQpO1xucm91dGVyLmdldCgnLzpkb2N1bWVudElkL2ZpbGUnLCBnZXREb2N1bWVudEZpbGUpO1xuXG5leHBvcnQgZGVmYXVsdCByb3V0ZXI7IiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvbWVtb3J5U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzXCI7Y29uc3QgbWVtb3J5TWFwID0gbmV3IE1hcCgpO1xuY29uc3QgREVGQVVMVF9NRU1PUllfV0lORE9XID0gcGFyc2VJbnQocHJvY2Vzcy5lbnYuTUVNT1JZX1dJTkRPV19UVVJOUykgfHwgMTA7XG5cbmV4cG9ydCBmdW5jdGlvbiBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCkge1xuICBpZiAoIW1lbW9yeU1hcC5oYXMoc2Vzc2lvbklkKSkge1xuICAgIG1lbW9yeU1hcC5zZXQoc2Vzc2lvbklkLCB7XG4gICAgICB0dXJuczogW10sXG4gICAgICBjcmVhdGVkQXQ6IG5ldyBEYXRlKClcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIG1ldGFkYXRhID0ge30pIHtcbiAgY29uc3QgbWVtb3J5ID0gbWVtb3J5TWFwLmdldChzZXNzaW9uSWQpIHx8IGluaXRpYWxpemVNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbWF4VHVybnMgPSBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG5cbiAgY29uc3QgdHVybiA9IHtcbiAgICBpZDogYHR1cm5fJHtEYXRlLm5vdygpfV8ke01hdGgucmFuZG9tKCkudG9TdHJpbmcoMzYpLnN1YnN0cigyLCA5KX1gLFxuICAgIHJvbGUsXG4gICAgY29udGVudCxcbiAgICB0aW1lc3RhbXA6IG5ldyBEYXRlKCksXG4gICAgLi4ubWV0YWRhdGFcbiAgfTtcblxuICBtZW1vcnkudHVybnMucHVzaCh0dXJuKTtcblxuICBpZiAobWVtb3J5LnR1cm5zLmxlbmd0aCA+IG1heFR1cm5zKSB7XG4gICAgbWVtb3J5LnR1cm5zID0gbWVtb3J5LnR1cm5zLnNsaWNlKC1tYXhUdXJucyk7XG4gIH1cblxuICByZXR1cm4gdHVybjtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeShzZXNzaW9uSWQpIHtcbiAgcmV0dXJuIG1lbW9yeU1hcC5nZXQoc2Vzc2lvbklkKSB8fCBpbml0aWFsaXplTWVtb3J5KHNlc3Npb25JZCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWNlbnRUdXJucyhzZXNzaW9uSWQsIG1heFR1cm5zID0gbnVsbCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgY29uc3QgbGltaXQgPSBtYXhUdXJucyB8fCBwYXJzZUludChwcm9jZXNzLmVudi5NRU1PUllfV0lORE9XX1RVUk5TKSB8fCBERUZBVUxUX01FTU9SWV9XSU5ET1c7XG4gIHJldHVybiBtZW1vcnkudHVybnMuc2xpY2UoLWxpbWl0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbnZlcnNhdGlvbkNvbnRleHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHR1cm5zLm1hcCh0ID0+ICh7XG4gICAgcm9sZTogdC5yb2xlLFxuICAgIGNvbnRlbnQ6IHQuY29udGVudFxuICB9KSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRNZW1vcnlGb3JQcm9tcHQoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IHR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkKTtcbiAgaWYgKHR1cm5zLmxlbmd0aCA9PT0gMCkgcmV0dXJuICcnO1xuXG4gIHJldHVybiB0dXJucy5tYXAodCA9PiB7XG4gICAgY29uc3QgcHJlZml4ID0gdC5yb2xlID09PSAndXNlcicgPyAnVXNlcjonIDogJ0Fzc2lzdGFudDonO1xuICAgIHJldHVybiBgJHtwcmVmaXh9ICR7dC5jb250ZW50fWA7XG4gIH0pLmpvaW4oJ1xcblxcbicpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2xlYXJNZW1vcnkoc2Vzc2lvbklkKSB7XG4gIG1lbW9yeU1hcC5kZWxldGUoc2Vzc2lvbklkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE1lbW9yeVN0YXRzKHNlc3Npb25JZCkge1xuICBjb25zdCBtZW1vcnkgPSBnZXRNZW1vcnkoc2Vzc2lvbklkKTtcbiAgcmV0dXJuIHtcbiAgICB0dXJuQ291bnQ6IG1lbW9yeS50dXJucy5sZW5ndGgsXG4gICAgY3JlYXRlZEF0OiBtZW1vcnkuY3JlYXRlZEF0LFxuICAgIGxhc3RUdXJuQXQ6IG1lbW9yeS50dXJucy5sZW5ndGggPiAwID8gbWVtb3J5LnR1cm5zW21lbW9yeS50dXJucy5sZW5ndGggLSAxXS50aW1lc3RhbXAgOiBudWxsXG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIGNpdGF0aW9ucyA9IFtdLCBjb3ZlcmFnZSA9IG51bGwsIGFuc3dlcklkID0gbnVsbCkge1xuICByZXR1cm4gYWRkVHVybihzZXNzaW9uSWQsIHJvbGUsIGNvbnRlbnQsIHtcbiAgICAuLi4oYW5zd2VySWQgJiYgeyBpZDogYW5zd2VySWQgfSksXG4gICAgY2l0YXRpb25zLFxuICAgIGNvdmVyYWdlLFxuICAgIGhhc0NpdGF0aW9uczogY2l0YXRpb25zLmxlbmd0aCA+IDBcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0VXNlck1lc3NhZ2Uoc2Vzc2lvbklkKSB7XG4gIGNvbnN0IG1lbW9yeSA9IGdldE1lbW9yeShzZXNzaW9uSWQpO1xuICBmb3IgKGxldCBpID0gbWVtb3J5LnR1cm5zLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKG1lbW9yeS50dXJuc1tpXS5yb2xlID09PSAndXNlcicpIHJldHVybiBtZW1vcnkudHVybnNbaV07XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRMYXN0QXNzaXN0YW50TWVzc2FnZShzZXNzaW9uSWQpIHtcbiAgY29uc3QgbWVtb3J5ID0gZ2V0TWVtb3J5KHNlc3Npb25JZCk7XG4gIGZvciAobGV0IGkgPSBtZW1vcnkudHVybnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBpZiAobWVtb3J5LnR1cm5zW2ldLnJvbGUgPT09ICdhc3Npc3RhbnQnKSByZXR1cm4gbWVtb3J5LnR1cm5zW2ldO1xuICB9XG4gIHJldHVybiBudWxsO1xufVxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvcHJvbXB0U2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9wcm9tcHRTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgZm9ybWF0TWVtb3J5Rm9yUHJvbXB0IH0gZnJvbSAnLi9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGZvcm1hdENvbnRleHRGb3JQcm9tcHQsIGNhbGN1bGF0ZUNvdmVyYWdlIH0gZnJvbSAnLi9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcblxuY29uc3QgU1lTVEVNX0lOU1RSVUNUSU9OID0gYFlvdSBhcmUgYW4gQUkgS25vd2xlZGdlIEFzc2lzdGFudCB0aGF0IGFuc3dlcnMgcXVlc3Rpb25zIGJhc2VkIG9uIGluZGV4ZWQgZG9jdW1lbnRzIHdoZW4gYXZhaWxhYmxlLlxuXG5SVUxFUzpcbjEuIFdoZW4gY29udGV4dCBpcyBwcm92aWRlZCwgYW5zd2VyIGJhc2VkIG9uIGl0IGFuZCBjaXRlIHNvdXJjZXMgdXNpbmcgWzFdLCBbMl0sIGV0Yy5cbjIuIEZvciBnZW5lcmFsIGNvbnZlcnNhdGlvbiAoZ3JlZXRpbmdzLCBjbGFyaWZ5aW5nIHF1ZXN0aW9ucywgc21hbGwgdGFsayksIHJlc3BvbmQgbmF0dXJhbGx5IGFuZCBoZWxwZnVsbHkgd2l0aG91dCByZXF1aXJpbmcgY29udGV4dC5cbjMuIElmIGEgZmFjdHVhbCBxdWVzdGlvbiBpcyBhc2tlZCBidXQgY29udGV4dCBpcyBpbnN1ZmZpY2llbnQsIHNheSBzbyBjbGVhcmx5IGFuZCBzdWdnZXN0IHVwbG9hZGluZyByZWxldmFudCBkb2N1bWVudHMuXG40LiBCZSBjb25jaXNlIGJ1dCB0aG9yb3VnaC4gVXNlIGJ1bGxldCBwb2ludHMgb3IgbnVtYmVyZWQgbGlzdHMgZm9yIGNvbXBsZXggYW5zd2Vycy5cbjUuIE1haW50YWluIGNvbnZlcnNhdGlvbiBjb250aW51aXR5IGJ1dCBkb24ndCByZXBlYXQgaW5mb3JtYXRpb24gdW5uZWNlc3NhcmlseS5cbjYuIEZvcm1hdCByZXNwb25zZXMgaW4gY2xlYXIsIHJlYWRhYmxlIG1hcmtkb3duLmA7XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFByb21wdCh7IHF1ZXJ5LCBjb250ZXh0LCBtZW1vcnlDb250ZXh0LCBjb3ZlcmFnZSB9KSB7XG4gIGNvbnN0IHBhcnRzID0gW107XG4gIHBhcnRzLnB1c2goU1lTVEVNX0lOU1RSVUNUSU9OKTtcbiAgaWYgKG1lbW9yeUNvbnRleHQpIHtcbiAgICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gUFJFVklPVVMgQ09OVkVSU0FUSU9OIC0tLVxcbicpO1xuICAgIHBhcnRzLnB1c2gobWVtb3J5Q29udGV4dCk7XG4gICAgcGFydHMucHVzaCgnXFxuLS0tIEVORCBQUkVWSU9VUyBDT05WRVJTQVRJT04gLS0tXFxuJyk7XG4gIH1cbiAgaWYgKGNvbnRleHQpIHtcbiAgICBwYXJ0cy5wdXNoKCdcXG5cXG4tLS0gUkVMRVZBTlQgQ09OVEVYVCBGUk9NIEtOT1dMRURHRSBCQVNFIC0tLVxcbicpO1xuICAgIHBhcnRzLnB1c2goY29udGV4dCk7XG4gICAgcGFydHMucHVzaCgnXFxuLS0tIEVORCBDT05URVhUIC0tLVxcbicpO1xuICB9XG4gIHBhcnRzLnB1c2goJ1xcblxcbi0tLSBDVVJSRU5UIFFVRVNUSU9OIC0tLVxcbicpO1xuICBwYXJ0cy5wdXNoKHF1ZXJ5KTtcbiAgcGFydHMucHVzaCgnXFxuXFxuUmVtZW1iZXI6IEFuc3dlciBiYXNlZCBPTkxZIG9uIHRoZSBwcm92aWRlZCBjb250ZXh0LiBVc2UgWzFdLCBbMl0sIGV0Yy4gZm9yIGNpdGF0aW9ucy4gSWYgdGhlIGNvbnRleHQgaXMgaW5zdWZmaWNpZW50LCBzYXkgc28gY2xlYXJseS4nKTtcbiAgcmV0dXJuIHBhcnRzLmpvaW4oJycpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYnVpbGRTdHJlYW1pbmdQcm9tcHQocXVlcnksIHJldHJpZXZlZFJlc3VsdHMsIHNlc3Npb25JZCwgbWVtb3J5U2VydmljZSkge1xuICBjb25zdCBtZW1vcnlDb250ZXh0ID0gZm9ybWF0TWVtb3J5Rm9yUHJvbXB0KHNlc3Npb25JZCk7XG4gIGNvbnN0IGNvbnRleHRTdHJpbmcgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJldHJpZXZlZFJlc3VsdHMpO1xuICByZXR1cm4gYnVpbGRQcm9tcHQoe1xuICAgIHF1ZXJ5LFxuICAgIGNvbnRleHQ6IGNvbnRleHRTdHJpbmcsXG4gICAgbWVtb3J5Q29udGV4dCxcbiAgICBjb3ZlcmFnZTogY2FsY3VsYXRlQ292ZXJhZ2UocmV0cmlldmVkUmVzdWx0cylcbiAgfSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZWZ1c2FsUmVzcG9uc2UoKSB7XG4gIC8vIE5vIGxvbmdlciB1c2VkIFx1MjAxNCBMTE0gZ2VuZXJhdGVzIGl0cyBvd24gbmF0dXJhbCByZWZ1c2FsXG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U3lzdGVtSW5zdHJ1Y3Rpb24oKSB7XG4gIHJldHVybiBTWVNURU1fSU5TVFJVQ1RJT047XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBidWlsZFdlYlNlYXJjaFByb21wdChxdWVyeSwgZ3JvdW5kaW5nTWV0YWRhdGEpIHtcbiAgcmV0dXJuIGBCYXNlZCBvbiB3ZWIgc2VhcmNoIHJlc3VsdHMsIGFuc3dlciB0aGUgZm9sbG93aW5nIHF1ZXN0aW9uOiAke3F1ZXJ5fVxuXG5HdWlkZWxpbmVzOlxuLSBVc2UgaW5mb3JtYXRpb24gZnJvbSB0aGUgd2ViIHNlYXJjaFxuLSBQcm92aWRlIHNvdXJjZXMvVVJMcyB3aGVyZSBhcHBsaWNhYmxlXG4tIEJlIGNvbmNpc2UgYW5kIGluZm9ybWF0aXZlXG4tIElmIG11bHRpcGxlIHNvdXJjZXMgYWdyZWUgb3IgY29udHJhZGljdCwgbWVudGlvbiB0aGF0YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZvcm1hdEdlbmVyYXRpb25Db25maWcoY3VzdG9tQ29uZmlnID0ge30pIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgIHRvcFA6IDAuOTUsXG4gICAgdG9wSzogNDAsXG4gICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4LFxuICAgIC4uLmN1c3RvbUNvbmZpZ1xuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZXh0cmFjdFNvdXJjZXNGcm9tUmVzcG9uc2UocmVzcG9uc2UpIHtcbiAgY29uc3QgY2l0YXRpb25QYXR0ZXJuID0gL1xcWyhcXGQrKVxcXS9nO1xuICBjb25zdCBjaXRhdGlvbnMgPSBuZXcgU2V0KCk7XG4gIGxldCBtYXRjaDtcbiAgd2hpbGUgKChtYXRjaCA9IGNpdGF0aW9uUGF0dGVybi5leGVjKHJlc3BvbnNlKSkgIT09IG51bGwpIHtcbiAgICBjaXRhdGlvbnMuYWRkKHBhcnNlSW50KG1hdGNoWzFdKSk7XG4gIH1cbiAgcmV0dXJuIEFycmF5LmZyb20oY2l0YXRpb25zKS5zb3J0KChhLCBiKSA9PiBhIC0gYik7XG59XG4iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy9nZW1pbmlTZXJ2aWNlLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL3NlcnZpY2VzL2dlbWluaVNlcnZpY2UuanNcIjtpbXBvcnQgeyBHb29nbGVHZW5lcmF0aXZlQUkgfSBmcm9tICdAZ29vZ2xlL2dlbmVyYXRpdmUtYWknO1xuaW1wb3J0IHsgYnVpbGRQcm9tcHQsIGdldFJlZnVzYWxSZXNwb25zZSB9IGZyb20gJy4vcHJvbXB0U2VydmljZS5qcyc7XG5pbXBvcnQgeyBMTE1VbmF2YWlsYWJsZUVycm9yIH0gZnJvbSAnLi4vdXRpbHMvZXJyb3JzLmpzJztcblxubGV0IGdlbkFJID0gbnVsbDtcblxuZnVuY3Rpb24gZ2V0R2VuQUkoKSB7XG4gIGlmICghZ2VuQUkpIHtcbiAgICBjb25zdCBhcGlLZXkgPSBwcm9jZXNzLmVudi5HRU1JTklfQVBJX0tFWTtcbiAgICBpZiAoIWFwaUtleSkgdGhyb3cgbmV3IEVycm9yKCdHRU1JTklfQVBJX0tFWSBpcyB1bmRlZmluZWQnKTtcbiAgICBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkoYXBpS2V5KTtcbiAgfVxuICByZXR1cm4gZ2VuQUk7XG59XG5cbmNvbnN0IFBSSU1BUllfTU9ERUwgPSBwcm9jZXNzLmVudi5HRU1JTklfTU9ERUxfUFJJTUFSWSB8fCAnZ2VtaW5pLTMuMS1mbGFzaC1saXRlJztcbmNvbnN0IEZBTExCQUNLX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX0ZBTExCQUNLIHx8ICdnZW1pbmktMi41LWZsYXNoJztcbmNvbnN0IEZJUlNUX1RPS0VOX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fRklSU1RfVE9LRU5fVElNRU9VVF9TRUNPTkRTKSAqIDEwMDAgfHwgMTIwMDA7XG5jb25zdCBSRVFVRVNUX1RJTUVPVVQgPSBwYXJzZUludChwcm9jZXNzLmVudi5MTE1fUkVRVUVTVF9USU1FT1VUX1NFQ09ORFMpICogMTAwMCB8fCA0NTAwMDtcblxubGV0IHByaW1hcnlNb2RlbCA9IG51bGw7XG5sZXQgZmFsbGJhY2tNb2RlbCA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldFByaW1hcnlNb2RlbCgpIHtcbiAgaWYgKCFwcmltYXJ5TW9kZWwpIHtcbiAgICBwcmltYXJ5TW9kZWwgPSBnZXRHZW5BSSgpLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBQUklNQVJZX01PREVMIH0pO1xuICB9XG4gIHJldHVybiBwcmltYXJ5TW9kZWw7XG59XG5cbmZ1bmN0aW9uIGdldEZhbGxiYWNrTW9kZWwoKSB7XG4gIGlmICghZmFsbGJhY2tNb2RlbCkge1xuICAgIGZhbGxiYWNrTW9kZWwgPSBnZXRHZW5BSSgpLmdldEdlbmVyYXRpdmVNb2RlbCh7IG1vZGVsOiBGQUxMQkFDS19NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gZmFsbGJhY2tNb2RlbDtcbn1cblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGdlbmVyYXRlUmVzcG9uc2UocHJvbXB0KSB7XG4gIC8vIEZJWCA2OiBjcmVhdGUgY29udHJvbGxlciBhbmQgYWN0dWFsbHkgcGFzcyBzaWduYWwgdG8gZ2VuZXJhdGVDb250ZW50XG4gIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7XG4gIGNvbnN0IHRpbWVvdXRJZCA9IHNldFRpbWVvdXQoKCkgPT4gY29udHJvbGxlci5hYm9ydCgpLCBSRVFVRVNUX1RJTUVPVVQpO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgZ2V0UHJpbWFyeU1vZGVsKCkuZ2VuZXJhdGVDb250ZW50KFxuICAgICAge1xuICAgICAgICBjb250ZW50czogW3sgcm9sZTogJ3VzZXInLCBwYXJ0czogW3sgdGV4dDogcHJvbXB0IH1dIH1dLFxuICAgICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgICAgdGVtcGVyYXR1cmU6IDAuNyxcbiAgICAgICAgICB0b3BQOiAwLjk1LFxuICAgICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgICB9XG4gICAgICB9LFxuICAgICAgeyBzaWduYWw6IGNvbnRyb2xsZXIuc2lnbmFsIH0gIC8vIEZJWCA2OiBwYXNzIHNpZ25hbFxuICAgICk7XG5cbiAgICBjbGVhclRpbWVvdXQodGltZW91dElkKTtcbiAgICByZXR1cm4gcmVzdWx0LnJlc3BvbnNlLnRleHQoKTtcbiAgfSBjYXRjaCAocHJpbWFyeUVycm9yKSB7XG4gICAgY2xlYXJUaW1lb3V0KHRpbWVvdXRJZCk7XG4gICAgY29uc29sZS5lcnJvcignUHJpbWFyeSBtb2RlbCBmYWlsZWQ6JywgcHJpbWFyeUVycm9yLm1lc3NhZ2UpO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IGZhbGxiYWNrUmVzdWx0ID0gYXdhaXQgZ2V0RmFsbGJhY2tNb2RlbCgpLmdlbmVyYXRlQ29udGVudCh7XG4gICAgICAgIGNvbnRlbnRzOiBbeyByb2xlOiAndXNlcicsIHBhcnRzOiBbeyB0ZXh0OiBwcm9tcHQgfV0gfV0sXG4gICAgICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICAgIHRvcFA6IDAuOTUsXG4gICAgICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgICAgIH1cbiAgICAgIH0pO1xuXG4gICAgICByZXR1cm4gZmFsbGJhY2tSZXN1bHQucmVzcG9uc2UudGV4dCgpO1xuICAgIH0gY2F0Y2ggKGZhbGxiYWNrRXJyb3IpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoJ0ZhbGxiYWNrIG1vZGVsIGFsc28gZmFpbGVkOicsIGZhbGxiYWNrRXJyb3IubWVzc2FnZSk7XG4gICAgICB0aHJvdyBuZXcgTExNVW5hdmFpbGFibGVFcnJvcigpO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVJlc3BvbnNlKHByb21wdCkge1xuICBsZXQgbW9kZWwgPSBnZXRQcmltYXJ5TW9kZWwoKTtcbiAgbGV0IHJldHJpZXMgPSAwO1xuICBjb25zdCBtYXhSZXRyaWVzID0gMjtcblxuICB3aGlsZSAocmV0cmllcyA8IG1heFJldHJpZXMpIHtcbiAgICB0cnkge1xuICAgICAgY29uc3QgY29udHJvbGxlciA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTtcblxuICAgICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50U3RyZWFtKHtcbiAgICAgICAgY29udGVudHM6IFt7IHJvbGU6ICd1c2VyJywgcGFydHM6IFt7IHRleHQ6IHByb21wdCB9XSB9XSxcbiAgICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgICAgdG9wUDogMC45NSxcbiAgICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGxldCBmaXJzdFRva2VuID0gdHJ1ZTtcbiAgICAgIGNvbnN0IGZpcnN0VG9rZW5UaW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiBjb250cm9sbGVyLmFib3J0KCksIEZJUlNUX1RPS0VOX1RJTUVPVVQpO1xuXG4gICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHJlc3VsdC5zdHJlYW0pIHtcbiAgICAgICAgaWYgKGNvbnRyb2xsZXIuc2lnbmFsLmFib3J0ZWQpIHtcbiAgICAgICAgICBjbGVhclRpbWVvdXQoZmlyc3RUb2tlblRpbWVvdXQpO1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignRmlyc3QgdG9rZW4gdGltZW91dCBcdTIwMTQgbm8gcmVzcG9uc2UgZnJvbSBtb2RlbCcpO1xuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdGV4dCA9IGNodW5rLnRleHQoKTtcbiAgICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgICBpZiAoZmlyc3RUb2tlbikge1xuICAgICAgICAgICAgZmlyc3RUb2tlbiA9IGZhbHNlO1xuICAgICAgICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgICAgICB9XG4gICAgICAgICAgeWllbGQgeyB0eXBlOiAndG9rZW4nLCB0ZXh0IH07XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgY2xlYXJUaW1lb3V0KGZpcnN0VG9rZW5UaW1lb3V0KTtcbiAgICAgIHJldHVybiB7IHN1Y2Nlc3M6IHRydWUgfTtcblxuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICByZXRyaWVzKys7XG4gICAgICBjb25zb2xlLmVycm9yKGBNb2RlbCBhdHRlbXB0ICR7cmV0cmllc30gZmFpbGVkOmAsIGVycm9yLm1lc3NhZ2UpO1xuXG4gICAgICBpZiAocmV0cmllcyA+PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ2Vycm9yJywgZXJyb3I6IGVycm9yLm1lc3NhZ2UgfTtcbiAgICAgICAgdGhyb3cgbmV3IExMTVVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgICAgIH1cblxuICAgICAgbW9kZWwgPSBnZXRGYWxsYmFja01vZGVsKCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiogc3RyZWFtQ2hhdFJlc3BvbnNlKHF1ZXJ5LCByZXRyaWV2ZWRSZXN1bHRzLCBzZXNzaW9uSWQsIG1lbW9yeVNlcnZpY2UpIHtcbiAgY29uc3QgbWVtb3J5Q29udGV4dCA9IG1lbW9yeVNlcnZpY2UgPyBtZW1vcnlTZXJ2aWNlLmZvcm1hdE1lbW9yeUZvclByb21wdChzZXNzaW9uSWQpIDogJyc7XG4gIGNvbnN0IGNvbnRleHRMaXN0ID0gcmV0cmlldmVkUmVzdWx0cyB8fCBbXTtcbiAgY29uc3QgY29udGV4dFRleHQgPSBjb250ZXh0TGlzdC5tYXAoKHIsIGkpID0+XG4gICAgYFske2kgKyAxfV0gJHtyLm1ldGFkYXRhLmZpbGVuYW1lIHx8ICdVbmtub3duJ306ICR7ci50ZXh0fWBcbiAgKS5qb2luKCdcXG5cXG4nKTtcblxuICBjb25zdCBwcm9tcHQgPSBidWlsZFByb21wdCh7XG4gICAgcXVlcnksXG4gICAgY29udGV4dDogY29udGV4dFRleHQsXG4gICAgbWVtb3J5Q29udGV4dCxcbiAgICBjb3ZlcmFnZTogeyBsZXZlbDogJ2hpZ2gnIH1cbiAgfSk7XG5cbiAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gIHRyeSB7XG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgeWllbGQgY2h1bms7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdlcnJvcicpIHtcbiAgICAgICAgeWllbGQgY2h1bms7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICB5aWVsZCB7IHR5cGU6ICdjb21wbGV0ZScsIHJlc3BvbnNlOiBmdWxsUmVzcG9uc2UgfTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlZnVzYWxUZXh0KCkge1xuICByZXR1cm4gZ2V0UmVmdXNhbFJlc3BvbnNlKCk7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBnZW5lcmF0ZVdlYlNlYXJjaFJlc3BvbnNlKHF1ZXJ5LCBncm91bmRpbmdDb250ZW50KSB7XG4gIGNvbnN0IG1vZGVsID0gZ2V0UHJpbWFyeU1vZGVsKCk7XG5cbiAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICBjb250ZW50czogW3tcbiAgICAgIHJvbGU6ICd1c2VyJyxcbiAgICAgIHBhcnRzOiBbeyB0ZXh0OiBgQmFzZWQgb24gdGhlc2Ugd2ViIHNlYXJjaCByZXN1bHRzLCBhbnN3ZXIgdGhlIHF1ZXN0aW9uOiBcIiR7cXVlcnl9XCJcXG5cXG4ke2dyb3VuZGluZ0NvbnRlbnR9YCB9XVxuICAgIH1dLFxuICAgIGdlbmVyYXRpb25Db25maWc6IHtcbiAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICB0b3BQOiAwLjk1LFxuICAgICAgbWF4T3V0cHV0VG9rZW5zOiAyMDQ4XG4gICAgfSxcbiAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICB9KTtcblxuICBjb25zdCByZXNwb25zZSA9IHJlc3VsdC5yZXNwb25zZTtcbiAgY29uc3QgdGV4dCA9IHJlc3BvbnNlLnRleHQoKTtcbiAgY29uc3QgZ3JvdW5kaW5nTWV0YWRhdGEgPSByZXNwb25zZS5jYW5kaWRhdGVzPy5bMF0/Lmdyb3VuZGluZ01ldGFkYXRhO1xuXG4gIHJldHVybiB7XG4gICAgdGV4dCxcbiAgICBncm91bmRpbmdNZXRhZGF0YSxcbiAgICBncm91bmRpbmdDaHVua3M6IGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MgfHwgW11cbiAgfTtcbn0iLCAiY29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2Rpcm5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwaVwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2NoYXQuanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHY0IGFzIHV1aWR2NCB9IGZyb20gJ3V1aWQnO1xuaW1wb3J0IHsgcmV0cmlldmVGb3JRdWVyeSwgZ2VuZXJhdGVDaXRhdGlvbnMsIGZvcm1hdENvbnRleHRGb3JQcm9tcHQgfSBmcm9tICcuLi9zZXJ2aWNlcy9yZXRyaWV2YWxTZXJ2aWNlLmpzJztcbmltcG9ydCB7IHN0cmVhbVJlc3BvbnNlIH0gZnJvbSAnLi4vc2VydmljZXMvZ2VtaW5pU2VydmljZS5qcyc7XG5pbXBvcnQgeyBhZGRUdXJuV2l0aENpdGF0aW9ucywgZ2V0UmVjZW50VHVybnMgfSBmcm9tICcuLi9zZXJ2aWNlcy9tZW1vcnlTZXJ2aWNlLmpzJztcbmltcG9ydCB7IGdldE9yQ3JlYXRlU2Vzc2lvbiB9IGZyb20gJy4uL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbmNvbnN0IE9VVF9PRl9TQ09QRV9QQVRURVJOID0gL2Rvbid0IGhhdmUgaW5mb3JtYXRpb258ZG8gbm90IGhhdmUgaW5mb3JtYXRpb258bm90IGluIG15IGtub3dsZWRnZXxjYW4ndCBmaW5kfGNhbm5vdCBmaW5kfG5vIGluZm9ybWF0aW9ufGtub3dsZWRnZSBiYXNlIGRvZXNuJ3R8bm90IGNvdmVyZWR8b3V0c2lkZS4qa25vd2xlZGdlL2k7XG5cbmZ1bmN0aW9uIGNsZWFuRXhjZXJwdCh0ZXh0KSB7XG4gIHJldHVybiB0ZXh0XG4gICAgLnJlcGxhY2UoLyg/PCFcXHcpKFtBLVphLXpdKVxccyhbQS1aYS16XSlcXHMoW0EtWmEtel0pKFxcc1tBLVphLXpdKSovZywgKG1hdGNoKSA9PlxuICAgICAgbWF0Y2gucmVwbGFjZSgvXFxzL2csICcnKVxuICAgIClcbiAgICAucmVwbGFjZSgvXFxzezIsfS9nLCAnICcpXG4gICAgLnJlcGxhY2UoL15cXCpcXHMqLywgJycpXG4gICAgLnRyaW0oKTtcbn1cblxuZnVuY3Rpb24gZXhwYW5kUXVlcnkocXVlcnksIHNlc3Npb25JZCkge1xuICBjb25zdCB3b3JkcyA9IHF1ZXJ5LnRyaW0oKS5zcGxpdCgvXFxzKy8pO1xuICBpZiAod29yZHMubGVuZ3RoID4gNCkgcmV0dXJuIHF1ZXJ5O1xuXG4gIGNvbnN0IHJlY2VudFR1cm5zID0gZ2V0UmVjZW50VHVybnMoc2Vzc2lvbklkLCA0KTtcbiAgY29uc3QgcmVjZW50Q29udGV4dCA9IHJlY2VudFR1cm5zXG4gICAgLmZpbHRlcih0ID0+IHQucm9sZSA9PT0gJ3VzZXInKVxuICAgIC5tYXAodCA9PiB0LmNvbnRlbnQpXG4gICAgLmpvaW4oJyAnKTtcblxuICBjb25zdCBleHBhbnNpb25zID0gW1xuICAgICdkZWZpbml0aW9uJywgJ292ZXJ2aWV3JywgJ3JvbGUnLCAncmVzcG9uc2liaWxpdGllcycsXG4gICAgJ2V4YW1wbGVzJywgJ2tleSBjb25jZXB0cycsICdob3cgaXQgd29ya3MnLCAncHVycG9zZSdcbiAgXTtcblxuICBjb25zdCBxdWVyeVdvcmRzID0gcXVlcnkudG9Mb3dlckNhc2UoKS5zcGxpdCgvXFxzKy8pO1xuICBjb25zdCBjb250ZXh0UmVsZXZhbnQgPSBxdWVyeVdvcmRzLnNvbWUodyA9PlxuICAgIHcubGVuZ3RoID4gMyAmJiByZWNlbnRDb250ZXh0LnRvTG93ZXJDYXNlKCkuaW5jbHVkZXModylcbiAgKTtcblxuICBjb25zdCBkb21haW5IaW50ID0gY29udGV4dFJlbGV2YW50ID8gYCR7cmVjZW50Q29udGV4dC5zbGljZSgwLCA4MCl9OiBgIDogJyc7XG5cbiAgcmV0dXJuIGAke2RvbWFpbkhpbnR9JHtxdWVyeX0gJHtleHBhbnNpb25zLmpvaW4oJyAnKX1gO1xufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gaGFuZGxlQ2hhdFN0cmVhbShyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5LCBzZXNzaW9uSWQ6IHByb3ZpZGVkU2Vzc2lvbklkIH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7IGVycm9yOiAnUXVlcnkgaXMgcmVxdWlyZWQnLCBjb2RlOiAnTUlTU0lOR19RVUVSWScgfSk7XG4gIH1cblxuICBjb25zdCBzZXNzaW9uSWQgPSBwcm92aWRlZFNlc3Npb25JZCB8fCB1dWlkdjQoKTtcbiAgY29uc3QgYW5zd2VySWQgPSB1dWlkdjQoKTtcblxuICBnZXRPckNyZWF0ZVNlc3Npb24oc2Vzc2lvbklkKTtcblxuICByZXMuc2V0SGVhZGVyKCdDb250ZW50LVR5cGUnLCAndGV4dC9ldmVudC1zdHJlYW0nKTtcbiAgcmVzLnNldEhlYWRlcignQ2FjaGUtQ29udHJvbCcsICduby1jYWNoZScpO1xuICByZXMuc2V0SGVhZGVyKCdDb25uZWN0aW9uJywgJ2tlZXAtYWxpdmUnKTtcbiAgcmVzLnNldEhlYWRlcigneC1zZXNzaW9uLWlkJywgc2Vzc2lvbklkKTtcbiAgcmVzLnNldEhlYWRlcigneC1hbnN3ZXItaWQnLCBhbnN3ZXJJZCk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5gKTtcbiAgICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH07XG5cbiAgYWRkVHVybldpdGhDaXRhdGlvbnMoc2Vzc2lvbklkLCAndXNlcicsIHF1ZXJ5LnRyaW0oKSk7XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdyZXRyaWV2aW5nJywgbWVzc2FnZTogJ1NlYXJjaGluZyBrbm93bGVkZ2UgYmFzZS4uLicgfSk7XG5cbiAgICBjb25zdCBleHBhbmRlZFF1ZXJ5ID0gZXhwYW5kUXVlcnkocXVlcnksIHNlc3Npb25JZCk7XG4gICAgY29uc3QgeyByZXN1bHRzLCBjb3ZlcmFnZSB9ID0gYXdhaXQgcmV0cmlldmVGb3JRdWVyeShleHBhbmRlZFF1ZXJ5LCBzZXNzaW9uSWQsIHsgdG9wSzogNSB9KTtcblxuICAgIHNlbmRFdmVudCgncmV0cmlldmFsJywge1xuICAgICAgcmVzdWx0czogcmVzdWx0cy5sZW5ndGgsXG4gICAgICBsZXZlbDogY292ZXJhZ2UubGV2ZWwsXG4gICAgICBzY29yZTogY292ZXJhZ2Uuc2NvcmUsXG4gICAgICB0b3BTY29yZTogY292ZXJhZ2UudG9wU2NvcmVcbiAgICB9KTtcblxuICAgIGNvbnN0IGNpdGF0aW9ucyA9IGdlbmVyYXRlQ2l0YXRpb25zKHJlc3VsdHMpO1xuICAgIGNvbnN0IHNvdXJjZXMgPSByZXN1bHRzLm1hcChyID0+ICh7XG4gICAgICBjaHVua0lkOiByLmlkLFxuICAgICAgZG9jdW1lbnRJZDogci5tZXRhZGF0YS5kb2N1bWVudF9pZCxcbiAgICAgIGZpbGVuYW1lOiByLm1ldGFkYXRhLmZpbGVuYW1lLFxuICAgICAgcGFnZU51bWJlcjogci5tZXRhZGF0YS5wYWdlX251bWJlcixcbiAgICAgIGV4Y2VycHQ6IGNsZWFuRXhjZXJwdChyLnRleHQuc2xpY2UoMCwgMjAwKSksXG4gICAgICBzY29yZTogci5zY29yZSxcbiAgICAgIHNvdXJjZVR5cGU6IHIuc291cmNlX3R5cGVcbiAgICB9KSk7XG5cbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdnZW5lcmF0aW5nJywgbWVzc2FnZTogJ0dlbmVyYXRpbmcgcmVzcG9uc2UuLi4nIH0pO1xuXG4gICAgY29uc3QgY29udGV4dFRleHQgPSBmb3JtYXRDb250ZXh0Rm9yUHJvbXB0KHJlc3VsdHMpO1xuXG4gICAgY29uc3QgbWVtb3J5Q29udGV4dCA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgNSlcbiAgICAgIC5tYXAodCA9PiBgJHt0LnJvbGUgPT09ICd1c2VyJyA/ICdVc2VyJyA6ICdBc3Npc3RhbnQnfTogJHt0LmNvbnRlbnR9YClcbiAgICAgIC5qb2luKCdcXG5cXG4nKTtcblxuICAgIGNvbnN0IHByb21wdCA9IGBZb3UgYXJlIGFuIEFJIEtub3dsZWRnZSBBc3Npc3RhbnQuIFlvdXIgYmVoYXZpb3VyIGRlcGVuZHMgb24gdGhlIHR5cGUgb2YgaW5wdXQ6XG5cbjEuIEdSRUVUSU5HUyAmIFNNQUxMIFRBTEsgKGhpLCBoZWxsbywgaG93IGFyZSB5b3UsIGRvIHlvdSBoYXZlIGEgbGlmZSwgam9rZXMsIGdlbmVyYWwgY2hhdCk6XG4gICAtIFJlc3BvbmQgd2FybWx5IGFuZCBuYXR1cmFsbHkuIERvIE5PVCBtZW50aW9uIHRoZSBrbm93bGVkZ2UgYmFzZSBvciBkb2N1bWVudHMgYXQgYWxsLlxuICAgLSBEbyBOT1QgYWRkIGFueSBjaXRhdGlvbnMuXG5cbjIuIEZBQ1RVQUwgUVVFU1RJT05TIFdJVEggQ09OVEVYVCAoY29udGV4dCBiZWxvdyBpcyByZWxldmFudCk6XG4gICAtIEFuc3dlciBzdHJpY3RseSB1c2luZyB0aGUgbnVtYmVyZWQgY29udGV4dCBwcm92aWRlZC5cbiAgIC0gQ2l0ZSBzb3VyY2VzIGlubGluZSBhcyBbMV0gWzJdIFx1MjAxNCBhbHdheXMgc2VwYXJhdGUgYnJhY2tldHMsIG5ldmVyIFsxLCAyXS5cbiAgIC0gT25seSBjaXRlIG51bWJlcnMgeW91IGFjdHVhbGx5IHVzZWQuXG5cbjMuIEZBQ1RVQUwgUVVFU1RJT05TIFdJVEhPVVQgQ09OVEVYVCAoY29udGV4dCBpcyBlbXB0eSBvciBpcnJlbGV2YW50KTpcbiAgIC0gUG9saXRlbHkgZGVjbGluZSBpbiB5b3VyIG93biB3b3JkcyBcdTIwMTQgdmFyeSB5b3VyIHBocmFzaW5nIG5hdHVyYWxseS5cbiAgIC0gRG8gTk9UIGFkZCBjaXRhdGlvbnMuXG4gICAtIERvIE5PVCB1c2UgYSBmaXhlZCB0ZW1wbGF0ZSBvciByb2JvdGljIHJlc3BvbnNlLlxuXG5DT05URVhUOlxuJHtjb250ZXh0VGV4dCB8fCAnKE5vIHJlbGV2YW50IGRvY3VtZW50cyBmb3VuZCBpbiBrbm93bGVkZ2UgYmFzZSknfVxuXG5DT05WRVJTQVRJT04gSElTVE9SWTpcbiR7bWVtb3J5Q29udGV4dH1cblxuQ1VSUkVOVCBRVUVTVElPTjogJHtxdWVyeX1gO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiBzdHJlYW1SZXNwb25zZShwcm9tcHQpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ0xMTV9FUlJPUicgfSk7XG4gICAgICB9IGVsc2UgaWYgKGNodW5rLnR5cGUgPT09ICdjb21wbGV0ZScpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlID0gY2h1bmsucmVzcG9uc2U7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gRXh0cmFjdCBjaXRlZCBpbmRpY2VzIGluIE9SREVSIE9GIEZJUlNUIEFQUEVBUkFOQ0VcbiAgICBjb25zdCBjaXRlZEluZGljZXMgPSBbXTtcbiAgICBjb25zdCBzZWVuID0gbmV3IFNldCgpO1xuICAgIGZvciAoY29uc3QgbWF0Y2ggb2YgZnVsbFJlc3BvbnNlLm1hdGNoQWxsKC9cXFsoXFxkKylcXF0vZykpIHtcbiAgICAgIGNvbnN0IG51bSA9IHBhcnNlSW50KG1hdGNoWzFdKTtcbiAgICAgIGlmICghc2Vlbi5oYXMobnVtKSkge1xuICAgICAgICBzZWVuLmFkZChudW0pO1xuICAgICAgICBjaXRlZEluZGljZXMucHVzaChudW0pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGlzT3V0T2ZTY29wZSA9IE9VVF9PRl9TQ09QRV9QQVRURVJOLnRlc3QoZnVsbFJlc3BvbnNlKTtcblxuICAgIGNvbnN0IG1hdGNoZWRDaXRhdGlvbnMgPSBjaXRhdGlvbnMuZmlsdGVyKGMgPT4gY2l0ZWRJbmRpY2VzLmluY2x1ZGVzKGMuaW5kZXgpKTtcblxuICAgIC8vIFJlbWFwIG9sZCBMTE0gaW5kaWNlcyBcdTIxOTIgbmV3IHNlcXVlbnRpYWwgaW5kaWNlcyBieSBmaXJzdCBhcHBlYXJhbmNlXG4gICAgY29uc3QgaW5kZXhNYXAgPSBuZXcgTWFwKCk7XG4gICAgY2l0ZWRJbmRpY2VzLmZvckVhY2goKG9sZElkeCwgaSkgPT4ge1xuICAgICAgaW5kZXhNYXAuc2V0KG9sZElkeCwgaSArIDEpO1xuICAgIH0pO1xuXG4gICAgLy8gUmV3cml0ZSByZXNwb25zZSB0ZXh0IHNvIFszXVsyXVsxXSBiZWNvbWVzIFsxXVsyXVszXVxuICAgIGNvbnN0IHJld3JpdHRlblJlc3BvbnNlID0gZnVsbFJlc3BvbnNlLnJlcGxhY2UoL1xcWyhcXGQrKVxcXS9nLCAobWF0Y2gsIG51bSkgPT4ge1xuICAgICAgY29uc3QgbmV3SWR4ID0gaW5kZXhNYXAuZ2V0KHBhcnNlSW50KG51bSkpO1xuICAgICAgcmV0dXJuIG5ld0lkeCAhPT0gdW5kZWZpbmVkID8gYFske25ld0lkeH1dYCA6IG1hdGNoO1xuICAgIH0pO1xuXG4gICAgLy8gUmVtYXAgY2l0YXRpb25zIHdpdGggbmV3IGluZGljZXMsIHNvcnRlZCBieSBmaXJzdCBhcHBlYXJhbmNlXG4gICAgY29uc3QgZmluYWxDaXRhdGlvbnMgPSAoaXNPdXRPZlNjb3BlIHx8IG1hdGNoZWRDaXRhdGlvbnMubGVuZ3RoID09PSAwKVxuICAgICAgPyBbXVxuICAgICAgOiBtYXRjaGVkQ2l0YXRpb25zXG4gICAgICAgICAgLm1hcChjID0+ICh7IC4uLmMsIGluZGV4OiBpbmRleE1hcC5nZXQoYy5pbmRleCkgfSkpXG4gICAgICAgICAgLmZpbHRlcihjID0+IGMuaW5kZXggIT09IHVuZGVmaW5lZClcbiAgICAgICAgICAuc29ydCgoYSwgYikgPT4gYS5pbmRleCAtIGIuaW5kZXgpO1xuXG4gICAgLy8gTWF0Y2ggc291cmNlcyBieSBjaHVua0lkLCBzb3J0ZWQgaW4gc2FtZSBvcmRlciBhcyBmaW5hbENpdGF0aW9uc1xuICAgIGNvbnN0IG1hdGNoZWRDaHVua0lkcyA9IG5ldyBTZXQobWF0Y2hlZENpdGF0aW9ucy5tYXAoYyA9PiBjLmNodW5rSWQpKTtcblxuICAgIGNvbnN0IGZpbmFsU291cmNlcyA9IChpc091dE9mU2NvcGUgfHwgbWF0Y2hlZENpdGF0aW9ucy5sZW5ndGggPT09IDApXG4gICAgICA/IFtdXG4gICAgICA6IHNvdXJjZXNcbiAgICAgICAgICAuZmlsdGVyKHMgPT4gbWF0Y2hlZENodW5rSWRzLmhhcyhzLmNodW5rSWQpKVxuICAgICAgICAgIC5zb3J0KChhLCBiKSA9PiB7XG4gICAgICAgICAgICBjb25zdCBpZHhBID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYS5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgICBjb25zdCBpZHhCID0gZmluYWxDaXRhdGlvbnMuZmluZChjID0+IGMuY2h1bmtJZCA9PT0gYi5jaHVua0lkKT8uaW5kZXggPz8gOTk7XG4gICAgICAgICAgICByZXR1cm4gaWR4QSAtIGlkeEI7XG4gICAgICAgICAgfSk7XG5cbiAgICBhZGRUdXJuV2l0aENpdGF0aW9ucyhzZXNzaW9uSWQsICdhc3Npc3RhbnQnLCByZXdyaXR0ZW5SZXNwb25zZSwgZmluYWxDaXRhdGlvbnMsIGNvdmVyYWdlLCBhbnN3ZXJJZCk7XG5cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgYW5zd2VySWQsXG4gICAgICByZXNwb25zZTogcmV3cml0dGVuUmVzcG9uc2UsXG4gICAgICBjaXRhdGlvbnM6IGZpbmFsQ2l0YXRpb25zLFxuICAgICAgY292ZXJhZ2UsXG4gICAgICBzb3VyY2VzOiBmaW5hbFNvdXJjZXNcbiAgICB9KTtcblxuICAgIHJlcy5lbmQoKTtcblxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ0NoYXQgc3RyZWFtIGVycm9yOicsIGVycm9yKTtcbiAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdBbiBlcnJvciBvY2N1cnJlZCcsIGNvZGU6IGVycm9yLmNvZGUgfHwgJ0NIQVRfRVJST1InIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0U291cmNlcyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ10gfHwgcmVxLnF1ZXJ5LnNlc3Npb25JZDtcblxuICBjb25zdCByZWNlbnRUdXJucyA9IGdldFJlY2VudFR1cm5zKHNlc3Npb25JZCwgMjApO1xuXG4gIGNvbnN0IGV4YWN0TWF0Y2ggPSByZWNlbnRUdXJucy5maW5kKHQgPT4gdC5pZCA9PT0gYW5zd2VySWQpO1xuICBpZiAoZXhhY3RNYXRjaD8uY2l0YXRpb25zPy5sZW5ndGggPiAwKSB7XG4gICAgcmV0dXJuIHJlcy5qc29uKHsgc291cmNlczogZXhhY3RNYXRjaC5jaXRhdGlvbnMgfSk7XG4gIH1cblxuICBjb25zdCBmYWxsYmFjayA9IFsuLi5yZWNlbnRUdXJuc10ucmV2ZXJzZSgpLmZpbmQodCA9PlxuICAgIHQucm9sZSA9PT0gJ2Fzc2lzdGFudCcgJiYgdC5jaXRhdGlvbnM/Lmxlbmd0aCA+IDBcbiAgKTtcblxuICBpZiAoZmFsbGJhY2spIHJldHVybiByZXMuanNvbih7IHNvdXJjZXM6IGZhbGxiYWNrLmNpdGF0aW9ucyB9KTtcblxuICByZXMuc3RhdHVzKDQwNCkuanNvbih7IGVycm9yOiAnU291cmNlcyBub3QgZm91bmQnLCBjb2RlOiAnU09VUkNFU19OT1RfRk9VTkQnIH0pO1xufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZUNoYXRTdHJlYW0pO1xucm91dGVyLmdldCgnL3NvdXJjZXMvOmFuc3dlcklkJywgZ2V0U291cmNlcyk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvZmVlZGJhY2suanNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpL2ZlZWRiYWNrLmpzXCI7aW1wb3J0IHsgUm91dGVyIH0gZnJvbSAnZXhwcmVzcyc7XG5pbXBvcnQgeyB2NCBhcyB1dWlkdjQgfSBmcm9tICd1dWlkJztcblxuY29uc3Qgcm91dGVyID0gUm91dGVyKCk7XG5cbi8vIEluLW1lbW9yeSBmZWVkYmFjayBzdG9yZSAoY291bGQgYmUgcmVwbGFjZWQgd2l0aCBkYXRhYmFzZSlcbmNvbnN0IGZlZWRiYWNrU3RvcmUgPSBuZXcgTWFwKCk7XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBzdWJtaXRGZWVkYmFjayhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkLCBzZXNzaW9uSWQsIHR5cGUsIGNvbW1lbnQsIHJhdGluZyB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFhbnN3ZXJJZCB8fCAhdHlwZSkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ2Fuc3dlcklkIGFuZCB0eXBlIGFyZSByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19GSUVMRFMnXG4gICAgfSk7XG4gIH1cblxuICBjb25zdCB2YWxpZFR5cGVzID0gWydwb3NpdGl2ZScsICduZWdhdGl2ZScsICdoZWxwZnVsJywgJ25vdF9oZWxwZnVsJywgJ3JlcG9ydF9pc3N1ZSddO1xuICBpZiAoIXZhbGlkVHlwZXMuaW5jbHVkZXModHlwZSkpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdJbnZhbGlkIGZlZWRiYWNrIHR5cGUnLFxuICAgICAgY29kZTogJ0lOVkFMSURfVFlQRScsXG4gICAgICB2YWxpZFR5cGVzXG4gICAgfSk7XG4gIH1cblxuICB0cnkge1xuICAgIGNvbnN0IGZlZWRiYWNrID0ge1xuICAgICAgaWQ6IHV1aWR2NCgpLFxuICAgICAgYW5zd2VySWQsXG4gICAgICBzZXNzaW9uSWQ6IHNlc3Npb25JZCB8fCAndW5rbm93bicsXG4gICAgICB0eXBlLFxuICAgICAgcmF0aW5nOiByYXRpbmcgfHwgbnVsbCxcbiAgICAgIGNvbW1lbnQ6IGNvbW1lbnQgfHwgbnVsbCxcbiAgICAgIGNyZWF0ZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgdXNlckFnZW50OiByZXEuaGVhZGVyc1sndXNlci1hZ2VudCddIHx8IG51bGwsXG4gICAgICBpcDogcmVxLmlwIHx8IG51bGxcbiAgICB9O1xuXG4gICAgZmVlZGJhY2tTdG9yZS5zZXQoZmVlZGJhY2suaWQsIGZlZWRiYWNrKTtcblxuICAgIHJlcy5zdGF0dXMoMjAxKS5qc29uKHtcbiAgICAgIHN1Y2Nlc3M6IHRydWUsXG4gICAgICBmZWVkYmFja0lkOiBmZWVkYmFjay5pZCxcbiAgICAgIG1lc3NhZ2U6ICdUaGFuayB5b3UgZm9yIHlvdXIgZmVlZGJhY2snXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignRmVlZGJhY2sgc3VibWlzc2lvbiBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gc3VibWl0IGZlZWRiYWNrJyxcbiAgICAgIGNvZGU6ICdGRUVEQkFDS19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0RmVlZGJhY2tTdGF0cyhyZXEsIHJlcykge1xuICBjb25zdCB7IGFuc3dlcklkIH0gPSByZXEucGFyYW1zO1xuXG4gIHRyeSB7XG4gICAgY29uc3QgYWxsRmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuICAgIGNvbnN0IGFuc3dlckZlZWRiYWNrID0gYWxsRmVlZGJhY2suZmlsdGVyKGYgPT4gZi5hbnN3ZXJJZCA9PT0gYW5zd2VySWQpO1xuXG4gICAgY29uc3Qgc3RhdHMgPSB7XG4gICAgICB0b3RhbDogYW5zd2VyRmVlZGJhY2subGVuZ3RoLFxuICAgICAgcG9zaXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ3Bvc2l0aXZlJyB8fCBmLnR5cGUgPT09ICdoZWxwZnVsJykubGVuZ3RoLFxuICAgICAgbmVnYXRpdmU6IGFuc3dlckZlZWRiYWNrLmZpbHRlcihmID0+IGYudHlwZSA9PT0gJ25lZ2F0aXZlJyB8fCBmLnR5cGUgPT09ICdub3RfaGVscGZ1bCcpLmxlbmd0aCxcbiAgICAgIGF2ZXJhZ2VSYXRpbmc6IGFuc3dlckZlZWRiYWNrXG4gICAgICAgIC5maWx0ZXIoZiA9PiBmLnJhdGluZylcbiAgICAgICAgLnJlZHVjZSgoc3VtLCBmLCBfLCBhcnIpID0+IHN1bSArIGYucmF0aW5nIC8gYXJyLmxlbmd0aCwgMCkgfHwgbnVsbFxuICAgIH07XG5cbiAgICByZXMuanNvbihzdGF0cyk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gZ2V0IGZlZWRiYWNrIHN0YXRzJyxcbiAgICAgIGNvZGU6ICdTVEFUU19FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gbGlzdEZlZWRiYWNrKHJlcSwgcmVzKSB7XG4gIGNvbnN0IHsgc2Vzc2lvbklkIH0gPSByZXEucXVlcnk7XG5cbiAgdHJ5IHtcbiAgICBsZXQgZmVlZGJhY2sgPSBBcnJheS5mcm9tKGZlZWRiYWNrU3RvcmUudmFsdWVzKCkpO1xuXG4gICAgaWYgKHNlc3Npb25JZCkge1xuICAgICAgZmVlZGJhY2sgPSBmZWVkYmFjay5maWx0ZXIoZiA9PiBmLnNlc3Npb25JZCA9PT0gc2Vzc2lvbklkKTtcbiAgICB9XG5cbiAgICByZXMuanNvbih7XG4gICAgICB0b3RhbDogZmVlZGJhY2subGVuZ3RoLFxuICAgICAgZmVlZGJhY2s6IGZlZWRiYWNrLnNsaWNlKC01MCkgLy8gTGFzdCA1MCBlbnRyaWVzXG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgcmVzLnN0YXR1cyg1MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdGYWlsZWQgdG8gbGlzdCBmZWVkYmFjaycsXG4gICAgICBjb2RlOiAnTElTVF9FUlJPUidcbiAgICB9KTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnLycsIHN1Ym1pdEZlZWRiYWNrKTtcbnJvdXRlci5nZXQoJy9zdGF0cy86YW5zd2VySWQnLCBnZXRGZWVkYmFja1N0YXRzKTtcbnJvdXRlci5nZXQoJy9saXN0JywgbGlzdEZlZWRiYWNrKTtcblxuZXhwb3J0IGRlZmF1bHQgcm91dGVyO1xuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvc2VydmljZXMvd2ViU2VhcmNoU2VydmljZS5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9zZXJ2aWNlcy93ZWJTZWFyY2hTZXJ2aWNlLmpzXCI7aW1wb3J0IHsgR29vZ2xlR2VuZXJhdGl2ZUFJIH0gZnJvbSAnQGdvb2dsZS9nZW5lcmF0aXZlLWFpJztcbmltcG9ydCB7IFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IgfSBmcm9tICcuLi91dGlscy9lcnJvcnMuanMnO1xuXG5jb25zdCBnZW5BSSA9IG5ldyBHb29nbGVHZW5lcmF0aXZlQUkocHJvY2Vzcy5lbnYuR0VNSU5JX0FQSV9LRVkpO1xuXG5jb25zdCBQUklNQVJZX01PREVMID0gcHJvY2Vzcy5lbnYuR0VNSU5JX01PREVMX1BSSU1BUlkgfHwgJ2dlbWluaS0zLjEtZmxhc2gtbGl0ZSc7XG5cbmxldCBtb2RlbCA9IG51bGw7XG5cbmZ1bmN0aW9uIGdldE1vZGVsKCkge1xuICBpZiAoIW1vZGVsKSB7XG4gICAgbW9kZWwgPSBnZW5BSS5nZXRHZW5lcmF0aXZlTW9kZWwoeyBtb2RlbDogUFJJTUFSWV9NT0RFTCB9KTtcbiAgfVxuICByZXR1cm4gbW9kZWw7XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBwZXJmb3JtV2ViU2VhcmNoKHF1ZXJ5KSB7XG4gIHRyeSB7XG4gICAgY29uc3QgbW9kZWwgPSBnZXRNb2RlbCgpO1xuXG4gICAgY29uc3QgcmVzdWx0ID0gYXdhaXQgbW9kZWwuZ2VuZXJhdGVDb250ZW50KHtcbiAgICAgIGNvbnRlbnRzOiBbe1xuICAgICAgICByb2xlOiAndXNlcicsXG4gICAgICAgIHBhcnRzOiBbeyB0ZXh0OiBxdWVyeSB9XVxuICAgICAgfV0sXG4gICAgICBnZW5lcmF0aW9uQ29uZmlnOiB7XG4gICAgICAgIHRlbXBlcmF0dXJlOiAwLjcsXG4gICAgICAgIG1heE91dHB1dFRva2VuczogMjA0OFxuICAgICAgfSxcbiAgICAgIHRvb2xzOiBbeyBnb29nbGVTZWFyY2g6IHt9IH1dXG4gICAgfSk7XG5cbiAgICBjb25zdCByZXNwb25zZSA9IHJlc3VsdC5yZXNwb25zZTtcbiAgICBjb25zdCB0ZXh0ID0gcmVzcG9uc2UudGV4dCgpO1xuICAgIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzcG9uc2UuY2FuZGlkYXRlcz8uWzBdPy5ncm91bmRpbmdNZXRhZGF0YTtcblxuICAgIC8vIEV4dHJhY3Qgc2VhcmNoIHF1ZXJpZXMgYW5kIHNvdXJjZXNcbiAgICBjb25zdCB3ZWJTZWFyY2hRdWVyaWVzID0gW107XG4gICAgY29uc3Qgd2ViU291cmNlcyA9IFtdO1xuXG4gICAgaWYgKGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgIGZvciAoY29uc3QgY2h1bmsgb2YgZ3JvdW5kaW5nTWV0YWRhdGEuZ3JvdW5kaW5nQ2h1bmtzKSB7XG4gICAgICAgIGlmIChjaHVuay53ZWIpIHtcbiAgICAgICAgICB3ZWJTb3VyY2VzLnB1c2goe1xuICAgICAgICAgICAgdXJpOiBjaHVuay53ZWIudXJpLFxuICAgICAgICAgICAgdGl0bGU6IGNodW5rLndlYi50aXRsZVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGdyb3VuZGluZ01ldGFkYXRhPy53ZWJTZWFyY2hRdWVyaWVzKSB7XG4gICAgICB3ZWJTZWFyY2hRdWVyaWVzLnB1c2goLi4uZ3JvdW5kaW5nTWV0YWRhdGEud2ViU2VhcmNoUXVlcmllcyk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRleHQsXG4gICAgICBzb3VyY2VzOiB3ZWJTb3VyY2VzLFxuICAgICAgcXVlcmllczogd2ViU2VhcmNoUXVlcmllcyxcbiAgICAgIHJhd01ldGFkYXRhOiBncm91bmRpbmdNZXRhZGF0YVxuICAgIH07XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBlcnJvcjonLCBlcnJvcik7XG4gICAgdGhyb3cgbmV3IFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgfVxufVxuXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24qIHN0cmVhbVdlYlNlYXJjaChxdWVyeSkge1xuICB0cnkge1xuICAgIGNvbnN0IG1vZGVsID0gZ2V0TW9kZWwoKTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IG1vZGVsLmdlbmVyYXRlQ29udGVudFN0cmVhbSh7XG4gICAgICBjb250ZW50czogW3tcbiAgICAgICAgcm9sZTogJ3VzZXInLFxuICAgICAgICBwYXJ0czogW3sgdGV4dDogcXVlcnkgfV1cbiAgICAgIH1dLFxuICAgICAgZ2VuZXJhdGlvbkNvbmZpZzoge1xuICAgICAgICB0ZW1wZXJhdHVyZTogMC43LFxuICAgICAgICBtYXhPdXRwdXRUb2tlbnM6IDIwNDhcbiAgICAgIH0sXG4gICAgICB0b29sczogW3sgZ29vZ2xlU2VhcmNoOiB7fSB9XVxuICAgIH0pO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuXG4gICAgZm9yIGF3YWl0IChjb25zdCBjaHVuayBvZiByZXN1bHQuc3RyZWFtKSB7XG4gICAgICBjb25zdCB0ZXh0ID0gY2h1bmsudGV4dCgpO1xuICAgICAgaWYgKHRleHQpIHtcbiAgICAgICAgZnVsbFJlc3BvbnNlICs9IHRleHQ7XG4gICAgICAgIHlpZWxkIHsgdHlwZTogJ3Rva2VuJywgdGV4dCB9O1xuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgcmVzdWx0LnJlc3BvbnNlO1xuICAgIGNvbnN0IGdyb3VuZGluZ01ldGFkYXRhID0gcmVzcG9uc2U/LmNhbmRpZGF0ZXM/LlswXT8uZ3JvdW5kaW5nTWV0YWRhdGE7XG5cbiAgICBjb25zdCBzb3VyY2VzID0gW107XG4gICAgaWYgKGdyb3VuZGluZ01ldGFkYXRhPy5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgIGZvciAoY29uc3QgaXRlbSBvZiBncm91bmRpbmdNZXRhZGF0YS5ncm91bmRpbmdDaHVua3MpIHtcbiAgICAgICAgaWYgKGl0ZW0ud2ViKSB7XG4gICAgICAgICAgc291cmNlcy5wdXNoKHtcbiAgICAgICAgICAgIHVyaTogaXRlbS53ZWIudXJpLFxuICAgICAgICAgICAgdGl0bGU6IGl0ZW0ud2ViLnRpdGxlXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICB5aWVsZCB7XG4gICAgICB0eXBlOiAnY29tcGxldGUnLFxuICAgICAgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSxcbiAgICAgIHNvdXJjZXNcbiAgICB9O1xuICB9IGNhdGNoIChlcnJvcikge1xuICAgIGNvbnNvbGUuZXJyb3IoJ1dlYiBzZWFyY2ggc3RyZWFtaW5nIGVycm9yOicsIGVycm9yKTtcbiAgICB5aWVsZCB7IHR5cGU6ICdlcnJvcicsIGVycm9yOiBlcnJvci5tZXNzYWdlIH07XG4gICAgdGhyb3cgbmV3IFdlYlNlYXJjaFVuYXZhaWxhYmxlRXJyb3IoKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0V2ViU2VhcmNoUmVzcG9uc2UocmVzdWx0KSB7XG4gIHJldHVybiB7XG4gICAgYW5zd2VyOiByZXN1bHQudGV4dCxcbiAgICBzb3VyY2VzOiByZXN1bHQuc291cmNlcy5tYXAocyA9PiAoe1xuICAgICAgdXJpOiBzLnVyaSxcbiAgICAgIHRpdGxlOiBzLnRpdGxlLFxuICAgICAgdHlwZTogJ3dlYidcbiAgICB9KSksXG4gICAgcXVlcmllc1VzZWQ6IHJlc3VsdC5xdWVyaWVzLFxuICAgIG1ldGFkYXRhOiB7XG4gICAgICBwZXJmb3JtZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgc2VhcmNoVHlwZTogJ2dvb2dsZV9zZWFyY2hfZ3JvdW5kaW5nJ1xuICAgIH1cbiAgfTtcbn1cbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXIvYXBpXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ZpbGVuYW1lID0gXCIvaG9tZS9wcm9qZWN0L3NlcnZlci9hcGkvc2VhcmNoLmpzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9ob21lL3Byb2plY3Qvc2VydmVyL2FwaS9zZWFyY2guanNcIjtpbXBvcnQgeyBSb3V0ZXIgfSBmcm9tICdleHByZXNzJztcbmltcG9ydCB7IHBlcmZvcm1XZWJTZWFyY2gsIHN0cmVhbVdlYlNlYXJjaCB9IGZyb20gJy4uL3NlcnZpY2VzL3dlYlNlYXJjaFNlcnZpY2UuanMnO1xuXG5jb25zdCByb3V0ZXIgPSBSb3V0ZXIoKTtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIGhhbmRsZVdlYlNlYXJjaChyZXEsIHJlcykge1xuICBjb25zdCB7IHF1ZXJ5IH0gPSByZXEuYm9keTtcblxuICBpZiAoIXF1ZXJ5IHx8IHR5cGVvZiBxdWVyeSAhPT0gJ3N0cmluZycgfHwgcXVlcnkudHJpbSgpLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiByZXMuc3RhdHVzKDQwMCkuanNvbih7XG4gICAgICBlcnJvcjogJ1F1ZXJ5IGlzIHJlcXVpcmVkJyxcbiAgICAgIGNvZGU6ICdNSVNTSU5HX1FVRVJZJ1xuICAgIH0pO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCByZXN1bHQgPSBhd2FpdCBwZXJmb3JtV2ViU2VhcmNoKHF1ZXJ5LnRyaW0oKSk7XG5cbiAgICByZXMuanNvbih7XG4gICAgICBzdWNjZXNzOiB0cnVlLFxuICAgICAgYW5zd2VyOiByZXN1bHQudGV4dCxcbiAgICAgIHNvdXJjZXM6IHJlc3VsdC5zb3VyY2VzLFxuICAgICAgcXVlcmllczogcmVzdWx0LnF1ZXJpZXMsXG4gICAgICBtZXRhZGF0YToge1xuICAgICAgICBwZXJmb3JtZWRBdDogbmV3IERhdGUoKS50b0lTT1N0cmluZygpLFxuICAgICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgICB9XG4gICAgfSk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBlcnJvcjonLCBlcnJvcik7XG4gICAgcmVzLnN0YXR1cyhlcnJvci5zdGF0dXNDb2RlIHx8IDUwMykuanNvbih7XG4gICAgICBlcnJvcjogZXJyb3IubWVzc2FnZSB8fCAnV2ViIHNlYXJjaCB1bmF2YWlsYWJsZScsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdXRUJfU0VBUkNIX0VSUk9SJ1xuICAgIH0pO1xuICB9XG59XG5cbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBoYW5kbGVXZWJTZWFyY2hTdHJlYW0ocmVxLCByZXMpIHtcbiAgY29uc3QgeyBxdWVyeSB9ID0gcmVxLmJvZHk7XG5cbiAgaWYgKCFxdWVyeSB8fCB0eXBlb2YgcXVlcnkgIT09ICdzdHJpbmcnIHx8IHF1ZXJ5LnRyaW0oKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oe1xuICAgICAgZXJyb3I6ICdRdWVyeSBpcyByZXF1aXJlZCcsXG4gICAgICBjb2RlOiAnTUlTU0lOR19RVUVSWSdcbiAgICB9KTtcbiAgfVxuXG4gIC8vIFNldCB1cCBTU0VcbiAgcmVzLnNldEhlYWRlcignQ29udGVudC1UeXBlJywgJ3RleHQvZXZlbnQtc3RyZWFtJyk7XG4gIHJlcy5zZXRIZWFkZXIoJ0NhY2hlLUNvbnRyb2wnLCAnbm8tY2FjaGUnKTtcbiAgcmVzLnNldEhlYWRlcignQ29ubmVjdGlvbicsICdrZWVwLWFsaXZlJyk7XG5cbiAgY29uc3Qgc2VuZEV2ZW50ID0gKGV2ZW50LCBkYXRhKSA9PiB7XG4gICAgcmVzLndyaXRlKGBldmVudDogJHtldmVudH1cXG5gKTtcbiAgICByZXMud3JpdGUoYGRhdGE6ICR7SlNPTi5zdHJpbmdpZnkoZGF0YSl9XFxuXFxuYCk7XG4gIH07XG5cbiAgdHJ5IHtcbiAgICBzZW5kRXZlbnQoJ3N0YXR1cycsIHsgc3RhZ2U6ICdzZWFyY2hpbmcnLCBtZXNzYWdlOiAnU2VhcmNoaW5nIHRoZSB3ZWIuLi4nIH0pO1xuXG4gICAgbGV0IGZ1bGxSZXNwb25zZSA9ICcnO1xuICAgIGxldCBzb3VyY2VzID0gW107XG5cbiAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIHN0cmVhbVdlYlNlYXJjaChxdWVyeS50cmltKCkpKSB7XG4gICAgICBpZiAoY2h1bmsudHlwZSA9PT0gJ3Rva2VuJykge1xuICAgICAgICBmdWxsUmVzcG9uc2UgKz0gY2h1bmsudGV4dDtcbiAgICAgICAgc2VuZEV2ZW50KCd0b2tlbicsIHsgdGV4dDogY2h1bmsudGV4dCB9KTtcbiAgICAgIH0gZWxzZSBpZiAoY2h1bmsudHlwZSA9PT0gJ2Vycm9yJykge1xuICAgICAgICBzZW5kRXZlbnQoJ2Vycm9yJywgeyBtZXNzYWdlOiBjaHVuay5lcnJvciwgY29kZTogJ1dFQl9TRUFSQ0hfRVJST1InIH0pO1xuICAgICAgfSBlbHNlIGlmIChjaHVuay50eXBlID09PSAnY29tcGxldGUnKSB7XG4gICAgICAgIGZ1bGxSZXNwb25zZSA9IGNodW5rLnJlc3BvbnNlO1xuICAgICAgICBzb3VyY2VzID0gY2h1bmsuc291cmNlcyB8fCBbXTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBzZW5kRXZlbnQoJ2NvbXBsZXRlJywge1xuICAgICAgcmVzcG9uc2U6IGZ1bGxSZXNwb25zZSxcbiAgICAgIHNvdXJjZXMsXG4gICAgICBzZWFyY2hUeXBlOiAnZ29vZ2xlX3NlYXJjaF9ncm91bmRpbmcnXG4gICAgfSk7XG5cbiAgICByZXMuZW5kKCk7XG4gIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgY29uc29sZS5lcnJvcignV2ViIHNlYXJjaCBzdHJlYW0gZXJyb3I6JywgZXJyb3IpO1xuICAgIHNlbmRFdmVudCgnZXJyb3InLCB7XG4gICAgICBtZXNzYWdlOiBlcnJvci5tZXNzYWdlIHx8ICdXZWIgc2VhcmNoIGZhaWxlZCcsXG4gICAgICBjb2RlOiBlcnJvci5jb2RlIHx8ICdXRUJfU0VBUkNIX0VSUk9SJ1xuICAgIH0pO1xuICAgIHJlcy5lbmQoKTtcbiAgfVxufVxuXG5yb3V0ZXIucG9zdCgnLycsIGhhbmRsZVdlYlNlYXJjaCk7XG5yb3V0ZXIucG9zdCgnL3N0cmVhbScsIGhhbmRsZVdlYlNlYXJjaFN0cmVhbSk7XG5cbmV4cG9ydCBkZWZhdWx0IHJvdXRlcjtcbiIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdC9zZXJ2ZXJcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIi9ob21lL3Byb2plY3Qvc2VydmVyL2FwcC5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3NlcnZlci9hcHAuanNcIjtpbXBvcnQgZXhwcmVzcyBmcm9tICdleHByZXNzJztcbmltcG9ydCBjb3JzIGZyb20gJ2NvcnMnO1xuaW1wb3J0IGRvdGVudiBmcm9tICdkb3RlbnYnO1xuaW1wb3J0IHsgRXZlbnRFbWl0dGVyIH0gZnJvbSAnZXZlbnRzJztcblxuZG90ZW52LmNvbmZpZygpO1xuXG5pbXBvcnQgaGVhbHRoUm91dGVyIGZyb20gJy4vYXBpL2hlYWx0aC5qcyc7XG5pbXBvcnQgZG9jdW1lbnRzUm91dGVyIGZyb20gJy4vYXBpL2RvY3VtZW50cy5qcyc7XG5pbXBvcnQgY2hhdFJvdXRlciBmcm9tICcuL2FwaS9jaGF0LmpzJztcbmltcG9ydCBmZWVkYmFja1JvdXRlciBmcm9tICcuL2FwaS9mZWVkYmFjay5qcyc7XG5pbXBvcnQgc2VhcmNoUm91dGVyIGZyb20gJy4vYXBpL3NlYXJjaC5qcyc7XG5pbXBvcnQgeyBnZXRPckNyZWF0ZVNlc3Npb24sIGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3MgfSBmcm9tICcuL3NlcnZpY2VzL3Nlc3Npb25TZXJ2aWNlLmpzJztcblxuY29uc3QgYXBwID0gZXhwcmVzcygpO1xuXG4vLyBQcm9ncmVzcyBjYWxsYmFja3NcbmFwcC5sb2NhbHMucHJvZ3Jlc3NDYWxsYmFja3MgPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbi8vIE1pZGRsZXdhcmVcbmFwcC51c2UoY29ycyh7XG4gIG9yaWdpbjogW1xuICAgICdodHRwOi8vbG9jYWxob3N0OjUxNzMnLFxuICAgICdodHRwOi8vbG9jYWxob3N0OjMwMDAnLFxuICAgICdodHRwOi8vMTI3LjAuMC4xOjUxNzMnXG4gIF0sXG4gIGNyZWRlbnRpYWxzOiB0cnVlXG59KSk7XG5cbmFwcC51c2UoZXhwcmVzcy5qc29uKHsgbGltaXQ6ICcxMG1iJyB9KSk7XG5hcHAudXNlKGV4cHJlc3MudXJsZW5jb2RlZCh7IGV4dGVuZGVkOiB0cnVlLCBsaW1pdDogJzEwbWInIH0pKTtcblxuLy8gUmVxdWVzdCBMb2dnZXJcbmFwcC51c2UoKHJlcSwgcmVzLCBuZXh0KSA9PiB7XG4gIGNvbnNvbGUubG9nKGAke3JlcS5tZXRob2R9ICR7cmVxLm9yaWdpbmFsVXJsfWApO1xuICBuZXh0KCk7XG59KTtcblxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuLy8gVEVTVCBST1VURVxuLy8gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PVxuYXBwLmdldCgnL3BpbmcnLCAocmVxLCByZXMpID0+IHtcbiAgY29uc29sZS5sb2coJ1x1MjcwNSBQSU5HIFJPVVRFIEVYRUNVVEVEJyk7XG4gIHJlcy5qc29uKHtcbiAgICBzdWNjZXNzOiB0cnVlLFxuICAgIG1lc3NhZ2U6ICdFeHByZXNzIGJhY2tlbmQgaXMgYWxpdmUnXG4gIH0pO1xufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFNFU1NJT04gSU5JVCBST1VURVxuLy8gQ2FsbGVkIGJ5IGZyb250ZW5kIG9uIGNoYXQgc2NyZWVuIG1vdW50IFx1MjAxNCBzZWVkcyBnbG9iYWwgZG9jcyBpbnRvIHNlc3Npb25cbi8vIGJlZm9yZSB0aGUgdXNlciBzZW5kcyB0aGVpciBmaXJzdCBtZXNzYWdlLCBlbGltaW5hdGluZyBmaXJzdC1tZXNzYWdlIGxhdGVuY3lcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC5wb3N0KCcvYXBpL3Nlc3Npb24vaW5pdCcsIGFzeW5jIChyZXEsIHJlcykgPT4ge1xuICBjb25zdCBzZXNzaW9uSWQgPSByZXEuaGVhZGVyc1sneC1zZXNzaW9uLWlkJ107XG5cbiAgaWYgKCFzZXNzaW9uSWQpIHtcbiAgICByZXR1cm4gcmVzLnN0YXR1cyg0MDApLmpzb24oeyBlcnJvcjogJ01pc3NpbmcgeC1zZXNzaW9uLWlkIGhlYWRlcicsIGNvZGU6ICdNSVNTSU5HX1NFU1NJT04nIH0pO1xuICB9XG5cbiAgZ2V0T3JDcmVhdGVTZXNzaW9uKHNlc3Npb25JZCk7XG5cbiAgLy8gRmlyZSBhbmQgZG9uJ3QgYmxvY2sgXHUyMDE0IGNsaWVudCBkb2Vzbid0IG5lZWQgdG8gd2FpdCBmb3IgZnVsbCBzZWVkaW5nXG4gIC8vIEJ1dCB3ZSBkbyBhd2FpdCBzbyB0aGUgY2xpZW50IGtub3dzIHdoZW4gaXQncyByZWFkeVxuICB0cnkge1xuICAgIGF3YWl0IGluaXRTZXNzaW9uV2l0aEdsb2JhbERvY3Moc2Vzc2lvbklkKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiB0cnVlLCBzZXNzaW9uSWQgfSk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIC8vIE5vbi1mYXRhbCBcdTIwMTQgY2hhdCBzdGlsbCB3b3Jrcywgc2VlZGluZyB3aWxsIHJldHJ5IG9uIGZpcnN0IG1lc3NhZ2VcbiAgICBjb25zb2xlLndhcm4oJ1Nlc3Npb24gaW5pdCB3YXJuaW5nOicsIGVyci5tZXNzYWdlKTtcbiAgICByZXMuanNvbih7IHJlYWR5OiBmYWxzZSwgc2Vzc2lvbklkLCB3YXJuaW5nOiBlcnIubWVzc2FnZSB9KTtcbiAgfVxufSk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIFJPVVRFUlNcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmNvbnNvbGUubG9nKCdNb3VudGluZyByb3V0ZXJzLi4uJyk7XG5cbmFwcC51c2UoJy9oZWFsdGgnLCBoZWFsdGhSb3V0ZXIpO1xuYXBwLnVzZSgnL2RvY3VtZW50cycsIGRvY3VtZW50c1JvdXRlcik7XG5hcHAudXNlKCcvY2hhdCcsIGNoYXRSb3V0ZXIpO1xuYXBwLnVzZSgnL2ZlZWRiYWNrJywgZmVlZGJhY2tSb3V0ZXIpO1xuYXBwLnVzZSgnL3NlYXJjaCcsIHNlYXJjaFJvdXRlcik7XG5cbmNvbnNvbGUubG9nKCdcdTI3MDUgUm91dGVycyBtb3VudGVkJyk7XG5cbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbi8vIEVSUk9SIEhBTkRMRVJcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKGVyciwgcmVxLCByZXMsIG5leHQpID0+IHtcbiAgY29uc29sZS5lcnJvcignRVJST1IgTUlERExFV0FSRScpO1xuICBjb25zb2xlLmVycm9yKGVycik7XG4gIHJlcy5zdGF0dXMoNTAwKS5qc29uKHtcbiAgICBlcnJvcjogZXJyLm1lc3NhZ2UsXG4gICAgc3RhY2s6IGVyci5zdGFja1xuICB9KTtcbn0pO1xuXG4vLyA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09XG4vLyA0MDRcbi8vID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT1cbmFwcC51c2UoKHJlcSwgcmVzKSA9PiB7XG4gIHJlcy5zdGF0dXMoNDA0KS5qc29uKHtcbiAgICBlcnJvcjogJ0VuZHBvaW50IG5vdCBmb3VuZCcsXG4gICAgY29kZTogJ05PVF9GT1VORCdcbiAgfSk7XG59KTtcblxuZXhwb3J0IGRlZmF1bHQgYXBwOyIsICJjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZGlybmFtZSA9IFwiL2hvbWUvcHJvamVjdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiL2hvbWUvcHJvamVjdC92aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vaG9tZS9wcm9qZWN0L3ZpdGUuY29uZmlnLmpzXCI7dmFyIF9fYXdhaXRlciA9ICh0aGlzICYmIHRoaXMuX19hd2FpdGVyKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgX2FyZ3VtZW50cywgUCwgZ2VuZXJhdG9yKSB7XG4gICAgZnVuY3Rpb24gYWRvcHQodmFsdWUpIHsgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgUCA/IHZhbHVlIDogbmV3IFAoZnVuY3Rpb24gKHJlc29sdmUpIHsgcmVzb2x2ZSh2YWx1ZSk7IH0pOyB9XG4gICAgcmV0dXJuIG5ldyAoUCB8fCAoUCA9IFByb21pc2UpKShmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICAgIGZ1bmN0aW9uIGZ1bGZpbGxlZCh2YWx1ZSkgeyB0cnkgeyBzdGVwKGdlbmVyYXRvci5uZXh0KHZhbHVlKSk7IH0gY2F0Y2ggKGUpIHsgcmVqZWN0KGUpOyB9IH1cbiAgICAgICAgZnVuY3Rpb24gcmVqZWN0ZWQodmFsdWUpIHsgdHJ5IHsgc3RlcChnZW5lcmF0b3JbXCJ0aHJvd1wiXSh2YWx1ZSkpOyB9IGNhdGNoIChlKSB7IHJlamVjdChlKTsgfSB9XG4gICAgICAgIGZ1bmN0aW9uIHN0ZXAocmVzdWx0KSB7IHJlc3VsdC5kb25lID8gcmVzb2x2ZShyZXN1bHQudmFsdWUpIDogYWRvcHQocmVzdWx0LnZhbHVlKS50aGVuKGZ1bGZpbGxlZCwgcmVqZWN0ZWQpOyB9XG4gICAgICAgIHN0ZXAoKGdlbmVyYXRvciA9IGdlbmVyYXRvci5hcHBseSh0aGlzQXJnLCBfYXJndW1lbnRzIHx8IFtdKSkubmV4dCgpKTtcbiAgICB9KTtcbn07XG52YXIgX19nZW5lcmF0b3IgPSAodGhpcyAmJiB0aGlzLl9fZ2VuZXJhdG9yKSB8fCBmdW5jdGlvbiAodGhpc0FyZywgYm9keSkge1xuICAgIHZhciBfID0geyBsYWJlbDogMCwgc2VudDogZnVuY3Rpb24oKSB7IGlmICh0WzBdICYgMSkgdGhyb3cgdFsxXTsgcmV0dXJuIHRbMV07IH0sIHRyeXM6IFtdLCBvcHM6IFtdIH0sIGYsIHksIHQsIGcgPSBPYmplY3QuY3JlYXRlKCh0eXBlb2YgSXRlcmF0b3IgPT09IFwiZnVuY3Rpb25cIiA/IEl0ZXJhdG9yIDogT2JqZWN0KS5wcm90b3R5cGUpO1xuICAgIHJldHVybiBnLm5leHQgPSB2ZXJiKDApLCBnW1widGhyb3dcIl0gPSB2ZXJiKDEpLCBnW1wicmV0dXJuXCJdID0gdmVyYigyKSwgdHlwZW9mIFN5bWJvbCA9PT0gXCJmdW5jdGlvblwiICYmIChnW1N5bWJvbC5pdGVyYXRvcl0gPSBmdW5jdGlvbigpIHsgcmV0dXJuIHRoaXM7IH0pLCBnO1xuICAgIGZ1bmN0aW9uIHZlcmIobikgeyByZXR1cm4gZnVuY3Rpb24gKHYpIHsgcmV0dXJuIHN0ZXAoW24sIHZdKTsgfTsgfVxuICAgIGZ1bmN0aW9uIHN0ZXAob3ApIHtcbiAgICAgICAgaWYgKGYpIHRocm93IG5ldyBUeXBlRXJyb3IoXCJHZW5lcmF0b3IgaXMgYWxyZWFkeSBleGVjdXRpbmcuXCIpO1xuICAgICAgICB3aGlsZSAoZyAmJiAoZyA9IDAsIG9wWzBdICYmIChfID0gMCkpLCBfKSB0cnkge1xuICAgICAgICAgICAgaWYgKGYgPSAxLCB5ICYmICh0ID0gb3BbMF0gJiAyID8geVtcInJldHVyblwiXSA6IG9wWzBdID8geVtcInRocm93XCJdIHx8ICgodCA9IHlbXCJyZXR1cm5cIl0pICYmIHQuY2FsbCh5KSwgMCkgOiB5Lm5leHQpICYmICEodCA9IHQuY2FsbCh5LCBvcFsxXSkpLmRvbmUpIHJldHVybiB0O1xuICAgICAgICAgICAgaWYgKHkgPSAwLCB0KSBvcCA9IFtvcFswXSAmIDIsIHQudmFsdWVdO1xuICAgICAgICAgICAgc3dpdGNoIChvcFswXSkge1xuICAgICAgICAgICAgICAgIGNhc2UgMDogY2FzZSAxOiB0ID0gb3A7IGJyZWFrO1xuICAgICAgICAgICAgICAgIGNhc2UgNDogXy5sYWJlbCsrOyByZXR1cm4geyB2YWx1ZTogb3BbMV0sIGRvbmU6IGZhbHNlIH07XG4gICAgICAgICAgICAgICAgY2FzZSA1OiBfLmxhYmVsKys7IHkgPSBvcFsxXTsgb3AgPSBbMF07IGNvbnRpbnVlO1xuICAgICAgICAgICAgICAgIGNhc2UgNzogb3AgPSBfLm9wcy5wb3AoKTsgXy50cnlzLnBvcCgpOyBjb250aW51ZTtcbiAgICAgICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICAgICAgICBpZiAoISh0ID0gXy50cnlzLCB0ID0gdC5sZW5ndGggPiAwICYmIHRbdC5sZW5ndGggLSAxXSkgJiYgKG9wWzBdID09PSA2IHx8IG9wWzBdID09PSAyKSkgeyBfID0gMDsgY29udGludWU7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSAzICYmICghdCB8fCAob3BbMV0gPiB0WzBdICYmIG9wWzFdIDwgdFszXSkpKSB7IF8ubGFiZWwgPSBvcFsxXTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKG9wWzBdID09PSA2ICYmIF8ubGFiZWwgPCB0WzFdKSB7IF8ubGFiZWwgPSB0WzFdOyB0ID0gb3A7IGJyZWFrOyB9XG4gICAgICAgICAgICAgICAgICAgIGlmICh0ICYmIF8ubGFiZWwgPCB0WzJdKSB7IF8ubGFiZWwgPSB0WzJdOyBfLm9wcy5wdXNoKG9wKTsgYnJlYWs7IH1cbiAgICAgICAgICAgICAgICAgICAgaWYgKHRbMl0pIF8ub3BzLnBvcCgpO1xuICAgICAgICAgICAgICAgICAgICBfLnRyeXMucG9wKCk7IGNvbnRpbnVlO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgb3AgPSBib2R5LmNhbGwodGhpc0FyZywgXyk7XG4gICAgICAgIH0gY2F0Y2ggKGUpIHsgb3AgPSBbNiwgZV07IHkgPSAwOyB9IGZpbmFsbHkgeyBmID0gdCA9IDA7IH1cbiAgICAgICAgaWYgKG9wWzBdICYgNSkgdGhyb3cgb3BbMV07IHJldHVybiB7IHZhbHVlOiBvcFswXSA/IG9wWzFdIDogdm9pZCAwLCBkb25lOiB0cnVlIH07XG4gICAgfVxufTtcbmltcG9ydCB7IGRlZmluZUNvbmZpZyB9IGZyb20gJ3ZpdGUnO1xuaW1wb3J0IHJlYWN0IGZyb20gJ0B2aXRlanMvcGx1Z2luLXJlYWN0JztcbmltcG9ydCBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0IHsgZmlsZVVSTFRvUGF0aCB9IGZyb20gJ3VybCc7XG52YXIgX19kaXJuYW1lID0gcGF0aC5kaXJuYW1lKGZpbGVVUkxUb1BhdGgoaW1wb3J0Lm1ldGEudXJsKSk7XG5mdW5jdGlvbiBleHByZXNzUGx1Z2luKCkge1xuICAgIHZhciBhcHA7XG4gICAgcmV0dXJuIHtcbiAgICAgICAgbmFtZTogJ2V4cHJlc3MtcGx1Z2luJyxcbiAgICAgICAgY29uZmlndXJlU2VydmVyOiBmdW5jdGlvbiAoc2VydmVyKSB7XG4gICAgICAgICAgICByZXR1cm4gX19hd2FpdGVyKHRoaXMsIHZvaWQgMCwgdm9pZCAwLCBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgdmFyIGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgcmV0dXJuIF9fZ2VuZXJhdG9yKHRoaXMsIGZ1bmN0aW9uIChfYSkge1xuICAgICAgICAgICAgICAgICAgICBzd2l0Y2ggKF9hLmxhYmVsKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICBjYXNlIDA6IHJldHVybiBbNCAvKnlpZWxkKi8sIGltcG9ydCgnLi9zZXJ2ZXIvYXBwLmpzJyldO1xuICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSAxOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3NBcHAgPSAoX2Euc2VudCgpKS5kZWZhdWx0O1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFwcCA9IGV4cHJlc3NBcHA7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgc2VydmVyLm1pZGRsZXdhcmVzLnVzZSgnL2FwaScsIGZ1bmN0aW9uIChyZXEsIHJlcywgbmV4dCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhcHAocmVxLCByZXMsIG5leHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBbMiAvKnJldHVybiovXTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgfTtcbn1cbmV4cG9ydCBkZWZhdWx0IGRlZmluZUNvbmZpZyh7XG4gICAgcGx1Z2luczogW3JlYWN0KCksIGV4cHJlc3NQbHVnaW4oKV0sXG4gICAgcmVzb2x2ZToge1xuICAgICAgICBhbGlhczoge1xuICAgICAgICAgICAgJ0AnOiBwYXRoLnJlc29sdmUoX19kaXJuYW1lLCAnLi9zcmMnKSxcbiAgICAgICAgfSxcbiAgICB9LFxuICAgIHNlcnZlcjoge1xuICAgICAgICBwb3J0OiA1MTczLFxuICAgIH0sXG59KTtcbiJdLAogICJtYXBwaW5ncyI6ICI7Ozs7Ozs7Ozs7O0FBQTZRLFNBQVMsbUJBQW1CO0FBQ3pTLFNBQVMsTUFBTSxjQUFjO0FBTTdCLFNBQVMsaUJBQWlCO0FBQ3hCLE1BQUksQ0FBQyxhQUFhO0FBQ2hCLFVBQU0sU0FBUyxRQUFRLElBQUk7QUFDM0IsVUFBTSxTQUFTLFFBQVEsSUFBSSxpQkFBaUI7QUFDNUMsVUFBTSxXQUFXLFFBQVEsSUFBSSxtQkFBbUI7QUFDaEQsVUFBTSxPQUFPLFFBQVEsSUFBSSxlQUFlO0FBRXhDLFlBQVEsSUFBSSxxQ0FBcUM7QUFDakQsWUFBUSxJQUFJLGVBQWUsUUFBUSw2QkFBNkI7QUFDaEUsWUFBUSxJQUFJLGVBQWUsTUFBTTtBQUNqQyxZQUFRLElBQUksZUFBZSxRQUFRO0FBQ25DLFlBQVEsSUFBSSxlQUFlLFNBQVMsbUJBQW1CLHFCQUFxQjtBQUM1RSxZQUFRLElBQUkscUNBQXFDO0FBRWpELFFBQUksQ0FBQyxRQUFRO0FBQ1gsWUFBTSxJQUFJO0FBQUEsUUFDUjtBQUFBLE1BRUY7QUFBQSxJQUNGO0FBRUEsVUFBTSxnQkFBZ0IsRUFBRSxRQUFRLFFBQVEsU0FBUztBQUNqRCxRQUFJLEtBQU0sZUFBYyxPQUFPO0FBRS9CLGtCQUFjLElBQUksWUFBWSxhQUFhO0FBQUEsRUFDN0M7QUFDQSxTQUFPO0FBQ1Q7QUFNQSxlQUFzQixzQkFBc0I7QUFDMUMsTUFBSSxDQUFDLGtCQUFrQjtBQUNyQixVQUFNLFNBQVMsZUFBZTtBQUM5QixVQUFNLGlCQUFpQixRQUFRLElBQUksNEJBQTRCO0FBQy9ELFFBQUk7QUFDRix5QkFBbUIsTUFBTSxPQUFPLHNCQUFzQjtBQUFBLFFBQ3BELE1BQU07QUFBQSxRQUNOLFVBQVU7QUFBQSxVQUNSLGFB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QUFhLHVEQUF5QjtBQUFBLFlBQ3RELEtBQUs7QUFDRCwyQkFBYyxHQUFHLEtBQUssRUFBRztBQUN6QixjQUFBQSxPQUFNO0FBQ04scUJBQU8sWUFBWSxJQUFJLFFBQVEsU0FBVSxLQUFLLEtBQUssTUFBTTtBQUNyRCxnQkFBQUEsS0FBSSxLQUFLLEtBQUssSUFBSTtBQUFBLGNBQ3RCLENBQUM7QUFDRCxxQkFBTztBQUFBLGdCQUFDO0FBQUE7QUFBQSxjQUFZO0FBQUEsVUFDNUI7QUFBQSxRQUNKLENBQUM7QUFBQSxNQUNMLENBQUM7QUFBQSxJQUNMO0FBQUEsRUFDSjtBQUNKO0FBQ0EsSUFBTyxzQkFBUSxhQUFhO0FBQUEsRUFDeEIsU0FBUyxDQUFDLE1BQU0sR0FBRyxjQUFjLENBQUM7QUFBQSxFQUNsQyxTQUFTO0FBQUEsSUFDTCxPQUFPO0FBQUEsTUFDSCxLQUFLRixNQUFLLFFBQVFELFlBQVcsT0FBTztBQUFBLElBQ3hDO0FBQUEsRUFDSjtBQUFBLEVBQ0EsUUFBUTtBQUFBLElBQ0osTUFBTTtBQUFBLEVBQ1Y7QUFDSixDQUFDOyIsCiAgIm5hbWVzIjogWyJtb2RlbCIsICJ1dWlkdjQiLCAiZ2xvYmFsQ29sbGVjdGlvbiIsICJ1dWlkdjQiLCAiUm91dGVyIiwgInBhdGgiLCAidXVpZHY0IiwgInJvdXRlciIsICJHb29nbGVHZW5lcmF0aXZlQUkiLCAiZ2VuQUkiLCAibW9kZWwiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiUm91dGVyIiwgInV1aWR2NCIsICJyb3V0ZXIiLCAiR29vZ2xlR2VuZXJhdGl2ZUFJIiwgImdlbkFJIiwgIlBSSU1BUllfTU9ERUwiLCAibW9kZWwiLCAiUm91dGVyIiwgInJvdXRlciIsICJwYXRoIiwgImZpbGVVUkxUb1BhdGgiLCAiX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCIsICJfX2Rpcm5hbWUiLCAicGF0aCIsICJmaWxlVVJMVG9QYXRoIiwgImFwcCJdCn0K
