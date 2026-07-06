import { GoogleGenAI } from '@google/genai';
import { EmbeddingError, is429Error } from '../utils/errors.js';
import fs from "fs";
import path from "path";
import { fileURLToPath } from 'url';

// ============================================================
// 1. SLIDING WINDOW RATE LIMITER
// ============================================================
class SlidingWindowRateLimiter {
  constructor(limitPerMinute) {
    this.limitPerMinute = limitPerMinute;
    this.windowMs = 60000;
    this.requests = [];
  }

  async consume(tokens) {
    const now = Date.now();
    // Remove entries older than 60 seconds
    this.requests = this.requests.filter(req => req.timestamp > now - this.windowMs);

    const currentTotal = this.requests.reduce((sum, req) => sum + req.tokens, 0);

    // If we have room, consume instantly (burst)
    if (currentTotal + tokens <= this.limitPerMinute) {
      this.requests.push({ timestamp: now, tokens });
      return;
    }

    // Otherwise, wait until the oldest request expires (plus a small buffer)
    const needed = tokens - (this.limitPerMinute - currentTotal);
    let accumulatedExpired = 0;
    let waitUntil = now + this.windowMs; // fallback

    const sorted = [...this.requests].sort((a, b) => a.timestamp - b.timestamp);
    for (const req of sorted) {
      accumulatedExpired += req.tokens;
      if (accumulatedExpired >= needed) {
        // +10ms buffer to slide the window cleanly
        waitUntil = req.timestamp + this.windowMs + 10;
        break;
      }
    }

    const delay = waitUntil - now;
    if (delay > 0) {
      console.log(
        `[rate-limit] Window full (${currentTotal}/${this.limitPerMinute}). ` +
        `Waiting ${(delay / 1000).toFixed(1)}s to send ${tokens} tokens...`
      );
      await new Promise(resolve => setTimeout(resolve, delay));
    }

    // Record the consumption at the new time
    this.requests.push({ timestamp: Date.now(), tokens });
    // Cleanup again just in case
    this.requests = this.requests.filter(req => req.timestamp > Date.now() - this.windowMs);
  }
}

// ============================================================
// 2. CONFIGURATION
// ============================================================
const TPM_LIMIT = parseInt(process.env.GEMINI_EMBEDDING_TPM_LIMIT) || 500000;
const RATE_LIMITER = new SlidingWindowRateLimiter(TPM_LIMIT);

// BATCH_SIZE: number of chunks per embedContent call
// (kept at 10; note the real ceiling is the API's ~100-requests-per-call limit,
// not a "context window" limit — 10 just keeps batches small and retry-friendly)
const BATCH_SIZE = () => 10;   // 10 chunks × 750 tokens = 7,500 tokens per API request
const PARALLEL_CALLS = () => 10; // Send 10 batches concurrently to clear the burst fast

// Retry configuration (exponential backoff + jitter)
const RETRY_BASE_DELAY_MS = 2000;   // 2 seconds
const RETRY_MAX_DELAY_MS = 60000;   // 60 seconds cap
const MAX_RETRY_ATTEMPTS = 5;

// ============================================================
// 3. AI CLIENT (single, reusable instance)
// ============================================================
const ai = new GoogleGenAI({
  vertexai: true,
  project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || 'project-d48e2f39-2685-4746-aa0',
  location: process.env.GOOGLE_CLOUD_LOCATION || 'us-central1'
});

// ============================================================
// 4. TOKEN CALCULATION (uses stored token_count if available)
// ============================================================
function getTokenCountForChunks(chunks) {
  return chunks.reduce((sum, chunk) => {
    // Prefer the exact token count from chunker, otherwise fallback to rough estimate
    const tokenCount = chunk.metadata?.token_count || Math.ceil(chunk.text.length / 4);
    return sum + tokenCount;
  }, 0);
}

// Same rough estimate as above, but for raw strings that don't carry chunk metadata
// (used for retries inside embedBatch, and for embedQuery).
function estimateTokensForTexts(texts) {
  return texts.reduce((sum, text) => sum + Math.ceil(String(text).length / 4), 0);
}

// ============================================================
// 5. EMBED BATCH (with exponential backoff + jitter)
// ============================================================
async function embedBatch(texts, taskType = 'RETRIEVAL_DOCUMENT', attempt = 1) {
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001';
  const outputDimensionality = parseInt(process.env.GEMINI_EMBEDDING_DIMENSIONS) || 3072;
  const __dirname = path.dirname(fileURLToPath(import.meta.url));

  const credentialPath = "google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json";
  const credsSrc = path.resolve(__dirname, 'google_credentials');
  const credsDest = path.resolve(__dirname, 'dist/google_credentials');
  console.log('CWD' + process.cwd())
  console.log('Dir name' + __dirname);
  console.log(credsSrc);
  console.log(credsDest);
  console.log("resolved =", path.resolve(credentialPath));
  console.log("exists =", fs.existsSync(credentialPath));
  console.log("exists abs =", fs.existsSync(path.resolve(credentialPath)));
  console.log("root files =", fs.readdirSync(process.cwd()));
  try {
    // FIX: `ai.batches.createEmbeddings` is not a real method on the @google/genai SDK.
    // `ai.batches` is for async batch-prediction jobs. Synchronous embedding calls go
    // through `ai.models.embedContent`, with one shared taskType/outputDimensionality
    // config applied across all `contents` in the call.
    const response = await ai.models.embedContent({
      model: modelName,
      contents: texts.map(text => (typeof text === 'string' ? text : String(text))),
      config: {
        taskType: taskType,
        outputDimensionality: outputDimensionality
      }
    });

    const embeddings = response?.embeddings?.map(e => e.values) || [];
    if (embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${embeddings.length}`);
    }
    return embeddings;

  } catch (error) {
    const isRetryable = is429Error(error) ||
      error?.status === 429 ||
      error?.status === 502 ||
      error?.status === 503 ||
      error?.message?.includes('RESOURCE_EXHAUSTED') ||
      error?.message?.includes('Service Unavailable') ||
      error?.message?.includes('Bad Gateway');

    if (isRetryable && attempt < MAX_RETRY_ATTEMPTS) {
      // Exponential backoff: 2^attempt * base (capped)
      let delay = Math.min(RETRY_MAX_DELAY_MS, RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1));
      // Add jitter (0.8–1.2x) to avoid thundering herd
      const jitter = 0.8 + (0.4 * Math.random());
      delay = Math.floor(delay * jitter);
      // Respect retry-after header if present
      if (error.retryAfter) {
        delay = Math.max(delay, error.retryAfter * 1000);
      }

      console.log(
        `[embedding] ⏳ Retryable error (${error?.status || 'unknown'}), ` +
        `waiting ${(delay / 1000).toFixed(1)}s (attempt ${attempt}/${MAX_RETRY_ATTEMPTS})...`
      );
      await new Promise(resolve => setTimeout(resolve, delay));

      // FIX: a retry is a brand new API call and consumes real quota, even though
      // the original call failed. Skipping consumption here (as before) let the local
      // limiter under-report actual usage during error storms, which meant it kept
      // waving through new groups while retries were also hitting the API — making
      // 429 storms worse instead of backing off from them.
      await RATE_LIMITER.consume(estimateTokensForTexts(texts));

      return embedBatch(texts, taskType, attempt + 1);
    }

    throw new EmbeddingError(error.message || 'Batch embedding failed');
  }
}

// ============================================================
// 6. EXPORTED generateEmbeddings (with rate limiter & accurate tokens)
// ============================================================
export async function generateEmbeddings(chunks, taskType = 'RETRIEVAL_DOCUMENT', onProgress) {
  if (!chunks || chunks.length === 0) return [];

  const batchSize = BATCH_SIZE();
  const parallelCalls = PARALLEL_CALLS();

  // Fixed-size array to preserve chronological order
  const embeddings = new Array(chunks.length);

  // Group chunks into batches with their starting index
  const batches = [];
  for (let i = 0; i < chunks.length; i += batchSize) {
    batches.push({
      chunks: chunks.slice(i, i + batchSize),
      startIndex: i
    });
  }

  const totalGroups = Math.ceil(batches.length / parallelCalls);

  for (let i = 0; i < batches.length; i += parallelCalls) {
    const parallelBatches = batches.slice(i, i + parallelCalls);
    const groupNum = Math.floor(i / parallelCalls) + 1;

    // Calculate exact tokens using stored token_count (or fallback)
    const allChunksInGroup = parallelBatches.flatMap(b => b.chunks);
    const tokensToConsume = getTokenCountForChunks(allChunksInGroup);
    await RATE_LIMITER.consume(tokensToConsume);

    console.log(
      `[embedding] Group ${groupNum}/${totalGroups} — firing ${parallelBatches.length} batches ` +
      `in parallel (${tokensToConsume} tokens)`
    );

    const results = await Promise.allSettled(
      parallelBatches.map(b => embedBatch(b.chunks.map(c => c.text), taskType))
    );

    const failedBatches = [];
    results.forEach((result, batchIdx) => {
      const currentBatchInfo = parallelBatches[batchIdx];
      if (result.status === 'fulfilled') {
        const vectors = result.value;
        currentBatchInfo.chunks.forEach((chunk, chunkIdx) => {
          const globalIndex = currentBatchInfo.startIndex + chunkIdx;
          embeddings[globalIndex] = {
            id: chunk.metadata?.chunk_id || `chunk_${globalIndex}`,
            embedding: vectors[chunkIdx],
            metadata: chunk.metadata,
            text: chunk.text
          };
        });
      } else {
        console.warn(`[embedding] Batch starting at index ${currentBatchInfo.startIndex} failed:`, result.reason?.message);
        failedBatches.push(currentBatchInfo);
      }
    });

    if (onProgress) {
      onProgress({ current_batch: groupNum, total_batches: totalGroups });
    }

    // Retry failed batches individually
    for (const failedBatch of failedBatches) {
      console.log(`[embedding] Retrying failed batch elements starting at index ${failedBatch.startIndex}...`);
      for (let chunkIdx = 0; chunkIdx < failedBatch.chunks.length; chunkIdx++) {
        const chunk = failedBatch.chunks[chunkIdx];
        const globalIndex = failedBatch.startIndex + chunkIdx;
        try {
          // FIX: this retry is a fresh, real API call — track its tokens against
          // the limiter instead of assuming it was "already paid for".
          await RATE_LIMITER.consume(getTokenCountForChunks([chunk]));
          const vectors = await embedBatch([chunk.text], taskType);
          embeddings[globalIndex] = {
            id: chunk.metadata?.chunk_id || `chunk_retry_${globalIndex}`,
            embedding: vectors[0],
            metadata: chunk.metadata,
            text: chunk.text
          };
          console.log(`[embedding] ✅ Retry succeeded for chunk ${chunk.metadata?.chunk_id || globalIndex}`);
        } catch (err) {
          console.error(`[embedding] ❌ Retry failed for chunk ${chunk.metadata?.chunk_id || globalIndex}:`, err.message);
        }
      }
    }
  }

  // FIX: permanently-failed chunks are dropped here, which shifts array indices
  // relative to the original `chunks` input. This log makes that loss visible
  // instead of silent; callers that need to know exactly which chunks were lost
  // can compare returned `id`s against their original chunk list.
  const failedCount = embeddings.filter(e => !e).length;
  if (failedCount > 0) {
    console.warn(`[embedding] ${failedCount}/${chunks.length} chunk(s) permanently failed to embed and were dropped.`);
  }

  // Filter out any elements that permanently failed
  return embeddings.filter(Boolean);
}

// ============================================================
// 7. EXPORTED embedQuery
// ============================================================
export async function embedQuery(query) {
  // FIX: this call was bypassing the rate limiter entirely. If it runs concurrently
  // with document ingestion (e.g. a user searches while a batch job is in flight),
  // it could push total usage over the configured TPM budget unnoticed.
  await RATE_LIMITER.consume(estimateTokensForTexts([query]));
  const vectors = await embedBatch([query], 'RETRIEVAL_QUERY');
  return vectors[0];
}

export async function embedSingleBatchGroup(texts, taskType = 'RETRIEVAL_DOCUMENT') {
  console.log(`[embedding] embedSingleBatchGroup — ${texts.length} texts, taskType=${taskType}`);
  await RATE_LIMITER.consume(estimateTokensForTexts(texts));
  const vectors = await embedBatch(texts, taskType);
  console.log(`[embedding] embedSingleBatchGroup — got ${vectors.length} vectors`);
  return vectors;
}