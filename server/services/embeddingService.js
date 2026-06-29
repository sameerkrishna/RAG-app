import { GoogleGenerativeAI } from '@google/generative-ai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

let genAI = null;
let embeddingModel = null;

// Fix 10: Lazy init — avoids dotenv timing bug
function getEmbeddingModel() {
  if (!embeddingModel) {
    genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
    embeddingModel = genAI.getGenerativeModel({
      model: process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001'
    });
  }
  return embeddingModel;
}

const BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
const PARALLEL_CALLS = () => parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
const OUTPUT_DIMENSIONS = () => parseInt(process.env.GEMINI_EMBEDDING_DIMENSIONS) || 3072; // Fix 9
const GROUP_WAIT_MS = 61000;

// Fix 11+12: Use batchEmbedContents — one API call returns N vectors (one per text)
// Fix 13: taskType passed per request
async function embedBatch(texts, taskType = 'RETRIEVAL_DOCUMENT', attempt = 1) {
  const maxAttempts = 5;
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001';

  try {
    const model = getEmbeddingModel();

    // Fix 12: Correct API format using batchEmbedContents
    // Fix 9: outputDimensionality explicitly set
    const result = await model.batchEmbedContents({
      requests: texts.map(text => ({
        model: `models/${modelName}`,
        content: { parts: [{ text }] },
        taskType,                                    // Fix 13
        outputDimensionality: OUTPUT_DIMENSIONS()    // Fix 9
      }))
    });

    if (!result?.embeddings || result.embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${result?.embeddings?.length ?? 0}`);
    }

    // Returns array of vectors — one per input text ✅
    return result.embeddings.map(e => {
      if (!e?.values) throw new EmbeddingError('Missing values in embedding response');
      return e.values;
    });

  } catch (error) {
    const is429 = is429Error(error) ||
      error?.status === 429 ||
      error?.message?.includes('RESOURCE_EXHAUSTED');

    if (is429 && attempt < maxAttempts) {
      const retryDelay = error.retryAfter || GROUP_WAIT_MS;
      console.log(`Rate limited, waiting ${retryDelay / 1000}s (attempt ${attempt}/${maxAttempts})`);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
      return embedBatch(texts, taskType, attempt + 1);
    }

    throw new EmbeddingError(error.message || 'Batch embedding failed');
  }
}

// Strategy:
//   - Split chunks into batches of BATCH_SIZE (default 7) → 1 API call per batch → 7 vectors back
//   - Fire PARALLEL_CALLS (default 4) batches simultaneously → 28 chunks per group
//   - Wait 61s between groups to respect rate limits
//
// For 48 chunks: ceil(48/7) = 7 batches → 2 parallel groups (4+3) → ~2 minutes total
export async function generateEmbeddings(chunks, taskType = 'RETRIEVAL_DOCUMENT', onProgress) {
  if (!chunks || chunks.length === 0) return [];

  const batchSize = BATCH_SIZE();
  const parallelCalls = PARALLEL_CALLS();
  const embeddings = [];

  // Split chunks into batches of batchSize
  const batches = [];
  for (let i = 0; i < chunks.length; i += batchSize) {
    batches.push(chunks.slice(i, i + batchSize));
  }

  // Group batches into parallel sets of parallelCalls
  const totalGroups = Math.ceil(batches.length / parallelCalls);

  for (let i = 0; i < batches.length; i += parallelCalls) {
    const parallelBatches = batches.slice(i, i + parallelCalls);
    const groupNum = Math.floor(i / parallelCalls) + 1;
    const chunksCovered = Math.min((i + parallelCalls) * batchSize, chunks.length);

    console.log(`  Embedding group ${groupNum}/${totalGroups} — ${parallelBatches.length} batch call(s) in parallel (chunks ${i * batchSize + 1}–${chunksCovered})...`);

    // Fire parallelCalls batch calls simultaneously
    const results = await Promise.allSettled(
      parallelBatches.map(batch => embedBatch(batch.map(c => c.text), taskType))
    );

    // Collect results per batch
    const failedBatches = [];
    results.forEach((result, batchIdx) => {
      const batch = parallelBatches[batchIdx];
      if (result.status === 'fulfilled') {
        const vectors = result.value; // array of N vectors
        batch.forEach((chunk, chunkIdx) => {
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_${i * batchSize + batchIdx * batchSize + chunkIdx}`,
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

    // Wait between groups (skip wait after last group unless there are retries)
    const isLastGroup = i + parallelCalls >= batches.length;
    if (!isLastGroup || failedBatches.length > 0) {
      console.log(`  Waiting ${GROUP_WAIT_MS / 1000}s before next group...`);
      await new Promise(resolve => setTimeout(resolve, GROUP_WAIT_MS));
    }

    // Retry failed batches one chunk at a time
    for (const { batch, batchIdx } of failedBatches) {
      for (const chunk of batch) {
        try {
          const vectors = await embedBatch([chunk.text], taskType);
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_retry_${batchIdx}`,
            embedding: vectors[0],
            metadata: chunk.metadata,
            text: chunk.text
          });
          console.log(`  ✅ Retry succeeded for chunk ${chunk.metadata?.chunk_id}`);
        } catch (err) {
          console.error(`  ❌ Retry failed for chunk ${chunk.metadata?.chunk_id}:`, err.message);
        }
      }
    }
  }

  return embeddings;
}

// Fix 13: Query uses RETRIEVAL_QUERY task type, single embed via batch of 1
export async function embedQuery(query) {
  const vectors = await embedBatch([query], 'RETRIEVAL_QUERY');
  return vectors[0];
}

export async function embedSingle(text) {
  const vectors = await embedBatch([text], 'RETRIEVAL_DOCUMENT');
  return vectors[0];
}

export function getRateLimitState() {
  return {
    maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 30000,
    parallelCalls: PARALLEL_CALLS(),
    maxChunksPerCall: BATCH_SIZE(),
    outputDimensions: OUTPUT_DIMENSIONS()  // Fix 9: expose for health check
  };
}