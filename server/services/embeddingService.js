import { GoogleGenAI } from '@google/genai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

let ai = null;
let embeddingModel = null;

function getEmbeddingModel() {
  if (!embeddingModel) {
    ai = new GoogleGenAI({
      vertexai: true,
      project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || 'project-d48e2f39-2685-4746-aa0',
      location: process.env.GOOGLE_CLOUD_LOCATION || 'us-central1'
    });

    embeddingModel = ai.models;
  }
  return embeddingModel;
}

const BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
const PARALLEL_CALLS = () => parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
const OUTPUT_DIMENSIONS = () => parseInt(process.env.GEMINI_EMBEDDING_DIMENSIONS) || 3072;
const GROUP_WAIT_MS = 61000;
const RETRY_WAIT_MS = 15000;

// Embed a single batch of texts (up to BATCH_SIZE).
// Retries on 429 and transient 502/503 errors up to 5 times.
async function embedBatch(texts, taskType = 'RETRIEVAL_DOCUMENT', attempt = 1) {
  const maxAttempts = 5;
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001';

  try {
    const model = getEmbeddingModel();

    const embeddingPromises = texts.map(async (rawText) => {
      // Coerce safely to string to prevent API input validation failures
      const text = typeof rawText === 'string' ? rawText : String(rawText);

      if (!text || text.trim() === '') {
        throw new EmbeddingError('Cannot embed an empty or missing text block');
      }

      const response = await model.embedContent({
        model: modelName,
        contents: text,
        config: {
          taskType,
          outputDimensionality: OUTPUT_DIMENSIONS()
        }
      });

      // Handle structural variations in the SDK response payload
      const values = response?.embeddings?.[0]?.values ||
        response?.embedding?.values ||
        response?.values;

      if (!values) {
        console.error('[embedding] Unexpected API response shape:', JSON.stringify(response));
        throw new EmbeddingError('Missing values in embedding response');
      }

      return values;
    });

    const embeddings = await Promise.all(embeddingPromises);

    if (embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${embeddings.length}`);
    }

    return embeddings;

  } catch (error) {
    // Retry on rate limits (429) as well as temporary gateway/service disruptions (502, 503)
    const isRetryable = is429Error(error) ||
      error?.status === 429 ||
      error?.status === 502 ||
      error?.status === 503 ||
      error?.message?.includes('RESOURCE_EXHAUSTED') ||
      error?.message?.includes('Service Unavailable') ||
      error?.message?.includes('Bad Gateway');

    if (isRetryable && attempt < maxAttempts) {
      // Scale wait dynamically if it's a structural gateway error
      const baseDelay = error.retryAfter || (attempt * RETRY_WAIT_MS);
      const retryDelay = error?.status === 429 ? GROUP_WAIT_MS : baseDelay;

      console.log(`[embedding] Transient error (${error?.status || 'unknown'}), waiting ${retryDelay / 1000}s (attempt ${attempt}/${maxAttempts})...`);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
      return embedBatch(texts, taskType, attempt + 1);
    }

    throw new EmbeddingError(error.message || 'Batch embedding failed');
  }
}

// Exported for documents.js upload handler — embeds one batch group (up to BATCH_SIZE texts)
// and returns raw vectors array. Caller manages parallelism, waiting, and Chroma writes.
export async function embedSingleBatchGroup(texts, taskType = 'RETRIEVAL_DOCUMENT') {
  console.log(`[embedding] embedSingleBatchGroup — ${texts.length} texts, taskType=${taskType}`);
  const vectors = await embedBatch(texts, taskType);
  console.log(`[embedding] embedSingleBatchGroup — got ${vectors.length} vectors`);
  return vectors;
}

// Full pipeline: embed all chunks with built-in batching + waiting.
// Used by seed ingestion and any callers that don't need streaming progress.
export async function generateEmbeddings(chunks, taskType = 'RETRIEVAL_DOCUMENT', onProgress) {
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

    console.log(`[embedding] Group ${groupNum}/${totalGroups} — ${parallelBatches.length} batch call(s) in parallel (chunks ${i * batchSize + 1}–${chunksCovered})...`);

    const results = await Promise.allSettled(
      parallelBatches.map(batch => embedBatch(batch.map(c => c.text), taskType))
    );

    const failedBatches = [];
    results.forEach((result, batchIdx) => {
      const batch = parallelBatches[batchIdx];
      if (result.status === 'fulfilled') {
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
        console.warn(`[embedding] Batch ${i + batchIdx} failed, will retry individually:`, result.reason?.message);
        failedBatches.push({ batch, batchIdx: i + batchIdx });
      }
    });

    if (onProgress) {
      onProgress({ current_batch: groupNum, total_batches: totalGroups });
    }

    const isLastGroup = i + parallelCalls >= batches.length;
    if (!isLastGroup || failedBatches.length > 0) {
      console.log(`[embedding] Waiting ${GROUP_WAIT_MS / 1000}s before next group...`);
      await new Promise(resolve => setTimeout(resolve, GROUP_WAIT_MS));
    }

    for (const { batch, batchIdx } of failedBatches) {
      console.log(`[embedding] Waiting ${RETRY_WAIT_MS / 1000}s before retrying failed batch ${batchIdx}...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_WAIT_MS));
      for (const chunk of batch) {
        try {
          const vectors = await embedBatch([chunk.text], taskType);
          embeddings.push({
            id: chunk.metadata?.chunk_id || `chunk_retry_${batchIdx}`,
            embedding: vectors[0],
            metadata: chunk.metadata,
            text: chunk.text
          });
          console.log(`[embedding] ✅ Retry succeeded for chunk ${chunk.metadata?.chunk_id}`);
        } catch (err) {
          console.error(`[embedding] ❌ Retry failed for chunk ${chunk.metadata?.chunk_id}:`, err.message);
        }
      }
    }
  }

  return embeddings;
}

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
    outputDimensions: OUTPUT_DIMENSIONS()
  };
}
