import { GoogleGenerativeAI } from '@google/generative-ai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

let genAI = null;
let embeddingModel = null;

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
const OUTPUT_DIMENSIONS = () => parseInt(process.env.GEMINI_EMBEDDING_DIMENSIONS) || 3072;
const GROUP_WAIT_MS = 61000;
const RETRY_WAIT_MS = 15000; // FIX 3: wait before individual chunk retries

async function embedBatch(texts, taskType = 'RETRIEVAL_DOCUMENT', attempt = 1) {
  const maxAttempts = 5;
  const modelName = process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001';

  try {
    const model = getEmbeddingModel();

    const result = await model.batchEmbedContents({
      requests: texts.map(text => ({
        model: `models/${modelName}`,
        content: { parts: [{ text }] },
        taskType,
        outputDimensionality: OUTPUT_DIMENSIONS()
      }))
    });

    if (!result?.embeddings || result.embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${result?.embeddings?.length ?? 0}`);
    }

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

    console.log(`  Embedding group ${groupNum}/${totalGroups} — ${parallelBatches.length} batch call(s) in parallel (chunks ${i * batchSize + 1}–${chunksCovered})...`);

    const results = await Promise.allSettled(
      parallelBatches.map(batch => embedBatch(batch.map(c => c.text), taskType))
    );

    const failedBatches = [];
    results.forEach((result, batchIdx) => {
      const batch = parallelBatches[batchIdx];
      if (result.status === 'fulfilled') {
        const vectors = result.value;
        batch.forEach((chunk, chunkIdx) => {
          // FIX 2: correct fallback chunk ID — (i + batchIdx) is the absolute batch index
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
      console.log(`  Waiting ${GROUP_WAIT_MS / 1000}s before next group...`);
      await new Promise(resolve => setTimeout(resolve, GROUP_WAIT_MS));
    }

    // FIX 3: wait before retrying individual chunks to avoid immediate 429
    for (const { batch, batchIdx } of failedBatches) {
      console.log(`  Waiting ${RETRY_WAIT_MS / 1000}s before retrying failed batch ${batchIdx}...`);
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
          console.log(`  ✅ Retry succeeded for chunk ${chunk.metadata?.chunk_id}`);
        } catch (err) {
          console.error(`  ❌ Retry failed for chunk ${chunk.metadata?.chunk_id}:`, err.message);
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