import { VertexAI } from '@google-cloud/vertexai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

let embeddingModel = null;

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT;
const LOCATION = process.env.GOOGLE_CLOUD_LOCATION || 'us-central1';
const MODEL_NAME = 'gemini-embedding-001';
const OUTPUT_DIMENSIONS = 3072;

const vertexAI = new VertexAI({
  project: PROJECT_ID,
  location: LOCATION
});

function getEmbeddingModel() {
  if (!embeddingModel) {
    embeddingModel = vertexAI.getGenerativeModel({
      model: MODEL_NAME
    });
  }
  return embeddingModel;
}

const BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
const PARALLEL_CALLS = () => parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
const GROUP_WAIT_MS = 61000;
const RETRY_WAIT_MS = 15000;

async function embedBatch(texts, taskType = 'RETRIEVAL_DOCUMENT', attempt = 1) {
  const maxAttempts = 5;

  try {
    const model = getEmbeddingModel();

    const embeddingPromises = texts.map(async (text) => {
      const response = await model.embedContent({
        content: {
          parts: [{ text }]
        },
        taskType,
        outputDimensionality: OUTPUT_DIMENSIONS
      });

      const values = response?.embedding?.values;
      if (!values) {
        throw new EmbeddingError('Missing values in individual embedding response');
      }

      return values;
    });

    const embeddings = await Promise.all(embeddingPromises);

    if (embeddings.length !== texts.length) {
      throw new EmbeddingError(`Expected ${texts.length} embeddings, got ${embeddings.length}`);
    }

    return embeddings;
  } catch (error) {
    const is429 =
      is429Error(error) ||
      error?.status === 429 ||
      error?.statusCode === 429 ||
      error?.message?.includes('RESOURCE_EXHAUSTED') ||
      error?.message?.includes('429');

    if (is429 && attempt < maxAttempts) {
      const retryDelay = error.retryAfter || GROUP_WAIT_MS;
      console.log(
        `[embedding] Rate limited, waiting ${retryDelay / 1000}s (attempt ${attempt}/${maxAttempts})`
      );
      await new Promise(resolve => setTimeout(resolve, retryDelay));
      return embedBatch(texts, taskType, attempt + 1);
    }

    throw new EmbeddingError(error.message || 'Batch embedding failed');
  }
}

export async function embedSingleBatchGroup(texts, taskType = 'RETRIEVAL_DOCUMENT') {
  console.log(`[embedding] embedSingleBatchGroup — ${texts.length} texts, taskType=${taskType}`);
  const vectors = await embedBatch(texts, taskType);
  console.log(`[embedding] embedSingleBatchGroup — got ${vectors.length} vectors`);
  return vectors;
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

    console.log(
      `[embedding] Group ${groupNum}/${totalGroups} — ${parallelBatches.length} batch call(s) in parallel (chunks ${i * batchSize + 1}–${chunksCovered})...`
    );

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
        console.warn(
          `[embedding] Batch ${i + batchIdx} failed, will retry individually:`,
          result.reason?.message
        );
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
    outputDimensions: OUTPUT_DIMENSIONS
  };
}