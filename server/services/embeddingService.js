import { GoogleGenerativeAI } from '@google/generative-ai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

// ✅ Lazy init — avoids dotenv timing bug
let genAI = null;
let embeddingModel = null;

function getEmbeddingModel() {
  if (!embeddingModel) {
    genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
    embeddingModel = genAI.getGenerativeModel({
      model: process.env.GEMINI_EMBEDDING_MODEL || 'text-embedding-004'
    });
  }
  return embeddingModel;
}

const BATCH_SIZE = () => parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7;
const PARALLEL_CALLS = () => parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4;
const GROUP_WAIT_MS = 61000; // 61s to safely clear the 1-minute rate limit window

function estimateTokens(text) {
  return Math.ceil(text.length / 4);
}

// ✅ Embed a single chunk — one API call, one embedding returned
async function embedSingleChunk(text, taskType = 'RETRIEVAL_DOCUMENT', attempt = 1) {
  const maxAttempts = 5;
  try {
    const model = getEmbeddingModel();
    const result = await model.embedContent({
      content: { parts: [{ text }] },
      taskType
    });
    if (!result?.embedding?.values) {
      throw new EmbeddingError('No embedding returned from API');
    }
    return result.embedding.values;
  } catch (error) {
    const is429 = is429Error(error) ||
      error?.status === 429 ||
      error?.message?.includes('RESOURCE_EXHAUSTED');

    if (is429 && attempt < maxAttempts) {
      const retryDelay = error.retryAfter || GROUP_WAIT_MS;
      console.log(`Rate limited on embed, waiting ${retryDelay / 1000}s (attempt ${attempt}/${maxAttempts})`);
      await new Promise(resolve => setTimeout(resolve, retryDelay));
      return embedSingleChunk(text, taskType, attempt + 1);
    }
    throw new EmbeddingError(error.message || 'Embedding generation failed');
  }
}

// ✅ Generate embeddings for an array of chunks
// Strategy: 4 parallel calls per group, 60s wait between groups
export async function generateEmbeddings(chunks, taskType = 'RETRIEVAL_DOCUMENT', onProgress) {
  if (!chunks || chunks.length === 0) return [];

  const parallelCalls = PARALLEL_CALLS();
  const embeddings = [];

  // Each chunk = one API call (Gemini embedding is per-text, not per-batch)
  // Group into parallel sets of PARALLEL_CALLS
  const totalGroups = Math.ceil(chunks.length / parallelCalls);

  for (let i = 0; i < chunks.length; i += parallelCalls) {
    const group = chunks.slice(i, i + parallelCalls);
    const groupNum = Math.floor(i / parallelCalls) + 1;

    console.log(`  Embedding group ${groupNum}/${totalGroups} (${group.length} chunks in parallel)...`);

    // ✅ Send group in parallel using Promise.allSettled
    const results = await Promise.allSettled(
      group.map(chunk => embedSingleChunk(chunk.text, taskType))
    );

    // Collect results, retry failed ones individually
    const failedChunks = [];
    results.forEach((result, idx) => {
      if (result.status === 'fulfilled') {
        embeddings.push({
          id: group[idx].metadata?.chunk_id || `chunk_${i + idx}`,
          embedding: result.value,
          metadata: group[idx].metadata,
          text: group[idx].text
        });
      } else {
        console.warn(`  Chunk ${i + idx} failed in group, will retry:`, result.reason?.message);
        failedChunks.push({ chunk: group[idx], idx: i + idx });
      }
    });

    if (onProgress) {
      onProgress({ current_batch: groupNum, total_batches: totalGroups });
    }

    // ✅ Wait between groups to respect rate limits (skip wait after last group)
    const isLastGroup = i + parallelCalls >= chunks.length;
    if (!isLastGroup || failedChunks.length > 0) {
      console.log(`  Waiting ${GROUP_WAIT_MS / 1000}s before next group...`);
      await new Promise(resolve => setTimeout(resolve, GROUP_WAIT_MS));
    }

    // Retry failed chunks individually after the wait
    for (const { chunk, idx } of failedChunks) {
      try {
        const embedding = await embedSingleChunk(chunk.text, taskType);
        embeddings.push({
          id: chunk.metadata?.chunk_id || `chunk_${idx}`,
          embedding,
          metadata: chunk.metadata,
          text: chunk.text
        });
        console.log(`  ✅ Retry succeeded for chunk ${idx}`);
      } catch (err) {
        console.error(`  ❌ Retry failed for chunk ${idx}:`, err.message);
      }
    }
  }

  return embeddings;
}

// ✅ Single query embedding — RETRIEVAL_QUERY task type
export async function embedQuery(query) {
  return embedSingleChunk(query, 'RETRIEVAL_QUERY');
}

export async function embedSingle(text) {
  return embedSingleChunk(text, 'RETRIEVAL_DOCUMENT');
}

export function getRateLimitState() {
  return {
    maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 30000,
    parallelCalls: PARALLEL_CALLS(),
    maxChunksPerCall: BATCH_SIZE(),
  };
}