import { GoogleGenerativeAI } from '@google/generative-ai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const embeddingModel = genAI.getGenerativeModel({
  model: process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-2'
});

const rateLimitState = {
  tokenCount: 0,
  windowStart: Date.now(),
  maxTokensPerMinute: parseInt(process.env.EMBEDDING_RATE_LIMIT_TOKENS_PER_MINUTE) || 30000,
  parallelCalls: parseInt(process.env.EMBEDDING_PARALLEL_CALLS) || 4,
  maxChunksPerCall: parseInt(process.env.EMBEDDING_BATCH_MAX_CHUNKS) || 7,
  lastCallGroupTime: null
};

function estimateTokens(text) {
  return Math.ceil(text.length / 4);
}

// ✅ FIX 3: Accept tokens param so callers can accurately track usage
async function waitForRateLimit(tokens = 0) {
  const now = Date.now();
  const windowElapsed = now - rateLimitState.windowStart;

  if (windowElapsed >= 60000) {
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = now;
  }

  const remainingTokens = rateLimitState.maxTokensPerMinute - rateLimitState.tokenCount;
  if (remainingTokens <= 0) {
    const waitTime = 60000 - (Date.now() - rateLimitState.windowStart);
    console.log(`Rate limit reached, waiting ${Math.ceil(waitTime / 1000)}s`);
    await new Promise(resolve => setTimeout(resolve, waitTime));
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = Date.now();
  }

  // ✅ FIX 3: Increment token count for this call
  rateLimitState.tokenCount += tokens;
}

async function embedWithRetry(text, attempt = 1, maxAttempts = 5) {
  const baseRetryDelay = 60000;
  // ✅ FIX 4: Short delay for transient spurious API_KEY_INVALID 400s
  const invalidKeyRetryDelay = 2000;

  try {
    const result = await embeddingModel.embedContent(text);

    if (result.embedding) {
      return result.embedding.values;
    }

    throw new EmbeddingError('No embedding returned from API');
  } catch (error) {
    // ✅ FIX 4: Retry on intermittent spurious API_KEY_INVALID — valid keys
    // occasionally get a 400 from Google's gateway on cold/first requests
    const isSpuriousInvalidKey =
      error?.status === 400 &&
      error?.message?.includes('API_KEY_INVALID');

    if (isSpuriousInvalidKey) {
      if (attempt >= maxAttempts) {
        throw new EmbeddingError('API key validation failed after retries — check GEMINI_API_KEY');
      }
      console.warn(`Spurious API_KEY_INVALID (attempt ${attempt}/${maxAttempts}), retrying in ${invalidKeyRetryDelay / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, invalidKeyRetryDelay));
      return embedWithRetry(text, attempt + 1, maxAttempts);
    }

    if (is429Error(error) || error?.status === 429 || error?.message?.includes('RESOURCE_EXHAUSTED')) {
      if (attempt >= maxAttempts) {
        throw new EmbeddingError('Max retry attempts reached for rate limiting');
      }

      const retryDelay = error.retryAfter || baseRetryDelay;
      console.log(`Rate limited, waiting ${retryDelay / 1000}s before retry ${attempt}/${maxAttempts}`);

      await new Promise(resolve => setTimeout(resolve, retryDelay));
      return embedWithRetry(text, attempt + 1, maxAttempts);
    }

    throw new EmbeddingError(error.message || 'Embedding generation failed');
  }
}

export async function generateEmbeddings(chunks) {
  if (!chunks || chunks.length === 0) {
    return [];
  }

  const embeddings = [];
  const maxChunksPerCall = rateLimitState.maxChunksPerCall;
  const maxParallelCalls = rateLimitState.parallelCalls;

  const groups = [];
  for (let i = 0; i < chunks.length; i += maxChunksPerCall) {
    groups.push(chunks.slice(i, i + maxChunksPerCall));
  }

  for (let i = 0; i < groups.length; i += maxParallelCalls) {
    const batch = groups.slice(i, i + maxParallelCalls);

    if (i > 0) {
      console.log('Waiting 1 minute before next embedding batch...');
      await new Promise(resolve => setTimeout(resolve, 60000));
    }

    // ✅ FIX 2: Removed embedBatch() — it was called and its result discarded,
    // then every chunk was re-embedded individually anyway. Embed directly.
    const batchPromises = batch.flatMap(group =>
      group.map(async (chunk) => {
        const tokens = estimateTokens(chunk.text);
        await waitForRateLimit(tokens);
        try {
          const embedding = await embedWithRetry(chunk.text);
          return {
            id: chunk.metadata.chunk_id,
            embedding,
            metadata: chunk.metadata,
            text: chunk.text
          };
        } catch (error) {
          console.error(`Failed to embed chunk ${chunk.metadata.chunk_id}:`, error);
          return null;
        }
      })
    );

    const results = await Promise.all(batchPromises);
    for (const result of results) {
      if (result) embeddings.push(result);
    }
  }

  return embeddings;
}

export async function embedQuery(query) {
  // ✅ FIX 3: Track tokens so rate limit state stays accurate
  const tokens = estimateTokens(query);
  await waitForRateLimit(tokens);
  return embedWithRetry(query);
}

export async function embedSingle(text) {
  // ✅ FIX 3: Track tokens so rate limit state stays accurate
  const tokens = estimateTokens(text);
  await waitForRateLimit(tokens);
  return embedWithRetry(text);
}

export function getRateLimitState() {
  return { ...rateLimitState };
}