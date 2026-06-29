import { GoogleGenerativeAI } from '@google/generative-ai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const embeddingModel = genAI.getGenerativeModel({
  // ✅ FIX 1: Correct model name — 'gemini-embedding-2' does not exist
  model: process.env.GEMINI_EMBEDDING_MODEL || 'text-embedding-004'
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

  // ✅ FIX 3: Track tokens for this call so rate limiting is accurate
  rateLimitState.tokenCount += tokens;
}

async function embedWithRetry(text, attempt = 1, maxAttempts = 5) {
  const baseRetryDelay = 60000;

  try {
    const result = await embeddingModel.embedContent(text);

    if (result.embedding) {
      return result.embedding.values;
    }

    throw new EmbeddingError('No embedding returned from API');
  } catch (error) {
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

    // ✅ FIX 2: Removed the pointless embedBatch() call — embed each chunk
    // individually directly. The old code called embedBatch() then threw
    // the result away and re-embedded everything individually anyway.
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
  const tokens = estimateTokens(query);
  // ✅ FIX 3: Pass token count so rate limit state stays accurate
  await waitForRateLimit(tokens);
  return embedWithRetry(query);
}

export async function embedSingle(text) {
  const tokens = estimateTokens(text);
  await waitForRateLimit(tokens);
  return embedWithRetry(text);
}

export function getRateLimitState() {
  return { ...rateLimitState };
}