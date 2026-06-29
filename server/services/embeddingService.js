import { GoogleGenerativeAI } from '@google/generative-ai';
import { EmbeddingError, is429Error } from '../utils/errors.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const embeddingModel = genAI.getGenerativeModel({
  model: process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-2'
});
// Rate limiting state
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

async function waitForRateLimit() {
  const now = Date.now();
  const windowElapsed = now - rateLimitState.windowStart;

  // Reset window if a minute has passed
  if (windowElapsed >= 60000) {
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = now;
    return;
  }

  // Check if we're at the rate limit
  const remainingTokens = rateLimitState.maxTokensPerMinute - rateLimitState.tokenCount;
  if (remainingTokens <= 0) {
    const waitTime = 60000 - windowElapsed;
    console.log(`Rate limit reached, waiting ${Math.ceil(waitTime / 1000)}s`);
    await new Promise(resolve => setTimeout(resolve, waitTime));
    rateLimitState.tokenCount = 0;
    rateLimitState.windowStart = Date.now();
  }
}

async function embedWithRetry(text, attempt = 1, maxAttempts = 5) {
  const baseRetryDelay = 60000; // 60 seconds for 429

  try {
    const result = await embeddingModel.embedContent(text);

    if (result.embedding) {
      return result.embedding.values;
    }

    throw new EmbeddingError('No embedding returned from API');
  } catch (error) {
    // Check for 429 rate limit
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

async function embedBatch(texts) {
  // Join texts for single embedding call (Gemini supports up to specified token limit)
  const combinedText = texts.join('\n\n---\n\n');

  const tokens = estimateTokens(combinedText);
  await waitForRateLimit();

  rateLimitState.tokenCount += tokens;

  return embedWithRetry(combinedText);
}

export async function generateEmbeddings(chunks) {
  if (!chunks || chunks.length === 0) {
    return [];
  }

  const embeddings = [];
  const maxChunksPerCall = rateLimitState.maxChunksPerCall;
  const maxParallelCalls = rateLimitState.parallelCalls;

  // Split chunks into groups of maxChunksPerCall
  const groups = [];
  for (let i = 0; i < chunks.length; i += maxChunksPerCall) {
    groups.push(chunks.slice(i, i + maxChunksPerCall));
  }

  // Process groups in batches of parallel calls
  for (let i = 0; i < groups.length; i += maxParallelCalls) {
    const batch = groups.slice(i, i + maxParallelCalls);

    // Wait 1 minute between call groups (not on first group)
    if (i > 0) {
      console.log('Waiting 1 minute before next embedding batch...');
      await new Promise(resolve => setTimeout(resolve, 60000));
    }

    // Execute batch in parallel
    const batchPromises = batch.map(async (group) => {
      const texts = group.map(c => c.text);
      try {
        return await embedBatch(texts);
      } catch (error) {
        console.error('Batch embedding failed:', error);
        // Return null for failed batches - we'll handle this below
        return null;
      }
    });

    const batchResults = await Promise.all(batchPromises);

    // Process results
    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      const group = batch[j];

      if (result) {
        // If embedding succeeded, we need to split back into individual embeddings
        // Since Gemini returns one embedding for the combined text, we'll generate individual embeddings
        for (const chunk of group) {
          try {
            const individualEmbedding = await embedWithRetry(chunk.text);
            embeddings.push({
              id: chunk.metadata.chunk_id,
              embedding: individualEmbedding,
              metadata: chunk.metadata,
              text: chunk.text
            });
          } catch (error) {
            console.error(`Failed to embed chunk ${chunk.metadata.chunk_id}:`, error);
          }
        }
      } else {
        // Retry failed group individually
        for (const chunk of group) {
          try {
            const individualEmbedding = await embedWithRetry(chunk.text);
            embeddings.push({
              id: chunk.metadata.chunk_id,
              embedding: individualEmbedding,
              metadata: chunk.metadata,
              text: chunk.text
            });
          } catch (error) {
            console.error(`Failed to embed chunk ${chunk.metadata.chunk_id}:`, error);
          }
        }
      }
    }
  }

  return embeddings;
}

export async function embedQuery(query) {
  await waitForRateLimit();
  return embedWithRetry(query);
}

export async function embedSingle(text) {
  await waitForRateLimit();
  return embedWithRetry(text);
}

export function getRateLimitState() {
  return { ...rateLimitState };
}
