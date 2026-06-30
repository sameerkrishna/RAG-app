import { getGlobalCollection, getSessionCollection, queryCollection } from './chromaService.js';
import { embedQuery } from './embeddingService.js';
import { v4 as uuidv4 } from 'uuid';

const TOP_K = parseInt(process.env.TOP_K) || 5;
const REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;

// Cache resolved collection objects — never hit Chroma more than once per session
let cachedGlobalCollection = null;
const cachedSessionCollections = new Map();

async function getOrCacheGlobalCollection() {
  if (!cachedGlobalCollection) {
    cachedGlobalCollection = await getGlobalCollection();
  }
  return cachedGlobalCollection;
}

async function getOrCacheSessionCollection(sessionId) {
  if (cachedSessionCollections.has(sessionId)) {
    return cachedSessionCollections.get(sessionId);
  }
  try {
    const collection = await getSessionCollection(sessionId);
    if (collection) {
      cachedSessionCollections.set(sessionId, collection);
    }
    return collection;
  } catch {
    return null;
  }
}

function calculateCoverage(results, topK = TOP_K) {
  if (!results || results.length === 0) {
    return { confidence: 0, topScore: 0 };
  }

  // Clamp negative scores (possible with cosine distance > 1) to 0
  const scores = results.slice(0, topK).map(r => Math.max(0, r.score));
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;

  return {
    confidence: Math.round(avgScore * 100),
    topScore: Math.max(...scores)
  };
}

export async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || TOP_K;
  const includeGlobal = options.includeGlobal !== false;

  try {
    // Run embedding + both collection fetches in parallel
    const [queryEmbedding, globalCollection, sessionCollection] = await Promise.all([
      embedQuery(query),
      includeGlobal ? getOrCacheGlobalCollection() : Promise.resolve(null),
      sessionId ? getOrCacheSessionCollection(sessionId) : Promise.resolve(null)
    ]);

    // Query both collections in parallel
    const queryPromises = [];

    if (globalCollection) {
      queryPromises.push(
        queryCollection(globalCollection, queryEmbedding, topK)
          .then(results => ({ type: 'global', results }))
          .catch(() => ({ type: 'global', results: [] }))
      );
    }

    if (sessionCollection) {
      queryPromises.push(
        queryCollection(sessionCollection, queryEmbedding, topK)
          .then(results => ({ type: 'session', results }))
          .catch(() => ({ type: 'session', results: [] }))
      );
    }

    const queryResults = await Promise.all(queryPromises);

    const allResults = [];
    for (const { type, results: typeResults } of queryResults) {
      for (const result of typeResults) {
        allResults.push({ ...result, source_type: type });
      }
    }

    allResults.sort((a, b) => b.score - a.score);
    const topResults = allResults.slice(0, topK);
    const coverage = calculateCoverage(topResults, topK);

    // Debug log — remove once confirmed working
    console.log('🔍 Query:', query);
    console.log('📊 Coverage:', coverage);
    console.log('📈 Raw scores:', topResults.map(r => r.score.toFixed(4)));

    return { results: topResults, coverage, queryEmbedding };

  } catch (error) {
    console.error('Retrieval error:', error);
    throw error;
  }
}

// Call this after a user uploads a document to a session
// so the next query fetches the updated collection fresh
export function invalidateSessionCollectionCache(sessionId) {
  cachedSessionCollections.delete(sessionId);
}

export function formatContextForPrompt(results, maxTokens = 7000) {
  if (!results || results.length === 0) return '';

  let totalTokens = 0;
  const contextParts = [];

  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    const tokenEstimate = result.text.length / 4;
    if (totalTokens + tokenEstimate > maxTokens) break;
    totalTokens += tokenEstimate;

    const sourceLabel = result.source_type === 'global' ? '[Seed Document]' : '[Session Upload]';
    const page = result.metadata.page_number ? ` (Page ${result.metadata.page_number})` : '';
    contextParts.push(`[${i + 1}] ${sourceLabel} ${result.metadata.filename || 'Unknown'}${page}:\n${result.text}`);
  }

  return contextParts.join('\n\n---\n\n');
}

export function generateCitations(results) {
  if (!results || results.length === 0) return [];

  return results.map((result, idx) => ({
    id: uuidv4(),
    index: idx + 1,
    documentId: result.metadata.document_id,
    filename: result.metadata.filename,
    pageNumber: result.metadata.page_number,
    section: result.metadata.section_title,
    excerpt: result.text.slice(0, 200) + (result.text.length > 200 ? '...' : ''),
    score: result.score,
    sourceType: result.source_type,
    chunkId: result.id
  }));
}

// Refuse only if top score is genuinely near-zero — no relevant content at all
export function shouldShowRefusal(coverage) {
  return coverage.topScore < REFUSAL_THRESHOLD;
}

export { calculateCoverage };
