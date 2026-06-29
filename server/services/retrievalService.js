import { getGlobalCollection, getSessionCollection, queryCollection } from './chromaService.js';
import { embedQuery } from './embeddingService.js';
import { v4 as uuidv4 } from 'uuid';

const TOP_K = parseInt(process.env.TOP_K) || 5;
const COVERAGE_HIGH_THRESHOLD = parseFloat(process.env.COVERAGE_HIGH_THRESHOLD) || 0.75;
const COVERAGE_MEDIUM_THRESHOLD = parseFloat(process.env.COVERAGE_MEDIUM_THRESHOLD) || 0.55;

// ✅ Cache resolved collection objects — never hit Chroma more than once per session
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
    return { level: 'low', score: 0, reason: 'No results found' };
  }

  const topResults = results.slice(0, topK);
  const scores = topResults.map(r => r.score);
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;

  let level;
  let reason;

  if (avgScore >= COVERAGE_HIGH_THRESHOLD) {
    level = 'high';
    reason = 'High confidence in retrieved context';
  } else if (avgScore >= COVERAGE_MEDIUM_THRESHOLD) {
    level = 'medium';
    reason = 'Moderate confidence in retrieved context';
  } else {
    level = 'low';
    reason = 'Insufficient relevant information found';
  }

  return {
    level,
    score: avgScore,
    topScore: Math.max(...scores),
    bottomScore: Math.min(...scores),
    reason
  };
}

export async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || TOP_K;
  const includeGlobal = options.includeGlobal !== false;

  try {
    // ✅ Run embedding + both collection fetches in parallel
    // Collections are served from cache after the first call — zero Chroma round-trips
    const [queryEmbedding, globalCollection, sessionCollection] = await Promise.all([
      embedQuery(query),
      includeGlobal ? getOrCacheGlobalCollection() : Promise.resolve(null),
      sessionId ? getOrCacheSessionCollection(sessionId) : Promise.resolve(null)
    ]);

    // ✅ Query both collections in parallel
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

    return { results: topResults, coverage, queryEmbedding };

  } catch (error) {
    console.error('Retrieval error:', error);
    throw error;
  }
}

// ✅ Call this after a user uploads a document to a session
// so the next query fetches the updated collection fresh
export function invalidateSessionCollectionCache(sessionId) {
  cachedSessionCollections.delete(sessionId);
}

export function formatContextForPrompt(results, maxTokens = 7000) {
  if (!results || results.length === 0) {
    return '';
  }

  let totalTokens = 0;
  const maxTokensPerChar = 4;
  const contextParts = [];

  for (let i = 0; i < results.length; i++) {
    const result = results[i];
    const tokenEstimate = result.text.length / maxTokensPerChar;

    if (totalTokens + tokenEstimate > maxTokens) {
      break;
    }

    totalTokens += tokenEstimate;

    const sourceLabel = result.source_type === 'global' ? '[Seed Document]' : '[Session Upload]';
    const citation = `[${i + 1}] ${sourceLabel} ${result.metadata.filename || 'Unknown'}`;
    const page = result.metadata.page_number ? ` (Page ${result.metadata.page_number})` : '';

    contextParts.push(`${citation}${page}:\n${result.text}`);
  }

  return contextParts.join('\n\n---\n\n');
}

export function generateCitations(results) {
  if (!results || results.length === 0) {
    return [];
  }

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

export function shouldShowRefusal(coverage) {
  return coverage.level === 'low' && coverage.score > 0;
}

export { calculateCoverage };