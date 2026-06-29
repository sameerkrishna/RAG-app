import { getGlobalCollection, getSessionCollection, queryCollection } from './chromaService.js';
import { embedQuery } from './embeddingService.js';
import { v4 as uuidv4 } from 'uuid';

const TOP_K = parseInt(process.env.TOP_K) || 5;
const COVERAGE_HIGH_THRESHOLD = parseFloat(process.env.COVERAGE_HIGH_THRESHOLD) || 0.75;
const COVERAGE_MEDIUM_THRESHOLD = parseFloat(process.env.COVERAGE_MEDIUM_THRESHOLD) || 0.55;

function computeSimilarityScore(distance) {
  // Convert distance to similarity (assuming cosine distance)
  return 1 - distance;
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
    // Embed the query
    const queryEmbedding = await embedQuery(query);

    // Query both collections in parallel
    const queries = [];

    // Global collection
    if (includeGlobal) {
      const globalCollection = await getGlobalCollection();
      queries.push({
        type: 'global',
        promise: queryCollection(globalCollection, queryEmbedding, topK)
      });
    }

    // Session collection
    if (sessionId) {
      try {
        const sessionCollection = await getSessionCollection(sessionId);
        if (sessionCollection) {
          queries.push({
            type: 'session',
            promise: queryCollection(sessionCollection, queryEmbedding, topK)
          });
        }
      } catch (e) {
        // Session collection doesn't exist yet, that's ok
      }
    }

    // Wait for all queries
    const results = await Promise.all(queries.map(async q => ({
      type: q.type,
      results: await q.promise
    })));

    // Merge and re-rank results
    const allResults = [];

    for (const { type, results: typeResults } of results) {
      for (const result of typeResults) {
        allResults.push({
          ...result,
          source_type: type // 'global' or 'session'
        });
      }
    }

    // Sort by score descending
    allResults.sort((a, b) => b.score - a.score);

    // Take top K after merging
    const topResults = allResults.slice(0, topK);

    // Calculate coverage
    const coverage = calculateCoverage(topResults, topK);

    return {
      results: topResults,
      coverage,
      queryEmbedding
    };
  } catch (error) {
    console.error('Retrieval error:', error);
    throw error;
  }
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
  return coverage.level === 'low';
}

export { calculateCoverage };
