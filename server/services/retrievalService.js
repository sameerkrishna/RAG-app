import { getGlobalCollection, getSessionCollection, queryCollection } from './chromaService.js';
import { embedQuery } from './embeddingService.js';
import { v4 as uuidv4 } from 'uuid';

const TOP_K = parseInt(process.env.TOP_K) || 5;
const REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;

function calculateCoverage(results, topK = TOP_K) {
  if (!results || results.length === 0) {
    return { confidence: 0, topScore: 0 };
  }

  const scores = results.slice(0, topK).map(r => r.score);
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;

  return {
    confidence: Math.round(avgScore * 100), // 0.47 → 47
    topScore: Math.max(...scores)
  };
}

export async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || TOP_K;
  const includeGlobal = options.includeGlobal !== false;

  try {
    const queryEmbedding = await embedQuery(query);
    const queries = [];

    if (includeGlobal) {
      const globalCollection = await getGlobalCollection();
      queries.push({ type: 'global', promise: queryCollection(globalCollection, queryEmbedding, topK) });
    }

    if (sessionId) {
      try {
        const sessionCollection = await getSessionCollection(sessionId);
        if (sessionCollection) {
          queries.push({ type: 'session', promise: queryCollection(sessionCollection, queryEmbedding, topK) });
        }
      } catch (e) {
        // Session collection doesn't exist yet — ok
      }
    }

    const results = await Promise.all(queries.map(async q => ({
      type: q.type,
      results: await q.promise
    })));

    const allResults = [];
    for (const { type, results: typeResults } of results) {
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

// Refuse only if top score is genuinely near-zero
export function shouldShowRefusal(coverage) {
  return coverage.topScore < REFUSAL_THRESHOLD;
}

export { calculateCoverage };