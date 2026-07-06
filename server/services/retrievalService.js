import { getCollection, hybridQueryCollection } from './chromaService.js';
import { embedQuery } from './embeddingService.js';
import { v4 as uuidv4 } from 'uuid';

const TOP_K = parseInt(process.env.TOP_K) || 20;
const REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;

function calculateCoverage(results, topK = 5) {
  if (!results || results.length === 0) return { confidence: 0, topScore: 0 };
  const scores = results.slice(0, topK).map(r => Math.max(0, r.score));
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  return {
    confidence: Math.round(avgScore * 100),
    topScore: Math.max(...scores)
  };
}

// ── Main retrieval function (Hybrid: dense + BM25 via Chroma RRF) ──────
export async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || 5;

  try {
    const [queryEmbedding, { collection }] = await Promise.all([
      embedQuery(query),
      getCollection()
    ]);

    if (!collection) {
      console.warn(`⚠️  No collection available`);
      return { results: [], coverage: { confidence: 0, topScore: 0, level: 'low', score: 0 }, queryEmbedding };
    }

    // Build metadata filter: include both 'global' vectors and this session's vectors
    const where = sessionId
      ? { session_id: { "$in": ["global", sessionId] } }
      : { session_id: "global" };

    const rawResults = await hybridQueryCollection(collection, query, queryEmbedding, topK, where);

    const results = rawResults.map(r => ({
      ...r,
      source_type: r.metadata?.source_type || 'session'
    }));

    const coverage = calculateCoverage(results, topK);
    const topScore = coverage.topScore;
    const level = topScore >= 0.6 ? 'high' : topScore >= 0.3 ? 'medium' : 'low';

    console.log('🔍 Query:', query);
    console.log('📊 Coverage:', { ...coverage, level });
    console.log('📈 Scores:', results.map(r => r.score.toFixed(4)));

    return {
      results,
      coverage: { ...coverage, level, score: topScore },
      queryEmbedding
    };

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
    const sourceLabel = result.source_type === 'session_upload' ? '[Session Upload]' : '[Seed Document]';
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
    excerpt: result.text,
    score: result.score,
    sourceType: result.source_type,
    chunkId: result.id
  }));
}

export function shouldShowRefusal(coverage) {
  return coverage.topScore < REFUSAL_THRESHOLD;
}

export { calculateCoverage };
