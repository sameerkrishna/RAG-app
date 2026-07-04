import { getSessionCollection, queryCollection } from './chromaService.js';
import { embedQuery } from './embeddingService.js';
import { v4 as uuidv4 } from 'uuid';
import { BM25 } from 'fast-bm25';

const TOP_K = parseInt(process.env.TOP_K) || 20;
const REFUSAL_THRESHOLD = parseFloat(process.env.REFUSAL_THRESHOLD) || 0.05;

const cachedSessionCollections = new Map();

// ── Hybrid search state (BM25 index, chunk lookup, chunk count snapshot) ──
const sessionBM25Indices = new Map();
const sessionChunksMap = new Map();
const sessionLastChunkCount = new Map();

/**
 * Rebuilds BM25 index and chunk lookup for a session if needed.
 */
async function rebuildSessionBM25Index(sessionId, sessionCollection) {
  const allData = await sessionCollection.get({ include: ['documents', 'metadatas'] });

  if (allData.ids && allData.ids.length > 0) {
    sessionBM25Indices.set(sessionId, new BM25(allData.documents));

    const lookUpCache = new Map();
    allData.ids.forEach((id, idx) => {
      lookUpCache.set(id, {
        id,
        text: allData.documents[idx],
        metadata: allData.metadatas[idx]
      });
    });
    sessionChunksMap.set(sessionId, lookUpCache);
    sessionLastChunkCount.set(sessionId, allData.ids.length);
    console.log(`♻️ JIT Matrix Rebuilt for session ${sessionId} with ${allData.ids.length} chunks.`);
  }
}

/**
 * TWO-STAGE JIT RETRIEVAL ENGINE (BM25 + vector → RRF fusion → Cohere rerank)
 */
async function dynamicSessionSearchPipeline(sessionId, sessionCollection, queryText, queryEmbedding, finalTopK = 5) {
  try {
    // Just-in-time parity verification – rebuild BM25 if chunks changed
    const currentChromaCount = await sessionCollection.count();
    const cachedCount = sessionLastChunkCount.get(sessionId) || 0;

    if (currentChromaCount !== cachedCount || !sessionBM25Indices.has(sessionId)) {
      await rebuildSessionBM25Index(sessionId, sessionCollection);
    }

    const currentBM25 = sessionBM25Indices.get(sessionId);
    const currentLookupCache = sessionChunksMap.get(sessionId);

    if (!currentBM25 || !currentLookupCache) return [];

    // Stage 1: Hybrid retrieval (vector + BM25) with Reciprocal Rank Fusion
    const vectorResults = await queryCollection(sessionCollection, queryEmbedding, TOP_K);
    const bm25Scores = currentBM25.search(queryText);

    const allChunkIds = Array.from(currentLookupCache.keys());
    const lexicalResults = bm25Scores
      .map((score, index) => ({ id: allChunkIds[index], score }))
      .filter(item => item.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, TOP_K);

    // RRF merge
    const rrfScores = {};
    const k_constant = 60;

    vectorResults.forEach((item, index) => {
      rrfScores[item.id] = (rrfScores[item.id] || 0) + (1 / (k_constant + (index + 1)));
    });

    lexicalResults.forEach((item, index) => {
      rrfScores[item.id] = (rrfScores[item.id] || 0) + (1 / (k_constant + (index + 1)));
    });

    const candidateIds = Object.keys(rrfScores)
      .sort((a, b) => rrfScores[b] - rrfScores[a])
      .slice(0, TOP_K);

    const candidateChunks = candidateIds
      .map(id => currentLookupCache.get(id))
      .filter(Boolean);   // remove any missing entries

    if (candidateChunks.length === 0) return [];

    // ── Stage 2: Rerank via OpenRouter ────────────────────────────────
    const documentsForRerank = candidateChunks.map(chunk => chunk.text);

    // Helper: build a safe result object with guaranteed numeric score
    const safeResult = (chunk, score) => ({
      id: chunk.id,
      text: chunk.text,
      metadata: chunk.metadata,
      score: typeof score === 'number' ? score : 0.5,
      source_type: chunk.metadata?.source_type || 'session',
    });

    try {
  const response = await fetch('https://openrouter.ai/api/v1/rerank', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'cohere/rerank-v3.5',  // ✅ corrected
      query: queryText,
      documents: documentsForRerank,
      top_n: finalTopK,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error('OpenRouter rerank HTTP error:', response.status, errorText);
    throw new Error(`OpenRouter rerank failed: ${response.status} ${errorText}`);
  }

  const rerankData = await response.json();
  console.log('OpenRouter rerank raw response:', JSON.stringify(rerankData)); // first 200 chars

  if (!rerankData.results || rerankData.results.length === 0) {
    console.warn('⚠️ Rerank returned empty; falling back to RRF candidates.');
    return candidateChunks.slice(0, finalTopK).map(chunk => safeResult(chunk, 0.5));
  }

  return rerankData.results.map(result => {
    const initialCandidate = candidateChunks[result.index];
    const score = result.relevance_score ?? result.score ?? 0.5;
    return safeResult(initialCandidate, score);
  });
} catch (rerankError) {
  console.error('OpenRouter rerank error:', rerankError);
  // fallback
  return candidateChunks.slice(0, finalTopK).map(chunk => safeResult(chunk, 0.5));
}
  } catch (error) {
    console.error(`❌ Search failure on session ${sessionId}:`, error);
    throw error;
  }
}

async function getOrCacheSessionCollection(sessionId) {
  if (cachedSessionCollections.has(sessionId)) {
    return cachedSessionCollections.get(sessionId);
  }
  try {
    const { collection } = await getSessionCollection(sessionId);
    if (collection) cachedSessionCollections.set(sessionId, collection);
    return collection;
  } catch {
    return null;
  }
}

function calculateCoverage(results, topK = 5) {
  if (!results || results.length === 0) return { confidence: 0, topScore: 0 };
  const scores = results.slice(0, topK).map(r => Math.max(0, r.score));
  const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
  return {
    confidence: Math.round(avgScore * 100),
    topScore: Math.max(...scores)
  };
}

// ── Main retrieval function (modified) ─────────────────────────────────
export async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || 5;

  try {
    const [queryEmbedding, sessionCollection] = await Promise.all([
      embedQuery(query),
      sessionId ? getOrCacheSessionCollection(sessionId) : Promise.resolve(null)
    ]);

    if (!sessionCollection) {
      console.warn(`⚠️  No session collection found for ${sessionId}`);
      return { results: [], coverage: { confidence: 0, topScore: 0, level: 'low', score: 0 }, queryEmbedding };
    }

    // 🚀 Replace the old simple vector search with the hybrid pipeline
    const rawResults = await dynamicSessionSearchPipeline(
      sessionId, sessionCollection, query, queryEmbedding, topK
    );

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

export function shouldShowRefusal(coverage) {
  return coverage.topScore < REFUSAL_THRESHOLD;
}

export { calculateCoverage };
