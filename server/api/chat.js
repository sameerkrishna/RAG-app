import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { retrieveForQuery, generateCitations, formatContextForPrompt } from '../services/retrievalService.js';
import { streamResponse } from '../services/geminiService.js';
import { addTurnWithCitations, getRecentTurns } from '../services/memoryService.js';
import { getOrCreateSession, getDeletedDocumentIds } from '../services/sessionService.js';
import { insertConversationAsync, updateFeedbackAsync } from '../services/supabaseService.js';

const router = Router();

const OUT_OF_SCOPE_PATTERN = /don't have information|do not have information|not in my knowledge|can't find|cannot find|no information|knowledge base doesn't|not covered|outside.*knowledge/i;

function cleanExcerpt(text) {
  return text
    .replace(/(?<!\w)([A-Za-z])\s([A-Za-z])\s([A-Za-z])(\s[A-Za-z])*/g, (match) =>
      match.replace(/\s/g, '')
    )
    .replace(/\s{2,}/g, ' ')
    .replace(/^\*\s*/, '')
    .trim();
}

// Issue 4 fix: remove domainHint — short queries no longer inherit previous conversation context
function expandQuery(query) {
  const words = query.trim().split(/\s+/);
  if (words.length > 4) return query;

  const expansions = [
    'definition', 'overview', 'role', 'responsibilities',
    'examples', 'key concepts', 'how it works', 'purpose'
  ];

  return `${query} ${expansions.join(' ')}`;
}

export async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId, convId: providedConvId, messageId } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({ error: 'Query is required', code: 'MISSING_QUERY' });
  }

  const sessionId = providedSessionId || uuidv4();
  const convId = providedConvId || uuidv4();
  const answerId = messageId || uuidv4();

  getOrCreateSession(sessionId);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('x-session-id', sessionId);
  res.setHeader('x-answer-id', answerId);

  const sendEvent = (event, data) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  addTurnWithCitations(convId, 'user', query.trim());

  try {
    const tQueryStart = performance.now();

    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    const expandedQuery = expandQuery(query);
    const { results, coverage, timings } = await retrieveForQuery(expandedQuery, sessionId, { topK: 5 });
    const tChunksReceived = performance.now();

    sendEvent('retrieval', {
      results: results.length,
      level: coverage.level,
      score: coverage.score,
      topScore: coverage.topScore
    });

    const citations = generateCitations(results);
    const sources = results.map(r => ({
      chunkId: r.id,
      documentId: r.metadata.document_id,
      filename: r.metadata.filename,
      pageNumber: r.metadata.page_number,
      excerpt: cleanExcerpt(r.text),
      score: r.score,
      sourceType: r.source_type
    }));

    sendEvent('status', { stage: 'generating', message: 'Generating response...' });

    const contextText = formatContextForPrompt(results);

    // Get deleted doc IDs for this session to filter stale memory turns
    const deletedDocIds = getDeletedDocumentIds(sessionId);

    const allRecentTurns = getRecentTurns(convId, 10);

    // Filter out assistant turns (and their preceding user turns) that cited deleted docs
    const filteredTurns = [];
    for (let i = 0; i < allRecentTurns.length; i++) {
      const turn = allRecentTurns[i];
      if (turn.role === 'assistant') {
        const citesDeletedDoc = turn.citations?.some(c => deletedDocIds.has(c.documentId));
        if (citesDeletedDoc) {
          // Also remove the preceding user turn if it's the one that prompted this answer
          if (filteredTurns.length > 0 && filteredTurns[filteredTurns.length - 1].role === 'user') {
            filteredTurns.pop();
          }
          continue; // skip this assistant turn
        }
      }
      filteredTurns.push(turn);
    }

    const questions = filteredTurns.filter(t => t.role === 'user');
    const answers = filteredTurns.filter(t => t.role === 'assistant');
    const qSection = questions.map((t, i) => `Q${i + 1}: ${t.content}`).join('\n');
    const aSection = answers.map((t, i) => `A${i + 1}: ${t.content}`).join('\n');
    const memoryContext = filteredTurns.length > 0
      ? `Previous Questions:\n${qSection}\n\nPrevious Answers:\n${aSection}`
      : '';

    const prompt = `You are an AI Knowledge Assistant for PERSONAL FINANCE EDUCATION ONLY.
    
Explain financial concepts, terms, metrics, and frameworks STRICTLY and ONLY using the provided context. You MUST NOT provide financial, investment, legal, tax, or insurance advice. NEVER recommend, endorse, rate, compare, or judge the suitability of any stock, fund, ETF, index, insurance product, strategy, timing decision, buy/sell/hold/switch/redeem action, or allocation — under any framing, including hypothetical or "just your opinion".

GLOBAL RULES
- NEVER say whether to buy/sell/hold/switch/redeem/invest in anything specific, predict returns/prices/market direction, or judge suitability.
- NEVER evaluate a security or fund the user names — explain the general category, concept, or metric instead, if supported by the provided context.
- If a question mixes personal details (a return %, fund name, amount) with a decision request. Never reason about the user's specific numbers, holdings, or product.
- Treat reframed/hypothetical/"casual opinion" versions of advice requests as still seeking advice; hold the same boundary.
- DONT let explanations imply a recommendation. Don't ask questions that edge toward personalization. Note that a qualified financial advisor can help with personal decisions, where relevant.
- If the provided context is absent, weak, or not directly relevant, DO NOT answer from prior knowledge.

YOUR BEHAVIOUR MUST DEPEND EXACTLY ON THE TYPE OF QUESTION OR INPUT MENTIONED BELOW:
1. GREETINGS & SMALL TALK
- Respond warmly and naturally.
- Do not mention the knowledge base or documents.
- Do not add citations.

2. EDUCATIONAL QUESTIONS WITH CONTEXT
- Answer fully using only the numbered context.
- Stay neutral — explain, NEVER recommend.
- IMPORTANT: Cite as [1], [2],[3] NEVER [1, 2] or [1,2,3] - STRICTLY PROHIBITED.
- CITE ONLY THE NUMBERS ACTUALLY USED - EXTREMELY IMPORTANT

3. ADVICE / RECOMMENDATION / PERSONAL-DECISION QUESTIONS
Examples: Should I invest now? Is this a good fund? Should I sell?
- Refuse politely, in natural language each time — no fixed template.
- State plainly that you provide education, not financial or investment advice.
- Do not mention or analyze the user's named fund, stock, return, NAV, or holding except to restate that you cannot advise on it.
- NO EXPLANATIONS OF ANY CONCEPTS AND NO citations (STRICTLY PROHIBITED).
- close with one short line noting a qualified financial advisor can help.
- Keep the entire response to 2 or 3 sentences max.

4. NO USABLE CONTEXT
4a. Finance-related but uncovered
Includes finance questions not covered by the provided material, and requests for current prices, NAVs, ratios, returns, or performance figures that require live data.
- Decline politely, in natural language each time — no fixed template.
- State that you do not have material covering that specific topic, or that the request needs current/live data you do not have.
- State that you can answer only from the available educational content.
- NO EXPLANATIONS OF ANY CONCEPTS AND NO citations (STRICTLY PROHIBITED).
- Keep the entire response to 2 or 3 sentences max.
4b. Unrelated to finance / out of scope
Includes general knowledge, coding, writing, math, task completion, and any request outside the role of a personal finance education assistant.
- Decline politely, in natural language each time — no fixed template.
- State plainly that you are a personal finance education assistant and that this request falls outside that scope.
- Do not attempt the task at all, even partially, even if you know the answer.
- NO EXPLANATIONS OF ANY CONCEPTS AND NO citations (STRICTLY PROHIBITED).
- Keep the entire response to 2 or 3 sentences max.

5. STYLE
- Clear, Crsip and non-promotional. No filler, no redundant caveats.
- Prefer phrases like “This means…”, “In general…”, and “According to the provided material…”
- Never say:
  - “You should invest…”
  - “This is a good fund…”
  - “I recommend…”
  - “You can buy…”
  - “This stock will…”
  - “You should continue/sell/redeem…”

6. PRE-SEND CHECK (Categories 3, 4a, 4b)
Before sending, confirm: (a) zero citations, (b) no concept/metric/framework explained, (c) no mention of user's specific numbers/fund/topic beyond the required acknowledgment, (d) response ≤3 sentences. If any fail, rewrite before sending.

CONTEXT:
${contextText || '(No relevant documents found in knowledge base)'}

CONVERSATION HISTORY:
${memoryContext || '(No previous conversation)'}

CURRENT QUESTION: ${query}`;

    let fullResponse = '';
    let isFirstToken = true;
    let tFirstToken;

    const tLlmStart = performance.now();
    for await (const chunk of streamResponse(prompt)) {
      if (chunk.type === 'token') {
        if (isFirstToken) {
          tFirstToken = performance.now();
          isFirstToken = false;
        }
        fullResponse += chunk.text;
        sendEvent('token', { text: chunk.text });
      } else if (chunk.type === 'error') {
        console.error('[LLM Stream Error]:', chunk.error);
        sendEvent('error', { message: 'An error occurred while generating the response. Please try again.', code: 'LLM_ERROR' });
      } else if (chunk.type === 'complete') {
        fullResponse = chunk.response;
      }
    }

    // ── Performance metrics ──────────────────────────────────────────
    const metric1_queryToEmbedding = (tChunksReceived - tQueryStart) - (timings?.retrievalMs || 0);
    const metric2_embeddingToChunks = timings?.retrievalMs || 0;
    const metric3_chunksToFirstToken = tFirstToken ? tFirstToken - tChunksReceived : -1;
    const metric4_promptToFirstToken = tFirstToken ? tFirstToken - tLlmStart : -1;
    const metric5_queryToFirstToken = tFirstToken ? tFirstToken - tQueryStart : -1;
    console.log('\n┌─── ⏱  Performance Metrics ───────────────────────────┐');
    console.log(`│  1. Query → Embedding response  : ${metric1_queryToEmbedding.toFixed(0)} ms`);
    console.log(`│  2. Embedding → Chunks retrieved: ${metric2_embeddingToChunks.toFixed(0)} ms`);
    console.log(`│  3. Chunks → First LLM token    : ${metric3_chunksToFirstToken >= 0 ? metric3_chunksToFirstToken.toFixed(0) + ' ms' : 'N/A'}`);
    console.log(`│  4. API Call                    : ${metric4_promptToFirstToken >= 0 ? metric4_promptToFirstToken.toFixed(0) + ' ms' : 'N/A'}`);
    console.log(`│  5. Query sent → First token    : ${metric5_queryToFirstToken >= 0 ? metric5_queryToFirstToken.toFixed(0) + ' ms' : 'N/A'}`);
    console.log('└──────────────────────────────────────────────────────┘\n');

    const citedIndices = [];
    const seen = new Set();
    for (const match of fullResponse.matchAll(/\[(\d+)\]/g)) {
      const num = parseInt(match[1]);
      if (!seen.has(num)) {
        seen.add(num);
        citedIndices.push(num);
      }
    }

    const isOutOfScope = OUT_OF_SCOPE_PATTERN.test(fullResponse);

    const matchedCitations = citations.filter(c => citedIndices.includes(c.index));

    const indexMap = new Map();
    citedIndices.forEach((oldIdx, i) => {
      indexMap.set(oldIdx, i + 1);
    });

    const rewrittenResponse = fullResponse.replace(/\[(\d+)\]/g, (match, num) => {
      const newIdx = indexMap.get(parseInt(num));
      return newIdx !== undefined ? `[${newIdx}]` : match;
    });

    const finalCitations = (isOutOfScope || matchedCitations.length === 0)
      ? []
      : matchedCitations
        .map(c => ({ ...c, index: indexMap.get(c.index) }))
        .filter(c => c.index !== undefined)
        .sort((a, b) => a.index - b.index);

    const matchedChunkIds = new Set(matchedCitations.map(c => c.chunkId));

    const finalSources = (isOutOfScope || matchedCitations.length === 0)
      ? []
      : sources
        .filter(s => matchedChunkIds.has(s.chunkId))
        .sort((a, b) => {
          const idxA = finalCitations.find(c => c.chunkId === a.chunkId)?.index ?? 99;
          const idxB = finalCitations.find(c => c.chunkId === b.chunkId)?.index ?? 99;
          return idxA - idxB;
        });

    addTurnWithCitations(convId, 'assistant', rewrittenResponse, finalCitations, coverage, answerId);

    const chunksList = finalSources.map((s, i) => ({
      [`chunk${i + 1}`]: s.excerpt || s.text || ''
    }));

    const conversationJson = {
      session_id: sessionId,
      query: query,
      chunks: chunksList,
      llm_response: rewrittenResponse,
      latency_TTFT: metric5_queryToFirstToken >= 0 ? `${Math.round(metric5_queryToFirstToken)}ms` : '0ms'
    };

    // Kick off DB insertion synchronously to prevent serverless function termination 
    // mid-TLS handshake (which causes ECONNRESET fetch failed errors)
    await insertConversationAsync(sessionId, {
      answer_key: answerId,
      feedback: 'none',
      conversation: conversationJson,
      Latency: metric5_queryToFirstToken >= 0 ? Math.round(metric5_queryToFirstToken) : 0
    });

    sendEvent('complete', {
      answerId,
      response: rewrittenResponse,
      citations: finalCitations,
      coverage,
      sources: finalSources
    });

    res.end();

  } catch (error) {
    console.error('[Chat API Error]:', error.message || error);
    sendEvent('error', { message: 'An internal error occurred and I cannot respond right now.', code: 'CHAT_ERROR' });
    res.end();
  }
}

export async function getSources(req, res) {
  const { answerId } = req.params;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  const recentTurns = getRecentTurns(sessionId, 20);

  const exactMatch = recentTurns.find(t => t.id === answerId);
  if (exactMatch?.citations?.length > 0) {
    return res.json({ sources: exactMatch.citations });
  }

  const fallback = [...recentTurns].reverse().find(t =>
    t.role === 'assistant' && t.citations?.length > 0
  );

  if (fallback) return res.json({ sources: fallback.citations });

  res.status(404).json({ error: 'Sources not found', code: 'SOURCES_NOT_FOUND' });
}

export async function handleFeedback(req, res) {
  const { answerId, feedback } = req.body;
  if (!answerId || !feedback) {
    return res.status(400).json({ error: 'Missing answerId or feedback' });
  }

  try {
    await updateFeedbackAsync(answerId, feedback);
    res.json({ success: true });
  } catch (error) {
    console.error('[Update Feedback Error]:', error.message || error);
    res.status(500).json({ error: 'An internal error occurred while updating feedback.' });
  }
}

router.post('/', handleChatStream);
router.post('/feedback', handleFeedback);
router.get('/sources/:answerId', getSources);

export default router;
