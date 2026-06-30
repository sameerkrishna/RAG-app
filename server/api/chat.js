import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { retrieveForQuery, generateCitations, formatContextForPrompt } from '../services/retrievalService.js';
import { streamResponse } from '../services/geminiService.js';
import { addTurnWithCitations, getRecentTurns } from '../services/memoryService.js';
import { getOrCreateSession } from '../services/sessionService.js';

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

function expandQuery(query, sessionId) {
  const words = query.trim().split(/\s+/);
  if (words.length > 4) return query;

  const recentTurns = getRecentTurns(sessionId, 4);
  const recentContext = recentTurns
    .filter(t => t.role === 'user')
    .map(t => t.content)
    .join(' ');

  const expansions = [
    'definition', 'overview', 'role', 'responsibilities',
    'examples', 'key concepts', 'how it works', 'purpose'
  ];

  const queryWords = query.toLowerCase().split(/\s+/);
  const contextRelevant = queryWords.some(w =>
    w.length > 3 && recentContext.toLowerCase().includes(w)
  );

  const domainHint = contextRelevant ? `${recentContext.slice(0, 80)}: ` : '';

  return `${domainHint}${query} ${expansions.join(' ')}`;
}

export async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({ error: 'Query is required', code: 'MISSING_QUERY' });
  }

  const sessionId = providedSessionId || uuidv4();
  const answerId = uuidv4();

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

  addTurnWithCitations(sessionId, 'user', query.trim());

  try {
    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    const expandedQuery = expandQuery(query, sessionId);
    const { results, coverage } = await retrieveForQuery(expandedQuery, sessionId, { topK: 5 });

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
      excerpt: cleanExcerpt(r.text.slice(0, 200)),
      score: r.score,
      sourceType: r.source_type
    }));

    sendEvent('status', { stage: 'generating', message: 'Generating response...' });

    const contextText = formatContextForPrompt(results);

    const memoryContext = getRecentTurns(sessionId, 5)
      .map(t => `${t.role === 'user' ? 'User' : 'Assistant'}: ${t.content}`)
      .join('\n\n');

    const prompt = `You are an AI Knowledge Assistant. Your behaviour depends on the type of input:

1. GREETINGS & SMALL TALK (hi, hello, how are you, do you have a life, jokes, general chat):
   - Respond warmly and naturally. Do NOT mention the knowledge base or documents at all.
   - Do NOT add any citations.

2. FACTUAL QUESTIONS WITH CONTEXT (context below is relevant):
   - Answer strictly using the numbered context provided.
   - Cite sources inline as [1] [2] — always separate brackets, never [1, 2].
   - Only cite numbers you actually used.

3. FACTUAL QUESTIONS WITHOUT CONTEXT (context is empty or irrelevant):
   - Politely decline in your own words — vary your phrasing naturally.
   - Do NOT add citations.
   - Do NOT use a fixed template or robotic response.

CONTEXT:
${contextText || '(No relevant documents found in knowledge base)'}

CONVERSATION HISTORY:
${memoryContext}

CURRENT QUESTION: ${query}`;

    let fullResponse = '';

    for await (const chunk of streamResponse(prompt)) {
      if (chunk.type === 'token') {
        fullResponse += chunk.text;
        sendEvent('token', { text: chunk.text });
      } else if (chunk.type === 'error') {
        sendEvent('error', { message: chunk.error, code: 'LLM_ERROR' });
      } else if (chunk.type === 'complete') {
        fullResponse = chunk.response;
      }
    }

    const citedIndices = [...fullResponse.matchAll(/\[(\d+(?:,\s*\d+)*)\]/g)]
      .flatMap(m => m[1].split(',').map(n => parseInt(n.trim())))
      .filter((v, i, a) => a.indexOf(v) === i);

    const isOutOfScope = OUT_OF_SCOPE_PATTERN.test(fullResponse);

    const matchedCitations = citations.filter(c => citedIndices.includes(c.index));

    // ✅ No renumbering — keep original indices to match answer text exactly
    const finalCitations = (isOutOfScope || matchedCitations.length === 0)
      ? []
      : matchedCitations;

    // ✅ Match sources by chunkId — always aligned with finalCitations
    const matchedChunkIds = new Set(matchedCitations.map(c => c.chunkId));

    const finalSources = (isOutOfScope || matchedCitations.length === 0)
      ? []
      : sources.filter(s => matchedChunkIds.has(s.chunkId));

    addTurnWithCitations(sessionId, 'assistant', fullResponse, finalCitations, coverage, answerId);

    sendEvent('complete', {
      answerId,
      response: fullResponse,
      citations: finalCitations,
      coverage,
      sources: finalSources
    });

    res.end();

  } catch (error) {
    console.error('Chat stream error:', error);
    sendEvent('error', { message: error.message || 'An error occurred', code: error.code || 'CHAT_ERROR' });
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

  if (fallback) {
    return res.json({ sources: fallback.citations });
  }

  res.status(404).json({ error: 'Sources not found', code: 'SOURCES_NOT_FOUND' });
}

router.post('/', handleChatStream);
router.get('/sources/:answerId', getSources);

export default router;