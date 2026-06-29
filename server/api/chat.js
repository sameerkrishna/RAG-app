import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { retrieveForQuery, generateCitations, shouldShowRefusal, formatContextForPrompt } from '../services/retrievalService.js';
import { streamResponse, getRefusalText } from '../services/geminiService.js';
import { addTurnWithCitations, getRecentTurns } from '../services/memoryService.js';
import { getOrCreateSession } from '../services/sessionService.js';

const router = Router();

// Fix 2: Short-circuit greetings — no retrieval, no citations
const GREETING_PATTERN = /^(hi|hello|hey|thanks|thank you|bye|ok|okay|cool|great|sure|yes|no|got it|nice|awesome|perfect|sounds good|good|noted)[\s!?.]*$/i;

// Fix 4: Detect when LLM says it can't answer from context
const OUT_OF_SCOPE_PATTERN = /does not contain|no information|cannot find|not in the (provided )?context|outside.*knowledge base/i;

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

  // Fix 2: Greeting bypass — respond immediately, skip retrieval entirely
  if (GREETING_PATTERN.test(query.trim())) {
    const greetingResponse = 'Hello! How can I help you today?';
    addTurnWithCitations(sessionId, 'user', query.trim());
    addTurnWithCitations(sessionId, 'assistant', greetingResponse, [], { confidence: 0, topScore: 0 }, answerId);
    sendEvent('complete', {
      answerId,
      response: greetingResponse,
      citations: [],
      coverage: { confidence: 0, topScore: 0 },
      sources: [],
    });
    res.end();
    return;
  }

  addTurnWithCitations(sessionId, 'user', query.trim());

  try {
    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    const { results, coverage } = await retrieveForQuery(query, sessionId, { topK: 5 });

    sendEvent('retrieval', {
      results: results.length,
      confidence: coverage.confidence,
      topScore: coverage.topScore
    });

    // Pre-build sources + citations — used in both refusal and normal paths
    const citations = generateCitations(results);
    const sources = results.map(r => ({
      chunkId: r.id,
      documentId: r.metadata.document_id,
      filename: r.metadata.filename,
      pageNumber: r.metadata.page_number,
      excerpt: r.text.slice(0, 200),
      score: r.score,
      sourceType: r.source_type
    }));

    if (shouldShowRefusal(coverage)) {
      const refusalText = getRefusalText();
      addTurnWithCitations(sessionId, 'assistant', refusalText, [], coverage, answerId);
      sendEvent('complete', {
        answerId,
        response: refusalText,
        citations: [],       // no citations on hard refusal
        coverage,
        sources: [],
        action: 'refusal'
      });
      res.end();
      return;
    }

    sendEvent('status', { stage: 'generating', message: 'Generating response...' });

    const contextText = formatContextForPrompt(results);

    const memoryContext = getRecentTurns(sessionId, 10)
      .map(t => `${t.role === 'user' ? 'User' : 'Assistant'}: ${t.content}`)
      .join('\n\n');

    const prompt = `You are an AI Knowledge Assistant. Answer ONLY using the numbered context below.
If the answer is not in the context, politely say you don't have information on that topic in your knowledge base. Do NOT answer from general knowledge. Do NOT add citations if the context is not relevant.

CONTEXT:
${contextText}

CONVERSATION HISTORY:
${memoryContext}

CURRENT QUESTION: ${query}

Rules:
- Cite sources inline as [1], [2] only for context chunks you actually used
- Never add citation numbers you did not use
- For greetings or small talk, respond naturally without citations
- If context is irrelevant, decline politely with no citations`;

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

    // Fix 3: Only keep citations whose index number appears in the response text
    const citedIndices = [...fullResponse.matchAll(/\[(\d+)\]/g)]
      .map(m => parseInt(m[1]))
      .filter((v, i, a) => a.indexOf(v) === i); // dedupe

    // Fix 4: If LLM went off-context, strip all citations
    const isOutOfScope = OUT_OF_SCOPE_PATTERN.test(fullResponse);

    const finalCitations = (isOutOfScope || citedIndices.length === 0)
      ? []
      : citations.filter(c => citedIndices.includes(c.index));

    const finalSources = (isOutOfScope || citedIndices.length === 0)
      ? []
      : sources.filter((_, i) => citedIndices.includes(i + 1));

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

  const recentTurns = getRecentTurns(sessionId, 10);

  // Strict match by answerId first
  const exactMatch = recentTurns.find(t => t.id === answerId);
  if (exactMatch?.citations?.length > 0) {
    return res.json({ sources: exactMatch.citations });
  }

  // Fallback: most recent assistant turn with citations
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