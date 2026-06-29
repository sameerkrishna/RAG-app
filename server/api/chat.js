import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { retrieveForQuery, generateCitations, shouldShowRefusal, formatContextForPrompt } from '../services/retrievalService.js';
import { streamResponse, getRefusalText } from '../services/geminiService.js';
import { addTurnWithCitations, getRecentTurns } from '../services/memoryService.js';
import { getOrCreateSession } from '../services/sessionService.js';

const router = Router();

export async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({ error: 'Query is required', code: 'MISSING_QUERY' });
  }

  const sessionId = providedSessionId || uuidv4();
  const answerId = uuidv4();

  getOrCreateSession(sessionId);
  addTurnWithCitations(sessionId, 'user', query.trim());

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('x-session-id', sessionId);
  res.setHeader('x-answer-id', answerId);

  const sendEvent = (event, data) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  try {
    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    const { results, coverage } = await retrieveForQuery(query, sessionId, { topK: 5 });

    // coverage is now { confidence: 47, topScore: 0.47 }
    sendEvent('retrieval', {
      results: results.length,
      confidence: coverage.confidence   // e.g. 47  → display as "47% confidence"
    });

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
      addTurnWithCitations(sessionId, 'assistant', refusalText, citations, coverage, answerId);

      sendEvent('complete', {
        answerId,
        response: refusalText,
        citations,
        coverage,
        sources,
        action: 'refusal'
      });

      res.end();
      return;
    }

    sendEvent('status', { stage: 'generating', message: 'Generating response...' });

    // Uses formatContextForPrompt — includes [Seed Document] labels + page numbers
    const contextText = formatContextForPrompt(results);

    const memoryContext = getRecentTurns(sessionId, 5)
      .map(t => `${t.role === 'user' ? 'User' : 'Assistant'}: ${t.content}`)
      .join('\n\n');

    const prompt = `You are an AI Knowledge Assistant. Answer based ONLY on the provided context. \
If the context contains relevant information, use it to answer clearly and cite sources with [1], [2] etc.

CONTEXT:
${contextText}

CONVERSATION HISTORY:
${memoryContext}

CURRENT QUESTION: ${query}

Answer concisely with citations like [1], [2] referring to context numbers.`;

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

    addTurnWithCitations(sessionId, 'assistant', fullResponse, citations, coverage, answerId);

    sendEvent('complete', { answerId, response: fullResponse, citations, coverage, sources });

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