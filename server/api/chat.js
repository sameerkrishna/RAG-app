import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { retrieveForQuery, generateCitations, shouldShowRefusal } from '../services/retrievalService.js';
import { streamResponse, getRefusalText } from '../services/geminiService.js';
import { addTurnWithCitations, getRecentTurns, getLastUserMessage } from '../services/memoryService.js';
import { getOrCreateSession } from '../services/sessionService.js';

const router = Router();

export async function handleChatStream(req, res) {
  const { query, sessionId: providedSessionId } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({
      error: 'Query is required',
      code: 'MISSING_QUERY'
    });
  }

  const sessionId = providedSessionId || uuidv4();
  const answerId = uuidv4();

  getOrCreateSession(sessionId);
  const userTurn = addTurnWithCitations(sessionId, 'user', query.trim());

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('x-session-id', sessionId);
  res.setHeader('x-answer-id', answerId);

  const sendEvent = (event, data) => {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
  // ✅ Force flush through Vite's middleware buffer immediately
  if (typeof res.flush === 'function') res.flush();
};

  try {
    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    const { results, coverage } = await retrieveForQuery(query, sessionId, { topK: 5 });

    sendEvent('retrieval', {
      results: results.length,
      coverage: coverage.level,
      coverageScore: coverage.score
    });

    if (shouldShowRefusal(coverage)) {
      const citations = generateCitations(results);
      addTurnWithCitations(sessionId, 'assistant', getRefusalText(), citations, coverage);
      sendEvent('complete', {
        answerId,
        response: getRefusalText(),
        citations,
        coverage,
        action: 'refusal'
      });
      res.end();
      return;
    }

    sendEvent('status', { stage: 'generating', message: 'Generating response...' });

    const memoryContext = getRecentTurns(sessionId, 5)
      .map(t => `${t.role === 'user' ? 'User' : 'Assistant'}: ${t.content}`)
      .join('\n\n');

    // ✅ FIX: Build prompt based on whether context exists
    let prompt;

    if (results.length > 0) {
      const contextText = results.map((r, i) =>
        `[${i + 1}] ${r.metadata.filename || 'Source'}: ${r.text}`
      ).join('\n\n');

      prompt = `You are a helpful AI Knowledge Assistant. Answer based on the provided context documents.

CONTEXT:
${contextText}

${memoryContext ? `CONVERSATION HISTORY:\n${memoryContext}\n\n` : ''}CURRENT QUESTION: ${query}

Answer concisely and cite sources using [1], [2] etc. referring to the context numbers above.`;

    } else {
       // ✅ No context — greet naturally but don't answer knowledge questions
  prompt = `You are a Knowledge Assistant that answers questions strictly based on uploaded documents.

${memoryContext ? `CONVERSATION HISTORY:\n${memoryContext}\n\n` : ''}CURRENT QUESTION: ${query}

RULES:
- For greetings or small talk (e.g. "hi", "hello", "how are you"), respond briefly and warmly.
- For ANY factual, technical, or knowledge-based question, do NOT attempt to answer it. Instead, tell the user that no documents have been uploaded yet and invite them to upload relevant documents so you can provide a grounded answer.
- Never write code, explain general concepts, or answer from your own training knowledge.`;
}

    let fullResponse = '';

    for await (const chunk of streamResponse(prompt)) {
      if (chunk.type === 'token') {
        fullResponse += chunk.text;
        sendEvent('token', { text: chunk.text });
        // ✅ Add a small delay between tokens for natural streaming feel
    await new Promise(resolve => setTimeout(resolve, 90));
      } else if (chunk.type === 'error') {
        sendEvent('error', { message: chunk.error, code: 'LLM_ERROR' });
      } else if (chunk.type === 'complete') {
        fullResponse = chunk.response;
      }
    }

    const citations = generateCitations(results);
    addTurnWithCitations(sessionId, 'assistant', fullResponse, citations, coverage);

    sendEvent('complete', {
      answerId,
      response: fullResponse,
      citations,
      coverage,
      sources: results.map(r => ({
        chunkId: r.id,
        documentId: r.metadata.document_id,
        filename: r.metadata.filename,
        pageNumber: r.metadata.page_number,
        excerpt: r.text.slice(0, 200),
        sourceType: r.source_type
      }))
    });

    res.end();

  } catch (error) {
    console.error('Chat stream error:', error);
    sendEvent('error', {
      message: error.message || 'An error occurred',
      code: error.code || 'CHAT_ERROR'
    });
    res.end();
  }
}

export async function getSources(req, res) {
  const { answerId } = req.params;
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;

  const recentTurns = getRecentTurns(sessionId, 10);

  for (const turn of recentTurns) {
    if (turn.id === answerId || turn.citations?.length > 0) {
      return res.json({
        sources: turn.citations || []
      });
    }
  }

  res.status(404).json({
    error: 'Sources not found for this answer',
    code: 'SOURCES_NOT_FOUND'
  });
}

router.post('/', handleChatStream);
router.get('/sources/:answerId', getSources);

export default router;