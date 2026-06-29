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

  // Ensure session exists
  getOrCreateSession(sessionId);

  // Store user question in memory
  const userTurn = addTurnWithCitations(sessionId, 'user', query.trim());

  // Set up SSE
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
    // Send initial status
    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    // Retrieve relevant context
    const { results, coverage } = await retrieveForQuery(query, sessionId, { topK: 5 });

    // Send retrieval results
    sendEvent('retrieval', {
      results: results.length,
      coverage: coverage.level,
      coverageScore: coverage.score
    });

    // Check if we should refuse
    if (shouldShowRefusal(coverage)) {
      sendEvent('status', { stage: 'refusal', message: getRefusalText() });

      // Generate citations anyway for the refusal
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

    // Build prompt with context
    sendEvent('status', { stage: 'generating', message: 'Generating response...' });

    const contextText = results.map((r, i) =>
      `[${i + 1}] ${r.metadata.filename || 'Source'}: ${r.text}`
    ).join('\n\n');

    const memoryContext = getRecentTurns(sessionId, 5)
      .map(t => `${t.role === 'user' ? 'User' : 'Assistant'}: ${t.content}`)
      .join('\n\n');

    const prompt = `You are an AI Knowledge Assistant. Answer based ONLY on the provided context.

CONTEXT:
${contextText}

CONVERSATION HISTORY:
${memoryContext}

CURRENT QUESTION: ${query}

Answer concisely with citations like [1], [2] referring to context numbers.`;

    let fullResponse = '';

    // Stream response
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

    // Generate citations
    const citations = generateCitations(results);

    // Store assistant response in memory
    addTurnWithCitations(sessionId, 'assistant', fullResponse, citations, coverage);

    // Send completion
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

  // Get from memory
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
