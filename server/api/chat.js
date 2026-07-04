import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { retrieveForQuery, generateCitations, formatContextForPrompt } from '../services/retrievalService.js';
import { streamResponse } from '../services/geminiService.js';
import { addTurnWithCitations, getRecentTurns } from '../services/memoryService.js';
import { getOrCreateSession, getDeletedDocumentIds } from '../services/sessionService.js';

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
  const { query, sessionId: providedSessionId, convId: providedConvId } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({ error: 'Query is required', code: 'MISSING_QUERY' });
  }

  const sessionId = providedSessionId || uuidv4();
  const convId    = providedConvId || uuidv4();
  const answerId  = uuidv4();

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
    sendEvent('status', { stage: 'retrieving', message: 'Searching knowledge base...' });

    const expandedQuery = expandQuery(query);
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
    const answers   = filteredTurns.filter(t => t.role === 'assistant');
    const qSection  = questions.map((t, i) => `Q${i + 1}: ${t.content}`).join('\n');
    const aSection  = answers.map((t, i) => `A${i + 1}: ${t.content}`).join('\n');
    const memoryContext = filteredTurns.length > 0
      ? `Previous Questions:\n${qSection}\n\nPrevious Answers:\n${aSection}`
      : '';

    const prompt = `You are an AI Knowledge Assistant. Your behaviour depends on the type of input:

1. GREETINGS & SMALL TALK (hi, hello, how are you, do you have a life, jokes, general chat):
   - Respond warmly and naturally. Do NOT mention the knowledge base or documents at all.
   - Do NOT add any citations.

2. FACTUAL & CONCEPTUAL QUESTIONS WITH CONTEXT (context below contains related terms or definitions):
   - Answer the question by anchoring your core facts in the provided numbered context.
   - You are explicitly permitted to use your own pre-trained AI knowledge to explain, contextualize, or expand on the importance, implications, or real-world utility of the concepts found in the documents.
   - If the user asks about the "importance," "why," or "how" of a metric defined in the context, use your pre-trained knowledge to thoroughly explain that context in a detailed, professional manner.
   - Organize your response using clear markdown structure, including bold text or bullet points for readability.
   - Cite sources inline as [1] [2] — always separate brackets, never.
   - Only cite numbers for the specific factual claims pulled directly from the text.

3. FACTUAL QUESTIONS WITHOUT CONTEXT (context is completely empty or completely irrelevant to the topic):
   - Politely decline in your own words — vary your phrasing naturally.
   - Do NOT add citations.
   - Do NOT use a fixed template or robotic response.

CONTEXT:
${contextText || '(No relevant documents found in knowledge base)'}

CONVERSATION HISTORY:
${memoryContext || '(No previous conversation)'}

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

    sendEvent('complete', {
      answerId,
      response: rewrittenResponse,
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

  if (fallback) return res.json({ sources: fallback.citations });

  res.status(404).json({ error: 'Sources not found', code: 'SOURCES_NOT_FOUND' });
}

router.post('/', handleChatStream);
router.get('/sources/:answerId', getSources);

export default router;
