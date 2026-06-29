import { Router } from 'express';
import { performWebSearch, streamWebSearch } from '../services/webSearchService.js';

const router = Router();

export async function handleWebSearch(req, res) {
  const { query } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({
      error: 'Query is required',
      code: 'MISSING_QUERY'
    });
  }

  try {
    const result = await performWebSearch(query.trim());

    res.json({
      success: true,
      answer: result.text,
      sources: result.sources,
      queries: result.queries,
      metadata: {
        performedAt: new Date().toISOString(),
        searchType: 'google_search_grounding'
      }
    });
  } catch (error) {
    console.error('Web search error:', error);
    res.status(error.statusCode || 503).json({
      error: error.message || 'Web search unavailable',
      code: error.code || 'WEB_SEARCH_ERROR'
    });
  }
}

export async function handleWebSearchStream(req, res) {
  const { query } = req.body;

  if (!query || typeof query !== 'string' || query.trim().length === 0) {
    return res.status(400).json({
      error: 'Query is required',
      code: 'MISSING_QUERY'
    });
  }

  // Set up SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const sendEvent = (event, data) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  try {
    sendEvent('status', { stage: 'searching', message: 'Searching the web...' });

    let fullResponse = '';
    let sources = [];

    for await (const chunk of streamWebSearch(query.trim())) {
      if (chunk.type === 'token') {
        fullResponse += chunk.text;
        sendEvent('token', { text: chunk.text });
      } else if (chunk.type === 'error') {
        sendEvent('error', { message: chunk.error, code: 'WEB_SEARCH_ERROR' });
      } else if (chunk.type === 'complete') {
        fullResponse = chunk.response;
        sources = chunk.sources || [];
      }
    }

    sendEvent('complete', {
      response: fullResponse,
      sources,
      searchType: 'google_search_grounding'
    });

    res.end();
  } catch (error) {
    console.error('Web search stream error:', error);
    sendEvent('error', {
      message: error.message || 'Web search failed',
      code: error.code || 'WEB_SEARCH_ERROR'
    });
    res.end();
  }
}

router.post('/', handleWebSearch);
router.post('/stream', handleWebSearchStream);

export default router;
