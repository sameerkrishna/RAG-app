import { GoogleGenerativeAI } from '@google/generative-ai';
import { WebSearchUnavailableError } from '../utils/errors.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || 'gemini-3.1-flash-lite';

let model = null;

function getModel() {
  if (!model) {
    model = genAI.getGenerativeModel({ model: PRIMARY_MODEL });
  }
  return model;
}

export async function performWebSearch(query) {
  try {
    const model = getModel();

    const result = await model.generateContent({
      contents: [{
        role: 'user',
        parts: [{ text: query }]
      }],
      generationConfig: {
        temperature: 0.7,
        maxOutputTokens: 2048
      },
      tools: [{ googleSearch: {} }]
    });

    const response = result.response;
    const text = response.text();
    const groundingMetadata = response.candidates?.[0]?.groundingMetadata;

    // Extract search queries and sources
    const webSearchQueries = [];
    const webSources = [];

    if (groundingMetadata?.groundingChunks) {
      for (const chunk of groundingMetadata.groundingChunks) {
        if (chunk.web) {
          webSources.push({
            uri: chunk.web.uri,
            title: chunk.web.title
          });
        }
      }
    }

    if (groundingMetadata?.webSearchQueries) {
      webSearchQueries.push(...groundingMetadata.webSearchQueries);
    }

    return {
      text,
      sources: webSources,
      queries: webSearchQueries,
      rawMetadata: groundingMetadata
    };
  } catch (error) {
    console.error('Web search error:', error);
    throw new WebSearchUnavailableError();
  }
}

export async function* streamWebSearch(query) {
  try {
    const model = getModel();

    const result = await model.generateContentStream({
      contents: [{
        role: 'user',
        parts: [{ text: query }]
      }],
      generationConfig: {
        temperature: 0.7,
        maxOutputTokens: 2048
      },
      tools: [{ googleSearch: {} }]
    });

    let fullResponse = '';

    for await (const chunk of result.stream) {
      const text = chunk.text();
      if (text) {
        fullResponse += text;
        yield { type: 'token', text };
      }
    }

    const response = await result.response;
    const groundingMetadata = response?.candidates?.[0]?.groundingMetadata;

    const sources = [];
    if (groundingMetadata?.groundingChunks) {
      for (const item of groundingMetadata.groundingChunks) {
        if (item.web) {
          sources.push({
            uri: item.web.uri,
            title: item.web.title
          });
        }
      }
    }

    yield {
      type: 'complete',
      response: fullResponse,
      sources
    };
  } catch (error) {
    console.error('Web search streaming error:', error);
    yield { type: 'error', error: error.message };
    throw new WebSearchUnavailableError();
  }
}

export function formatWebSearchResponse(result) {
  return {
    answer: result.text,
    sources: result.sources.map(s => ({
      uri: s.uri,
      title: s.title,
      type: 'web'
    })),
    queriesUsed: result.queries,
    metadata: {
      performedAt: new Date().toISOString(),
      searchType: 'google_search_grounding'
    }
  };
}
