import { GoogleGenerativeAI } from '@google/generative-ai';
import { buildPrompt, getRefusalResponse } from './promptService.js';
import { LLMUnavailableError } from '../utils/errors.js';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || 'gemini-2.0-flash-lite';
const FALLBACK_MODEL = process.env.GEMINI_MODEL_FALLBACK || 'gemini-2.0-flash';
const FIRST_TOKEN_TIMEOUT = parseInt(process.env.LLM_FIRST_TOKEN_TIMEOUT_SECONDS) * 1000 || 12000;
const REQUEST_TIMEOUT = parseInt(process.env.LLM_REQUEST_TIMEOUT_SECONDS) * 1000 || 45000;

let primaryModel = null;
let fallbackModel = null;

function getPrimaryModel() {
  if (!primaryModel) {
    primaryModel = genAI.getGenerativeModel({ model: PRIMARY_MODEL });
  }
  return primaryModel;
}

function getFallbackModel() {
  if (!fallbackModel) {
    fallbackModel = genAI.getGenerativeModel({ model: FALLBACK_MODEL });
  }
  return fallbackModel;
}

async function generateWithModel(model, prompt, signal) {
  const result = await model.generateContent({
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: {
      temperature: 0.7,
      topP: 0.95,
      maxOutputTokens: 2048
    }
  }, { signal });

  return result.response.text();
}

export async function generateResponse(prompt) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

  try {
    const result = await getPrimaryModel().generateContent({
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0.7,
        topP: 0.95,
        maxOutputTokens: 2048
      }
    });

    clearTimeout(timeoutId);
    return result.response.text();
  } catch (primaryError) {
    console.error('Primary model failed:', primaryError.message);

    try {
      const fallbackResult = await getFallbackModel().generateContent({
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      });

      clearTimeout(timeoutId);
      return fallbackResult.response.text();
    } catch (fallbackError) {
      console.error('Fallback model also failed:', fallbackError.message);
      throw new LLMUnavailableError();
    }
  }
}

export async function* streamResponse(prompt) {
  let model = getPrimaryModel();
  let retries = 0;
  const maxRetries = 2;

  while (retries < maxRetries) {
    try {
      const result = await model.generateContentStream({
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      });

      let firstToken = true;
      const controller = new AbortController();
const firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);

      for await (const chunk of result.stream) {
        const text = chunk.text();
        if (text) {
          if (firstToken) {
            firstToken = false;
            clearTimeout(firstTokenTimeout);
          }
          yield { type: 'token', text };
        }
      }

      return { success: true };
    } catch (error) {
      retries++;
      console.error(`Model attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        yield { type: 'error', error: error.message };
        throw new LLMUnavailableError();
      }

      // Switch to fallback model
      model = getFallbackModel();
    }
  }
}

export async function* streamChatResponse(query, retrievedResults, sessionId, memoryService) {
  // Build the prompt
  const memoryContext = memoryService ? memoryService.formatMemoryForPrompt(sessionId) : '';
  const contextList = retrievedResults || [];
  const contextText = contextList.map((r, i) =>
    `[${i + 1}] ${r.metadata.filename || 'Unknown'}: ${r.text}`
  ).join('\n\n');

  const prompt = buildPrompt({
    query,
    context: contextText,
    memoryContext,
    coverage: { level: 'high' }
  });

  // Stream the response
  let fullResponse = '';

  try {
    for await (const chunk of streamResponse(prompt)) {
      if (chunk.type === 'token') {
        fullResponse += chunk.text;
        yield chunk;
      } else if (chunk.type === 'error') {
        yield chunk;
        return;
      }
    }

    yield { type: 'complete', response: fullResponse };
  } catch (error) {
    yield { type: 'error', error: error.message };
  }
}

export function getRefusalText() {
  return getRefusalResponse();
}

export async function generateWebSearchResponse(query, groundingContent) {
  const model = getPrimaryModel();

  const result = await model.generateContent({
    contents: [{
      role: 'user',
      parts: [{ text: `Based on these web search results, answer the question: "${query}"\n\n${groundingContent}` }]
    }],
    generationConfig: {
      temperature: 0.7,
      topP: 0.95,
      maxOutputTokens: 2048
    },
    tools: [{ googleSearch: {} }]
  });

  const response = result.response;
  const text = response.text();
  const groundingMetadata = response.candidates?.[0]?.groundingMetadata;

  return {
    text,
    groundingMetadata,
    groundingChunks: groundingMetadata?.groundingChunks || []
  };
}
