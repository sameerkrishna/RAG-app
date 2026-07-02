import { GoogleGenAI } from '@google/genai';
import { buildPrompt, getRefusalResponse } from './promptService.js';
import { LLMUnavailableError } from '../utils/errors.js';

let genAI = null;

function getGenAI() {
  if (!genAI) {
    genAI = new GoogleGenAI({
      vertexai: true,
      project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT || 'project-d48e2f39-2685-4746-aa0',
      location: 'global'
    });
  }
  return genAI;
}

const PRIMARY_MODEL = process.env.GEMINI_MODEL_PRIMARY || 'gemini-3.1-flash-lite';
const FALLBACK_MODEL = process.env.GEMINI_MODEL_FALLBACK || 'gemini-2.5-flash';
const FIRST_TOKEN_TIMEOUT = parseInt(process.env.LLM_FIRST_TOKEN_TIMEOUT_SECONDS) * 1000 || 12000;
const REQUEST_TIMEOUT = parseInt(process.env.LLM_REQUEST_TIMEOUT_SECONDS) * 1000 || 45000;

function getPrimaryModelName() {
  return PRIMARY_MODEL;
}

function getFallbackModelName() {
  return FALLBACK_MODEL;
}

function getTextFromResponse(result) {
  return result?.text || result?.response?.text?.() || '';
}

function getTextFromChunk(chunk) {
  if (typeof chunk?.text === 'string') return chunk.text;
  if (typeof chunk?.text === 'function') return chunk.text();
  return '';
}

export async function generateResponse(prompt) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

  try {
    const result = await getGenAI().models.generateContent({
      model: getPrimaryModelName(),
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      config: {
        temperature: 0.7,
        topP: 0.95,
        maxOutputTokens: 2048
      }
    }, {
      signal: controller.signal
    });

    clearTimeout(timeoutId);
    return getTextFromResponse(result);
  } catch (primaryError) {
    clearTimeout(timeoutId);
    console.error('Primary model failed:', primaryError.message);

    try {
      const fallbackResult = await getGenAI().models.generateContent({
        model: getFallbackModelName(),
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        config: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      });

      return getTextFromResponse(fallbackResult);
    } catch (fallbackError) {
      console.error('Fallback model also failed:', fallbackError.message);
      throw new LLMUnavailableError();
    }
  }
}

export async function* streamResponse(prompt) {
  let modelName = getPrimaryModelName();
  let retries = 0;
  const maxRetries = 2;

  while (retries < maxRetries) {
    try {
      const controller = new AbortController();

      const result = await getGenAI().models.generateContentStream({
        model: modelName,
        contents: [{ role: 'user', parts: [{ text: prompt }] }],
        config: {
          temperature: 0.7,
          topP: 0.95,
          maxOutputTokens: 2048
        }
      }, {
        signal: controller.signal
      });

      let firstToken = true;
      const firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);

      for await (const chunk of result.stream) {
        if (controller.signal.aborted) {
          clearTimeout(firstTokenTimeout);
          throw new Error('First token timeout — no response from model');
        }

        const text = getTextFromChunk(chunk);
        if (text) {
          if (firstToken) {
            firstToken = false;
            clearTimeout(firstTokenTimeout);
          }
          yield { type: 'token', text };
        }
      }

      clearTimeout(firstTokenTimeout);
      return { success: true };
    } catch (error) {
      retries++;
      console.error(`Model attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        yield { type: 'error', error: error.message };
        throw new LLMUnavailableError();
      }

      modelName = getFallbackModelName();
    }
  }
}

export async function* streamChatResponse(query, retrievedResults, sessionId, memoryService) {
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
  const result = await getGenAI().models.generateContent({
    model: getPrimaryModelName(),
    contents: [{
      role: 'user',
      parts: [{ text: `Based on these web search results, answer the question: "${query}"\n\n${groundingContent}` }]
    }],
    config: {
      temperature: 0.7,
      topP: 0.95,
      maxOutputTokens: 2048,
      tools: [{ googleSearch: {} }]
    }
  });

  const text = getTextFromResponse(result);
  const groundingMetadata = result?.candidates?.[0]?.groundingMetadata;

  return {
    text,
    groundingMetadata,
    groundingChunks: groundingMetadata?.groundingChunks || []
  };
}