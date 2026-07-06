import { GoogleGenAI } from '@google/genai';
import { LLMUnavailableError } from '../utils/errors.js';

let genAI = null;

function getGenAI() {
  if (!genAI) {
    const project = process.env.GOOGLE_CLOUD_PROJECT || 'project-d48e2f39-2685-4746-aa0';
    const location = 'global';

    // Support credentials from env var (for serverless) or file (for local dev)
    const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;

    if (credentialsJson) {
      try {
        const credentials = JSON.parse(credentialsJson);
        genAI = new GoogleGenAI({
          vertexai: true,
          project,
          location,
          credentials
        });
        return genAI;
      } catch (e) {
        console.warn('Failed to parse GOOGLE_CREDENTIALS_JSON, falling back to default auth');
      }
    }

    genAI = new GoogleGenAI({
      vertexai: true,
      project,
      location
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

function getTextFromChunk(chunk) {
  if (typeof chunk?.text === 'string') return chunk.text;
  if (typeof chunk?.text === 'function') return chunk.text();
  return '';
}

function buildGenerationRequest(model, prompt) {
  return {
    model,
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    config: {
      temperature: 0.7,
      topP: 0.95,
      maxOutputTokens: 2048
    }
  };
}

export async function* streamResponse(prompt) {
  let modelName = getPrimaryModelName();
  let retries = 0;
  const maxRetries = 2;

  while (retries < maxRetries) {
    let firstTokenTimeout = null;
    let requestTimeoutId = null;
    const controller = new AbortController();

    try {
      requestTimeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

      const responseStream = await getGenAI().models.generateContentStream(
        buildGenerationRequest(modelName, prompt),
        { signal: controller.signal }
      );

      if (!responseStream || typeof responseStream[Symbol.asyncIterator] !== 'function') {
        throw new Error(`Streaming unavailable for model ${modelName}`);
      }

      let firstToken = true;
      firstTokenTimeout = setTimeout(() => controller.abort(), FIRST_TOKEN_TIMEOUT);

      for await (const chunk of responseStream) {
        if (controller.signal.aborted) {
          throw new Error('Stream execution aborted by timeout constraint.');
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
      clearTimeout(requestTimeoutId);
      return;

    } catch (error) {
      retries++;

      if (firstTokenTimeout) clearTimeout(firstTokenTimeout);
      if (requestTimeoutId) clearTimeout(requestTimeoutId);

      console.error(`Model attempt ${retries} failed:`, error.message);

      if (retries >= maxRetries) {
        yield { type: 'error', error: error.message };
        throw new LLMUnavailableError();
      }

      modelName = getFallbackModelName();
    }
  }
}
