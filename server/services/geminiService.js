import { GoogleGenAI } from '@google/genai';
import { LLMUnavailableError } from '../utils/errors.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let genAI = null;

function loadGoogleCredentials() {
  // 1. Try env var first (for serverless where secrets work)
  const credentialsJson = process.env.GOOGLE_CREDENTIALS_JSON;
  if (credentialsJson) {
    try {
      return JSON.parse(credentialsJson);
    } catch (e) {
      console.warn('[gemini] Failed to parse GOOGLE_CREDENTIALS_JSON');
    }
  }

  // 2. Try GOOGLE_APPLICATION_CREDENTIALS file path
  const credsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (credsPath) {
    try {
      const absolutePath = path.isAbsolute(credsPath)
        ? credsPath
        : path.resolve(process.cwd(), credsPath);
      if (fs.existsSync(absolutePath)) {
        return JSON.parse(fs.readFileSync(absolutePath, 'utf-8'));
      }
    } catch (e) {
      console.warn('[gemini] Failed to read GOOGLE_APPLICATION_CREDENTIALS:', e.message);
    }
  }

  // 3. Try common deployed locations
  const possiblePaths = [
    path.resolve(__dirname, '../../google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json'),
    path.resolve(process.cwd(), 'google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json'),
    path.resolve(process.cwd(), 'dist/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json'),
    '/var/task/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json',
    '/tmp/google_credentials/project-d48e2f39-2685-4746-aa0-e80a4893d1bc.json'
  ];

  for (const p of possiblePaths) {
    try {
      if (fs.existsSync(p)) {
        console.log('[gemini] Found credentials at:', p);
        return JSON.parse(fs.readFileSync(p, 'utf-8'));
      }
    } catch (e) {
      // Continue to next path
    }
  }

  return null;
}

function getGenAI() {
  if (!genAI) {
    const project = process.env.GOOGLE_CLOUD_PROJECT || 'project-d48e2f39-2685-4746-aa0';
    const location = 'global';

    const credentials = loadGoogleCredentials();

    if (credentials) {
      console.log('[gemini] Using explicit Google credentials');
      genAI = new GoogleGenAI({
        vertexai: true,
        project,
        location,
        credentials
      });
    } else {
      console.log('[gemini] Using default Google auth');
      genAI = new GoogleGenAI({
        vertexai: true,
        project,
        location
      });
    }
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
