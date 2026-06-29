import { formatMemoryForPrompt } from './memoryService.js';
import { formatContextForPrompt, calculateCoverage } from './retrievalService.js';

const SYSTEM_INSTRUCTION = `You are an AI Knowledge Assistant that answers questions based on indexed documents when available.

RULES:
1. When context is provided, answer based on it and cite sources using [1], [2], etc.
2. For general conversation (greetings, clarifying questions, small talk), respond naturally and helpfully without requiring context.
3. If a factual question is asked but context is insufficient, say so clearly and suggest uploading relevant documents.
4. Be concise but thorough. Use bullet points or numbered lists for complex answers.
5. Maintain conversation continuity but don't repeat information unnecessarily.
6. Format responses in clear, readable markdown.`;

const REFUSAL_MESSAGE = "I don't have enough information in the knowledge base to answer that confidently. Try uploading relevant documents, or ask me a general question.";
export function buildPrompt({ query, context, memoryContext, coverage }) {
  const parts = [];

  // System instruction
  parts.push(SYSTEM_INSTRUCTION);

  // Past conversation if available
  if (memoryContext) {
    parts.push('\n\n--- PREVIOUS CONVERSATION ---\n');
    parts.push(memoryContext);
    parts.push('\n--- END PREVIOUS CONVERSATION ---\n');
  }

  // Retrieved context
  if (context) {
    parts.push('\n\n--- RELEVANT CONTEXT FROM KNOWLEDGE BASE ---\n');
    parts.push(context);
    parts.push('\n--- END CONTEXT ---\n');
  }

  // Current question
  parts.push('\n\n--- CURRENT QUESTION ---\n');
  parts.push(query);
  parts.push('\n\nRemember: Answer based ONLY on the provided context. Use [1], [2], etc. for citations. If the context is insufficient, say so clearly.');

  return parts.join('');
}

export function buildStreamingPrompt(query, retrievedResults, sessionId, memoryService) {
  const memoryContext = formatMemoryForPrompt(sessionId);
  const contextString = formatContextForPrompt(retrievedResults);

  return buildPrompt({
    query,
    context: contextString,
    memoryContext,
    coverage: calculateCoverage(retrievedResults)
  });
}

export function getRefusalResponse() {
  return REFUSAL_MESSAGE;
}

export function getSystemInstruction() {
  return SYSTEM_INSTRUCTION;
}

export function buildWebSearchPrompt(query, groundingMetadata) {
  return `Based on web search results, answer the following question: ${query}

Guidelines:
- Use information from the web search
- Provide sources/URLs where applicable
- Be concise and informative
- If multiple sources agree or contradict, mention that`;
}

export function formatGenerationConfig(customConfig = {}) {
  return {
    temperature: 0.7,
    topP: 0.95,
    topK: 40,
    maxOutputTokens: 2048,
    ...customConfig
  };
}

export function extractSourcesFromResponse(response) {
  // Extract citation patterns like [1], [2], etc.
  const citationPattern = /\[(\d+)\]/g;
  const citations = new Set();
  let match;

  while ((match = citationPattern.exec(response)) !== null) {
    citations.add(parseInt(match[1]));
  }

  return Array.from(citations).sort((a, b) => a - b);
}
