const memoryMap = new Map();
const DEFAULT_MEMORY_WINDOW = parseInt(process.env.MEMORY_WINDOW_TURNS) || 10;

export function initializeMemory(sessionId) {
  if (!memoryMap.has(sessionId)) {
    memoryMap.set(sessionId, {
      turns: [],
      createdAt: new Date()
    });
  }
  return memoryMap.get(sessionId);
}

export function addTurn(sessionId, role, content, metadata = {}) {
  const memory = memoryMap.get(sessionId) || initializeMemory(sessionId);
  const maxTurns = parseInt(process.env.MEMORY_WINDOW_TURNS) || DEFAULT_MEMORY_WINDOW;

  const turn = {
    id: `turn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    role,
    content,
    timestamp: new Date(),
    ...metadata
  };

  memory.turns.push(turn);

  // Keep only the last N turns
  if (memory.turns.length > maxTurns) {
    memory.turns = memory.turns.slice(-maxTurns);
  }

  return turn;
}

export function getMemory(sessionId) {
  return memoryMap.get(sessionId) || initializeMemory(sessionId);
}

export function getRecentTurns(sessionId, maxTurns = null) {
  const memory = getMemory(sessionId);
  const limit = maxTurns || parseInt(process.env.MEMORY_WINDOW_TURNS) || DEFAULT_MEMORY_WINDOW;

  return memory.turns.slice(-limit);
}

export function getConversationContext(sessionId) {
  const turns = getRecentTurns(sessionId);
  return turns.map(t => ({
    role: t.role,
    content: t.content
  }));
}

export function formatMemoryForPrompt(sessionId) {
  const turns = getRecentTurns(sessionId);
  if (turns.length === 0) {
    return '';
  }

  const formatted = turns.map(t => {
    const prefix = t.role === 'user' ? 'User:' : 'Assistant:';
    return `${prefix} ${t.content}`;
  }).join('\n\n');

  return formatted;
}

export function clearMemory(sessionId) {
  memoryMap.delete(sessionId);
}

export function getMemoryStats(sessionId) {
  const memory = getMemory(sessionId);
  return {
    turnCount: memory.turns.length,
    createdAt: memory.createdAt,
    lastTurnAt: memory.turns.length > 0 ? memory.turns[memory.turns.length - 1].timestamp : null
  };
}

// FIXED — answerId stored as turn.id, overriding the auto-generated one
export function addTurnWithCitations(sessionId, role, content, citations = [], coverage = null, answerId = null) {
  return addTurn(sessionId, role, content, {
    ...(answerId && { id: answerId }),  // override turn id so getSources can match it
    citations,
    coverage,
    hasCitations: citations.length > 0
  });
}

export function getLastUserMessage(sessionId) {
  const memory = getMemory(sessionId);
  for (let i = memory.turns.length - 1; i >= 0; i--) {
    if (memory.turns[i].role === 'user') {
      return memory.turns[i];
    }
  }
  return null;
}

export function getLastAssistantMessage(sessionId) {
  const memory = getMemory(sessionId);
  for (let i = memory.turns.length - 1; i >= 0; i--) {
    if (memory.turns[i].role === 'assistant') {
      return memory.turns[i];
    }
  }
  return null;
}
