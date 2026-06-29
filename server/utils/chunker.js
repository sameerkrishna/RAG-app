// Token estimation: ~4 characters per token for English text
const CHARS_PER_TOKEN = 4;
const DEFAULT_CHUNK_SIZE_TOKENS = 1000;
const DEFAULT_OVERLAP_TOKENS = 200;

export function estimateTokens(text) {
  if (!text || typeof text !== 'string') return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

export function chunkText(text, options = {}) {
  const chunkSizeTokens = options.chunkSizeTokens || DEFAULT_CHUNK_SIZE_TOKENS;
  const overlapTokens = options.overlapTokens || DEFAULT_OVERLAP_TOKENS;

  if (!text || typeof text !== 'string') {
    return [];
  }

  const chunkSizeChars = chunkSizeTokens * CHARS_PER_TOKEN;
  const overlapChars = overlapTokens * CHARS_PER_TOKEN;

  const chunks = [];
  let start = 0;
  let chunkIndex = 0;

  while (start < text.length) {
    let end = start + chunkSizeChars;

    // Try to find a good break point
    if (end < text.length) {
      const breakPoints = ['. ', '.\n', '! ', '? ', '\n\n', '\n', ' '];
      let bestBreak = -1;

      // Look for break points in the last 20% of the chunk
      const searchStart = end - Math.floor(chunkSizeChars * 0.2);

      for (const breakpoint of breakPoints) {
        const idx = text.lastIndexOf(breakpoint, end);
        if (idx > searchStart && idx > start) {
          bestBreak = idx + breakpoint.length;
          break;
        }
      }

      if (bestBreak > start) {
        end = bestBreak;
      }
    }

    const chunkText = text.slice(start, end).trim();
    if (chunkText.length > 0) {
      chunks.push({
        text: chunkText,
        tokenCount: estimateTokens(chunkText),
        charStart: start,
        charEnd: end,
        chunkIndex: chunkIndex++
      });
    }

    // Move to next chunk with overlap
    start = end - overlapChars;
    if (start <= chunks[chunks.length - 1]?.charStart) {
      start = end;
    }

    // Safety check to prevent infinite loops
    if (chunkIndex > 10000) {
      console.warn('Chunk limit reached, stopping');
      break;
    }
  }

  return chunks;
}

export function chunkPDFContent(pdfData, options = {}) {
  const { filename, documentId, pageNumber, text } = pdfData;

  const textChunks = chunkText(text, options);

  return textChunks.map(chunk => ({
    text: chunk.text,
    metadata: {
      document_id: documentId,
      filename: filename,
      chunk_id: `${documentId}_${chunk.chunkIndex}`,
      chunk_index: chunk.chunkIndex,
      page_number: pageNumber || 1,
      section_title: extractSectionTitle(chunk.text),
      source_type: 'pdf',
      upload_timestamp: new Date().toISOString(),
      token_start: chunk.charStart,
      token_end: chunk.charEnd,
      token_count: chunk.tokenCount
    }
  }));
}

function extractSectionTitle(text) {
  // Try to extract a potential section title from the beginning of the chunk
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length > 0) {
    const firstLine = lines[0].trim();
    if (firstLine.length < 100 && !firstLine.endsWith('.')) {
      return firstLine.slice(0, 50);
    }
  }
  return null;
}

export function mergeChunks(chunks, maxTokens = 7000) {
  // Merge small chunks to reduce API calls
  const merged = [];
  let current = { texts: [], totalTokens: 0, metadata: [] };

  for (const chunk of chunks) {
    if (current.totalTokens + chunk.tokenCount <= maxTokens) {
      current.texts.push(chunk.text);
      current.metadata.push(chunk.metadata);
      current.totalTokens += chunk.tokenCount;
    } else {
      if (current.texts.length > 0) {
        merged.push({ texts: current.texts, metadata: current.metadata });
      }
      current = { texts: [chunk.text], metadata: [chunk.metadata], totalTokens: chunk.tokenCount };
    }
  }

  if (current.texts.length > 0) {
    merged.push({ texts: current.texts, metadata: current.metadata });
  }

  return merged;
}
