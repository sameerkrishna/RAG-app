import { createHash } from 'crypto';

const CHARS_PER_TOKEN = 4;
const DEFAULT_CHUNK_SIZE_TOKENS = 1000;
const DEFAULT_OVERLAP_TOKENS = 200;
const MIN_CHUNK_CHARS = 100;

export function estimateTokens(text) {
  if (!text || typeof text !== 'string') return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

export function cleanText(text) {
  if (!text || typeof text !== 'string') return '';
  return text
    .replace(/\f/g, '\n')
    .replace(/(\s*\n){3,}/g, '\n\n')
    .replace(/^\s*\d+\s*$/gm, '')
    .replace(/[ \t]{2,}/g, ' ')
    .trim();
}

function generateChunkId(text, filename) {
  return createHash('md5')
    .update(`${filename}::${text}`)
    .digest('hex')
    .slice(0, 16);
}

export function chunkText(text, options = {}) {
  const chunkSizeTokens = options.chunkSizeTokens || DEFAULT_CHUNK_SIZE_TOKENS;
  const overlapTokens = options.overlapTokens || DEFAULT_OVERLAP_TOKENS;

  if (!text || typeof text !== 'string') return [];

  const chunkSizeChars = chunkSizeTokens * CHARS_PER_TOKEN;
  const overlapChars = overlapTokens * CHARS_PER_TOKEN;

  const chunks = [];
  let start = 0;
  let chunkIndex = 0;

  while (start < text.length) {
    let end = start + chunkSizeChars;

    if (end < text.length) {
      const breakPoints = ['. ', '.\n', '! ', '? ', '\n\n', '\n', ' '];
      const searchStart = end - Math.floor(chunkSizeChars * 0.2);

      for (const breakpoint of breakPoints) {
        const idx = text.lastIndexOf(breakpoint, end);
        if (idx > searchStart && idx > start) {
          end = idx + breakpoint.length;
          break;
        }
      }
    }

    end = Math.min(end, text.length);
    const chunkContent = text.slice(start, end).trim();

    if (chunkContent.length >= MIN_CHUNK_CHARS) {
      chunks.push({
        text: chunkContent,
        tokenCount: estimateTokens(chunkContent),
        charStart: start,
        charEnd: end,
        chunkIndex: chunkIndex++
      });
    }

    const nextStart = end - overlapChars;
    start = nextStart > start ? nextStart : end;

    if (chunkIndex > 10000) {
      console.warn('Chunk limit reached, stopping');
      break;
    }
  }

  return chunks;
}

export function chunkPDFContent(pdfData, options = {}) {
  const { filename, documentId, pageNumber, text, totalPages } = pdfData;

  if (!text || text.trim().length < 50) {
    console.warn(`⚠️  ${filename} page ${pageNumber}: extracted text too short — may be a scanned page, skipping`);
    return [];
  }

  const cleanedText = cleanText(text);
  const textChunks = chunkText(cleanedText, options);
  const totalChunks = textChunks.length;

  // FIX 4: use sourceType from options, fall back to 'pdf'
  const sourceType = options.sourceType || 'pdf';

  return textChunks.map(chunk => {
    const chunkId = generateChunkId(chunk.text, filename);

    return {
      text: chunk.text,
      metadata: {
        document_id: documentId,
        filename: filename,
        chunk_id: chunkId,
        chunk_index: chunk.chunkIndex,
        total_chunks: totalChunks,
        page_number: pageNumber || 1,
        total_pages: totalPages || null,
        section_title: extractSectionTitle(chunk.text),
        source_type: sourceType,            // FIX 4
        upload_timestamp: new Date().toISOString(),
        char_start: chunk.charStart,
        char_end: chunk.charEnd,
        token_count: chunk.tokenCount
      }
    };
  });
}

function extractSectionTitle(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length > 0) {
    const firstLine = lines[0].trim();
    if (firstLine.length < 100 && !firstLine.endsWith('.')) {
      return firstLine.slice(0, 50);
    }
  }
  return null;
}