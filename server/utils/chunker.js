import { createHash } from 'crypto';

const CHARS_PER_TOKEN = 4;
const DEFAULT_CHUNK_SIZE_TOKENS = 1000;
const DEFAULT_OVERLAP_TOKENS = 200;
const MIN_CHUNK_CHARS = 100;

export function estimateTokens(text) {
  if (!text || typeof text !== 'string') return 0;
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

// Fix 5: Clean raw PDF text before chunking
export function cleanText(text) {
  if (!text || typeof text !== 'string') return '';
  return text
    .replace(/\f/g, '\n')                // form feeds → newline
    .replace(/(\s*\n){3,}/g, '\n\n')     // collapse 3+ blank lines to 2
    .replace(/^\s*\d+\s*$/gm, '')        // remove lone page numbers
    .replace(/[ \t]{2,}/g, ' ')          // collapse multiple spaces/tabs
    .trim();
}

// Fix 4: Content-hash based chunk ID for deduplication
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

    // Fix 6: Skip chunks below minimum size
    if (chunkContent.length >= MIN_CHUNK_CHARS) {
      chunks.push({
        text: chunkContent,
        tokenCount: estimateTokens(chunkContent),
        charStart: start,
        charEnd: end,
        chunkIndex: chunkIndex++
      });
    }

    // Fix 1: Correct overlap — only skip overlap if it would cause infinite loop
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

  // Fix 3: Detect scanned/empty PDFs
  if (!text || text.trim().length < 50) {
    console.warn(`⚠️  ${filename} page ${pageNumber}: extracted text too short — may be a scanned page, skipping`);
    return [];
  }

  // Fix 5: Clean text before chunking
  const cleanedText = cleanText(text);

  const textChunks = chunkText(cleanedText, options);

  // Fix 7: Compute total_chunks and attach to all metadata
  const totalChunks = textChunks.length;

  return textChunks.map(chunk => {
    // Fix 4: Use content hash as chunk ID for deduplication
    const chunkId = generateChunkId(chunk.text, filename);

    return {
      text: chunk.text,
      metadata: {
        document_id: documentId,
        filename: filename,
        chunk_id: chunkId,
        chunk_index: chunk.chunkIndex,
        total_chunks: totalChunks,         // Fix 7
        page_number: pageNumber || 1,      // Fix 2: caller must pass correct page
        total_pages: totalPages || null,
        section_title: extractSectionTitle(chunk.text),
        source_type: 'pdf',
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

// Fix 8: mergeChunks removed — was dead code from old broken batching strategy