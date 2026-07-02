import { createHash } from 'crypto';
import { getEncoding } from 'js-tiktoken';

// Instantiate a standard token decoder matching OpenAI cl100k_base models
const encoder = getEncoding('cl100k_base');

const TARGET_CHUNK_TOKENS = 600;   // soft target per chunk
const MAX_CHUNK_TOKENS    = 750;   // hard cap before forced split
const OVERLAP_TOKENS      = 100;   // overlap only on oversized paragraphs
const MIN_CHUNK_CHARS     = 100;

// Matches ALL-CAPS headings, markdown headings, or numbered section headings
const HEADING_RE = /^(?:[A-Z][A-Z\s]{2,60}$|#{1,4}\s.+|(?:\d+\.)+\s.+)/m;

export function estimateTokens(text) {
  if (!text || typeof text !== 'string') return 0;
  return encoder.encode(text).length;
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

/**
 * Structure-aware chunking:
 *  1. Split on blank lines (\n\n) into paragraphs.
 *  2. A line matching HEADING_RE always starts a fresh chunk.
 *  3. Accumulate paragraphs until the soft TARGET is reached, then flush.
 *  4. Paragraphs larger than MAX are split with a sliding window + overlap as fallback.
 */
export function chunkText(text, options = {}) {
  const targetTokens = options.chunkSizeTokens || TARGET_CHUNK_TOKENS;
  const maxTokens    = options.maxChunkTokens  || MAX_CHUNK_TOKENS;
  const overlapTk    = options.overlapTokens   || OVERLAP_TOKENS;

  if (!text || typeof text !== 'string') return [];

  // 1. Split into paragraphs
  const rawParas = text
    .split(/\n{2,}/)
    .map(p => p.trim())
    .filter(p => p.length >= MIN_CHUNK_CHARS);

  const chunks     = [];
  let   buffer     = '';
  let   bufStart   = 0;
  let   chunkIndex = 0;
  let   charCursor = 0;

  const textDecoder = new TextDecoder();

  const flush = (forceText) => {
    const content = (forceText ?? buffer).trim();
    if (content.length >= MIN_CHUNK_CHARS) {
      chunks.push({
        text:       content,
        tokenCount: estimateTokens(content),
        charStart:  bufStart,
        charEnd:    bufStart + content.length,
        chunkIndex: chunkIndex++
      });
    }
    buffer   = '';
    bufStart = charCursor;
  };

  for (const para of rawParas) {
    const isHeading  = HEADING_RE.test(para.split('\n')[0]);
    const paraTokens = estimateTokens(para);

    // 2. Heading always starts a new chunk
    if (isHeading && buffer.length > 0) flush();

    if (paraTokens > maxTokens) {
      // 3. Oversized paragraph -> sliding-window token fallback
      if (buffer.length > 0) flush();

      // Uint8Array required by js-tiktoken encode/decode
      const tokens = new Uint8Array(encoder.encode(para));
      let s = 0;
      while (s < tokens.length) {
        const e     = Math.min(s + targetTokens, tokens.length);
        const slice = textDecoder.decode(encoder.decode(new Uint8Array(tokens.slice(s, e)))).trim();

        if (slice.length >= MIN_CHUNK_CHARS) {
          chunks.push({
            text:       slice,
            tokenCount: e - s,
            charStart:  charCursor,
            charEnd:    charCursor + slice.length,
            chunkIndex: chunkIndex++
          });
        }
        const next = e - overlapTk;
        s = next > s ? next : e;
      }
      charCursor += para.length + 2;
      bufStart    = charCursor;
      continue;
    }

    // 4. Normal paragraph — hard cap lookahead before accumulating
    const currentBufferTokens = estimateTokens(buffer);
    if (currentBufferTokens + paraTokens > maxTokens && buffer.length > 0) {
      flush();
    }

    buffer     = buffer ? buffer + '\n\n' + para : para;
    charCursor += para.length + 2;

    // Soft cap: flush once target is reached
    if (estimateTokens(buffer) >= targetTokens) {
      flush();
    }
  }

  // 5. Flush remainder
  flush();

  return chunks;
}

export function chunkPDFContent(pdfData, options = {}) {
  const { filename, documentId, pageNumber, text, totalPages } = pdfData;

  if (!text || text.trim().length < 50) {
    console.warn(`⚠️  ${filename} page ${pageNumber}: extracted text too short — may be a scanned page, skipping`);
    return [];
  }

  const cleanedText = cleanText(text);
  const textChunks  = chunkText(cleanedText, options);
  const totalChunks = textChunks.length;
  const sourceType  = options.sourceType || 'pdf';

  return textChunks.map(chunk => {
    const chunkId = generateChunkId(chunk.text, filename);
    return {
      text: chunk.text,
      metadata: {
        document_id:      documentId,
        filename,
        chunk_id:         chunkId,
        chunk_index:      chunk.chunkIndex,
        total_chunks:     totalChunks,
        page_number:      pageNumber || 1,
        total_pages:      totalPages || null,
        section_title:    extractSectionTitle(chunk.text),
        source_type:      sourceType,
        upload_timestamp: new Date().toISOString(),
        char_start:       chunk.charStart,
        char_end:         chunk.charEnd,
        token_count:      chunk.tokenCount
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
