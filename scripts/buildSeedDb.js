import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { v4 as uuidv4 } from 'uuid';
import pdf from 'pdf-parse';
import dotenv from 'dotenv';

dotenv.config();

import { getGlobalCollection, addVectors } from '../server/services/chromaService.js';
import { chunkText, cleanText } from '../server/utils/chunker.js';
import { generateEmbeddings } from '../server/services/embeddingService.js';
import { createHash } from 'crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const SEED_DIR = path.join(__dirname, '..', 'seed_documents');
const BATCH_SIZE = 300;

/**
 * Join pdf.js text-content items into a single string using each item's
 * x-position (transform[4]) and width to decide whether a space belongs
 * between two items, instead of always joining with a single space.
 *
 * This avoids two common artifacts from naive `.join(' ')`:
 *  - words split across adjacent text runs getting a phantom space
 *    inserted in the middle (e.g. "Sav ings")
 *  - adjacent words with no space in the PDF's internal runs getting
 *    glued together (e.g. "the report" -> "thereport")
 *
 * Empty-string items are pdf.js's signal for a line break, which we
 * convert to a newline so paragraph structure isn't lost.
 */
function joinTextItems(items) {
  let out = '';
  let prevItem = null;

  for (const item of items) {
    const str = item.str;
    if (str === undefined) { prevItem = item; continue; }

    if (str === '') {
      // pdf.js emits empty items to signal line breaks
      if (!/\n$/.test(out)) out += '\n';
      prevItem = null;
      continue;
    }

    if (prevItem && prevItem.str) {
      const prevEnd = prevItem.transform[4] + (prevItem.width || 0);
      const curStart = item.transform[4];
      const gap = curStart - prevEnd;
      const fontH = Math.abs(item.transform[3]) || 10;
      const spaceThreshold = fontH * 0.25;

      const alreadySpaced = /\s$/.test(out) || /^\s/.test(str);
      if (!alreadySpaced && gap > spaceThreshold) {
        out += ' ';
      }
      // else: items are touching/overlapping -> same word, no space inserted
    }

    out += str;
    prevItem = item;
  }

  return out;
}

async function parsePDF(filePath) {
  const buffer = fs.readFileSync(filePath);

  const pages = [];
  await pdf(buffer, {
    pagerender: (pageData) => {
      return pageData.getTextContent().then(tc => {
        const pageText = joinTextItems(tc.items);
        pages.push(pageText);
        return pageText;
      });
    }
  });

  if (pages.length === 0 || pages.every(p => !p.trim())) {
    const full = await pdf(buffer);
    pages.push(full.text);
  }

  const totalPages = pages.length;
  const cleanedPages = pages.map(p => cleanText(p));

  const pageMap = [];
  let charPos = 0;
  for (let i = 0; i < cleanedPages.length; i++) {
    const pageText = cleanedPages[i];
    pageMap.push({ page: i + 1, start: charPos, end: charPos + pageText.length });
    charPos += pageText.length + 1;
  }

  const fullText = cleanedPages.join('\n');
  return { fullText, pageMap, totalPages };
}

/**
 * Given a chunk's [charStart, charEnd) range, find which page(s) it
 * overlaps. Returns the majority page (most overlapping chars, used
 * for `page_number` for backward compatibility) plus the true start/end
 * pages so chunks spanning a page break aren't silently mislabeled with
 * just the first page.
 */
function getPageRange(charStart, charEnd, pageMap) {
  let startPage = null;
  let endPage = null;
  let bestPage = null;
  let maxOverlap = -1;

  for (const entry of pageMap) {
    const overlapStart = Math.max(charStart, entry.start);
    const overlapEnd = Math.min(charEnd, entry.end);
    const overlap = overlapEnd - overlapStart;
    if (overlap <= 0) continue;

    if (startPage === null) startPage = entry.page;
    endPage = entry.page;

    if (overlap > maxOverlap) {
      maxOverlap = overlap;
      bestPage = entry.page;
    }
  }

  if (startPage === null) {
    const lastPage = pageMap[pageMap.length - 1]?.page || 1;
    return { page: lastPage, pageStart: lastPage, pageEnd: lastPage };
  }

  return { page: bestPage, pageStart: startPage, pageEnd: endPage };
}

/**
 * Collect all unique filenames already indexed in the global collection.
 * Paginates with BATCH_SIZE=300 so collections larger than Chroma's
 * default get() limit (100) are fully enumerated.
 */
async function getIndexedFilenames(collection) {
  try {
    const filenames = new Set();
    let offset = 0;

    while (true) {
      const batch = await collection.get({
        include: ['metadatas'],
        limit: BATCH_SIZE,
        offset
      });

      if (!batch.metadatas || batch.metadatas.length === 0) break;

      batch.metadatas.forEach(meta => {
        if (meta?.filename) filenames.add(meta.filename);
      });

      console.log(`  [getIndexedFilenames] offset=${offset}, got=${batch.metadatas.length}, unique filenames so far=${filenames.size}`);

      if (batch.metadatas.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    return filenames;
  } catch {
    return new Set();
  }
}

async function buildSeedDatabase() {
  console.log('=== Building Seed Database ===\n');
  console.log(`Seed directory:  ${SEED_DIR}`);
  console.log(`Collection:      ${process.env.CHROMA_GLOBAL_COLLECTION || 'seed_db'}`);
  console.log(`Embedding model: ${process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001'}`);
  console.log(`Dimensions:      ${process.env.GEMINI_EMBEDDING_DIMENSIONS || 3072}`);

  if (!fs.existsSync(SEED_DIR)) {
    fs.mkdirSync(SEED_DIR, { recursive: true });
    console.log('\nCreated seed_documents/ — add PDFs and run again.');
    return;
  }

  const pdfFiles = fs.readdirSync(SEED_DIR)
    .filter(f => f.toLowerCase().endsWith('.pdf'))
    .map(f => path.join(SEED_DIR, f));

  if (pdfFiles.length === 0) {
    console.log('\nNo PDF files found in seed_documents/');
    return;
  }

  console.log(`\nFound ${pdfFiles.length} PDF file(s)`);

  const collection = await getGlobalCollection();
  const beforeCount = await collection.count();
  console.log(`Collection currently has ${beforeCount} vectors`);

  const indexedFilenames = await getIndexedFilenames(collection);
  if (indexedFilenames.size > 0) {
    console.log(`Already indexed: ${[...indexedFilenames].join(', ')}`);
  }

  let docsProcessed = 0;
  let docsSkipped = 0;
  let totalChunksUploaded = 0;

  for (const pdfFile of pdfFiles) {
    const filename = path.basename(pdfFile);

    if (indexedFilenames.has(filename)) {
      console.log(`\nSkipping: ${filename} (already indexed)`);
      docsSkipped++;
      continue;
    }

    console.log(`\nProcessing: ${filename}`);

    let fullText, pageMap, totalPages;
    try {
      ({ fullText, pageMap, totalPages } = await parsePDF(pdfFile));
    } catch (err) {
      console.error(`  ❌ Failed to parse PDF: ${err.message}`);
      docsSkipped++;
      continue;
    }

    console.log(`  - Pages: ${totalPages}`);

    if (!fullText || fullText.trim().length < 50) {
      console.log(`  ❌ No extractable text — likely a fully scanned PDF. Skipping.`);
      docsSkipped++;
      continue;
    }

    const documentId = uuidv4();

    const rawChunks = chunkText(fullText, {
      targetTokens: 600,
      maxTokens: 750,
      overlapTk: 100
    });

    const chunks = rawChunks.map((chunk, idx) => {
      const { page, pageStart, pageEnd } = getPageRange(chunk.charStart, chunk.charEnd, pageMap);
      const chunkId = createHash('md5')
        .update(`${filename}::${chunk.text}`)
        .digest('hex')
        .slice(0, 16);

      return {
        text: chunk.text,
        metadata: {
          document_id: documentId,
          filename,
          chunk_id: chunkId,
          chunk_index: idx,
          total_chunks: rawChunks.length,
          page_number: page,       // majority page — kept for backward compatibility
          page_start: pageStart,   // new: first page this chunk overlaps
          page_end: pageEnd,       // new: last page this chunk overlaps
          total_pages: totalPages,
          source_type: 'global',
          session_id: 'global',
          upload_timestamp: new Date().toISOString(),
          char_start: chunk.charStart,
          char_end: chunk.charEnd,
          token_count: chunk.tokenCount
        }
      };
    });

    console.log(`  - Generated ${chunks.length} chunks (page numbers from boundary map)`);

    if (chunks.length === 0) {
      console.log(`  ❌ No chunks generated. Skipping.`);
      docsSkipped++;
      continue;
    }

    console.log(`  - Embedding ${chunks.length} chunks...`);

    const embeddings = await generateEmbeddings(
      chunks,
      'RETRIEVAL_DOCUMENT',
      ({ current_batch, total_batches }) => {
        console.log(`  - Embedding group ${current_batch}/${total_batches} complete`);
      }
    );

    if (embeddings.length === 0) {
      console.log(`  ❌ No embeddings generated. Skipping upload.`);
      docsSkipped++;
      continue;
    }

    // 🔥 FIX 2: Wrap ChromaDB upload in try/catch so a single failure
    // does not crash the entire batch of 10–20 PDFs.
    console.log(`  - Uploading ${embeddings.length} vectors to ChromaDB...`);
    try {
      await addVectors(
        collection,
        embeddings.map(e => ({ text: e.text, metadata: e.metadata })),
        embeddings.map(e => e.embedding),
        embeddings.map(e => e.id)
      );
      console.log(`  ✅ Uploaded ${embeddings.length} vectors for: ${filename}`);
      docsProcessed++;
      totalChunksUploaded += embeddings.length;
    } catch (uploadErr) {
      console.error(`  ❌ ChromaDB upload failed for ${filename}:`, uploadErr.message);
      docsSkipped++;
      // File remains unprocessed – next run will retry it
    }
  }

  const afterCount = await collection.count();

  console.log('\n=== Seed Database Build Complete ===');
  console.log(`Documents processed:   ${docsProcessed}/${pdfFiles.length}`);
  console.log(`Documents skipped:     ${docsSkipped}`);
  console.log(`Total chunks uploaded: ${totalChunksUploaded}`);
  console.log(`Collection vectors:    ${beforeCount} → ${afterCount}`);
}

buildSeedDatabase()
  .then(() => { console.log('\nBuild completed successfully.'); process.exit(0); })
  .catch(err => { console.error('\nBuild failed:', err); process.exit(1); });
