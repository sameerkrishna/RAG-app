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

async function parsePDF(filePath) {
  const buffer = fs.readFileSync(filePath);

  const pages = [];
  await pdf(buffer, {
    pagerender: (pageData) => {
      return pageData.getTextContent().then(tc => {
        const pageText = tc.items.map(i => i.str).join(' ');
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

function getPageNumber(charStart, pageMap) {
  for (const entry of pageMap) {
    if (charStart >= entry.start && charStart < entry.end) return entry.page;
  }
  return pageMap[pageMap.length - 1]?.page || 1;
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
  console.log(`Collection:      ${process.env.CHROMA_GLOBAL_COLLECTION || 'dev'}`);
  console.log(`Embedding model: ${process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001'}`);
  console.log(`Dimensions:      ${process.env.GEMINI_EMBEDDING_DIMENSIONS || 3072}`);

  if (!fs.existsSync(SEED_DIR)) {
    fs.mkdirSync(SEED_DIR, { recursive: true });
    console.log('\nCreated seed_documents/ \u2014 add PDFs and run again.');
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
      console.error(`  \u274c Failed to parse PDF: ${err.message}`);
      docsSkipped++;
      continue;
    }

    console.log(`  - Pages: ${totalPages}`);

    if (!fullText || fullText.trim().length < 50) {
      console.log(`  \u274c No extractable text \u2014 likely a fully scanned PDF. Skipping.`);
      docsSkipped++;
      continue;
    }

    const documentId = uuidv4();

    const rawChunks = chunkText(fullText, { chunkSizeTokens: 1000, overlapTokens: 200 });

    const chunks = rawChunks.map((chunk, idx) => {
      const pageNumber = getPageNumber(chunk.charStart, pageMap);
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
          page_number: pageNumber,
          total_pages: totalPages,
          source_type: 'pdf',
          upload_timestamp: new Date().toISOString(),
          char_start: chunk.charStart,
          char_end: chunk.charEnd,
          token_count: chunk.tokenCount
        }
      };
    });

    console.log(`  - Generated ${chunks.length} chunks (page numbers from boundary map)`);

    if (chunks.length === 0) {
      console.log(`  \u274c No chunks generated. Skipping.`);
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
      console.log(`  \u274c No embeddings generated. Skipping upload.`);
      docsSkipped++;
      continue;
    }

    console.log(`  - Uploading ${embeddings.length} vectors to ChromaDB...`);
    await addVectors(
      collection,
      embeddings.map(e => ({ text: e.text, metadata: e.metadata })),
      embeddings.map(e => e.embedding),
      embeddings.map(e => e.id)
    );

    console.log(`  \u2705 Uploaded ${embeddings.length} vectors for: ${filename}`);
    docsProcessed++;
    totalChunksUploaded += embeddings.length;
  }

  const afterCount = await collection.count();

  console.log('\n=== Seed Database Build Complete ===');
  console.log(`Documents processed:   ${docsProcessed}/${pdfFiles.length}`);
  console.log(`Documents skipped:     ${docsSkipped}`);
  console.log(`Total chunks uploaded: ${totalChunksUploaded}`);
  console.log(`Collection vectors:    ${beforeCount} \u2192 ${afterCount}`);
}

buildSeedDatabase()
  .then(() => { console.log('\nBuild completed successfully.'); process.exit(0); })
  .catch(err => { console.error('\nBuild failed:', err); process.exit(1); });
