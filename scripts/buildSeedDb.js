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

// Parse PDF and return full text + per-page texts for boundary map
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

  // Fallback if pagerender yields nothing
  if (pages.length === 0 || pages.every(p => !p.trim())) {
    const full = await pdf(buffer);
    pages.push(full.text);
  }

  const totalPages = pages.length;

  // Clean each page text before joining
  const cleanedPages = pages.map(p => cleanText(p));

  // Build page boundary map: char position → page number
  const pageMap = [];
  let charPos = 0;
  for (let i = 0; i < cleanedPages.length; i++) {
    const pageText = cleanedPages[i];
    pageMap.push({
      page: i + 1,
      start: charPos,
      end: charPos + pageText.length
    });
    charPos += pageText.length + 1; // +1 for the \n separator
  }

  // Join all pages into one full document string
  const fullText = cleanedPages.join('\n');

  return { fullText, pageMap, totalPages };
}

// Look up which page a char position falls on
function getPageNumber(charStart, pageMap) {
  for (const entry of pageMap) {
    if (charStart >= entry.start && charStart < entry.end) {
      return entry.page;
    }
  }
  // Fallback: last page
  return pageMap[pageMap.length - 1]?.page || 1;
}

async function getIndexedFilenames(collection) {
  try {
    const results = await collection.get({ include: ['metadatas'] });
    const filenames = new Set();
    results?.metadatas?.forEach(meta => {
      if (meta?.filename) filenames.add(meta.filename);
    });
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

    // Chunk full document at 1000 tokens / 200 overlap
    const rawChunks = chunkText(fullText, {
      chunkSizeTokens: 1000,
      overlapTokens: 200
    });

    // Build final chunks with accurate page numbers from boundary map
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
          total_chunks: rawChunks.length,   // document-level ✅
          page_number: pageNumber,           // accurate via boundary map ✅
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

    console.log(`  - Uploading ${embeddings.length} vectors to ChromaDB...`);
    await addVectors(
      collection,
      embeddings.map(e => ({ text: e.text, metadata: e.metadata })),
      embeddings.map(e => e.embedding),
      embeddings.map(e => e.id)
    );

    console.log(`  ✅ Uploaded ${embeddings.length} vectors for: ${filename}`);
    docsProcessed++;
    totalChunksUploaded += embeddings.length;
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
