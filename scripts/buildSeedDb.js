import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import pdf from 'pdf-parse';
import dotenv from 'dotenv';

dotenv.config();

import { getGlobalCollection, addVectors, getDocumentCount } from '../server/services/chromaService.js';
import { chunkPDFContent } from '../server/utils/chunker.js';
import { generateEmbeddings } from '../server/services/embeddingService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const SEED_DIR = path.join(__dirname, '..', 'seed_documents');

// Fix 2: Parse PDF per-page so page_number metadata is accurate
async function parsePDF(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);

    // First pass: get full doc metadata
    const fullData = await pdf(buffer);
    const totalPages = fullData.numpages;

    // Second pass: extract text per page using pagerender
    const pages = [];
    await pdf(buffer, {
      pagerender: (pageData) => {
        return pageData.getTextContent().then(textContent => {
          const pageText = textContent.items.map(item => item.str).join(' ');
          pages.push(pageText);
          return pageText;
        });
      }
    });

    // Fallback: if per-page extraction yields nothing, use full text as page 1
    if (pages.length === 0 || pages.every(p => !p.trim())) {
      pages.push(fullData.text);
    }

    return { pages, totalPages, info: fullData.info };
  } catch (error) {
    console.error(`Failed to parse PDF ${filePath}:`, error.message);
    throw error;
  }
}

// Fix 4: Check if document already indexed by filename to skip re-runs
async function getIndexedFilenames(collection) {
  try {
    const { ChromaClient } = await import('chromadb');
    const results = await collection.get({ include: ['metadatas'] });
    const filenames = new Set();
    if (results?.metadatas) {
      results.metadatas.forEach(meta => {
        if (meta?.filename) filenames.add(meta.filename);
      });
    }
    return filenames;
  } catch {
    return new Set();
  }
}

async function processSeedDocument(filePath, documentId, indexedFilenames) {
  const filename = path.basename(filePath);

  // Fix 4: Skip already-indexed documents
  if (indexedFilenames.has(filename)) {
    console.log(`\nSkipping: ${filename} (already indexed)`);
    return null;
  }

  console.log(`\nProcessing: ${filename}`);
  const { pages, totalPages, info } = await parsePDF(filePath);

  console.log(`  - Pages: ${totalPages}`);

  // Fix 2+3: Chunk per page, detect scanned pages
  const allChunks = [];
  let skippedPages = 0;

  for (let pageIdx = 0; pageIdx < pages.length; pageIdx++) {
    const pageNumber = pageIdx + 1;
    const pageText = pages[pageIdx];

    // Fix 3: Scanned page detection happens inside chunkPDFContent
    // but we also track skipped pages here for reporting
    const pageChunks = chunkPDFContent({
      text: pageText,
      filename,
      documentId,
      pageNumber,      // Fix 2: correct per-page number
      totalPages
    });

    if (pageChunks.length === 0) {
      skippedPages++;
    } else {
      allChunks.push(...pageChunks);
    }
  }

  if (skippedPages > 0) {
    console.log(`  ⚠️  Skipped ${skippedPages}/${totalPages} pages (scanned or empty)`);
  }

  // Fix 3: If ALL pages are scanned/empty, skip the document entirely
  if (allChunks.length === 0) {
    console.log(`  ❌ No extractable text found — likely a fully scanned PDF. Skipping.`);
    return null;
  }

  console.log(`  - Generated ${allChunks.length} chunks across ${totalPages - skippedPages} page(s)`);

  return { filename, documentId, chunks: allChunks, pageCount: totalPages };
}

async function buildSeedDatabase() {
  console.log('=== Building Seed Database ===\n');
  console.log(`Seed directory: ${SEED_DIR}`);
  console.log(`Collection:     ${process.env.CHROMA_GLOBAL_COLLECTION || 'dev'}`);
  console.log(`Embedding model: ${process.env.GEMINI_EMBEDDING_MODEL || 'gemini-embedding-001'}`);
  console.log(`Dimensions:     ${process.env.GEMINI_EMBEDDING_DIMENSIONS || 3072}`);

  if (!fs.existsSync(SEED_DIR)) {
    console.log('\nSeed directory does not exist. Creating...');
    fs.mkdirSync(SEED_DIR, { recursive: true });
    console.log('Created seed_documents/ — add PDFs and run again.');
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

  // Fix 4: Load already-indexed filenames to skip re-indexing
  const indexedFilenames = await getIndexedFilenames(collection);
  if (indexedFilenames.size > 0) {
    console.log(`Already indexed: ${[...indexedFilenames].join(', ')}`);
  }

  let docsProcessed = 0;
  let docsSkipped = 0;
  let totalChunksUploaded = 0;

  for (const pdfFile of pdfFiles) {
    const { v4: uuidv4 } = await import('uuid');
    const documentId = uuidv4();

    const result = await processSeedDocument(pdfFile, documentId, indexedFilenames);

    if (!result) {
      docsSkipped++;
      continue;
    }

    const { filename, chunks } = result;

    console.log(`  - Embedding ${chunks.length} chunks...`);

    // Delegate all batching + rate limiting to generateEmbeddings
    // (7 chunks per batchEmbedContents call, 4 parallel calls, 61s wait between groups)
    const embeddings = await generateEmbeddings(chunks, 'RETRIEVAL_DOCUMENT', ({ current_batch, total_batches }) => {
      console.log(`  - Embedding group ${current_batch}/${total_batches} complete`);
    });

    if (embeddings.length === 0) {
      console.log(`  ❌ No embeddings generated for ${filename}, skipping upload`);
      docsSkipped++;
      continue;
    }

    // Upload to Chroma
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
  console.log(`Documents processed:  ${docsProcessed}/${pdfFiles.length}`);
  console.log(`Documents skipped:    ${docsSkipped}`);
  console.log(`Total chunks uploaded: ${totalChunksUploaded}`);
  console.log(`Collection vectors:   ${beforeCount} → ${afterCount}`);
}

buildSeedDatabase()
  .then(() => {
    console.log('\nBuild completed successfully.');
    process.exit(0);
  })
  .catch(err => {
    console.error('\nBuild failed:', err);
    process.exit(1);
  });