import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import pdf from 'pdf-parse';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

dotenv.config();

import { getGlobalCollection, addVectors } from '../server/services/chromaService.js';
import { chunkPDFContent } from '../server/utils/chunker.js';
import { generateEmbeddings } from '../server/services/embeddingService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SEED_DIR = path.join(__dirname, '..', 'seed_documents');

async function parsePDF(filePath) {
  try {
    const buffer = fs.readFileSync(filePath);
    const data = await pdf(buffer);
    return {
      text: data.text,
      pageCount: data.numpages,
      info: data.info
    };
  } catch (error) {
    console.error(`Failed to parse PDF ${filePath}:`, error);
    throw error;
  }
}

async function processSeedDocument(filePath) {
  const filename = path.basename(filePath);
  console.log(`\nProcessing: ${filename}`);

  const pdfData = await parsePDF(filePath);
  const documentId = uuidv4();

  console.log(`  - Pages: ${pdfData.pageCount}`);
  console.log(`  - Text length: ${pdfData.text.length} characters`);

  const chunks = chunkPDFContent({
    text: pdfData.text,
    filename,
    documentId,
    pageNumber: 1
  });

  console.log(`  - Generated ${chunks.length} chunks`);

  return {
    documentId,
    filename,
    chunks,
    pageCount: pdfData.pageCount
  };
}

async function buildSeedDatabase() {
  console.log('=== Building Seed Database ===\n');
  console.log(`Seed directory: ${SEED_DIR}`);
  console.log(`Collection: ${process.env.CHROMA_GLOBAL_COLLECTION || 'dev'}`);

  if (!fs.existsSync(SEED_DIR)) {
    console.log('\nSeed directory does not exist. Creating...');
    fs.mkdirSync(SEED_DIR, { recursive: true });
    console.log('Created seed_documents/ directory.');
    console.log('\nAdd your PDF documents to this directory and run again.');
    return;
  }

  const pdfFiles = fs.readdirSync(SEED_DIR)
    .filter(f => f.toLowerCase().endsWith('.pdf'))
    .map(f => path.join(SEED_DIR, f));

  if (pdfFiles.length === 0) {
    console.log('\nNo PDF files found in seed_documents/ directory.');
    console.log('Add your seed PDF documents and run again.');
    return;
  }

  console.log(`\nFound ${pdfFiles.length} PDF file(s) to process`);

  try {
    const collection = await getGlobalCollection();
    const beforeCount = await collection.count();
    console.log(`\nConnected to ChromaDB — collection currently has ${beforeCount} vectors`);

    let totalChunksEmbedded = 0;
    let totalDocsProcessed = 0;

    for (const pdfFile of pdfFiles) {
      const { documentId, filename, chunks, pageCount } = await processSeedDocument(pdfFile);

      console.log(`  - Generating embeddings for ${chunks.length} chunks...`);

      // ✅ FIX: generateEmbeddings handles rate limiting internally — no manual wait needed
      const embeddings = await generateEmbeddings(chunks);

      if (!embeddings || embeddings.length === 0) {
        console.warn(`  - No embeddings generated for ${filename}, skipping.`);
        continue;
      }

      console.log(`  - Embedded ${embeddings.length}/${chunks.length} chunks`);

      // ✅ FIX: Actually upload vectors to ChromaDB
      const vectors = embeddings.map(e => ({
        text: e.text,
        metadata: {
          ...e.metadata,
          source_type: 'seed_document',
          filename,
          document_id: documentId,
          page_count: pageCount,
          upload_timestamp: new Date().toISOString()
        }
      }));

      const embeddingValues = embeddings.map(e => e.embedding);
      const ids = embeddings.map(e => e.id || uuidv4());

      await addVectors(collection, vectors, embeddingValues, ids);

      console.log(`  ✅ Uploaded ${embeddings.length} vectors for: ${filename}`);

      totalChunksEmbedded += embeddings.length;
      totalDocsProcessed++;
    }

    const afterCount = await collection.count();

    console.log('\n=== Seed Database Build Complete ===');
    console.log(`Documents processed: ${totalDocsProcessed}/${pdfFiles.length}`);
    console.log(`Total chunks uploaded: ${totalChunksEmbedded}`);
    console.log(`Collection vectors before: ${beforeCount} → after: ${afterCount}`);
    console.log(`Collection: ${process.env.CHROMA_GLOBAL_COLLECTION || 'dev'}`);

  } catch (error) {
    console.error('\nError building seed database:', error);
    throw error;
  }
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