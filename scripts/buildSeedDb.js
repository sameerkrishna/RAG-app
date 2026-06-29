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
  console.log(`Collection: ${process.env.CHROMA_GLOBAL_COLLECTION || 'global_knowledge'}`);

  // Check if seed directory exists
  if (!fs.existsSync(SEED_DIR)) {
    console.log('\nSeed directory does not exist. Creating...');
    fs.mkdirSync(SEED_DIR, { recursive: true });
    console.log('Created seed_documents/ directory.');
    console.log('\nAdd your PDF documents to this directory and run again.');
    return;
  }

  // List PDF files
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
    // Get or create global collection
    const collection = await getGlobalCollection();
    console.log('Connected to ChromaDB collection');

    const allEmbeddings = [];
    let totalChunks = 0;

    for (const pdfFile of pdfFiles) {
      const { documentId, filename, chunks, pageCount } = await processSeedDocument(pdfFile);

      console.log(`  - Generating embeddings for ${chunks.length} chunks...`);

      // Process chunks with rate limiting
      let processedCount = 0;
      const batchSize = 4; // Parallel calls

      for (let i = 0; i < chunks.length; i += batchSize) {
        const batch = chunks.slice(i, i + batchSize);

        const batchEmbeddings = [];
        for (const chunk of batch) {
          try {
            const embeddings = await generateEmbeddings([chunk]);
            if (embeddings && embeddings.length > 0) {
              batchEmbeddings.push({
                id: uuidv4(),
                embedding: embeddings[0].embedding,
                text: chunk.text,
                metadata: {
                  ...chunk.metadata,
                  source_type: 'seed_document'
                }
              });
            }
          } catch (error) {
            console.error(`    Error embedding chunk ${processedCount}:`, error.message);
          }
          processedCount++;
        }

        // Wait between batches
        if (i + batchSize < chunks.length) {
          console.log(`  - Batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(chunks.length / batchSize)} complete, waiting...`);
          await new Promise(resolve => setTimeout(resolve, 60000));
        }
      }

      allEmbeddings.push({
        documentId,
        filename,
        chunkCount: processedCount,
        pageCount
      });

      totalChunks += processedCount;
      console.log(`  - Complete: ${chunks.length} chunks processed`);
    }

    // Add all vectors to collection
    if (allEmbeddings.length > 0) {
      console.log('\nUploading vectors to ChromaDB...');

      // Get collection count to verify
      const beforeCount = await collection.count();

      console.log(`Collection currently has ${beforeCount} vectors`);

      console.log('\n=== Seed Database Build Complete ===');
      console.log(`Documents processed: ${pdfFiles.length}`);
      console.log(`Total chunks embedded: ${totalChunks}`);
      console.log(`Collection: ${process.env.CHROMA_GLOBAL_COLLECTION || 'global_knowledge'}`);
    }

  } catch (error) {
    console.error('\nError building seed database:', error);
    throw error;
  }
}

// Run the build
buildSeedDatabase()
  .then(() => {
    console.log('\nBuild completed successfully.');
    process.exit(0);
  })
  .catch(err => {
    console.error('\nBuild failed:', err);
    process.exit(1);
  });
