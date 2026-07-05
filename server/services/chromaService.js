import { CloudClient, Schema, SparseVectorIndexConfig, DOCUMENT_KEY, Search, Knn, Rrf } from 'chromadb';
import { ChromaBm25EmbeddingFunction } from '@chroma-core/chroma-bm25';
import { v4 as uuidv4 } from 'uuid';

const BATCH_SIZE = 300;

// ── Shared schema: dense embeddings (managed externally) + BM25 sparse index ──
const bm25EmbeddingFunction = new ChromaBm25EmbeddingFunction();
const collectionSchema = new Schema().createIndex(
  new SparseVectorIndexConfig({
    embeddingFunction: bm25EmbeddingFunction,
    sourceKey: DOCUMENT_KEY,
    bm25: true
  }),
  'sparse_bm25'
);

let cloudClient = null;
let globalCollection = null;
const sessionCollections = new Map();

function getCloudClient() {
  if (!cloudClient) {
    const apiKey = process.env.CHROMA_API_KEY;
    const tenant = process.env.CHROMA_TENANT || 'default_tenant';
    const database = process.env.CHROMA_DATABASE || 'default_database';
    const host = process.env.CHROMA_HOST || undefined;

    console.log("---- CHROMA CONNECTIVITY DEBUG ----");
    console.log("Host:      ", host || "api.trychroma.com (default)");
    console.log("Tenant:    ", tenant);
    console.log("DB Name:   ", database);
    console.log("API Key:   ", apiKey ? "LOADED (VALID)" : "MISSING (UNDEFINED)");
    console.log("-----------------------------------");

    if (!apiKey) {
      throw new Error(
        "CRITICAL ERROR: CHROMA_API_KEY is undefined. " +
        "Ensure your environment variables are correctly loaded before executing this file."
      );
    }

    const clientOptions = { apiKey, tenant, database };
    if (host) clientOptions.host = host;
    cloudClient = new CloudClient(clientOptions);
  }
  return cloudClient;
}

export async function getGlobalCollection() {
  if (!globalCollection) {
    const client = getCloudClient();
    const collectionName = process.env.CHROMA_GLOBAL_COLLECTION || 'seed_db';
    try {
      globalCollection = await client.getOrCreateCollection({
        name: collectionName,
        schema: collectionSchema,
        metadata: {
          description: 'Permanent seed documents for RAG',
          type: 'global_knowledge'
        },
        embeddingFunction: null
      });
      console.log(`\u2705 Global collection ready: ${collectionName}`);
    } catch (error) {
      console.error('Failed to connect to global collection:', error);
      throw error;
    }
  }
  return globalCollection;
}

/**
 * Returns { collection, isNew }.
 * isNew = true  → freshly created, needs seeding from global.
 * isNew = false → already existed on Chroma Cloud, respect its current state.
 */
export async function getSessionCollection(sessionId) {
  if (sessionCollections.has(sessionId)) {
    return { collection: sessionCollections.get(sessionId), isNew: false };
  }

  const client = getCloudClient();
  const collectionName = `session_${sessionId}`;

  let collection;
  let isNew;

  try {
    collection = await client.getCollection({
      name: collectionName,
      embeddingFunction: null
    });
    isNew = false;
    console.log(`\u267b\ufe0f  Session collection exists, reusing: ${collectionName}`);
  } catch {
    collection = await client.createCollection({
      name: collectionName,
      schema: collectionSchema,
      metadata: {
        type: 'session_upload',
        session_id: sessionId,
        created: new Date().toISOString()
      },
      embeddingFunction: null
    });
    isNew = true;
    console.log(`\u2705 Session collection created: ${collectionName}`);
  }

  sessionCollections.set(sessionId, collection);
  return { collection, isNew };
}

export async function deleteSessionCollection(sessionId) {
  const collectionName = `session_${sessionId}`;
  try {
    const client = getCloudClient();
    await client.deleteCollection({ name: collectionName });
    sessionCollections.delete(sessionId);
    console.log(`\u2705 Session collection deleted: ${collectionName}`);
    return true;
  } catch (error) {
    console.error(`Failed to delete session collection ${collectionName}:`, error);
    return false;
  }
}

/**
 * Add vectors in batches of BATCH_SIZE to avoid Chroma payload limits.
 */
export async function addVectors(collection, vectors, embeddings, ids) {
  try {
    for (let i = 0; i < ids.length; i += BATCH_SIZE) {
      const batchIds = ids.slice(i, i + BATCH_SIZE);
      const batchEmbeddings = embeddings.slice(i, i + BATCH_SIZE);
      const batchDocuments = vectors.slice(i, i + BATCH_SIZE).map(v => v.text);
      const batchMetadatas = vectors.slice(i, i + BATCH_SIZE).map(v => v.metadata);

      await collection.add({
        ids: batchIds,
        embeddings: batchEmbeddings,
        documents: batchDocuments,
        metadatas: batchMetadatas
      });
      console.log(`  [addVectors] batch ${Math.floor(i / BATCH_SIZE) + 1}: added ${batchIds.length} vectors`);
    }
    return true;
  } catch (error) {
    console.error('Failed to add vectors:', error);
    throw error;
  }
}

export async function queryCollection(collection, queryEmbedding, topK = 5) {
  try {
    const results = await collection.query({
      queryEmbeddings: [queryEmbedding],
      nResults: topK,
      include: ['documents', 'metadatas', 'distances']
    });
    
  
    if (!results.ids || results.ids.length === 0 || results.ids[0].length === 0) {
      return [];
    }

    return results.ids[0].map((id, idx) => ({
      id,
      text: results.documents[0][idx],
      metadata: results.metadatas[0][idx],
      distance: results.distances[0][idx],
      score: 1 - results.distances[0][idx]
    }));
  } catch (error) {
    console.error('Failed to query collection:', error);
    throw error;
  }
}

/**
 * Hybrid search using Chroma Cloud Search API with RRF (dense + sparse BM25).
 * Returns results in the same shape as queryCollection() for backward compatibility.
 */
export async function hybridQueryCollection(collection, queryText, queryEmbedding, topK = 5) {
  try {
    const search = new Search()
      .rank(Rrf({
        ranks: [
          Knn({ query: queryEmbedding, returnRank: true, limit: 100 }),
          Knn({ query: queryText, key: 'sparse_bm25', returnRank: true, limit: 100 })
        ],
        weights: [0.7, 0.3],
        k: 60
      }))
      .select("#document","#metadata", "#score")
      .limit(topK);

    const results = await collection.search(search);

    console.log('=== HYBRID SEARCH RAW RESPONSE ===');
    console.log(JSON.stringify(results, null, 2));
    console.log('=== END RAW RESPONSE ===');
    
    if (!results || !results.ids || results.ids.length === 0) {
      return [];
    }

    // Map results to the same shape as queryCollection()
     return results.ids.map((id, idx) => ({
       id,
       text: results.documents?.[idx] ?? '',
       metadata: results.metadatas?.[idx] ?? {},
       distance: 1- (results.scores?.[idx] ?? 0),
       score: results.scores?.[idx] ?? (1 - (results.distances?.[idx] ?? 0))
    }));
    
  } catch (error) {
    console.error('Hybrid query failed, falling back to dense-only:', error.message);
    // Graceful fallback to dense-only search for backward compatibility
    return queryCollection(collection, queryEmbedding, topK);
  }
}

/**
 * Delete all vectors for a given documentId.
 * Paginates collection.get() in BATCH_SIZE chunks so documents with
 * many chunks (> default 100 limit) are fully deleted.
 */
export async function deleteDocumentVectors(collection, documentId) {
  try {
    const allIds = [];
    let offset = 0;

    while (true) {
      const batch = await collection.get({
        where: { document_id: documentId },
        include: [],
        limit: BATCH_SIZE,
        offset
      });

      if (!batch.ids || batch.ids.length === 0) break;
      allIds.push(...batch.ids);

      if (batch.ids.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    if (allIds.length > 0) {
      await collection.delete({ ids: allIds });
    }
    return allIds.length;
  } catch (error) {
    console.error('Failed to delete document vectors:', error);
    throw error;
  }
}

export async function getDocumentCount(collection) {
  try {
    return await collection.count();
  } catch (error) {
    console.error('Failed to get document count:', error);
    return 0;
  }
}

/**
 * List all unique documents in a collection.
 * Paginates collection.get() with BATCH_SIZE=300 so collections larger
 * than Chroma's default get() limit (100) are fully enumerated.
 */
export async function listDocuments(collection) {
  try {
    const documentsMap = new Map();
    let offset = 0;

    while (true) {
      const batch = await collection.get({
        include: ['metadatas', 'documents'],
        limit: BATCH_SIZE,
        offset
      });

      if (!batch.ids || batch.ids.length === 0) break;

      batch.ids.forEach((id, idx) => {
        const meta = batch.metadatas[idx];
        const docId = meta.document_id;

        if (!documentsMap.has(docId)) {
          documentsMap.set(docId, {
            document_id: docId,
            filename: meta.filename,
            chunk_count: 0,
            page_count: meta.page_number || 1,
            upload_timestamp: meta.upload_timestamp,
            source_type: meta.source_type,
            first_chunk_text: batch.documents[idx]
          });
        }

        const doc = documentsMap.get(docId);
        doc.chunk_count++;
        doc.page_count = Math.max(doc.page_count, meta.page_number || 1);
      });

      console.log(`  [listDocuments] offset=${offset}, got=${batch.ids.length}, unique so far=${documentsMap.size}`);

      if (batch.ids.length < BATCH_SIZE) break;
      offset += BATCH_SIZE;
    }

    return Array.from(documentsMap.values());
  } catch (error) {
    console.error('Failed to list documents:', error);
    return [];
  }
}

export async function healthCheck() {
  try {
    const client = getCloudClient();
    const heartbeat = await client.heartbeat();
    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      heartbeat
    };
  } catch (error) {
    return {
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

export async function cleanupSessionCollections() {
  try {
    const client = getCloudClient();
    const collections = await client.listCollections();

    const sessionCollectionNames = collections
      .map(c => (typeof c === 'string' ? c : c.name))
      .filter(name => name.startsWith('session_'));

    if (sessionCollectionNames.length === 0) {
      console.log('\u2705 No stale session collections found.');
      return;
    }

    console.log(`\ud83e\uddf9 Cleaning up ${sessionCollectionNames.length} stale session collection(s)...`);

    await Promise.allSettled(
      sessionCollectionNames.map(async name => {
        try {
          await client.deleteCollection({ name });
          console.log(`  \u2705 Deleted: ${name}`);
        } catch (err) {
          console.warn(`  \u26a0\ufe0f Could not delete ${name}:`, err.message);
        }
      })
    );

    sessionCollections.clear();
    console.log('\u2705 Session collection cleanup complete.');
  } catch (error) {
    console.warn('\u26a0\ufe0f Session cleanup failed (non-fatal):', error.message);
  }
}
