import { CloudClient, Schema, SparseVectorIndexConfig, DOCUMENT_KEY, Search, Knn, Rrf } from 'chromadb';
import { ChromaBm25EmbeddingFunction } from '@chroma-core/chroma-bm25';
import { v4 as uuidv4 } from 'uuid';

const BATCH_SIZE = 300;

let cloudClient = null;
let globalCollection = null;
let _globalCollectionPromise = null;
let _collectionSchema = null;

// Lazy — avoids loading BM25 model files at module import time
function getCollectionSchema() {
  if (!_collectionSchema) {
    const bm25 = new ChromaBm25EmbeddingFunction();
    _collectionSchema = new Schema().createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: bm25,
        sourceKey: DOCUMENT_KEY,
        bm25: true
      }),
      'sparse_bm25'
    );
  }
  return _collectionSchema;
}

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
  if (globalCollection) return globalCollection;

  // Deduplicate concurrent callers — all await the same promise
  if (!_globalCollectionPromise) {
    _globalCollectionPromise = (async () => {
      const client = getCloudClient();
      const collectionName = process.env.CHROMA_GLOBAL_COLLECTION || 'seed_db';
      try {
        const col = await client.getOrCreateCollection({
          name: collectionName,
          schema: getCollectionSchema(),
          metadata: {
            description: 'Permanent seed documents for RAG',
            type: 'global_knowledge'
          },
          embeddingFunction: null
        });
        globalCollection = col;
        console.log(`\u2705 Global collection ready: ${collectionName}`);
        return col;
      } catch (error) {
        _globalCollectionPromise = null; // allow retry on next call
        console.error('Failed to connect to global collection:', error);
        throw error;
      }
    })();
  }

  return _globalCollectionPromise;
}

/**
 * Returns the single shared collection.
 * Drop-in replacement for the old getSessionCollection — callers that
 * previously destructured { collection } will still work.
 */
export async function getCollection() {
  const collection = await getGlobalCollection();
  return { collection, isNew: false };
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

export async function queryCollection(collection, queryEmbedding, topK = 5, where = undefined) {
  try {
    const queryOpts = {
      queryEmbeddings: [queryEmbedding],
      nResults: topK,
      include: ['documents', 'metadatas', 'distances']
    };
    if (where) queryOpts.where = where;

    const results = await collection.query(queryOpts);

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
 * Accepts an optional `where` clause for metadata filtering (e.g. session_id $in).
 */
export async function hybridQueryCollection(collection, queryText, queryEmbedding, topK = 5, where = undefined) {
  try {
    let search = new Search()
      .rank(Rrf({
        ranks: [
          Knn({ query: queryEmbedding, returnRank: true, limit: 20 }),
          Knn({ query: queryText, key: 'sparse_bm25', returnRank: true, limit: 20 })
        ],
        weights: [0.9, 0.1],
        k: 60
      }))
      .where(where)
      .select("#document", "#metadata", "#score")
      .limit(topK);

    const raw = await collection.search(search);

    // Parallel‑array structure: ids[0], documents[0], metadatas[0], scores[0]
    if (!raw.ids || !raw.ids[0] || raw.ids[0].length === 0) {
      return [];
    }

    const ids = raw.ids[0];
    const docs = raw.documents?.[0] ?? [];
    const metas = raw.metadatas?.[0] ?? [];
    const scores = raw.scores?.[0] ?? [];

    // 1. Define global RRF bounds based on your weights [0.7, 0.3] and limits (100)
    // Max possible raw RRF: 1 / (60 + 1) = 0.0163934
    // Min possible raw RRF: 1 / (60 + 100) = 0.0062500
    const MAX_RRF = 1 / 61;
    const MIN_RRF = 1 / 160;

    return ids.map((id, idx) => {
      // Chroma returns negative values (e.g. -0.01639), convert to positive raw RRF
      const rawRRF = Math.abs(scores[idx] ?? MIN_RRF);

      // 2. Linear min-max normalization to fit perfectly between 0.0 and 1.0
      let normalizedScore = (rawRRF - MIN_RRF) / (MAX_RRF - MIN_RRF);

      // Boundary protection
      normalizedScore = Math.max(0, Math.min(1, normalizedScore));

      //const finalScore = Math.round(normalizedScore * 100) / 100;

      return {
        id,
        text: docs[idx] ?? '',
        metadata: metas[idx] ?? {},
        distance: 1 - normalizedScore,
        score: normalizedScore
      };
    });


  } catch (error) {
    console.error('Hybrid query failed, falling back to dense-only:', error.message);
    // Graceful fallback to dense-only search for backward compatibility
    return queryCollection(collection, queryEmbedding, topK, where);
  }
}

/**
 * Delete all vectors for a given documentId.
 * Paginates collection.get() in BATCH_SIZE chunks so documents with
 * many chunks (>default 100 limit) are fully deleted.
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

/**
 * Delete all vectors belonging to a specific session.
 * Uses session_id metadata filter to find and remove them in batches.
 */
export async function deleteSessionVectors(sessionId) {
  try {
    const collection = await getGlobalCollection();
    const allIds = [];
    let offset = 0;

    while (true) {
      const batch = await collection.get({
        where: { session_id: sessionId },
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
    console.log(`\u2705 Deleted ${allIds.length} session vectors for session_id=${sessionId}`);
    return allIds.length;
  } catch (error) {
    console.error(`Failed to delete session vectors for ${sessionId}:`, error);
    return 0;
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
 * Accepts an optional `where` clause for metadata filtering.
 */
export async function listDocuments(collection, where = undefined) {
  try {
    const documentsMap = new Map();
    let offset = 0;

    while (true) {
      const getOpts = {
        include: ['metadatas', 'documents'],
        limit: BATCH_SIZE,
        offset
      };
      if (where) getOpts.where = where;

      const batch = await collection.get(getOpts);

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
