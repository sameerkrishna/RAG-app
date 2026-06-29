import { CloudClient } from "chromadb"; // ✅ Use CloudClient, not ChromaClient
import { v4 as uuidv4 } from 'uuid';

const chromaConfig = {
  apiKey: process.env.CHROMA_API_KEY,
  tenant: process.env.CHROMA_TENANT || 'default_tenant',
  database: process.env.CHROMA_DATABASE || 'default_database',
  // Only needed if connecting to a non-default region (e.g., GCP europe-west1)
  host: process.env.CHROMA_HOST || undefined,
};

// --- RUNTIME DEBUG LOGS ---
console.log("---- CHROMA CONNECTIVITY DEBUG ----");
console.log("Host:      ", chromaConfig.host || "api.trychroma.com (default)");
console.log("Tenant:    ", chromaConfig.tenant);
console.log("DB Name:   ", chromaConfig.database);
console.log("API Key:   ", chromaConfig.apiKey ? "LOADED (VALID)" : "MISSING (UNDEFINED)");
console.log("-----------------------------------");

let client = null;
let globalCollection = null;
const sessionCollections = new Map();

function getClient() {
  if (!client) {
    if (!chromaConfig.apiKey) {
      throw new Error(
        "CRITICAL ERROR: CHROMA_API_KEY is undefined. " +
        "Ensure your environment variables are correctly loaded before executing this file."
      );
    }

    // ✅ CloudClient handles auth internally — just pass apiKey, tenant, and database
    const clientOptions = {
      apiKey: chromaConfig.apiKey,
      tenant: chromaConfig.tenant,
      database: chromaConfig.database,
    };

    // Only add host if connecting to a non-default region
    if (chromaConfig.host) {
      clientOptions.host = chromaConfig.host;
    }

    client = new CloudClient(clientOptions);
  }
  return client;
}

export async function getGlobalCollection() {
  if (!globalCollection) {
    const client = getClient();
    const collectionName = process.env.CHROMA_GLOBAL_COLLECTION || 'dev';

    try {
      globalCollection = await client.getOrCreateCollection({
        name: collectionName,
        metadata: {
          description: 'Permanent seed documents for RAG',
          type: 'global_knowledge'
        }
      });
    } catch (error) {
      console.error('Failed to create global collection:', error);
      throw error;
    }
  }
  console.log("created global db");
  return globalCollection;
}

export async function createSessionCollection(sessionId) {
  const client = getClient();
  const collectionName = `session_${sessionId}`;

  try {
    const collection = await client.getOrCreateCollection({
      name: collectionName,
      metadata: {
        type: 'session_upload',
        session_id: sessionId,
        created: new Date().toISOString()
      }
    });

    sessionCollections.set(sessionId, collection);
    console.log("created session db");
    return collection;
  } catch (error) {
    console.error(`Failed to create session collection ${collectionName}:`, error);
    throw error;
  }
}

export async function getSessionCollection(sessionId) {
  if (sessionCollections.has(sessionId)) {
    return sessionCollections.get(sessionId);
  }
  return createSessionCollection(sessionId);
}

export async function deleteSessionCollection(sessionId) {
  const client = getClient();
  const collectionName = `session_${sessionId}`;

  try {
    await client.deleteCollection({ name: collectionName });
    sessionCollections.delete(sessionId);
    return true;
  } catch (error) {
    console.error(`Failed to delete session collection ${collectionName}:`, error);
    return false;
  }
}

export async function addVectors(collection, vectors, embeddings, ids) {
  try {
    await collection.add({
      ids,
      embeddings,
      documents: vectors.map(v => v.text),
      metadatas: vectors.map(v => v.metadata)
    });
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

export async function deleteDocumentVectors(collection, documentId) {
  try {
    const existing = await collection.get({
      where: { document_id: documentId }
    });

    if (existing.ids && existing.ids.length > 0) {
      await collection.delete({
        ids: existing.ids
      });
      return existing.ids.length;
    }
    return 0;
  } catch (error) {
    console.error('Failed to delete document vectors:', error);
    throw error;
  }
}

export async function getDocumentCount(collection) {
  try {
    const count = await collection.count();
    return count;
  } catch (error) {
    console.error('Failed to get document count:', error);
    return 0;
  }
}

export async function listDocuments(collection) {
  try {
    const allItems = await collection.get({
      include: ['metadatas', 'documents']
    });

    const documentsMap = new Map();

    if (allItems.ids) {
      allItems.ids.forEach((id, idx) => {
        const meta = allItems.metadatas[idx];
        const docId = meta.document_id;

        if (!documentsMap.has(docId)) {
          documentsMap.set(docId, {
            document_id: docId,
            filename: meta.filename,
            chunk_count: 0,
            page_count: meta.page_number || 1,
            upload_timestamp: meta.upload_timestamp,
            source_type: meta.source_type,
            first_chunk_text: allItems.documents[idx]
          });
        }

        const doc = documentsMap.get(docId);
        doc.chunk_count++;
        doc.page_count = Math.max(doc.page_count, meta.page_number || 1);
      });
    }

    return Array.from(documentsMap.values());
  } catch (error) {
    console.error('Failed to list documents:', error);
    return [];
  }
}

export async function healthCheck() {
  try {
    const client = getClient();
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