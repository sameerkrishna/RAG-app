export async function retrieveForQuery(query, sessionId, options = {}) {
  const topK = options.topK || TOP_K;
  const includeGlobal = options.includeGlobal !== false;

  try {
    // ✅ Run embedding + both collection fetches in parallel
    const [queryEmbedding, globalCollection, sessionCollection] = await Promise.all([
      embedQuery(query),
      includeGlobal ? getGlobalCollection() : Promise.resolve(null),
      sessionId
        ? getSessionCollection(sessionId).catch(() => null)
        : Promise.resolve(null)
    ]);

    // ✅ Now query both collections in parallel
    const queryPromises = [];

    if (globalCollection) {
      queryPromises.push(
        queryCollection(globalCollection, queryEmbedding, topK)
          .then(results => ({ type: 'global', results }))
          .catch(() => ({ type: 'global', results: [] }))
      );
    }

    if (sessionCollection) {
      queryPromises.push(
        queryCollection(sessionCollection, queryEmbedding, topK)
          .then(results => ({ type: 'session', results }))
          .catch(() => ({ type: 'session', results: [] }))
      );
    }

    const queryResults = await Promise.all(queryPromises);

    const allResults = [];
    for (const { type, results: typeResults } of queryResults) {
      for (const result of typeResults) {
        allResults.push({ ...result, source_type: type });
      }
    }

    allResults.sort((a, b) => b.score - a.score);
    const topResults = allResults.slice(0, topK);
    const coverage = calculateCoverage(topResults, topK);

    return { results: topResults, coverage, queryEmbedding };

  } catch (error) {
    console.error('Retrieval error:', error);
    throw error;
  }
}