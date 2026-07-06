import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
const supabaseKey = process.env.VITE_SUPABASE_ANON_KEY || process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.warn('Supabase URL or Key is missing. Database operations will not work properly.');
}

export const supabase = createClient(
  supabaseUrl || 'http://localhost',
  supabaseKey || 'public-anon-key'
);

// Map to track the last insertion promise per session
const sessionInsertPromises = new Map();

/**
 * Asynchronously inserts conversation data into Supabase.
 * Chains insertions for the same session to ensure they complete in order.
 */
export function insertConversationAsync(sessionId, data) {
  const previousPromise = sessionInsertPromises.get(sessionId) || Promise.resolve();

  const nextPromise = previousPromise
    .then(async () => {
      console.log(`[Supabase] Inserting conversation for session ${sessionId}, answer_key: ${data.answer_key}`);
      const { error } = await supabase.from('Conversation_History').insert(data);
      if (error) {
        console.error('[Supabase] Error inserting conversation history:', error);
      } else {
        console.log(`[Supabase] Successfully inserted conversation for session ${sessionId}`);
      }
    })
    .catch((err) => {
      console.error('[Supabase] Unexpected error during insertion chain:', err);
    });

  sessionInsertPromises.set(sessionId, nextPromise);

  // Optional: clean up the promise from the map if it's the last one
  nextPromise.finally(() => {
    if (sessionInsertPromises.get(sessionId) === nextPromise) {
      sessionInsertPromises.delete(sessionId);
    }
  });

  return nextPromise;
}
