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
 * Recursively removes null bytes (\u0000) from strings, arrays, or objects.
 * PostgreSQL (Supabase) does not support \u0000 in text/jsonb fields.
 */
function sanitizeNullBytes(val) {
  if (typeof val === 'string') {
    return val.replace(/\u0000/g, '');
  }
  if (Array.isArray(val)) {
    return val.map(sanitizeNullBytes);
  }
  if (val !== null && typeof val === 'object') {
    const cleanObj = {};
    for (const key of Object.keys(val)) {
      cleanObj[key] = sanitizeNullBytes(val[key]);
    }
    return cleanObj;
  }
  return val;
}

/**
 * Asynchronously inserts conversation data into Supabase.
 * Chains insertions for the same session to ensure they complete in order.
 */
export function insertConversationAsync(sessionId, data) {
  const previousPromise = sessionInsertPromises.get(sessionId) || Promise.resolve();

  const nextPromise = previousPromise
    .then(async () => {
      const cleanData = sanitizeNullBytes(data);
      console.log(`[Supabase] Inserting conversation for session ${sessionId}, answer_key: ${cleanData.answer_key}`);
      const { error } = await supabase.from('Conversation_History').insert(cleanData);
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

/**
 * Asynchronously updates the feedback for a conversation in Supabase.
 */
export async function updateFeedbackAsync(answerKey, feedback, retries = 2) {
  try {
    const { error } = await supabase
      .from('Conversation_History')
      .update({ feedback })
      .eq('answer_key', answerKey);

    if (error) {
      throw error;
    } else {
      console.log(`[Supabase] Successfully updated feedback for answer_key: ${answerKey}`);
    }
  } catch (error) {
    const isNetworkError = error.message && error.message.includes('fetch failed');
    if (isNetworkError && retries > 0) {
      //console.warn(`[Supabase] Network error during update, retrying... (${retries} attempts left)`);
      // Wait briefly before retrying (e.g., 500ms)
      await new Promise(res => setTimeout(res, 500));
      return updateFeedbackAsync(answerKey, feedback, retries - 1);
    }
    //console.error('[Supabase] Error updating feedback:', error);
    throw error;
  }
}
