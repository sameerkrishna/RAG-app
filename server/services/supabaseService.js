import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

/**
 * Insert a new conversation row as soon as the LLM response is complete.
 * feedback defaults to 'unrated' per the table definition.
 */
export async function logAnswer({ sessionId, answerKey, query, chunks, llmResponse }) {
  const conversation = {
    session_id:   sessionId,
    query,
    chunks:       chunks.map(r => ({
      text:     r.text?.slice(0, 500) ?? '',
      filename: r.metadata?.filename ?? '',
      page:     r.metadata?.page_number ?? null,
      score:    r.score ?? null
    })),
    llm_response: llmResponse
  };

  const { error } = await supabase
    .from('Convesation_History')
    .insert({
      answer_key:   answerKey,
      conversation,
      feedback:     'unrated'
    });

  if (error) {
    // Non-fatal — log but don't crash the request
    console.error('[supabase] logAnswer failed:', error.message);
  } else {
    console.log(`[supabase] Logged answer ${answerKey}`);
  }
}

/**
 * Update feedback for an existing row identified by answer_key.
 * @param {string} answerKey  UUID of the answer
 * @param {'liked'|'disliked'} feedback
 */
export async function updateFeedback(answerKey, feedback) {
  const { error } = await supabase
    .from('Convesation_History')
    .update({ feedback })
    .eq('answer_key', answerKey);

  if (error) {
    console.error('[supabase] updateFeedback failed:', error.message);
    throw error;
  } else {
    console.log(`[supabase] Feedback updated for ${answerKey}: ${feedback}`);
  }
}
