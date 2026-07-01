import { Router } from 'express';
import { updateFeedback } from '../services/supabaseService.js';

const router = Router();

/**
 * PATCH /feedback/:answerKey
 * Body: { feedback: 'liked' | 'disliked' }
 */
export async function submitFeedback(req, res) {
  const { answerKey } = req.params;
  const { feedback } = req.body;

  if (!answerKey) {
    return res.status(400).json({ error: 'answerKey is required', code: 'MISSING_ANSWER_KEY' });
  }

  if (!['liked', 'disliked'].includes(feedback)) {
    return res.status(400).json({
      error: 'feedback must be "liked" or "disliked"',
      code: 'INVALID_FEEDBACK'
    });
  }

  try {
    await updateFeedback(answerKey, feedback);
    res.json({ success: true, answerKey, feedback });
  } catch (error) {
    console.error('Feedback update error:', error);
    res.status(500).json({ error: 'Failed to update feedback', code: 'FEEDBACK_ERROR' });
  }
}

router.patch('/:answerKey', submitFeedback);

export default router;
