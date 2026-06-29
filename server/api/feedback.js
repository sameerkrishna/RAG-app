import { Router } from 'express';
import { v4 as uuidv4 } from 'uuid';

const router = Router();

// In-memory feedback store (could be replaced with database)
const feedbackStore = new Map();

export async function submitFeedback(req, res) {
  const { answerId, sessionId, type, comment, rating } = req.body;

  if (!answerId || !type) {
    return res.status(400).json({
      error: 'answerId and type are required',
      code: 'MISSING_FIELDS'
    });
  }

  const validTypes = ['positive', 'negative', 'helpful', 'not_helpful', 'report_issue'];
  if (!validTypes.includes(type)) {
    return res.status(400).json({
      error: 'Invalid feedback type',
      code: 'INVALID_TYPE',
      validTypes
    });
  }

  try {
    const feedback = {
      id: uuidv4(),
      answerId,
      sessionId: sessionId || 'unknown',
      type,
      rating: rating || null,
      comment: comment || null,
      createdAt: new Date().toISOString(),
      userAgent: req.headers['user-agent'] || null,
      ip: req.ip || null
    };

    feedbackStore.set(feedback.id, feedback);

    res.status(201).json({
      success: true,
      feedbackId: feedback.id,
      message: 'Thank you for your feedback'
    });
  } catch (error) {
    console.error('Feedback submission error:', error);
    res.status(500).json({
      error: 'Failed to submit feedback',
      code: 'FEEDBACK_ERROR'
    });
  }
}

export async function getFeedbackStats(req, res) {
  const { answerId } = req.params;

  try {
    const allFeedback = Array.from(feedbackStore.values());
    const answerFeedback = allFeedback.filter(f => f.answerId === answerId);

    const stats = {
      total: answerFeedback.length,
      positive: answerFeedback.filter(f => f.type === 'positive' || f.type === 'helpful').length,
      negative: answerFeedback.filter(f => f.type === 'negative' || f.type === 'not_helpful').length,
      averageRating: answerFeedback
        .filter(f => f.rating)
        .reduce((sum, f, _, arr) => sum + f.rating / arr.length, 0) || null
    };

    res.json(stats);
  } catch (error) {
    res.status(500).json({
      error: 'Failed to get feedback stats',
      code: 'STATS_ERROR'
    });
  }
}

export async function listFeedback(req, res) {
  const { sessionId } = req.query;

  try {
    let feedback = Array.from(feedbackStore.values());

    if (sessionId) {
      feedback = feedback.filter(f => f.sessionId === sessionId);
    }

    res.json({
      total: feedback.length,
      feedback: feedback.slice(-50) // Last 50 entries
    });
  } catch (error) {
    res.status(500).json({
      error: 'Failed to list feedback',
      code: 'LIST_ERROR'
    });
  }
}

router.post('/', submitFeedback);
router.get('/stats/:answerId', getFeedbackStats);
router.get('/list', listFeedback);

export default router;
