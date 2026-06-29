import { Router } from 'express';
import { healthCheck as chromaHealthCheck } from '../services/chromaService.js';
import { getRateLimitState } from '../services/embeddingService.js';

const router = Router();

export async function health(req, res) {
  const healthStatus = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    services: {}
  };

  // Check ChromaDB
  try {
    const chromaHealth = await chromaHealthCheck();
    healthStatus.services.chromadb = chromaHealth;
  } catch (error) {
    healthStatus.services.chromadb = {
      status: 'error',
      error: error.message
    };
  }

  // Check Gemini (via API key presence)
  healthStatus.services.gemini = {
    status: process.env.GEMINI_API_KEY ? 'configured' : 'not_configured'
  };

  // Get rate limit state
  healthStatus.rateLimit = getRateLimitState();

  // Overall status
  const hasErrors = Object.values(healthStatus.services).some(
    s => s.status === 'error' || s.status === 'unhealthy'
  );

  if (hasErrors) {
    healthStatus.status = 'degraded';
  }

  res.json(healthStatus);
}

router.get('/', health);

export default router;
