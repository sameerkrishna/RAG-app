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
