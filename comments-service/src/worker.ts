import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import 'dotenv/config';

const prisma = new PrismaClient();
const redisUrl = process.env.REDIS_URL || process.env.UPSTASH_REDIS_REST_URL || '';

if (!redisUrl) {
  // eslint-disable-next-line no-console
  console.error('REDIS_URL is not defined. Worker cannot start.');
  process.exit(1);
}

const redis = new Redis(redisUrl);
const QUEUE_NAME = 'comments:queue';
const DLQ_NAME = 'comments:queue:dlq'; // Dead-Letter Queue
const BATCH_SIZE = 100; // Max comments to process at once
const BATCH_INTERVAL_MS = 1000; // Process every 1 second

// eslint-disable-next-line no-console
console.log('Worker started, listening for comments to persist...');

async function processBatch() {
  try {
    // Atomically fetch and trim the list
    const messages = await redis.lrange(QUEUE_NAME, 0, BATCH_SIZE - 1);
    if (messages.length === 0) {
      return;
    }
    await redis.ltrim(QUEUE_NAME, messages.length, -1);

    const validComments = [];
    const invalidMessages = [];

    for (const message of messages) {
      try {
        const commentData = JSON.parse(message);
        // Basic validation
        if (commentData && commentData.videoId && commentData.userId && commentData.content) {
          validComments.push({
            videoId: String(commentData.videoId),
            userId: String(commentData.userId),
            content: String(commentData.content),
          });
        } else {
          invalidMessages.push(message);
        }
      } catch (e) {
        invalidMessages.push(message); // JSON parsing failed
      }
    }

    // Batch insert valid comments
    if (validComments.length > 0) {
      try {
        const result = await prisma.comment.createMany({
          data: validComments,
          skipDuplicates: true, // Good practice for retries
        });
        // eslint-disable-next-line no-console
        console.log(`Successfully persisted ${result.count} comments.`);
      } catch (dbError) {
        // If batch insert fails, move all messages from this batch to DLQ
        // eslint-disable-next-line no-console
        console.error('Database batch insert failed. Moving batch to DLQ.', dbError);
        const allBatchMessages = validComments.map(c => JSON.stringify(c)).concat(invalidMessages);
        if (allBatchMessages.length > 0) {
          await redis.rpush(DLQ_NAME, ...allBatchMessages);
        }
        return; // Skip individual DLQ handling below
      }
    }

    // Move invalid messages to DLQ
    if (invalidMessages.length > 0) {
      await redis.rpush(DLQ_NAME, ...invalidMessages);
      // eslint-disable-next-line no-console
      console.warn(`Moved ${invalidMessages.length} invalid messages to DLQ.`);
    }
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('An unexpected error occurred in worker:', error);
  }
}

async function startWorker() {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    await processBatch();
    await new Promise(resolve => setTimeout(resolve, BATCH_INTERVAL_MS));
  }
}

startWorker().catch(e => {
  // eslint-disable-next-line no-console
  console.error('Worker process failed:', e);
  process.exit(1);
});
