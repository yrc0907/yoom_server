import { PrismaClient } from '@prisma/client';
import { kafka, TOPIC_COMMENTS } from './kafka';
import 'dotenv/config';

const prisma = new PrismaClient();

const consumer = kafka.consumer({ groupId: 'comments-group' });

// eslint-disable-next-line no-console
console.log('Worker started, listening for comments to persist...');

async function startWorker() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_COMMENTS, fromBeginning: true });
  // eslint-disable-next-line no-console
  console.log('Kafka Consumer connected and subscribed.');

  await consumer.run({
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      const validComments = [];

      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        try {
          const commentData = JSON.parse(message.value.toString());
          if (commentData && commentData.videoId && commentData.userId && commentData.content) {
            validComments.push({
              videoId: String(commentData.videoId),
              userId: String(commentData.userId),
              content: String(commentData.content),
            });
          } else {
            // Here you would ideally send to a DLQ topic
            // eslint-disable-next-line no-console
            console.warn('Invalid message structure, skipping:', message.value.toString());
          }
        } catch (e) {
          // eslint-disable-next-line no-console
          console.warn('Failed to parse message, skipping:', message.value.toString());
        }

        resolveOffset(message.offset);
        await heartbeat();
      }

      if (validComments.length > 0) {
        try {
          const result = await prisma.comment.createMany({
            data: validComments,
            skipDuplicates: true,
          });
          // eslint-disable-next-line no-console
          console.log(`Successfully persisted ${result.count} of ${validComments.length} comments.`);
        } catch (dbError) {
          // In a real-world scenario, you'd have a robust retry or DLQ strategy here.
          // For now, we log the error. The offsets are committed, so these messages won't be reprocessed.
          // eslint-disable-next-line no-console
          console.error('Database batch insert failed:', dbError);
        }
      }
    },
  });

  const gracefulShutdown = async () => {
    // eslint-disable-next-line no-console
    console.log('Shutting down consumer gracefully...');
    await consumer.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
}

startWorker().catch(e => {
  // eslint-disable-next-line no-console
  console.error('Failed to start Kafka consumer:', e);
  process.exit(1);
});
