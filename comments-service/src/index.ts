import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import 'dotenv/config';
import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import { WebSocketServer } from 'ws';
import Redis from 'ioredis';
import { kafka, TOPIC_COMMENTS } from './kafka';
import { Producer } from 'kafkajs';
// Fallback type for ws when @types/ws is not installed
type WS = any;

// Use official ioredis import and Upstash rediss URL

const prisma = new PrismaClient();
const app = new Hono();
const producer = kafka.producer();

// CORS (simple)
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*';
app.use('*', async (c, next) => {
  c.header('Access-Control-Allow-Origin', allowedOrigin);
  c.header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  c.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (c.req.method === 'OPTIONS') return c.body(null, 204);
  await next();
});

// Health
app.get('/health', (c) => c.json({ ok: true }));

// List comments by videoId
app.get('/comments', async (c) => {
  const videoId = c.req.query('videoId');
  if (!videoId) return c.json({ error: 'videoId required' }, 400);
  const limitRaw = c.req.query('limit');
  const limit = Math.min(Math.max(Number(limitRaw || 0) || 0, 0), 1000) || 200;
  const scope = (c.req.query('scope') || 'live').toLowerCase(); // live|vod
  const key = `chat:${scope}:${videoId}`;
  if (redisPublisher) {
    try {
      const arr = await redisPublisher.lrange(key, 0, limit - 1);
      if (Array.isArray(arr) && arr.length > 0) {
        const parsed = arr.map((s: string) => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
        return c.json({ items: parsed.reverse() });
      }
    } catch { }
  }
  const itemsDesc = await prisma.comment.findMany({ where: { videoId }, take: limit, orderBy: { createdAt: 'desc' } });
  return c.json({ items: itemsDesc.reverse() });
});

// Create a comment
const createSchema = z.object({
  videoId: z.string().min(1),
  userId: z.string().min(1),
  content: z.string().min(1).max(2000),
  replyToId: z.string().optional(),
  replyToUserId: z.string().optional(),
  persist: z.boolean().optional(),
});
const PERSIST_MODE = (process.env.COMMENTS_PERSIST || 'none').toLowerCase(); // none|sample|flagged|all
const SAMPLE_RATE = Math.min(Math.max(Number(process.env.SAMPLE_RATE || '0') || 0, 0), 1);

app.post('/comments', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = createSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body', details: parsed.error.flatten() }, 400);
  const { videoId, userId, content, replyToId, replyToUserId } = parsed.data;
  const scope = (c.req.query('scope') || 'live').toLowerCase();
  // Generate lightweight object first
  const now = new Date().toISOString();
  const temp = { id: randomUUID(), videoId, userId, content, createdAt: now, replyToId, replyToUserId } as any;
  let created = temp as any;
  // Decide persistence policy
  const forcePersist = c.req.query('persist') === '1' || (parsed.data as any).persist === true;
  const shouldPersist = forcePersist || PERSIST_MODE === 'all' || (PERSIST_MODE === 'sample' && Math.random() < SAMPLE_RATE);
  if (shouldPersist) {
    try {
      await producer.send({
        topic: TOPIC_COMMENTS,
        messages: [
          {
            // Use videoId as key for partitioning, ensuring comments for the same video go to the same partition.
            key: videoId,
            // Send the full temporary object. The 'id' is crucial for idempotency in the consumer.
            value: JSON.stringify(temp),
          },
        ],
      });
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error('[kafka] failed to produce message:', e instanceof Error ? e.message : e);
    }
  }
  // Broadcast immediately to ensure UX first (room channel)
  publish(String(videoId), { type: 'comment', item: created });
  if (replyToUserId) publish(`user:${replyToUserId}`, { type: 'reply', item: created });
  // Offload Redis cache write to next tick to avoid blocking
  if (redisPublisher) {
    setImmediate(async () => {
      try {
        const key = `chat:${scope}:${videoId}`;
        await redisPublisher.lpush(key, JSON.stringify(created));
        await redisPublisher.ltrim(key, 0, 499);
        await redisPublisher.expire(key, 24 * 60 * 60);
      } catch (e) {
        // eslint-disable-next-line no-console
        console.error('[redis] cache write failed:', e instanceof Error ? e.message : e);
      }
    });
  }
  return c.json(created);
});

// --- Realtime: WS + Pub/Sub (Redis optional) ---
const localBus = new EventEmitter();
const REDIS_URL = process.env.REDIS_URL || process.env.UPSTASH_REDIS_REST_URL || '';
const redisPublisher = REDIS_URL ? new Redis(REDIS_URL) : null;

function publish(roomId: string, payload: any) {
  const text = typeof payload === 'string' ? payload : JSON.stringify(payload);
  if (redisPublisher) redisPublisher.publish(`room:${roomId}`, text).catch(() => { });
  // local fall-back
  setImmediate(() => localBus.emit(`room:${roomId}`, text));
}

// --- WS server on separate port ---
const wsPort = Number(process.env.COMMENTS_WS_PORT || 4002);
const wss = new WebSocketServer({ port: wsPort, clientTracking: true });
const roomSubscribers = new Map<string, Set<WS>>();
const redisSubscriber = REDIS_URL ? new Redis(REDIS_URL) : null;
if (redisPublisher) {
  redisPublisher.on('error', (err) => {
    // eslint-disable-next-line no-console
    console.error('[redis] publisher error:', err?.message || err);
  });
}
if (redisSubscriber) {
  redisSubscriber.on('error', (err) => {
    // eslint-disable-next-line no-console
    console.error('[redis] subscriber error:', err?.message || err);
  });
}

if (redisSubscriber) {
  redisSubscriber.on('message', (channel: string, message: string) => {
    localBus.emit(channel, message);
  });
}

// Debug endpoint to verify Redis connectivity
app.get('/debug/redis', async (c) => {
  try {
    const ok = !!redisPublisher;
    const pong = redisPublisher ? await redisPublisher.ping() : null;
    return c.json({ ok, pong, url: REDIS_URL ? 'set' : 'unset' });
  } catch (e) {
    return c.json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 500);
  }
});

function ensureSubscribeRoom(roomId: string) {
  const channel = `room:${roomId}`;
  if (redisSubscriber) {
    // subscribe once per room
    if (!roomSubscribers.has(roomId)) {
      redisSubscriber.subscribe(channel).catch(() => { });
    }
  }
}

wss.on('connection', (ws: WS, req: any) => {
  try {
    const url = new URL(req.url || '/', 'http://localhost');
    const roomId = url.searchParams.get('roomId') || '';
    if (!roomId) { try { ws.close(); } catch { } return; }
    const channel = `room:${roomId}`;

    // track subscribers
    if (!roomSubscribers.has(roomId)) roomSubscribers.set(roomId, new Set());
    roomSubscribers.get(roomId)!.add(ws);

    ensureSubscribeRoom(roomId);

    const onLocal = (msg: string) => { try { ws.send(msg); } catch { } };
    localBus.on(channel, onLocal);

    ws.on('message', (raw: any) => {
      try {
        const text = typeof raw === 'string' ? raw : (raw as any).toString();
        const data = JSON.parse(text);
        if (data && data.type === 'comment' && data.videoId && data.userId && data.content) {
          const content: string = String(data.content).slice(0, 2000);
          const now = new Date().toISOString();
          const temp = { id: randomUUID(), videoId: String(data.videoId), userId: String(data.userId), content, createdAt: now, replyToId: data.replyToId, replyToUserId: data.replyToUserId } as any;
          const scope = (data.scope || 'live').toLowerCase();
          const save = async () => {
            if (PERSIST_MODE === 'all' || (PERSIST_MODE === 'sample' && Math.random() < SAMPLE_RATE)) {
              try { return await prisma.comment.create({ data: { videoId: String(data.videoId), userId: String(data.userId), content } }); } catch (e) { /* eslint-disable-next-line no-console */ console.error('[db] persist failed:', e instanceof Error ? e.message : e); }
            }
            return temp as any;
          };
          save().then((created) => {
            publish(String(data.videoId), { type: 'comment', item: created });
            if (data.replyToUserId) publish(`user:${String(data.replyToUserId)}`, { type: 'reply', item: created });
            if (redisPublisher) {
              setImmediate(async () => {
                try {
                  const key = `chat:${scope}:${created.videoId}`;
                  await redisPublisher.lpush(key, JSON.stringify(created));
                  await redisPublisher.ltrim(key, 0, 499);
                  await redisPublisher.expire(key, 24 * 60 * 60);
                } catch (e) { console.error('[redis] cache write failed:', e instanceof Error ? e.message : e); }
              });
            }
          });
        }
      } catch { }
    });

    ws.on('close', () => {
      localBus.off(channel, onLocal as any);
      const set = roomSubscribers.get(roomId);
      if (set) {
        set.delete(ws);
        if (set.size === 0) {
          roomSubscribers.delete(roomId);
          if (redisSubscriber) { try { redisSubscriber.unsubscribe(channel); } catch { } }
        }
      }
    });
  } catch { try { (ws as any).close(); } catch { } }
});

const port = Number(process.env.PORT || 4001);

const startServer = async () => {
  // Connect the producer
  await producer.connect();
  // eslint-disable-next-line no-console
  console.log('Kafka Producer connected.');

  serve({ fetch: app.fetch, port }, () => {
    // eslint-disable-next-line no-console
    console.log(`comments-service listening on http://localhost:${port}, ws://localhost:${wsPort}/?roomId=...`);
  });

  const gracefulShutdown = async () => {
    // eslint-disable-next-line no-console
    console.log('Shutting down gracefully...');
    await producer.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
};

startServer().catch(e => {
  // eslint-disable-next-line no-console
  console.error('Failed to start server:', e);
  process.exit(1);
});


