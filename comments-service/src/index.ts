import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import 'dotenv/config';

const prisma = new PrismaClient();
const app = new Hono();

// CORS (simple)
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*';
app.use('*', async (c, next) => {
  c.header('Access-Control-Allow-Origin', allowedOrigin);
  c.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
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
  const items = await prisma.comment.findMany({
    where: { videoId },
    orderBy: { createdAt: 'asc' },
  });
  return c.json({ items });
});

// Create a comment
const createSchema = z.object({
  videoId: z.string().min(1),
  userId: z.string().min(1),
  content: z.string().min(1).max(2000),
});

app.post('/comments', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = createSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body', details: parsed.error.flatten() }, 400);
  const { videoId, userId, content } = parsed.data;
  const created = await prisma.comment.create({ data: { videoId, userId, content } });
  return c.json(created);
});

const port = Number(process.env.PORT || 4001);
serve({ fetch: app.fetch, port }, () => {
  // eslint-disable-next-line no-console
  console.log(`comments-service listening on http://localhost:${port}`);
});


