import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { PrismaClient } from '@prisma/client';
import 'dotenv/config';
import { z } from 'zod';
import jwt, { JwtPayload } from 'jsonwebtoken';

const prisma = new PrismaClient();
const app = new Hono();

const allowedOrigin = process.env.ALLOWED_ORIGIN || '*';
app.use('*', async (c, next) => {
  c.header('Access-Control-Allow-Origin', allowedOrigin);
  c.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  c.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (c.req.method === 'OPTIONS') return c.body(null, 204);
  await next();
});

function verify(c: any): { userId: string } | null {
  const auth = c.req.header('authorization') || c.req.header('Authorization') || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return null;
  try {
    const secret = process.env.JWT_SECRET || process.env.AUTH_JWT_SECRET || 'dev-secret';
    const decoded = jwt.verify(token, secret) as JwtPayload & { sub?: string };
    if (!decoded?.sub) return null;
    return { userId: String(decoded.sub) };
  } catch { return null; }
}

app.get('/health', (c) => c.json({ ok: true }));

// 发布视频
const publishSchema = z.object({ videoKey: z.string().min(1), title: z.string().optional(), allowComments: z.boolean().optional() });
app.post('/publish', async (c) => {
  const auth = verify(c); if (!auth) return c.json({ error: 'unauthorized' }, 401);
  const body = await c.req.json().catch(() => ({}));
  const parsed = publishSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body', details: parsed.error.flatten() }, 400);
  const created = await prisma.publish.create({ data: { authorId: auth.userId, videoKey: parsed.data.videoKey, title: parsed.data.title, allowComments: parsed.data.allowComments ?? true } });
  return c.json(created);
});

// 列出发布（公共）
app.get('/feeds', async (c) => {
  const items = await prisma.publish.findMany({ orderBy: { createdAt: 'desc' }, take: 100 });
  return c.json({ items });
});

// 单个发布详情（公共）
app.get('/feeds/:id', async (c) => {
  const id = c.req.param('id');
  const pub = await prisma.publish.findUnique({ where: { id } });
  if (!pub) return c.json({ error: 'not found' }, 404);
  return c.json(pub);
});

// 记录评论回复（弱引用到 comments-service 的 commentId）
const replySchema = z.object({ publishId: z.string().min(1), commentId: z.string().min(1), content: z.string().min(1).max(2000) });
app.post('/reply', async (c) => {
  const auth = verify(c); if (!auth) return c.json({ error: 'unauthorized' }, 401);
  const body = await c.req.json().catch(() => ({}));
  const parsed = replySchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body' }, 400);
  const created = await prisma.reply.create({ data: { publishId: parsed.data.publishId, commentId: parsed.data.commentId, content: parsed.data.content, authorId: auth.userId } });
  return c.json(created);
});

// 列出某个发布下的回复
app.get('/replies', async (c) => {
  const publishId = c.req.query('publishId');
  if (!publishId) return c.json({ error: 'publishId required' }, 400);
  const items = await prisma.reply.findMany({ where: { publishId }, orderBy: { createdAt: 'asc' } });
  return c.json({ items });
});

const port = Number(process.env.PORT || 4004);
serve({ fetch: app.fetch, port }, () => {
  console.log(`feed-service listening on http://localhost:${port}`);
});


