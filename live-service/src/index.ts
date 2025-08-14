import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { PrismaClient } from '@prisma/client';
import { z } from 'zod';
import 'dotenv/config';
import jwt, { JwtPayload } from 'jsonwebtoken';

const prisma = new PrismaClient();
const app = new Hono();

const allowedOrigin = process.env.ALLOWED_ORIGIN || '*';
const RTMP_BASE = process.env.LIVE_RTMP_BASE || 'rtmp://localhost:1935'; // e.g. SRS/Nginx-RTMP
const HLS_BASE = process.env.LIVE_HLS_BASE || 'http://localhost:8080';    // e.g. SRS http server
const WEBRTC_BASE = process.env.LIVE_WEBRTC_BASE || 'webrtc://localhost'; // e.g. SRS WebRTC
app.use('*', async (c, next) => {
  c.header('Access-Control-Allow-Origin', allowedOrigin);
  c.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  c.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (c.req.method === 'OPTIONS') return c.body(null, 204);
  await next();
});

app.get('/health', (c) => c.json({ ok: true }));

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

// Create stream (returns ingest key & playback placeholder)
const createSchema = z.object({ title: z.string().min(1), description: z.string().optional() });
app.post('/streams', async (c) => {
  const auth = verify(c); if (!auth) return c.json({ error: 'unauthorized' }, 401);
  const body = await c.req.json().catch(() => ({}));
  const parsed = createSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body' }, 400);
  const ingestKey = `ing_${Math.random().toString(36).slice(2, 10)}${Date.now().toString(36)}`;
  const ingestUrl = `${RTMP_BASE.replace(/\/$/, '')}/live/${ingestKey}`;
  // 默认将播放地址设置为 WebRTC（更低延迟）。如需 HLS 可通过 /status Webhook 覆盖
  const playbackId = `${WEBRTC_BASE.replace(/\/$/, '')}/live/${ingestKey}`;
  const created = await prisma.liveStream.create({ data: { title: parsed.data.title, description: parsed.data.description, ingestKey, authorId: auth.userId, status: 'LIVE', playbackId } });
  return c.json({ id: created.id, ingestKey: created.ingestKey, ingestUrl, status: created.status, playbackId: created.playbackId });
});

// Get stream by id
app.get('/streams/:id', async (c) => {
  const id = c.req.param('id');
  const s = await prisma.liveStream.findUnique({ where: { id } });
  if (!s) return c.json({ error: 'not found' }, 404);
  return c.json(s);
});

// 返回推流地址（需要登录）
app.get('/streams/:id/ingest', async (c) => {
  const auth = verify(c); if (!auth) return c.json({ error: 'unauthorized' }, 401);
  const id = c.req.param('id');
  const s = await prisma.liveStream.findUnique({ where: { id } });
  if (!s || s.authorId !== auth.userId) return c.json({ error: 'forbidden' }, 403);
  const ingestUrl = `${RTMP_BASE.replace(/\/$/, '')}/live/${s.ingestKey}`;
  return c.json({ ingestUrl });
});

// List streams
app.get('/streams', async (c) => {
  const onlyLive = c.req.query('live') === '1';
  const where = onlyLive ? { status: 'LIVE' } : undefined as any;
  const list = await prisma.liveStream.findMany({ where, orderBy: { createdAt: 'desc' } });
  return c.json({ items: list });
});

// Webhook to update status & playbackId (from your streaming infra)
const updateSchema = z.object({ status: z.enum(['PENDING', 'LIVE', 'ENDED']).optional(), playbackId: z.string().optional() });
app.post('/streams/:id/status', async (c) => {
  const id = c.req.param('id');
  const body = await c.req.json().catch(() => ({}));
  const parsed = updateSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body' }, 400);
  const updated = await prisma.liveStream.update({ where: { id }, data: parsed.data });
  // If stream ended, cleanup ephemeral comments of this room
  try {
    if (parsed.data.status === 'ENDED') {
      const COMMENTS_BASE = process.env.COMMENTS_BASE || 'http://localhost:4001';
      await fetch(`${COMMENTS_BASE}/comments?videoId=${encodeURIComponent(id)}`, { method: 'DELETE' });
      // Also purge Redis hot cache list if present (handled in comments-service DELETE)
    }
  } catch { /* ignore cleanup errors */ }
  return c.json(updated);
});

const port = Number(process.env.PORT || 4003);
serve({ fetch: app.fetch, port }, () => {
  console.log(`live-service listening on http://localhost:${port}`);
});


