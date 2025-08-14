import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { PrismaClient } from '@prisma/client';
import bcrypt from 'bcryptjs';
import jwt, { JwtPayload } from 'jsonwebtoken';
import { z } from 'zod';
import 'dotenv/config';

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

app.get('/health', (c) => c.json({ ok: true }));

const registerSchema = z.object({ email: z.string().email(), password: z.string().min(6) });
const loginSchema = registerSchema;

app.post('/register', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = registerSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body', details: parsed.error.flatten() }, 400);
  const { email, password } = parsed.data;
  const exists = await prisma.user.findUnique({ where: { email } });
  if (exists) return c.json({ error: 'email exists' }, 409);
  const hash = await bcrypt.hash(password, 10);
  const created = await prisma.user.create({ data: { email, password: hash } });
  return c.json({ id: created.id, email: created.email, createdAt: created.createdAt });
});

app.post('/login', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = loginSchema.safeParse(body);
  if (!parsed.success) return c.json({ error: 'invalid body' }, 400);
  const { email, password } = parsed.data;
  const user = await prisma.user.findUnique({ where: { email } });
  if (!user) return c.json({ error: 'invalid credentials' }, 401);
  const ok = await bcrypt.compare(password, user.password);
  if (!ok) return c.json({ error: 'invalid credentials' }, 401);
  const secret = process.env.JWT_SECRET || 'dev-secret';
  const token = jwt.sign({ sub: user.id, email }, secret, { expiresIn: '7d' });
  return c.json({ token });
});

// Middleware: verify JWT
function verifyAuth(c: any): { userId: string } | null {
  const auth = c.req.header('authorization') || c.req.header('Authorization') || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return null;
  try {
    const secret = process.env.JWT_SECRET || 'dev-secret';
    const decoded = jwt.verify(token, secret) as JwtPayload & { sub?: string };
    if (!decoded?.sub) return null;
    return { userId: String(decoded.sub) };
  } catch {
    return null;
  }
}

// GET /me: current user
app.get('/me', async (c) => {
  const auth = verifyAuth(c);
  if (!auth) return c.json({ error: 'unauthorized' }, 401);
  const user = await prisma.user.findUnique({ where: { id: auth.userId }, select: { id: true, email: true, createdAt: true } });
  if (!user) return c.json({ error: 'not found' }, 404);
  return c.json(user);
});

// GET /users/:id
app.get('/users/:id', async (c) => {
  const id = c.req.param('id');
  const user = await prisma.user.findUnique({ where: { id }, select: { id: true, email: true, createdAt: true } });
  if (!user) return c.json({ error: 'not found' }, 404);
  return c.json(user);
});

const port = Number(process.env.PORT || 4002);
serve({ fetch: app.fetch, port }, () => {
  // eslint-disable-next-line no-console
  console.log(`auth-service listening on http://localhost:${port}`);
});


