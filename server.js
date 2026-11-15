// Lakou — chat room tan reyèl, san okenn depandans (Node 18+ sèlman)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const PUBLIC = ['lakou', 'mizik', 'tek', 'blag'];
const CODE = /^[a-z0-9]{4,12}$/;
const MAX_MEDIA = 600000, WEEK = 7 * 24 * 3600, DAY = 24 * 3600 * 1000;
const TYPES = { image: ['image/jpeg', 'image/png', 'image/webp', 'image/gif'], audio: ['audio/webm', 'audio/mp4', 'audio/ogg', 'audio/mpeg'] };

/* ---------- Depo: Upstash Redis (si varyab yo la) oswa fichye lokal ---------- */
const UP_URL = process.env.UPSTASH_REDIS_REST_URL, UP_TOK = process.env.UPSTASH_REDIS_REST_TOKEN;
const DIR = path.join(__dirname, 'data');
async function redis(cmd) {
  const r = await fetch(UP_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOK, 'Content-Type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
}
async function kvSet(key, val, ttl) {
  if (UP_URL) return redis(ttl ? ['SET', key, val, 'EX', ttl] : ['SET', key, val]);
  await fs.promises.mkdir(DIR, { recursive: true });
  await fs.promises.writeFile(path.join(DIR, key + '.txt'), val);
}
async function kvGet(key) {
  if (UP_URL) return redis(['GET', key]);
  try { return await fs.promises.readFile(path.join(DIR, key + '.txt'), 'utf8'); } catch { return null; }
}

/* ---------- Eta ---------- */
let rooms = {}, seq = 1;
const clients = new Map(); // id -> {res, name, hue, room, timer}
const mediaCache = new Map(); // mid -> {type, buf}
const getRoom = id => rooms[id] || (rooms[id] = { msgs: [], at: Date.now() });
const send = (res, ev, data) => res.write(`event: ${ev}\ndata: ${JSON.stringify(data)}\n\n`);
const broadcast = (room, ev, data) => { for (const c of clients.values()) if (c.room === room && c.res) send(c.res, ev, data); };
const users = room => [...clients.entries()].filter(([, c]) => c.room === room).map(([id, c]) => ({ id, name: c.name, hue: c.hue }));
const counts = () => Object.fromEntries(PUBLIC.map(r => [r, users(r).length]));
const presence = (room, note) => {
  broadcast(room, 'presence', { users: users(room), note });
  const c = counts(); for (const cl of clients.values()) if (cl.res) send(cl.res, 'counts', c);
};

let saveT = null;
function dirty() { if (!saveT) saveT = setTimeout(save, 1500); }
async function save() {
  saveT = null;
  const now = Date.now();
  for (const id of Object.keys(rooms)) {
    if (PUBLIC.includes(id) || users(id).length) continue;
    if (!rooms[id].msgs.length || now - rooms[id].at > 30 * DAY) delete rooms[id];
  }
  const priv = Object.keys(rooms).filter(id => !PUBLIC.includes(id)).sort((a, b) => rooms[b].at - rooms[a].at);
  for (const id of priv.slice(300)) if (!users(id).length) delete rooms[id];
  try { await kvSet('lakou-state', JSON.stringify({ seq, rooms })); } catch (e) { console.error('Sove echwe:', e.message); }
}
async function load() {
  try {
    const s = JSON.parse(await kvGet('lakou-state') || 'null');
    if (s && s.rooms) { rooms = s.rooms; seq = s.seq || 1; }
    console.log(`Depo: ${UP_URL ? 'Upstash Redis' : 'fichye lokal'} — ${Object.values(rooms).reduce((n, r) => n + r.msgs.length, 0)} mesaj chaje`);
  } catch (e) { console.error('Chaje echwe:', e.message); }
  if (!UP_URL) try { // netwaye ansyen medya sou disk
    for (const f of await fs.promises.readdir(DIR)) if (f.startsWith('lakou-m-')) {
      const p = path.join(DIR, f); if (Date.now() - (await fs.promises.stat(p)).mtimeMs > WEEK * 1000) fs.promises.unlink(p).catch(() => {});
    }
  } catch {}
  PUBLIC.forEach(getRoom);
}

function addMsg(c, uid, text, replyTo, media) {
  const R = getRoom(c.room), h = R.msgs, r = h.find(m => m.id === replyTo);
  const label = m => m.text || (m.media ? (m.media.kind === 'image' ? '📷 Foto' : '🎤 Mesaj vokal') : '');
  const msg = { id: seq++, uid, name: c.name, hue: c.hue, text, ts: Date.now(), reacts: {},
    reply: r ? { name: r.name, text: label(r).slice(0, 80), hue: r.hue } : null };
  if (media) msg.media = media;
  h.push(msg); if (h.length > 150) h.shift();
  R.at = Date.now();
  broadcast(c.room, 'msg', msg); dirty();
}

const body = req => new Promise((ok, no) => {
  let b = ''; req.on('data', d => { b += d; if (b.length > 8000) { req.destroy(); no(); } });
  req.on('end', () => { try { ok(JSON.parse(b || '{}')); } catch { no(); } });
});
const raw = (req, max) => new Promise((ok, no) => {
  const a = []; let n = 0;
  req.on('data', d => { n += d.length; if (n > max) no(); else a.push(d); });
  req.on('end', () => ok(Buffer.concat(a))); req.on('error', no);
});
const num = (v, lo, hi) => Math.max(lo, Math.min(hi, Number(v) || 0));

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x'), q = url.searchParams;

  /* --- Koneksyon tan reyèl (SSE) --- */
  if (url.pathname === '/events') {
    const id = (q.get('id') || '').slice(0, 40);
    const name = (q.get('name') || 'Anonim').trim().slice(0, 20) || 'Anonim';
    const hue = Math.abs(parseInt(q.get('hue')) || 0) % 360;
    const rq = (q.get('room') || '').toLowerCase();
    const room = PUBLIC.includes(rq) || CODE.test(rq) ? rq : 'lakou';
    if (!id) { res.writeHead(400); return res.end(); }
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', 'X-Accel-Buffering': 'no', Connection: 'keep-alive' });
    res.write('retry: 2000\n\n');
    const prev = clients.get(id);
    let isNew = true;
    if (prev) {
      clearTimeout(prev.timer);
      if (prev.res) prev.res.end();
      isNew = prev.room !== room;
      if (isNew) { clients.delete(id); presence(prev.room, { type: 'leave', name: prev.name }); }
    }
    clients.set(id, { res, name, hue, room });
    send(res, 'hello', { room, history: getRoom(room).msgs, users: users(room), counts: counts() });
    presence(room, isNew ? { type: 'join', name, id } : null);
    const hb = setInterval(() => res.write(': ping\n\n'), 20000);
    req.on('close', () => {
      clearInterval(hb);
      const c = clients.get(id);
      if (!c || c.res !== res) return;
      c.res = null;
      c.timer = setTimeout(() => { clients.delete(id); presence(room, { type: 'leave', name }); }, 4000);
    });
    return;
  }

  /* --- Voye foto / mesaj vokal --- */
  if (req.method === 'POST' && url.pathname === '/media') {
    const c = clients.get(q.get('id')), kind = q.get('kind');
    const type = (req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
    if (!c) { res.writeHead(403); return res.end(); }
    if (!TYPES[kind] || !TYPES[kind].includes(type)) { res.writeHead(415); return res.end(); }
    let buf; try { buf = await raw(req, MAX_MEDIA); } catch { res.writeHead(413, { Connection: 'close' }); return res.end(); }
    if (!buf.length) { res.writeHead(400); return res.end(); }
    const mid = crypto.randomBytes(8).toString('hex');
    mediaCache.set(mid, { type, buf });
    if (mediaCache.size > 40) mediaCache.delete(mediaCache.keys().next().value);
    try { await kvSet('lakou-m-' + mid, type + '|' + buf.toString('base64'), WEEK); } catch (e) { console.error('Medya pa sove:', e.message); }
    const media = kind === 'image' ? { id: mid, kind, w: Math.round(num(q.get('w'), 1, 4000)), h: Math.round(num(q.get('h'), 1, 4000)) }
                                   : { id: mid, kind, dur: Math.round(num(q.get('dur'), 1, 90) * 10) / 10 };
    addMsg(c, q.get('id'), '', Number(q.get('replyTo')) || 0, media);
    res.writeHead(204); return res.end();
  }

  /* --- Mesaj, ekri, reyaksyon --- */
  if (req.method === 'POST') {
    let d; try { d = await body(req); } catch { res.writeHead(400); return res.end(); }
    const c = clients.get(d.id);
    if (!c) { res.writeHead(403); return res.end(); }
    if (url.pathname === '/send') {
      const text = String(d.text || '').trim().slice(0, 1000);
      if (text) addMsg(c, d.id, text, d.replyTo);
    } else if (url.pathname === '/typing') {
      broadcast(c.room, 'typing', { id: d.id, name: c.name });
    } else if (url.pathname === '/react') {
      const m = getRoom(c.room).msgs.find(m => m.id === d.msgId);
      const e = String(d.emoji || '').slice(0, 8);
      if (m && e) {
        const a = m.reacts[e] || (m.reacts[e] = []);
        const i = a.indexOf(d.id);
        if (i >= 0) a.splice(i, 1); else a.push(d.id);
        if (!a.length) delete m.reacts[e];
        broadcast(c.room, 'react', { msgId: m.id, reacts: m.reacts, by: d.id, added: i < 0 });
        dirty();
      }
    }
    res.writeHead(204); return res.end();
  }

  /* --- Sèvi medya --- */
  const mm = url.pathname.match(/^\/media\/([a-f0-9]{16})$/);
  if (mm) {
    let m = mediaCache.get(mm[1]);
    if (!m) {
      let v = null; try { v = await kvGet('lakou-m-' + mm[1]); } catch {}
      if (v) { const i = v.indexOf('|'); m = { type: v.slice(0, i), buf: Buffer.from(v.slice(i + 1), 'base64') }; mediaCache.set(mm[1], m);
        if (mediaCache.size > 40) mediaCache.delete(mediaCache.keys().next().value); }
    }
    if (!m) { res.writeHead(404); return res.end(); }
    const h = { 'Content-Type': m.type, 'Accept-Ranges': 'bytes', 'Cache-Control': 'public, max-age=604800, immutable', 'X-Content-Type-Options': 'nosniff' };
    const rg = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
    if (rg && (rg[1] || rg[2])) {
      const L = m.buf.length;
      let a = rg[1] ? +rg[1] : L - +rg[2], b = rg[1] && rg[2] ? +rg[2] : L - 1;
      a = Math.max(0, a); b = Math.min(L - 1, b);
      if (a > b) { res.writeHead(416, { 'Content-Range': `bytes */${L}` }); return res.end(); }
      res.writeHead(206, { ...h, 'Content-Range': `bytes ${a}-${b}/${L}`, 'Content-Length': b - a + 1 });
      return res.end(m.buf.subarray(a, b + 1));
    }
    res.writeHead(200, { ...h, 'Content-Length': m.buf.length });
    return res.end(m.buf);
  }

  fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});

load().then(() => server.listen(PORT, '0.0.0.0', () => console.log('Lakou ap koute sou pò ' + PORT)));
for (const s of ['SIGTERM', 'SIGINT']) process.on(s, async () => { clearTimeout(saveT); await save(); process.exit(0); });
