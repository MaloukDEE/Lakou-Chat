// Lakou — chat room tan reyèl, san okenn depandans (Node sèlman)
const http = require('http'), fs = require('fs'), path = require('path');
const PORT = process.env.PORT || 3000;
const ROOMS = ['lakou', 'mizik', 'tek', 'blag'];
const history = Object.fromEntries(ROOMS.map(r => [r, []]));
const clients = new Map(); // id -> {res, name, hue, room, timer}
let seq = 1;

const send = (res, ev, data) => res.write(`event: ${ev}\ndata: ${JSON.stringify(data)}\n\n`);
const broadcast = (room, ev, data) => { for (const c of clients.values()) if (c.room === room && c.res) send(c.res, ev, data); };
const users = room => [...clients.entries()].filter(([, c]) => c.room === room).map(([id, c]) => ({ id, name: c.name, hue: c.hue }));
const counts = () => Object.fromEntries(ROOMS.map(r => [r, users(r).length]));
const presence = (room, note) => {
  broadcast(room, 'presence', { users: users(room), note });
  const c = counts(); for (const cl of clients.values()) if (cl.res) send(cl.res, 'counts', c);
};
const body = req => new Promise((ok, no) => {
  let b = ''; req.on('data', d => { b += d; if (b.length > 8000) { req.destroy(); no(); } });
  req.on('end', () => { try { ok(JSON.parse(b || '{}')); } catch { no(); } });
});

http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/events') {
    const id = (url.searchParams.get('id') || '').slice(0, 40);
    const name = (url.searchParams.get('name') || 'Anonim').trim().slice(0, 20) || 'Anonim';
    const hue = Math.abs(parseInt(url.searchParams.get('hue')) || 0) % 360;
    const room = ROOMS.includes(url.searchParams.get('room')) ? url.searchParams.get('room') : 'lakou';
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
    send(res, 'hello', { room, history: history[room], users: users(room), counts: counts() });
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
  if (req.method === 'POST') {
    let d; try { d = await body(req); } catch { res.writeHead(400); return res.end(); }
    const c = clients.get(d.id);
    if (!c) { res.writeHead(403); return res.end(); }
    if (url.pathname === '/send') {
      const text = String(d.text || '').trim().slice(0, 1000);
      if (text) {
        const h = history[c.room];
        const r = h.find(m => m.id === d.replyTo);
        const msg = { id: seq++, uid: d.id, name: c.name, hue: c.hue, text, ts: Date.now(), reacts: {},
          reply: r ? { name: r.name, text: r.text.slice(0, 80), hue: r.hue } : null };
        h.push(msg); if (h.length > 150) h.shift();
        broadcast(c.room, 'msg', msg);
      }
    } else if (url.pathname === '/typing') {
      broadcast(c.room, 'typing', { id: d.id, name: c.name });
    } else if (url.pathname === '/react') {
      const m = history[c.room].find(m => m.id === d.msgId);
      const e = String(d.emoji || '').slice(0, 8);
      if (m && e) {
        const a = m.reacts[e] || (m.reacts[e] = []);
        const i = a.indexOf(d.id);
        if (i >= 0) a.splice(i, 1); else a.push(d.id);
        if (!a.length) delete m.reacts[e];
        broadcast(c.room, 'react', { msgId: m.id, reacts: m.reacts, by: d.id, added: i < 0 });
      }
    }
    res.writeHead(204); return res.end();
  }
  fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(data);
  });
}).listen(PORT, '0.0.0.0', () => console.log('Lakou ap koute sou pò ' + PORT));
