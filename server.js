'use strict';
// Duel Block server: accounts, names, verified leaderboard, ELO, and an authoritative duel referee.
const http = require('http');
const path = require('path');
const { Worker } = require('worker_threads');
const { WebSocketServer } = require('ws');
const E = require('./engine');
const store = require('./store');

const PORT = process.env.PORT || 8080;
const DUEL_MS = +process.env.DUEL_MS || 90000;          // length of a duel; when time is up the higher score wins
const GRACE_MS = 1500;                                  // VS intro before the clock starts
const PEN_MS = +process.env.PEN_MS || 10000;            // freeze when a player has no move left (board is also wiped, points lost)
const IDLE_MS = +process.env.IDLE_MS || 40000;          // a player who still has moves but does not play for this long forfeits
const NAME_RE = /^[A-Za-z0-9]{3,12}$/, RESV = ['player', 'opponent', 'you', 'opp', 'admin', 'guest', 'system'];
const K = 32, START_R = 1000, MIN_R = 100, PAIR_MAX = 6;  // ELO settings; max rated games vs same opponent per hour
const MAX_MOVES = 8000;

/* ---------------- helpers ---------------- */
const nameBad = n => typeof n !== 'string' || !NAME_RE.test(n) || RESV.includes(n.toLowerCase());
const devOk = d => typeof d === 'string' && d.length >= 10 && d.length <= 80;
const hits = new Map();
function limited(key, max, win) {
  const now = Date.now(); let h = hits.get(key);
  if (!h || now - h.t > (win || 60000)) { h = { t: now, n: 0 }; hits.set(key, h); }
  return ++h.n > max;
}
setInterval(() => { const now = Date.now(); hits.forEach((h, k) => { if (now - h.t > 120000) hits.delete(k); }); }, 300000);

/* ---------------- leaderboards ---------------- */
const cache = { score: null, rating: null };
const invalidate = () => { cache.score = cache.rating = null; };
function board(kind) {
  const c = cache[kind]; if (c && Date.now() - c.t < 4000) return c;
  const arr = [];
  store.players.forEach((r, id) => {
    if (kind === 'score') { if (r.b > 0) arr.push({ id, r, v: r.b, t: r.bt }); }
    else if (r.g > 0) arr.push({ id, r, v: r.r, t: r.c });
  });
  arr.sort((a, b) => b.v - a.v || a.t - b.t);
  const rank = new Map(); arr.forEach((e, i) => rank.set(e.id, i + 1));
  return (cache[kind] = { t: Date.now(), arr, rank });
}
const rankOf = (kind, dh) => board(kind).rank.get(dh) || 0;
function profile(dh, rec) {
  return { ok: 1, name: rec.n, av: rec.a, rating: rec.r, games: rec.g, best: rec.b, rank: rankOf('score', dh), rrank: rankOf('rating', dh) };
}

/* ---------------- verified replay (anti-cheat for Endless scores) ---------------- */
let workers = 0;
function runReplay(seed, moves) {
  if (moves.length <= 400) return Promise.resolve(E.replay(seed, moves));
  if (workers >= 2) return Promise.reject(new Error('busy'));
  workers++;
  return new Promise((res, rej) => {
    const w = new Worker(path.join(__dirname, 'replay-worker.js'), { workerData: { seed, moves } });
    const to = setTimeout(() => { w.terminate(); rej(new Error('timeout')); }, 10000);
    w.once('message', m => { clearTimeout(to); res(m); });
    w.once('error', e => { clearTimeout(to); rej(e); });
    w.once('exit', () => { workers--; });
  });
}

/* ---------------- HTTP API ---------------- */
const CORS = { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type', 'access-control-allow-methods': 'POST, GET, OPTIONS' };
function out(res, code, o) { res.writeHead(code, Object.assign({ 'content-type': 'application/json' }, CORS)); res.end(JSON.stringify(o)); }
function readBody(req) {
  return new Promise((ok, no) => {
    let n = 0; const a = [];
    req.on('data', c => { n += c.length; if (n > 200 * 1024) { no(new Error('big')); req.destroy(); return; } a.push(c); });
    req.on('end', () => { try { ok(JSON.parse(Buffer.concat(a).toString() || '{}')); } catch (e) { no(e); } });
    req.on('error', no);
  });
}
const routes = {
  '/api/hello': async b => {
    if (!devOk(b.dev)) return [400, { err: 'dev' }];
    const dh = store.hash(b.dev); let rec = store.players.get(dh);
    if (!rec) {
      const n = String(b.name || '');
      if (nameBad(n)) return [200, { err: 'invalid' }];
      if (store.names.has(n.toLowerCase())) return [200, { err: 'taken' }];
      rec = store.create(dh, n, b.av); invalidate();
    } else if (Number.isInteger(b.av) && b.av >= 0 && b.av <= 9 && rec.a !== b.av) { rec.a = b.av; store.touch(dh); }
    return [200, profile(dh, rec)];
  },
  '/api/name': async b => {
    if (!devOk(b.dev)) return [400, { err: 'dev' }];
    const n = String(b.name || ''); if (nameBad(n)) return [200, { err: 'invalid' }];
    const dh = store.hash(b.dev), rec = store.players.get(dh), owner = store.names.get(n.toLowerCase());
    if (owner && owner !== dh) return [200, { err: 'taken' }];
    if (!rec) store.create(dh, n, b.av); else if (rec.n !== n) store.rename(dh, n);
    invalidate();
    return [200, profile(dh, store.players.get(dh))];
  },
  '/api/score': async (b, ip) => {
    if (!devOk(b.dev)) return [400, { err: 'dev' }];
    const dh = store.hash(b.dev), rec = store.players.get(dh);
    if (!rec) return [200, { err: 'unknown' }];
    if (limited('sc:' + dh, 10)) return [429, { err: 'rate' }];
    const seed = b.seed, mv = b.moves;
    if (!Number.isInteger(seed) || seed < 0 || seed > 2147483647 || !Array.isArray(mv) || mv.length < 1 || mv.length > MAX_MOVES) return [200, { err: 'invalid' }];
    let r;
    try { r = await runReplay(seed, mv); } catch (e) { return [e.message === 'busy' ? 503 : 500, { err: e.message }]; }
    if (!r.ok) { console.warn('rejected run from', rec.n, 'illegal move at', r.at); return [200, { err: 'invalid' }]; }
    const improved = r.score > rec.b;
    if (improved) { rec.b = r.score; rec.bt = Date.now(); store.touch(dh); invalidate(); }
    return [200, { ok: 1, score: r.score, best: rec.b, improved, rank: rankOf('score', dh) }];
  },
  '/api/lb': async b => {
    const kind = b.kind === 'rating' ? 'rating' : 'score', bd = board(kind);
    const top = bd.arr.slice(0, 10).map((e, i) => ({ rank: i + 1, name: e.r.n, av: e.r.a, v: e.v }));
    let me = null;
    if (devOk(b.dev)) { const dh = store.hash(b.dev), i = bd.rank.get(dh); if (i) me = { rank: i, v: bd.arr[i - 1].v }; }
    return [200, { ok: 1, kind, top, me, total: bd.arr.length }];
  }
};
const server = http.createServer(async (req, res) => {
  const url = req.url.split('?')[0];
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }
  if (url === '/' || url === '/health') return out(res, 200, { ok: true, online: wss.clients.size, waiting: queue.length, rooms: rooms.size, matches: matches.size, players: store.players.size, store: store.mode });
  const h = routes[url];
  if (!h || req.method !== 'POST') return out(res, 404, { err: 'nf' });
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (limited('ip:' + ip, 120)) return out(res, 429, { err: 'rate' });
  try { const [c, o] = await h(await readBody(req), ip); out(res, c, o); }
  catch (e) { out(res, 400, { err: 'bad' }); }
});

/* ---------------- duel referee ---------------- */
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const rooms = new Map(), matches = new Set(), active = new Map(), pairs = new Map();
let queue = [];
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const rec = ws => store.players.get(ws.dh);

function unqueue(ws) {
  queue = queue.filter(w => w !== ws);
  if (ws.room && rooms.get(ws.room) === ws) rooms.delete(ws.room);
  ws.room = null;
}
const elo = (ra, rb, sa) => Math.round(K * (sa - 1 / (1 + Math.pow(10, (rb - ra) / 400))));

class Match {
  constructor(a, b, rated) {
    this.s = [a, b].map(ws => ({ ws, e: null, score: 0, combo: 0, rm: false, last: 0, frz: 0 }));
    this.rated = rated; this.done = true; this.round = 0;
    a.m = b.m = this; a.state = b.state = 'play'; matches.add(this);
    this.begin();
  }
  begin() {
    this.seed = Math.floor(Math.random() * 1e9); this.done = false; this.round++;
    const now = Date.now();
    this.dl = now + GRACE_MS + DUEL_MS;
    this.s.forEach(x => { x.e = E.newGame(this.seed); x.frz = 0; x.score = 0; x.combo = 0; x.rm = false; x.last = now + GRACE_MS; });
    this.s.forEach((x, i) => {
      const o = rec(this.s[1 - i].ws), me = rec(x.ws);
      send(x.ws, { t: 'go', s: this.seed, d: DUEL_MS, on: o.n, oa: o.a, or: o.r, mr: me.r, rd: this.rated ? 1 : 0 });
    });
  }
  idx(ws) { return this.s[0].ws === ws ? 0 : 1; }
  move(ws, v) {
    if (this.done) return;
    const i = this.idx(ws), x = this.s[i], o = this.s[1 - i], now = Date.now();
    if (now > this.dl + 300) { this.timeUp(); return; }   // too late: the clock already ran out
    if (now < x.frz) return;                              // still serving the stuck penalty
    if (!E.applyMove(x.e, v)) { console.warn('invalid duel move from', rec(ws).n); this.finish(1 - i, 'Opponent was disqualified', 'Invalid move detected'); return; }
    x.score = x.e.score; x.combo = x.e.combo; x.last = now;
    if (!E.anyMove(x.e)) {                                // stuck: wipe the board, lose points, freeze
      const lost = E.stuckReset(x.e);
      x.score = x.e.score; x.combo = 0; x.frz = now + PEN_MS; x.last = x.frz;
      send(ws, { t: 'pen', sc: x.score, lost, ms: PEN_MS });
      send(o.ws, { t: 's', g: x.e.g, sc: x.score, cb: 0, pn: PEN_MS });
      return;
    }
    send(o.ws, { t: 's', g: x.e.g, sc: x.score, cb: x.combo });
  }
  timeUp() {
    const [a, b] = this.s;
    this.finish(a.score > b.score ? 0 : a.score < b.score ? 1 : 2, 'Time is up', 'Time is up');
  }
  finish(w, wWhy, lWhy) {
    if (this.done) return; this.done = true;
    const [a, b] = this.s, ra = rec(a.ws), rb = rec(b.ws);
    let da = 0, db = 0, counted = false, ur = this.rated ? '' : 'friendly';
    if (this.rated) {
      const key = [a.ws.dh, b.ws.dh].sort().join(':'), now = Date.now();
      const hist = (pairs.get(key) || []).filter(t => now - t < 3600000);
      if (hist.length >= PAIR_MAX) { ur = 'repeat'; pairs.set(key, hist); }
      else {
        hist.push(now); pairs.set(key, hist); counted = true;
        const sa = w === 0 ? 1 : w === 1 ? 0 : .5;
        da = elo(ra.r, rb.r, sa); db = -da;
        const na = Math.max(MIN_R, ra.r + da), nb = Math.max(MIN_R, rb.r + db);
        da = na - ra.r; db = nb - rb.r; ra.r = na; rb.r = nb;
        ra.g++; rb.g++;
        if (w === 0) { ra.w++; rb.l++; } else if (w === 1) { rb.w++; ra.l++; } else { ra.d++; rb.d++; }
        store.touch(a.ws.dh); store.touch(b.ws.dh); invalidate();
      }
    }
    [[a, b, ra, da, 0], [b, a, rb, db, 1]].forEach(([x, o, r, d, i]) => {
      const res = w === 2 ? 'draw' : w === i ? 'win' : 'lose';
      send(x.ws, { t: 'end', r: res, why: res === 'draw' ? wWhy : res === 'win' ? wWhy : lWhy, ms: x.score, os: o.score, dr: d, nr: r.r, rd: counted ? 1 : 0, ur });
    });
  }
  rematch(ws) {
    if (!this.done) return;
    const i = this.idx(ws), x = this.s[i], o = this.s[1 - i];
    if (x.rm) return; x.rm = true; send(o.ws, { t: 'rm' });
    if (o.rm) this.begin();
  }
  leave(ws) {
    const i = this.idx(ws), o = this.s[1 - i];
    if (!this.done) this.finish(1 - i, 'Opponent left', 'You left the match');
    matches.delete(this);
    if (o.ws.m === this) { o.ws.m = null; o.ws.state = 'idle'; send(o.ws, { t: 'left' }); }
  }
}
setInterval(() => {                                        // duel clock + inactivity forfeit
  const now = Date.now();
  matches.forEach(m => {
    if (m.done) return;
    if (now > m.dl + 300) { m.timeUp(); return; }
    for (let i = 0; i < 2; i++) { const x = m.s[i]; if (now >= x.frz && now - x.last > IDLE_MS) { m.finish(1 - i, 'Opponent was inactive', 'You were inactive'); break; } }
  });
}, 250);

function authed(ws, m) {
  if (!devOk(m.dev)) { send(ws, { t: 'err', msg: 'Not signed in' }); return false; }
  const dh = store.hash(m.dev);
  if (!store.players.has(dh)) { send(ws, { t: 'err', msg: 'Not registered yet. Try again.' }); return false; }
  const old = active.get(dh); if (old && old !== ws) { try { old.terminate(); } catch (e) {} }
  active.set(dh, ws); ws.dh = dh; return true;
}
wss.on('connection', ws => {
  ws.alive = true; ws.m = null; ws.room = null; ws.dh = null; ws.n = 0; ws.state = 'idle';
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    if (++ws.n > 80) { ws.terminate(); return; }           // flood protection (80 msg/sec)
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'm') { if (ws.m && typeof m.v === 'number') ws.m.move(ws, m.v); return; }
    if (m.t === 'rm') { if (ws.m) ws.m.rematch(ws); return; }
    if (m.t === 'cancel') { if (ws.state === 'wait') { unqueue(ws); ws.state = 'idle'; } return; }
    if (ws.state !== 'idle') return;
    if (m.t !== 'quick' && m.t !== 'room' && m.t !== 'join') return;
    if (!authed(ws, m)) return;

    if (m.t === 'quick') {
      const me = rec(ws); let bi = -1, bd = 1e9;
      queue.forEach((o, i) => { if (o.readyState !== 1 || o.state !== 'wait' || o.dh === ws.dh) return; const d = Math.abs(rec(o).r - me.r); if (d < bd) { bd = d; bi = i; } });
      if (bi >= 0) { const o = queue.splice(bi, 1)[0]; new Match(o, ws, true); }
      else { queue.push(ws); ws.state = 'wait'; }
    } else if (m.t === 'room') {
      let code = null;
      for (let i = 0; i < 60 && !code; i++) { const c = String(1000 + Math.floor(Math.random() * 9000)); if (!rooms.has(c)) code = c; }
      if (!code) { send(ws, { t: 'err', msg: 'Could not create a room' }); return; }
      rooms.set(code, ws); ws.room = code; ws.state = 'wait'; send(ws, { t: 'code', c: code });
    } else {
      const code = String(m.code || ''), host = /^\d{4}$/.test(code) ? rooms.get(code) : null;
      if (!host || host === ws || host.dh === ws.dh || host.readyState !== 1 || host.state !== 'wait') { send(ws, { t: 'err', msg: 'Room not found' }); return; }
      unqueue(host); new Match(host, ws, false);
    }
  });
  ws.on('close', () => {
    unqueue(ws);
    if (ws.dh && active.get(ws.dh) === ws) active.delete(ws.dh);
    if (ws.m) { const mm = ws.m; ws.m = null; mm.leave(ws); }
  });
  ws.on('error', () => {});
});
setInterval(() => { wss.clients.forEach(ws => { ws.n = 0; }); }, 1000);
setInterval(() => {                                        // drop dead connections
  wss.clients.forEach(ws => { if (!ws.alive) { ws.terminate(); return; } ws.alive = false; try { ws.ping(); } catch (e) {} });
}, 20000);

/* ---------------- start ---------------- */
store.load().then(() => {
  server.listen(PORT, () => console.log('Duel Block server on :' + PORT));
}).catch(e => { console.error('Could not load store:', e.message); process.exit(1); });
process.on('SIGTERM', async () => { await store.flush(); process.exit(0); });
