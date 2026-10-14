'use strict';
// Player storage. Persists to Upstash Redis (REST) when UPSTASH_REDIS_REST_URL / _TOKEN are set,
// otherwise to a local JSON file (data is lost on Render free tier restarts - use Upstash there).
const fs = require('fs'), path = require('path'), crypto = require('crypto');
const RURL = process.env.UPSTASH_REDIS_REST_URL, RTOK = process.env.UPSTASH_REDIS_REST_TOKEN;
const FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const mode = RURL && RTOK ? 'upstash' : 'file';
const players = new Map();   // devHash -> record
const names = new Map();     // lowercase name -> devHash
const dirty = new Set();
let timer = null;

const hash = dev => crypto.createHash('sha256').update('dblk|' + dev).digest('hex').slice(0, 32);

async function rcmd(cmd) {
  const r = await fetch(RURL, { method: 'POST', headers: { Authorization: 'Bearer ' + RTOK, 'content-type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
function index(dh, rec) { players.set(dh, rec); names.set(rec.n.toLowerCase(), dh); }

async function load() {
  if (mode === 'upstash') {
    const a = (await rcmd(['HGETALL', 'db:players'])) || [];
    for (let i = 0; i < a.length; i += 2) { try { index(a[i], JSON.parse(a[i + 1])); } catch (e) {} }
  } else {
    try { const o = JSON.parse(fs.readFileSync(FILE, 'utf8')); for (const k of Object.keys(o)) index(k, o[k]); } catch (e) {}
  }
  console.log('store:', mode, '-', players.size, 'players loaded');
}
function touch(dh) { dirty.add(dh); if (!timer) timer = setTimeout(() => { flush(); }, 2500); }
async function flush() {
  clearTimeout(timer); timer = null;
  const ids = [...dirty]; dirty.clear();
  if (!ids.length) return;
  try {
    if (mode === 'upstash') {
      for (let i = 0; i < ids.length; i += 40) {
        const cmd = ['HSET', 'db:players'];
        ids.slice(i, i + 40).forEach(id => { if (players.has(id)) cmd.push(id, JSON.stringify(players.get(id))); });
        if (cmd.length > 2) await rcmd(cmd);
      }
    } else {
      const o = {}; players.forEach((v, k) => { o[k] = v; });
      fs.writeFileSync(FILE + '.tmp', JSON.stringify(o)); fs.renameSync(FILE + '.tmp', FILE);
    }
  } catch (e) {
    console.error('store flush failed:', e.message);
    ids.forEach(id => dirty.add(id));
    if (!timer) timer = setTimeout(() => { flush(); }, 15000);
  }
}
function create(dh, name, av) {
  const rec = { n: name, a: (av | 0) >= 0 && (av | 0) <= 9 ? av | 0 : 0, r: 1000, g: 0, w: 0, l: 0, d: 0, b: 0, bt: 0, c: Date.now() };
  index(dh, rec); touch(dh); return rec;
}
function rename(dh, name) {
  const rec = players.get(dh); names.delete(rec.n.toLowerCase()); rec.n = name; names.set(name.toLowerCase(), dh); touch(dh);
}
module.exports = { mode, players, names, hash, load, flush, touch, create, rename };
