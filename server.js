// Duel Block - matchmaking + relay server
// Protocol (JSON):
//  client -> server: {t:'quick'} | {t:'room'} | {t:'join',code:'1234'} | {t:'cancel'} | {t:'r',m:{...}}  (r = relay to opponent)
//  server -> client: {t:'code',c:'1234'} | {t:'matched',host:true|false} | {t:'r',m:{...}} | {t:'left'} | {t:'err',msg:'...'}
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const rooms = new Map();   // code -> waiting host socket
let queue = [];            // quick-match waiting sockets

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify({ ok: true, online: wss.clients.size, waiting: queue.length, rooms: rooms.size }));
  } else { res.writeHead(404); res.end(); }
});
const wss = new WebSocketServer({ server, maxPayload: 8192 });

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const idle = ws => { ws.state = 'idle'; };

function unqueue(ws) {
  queue = queue.filter(w => w !== ws);
  if (ws.room && rooms.get(ws.room) === ws) rooms.delete(ws.room);
  ws.room = null;
}
function pair(a, b) {
  unqueue(a); unqueue(b);
  a.peer = b; b.peer = a; a.state = b.state = 'play';
  send(a, { t: 'matched', host: true });
  send(b, { t: 'matched', host: false });
}

wss.on('connection', ws => {
  ws.alive = true; ws.peer = null; ws.room = null; ws.n = 0; idle(ws);
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    if (++ws.n > 60) { ws.terminate(); return; }          // flood protection (60 msg/sec)
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'r') {                                    // relay to opponent
      if (ws.peer && m.m && typeof m.m === 'object') send(ws.peer, { t: 'r', m: m.m });
      return;
    }
    if (ws.state !== 'idle') { if (m.t === 'cancel' && ws.state === 'wait') { unqueue(ws); idle(ws); } return; }

    if (m.t === 'quick') {
      while (queue.length) {
        const o = queue.shift();
        if (o.readyState === 1 && o.state === 'wait') { pair(o, ws); return; }
      }
      queue.push(ws); ws.state = 'wait';
    } else if (m.t === 'room') {
      let code = null;
      for (let i = 0; i < 60 && !code; i++) {
        const c = String(1000 + Math.floor(Math.random() * 9000));
        if (!rooms.has(c)) code = c;
      }
      if (!code) { send(ws, { t: 'err', msg: 'Could not create a room' }); return; }
      rooms.set(code, ws); ws.room = code; ws.state = 'wait';
      send(ws, { t: 'code', c: code });
    } else if (m.t === 'join') {
      const code = String(m.code || '');
      const host = /^\d{4}$/.test(code) ? rooms.get(code) : null;
      if (!host || host === ws || host.readyState !== 1 || host.state !== 'wait') {
        send(ws, { t: 'err', msg: 'Room not found' }); return;
      }
      pair(host, ws);
    }
  });

  ws.on('close', () => {
    unqueue(ws);
    const p = ws.peer;
    if (p) { ws.peer = null; p.peer = null; idle(p); send(p, { t: 'left' }); }
  });
  ws.on('error', () => {});
});

setInterval(() => { wss.clients.forEach(ws => { ws.n = 0; }); }, 1000);
setInterval(() => {                                       // drop dead connections
  wss.clients.forEach(ws => {
    if (!ws.alive) { ws.terminate(); return; }
    ws.alive = false; try { ws.ping(); } catch (e) {}
  });
}, 20000);

server.listen(PORT, () => console.log('Duel Block server on :' + PORT));
