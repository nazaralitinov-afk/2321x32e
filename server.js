/**
 * LizardCraft multiplayer server
 * - Persists room edits to disk (survives restarts / all players leave)
 * - Broadcasts blocks, presence, chat, drops
 *
 * Deploy on Render: Web Service, start command: node server.js
 * Env: PORT (auto), WORLD_DIR optional (default ./worlds)
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const WORLD_DIR = process.env.WORLD_DIR || path.join(__dirname, "worlds");
const SAVE_EVERY_MS = 8000;

try { fs.mkdirSync(WORLD_DIR, { recursive: true }); } catch (e) {}

function worldPath(code) {
  const safe = String(code || "GLOBAL").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 64);
  return path.join(WORLD_DIR, safe + ".json");
}

function loadWorld(code) {
  try {
    const raw = fs.readFileSync(worldPath(code), "utf8");
    const data = JSON.parse(raw);
    return {
      edits: data.edits || {},
      beds: data.beds || [],
      torchFaces: data.torchFaces || [],
      ladderFaces: data.ladderFaces || [],
      chests: data.chests || [],
      claims: data.claims || [],
    };
  } catch (e) {
    return { edits: {}, beds: [], torchFaces: [], ladderFaces: [], chests: [], claims: [] };
  }
}

function saveWorld(code, room) {
  try {
    const data = {
      v: 1,
      code,
      savedAt: Date.now(),
      edits: room.edits || {},
      beds: room.beds || [],
      torchFaces: room.torchFaces || [],
      ladderFaces: room.ladderFaces || [],
      chests: room.chests || [],
      claims: room.claims || [],
    };
    fs.writeFileSync(worldPath(code), JSON.stringify(data));
  } catch (e) {
    console.warn("save failed", code, e.message);
  }
}

/** Apply one block into edits map: key "cx,cz" -> flat [i,t,i,t,...] or object */
function applyBlk(edits, x, y, z, t) {
  const CS = 16, H = 128;
  if (y < 0 || y >= H) return;
  const cx = x >> 4, cz = z >> 4;
  const lx = x & 15, lz = z & 15;
  const i = (y * CS + lz) * CS + lx;
  const key = cx + "," + cz;
  let arr = edits[key];
  if (!arr) arr = edits[key] = [];
  // arr is alternating i,t
  let found = false;
  for (let j = 0; j < arr.length; j += 2) {
    if (arr[j] === i) {
      arr[j + 1] = t;
      found = true;
      break;
    }
  }
  if (!found) arr.push(i, t);
}

const rooms = new Map(); // code -> { clients:Set, edits, beds, ..., dirty, hostId }

function getRoom(code) {
  code = String(code || "GLOBAL");
  let r = rooms.get(code);
  if (!r) {
    const w = loadWorld(code);
    r = {
      code,
      clients: new Set(),
      edits: w.edits,
      beds: w.beds,
      torchFaces: w.torchFaces,
      ladderFaces: w.ladderFaces,
      chests: w.chests,
      claims: w.claims,
      dirty: false,
      hostId: null,
    };
    rooms.set(code, r);
  }
  return r;
}

function broadcast(room, msg, except) {
  const s = JSON.stringify(msg);
  for (const c of room.clients) {
    if (c === except) continue;
    if (c.readyState === 1) try { c.send(s); } catch (e) {}
  }
}

function uid() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.url === "/" || req.url === "/health") {
    let players = 0;
    for (const r of rooms.values()) players += r.clients.size;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, game: "LizardCraft", rooms: rooms.size, players }));
    return;
  }
  res.writeHead(404);
  res.end("not found");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.id = uid();
  ws.roomCode = null;
  ws.nick = "";
  try { ws.send(JSON.stringify({ type: "hello", id: ws.id })); } catch (e) {}

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(String(raw)); } catch (e) { return; }
    if (!msg || !msg.type) return;

    if (msg.type === "join") {
      const code = String(msg.code || "GLOBAL").slice(0, 64);
      if (ws.roomCode) {
        const old = rooms.get(ws.roomCode);
        if (old) {
          old.clients.delete(ws);
          if (old.hostId === ws.id) old.hostId = [...old.clients][0]?.id || null;
          if (old.clients.size === 0) {
            saveWorld(old.code, old);
            rooms.delete(old.code);
          } else broadcast(old, { type: "bye", id: ws.id });
        }
      }
      const room = getRoom(code);
      ws.roomCode = code;
      ws.nick = String(msg.nick || "").slice(0, 24);
      room.clients.add(ws);
      if (!room.hostId) room.hostId = ws.id;
      const isHost = room.hostId === ws.id;
      try {
        ws.send(JSON.stringify({
          type: "joined",
          id: ws.id,
          code: room.code,
          host: isHost,
          players: room.clients.size,
          edits: room.edits,
          beds: room.beds,
          torchFaces: room.torchFaces,
          ladderFaces: room.ladderFaces,
          chests: room.chests,
          claims: room.claims,
        }));
      } catch (e) {}
      broadcast(room, { type: "peer", id: ws.id, nick: ws.nick }, ws);
      return;
    }

    const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
    if (!room) return;

    if (msg.type === "blk") {
      const x = msg.x | 0, y = msg.y | 0, z = msg.z | 0, t = msg.t | 0;
      applyBlk(room.edits, x, y, z, t);
      room.dirty = true;
      broadcast(room, { type: "blk", x, y, z, t, id: ws.id }, ws);
      return;
    }

    if (msg.type === "world_save") {
      if (msg.edits && typeof msg.edits === "object") room.edits = msg.edits;
      if (Array.isArray(msg.beds)) room.beds = msg.beds;
      if (Array.isArray(msg.torchFaces)) room.torchFaces = msg.torchFaces;
      if (Array.isArray(msg.ladderFaces)) room.ladderFaces = msg.ladderFaces;
      if (Array.isArray(msg.chests)) room.chests = msg.chests;
      if (Array.isArray(msg.claims)) {
        room.claims = msg.claims;
        broadcast(room, { type: "claims", claims: room.claims }, ws);
      }
      room.dirty = true;
      saveWorld(room.code, room);
      return;
    }

    if (msg.type === "pres") {
      msg.id = ws.id;
      if (ws.nick) msg.nick = ws.nick;
      broadcast(room, msg, ws);
      return;
    }

    if (msg.type === "chat") {
      broadcast(room, {
        type: "chat",
        text: String(msg.text || "").slice(0, 100),
        nick: ws.nick || msg.nick || "?",
        id: ws.id,
      });
      return;
    }

    if (msg.type === "drop" || msg.type === "pickup" || msg.type === "hit") {
      msg.id = msg.id || ws.id;
      broadcast(room, msg, ws);
      return;
    }
  });

  ws.on("close", () => {
    const code = ws.roomCode;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    room.clients.delete(ws);
    broadcast(room, { type: "bye", id: ws.id });
    if (room.hostId === ws.id) room.hostId = [...room.clients][0]?.id || null;
    if (room.clients.size === 0) {
      saveWorld(room.code, room);
      rooms.delete(room.code);
      console.log("room saved & closed", code);
    }
  });
});

setInterval(() => {
  for (const room of rooms.values()) {
    if (room.dirty) {
      saveWorld(room.code, room);
      room.dirty = false;
    }
  }
}, SAVE_EVERY_MS);

server.listen(PORT, () => console.log("LizardCraft MP on :" + PORT, "worlds →", WORLD_DIR));
