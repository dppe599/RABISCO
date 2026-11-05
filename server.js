'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const ROOT = __dirname;
const rooms = new Map();
const WORDS = [
  'abacaxi','avião','baleia','banana','bicicleta','bolo','borboleta','cachorro','cacto','cama',
  'câmera','canguru','carteira','cavalo','chave','chuva','computador','dinossauro','elefante','escada',
  'estrela','foguete','formiga','girassol','hambúrguer','helicóptero','jacaré','janela','leão','limão',
  'macaco','microfone','montanha','óculos','onça','panda','panqueca','paraguas','passarinho','piano',
  'pizza','planeta','pinguim','pirata','piscina','ponte','raio','robô','sanduíche',
  'sorvete','tartaruga','telefone','tesoura','tigre','tomate','trem','unicórnio','vampiro','violão',
  'vulcão','abajur','algodão-doce','âncora','arco-íris','aspirador','astronauta','bateria','berinjela','cabide',
  'cacto','cadeira','cereja','chapéu','chocolate','coração','dragão','escova','esquilo','fantasma',
  'foca','geladeira','guarda-chuva','ipê','lâmpada','melancia','mochila','nuvem','pão de queijo','pipoca',
  'raquete','relógio','sereia','skate','teclado','tênis','ventilador','violino','zebra','óvni'
].map(w => w.trim()).filter(Boolean);

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end('RABISCO ONLINE server online');
  }
  if (url.pathname === '/favicon.ico') {
    res.writeHead(204);
    return res.end();
  }
  let requested = url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname.slice(1));
  const filePath = path.resolve(ROOT, requested);
  if (!filePath.startsWith(ROOT + path.sep) && filePath !== path.join(ROOT, 'index.html')) {
    res.writeHead(403); return res.end('Forbidden');
  }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Arquivo não encontrado'); }
    const ext = path.extname(filePath).toLowerCase();
    const types = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'application/octet-stream', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});

class SimpleWebSocket extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.readyState = 1;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentBytes = 0;
    socket.on('data', chunk => this.consume(chunk));
    socket.on('close', () => this.finish());
    socket.on('end', () => this.finish());
    socket.on('error', error => { this.emit('error', error); this.finish(); });
  }
  frame(opcode, payload = Buffer.alloc(0)) {
    const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    let header;
    if (body.length < 126) {
      header = Buffer.from([0x80 | opcode, body.length]);
    } else if (body.length <= 65535) {
      header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(body.length, 2);
    } else {
      header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(body.length), 2);
    }
    return Buffer.concat([header, body]);
  }
  send(data) {
    if (this.readyState !== 1) return;
    this.socket.write(this.frame(1, Buffer.from(String(data))));
  }
  close() {
    if (this.readyState !== 1) return;
    this.readyState = 2;
    try { this.socket.write(this.frame(8)); } catch {}
    this.socket.end();
    this.finish();
  }
  finish() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit('close');
  }
  protocolError() {
    if (this.readyState !== 1) return;
    this.readyState = 2;
    try { this.socket.write(this.frame(8, Buffer.from([0x03, 0xea]))); } catch {}
    this.socket.end();
    this.finish();
  }
  consume(chunk) {
    if (this.readyState !== 1) return;
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 2) {
      const first = this.buffer[0], second = this.buffer[1];
      const fin = (first & 0x80) !== 0;
      const opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;
      let length = second & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (this.buffer.length < 4) return;
        length = this.buffer.readUInt16BE(2); offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) return;
        const bigLength = this.buffer.readBigUInt64BE(2);
        if (bigLength > 16384n) return this.protocolError();
        length = Number(bigLength); offset = 10;
      }
      if (!masked || length > 16384) return this.protocolError();
      if (this.buffer.length < offset + 4 + length) return;
      const mask = this.buffer.subarray(offset, offset + 4); offset += 4;
      const payload = Buffer.from(this.buffer.subarray(offset, offset + length));
      this.buffer = this.buffer.subarray(offset + length);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
      if (opcode === 8) {
        this.readyState = 2;
        try { this.socket.write(this.frame(8, payload)); } catch {}
        this.socket.end(); this.finish(); return;
      }
      if (opcode === 9) { this.socket.write(this.frame(10, payload)); continue; }
      if (opcode === 10) continue;
      if (opcode === 1 || opcode === 2) {
        if (this.fragments.length) return this.protocolError();
        if (fin) this.emit('message', payload);
        else { this.fragments = [payload]; this.fragmentBytes = payload.length; }
      } else if (opcode === 0) {
        if (!this.fragments.length) return this.protocolError();
        this.fragments.push(payload); this.fragmentBytes += payload.length;
        if (this.fragmentBytes > 16384) return this.protocolError();
        if (fin) {
          const complete = Buffer.concat(this.fragments, this.fragmentBytes);
          this.fragments = []; this.fragmentBytes = 0;
          this.emit('message', complete);
        }
      } else return this.protocolError();
    }
  }
}

server.on('upgrade', (request, socket) => {
  const key = request.headers['sec-websocket-key'];
  if (!key || String(request.headers.upgrade || '').toLowerCase() !== 'websocket') {
    socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
    socket.destroy(); return;
  }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  const ws = new SimpleWebSocket(socket);
  ws.on('message', raw => {
    try { handleMessage(ws, JSON.parse(raw.toString())); }
    catch (error) { send(ws, { type: 'error', message: 'Não consegui entender essa ação.' }); }
  });
  ws.on('close', () => removePlayer(ws));
  ws.on('error', () => removePlayer(ws));
  send(ws, { type: 'connected' });
});

function id() { return crypto.randomBytes(8).toString('hex'); }
function makeCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  do {
    code = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (rooms.has(code));
  return code;
}
function cleanName(value) {
  return String(value || 'Jogador').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 18) || 'Jogador';
}
function normalize(value) {
  return String(value || '').toLocaleLowerCase('pt-BR').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9 ]/g, '').replace(/\s+/g, ' ').trim();
}
function send(ws, payload) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(payload));
}
function systemMessage(room, text) {
  room.chat.push({ id: id(), kind: 'system', name: 'Sistema', text: String(text).slice(0, 160), at: Date.now() });
  room.chat = room.chat.slice(-45);
}
function stopTimers(room) {
  if (room.interval) clearInterval(room.interval);
  if (room.transition) clearTimeout(room.transition);
  room.interval = null;
  room.transition = null;
}
function playerById(room, playerId) { return room.players.find(p => p.id === playerId); }
function connectedPlayers(room) { return room.players.filter(p => p.ws && p.ws.readyState === 1); }
function publicState(room, player) {
  const drawer = playerById(room, room.drawerId);
  const isDrawer = !!player && player.id === room.drawerId;
  const guessed = room.guessedIds.has(player?.id);
  const word = room.word || '';
  let mask = word.split('').map(ch => ch === ' ' ? '   ' : '_').join(' ');
  if (room.phase === 'drawing' && room.timeLeft <= 40 && word) {
    const revealCount = Math.min(word.replace(/ /g, '').length, 1 + Math.floor((40 - room.timeLeft) / 15));
    let letterIndex = 0;
    mask = word.split('').map(ch => {
      if (ch === ' ') return '   ';
      const reveal = letterIndex < revealCount;
      letterIndex++;
      return reveal ? ch.toLocaleUpperCase('pt-BR') : '_';
    }).join(' ');
  }
  return {
    type: 'state', code: room.code, phase: room.phase, hostId: room.hostId,
    meId: player?.id || null,
    players: room.players.map(p => ({ id: p.id, name: p.name, score: p.score, connected: !!(p.ws && p.ws.readyState === 1), guessed: room.guessedIds.has(p.id) })),
    drawerId: room.drawerId,
    drawerName: drawer?.name || '',
    isDrawer,
    word: isDrawer || room.phase === 'reveal' || room.phase === 'finished' ? word : '',
    mask: room.phase === 'drawing' && !isDrawer ? mask : '',
    options: isDrawer && room.phase === 'choosing' ? room.options : [],
    timeLeft: room.timeLeft,
    roundNumber: room.roundNumber,
    totalRounds: room.totalRounds,
    turnNumber: room.turnIndex + 1,
    totalTurns: room.gameOrder.length * room.totalRounds,
    guessed,
    foundCount: room.guessedIds.size,
    guessCount: Math.max(0, connectedPlayers(room).filter(p => p.id !== room.drawerId).length),
    segments: room.segments,
    chat: room.chat
  };
}
function broadcastState(room) {
  for (const p of room.players) send(p.ws, publicState(room, p));
}
function broadcast(room, payload, exceptId = null) {
  for (const p of room.players) if (p.id !== exceptId) send(p.ws, payload);
}
function randomWords() {
  const pool = [...new Set(WORDS)];
  const result = [];
  while (result.length < 3 && pool.length) result.push(pool.splice(Math.floor(Math.random() * pool.length), 1)[0]);
  return result;
}
function beginTurn(room) {
  stopTimers(room);
  while (room.turnIndex < room.gameOrder.length * room.totalRounds) {
    const candidateId = room.gameOrder[room.turnIndex];
    const candidate = playerById(room, candidateId);
    if (candidate && candidate.ws && candidate.ws.readyState === 1) break;
    room.turnIndex++;
  }
  if (room.turnIndex >= room.gameOrder.length * room.totalRounds) return finishGame(room);

  const current = room.gameOrder[room.turnIndex];
  room.drawerId = current;
  room.roundNumber = Math.floor(room.turnIndex / room.gameOrder.length) + 1;
  room.phase = 'choosing';
  room.options = randomWords();
  room.word = '';
  room.timeLeft = 15;
  room.guessedIds = new Set();
  room.segments = [];
  systemMessage(room, `${playerById(room, current)?.name || 'O jogador'} vai desenhar!`);
  broadcastState(room);
  room.interval = setInterval(() => {
    if (room.phase !== 'choosing') return;
    room.timeLeft--;
    if (room.timeLeft <= 0) chooseWord(room, room.options[0]);
    else broadcastState(room);
  }, 1000);
}
function chooseWord(room, word) {
  if (room.phase !== 'choosing' || !room.options.includes(word)) return;
  stopTimers(room);
  room.word = word;
  room.phase = 'drawing';
  room.timeLeft = 80;
  systemMessage(room, 'A rodada começou! Desenhe e tente adivinhar.');
  broadcastState(room);
  room.interval = setInterval(() => {
    if (room.phase !== 'drawing') return;
    room.timeLeft--;
    if (room.timeLeft <= 0) endTurn(room);
    else broadcastState(room);
  }, 1000);
}
function endTurn(room) {
  if (!['drawing', 'choosing'].includes(room.phase)) return;
  stopTimers(room);
  if (room.phase === 'choosing' && room.options.length) room.word = room.options[0];
  room.phase = 'reveal';
  room.timeLeft = 0;
  systemMessage(room, `A palavra era: ${room.word || 'sem palavra'}`);
  broadcastState(room);
  room.transition = setTimeout(() => {
    room.turnIndex++;
    beginTurn(room);
  }, 3500);
}
function finishGame(room) {
  stopTimers(room);
  room.phase = 'finished';
  room.drawerId = null;
  room.timeLeft = 0;
  systemMessage(room, 'Fim de jogo! Veja a classificação.');
  broadcastState(room);
}
function addPlayer(ws, room, name) {
  const player = { id: id(), name: cleanName(name), score: 0, ws };
  room.players.push(player);
  ws.playerId = player.id;
  ws.roomCode = room.code;
  return player;
}
function removePlayer(ws) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const index = room.players.findIndex(p => p.id === ws.playerId);
  if (index < 0) return;
  const [player] = room.players.splice(index, 1);
  systemMessage(room, `${player.name} saiu da sala.`);
  if (room.players.length === 0) {
    stopTimers(room);
    rooms.delete(room.code);
    return;
  }
  if (room.hostId === player.id) room.hostId = room.players[0].id;
  if (room.phase !== 'lobby' && room.drawerId === player.id) {
    endTurn(room);
  } else if (room.phase === 'drawing') {
    const guessers = connectedPlayers(room).filter(p => p.id !== room.drawerId);
    if (guessers.length > 0 && guessers.every(p => room.guessedIds.has(p.id))) endTurn(room);
  }
  if (room.phase === 'finished' && room.players.length < 2) room.phase = 'lobby';
  broadcastState(room);
}
function createRoom(ws, name) {
  const code = makeCode();
  const room = {
    code, players: [], hostId: null, phase: 'lobby', totalRounds: 2,
    roundNumber: 0, turnIndex: 0, gameOrder: [], drawerId: null,
    options: [], word: '', timeLeft: 0, guessedIds: new Set(), segments: [], chat: [],
    interval: null, transition: null
  };
  rooms.set(code, room);
  const player = addPlayer(ws, room, name);
  room.hostId = player.id;
  systemMessage(room, `${player.name} criou a sala.`);
  broadcastState(room);
}
function joinRoom(ws, code, name) {
  const room = rooms.get(String(code || '').toUpperCase().replace(/[^A-Z0-9]/g, ''));
  if (!room) return send(ws, { type: 'error', message: 'Sala não encontrada. Confira o código.' });
  if (room.phase !== 'lobby') return send(ws, { type: 'error', message: 'Essa partida já começou.' });
  if (room.players.length >= 8) return send(ws, { type: 'error', message: 'A sala está cheia (máximo de 8 jogadores).' });
  const player = addPlayer(ws, room, name);
  systemMessage(room, `${player.name} entrou na sala.`);
  broadcastState(room);
}
function handleMessage(ws, msg) {
  if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;
  if (msg.type === 'create') return createRoom(ws, msg.name);
  if (msg.type === 'join') return joinRoom(ws, msg.code, msg.name);
  const room = rooms.get(ws.roomCode);
  const player = room && playerById(room, ws.playerId);
  if (!room || !player) return send(ws, { type: 'error', message: 'Entre em uma sala primeiro.' });

  if (msg.type === 'start') {
    if (player.id !== room.hostId) return send(ws, { type: 'error', message: 'Só o anfitrião pode iniciar.' });
    if (room.phase !== 'lobby' || room.players.length < 2) return send(ws, { type: 'error', message: 'São necessários pelo menos 2 jogadores.' });
    room.totalRounds = Math.max(1, Math.min(5, Number(msg.rounds) || 2));
    room.players.forEach(p => p.score = 0);
    room.gameOrder = room.players.map(p => p.id);
    room.turnIndex = 0;
    room.chat = [];
    systemMessage(room, 'A partida começou! Boa sorte!');
    beginTurn(room);
    return;
  }
  if (msg.type === 'choose') {
    if (player.id !== room.drawerId) return;
    return chooseWord(room, String(msg.word || ''));
  }
  if (msg.type === 'draw') {
    if (room.phase !== 'drawing' || player.id !== room.drawerId) return;
    const x1 = Number(msg.x1), y1 = Number(msg.y1), x2 = Number(msg.x2), y2 = Number(msg.y2);
    if (![x1,y1,x2,y2].every(Number.isFinite)) return;
    const segment = {
      x1: Math.max(0, Math.min(1, x1)), y1: Math.max(0, Math.min(1, y1)),
      x2: Math.max(0, Math.min(1, x2)), y2: Math.max(0, Math.min(1, y2)),
      color: /^#[0-9a-fA-F]{6}$/.test(msg.color) ? msg.color : '#26334a',
      size: Math.max(1, Math.min(30, Number(msg.size) || 5)),
      erase: msg.erase === true
    };
    room.segments.push(segment);
    if (room.segments.length > 5000) room.segments.splice(0, room.segments.length - 5000);
    broadcast(room, { type: 'draw', segment }, player.id);
    return;
  }
  if (msg.type === 'clear') {
    if (room.phase !== 'drawing' || player.id !== room.drawerId) return;
    room.segments = [];
    broadcast(room, { type: 'clear' });
    return;
  }
  if (msg.type === 'guess') {
    if (room.phase !== 'drawing' || player.id === room.drawerId || room.guessedIds.has(player.id)) return;
    const guess = String(msg.text || '').trim().slice(0, 80);
    if (!guess) return;
    if (normalize(guess) === normalize(room.word)) {
      const points = Math.max(20, Math.round(room.timeLeft * 1.25));
      player.score += points;
      const drawer = playerById(room, room.drawerId);
      if (drawer) drawer.score += 15;
      room.guessedIds.add(player.id);
      room.chat.push({ id: id(), kind: 'correct', name: player.name, text: `acertou! +${points} pontos`, at: Date.now() });
      room.chat = room.chat.slice(-45);
      broadcastState(room);
      const guessers = connectedPlayers(room).filter(p => p.id !== room.drawerId);
      if (guessers.length && guessers.every(p => room.guessedIds.has(p.id))) {
        room.transition = setTimeout(() => endTurn(room), 1200);
      }
    } else {
      room.chat.push({ id: id(), kind: 'guess', name: player.name, text: guess, at: Date.now() });
      room.chat = room.chat.slice(-45);
      broadcastState(room);
    }
    return;
  }
  if (msg.type === 'playAgain') {
    if (player.id !== room.hostId || room.phase !== 'finished' || room.players.length < 2) return;
    room.players.forEach(p => p.score = 0);
    room.gameOrder = room.players.map(p => p.id);
    room.turnIndex = 0;
    room.chat = [];
    systemMessage(room, 'Nova partida!');
    beginTurn(room);
  }
}

server.listen(PORT, HOST, () => {
  console.log(`Rabisco Online rodando em http://${HOST}:${PORT}`);
});
