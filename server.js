const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const rooms = new Map();

const wss = new WebSocketServer({ port: PORT });

wss.on('connection', (ws) => {
  let peerId = null;
  let roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case 'join': {
        roomId = msg.room;
        peerId = msg.peerId || uuidv4();
        ws.peerId = peerId;
        ws.roomId = roomId;

        if (!rooms.has(roomId)) rooms.set(roomId, new Map());
        const room = rooms.get(roomId);
        room.set(peerId, ws);

        ws.send(JSON.stringify({ type: 'joined', peerId, roomId }));

        const otherPeers = [...room.keys()].filter(id => id !== peerId);
        if (otherPeers.length > 0) {
          ws.send(JSON.stringify({ type: 'peers', peers: otherPeers }));
          otherPeers.forEach(otherId => {
            const other = room.get(otherId);
            if (other && other.readyState === 1) {
              other.send(JSON.stringify({ type: 'peer_joined', peerId }));
            }
          });
        }
        break;
      }

      case 'offer':
      case 'answer':
      case 'ice_candidate': {
        if (!roomId || !rooms.has(roomId)) break;
        const room = rooms.get(roomId);
        const target = room.get(msg.target);
        if (target && target.readyState === 1) {
          target.send(JSON.stringify({
            type: msg.type,
            sender: peerId,
            data: msg.data,
          }));
        }
        break;
      }

      case 'control': {
        if (!roomId || !rooms.has(roomId)) break;
        const room = rooms.get(roomId);
        const target = room.get(msg.target);
        if (target && target.readyState === 1) {
          target.send(JSON.stringify({
            type: 'control',
            sender: peerId,
            data: msg.data,
          }));
        }
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (roomId && rooms.has(roomId)) {
      const room = rooms.get(roomId);
      room.delete(peerId);
      if (room.size === 0) rooms.delete(roomId);
      else {
        room.forEach((other) => {
          if (other.readyState === 1) {
            other.send(JSON.stringify({ type: 'peer_left', peerId }));
          }
        });
      }
    }
  });
});

console.log(`Signaling server running on port ${PORT}`);
