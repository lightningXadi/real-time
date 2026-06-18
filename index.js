import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

const dbPath = process.env.DB_PATH || 'chat.db';
const db = await open({
  filename: dbPath,
  driver: sqlite3.Database
});

await db.exec(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_offset TEXT UNIQUE,
    content TEXT,
    user_id TEXT,
    color_index INTEGER
  );
`);

const app = express();
const server = createServer(app);
const io = new Server(server, {
  connectionStateRecovery: {}
});

const __dirname = dirname(fileURLToPath(import.meta.url));

// Track colour assignment on the server — stable per socket.id
const userColors = {};
let nextColor = 0;

function getColor(uid) {
  if (!(uid in userColors)) {
    userColors[uid] = nextColor++ % 10;
  }
  return userColors[uid];
}

app.get('/', (req, res) => {
  res.sendFile(join(__dirname, 'index.html'));
});

io.on('connection', async (socket) => {
  const userId = socket.id;
  // Assign colour on connect so it's ready before any message
  getColor(userId);

  // Tell this client what their own userId is
  socket.emit('your id', userId);

  socket.on('chat message', async (msg, clientOffset, callback) => {
    const colorIndex = getColor(userId);
    let result;
    try {
      result = await db.run(
        'INSERT INTO messages (content, client_offset, user_id, color_index) VALUES (?, ?, ?, ?)',
        msg, clientOffset, userId, colorIndex
      );
    } catch (e) {
      if (e.errno === 19) { callback(); }
      return;
    }
    // Broadcast: msg, serverOffset, senderId, colorIndex
    io.emit('chat message', msg, result.lastID, userId, colorIndex);
    callback();
  });

  socket.on('typing', () => {
    socket.broadcast.emit('typing', userId, getColor(userId));
  });

  socket.on('stop typing', () => {
    socket.broadcast.emit('stop typing', userId);
  });

  socket.on('clear chat', async () => {
    try {
      await db.run('DELETE FROM messages');
      await db.run("DELETE FROM sqlite_sequence WHERE name='messages'");
      // Reset colour assignments
      Object.keys(userColors).forEach(k => delete userColors[k]);
      nextColor = 0;
      io.emit('chat cleared');
    } catch (e) {
      console.error('Failed to clear chat:', e);
    }
  });

  socket.on('disconnect', () => {
    delete userColors[userId];
    socket.broadcast.emit('stop typing', userId);
  });

  if (!socket.recovered) {
    try {
      await db.each(
        'SELECT id, content, user_id, color_index FROM messages WHERE id > ?',
        [socket.handshake.auth.serverOffset || 0],
        (_err, row) => {
          socket.emit('chat message', row.content, row.id, row.user_id, row.color_index ?? 0);
        }
      );
    } catch (e) {}
  }
});

const port = process.env.PORT || 3000;
server.listen(port, () => {
  console.log(`server running at http://localhost:${port}`);
});
