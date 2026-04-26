import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { availableParallelism } from 'node:os';
import cluster from 'node:cluster';
import { createAdapter, setupPrimary } from '@socket.io/cluster-adapter';

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({ PORT: 3000 + i });
  }
  setupPrimary();
} else {
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT,
      user_id TEXT
    );
  `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter()
  });

  const __dirname = dirname(fileURLToPath(import.meta.url));

  app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'index.html'));
  });

  io.on('connection', async (socket) => {
    const userId = socket.id;

    socket.on('chat message', async (msg, clientOffset, callback) => {
      let result;
      try {
        result = await db.run(
          'INSERT INTO messages (content, client_offset, user_id) VALUES (?, ?, ?)',
          msg, clientOffset, userId
        );
      } catch (e) {
        if (e.errno === 19) { callback(); }
        return;
      }
      io.emit('chat message', msg, result.lastID, userId);
      callback();
    });

    socket.on('clear chat', async () => {
      try {
        await db.run('DELETE FROM messages');
        await db.run("DELETE FROM sqlite_sequence WHERE name='messages'");
        io.emit('chat cleared');
      } catch (e) {
        console.error('Failed to clear chat:', e);
      }
    });

    if (!socket.recovered) {
      try {
        await db.each('SELECT id, content, user_id FROM messages WHERE id > ?',
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            socket.emit('chat message', row.content, row.id, row.user_id || 'unknown');
          }
        );
      } catch (e) {}
    }
  });

  const port = process.env.PORT;
  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}
