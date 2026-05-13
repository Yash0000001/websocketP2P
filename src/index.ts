// websocket-server.ts

import express from "express";
import http from "http";
import cors from "cors";
import { WebSocketServer, WebSocket } from "ws";

const app = express();

app.use(cors());

const server = http.createServer(app);

const wss = new WebSocketServer({ server });

type PeerData = {
  socket: WebSocket;
  peerId: string;
  roomId: string;
};

const peers = new Map<WebSocket, PeerData>();

const rooms = new Map<string, Set<WebSocket>>();

function broadcastToRoom(
  roomId: string,
  sender: WebSocket,
  payload: unknown
) {
  const room = rooms.get(roomId);

  if (!room) return;

  room.forEach((client) => {
    if (
      client !== sender &&
      client.readyState === WebSocket.OPEN
    ) {
      client.send(JSON.stringify(payload));
    }
  });
}

wss.on("connection", (ws) => {
  console.log("Socket connected");

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      switch (data.type) {
        case "join-room": {
          const { roomId, peerId } = data;

          if (!roomId || !peerId) {
            return;
          }

          peers.set(ws, {
            socket: ws,
            peerId,
            roomId,
          });

          if (!rooms.has(roomId)) {
            rooms.set(roomId, new Set());
          }

          rooms.get(roomId)?.add(ws);

          const existingPeers = [
            ...(rooms.get(roomId) || []),
          ]
            .filter((client) => client !== ws)
            .map((client) => peers.get(client)?.peerId)
            .filter(Boolean);

          ws.send(
            JSON.stringify({
              type: "existing-peers",
              peers: existingPeers,
            })
          );

          broadcastToRoom(roomId, ws, {
            type: "user-joined",
            peerId,
          });

          console.log(
            `Peer ${peerId} joined room ${roomId}`
          );

          break;
        }

        case "offer":
        case "answer":
        case "ice-candidate": {
          const { targetPeerId } = data;

          const targetSocket = [...peers.entries()].find(
            ([_, value]) =>
              value.peerId === targetPeerId
          )?.[0];

          if (
            targetSocket &&
            targetSocket.readyState === WebSocket.OPEN
          ) {
            targetSocket.send(JSON.stringify(data));
          }

          break;
        }

        default:
          break;
      }
    } catch (err) {
      console.error("Message error:", err);
    }
  });

  ws.on("close", () => {
    const peer = peers.get(ws);

    if (!peer) return;

    const { roomId, peerId } = peer;

    rooms.get(roomId)?.delete(ws);

    const room = rooms.get(roomId);

    if (room && room.size === 0) {
      rooms.delete(roomId);
    }

    peers.delete(ws);

    broadcastToRoom(roomId, ws, {
      type: "user-left",
      peerId,
    });

    console.log(
      `Peer ${peerId} disconnected from room ${roomId}`
    );
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err);
  });
});

server.listen(8081, () => {
  console.log(
    "WebSocket signaling server running on port 8081"
  );
});