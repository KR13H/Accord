import Peer from "peerjs";

import { flushQueuedSales, getQueuedSales, mergeSharedQueueEntries } from "./offlineQueue";

const PEER_STORAGE_KEY = "accordMeshPeers";
const channelNameForBusiness = (businessId) => `accord-mesh-${businessId}`;

function sanitizeBusinessId(businessId) {
  return String(businessId || "SME-001")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-") || "sme-001";
}

function readKnownPeers() {
  try {
    return JSON.parse(window.localStorage.getItem(PEER_STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

function writeKnownPeers(peers) {
  window.localStorage.setItem(PEER_STORAGE_KEY, JSON.stringify(peers.slice(-32)));
}

async function buildSnapshotPayload() {
  const queue = await getQueuedSales();
  return queue.map((entry) => ({
    payload: entry.payload,
    createdAt: entry.createdAt,
  }));
}

export function initMeshSync({ businessId = "SME-001", onStatus } = {}) {
  if (typeof window === "undefined") {
    return () => {};
  }

  const normalizedBusinessId = sanitizeBusinessId(businessId);
  const localPeerId = `accord-${normalizedBusinessId}-${window.crypto.randomUUID().slice(0, 8)}`;
  const channel = "BroadcastChannel" in window ? new BroadcastChannel(channelNameForBusiness(normalizedBusinessId)) : null;
  const connections = new Map();

  const report = (message) => {
    if (typeof onStatus === "function") {
      onStatus(message);
    }
  };

  let peer;
  try {
    peer = new Peer(localPeerId);
  } catch {
    report("Mesh sync unavailable");
    return () => {};
  }

  const sendSnapshot = async (connection) => {
    if (!connection || !connection.open) {
      return;
    }
    const queueEntries = await buildSnapshotPayload();
    connection.send({
      type: "QUEUE_SNAPSHOT",
      businessId: normalizedBusinessId,
      fromPeerId: localPeerId,
      entries: queueEntries,
    });
  };

  const handleIncomingData = async (data, connection) => {
    if (!data || data.type !== "QUEUE_SNAPSHOT" || data.businessId !== normalizedBusinessId) {
      return;
    }
    const { merged } = await mergeSharedQueueEntries(data.entries || []);
    if (merged > 0 && navigator.onLine) {
      await flushQueuedSales();
    }
    report(`Mesh sync merged ${merged} entries`);
    await sendSnapshot(connection);
  };

  const setupConnection = (connection) => {
    if (!connection || connection.peer === localPeerId || connections.has(connection.peer)) {
      return;
    }

    connections.set(connection.peer, connection);

    connection.on("open", async () => {
      report(`Mesh peer connected: ${connection.peer}`);
      await sendSnapshot(connection);
    });

    connection.on("data", async (data) => {
      try {
        await handleIncomingData(data, connection);
      } catch {
        report("Mesh sync merge failed");
      }
    });

    connection.on("close", () => {
      connections.delete(connection.peer);
    });

    connection.on("error", () => {
      connections.delete(connection.peer);
      report(`Mesh peer error: ${connection.peer}`);
    });
  };

  peer.on("open", (id) => {
    const existing = readKnownPeers();
    const next = [...existing.filter((peerId) => peerId !== id), id];
    writeKnownPeers(next);
    report(`Mesh node online: ${id}`);

    for (const peerId of existing) {
      if (!peerId || peerId === id) {
        continue;
      }
      setupConnection(peer.connect(peerId, { reliable: true }));
    }

    if (channel) {
      channel.postMessage({ type: "PEER_ANNOUNCE", peerId: id });
    }
  });

  peer.on("connection", (connection) => {
    setupConnection(connection);
  });

  peer.on("error", () => {
    report("Mesh node error");
  });

  if (channel) {
    channel.onmessage = (event) => {
      const payload = event?.data;
      if (!payload || payload.type !== "PEER_ANNOUNCE") {
        return;
      }
      const remotePeerId = payload.peerId;
      if (!remotePeerId || remotePeerId === localPeerId || connections.has(remotePeerId)) {
        return;
      }
      setupConnection(peer.connect(remotePeerId, { reliable: true }));
    };
  }

  return () => {
    try {
      channel?.close();
    } catch {
      // no-op
    }

    for (const connection of connections.values()) {
      try {
        connection.close();
      } catch {
        // no-op
      }
    }

    connections.clear();

    try {
      peer.destroy();
    } catch {
      // no-op
    }
  };
}
